/* rocepool_fuse.c -- the native C client: a kernel FUSE daemon for the
 * distributed layout (MDTs + striped OSTs), replacing the Python pair
 * fs/fuse_lowlevel.py + fs/posix_api.py DistributedFileSystem on the data
 * and metadata path. Same kernel protocol (raw /dev/fuse, <linux/fuse.h>,
 * no libfuse), same wire formats (roce/protocol.py frames, fs/mdt_wire.py,
 * the OST PUT/GET/STAT/DELETE encodings of roce/pool_node.py), same layout
 * rules (fs/namespace_shard.py MDT choice, fs/layout.py stripe offset,
 * "<path>.stripe.%08d" keys) -- so files written by one client are read
 * correctly by the other (tests/test_native_fuse.py checks both ways).
 *
 * What is different from the Python client, on purpose:
 *   - no GIL: every FUSE worker thread and every stripe task runs truly in
 *     parallel; RPCs go straight to roce_transport.c's pooled connections;
 *   - 1 MiB kernel requests (FUSE_MAX_PAGES / big writes) instead of 128 KiB;
 *   - a directory listing is fetched once per opendir and served to every
 *     READDIR offset from the handle (the Python daemon re-listed the whole
 *     directory for every READDIR chunk);
 *   - a handle's buffered writes are capped (64 MiB, then flushed early).
 * Semantics kept: close-to-open consistency with a per-handle stripe read
 * cache + readahead, TTL attribute cache incl. negative entries, CAS
 * read-modify-write for partial stripes, whole-stripe blind PUTs.
 *
 * Usage: rocepool-fuse --mount DIR --mdts a:p,b:p --osts a:p,... [--attr-ttl 3]
 *        [--threads 16] [--io-threads 32] [--stripe-count N] [--stripe-size B]
 *        [--kernel-cache 1] [--readahead N]
 * Prints "READY" once mounted. Unmount (umount -l DIR) or SIGTERM to stop.
 */
#define _GNU_SOURCE
#include "rocepool_client.h"

#include <errno.h>
#include <fcntl.h>
/* no <sys/mount.h>: glibc's and the kernel's linux/fs.h (pulled in by older
 * linux/io_uring.h) define the same MS_ and BLK constants and conflict, in
 * either order, on some header combinations (Ubuntu 22.04). The three calls
 * and their constants are declared here instead -- stable syscall ABI. */
#include <linux/fuse.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <linux/io_uring.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#include "rocepool_fs.h"

/* ---------------------------------------------------------------- inode table */
#define IT_BUCKETS 262144
typedef struct it_ent {
    struct it_ent *next_p, *next_i;
    char *path;
    uint64_t ino;
    uint64_t nlookup;  /* kernel references (entry replies minus FORGETs) */
} it_ent;

static it_ent *g_by_path[IT_BUCKETS], *g_by_ino[IT_BUCKETS];
static pthread_mutex_t g_it_lk = PTHREAD_MUTEX_INITIALIZER;
static uint64_t g_next_ino = 2;

/* The inode for path, creating it; ref = 1 when the reply hands the kernel a
 * new reference (LOOKUP/CREATE/MKDIR entry replies), which FORGET releases.
 * Entries whose count drops to 0 are removed, so the table tracks what the
 * kernel still holds instead of every path ever seen. */
static uint64_t ino_get(const char *path, int ref) {
    if (!*path) return FUSE_ROOT_ID;
    uint32_t b = strhash(path) % IT_BUCKETS;
    pthread_mutex_lock(&g_it_lk);
    it_ent *e;
    for (e = g_by_path[b]; e; e = e->next_p)
        if (!strcmp(e->path, path)) break;
    if (!e) {
        e = calloc(1, sizeof(*e));
        e->path = xstrdup(path);
        e->ino = g_next_ino++;
        e->next_p = g_by_path[b];
        g_by_path[b] = e;
        uint32_t bi = (uint32_t)(e->ino % IT_BUCKETS);
        e->next_i = g_by_ino[bi];
        g_by_ino[bi] = e;
    }
    if (ref) e->nlookup++;
    uint64_t ino = e->ino;
    pthread_mutex_unlock(&g_it_lk);
    return ino;
}

/* d_ino for a READDIR entry: the live inode if the kernel holds one, else
 * a stable placeholder (plain READDIR hands out no references) */
static uint64_t ino_peek(const char *path) {
    uint32_t b = strhash(path) % IT_BUCKETS;
    uint64_t ino = 0;
    pthread_mutex_lock(&g_it_lk);
    for (it_ent *e = g_by_path[b]; e; e = e->next_p)
        if (!strcmp(e->path, path)) { ino = e->ino; break; }
    pthread_mutex_unlock(&g_it_lk);
    return ino ? ino : (sha256_u64(path) | (1ull << 63));
}

static void ino_forget(uint64_t ino, uint64_t n) {
    if (ino == FUSE_ROOT_ID) return;
    pthread_mutex_lock(&g_it_lk);
    it_ent **pi, *e = NULL;
    for (pi = &g_by_ino[ino % IT_BUCKETS]; *pi; pi = &(*pi)->next_i)
        if ((*pi)->ino == ino) { e = *pi; break; }
    if (e) {
        e->nlookup = e->nlookup > n ? e->nlookup - n : 0;
        if (e->nlookup == 0) {
            *pi = e->next_i;
            for (it_ent **pp = &g_by_path[strhash(e->path) % IT_BUCKETS]; *pp; pp = &(*pp)->next_p)
                if (*pp == e) { *pp = e->next_p; break; }
            free(e->path);
            free(e);
        }
    }
    pthread_mutex_unlock(&g_it_lk);
}

/* copies the path of ino into buf; -ENOENT if unknown */
static int path_for_ino(uint64_t ino, char *buf, size_t n) {
    if (ino == FUSE_ROOT_ID) { buf[0] = 0; return 0; }
    pthread_mutex_lock(&g_it_lk);
    it_ent *e;
    for (e = g_by_ino[ino % IT_BUCKETS]; e; e = e->next_i)
        if (e->ino == ino) break;
    int rc = -ENOENT;
    if (e && strlen(e->path) < n) { strcpy(buf, e->path); rc = 0; }
    pthread_mutex_unlock(&g_it_lk);
    return rc;
}

static int child_path(uint64_t parent, const char *name, char *buf, size_t n) {
    char par[4096];
    int rc = path_for_ino(parent, par, sizeof(par));
    if (rc) return rc;
    int w = *par ? snprintf(buf, n, "%s/%s", par, name) : snprintf(buf, n, "%s", name);
    return (w < 0 || (size_t)w >= n) ? -ENAMETOOLONG : 0;
}

/* ---------------------------------------------------------------- FUSE plumbing */
static int g_fuse_fd = -1;
static atomic_int g_stop;

/* ---------------------------------------------------------------- FUSE over io_uring
 * (Linux 6.14+, fs/fuse/dev_uring.c; module parameter fuse.enable_uring).
 * One queue per possible CPU, each with its own io_uring and a few ring
 * entries; each entry is a header buffer (in/out header, op header, ring
 * fields) and a payload buffer (request data in, reply data out). REGISTER
 * hands an entry to the kernel; a completion means a request is in it;
 * COMMIT_AND_FETCH sends the reply and re-arms the entry. The kernel routes
 * every request to the queue of the CPU it was issued on, so there is no
 * shared /dev/fuse channel to contend on and no read()/writev() per
 * request. The classic workers stay for what still comes through
 * /dev/fuse (FORGET, and everything until every queue is registered). */
/* uapi (include/uapi/linux/fuse.h, API 7.43+), spelled out here because the
 * system header may predate it */
#define FUSE_OVER_IO_URING_FLAG2 (1u << 9)   /* fuse_init flags2 bit: (1ULL << 41) >> 32 */
/* Everything newer than the oldest supported build host (Ubuntu 22.04:
 * kernel 5.15 uapi headers, glibc 2.35) is spelled out here by its ABI
 * value / offset -- these never change -- so the client builds against any
 * header version; whether the RUNNING kernel offers FUSE over io_uring is
 * negotiated in FUSE_INIT as before. Checked against the header when it has
 * them (the static asserts below). */
#define RP_FUSE_INIT_EXT (1u << 30)               /* fuse_init in/out flags: flags2 is valid (7.36) */
#define RP_INIT_IN_FLAGS2_OFF 16                  /* struct fuse_init_in.flags2 */
#define RP_INIT_OUT_FLAGS2_OFF 32                 /* struct fuse_init_out.flags2 (was unused[0]) */
#define RP_IORING_SETUP_SQE128 (1u << 10)
#define RP_IORING_SETUP_CQE32 (1u << 11)
#define RP_IORING_OP_URING_CMD 46
#define RP_SQE_CMD_OP_OFF 8                       /* struct io_uring_sqe.cmd_op */
#define RP_SQE_CMD_OFF 48                         /* struct io_uring_sqe.cmd[] */
#ifndef MS_NOSUID
#define MS_NOSUID 2
#endif
#ifndef MS_NODEV
#define MS_NODEV 4
#endif
#ifndef MNT_DETACH
#define MNT_DETACH 2
#endif
extern int mount(const char *source, const char *target, const char *fstype, unsigned long flags, const void *data);
extern int umount2(const char *target, int flags);
#ifdef FUSE_INIT_EXT
_Static_assert(FUSE_INIT_EXT == RP_FUSE_INIT_EXT, "FUSE_INIT_EXT");
_Static_assert(__builtin_offsetof(struct fuse_init_in, flags2) == RP_INIT_IN_FLAGS2_OFF, "init_in.flags2");
_Static_assert(__builtin_offsetof(struct fuse_init_out, flags2) == RP_INIT_OUT_FLAGS2_OFF, "init_out.flags2");
#endif
#ifdef IORING_SETUP_SQE128
_Static_assert(IORING_SETUP_SQE128 == RP_IORING_SETUP_SQE128 && IORING_SETUP_CQE32 == RP_IORING_SETUP_CQE32, "SQE128");
_Static_assert(IORING_OP_URING_CMD == RP_IORING_OP_URING_CMD, "URING_CMD");
_Static_assert(__builtin_offsetof(struct io_uring_sqe, cmd_op) == RP_SQE_CMD_OP_OFF, "cmd_op");
_Static_assert(__builtin_offsetof(struct io_uring_sqe, cmd) == RP_SQE_CMD_OFF, "cmd");
#endif
_Static_assert(sizeof(struct fuse_init_out) == 64, "fuse_init_out");
#define FUSE_URING_IN_OUT_HEADER_SZ 128
#define FUSE_URING_OP_IN_OUT_SZ 128
enum { FUSE_IO_URING_CMD_REGISTER = 1, FUSE_IO_URING_CMD_COMMIT_AND_FETCH = 2 };
struct fuse_uring_ent_in_out { uint64_t flags; uint64_t commit_id; uint32_t payload_sz; uint32_t padding; uint64_t reserved; };
struct fuse_uring_req_header {
    char in_out[FUSE_URING_IN_OUT_HEADER_SZ];
    char op_in[FUSE_URING_OP_IN_OUT_SZ];
    struct fuse_uring_ent_in_out ring_ent_in_out;
};
struct fuse_uring_cmd_req { uint64_t flags; uint64_t commit_id; uint16_t qid; uint8_t padding[6]; };

typedef struct {
    struct fuse_uring_req_header *hdr;
    uint8_t *payload;
    size_t payload_cap;
    struct iovec iov[2];
    uint64_t commit_id;
    int registered;
} uent_t;

typedef struct {
    int qid, ring_fd;
    uint8_t *sq_ring, *cq_ring, *sqes;
    size_t sq_ring_len, cq_ring_len, sqes_len;
    unsigned *sq_head, *sq_tail, *sq_mask, *sq_array, *cq_head, *cq_tail, *cq_mask;
    struct io_uring_cqe *cqes;
    uent_t *ents;
    int nent;
    pthread_mutex_t sq_lk;  /* the SQ is written by every thread of the queue (registers, commits) */
    pthread_mutex_t cq_lk;  /* one thread at a time takes completions */
    int nthreads;
} uqueue_t;

static __thread uent_t *tl_uent;   /* the ring entry the request being handled arrived in */
static __thread uqueue_t *tl_uq;

static atomic_ullong g_uring_reqs;  /* requests answered through the ring */
static int g_uring_offered;         /* the kernel's FUSE_INIT offered it */

/* Each queue is served by several threads (cfg.threads spread over the
 * queues, at least 2 each), all waiting on the same ring: whoever gets a
 * completion handles that request inline -- blocking on its RPCs -- and
 * commits the reply itself. No hand-over between threads per request; the
 * concurrency per queue is its thread count, not one. */
static int uring_setup(uqueue_t *q, int entries) {
    struct io_uring_params pr;
    memset(&pr, 0, sizeof(pr));
    pr.flags = RP_IORING_SETUP_SQE128 | RP_IORING_SETUP_CQE32;
    q->ring_fd = (int)syscall(__NR_io_uring_setup, entries, &pr);
    if (q->ring_fd < 0) return -errno;
    q->sq_ring_len = pr.sq_off.array + pr.sq_entries * sizeof(unsigned);
    q->cq_ring_len = pr.cq_off.cqes + pr.cq_entries * 2 * sizeof(struct io_uring_cqe);  /* CQE32 */
    q->sqes_len = pr.sq_entries * 128;                                                  /* SQE128 */
    q->sq_ring = mmap(NULL, q->sq_ring_len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, q->ring_fd, IORING_OFF_SQ_RING);
    q->cq_ring = mmap(NULL, q->cq_ring_len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, q->ring_fd, IORING_OFF_CQ_RING);
    q->sqes = mmap(NULL, q->sqes_len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, q->ring_fd, IORING_OFF_SQES);
    if (q->sq_ring == MAP_FAILED || q->cq_ring == MAP_FAILED || q->sqes == MAP_FAILED) return -ENOMEM;
    q->sq_head = (unsigned *)(q->sq_ring + pr.sq_off.head);
    q->sq_tail = (unsigned *)(q->sq_ring + pr.sq_off.tail);
    q->sq_mask = (unsigned *)(q->sq_ring + pr.sq_off.ring_mask);
    q->sq_array = (unsigned *)(q->sq_ring + pr.sq_off.array);
    q->cq_head = (unsigned *)(q->cq_ring + pr.cq_off.head);
    q->cq_tail = (unsigned *)(q->cq_ring + pr.cq_off.tail);
    q->cq_mask = (unsigned *)(q->cq_ring + pr.cq_off.ring_mask);
    q->cqes = (struct io_uring_cqe *)(q->cq_ring + pr.cq_off.cqes);
    return 0;
}

/* queue one FUSE ring command for entry i (not yet submitted) */
static void uring_push(uqueue_t *q, int i, uint32_t cmd, uint64_t commit_id) {
    unsigned tail = *q->sq_tail, idx = tail & *q->sq_mask;
    struct io_uring_sqe *sqe = (struct io_uring_sqe *)(q->sqes + (size_t)idx * 128);
    memset(sqe, 0, 128);
    sqe->opcode = RP_IORING_OP_URING_CMD;
    sqe->fd = g_fuse_fd;
    memcpy((uint8_t *)sqe + RP_SQE_CMD_OP_OFF, &cmd, sizeof(cmd));
    sqe->addr = (uint64_t)(uintptr_t)q->ents[i].iov;
    sqe->len = 2;
    sqe->user_data = (uint64_t)i;
    struct fuse_uring_cmd_req *r = (struct fuse_uring_cmd_req *)((uint8_t *)sqe + RP_SQE_CMD_OFF);
    r->qid = (uint16_t)q->qid;
    r->commit_id = commit_id;
    q->sq_array[idx] = idx;
    __atomic_store_n(q->sq_tail, tail + 1, __ATOMIC_RELEASE);
}

/* submit the SQEs queued so far (a worker's commit): no waiting */
static void uring_submit(uqueue_t *q, unsigned n) {
    for (;;) {
        long rc = syscall(__NR_io_uring_enter, q->ring_fd, n, 0, 0, NULL, 0);
        if (rc >= 0 || (errno != EINTR && errno != EAGAIN && errno != EBUSY)) return;
    }
}

static int uring_enter(uqueue_t *q, unsigned to_submit, unsigned min_complete) {
    for (;;) {
        long rc = syscall(__NR_io_uring_enter, q->ring_fd, to_submit, min_complete,
                          min_complete ? IORING_ENTER_GETEVENTS : 0, NULL, 0);
        if (rc >= 0) return (int)rc;
        if (errno == EINTR) continue;
        return -errno;
    }
}

static void reply(uint64_t unique, int err, const void *b1, size_t n1, const void *b2, size_t n2) {
    if (err >= 4096) err = EIO;  /* an internal code must never reach the kernel */
    struct fuse_out_header oh = {.len = (uint32_t)(sizeof(oh) + n1 + n2), .error = err ? -err : 0, .unique = unique};
    if (err) oh.len = sizeof(oh);
    if (tl_uent) {  /* through the ring: reply header + out args into the entry, then commit */
        uent_t *e = tl_uent;
        size_t pn = err ? 0 : n1 + n2;
        if (pn > e->payload_cap) { oh.error = -EIO; oh.len = sizeof(oh); pn = 0; }
        memcpy(e->hdr->in_out, &oh, sizeof(oh));
        if (pn) {
            if (n1) memcpy(e->payload, b1, n1);
            if (n2) memcpy(e->payload + n1, b2, n2);
        }
        e->hdr->ring_ent_in_out.payload_sz = (uint32_t)pn;
        uqueue_t *q = tl_uq;
        pthread_mutex_lock(&q->sq_lk);
        uring_push(q, (int)(e - q->ents), FUSE_IO_URING_CMD_COMMIT_AND_FETCH, e->commit_id);
        uring_submit(q, 1);
        pthread_mutex_unlock(&q->sq_lk);
        tl_uent = NULL;
        atomic_fetch_add(&g_uring_reqs, 1);
        return;
    }
    struct iovec iov[3] = {{&oh, sizeof(oh)}, {(void *)b1, err ? 0 : n1}, {(void *)b2, err ? 0 : n2}};
    ssize_t w = writev(g_fuse_fd, iov, 3);
    (void)w;  /* ENOENT here = the request was interrupted; nothing to do */
}

static void fill_attr(struct fuse_attr *fa, uint64_t ino, const attr_t *a) {
    memset(fa, 0, sizeof(*fa));
    fa->ino = ino;
    if (a->is_dir) {
        fa->mode = S_IFDIR | 0755;
        fa->nlink = 2;
    } else {
        fa->mode = S_IFREG | 0644;
        fa->nlink = 1;
        fa->size = a->size;
        fa->blocks = (a->size + 511) / 512;
    }
    fa->mtime = fa->atime = (uint64_t)a->mtime;
    fa->ctime = (uint64_t)a->ctime;
    fa->blksize = 4096;
}

static void reply_entry(uint64_t unique, const char *path, const attr_t *a, const void *extra, size_t nextra) {
    struct fuse_entry_out eo;
    memset(&eo, 0, sizeof(eo));
    eo.nodeid = ino_get(path, 1);
    eo.generation = 1;
    eo.entry_valid = eo.attr_valid = (uint64_t)cfg.kernel_cache_s;
    fill_attr(&eo.attr, eo.nodeid, a);
    reply(unique, 0, &eo, sizeof(eo), extra, nextra);
}

static void reply_attr(uint64_t unique, uint64_t ino, const attr_t *a) {
    struct fuse_attr_out ao;
    memset(&ao, 0, sizeof(ao));
    ao.attr_valid = (uint64_t)cfg.kernel_cache_s;
    fill_attr(&ao.attr, ino, a);
    reply(unique, 0, &ao, sizeof(ao), NULL, 0);
}

static void op_init(uint64_t u, const void *in) {
    const struct fuse_init_in *ii = in;
    struct fuse_init_out o;
    memset(&o, 0, sizeof(o));
    o.major = FUSE_KERNEL_VERSION;
    o.minor = ii->minor < FUSE_KERNEL_MINOR_VERSION ? ii->minor : FUSE_KERNEL_MINOR_VERSION;
    o.max_readahead = ii->max_readahead;
    uint32_t want = FUSE_ASYNC_READ | FUSE_BIG_WRITES | FUSE_MAX_PAGES | FUSE_PARALLEL_DIROPS;
    o.flags = ii->flags & want;
    uint32_t in_flags2 = 0;
    if (ii->flags & RP_FUSE_INIT_EXT)  /* only then did the kernel send the extended (64-byte) init_in */
        memcpy(&in_flags2, (const uint8_t *)in + RP_INIT_IN_FLAGS2_OFF, sizeof(in_flags2));
    if ((ii->flags & RP_FUSE_INIT_EXT) && (in_flags2 & FUSE_OVER_IO_URING_FLAG2) && cfg.uring) {
        uint32_t f2 = FUSE_OVER_IO_URING_FLAG2;
        o.flags |= RP_FUSE_INIT_EXT;
        memcpy((uint8_t *)&o + RP_INIT_OUT_FLAGS2_OFF, &f2, sizeof(f2));
        g_uring_offered = 1;
    }
    o.max_background = 64;
    o.congestion_threshold = 48;
    o.max_write = (o.flags & FUSE_MAX_PAGES) ? FUSE_MAX_WRITE : 131072;
    o.time_gran = 1;
    o.max_pages = (o.flags & FUSE_MAX_PAGES) ? (uint16_t)(FUSE_MAX_WRITE / 4096) : 0;
    reply(u, 0, &o, sizeof(o), NULL, 0);
}

static void op_lookup(uint64_t u, uint64_t node, const char *name) {
    char path[4096];
    attr_t a;
    int rc = child_path(node, name, path, sizeof(path));
    if (!rc) rc = fs_getattr(path, &a);
    if (rc) { reply(u, -rc, NULL, 0, NULL, 0); return; }
    reply_entry(u, path, &a, NULL, 0);
}

/* FUSE_NOTIFY_INVAL_INODE with off -1: the kernel forgets the inode's
 * cached attributes (its page cache is left alone). Goes through /dev/fuse
 * whatever channel the requests use (the ring carries no notifications). */
static void kernel_inval_attr(uint64_t ino) {
    struct fuse_out_header oh = {.len = sizeof(oh) + sizeof(struct fuse_notify_inval_inode_out),
                                 .error = FUSE_NOTIFY_INVAL_INODE, .unique = 0};
    struct fuse_notify_inval_inode_out no = {.ino = ino, .off = -1, .len = 0};
    struct iovec iov[2] = {{&oh, sizeof(oh)}, {&no, sizeof(no)}};
    ssize_t w = writev(g_fuse_fd, iov, 2);
    (void)w;  /* ENOENT: the kernel no longer has the inode; nothing to invalidate */
}

static void op_getattr(uint64_t u, uint64_t node) {
    char path[4096];
    attr_t a;
    int rc = path_for_ino(node, path, sizeof(path));
    if (!rc) rc = fs_getattr(path, &a);
    if (rc) { reply(u, -rc, NULL, 0, NULL, 0); return; }
    reply_attr(u, node, &a);
}

static void op_setattr(uint64_t u, uint64_t node, const struct fuse_setattr_in *si) {
    char path[4096];
    attr_t a;
    int rc = path_for_ino(node, path, sizeof(path));
    if (!rc && (si->valid & FATTR_SIZE)) {
        if (si->valid & FATTR_FH) {  /* ftruncate: this fd's pending writes go first */
            handle_t *h = (handle_t *)(uintptr_t)si->fh;
            pthread_mutex_lock(&h->lk);
            rc = handle_flush(h);
            pthread_mutex_unlock(&h->lk);
        }
        if (!rc) rc = fs_truncate(path, si->size);
        if (!rc) invalidate_open_caches(path);  /* no fd may serve pre-truncate bytes */
    }
    if (!rc) rc = fs_getattr(path, &a);
    if (rc) { reply(u, -rc, NULL, 0, NULL, 0); return; }
    reply_attr(u, node, &a);
}

static void op_mkdir(uint64_t u, uint64_t node, const char *name) {
    char path[4096];
    attr_t a;
    int rc = child_path(node, name, path, sizeof(path));
    if (!rc) rc = fs_mkdir(path, &a);
    if (rc) { reply(u, -rc, NULL, 0, NULL, 0); return; }
    reply_entry(u, path, &a, NULL, 0);
}

static void op_rmdir(uint64_t u, uint64_t node, const char *name) {
    char path[4096];
    int rc = child_path(node, name, path, sizeof(path));
    if (!rc) rc = fs_rmdir(path);
    reply(u, -rc, NULL, 0, NULL, 0);
}

static void op_unlink(uint64_t u, uint64_t node, const char *name) {
    char path[4096];
    int rc = child_path(node, name, path, sizeof(path));
    if (!rc) rc = fs_remove_file(path);
    reply(u, -rc, NULL, 0, NULL, 0);
}

static void op_opendir(uint64_t u, uint64_t node) {
    char path[4096];
    attr_t a;
    int rc = path_for_ino(node, path, sizeof(path));
    if (!rc) rc = fs_getattr(path, &a);
    if (!rc && !a.is_dir) rc = -ENOTDIR;
    if (rc) { reply(u, -rc, NULL, 0, NULL, 0); return; }
    struct fuse_open_out oo = {.fh = (uint64_t)(uintptr_t)handle_new(path, 1)};
    reply(u, 0, &oo, sizeof(oo), NULL, 0);
}

static void op_readdir(uint64_t u, const struct fuse_read_in *ri) {
    handle_t *h = (handle_t *)(uintptr_t)ri->fh;
    pthread_mutex_lock(&h->lk);
    int rc = 0;
    if (!h->listed) {
        rc = mdt_readdir(h->path, &h->dents, &h->ndents);
        if (rc == 0) {
            h->listed = 1;
            for (size_t i = 0; i < h->ndents; i++) {  /* READDIRPLUS idea: prime the attr cache */
                char child[4096];
                int w = *h->path ? snprintf(child, sizeof(child), "%s/%s", h->path, h->dents[i].name)
                                 : snprintf(child, sizeof(child), "%s", h->dents[i].name);
                if (w > 0 && (size_t)w < sizeof(child)) ac_put(child, &h->dents[i].a);
            }
        }
    }
    if (rc) {
        pthread_mutex_unlock(&h->lk);
        reply(u, -rc, NULL, 0, NULL, 0);
        return;
    }
    size_t cap = ri->size, used = 0;
    uint8_t *out = xmalloc(cap);
    uint64_t self_ino = *h->path ? ino_peek(h->path) : FUSE_ROOT_ID;
    for (uint64_t i = ri->offset; i < h->ndents + 2; i++) {
        const char *name;
        uint64_t ino;
        uint32_t type;
        char child[4096];
        if (i == 0 || i == 1) {
            name = i == 0 ? "." : "..";
            ino = self_ino;
            type = S_IFDIR >> 12;
        } else {
            dirent_t *d = &h->dents[i - 2];
            name = d->name;
            int w = *h->path ? snprintf(child, sizeof(child), "%s/%s", h->path, name)
                             : snprintf(child, sizeof(child), "%s", name);
            if (w < 0 || (size_t)w >= sizeof(child)) continue;
            ino = ino_peek(child);
            type = (d->a.is_dir ? S_IFDIR : S_IFREG) >> 12;
        }
        size_t nl = strlen(name), rec = FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + nl);
        if (used + rec > cap) break;
        struct fuse_dirent *de = (struct fuse_dirent *)(out + used);
        memset(de, 0, rec);
        de->ino = ino;
        de->off = i + 1;
        de->namelen = (uint32_t)nl;
        de->type = type;
        memcpy(de->name, name, nl);
        used += rec;
    }
    pthread_mutex_unlock(&h->lk);
    reply(u, 0, out, used, NULL, 0);
    free(out);
}

static void op_open(uint64_t u, uint64_t node) {
    char path[4096];
    attr_t a;
    int rc = path_for_ino(node, path, sizeof(path));
    if (!rc) rc = fs_getattr(path, &a);
    /* Close-to-open consistency for striped files (NFS's rule): an open
     * revalidates the attributes, so a file another node extended and closed
     * is read at its real size, not the size this node cached within the
     * TTL (IOR's shared file across nodes: "read returned EOF prematurely").
     * Inline files need none of this -- their reads carry the fresh entry. */
    if (!rc && !a.is_dir && !a.inl) {
        uint64_t cached = a.size;
        ac_del(path);
        rc = fs_getattr(path, &a);
        /* the kernel has the stale attributes too (from the path walk's
         * LOOKUP, valid kernel_cache_s): drop them, so the fstat/read that
         * follows this open asks again instead of stopping at the old size.
         * Synchronous and safe here: OPEN holds no inode lock. */
        if (!rc && a.size != cached) kernel_inval_attr(node);
    }
    if (!rc && a.is_dir) rc = -EISDIR;
    if (rc) { reply(u, -rc, NULL, 0, NULL, 0); return; }
    handle_t *h = handle_new(path, 0);
    h->sc = sc_new(path, &a);
    open_register(h);
    struct fuse_open_out oo = {.fh = (uint64_t)(uintptr_t)h};
    reply(u, 0, &oo, sizeof(oo), NULL, 0);
}

static void op_create(uint64_t u, uint64_t node, const char *name) {
    char path[4096];
    attr_t a;
    int rc = child_path(node, name, path, sizeof(path));
    if (!rc) rc = fs_create_file(path, &a);
    if (rc) { reply(u, -rc, NULL, 0, NULL, 0); return; }
    handle_t *h = handle_new(path, 0);
    h->sc = sc_new(path, &a);
    open_register(h);
    struct fuse_open_out oo = {.fh = (uint64_t)(uintptr_t)h};
    reply_entry(u, path, &a, &oo, sizeof(oo));
}

static void op_read(uint64_t u, const struct fuse_read_in *ri) {
    handle_t *h = (handle_t *)(uintptr_t)ri->fh;
    pthread_mutex_lock(&h->lk);
    int rc = handle_flush(h);  /* same-fd read-after-write */
    attr_t a;
    if (!rc) rc = fs_getattr(h->path, &a);
    if (!rc && a.is_dir) rc = -EISDIR;
    if (rc) { pthread_mutex_unlock(&h->lk); reply(u, -rc, NULL, 0, NULL, 0); return; }
    uint64_t off = ri->offset;
    size_t len = ri->size;
    if (a.inl) {  /* Data-on-MDT: one MDT round trip, which also says if it is still inline */
        resp_t r;
        const uint8_t *d;
        size_t n;
        rc = mdt_inline_read(h->path, 0, off, (uint32_t)len, &a, &r, &d, &n);
        if (rc == 0 && a.inl) {
            ac_put(h->path, &a);
            pthread_mutex_unlock(&h->lk);
            reply(u, 0, d, n, NULL, 0);
            resp_free(&r);
            return;
        }
        if (rc == 0) { resp_free(&r); ac_put(h->path, &a); }  /* spilled meanwhile: read the stripes */
        if (rc) { pthread_mutex_unlock(&h->lk); reply(u, -rc, NULL, 0, NULL, 0); return; }
    }
    if (off >= a.size) len = 0;
    else if (off + len > a.size) len = (size_t)(a.size - off);
    uint8_t *out = NULL;
    if (len && a.has_layout && a.ss && off != h->next_off && len < a.ss && off / a.ss == (off + len - 1) / a.ss) {
        /* random read inside one stripe (ior-hard): READ_AT, answered to
         * the kernel straight from the reply buffer the OST wrote -- no copy */
        static const uint8_t zeros[FUSE_MAX_WRITE];
        uint64_t idx = off / a.ss;
        char key[4200];
        stripe_key(key, sizeof(key), h->path, idx);
        rp_resp_t r;
        uint64_t ver;
        size_t got = 0;
        rc = rp_ost_read_at(&g_osts[ost_for_stripe(&a, idx)], key, off - idx * a.ss, (uint32_t)len, 0, &ver, &r);
        h->next_off = off + len;
        pthread_mutex_unlock(&h->lk);
        if (rc == 0) {
            got = r.vlen - 8 < len ? r.vlen - 8 : len;
            reply(u, 0, r.val + 8, got, zeros, len - got);
            rp_resp_free(&r);
        } else if (rc == -ENOENT) {
            reply(u, 0, zeros, len, NULL, 0);  /* a hole */
        } else {
            reply(u, -rc, NULL, 0, NULL, 0);
        }
        return;
    }
    if (len) {
        out = xmalloc(len);
        if (!a.has_layout || !a.ss) rc = -EIO;
        else if (off != h->next_off && len < a.ss) {
            rc = fs_read_direct(h->path, &a, off, len, out);  /* random/strided: exact bytes only */
        } else {  /* sequential: whole-stripe cache + readahead */
            pthread_mutex_lock(&h->sc->lk);
            h->sc->a = a;  /* size/layout snapshot for readahead */
            pthread_mutex_unlock(&h->sc->lk);
            rc = sc_read(h->sc, &a, off, len, out);
        }
        h->next_off = off + len;
    }
    pthread_mutex_unlock(&h->lk);
    if (rc) reply(u, -rc, NULL, 0, NULL, 0);
    else reply(u, 0, out, len, NULL, 0);
    free(out);
}

/* The WRITE's bytes stay in the kernel request buffer `base` (arena memory,
 * see fuse_worker), which this handle now owns: no copy here, none at the
 * flush, none in the transport -- the OST reads them from this buffer. */
static void op_write(uint64_t u, const struct fuse_write_in *wi, const uint8_t *data, uint8_t *base) {
    handle_t *h = (handle_t *)(uintptr_t)wi->fh;
    pthread_mutex_lock(&h->lk);
    int rc = handle_write(h, wi->offset, data, wi->size, base);
    pthread_mutex_unlock(&h->lk);
    if (rc) { reply(u, -rc, NULL, 0, NULL, 0); return; }
    struct fuse_write_out wo = {.size = wi->size};
    reply(u, 0, &wo, sizeof(wo), NULL, 0);
}

static void op_flush(uint64_t u, const struct fuse_flush_in *fi) {
    handle_t *h = (handle_t *)(uintptr_t)fi->fh;
    pthread_mutex_lock(&h->lk);
    int rc = handle_flush(h);
    pthread_mutex_unlock(&h->lk);
    reply(u, -rc, NULL, 0, NULL, 0);
}

static void op_release(uint64_t u, const struct fuse_release_in *ri) {
    handle_t *h = (handle_t *)(uintptr_t)ri->fh;
    int rc = 0;
    if (!h->is_dir) {
        pthread_mutex_lock(&h->lk);
        rc = handle_flush(h);
        pthread_mutex_unlock(&h->lk);
    }
    handle_free(h);
    reply(u, -rc, NULL, 0, NULL, 0);
}

static void op_statfs(uint64_t u) {
    struct fuse_statfs_out so;
    memset(&so, 0, sizeof(so));
    so.st.blocks = so.st.bfree = so.st.bavail = so.st.files = so.st.ffree = 1u << 20;
    so.st.bsize = so.st.frsize = 4096;
    so.st.namelen = 255;
    reply(u, 0, &so, sizeof(so), NULL, 0);
}

/* Per-opcode request counts and handling time, dumped as JSON on SIGUSR1
 * to $ROCEPOOL_FUSE_STATS (default /tmp/rocepool-fuse-<pid>.stats.json)
 * for bottleneck analysis (tools/procstat.py, tools/io500_run.py). */
static atomic_ullong g_opcnt[64], g_opns[64], g_stats_seq;

static void *stats_thread(void *arg) {
    (void)arg;
    pthread_setname_np(pthread_self(), "fuse-stats");
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    char path[512], tmp[540];
    const char *e = getenv("ROCEPOOL_FUSE_STATS");
    if (e && *e) snprintf(path, sizeof(path), "%s", e);
    else snprintf(path, sizeof(path), "/tmp/rocepool-fuse-%d.stats.json", (int)getpid());
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    for (;;) {
        int sig;
        if (sigwait(&set, &sig)) continue;
        FILE *f = fopen(tmp, "w");
        if (!f) continue;
        fprintf(f, "{\"seq\": %llu, \"mount\": \"%s\", \"ops\": {",
                (unsigned long long)atomic_fetch_add(&g_stats_seq, 1) + 1, cfg.mount);
        int first = 1;
        for (int i = 0; i < 64; i++) {
            unsigned long long c = atomic_load(&g_opcnt[i]);
            if (!c) continue;
            fprintf(f, "%s\"%d\": [%llu, %llu]", first ? "" : ", ", i, c, (unsigned long long)atomic_load(&g_opns[i]));
            first = 0;
        }
        unsigned long long mr = 0, orr = 0;
        for (int i = 0; i < cfg.nmdt; i++) mr += atomic_load(&g_mdts[i].rpcs);
        for (int i = 0; i < cfg.nost; i++) orr += atomic_load(&g_osts[i].rpcs);
        uint64_t ab, au, zc, cp, dr, db, dl, ds, lk, lm;
        rp_bulk_stats(&ab, &au, &zc, &cp);
        rp_direct_stats(&dr, &db, &dl, &ds);
        rp_lookup_stats(&lk, &lm);
        fprintf(f, "}, \"mdt_rpcs\": %llu, \"ost_rpcs\": %llu, \"arena_bytes\": %llu, \"arena_in_use\": %llu, "
                "\"body_bytes_zero_copy\": %llu, \"body_bytes_copied\": %llu, \"uring_reqs\": %llu, "
                "\"direct_reads\": %llu, \"direct_bytes\": %llu, \"leases\": %llu, \"stale_leases\": %llu, "
                "\"direct_lookups\": %llu, \"direct_lookup_misses\": %llu}\n", mr, orr,
                (unsigned long long)ab, (unsigned long long)au, (unsigned long long)zc, (unsigned long long)cp,
                (unsigned long long)atomic_load(&g_uring_reqs), (unsigned long long)dr, (unsigned long long)db,
                (unsigned long long)dl, (unsigned long long)ds, (unsigned long long)lk, (unsigned long long)lm);
        fclose(f);
        rename(tmp, path);
    }
    return NULL;
}

static int dispatch_one(uint8_t *buf, size_t n);
static int uring_start(void);
static int dispatch_parts(const struct fuse_in_header *ih, const uint8_t *in, size_t n_in, const uint8_t *wdata,
                          uint8_t *base);

/* returns 1 when the request buffer was taken over (a WRITE keeps it) */
static int dispatch(uint8_t *buf, size_t n) {
    if (n < sizeof(struct fuse_in_header)) return 0;
    uint32_t opc = ((const struct fuse_in_header *)buf)->opcode;
    struct timespec a, b;
    clock_gettime(CLOCK_MONOTONIC, &a);
    int taken = dispatch_one(buf, n);
    clock_gettime(CLOCK_MONOTONIC, &b);
    opc = opc < 63 ? opc : 63;
    atomic_fetch_add(&g_opcnt[opc], 1);
    atomic_fetch_add(&g_opns[opc], (unsigned long long)((b.tv_sec - a.tv_sec) * 1000000000ll + (b.tv_nsec - a.tv_nsec)));
    return taken;
}

static int dispatch_one(uint8_t *buf, size_t n) {
    return dispatch_parts((const struct fuse_in_header *)buf, buf + sizeof(struct fuse_in_header),
                          n - sizeof(struct fuse_in_header), NULL, buf);
}

/* in: the op header followed by the rest of the request (classic layout);
 * wdata: a WRITE's data when it lives elsewhere (ring payload); base: the
 * arena buffer a WRITE may keep, NULL = the data must be copied */
static int dispatch_parts(const struct fuse_in_header *ih, const uint8_t *in, size_t n_in, const uint8_t *wdata,
                          uint8_t *base) {
    size_t n = n_in + sizeof(*ih);
    uint64_t u = ih->unique, node = ih->nodeid;
    switch (ih->opcode) {
    case FUSE_INIT:
        op_init(u, in);
        if (g_uring_offered) uring_start();  /* the INIT reply is out: the kernel accepts registrations now */
        break;
    case FUSE_DESTROY: reply(u, 0, NULL, 0, NULL, 0); atomic_store(&g_stop, 1); break;
    case FUSE_FORGET:  /* no reply */
        ino_forget(node, ((const struct fuse_forget_in *)in)->nlookup);
        break;
    case FUSE_BATCH_FORGET: {  /* no reply */
        const struct fuse_batch_forget_in *bf = (const void *)in;
        const struct fuse_forget_one *one = (const void *)(bf + 1);
        size_t room = (n - sizeof(*ih) - sizeof(*bf)) / sizeof(*one);
        for (uint32_t i = 0; i < bf->count && i < room; i++) ino_forget(one[i].nodeid, one[i].nlookup);
        break;
    }
    case FUSE_LOOKUP: op_lookup(u, node, (const char *)in); break;
    case FUSE_GETATTR: op_getattr(u, node); break;
    case FUSE_SETATTR: op_setattr(u, node, (const void *)in); break;
    case FUSE_MKDIR: op_mkdir(u, node, (const char *)in + sizeof(struct fuse_mkdir_in)); break;
    case FUSE_RMDIR: op_rmdir(u, node, (const char *)in); break;
    case FUSE_UNLINK: op_unlink(u, node, (const char *)in); break;
    case FUSE_OPENDIR: op_opendir(u, node); break;
    case FUSE_READDIR: op_readdir(u, (const void *)in); break;
    case FUSE_RELEASEDIR: op_release(u, (const void *)in); break;
    case FUSE_OPEN: op_open(u, node); break;
    case FUSE_CREATE: op_create(u, node, (const char *)in + sizeof(struct fuse_create_in)); break;
    case FUSE_READ: op_read(u, (const void *)in); break;
    case FUSE_WRITE: op_write(u, (const void *)in, wdata ? wdata : in + sizeof(struct fuse_write_in), base); return base != NULL;
    case FUSE_FLUSH: op_flush(u, (const void *)in); break;
    case FUSE_RELEASE: op_release(u, (const void *)in); break;
    case FUSE_STATFS: op_statfs(u); break;
    case FUSE_ACCESS: reply(u, 0, NULL, 0, NULL, 0); break;
    case FUSE_FSYNC: {  /* make this fd's buffered writes durable on the OSTs */
        handle_t *h = (handle_t *)(uintptr_t)((const struct fuse_fsync_in *)in)->fh;
        int rc = 0;
        if (h && !h->is_dir) {
            pthread_mutex_lock(&h->lk);
            rc = handle_flush(h);
            pthread_mutex_unlock(&h->lk);
        }
        if (!rc) rc = rpfs_commit();  /* and the namespace changes (ROCEPOOL_MDT_ASYNC) */
        reply(u, -rc, NULL, 0, NULL, 0);
        break;
    }
    case FUSE_FSYNCDIR: reply(u, 0, NULL, 0, NULL, 0); break;
    case FUSE_INTERRUPT: break;  /* requests are short; let them finish */
    default: reply(u, ENOSYS, NULL, 0, NULL, 0); break;
    }
    return 0;
}

static void *fuse_worker(void *arg) {
    pthread_setname_np(pthread_self(), "fuse-worker");
    (void)arg;
    /* the request buffer is registered arena memory: a WRITE's data is
     * sent to the OSTs from right here, so a WRITE keeps the buffer and the
     * worker takes a fresh one (rp_bulk_alloc is a free-list pop) */
    uint8_t *buf = rp_bulk_alloc(FUSE_BUF_SIZE);
    if (!buf) buf = xmalloc(FUSE_BUF_SIZE);
    while (!atomic_load(&g_stop)) {
        ssize_t n = read(g_fuse_fd, buf, FUSE_BUF_SIZE);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            if (errno == ENOENT) continue;  /* request interrupted before we read it */
            break;                          /* ENODEV: unmounted */
        }
        if (n == 0) break;
        if (dispatch(buf, (size_t)n)) {
            buf = rp_bulk_alloc(FUSE_BUF_SIZE);
            if (!buf) buf = xmalloc(FUSE_BUF_SIZE);
        }
    }
    rp_bulk_free(buf);
    atomic_store(&g_stop, 1);
    return NULL;
}

/* bytes of the per-op input header the kernel puts in op_in (its
 * in_args[0]); ops whose first argument is a name have none there */
static size_t uring_op_header_size(uint32_t opcode) {
    switch (opcode) {
    case FUSE_GETATTR: return sizeof(struct fuse_getattr_in);
    case FUSE_SETATTR: return sizeof(struct fuse_setattr_in);
    case FUSE_MKDIR: return sizeof(struct fuse_mkdir_in);
    case FUSE_OPEN: case FUSE_OPENDIR: return sizeof(struct fuse_open_in);
    case FUSE_READ: case FUSE_READDIR: return sizeof(struct fuse_read_in);
    case FUSE_RELEASE: case FUSE_RELEASEDIR: return sizeof(struct fuse_release_in);
    case FUSE_CREATE: return sizeof(struct fuse_create_in);
    case FUSE_WRITE: return sizeof(struct fuse_write_in);
    case FUSE_FLUSH: return sizeof(struct fuse_flush_in);
    case FUSE_FSYNC: case FUSE_FSYNCDIR: return sizeof(struct fuse_fsync_in);
    case FUSE_ACCESS: return sizeof(struct fuse_access_in);
    case FUSE_INTERRUPT: return sizeof(struct fuse_interrupt_in);
    case FUSE_FORGET: return sizeof(struct fuse_forget_in);
    case FUSE_BATCH_FORGET: return sizeof(struct fuse_batch_forget_in);
    default: return 0;  /* LOOKUP, UNLINK, RMDIR, STATFS, ... */
    }
}

/* handle the request that arrived in entry `i` of queue `q` (a ring worker) */
static void ring_handle(uqueue_t *q, int i) {
    uent_t *e = &q->ents[i];
    const struct fuse_in_header *ih = (const struct fuse_in_header *)e->hdr->in_out;
    struct timespec ta, tb;
    clock_gettime(CLOCK_MONOTONIC, &ta);
    e->commit_id = e->hdr->ring_ent_in_out.commit_id;
    uint32_t psz = e->hdr->ring_ent_in_out.payload_sz;
    if (psz > e->payload_cap) psz = (uint32_t)e->payload_cap;
    tl_uq = q;
    tl_uent = e;
    if (ih->opcode == FUSE_WRITE) {
        dispatch_parts(ih, (const uint8_t *)e->hdr->op_in, sizeof(e->hdr->op_in) + psz, e->payload, NULL);
    } else {
        /* the classic layout for the handlers: the op header (op_in, as
         * many bytes as this opcode's header has -- none for LOOKUP/UNLINK/
         * RMDIR, whose name is the payload), then the payload (names) */
        uint8_t in[FUSE_URING_OP_IN_OUT_SZ + 4096];
        size_t hs = uring_op_header_size(ih->opcode);
        memcpy(in, e->hdr->op_in, hs);
        size_t take = psz < 4096 ? psz : 4096;
        memcpy(in + hs, e->payload, take);
        dispatch_parts(ih, in, hs + take, NULL, NULL);
    }
    if (tl_uent) reply(ih->unique, 0, NULL, 0, NULL, 0);  /* a no-reply op: re-arm the entry anyway */
    clock_gettime(CLOCK_MONOTONIC, &tb);
    uint32_t opc = ih->opcode < 63 ? ih->opcode : 63;  /* the same per-opcode counters as the classic path */
    atomic_fetch_add(&g_opcnt[opc], 1);
    atomic_fetch_add(&g_opns[opc], (unsigned long long)((tb.tv_sec - ta.tv_sec) * 1000000000ll + (tb.tv_nsec - ta.tv_nsec)));
}

/* a queue thread: (the first one registers the entries, then) waits for
 * completions and handles the requests inline */
typedef struct { uqueue_t *q; int first; } uthread_arg_t;

static void *uring_queue_thread(void *arg) {
    uthread_arg_t *ta = arg;
    uqueue_t *q = ta->q;
    int first = ta->first;
    free(ta);
    char name[16];
    snprintf(name, sizeof(name), "fuse-uring%d", q->qid);
    pthread_setname_np(pthread_self(), name);
    if (first) {
        pthread_mutex_lock(&q->sq_lk);
        for (int i = 0; i < q->nent; i++) uring_push(q, i, FUSE_IO_URING_CMD_REGISTER, 0);
        uring_submit(q, (unsigned)q->nent);
        pthread_mutex_unlock(&q->sq_lk);
    }
    while (!atomic_load(&g_stop)) {
        int rc = uring_enter(q, 0, 1);
        if (rc < 0) { if (rc == -EAGAIN || rc == -EBUSY || rc == -EINTR) continue; break; }
        for (;;) {
            pthread_mutex_lock(&q->cq_lk);
            unsigned head = *q->cq_head, tail = __atomic_load_n(q->cq_tail, __ATOMIC_ACQUIRE);
            if (head == tail) { pthread_mutex_unlock(&q->cq_lk); break; }
            struct io_uring_cqe *cqe = (struct io_uring_cqe *)((uint8_t *)q->cqes + (size_t)(head & *q->cq_mask) * 32);
            int i = (int)cqe->user_data, res = cqe->res;
            __atomic_store_n(q->cq_head, head + 1, __ATOMIC_RELEASE);
            pthread_mutex_unlock(&q->cq_lk);
            uent_t *e = &q->ents[i];
            if (res < 0) {
                if (!e->registered && res == -EAGAIN) {  /* the INIT reply is not processed yet: again */
                    usleep(1000);
                    pthread_mutex_lock(&q->sq_lk);
                    uring_push(q, i, FUSE_IO_URING_CMD_REGISTER, 0);
                    uring_submit(q, 1);
                    pthread_mutex_unlock(&q->sq_lk);
                    continue;
                }
                if (res == -ENOTCONN || res == -ECANCELED || res == -ECONNABORTED || atomic_load(&g_stop)) {
                    atomic_store(&g_stop, 1);  /* unmounted / aborted: going away */
                    continue;
                }
                if (!e->registered) {  /* the kernel refused the ring: stay on /dev/fuse */
                    fprintf(stderr, "rocepool-fuse: io_uring register (queue %d): %s; FUSE over io_uring off\n",
                            q->qid, strerror(-res));
                    return NULL;
                }
                continue;
            }
            e->registered = 1;
            ring_handle(q, i);
        }
    }
    return NULL;
}

static int possible_cpus(void) {
    FILE *f = fopen("/sys/devices/system/cpu/possible", "r");
    int lo = 0, hi = 0, n = (int)sysconf(_SC_NPROCESSORS_CONF);
    if (f) {
        if (fscanf(f, "%d-%d", &lo, &hi) == 2) n = hi - lo + 1;
        else if (lo == 0) n = 1;
        fclose(f);
    }
    return n > 0 ? n : 1;
}

/* after FUSE_INIT: one queue per possible CPU, entries in arena memory */
static int uring_start(void) {
    int nq = possible_cpus();
    uqueue_t *qs = calloc((size_t)nq, sizeof(*qs));
    for (int qi = 0; qi < nq; qi++) {
        uqueue_t *q = &qs[qi];
        q->qid = qi;
        q->nent = cfg.uring_entries;
        int rc = uring_setup(q, q->nent * 2);
        if (rc) { fprintf(stderr, "rocepool-fuse: io_uring_setup: %s (FUSE over io_uring off)\n", strerror(-rc)); return rc; }
        q->ents = calloc((size_t)q->nent, sizeof(uent_t));
        for (int i = 0; i < q->nent; i++) {
            uent_t *e = &q->ents[i];
            e->hdr = calloc(1, sizeof(*e->hdr));
            e->payload_cap = FUSE_MAX_WRITE;
            e->payload = rp_bulk_alloc(e->payload_cap);
            if (!e->payload) e->payload = xmalloc(e->payload_cap);
            e->iov[0] = (struct iovec){ e->hdr, sizeof(*e->hdr) };
            e->iov[1] = (struct iovec){ e->payload, e->payload_cap };
        }
    }
    int per_q = cfg.threads / nq;
    if (per_q < 2) per_q = 2;
    if (per_q > cfg.uring_entries) per_q = cfg.uring_entries;
    for (int qi = 0; qi < nq; qi++) {
        pthread_mutex_init(&qs[qi].sq_lk, NULL);
        pthread_mutex_init(&qs[qi].cq_lk, NULL);
        qs[qi].nthreads = per_q;
        for (int t = 0; t < per_q; t++) {
            uthread_arg_t *ta = xmalloc(sizeof(*ta));
            *ta = (uthread_arg_t){ &qs[qi], t == 0 };
            pthread_t th;
            pthread_attr_t at;
            pthread_attr_init(&at);
            pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
            if (pthread_create(&th, &at, uring_queue_thread, ta)) die("pthread_create failed");
            pthread_attr_destroy(&at);
        }
    }
    fprintf(stderr, "rocepool-fuse: FUSE over io_uring: %d queues x %d entries, %d threads per queue\n", nq,
            cfg.uring_entries, per_q);
    return 0;
}

static void on_signal(int sig) {
    (void)sig;
    if (cfg.mount) umount2(cfg.mount, MNT_DETACH);
    _exit(0);
}

static int split_list(char *s, char **out, int max) {
    int n = 0;
    for (char *tok = strtok(s, ","); tok && n < max; tok = strtok(NULL, ",")) out[n++] = tok;
    return n;
}

int main(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        const char *k = argv[i];
        const char *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!v) die("missing value for %s", k);
        if (!strcmp(k, "--mount")) cfg.mount = xstrdup(v);
        else if (!strcmp(k, "--mdts")) cfg.nmdt = split_list(xstrdup(v), cfg.mdt, MAX_PEERS);
        else if (!strcmp(k, "--osts")) cfg.nost = split_list(xstrdup(v), cfg.ost, MAX_PEERS);
        else if (!strcmp(k, "--attr-ttl")) cfg.attr_ttl = atof(v);
        else if (!strcmp(k, "--threads")) cfg.threads = atoi(v);
        else if (!strcmp(k, "--io-threads")) cfg.io_threads = atoi(v);
        else if (!strcmp(k, "--stripe-count")) cfg.stripe_count = atoi(v);
        else if (!strcmp(k, "--stripe-size")) cfg.stripe_size = strtoull(v, NULL, 0);
        else if (!strcmp(k, "--kernel-cache")) cfg.kernel_cache_s = atoi(v);
        else if (!strcmp(k, "--readahead")) cfg.readahead = atoi(v);
        else if (!strcmp(k, "--uring")) cfg.uring = atoi(v);
        else if (!strcmp(k, "--uring-entries")) cfg.uring_entries = atoi(v);
        else die("unknown option %s", k);
        i++;
    }
    const char *ue = getenv("ROCEPOOL_FUSE_URING");
    if (ue && *ue) cfg.uring = atoi(ue);
    if (cfg.uring_entries < 1) cfg.uring_entries = 1;
    if (!cfg.mount || cfg.nmdt < 1 || cfg.nost < 1) die("usage: rocepool-fuse --mount DIR --mdts a:p,... --osts a:p,...");
    sigset_t usr1;  /* blocked before any thread exists: only stats_thread takes SIGUSR1 */
    sigemptyset(&usr1);
    sigaddset(&usr1, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &usr1, NULL);
    int irc = rpfs_init();  /* environment, peers, the I/O thread pool */
    if (irc) die("init: %s", strerror(-irc));

    mkdir(cfg.mount, 0755);
    g_fuse_fd = open("/dev/fuse", O_RDWR | O_CLOEXEC);
    if (g_fuse_fd < 0) die("open /dev/fuse: %s", strerror(errno));
    char opts[256];
    snprintf(opts, sizeof(opts), "fd=%d,rootmode=40755,user_id=0,group_id=0,allow_other,max_read=%u",
             g_fuse_fd, FUSE_MAX_WRITE);
    if (mount("rocepool", cfg.mount, "fuse", MS_NOSUID | MS_NODEV, opts))
        die("mount(%s): %s", cfg.mount, strerror(errno));
    signal(SIGTERM, on_signal);
    signal(SIGINT, on_signal);
    signal(SIGPIPE, SIG_IGN);

    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    {
        pthread_t st;
        if (pthread_create(&st, &at, stats_thread, NULL)) die("pthread_create failed");
    }
    printf("READY %s\n", cfg.mount);
    fflush(stdout);
    pthread_t *fw = calloc((size_t)cfg.threads, sizeof(pthread_t));
    for (int i = 0; i < cfg.threads; i++)
        if (pthread_create(&fw[i], NULL, fuse_worker, NULL)) die("pthread_create failed");
    for (int i = 0; i < cfg.threads; i++) pthread_join(fw[i], NULL);
    umount2(cfg.mount, MNT_DETACH);
    return 0;
}

