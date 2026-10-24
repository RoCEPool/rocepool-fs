/* rocepool_fs.c -- the native client's file-system core, shared by the
 * kernel FUSE daemon (rocepool_fuse.c) and the interception library
 * (rocepool_intercept.c): MDT/OST operations, the TTL attribute cache,
 * layout, Data-on-MDT, striped reads/writes with the stripe cache,
 * buffered write handles. Same wire formats and layout rules as the Python
 * client (fs/posix_api.py), so files written by one are read by the other.
 * No kernel interface here: the callers map FUSE requests or libc calls
 * onto rpfs_* / fs_* calls.
 */
#define _GNU_SOURCE
#include "rocepool_fs.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#define CAS_MAX_ATTEMPTS 1000
#define DIRTY_CAP_DEFAULT_MIB 16  /* buffered writes per open file before a flush (env ROCEPOOL_DIRTY_CAP_MIB) */
size_t g_dirty_cap = (size_t)DIRTY_CAP_DEFAULT_MIB << 20;

/* ---------------------------------------------------------------- utils */
void die(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
    exit(2);
}

double mono_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

double wall_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

void *xmalloc(size_t n) {
    void *p = malloc(n ? n : 1);
    if (!p) die("out of memory (%zu bytes)", n);
    return p;
}

void *xrealloc(void *old, size_t n) {
    void *p = realloc(old, n ? n : 1);
    if (!p) die("out of memory (%zu bytes)", n);
    return p;
}

char *xstrdup(const char *s) {
    char *p = strdup(s);
    if (!p) die("out of memory");
    return p;
}

/* ---------------------------------------------------------------- config */
rpfs_cfg_t cfg = {.attr_ttl = 3.0, .threads = 16, .io_threads = 32, .stripe_size = 1u << 20, .kernel_cache_s = 1,
                  .direct_read = 1,
         .readahead = -1, .inline_ok = 1, .dir_stripe = 1, .uring = 0, .uring_entries = 8};

/* ---------------------------------------------------------------- RPC / OST
 * The pooled RPC layer and the OST operations live in rocepool_client.c
 * (shared with the KV library, rocepool_kv.c). */
#define rpc rp_rpc
#define ost_put rp_ost_put
#define ost_get rp_ost_get
#define ost_stat_version rp_ost_stat_version
#define ost_delete rp_ost_delete
#define peer_init rp_peer_init
#define OP_MDT RP_MDT
#define OP_MDT_OK RP_MDT_OK
#define OP_ERROR RP_ERROR

peer_t g_mdts[MAX_PEERS], g_osts[MAX_PEERS];

/* ---------------------------------------------------------------- MDT ops
 * fs/mdt_wire.py. The entry owning `path` lives on the MDT chosen by
 * hashing its PARENT (fs/namespace_shard.py). */


static const char *parent_of(const char *path, char *buf, size_t n) {
    const char *slash = strrchr(path, '/');
    size_t len = slash ? (size_t)(slash - path) : 0;
    if (len >= n) len = n - 1;
    memcpy(buf, path, len);
    buf[len] = 0;
    return buf;
}

static peer_t *mdt_pick(const char *s) { return &g_mdts[sha256_u64(s) % (uint64_t)cfg.nmdt]; }

/* the MDT holding path's own entry (placement rule: fs/namespace_shard.py) */
static peer_t *mdt_for(const char *path) {
    if (cfg.dir_stripe) return mdt_pick(path);
    char par[4096];
    return mdt_pick(parent_of(path, par, sizeof(par)));
}

/* the MDTs holding entries inside dir: every one (striped) or the one hash(dir) names */
static int mdts_for_children(const char *dir, peer_t **out) {
    if (!cfg.dir_stripe) { out[0] = mdt_pick(dir); return 1; }
    for (int i = 0; i < cfg.nmdt; i++) out[i] = &g_mdts[i];
    return cfg.nmdt;
}

static size_t decode_entry(const uint8_t *b, size_t n, attr_t *a) {
    if (n < 26) return 0;
    a->is_dir = b[0];
    a->has_layout = b[1] & 1;
    a->inl = (b[1] & 2) != 0;
    a->frozen = (b[1] & 4) != 0;
    memcpy(&a->size, b + 2, 8);
    memcpy(&a->mtime, b + 10, 8);
    memcpy(&a->ctime, b + 18, 8);
    size_t off = 26;
    a->sc = a->so = 0;
    a->ss = 0;
    if (a->has_layout) {
        if (n < 42) return 0;
        memcpy(&a->sc, b + 26, 4);
        memcpy(&a->ss, b + 30, 8);
        memcpy(&a->so, b + 38, 4);
        off = 42;
    }
    return off;
}

/* maps MDT error codes to errno */
static int mdt_errno(const char *code) {
    if (!strcmp(code, "NOT_FOUND")) return -ENOENT;
    if (!strcmp(code, "EXISTS")) return -EEXIST;
    if (!strcmp(code, "IS_DIR")) return -EISDIR;
    if (!strcmp(code, "NOT_DIR")) return -ENOTDIR;
    if (!strcmp(code, "NOT_INLINE")) return E_NOT_INLINE;
    if (!strcmp(code, "INLINE_FULL")) return E_INLINE_FULL;
    if (!strcmp(code, "FROZEN")) return E_FROZEN;
    if (!strcmp(code, "INLINE")) return E_INLINE;
    {   /* anything else is an I/O error to the program: say which, a few times */
        static _Atomic int said;
        if (atomic_fetch_add(&said, 1) < 5) fprintf(stderr, "rocepool: MDT error %s\n", code);
    }
    return -EIO;
}

static int mdt_entry_call(const char *path, const uint8_t *req, size_t rlen, attr_t *a) {
    resp_t r;
    int rc = rpc(mdt_for(path), OP_MDT, path, req, rlen, NULL, 0, &r);
    if (rc) return rc;
    if (r.op == OP_MDT_OK) rc = decode_entry(r.val, r.vlen, a) ? 0 : -EIO;
    else rc = r.op == OP_ERROR ? mdt_errno(r.code) : -EIO;
    resp_free(&r);
    return rc;
}

/* ---- one-sided lookups (the MDT's slot table, rocepool_servers.c) ---- */
typedef struct { uint64_t addr, nslots; uint32_t rkey; int state; pthread_mutex_t lk; } mdt_tab_t;  /* state: 0 unknown, 1 ok, -1 none */
static mdt_tab_t g_mdt_tab[MAX_PEERS];
static atomic_ullong g_direct_lookups, g_direct_lookup_misses;
#define MD_SLOT 256
#define MD_PROBE 8
#define MD_SLOT_TAIL (MD_SLOT - 8)

static uint64_t fnv1a64(const char *s, size_t n) {
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; i++) { h ^= (uint8_t)s[i]; h *= 1099511628211ULL; }
    return h;
}

void rp_lookup_stats(uint64_t *direct, uint64_t *misses) {
    *direct = atomic_load(&g_direct_lookups);
    *misses = atomic_load(&g_direct_lookup_misses);
}

static mdt_tab_t *mdt_table(peer_t *p) {
    mdt_tab_t *t = &g_mdt_tab[p - g_mdts];
    if (t->state) return t;
    static pthread_mutex_t once_lk = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&once_lk);
    if (!t->state) {
        uint8_t req[1] = {12};
        resp_t r;
        int rc = rpc(p, OP_MDT, "", req, 1, NULL, 0, &r);
        if (rc == 0 && r.op == OP_MDT_OK && r.vlen >= 20) {
            memcpy(&t->addr, r.val, 8);
            memcpy(&t->nslots, r.val + 8, 8);
            memcpy(&t->rkey, r.val + 16, 4);
            t->state = 1;
        } else {
            t->state = -1;  /* a Python MDT, or no table: RPC lookups */
        }
        if (rc == 0) resp_free(&r);
    }
    pthread_mutex_unlock(&once_lk);
    return t;
}

static __thread uint8_t *tl_slots;  /* MD_PROBE slots + 1 slot + 8 B of registered scratch */

/* One-sided lookups: -1 auto (on where the transport executes a read's
 * bytes in order -- RC; off on unordered ones -- EFA's SRD, where a
 * consistent lookup takes three round trips and loses to the RPC),
 * ROCEPOOL_DIRECT_LOOKUP=1/0 forces. */
static int g_direct_lookup = -1;

/* Unordered transports: the slot's bytes may have been fetched in any
 * order within the probe. Re-read the slot (after the probe completed),
 * then its seq (after that completed): the probe's seq precedes the
 * re-read, the last seq follows it, seqs never repeat, so equal seqs
 * mean nothing was written in between. */
static int slot_confirm(peer_t *p, const mdt_tab_t *t, uint64_t idx, uint64_t seq, uint8_t *buf, roce_mem_t *mem) {
    uint8_t *sl = buf + MD_PROBE * MD_SLOT, *s2 = sl + MD_SLOT;
    rp_read_t rd = { t->addr + idx * MD_SLOT, t->rkey, MD_SLOT, mem, sl };
    if (rp_rdma_read(p, &rd, 1)) return -1;
    rp_read_t rs = { t->addr + idx * MD_SLOT, t->rkey, 8, mem, s2 };
    if (rp_rdma_read(p, &rs, 1)) return -1;
    uint64_t a, b;
    memcpy(&a, sl, 8);
    memcpy(&b, s2, 8);
    return a == seq && b == seq ? 0 : -1;
}

/* 0 / -ENOENT from the table; -EAGAIN: ask the MDT */
static int mdt_lookup_direct(peer_t *p, const char *path, attr_t *a) {
    if (!cfg.direct_read) return -EAGAIN;
    if (g_direct_lookup < 0) g_direct_lookup = roce_transport_reads_ordered();
    if (!g_direct_lookup) return -EAGAIN;
    int ordered = roce_transport_reads_ordered();
    mdt_tab_t *t = mdt_table(p);
    if (t->state != 1) return -EAGAIN;
    uint64_t h = fnv1a64(path, strlen(path));
    uint64_t start = h % t->nslots;
    if (start + MD_PROBE > t->nslots) return -EAGAIN;  /* a wrapping run: rare, the RPC handles it */
    size_t scratch = (MD_PROBE + 1) * MD_SLOT + 64;
    if (!tl_slots) { tl_slots = rp_bulk_alloc(scratch); if (!tl_slots) return -EAGAIN; }
    roce_mem_t *mem = rp_bulk_mem(tl_slots, scratch);
    rp_read_t rd = { t->addr + start * MD_SLOT, t->rkey, MD_PROBE * MD_SLOT, mem, tl_slots };
    if (rp_rdma_read(p, &rd, 1)) return -EAGAIN;
    for (int i = 0; i < MD_PROBE; i++) {
        const uint8_t *sl = tl_slots + (size_t)i * MD_SLOT;
        uint64_t seq, ph, tail;
        uint32_t elen;
        memcpy(&seq, sl, 8);
        if (seq == 0) { atomic_fetch_add(&g_direct_lookups, 1); return -ENOENT; }
        memcpy(&ph, sl + 8, 8);
        if (ph != h) continue;
        if (!ordered && !(seq & 1)) {
            if (slot_confirm(p, t, start + i, seq, tl_slots, mem)) {
                atomic_fetch_add(&g_direct_lookup_misses, 1);
                return -EAGAIN;
            }
            sl = tl_slots + MD_PROBE * MD_SLOT;  /* the bracketed copy */
        }
        memcpy(&tail, sl + MD_SLOT_TAIL, 8);
        memcpy(&elen, sl + 16, 4);
        if ((seq & 1) || tail != seq || elen > MD_SLOT_TAIL - 24 || !decode_entry(sl + 24, elen, a)) {
            atomic_fetch_add(&g_direct_lookup_misses, 1);
            return -EAGAIN;  /* mid-write or a tombstone: the MDT says */
        }
        atomic_fetch_add(&g_direct_lookups, 1);
        return 0;
    }
    atomic_fetch_add(&g_direct_lookup_misses, 1);
    return -EAGAIN;  /* the run is full and not ours */
}

static int mdt_lookup(const char *path, attr_t *a) {
    int rc = mdt_lookup_direct(mdt_for(path), path, a);
    if (rc != -EAGAIN) return rc;
    uint8_t req[1] = {1};
    return mdt_entry_call(path, req, 1, a);
}

static int mdt_create(const char *path, int is_dir, int exclusive, const attr_t *lay, attr_t *a) {
    uint8_t req[20] = {2, (uint8_t)(is_dir ? 1 : 0), (uint8_t)(exclusive ? 1 : 0),
                       (uint8_t)((lay ? 1 : 0) | (!is_dir && cfg.inline_ok ? 2 : 0))};
    size_t n = 4;
    if (lay) {
        memcpy(req + 4, &lay->sc, 4);
        memcpy(req + 8, &lay->ss, 8);
        memcpy(req + 16, &lay->so, 4);
        n = 20;
    }
    return mdt_entry_call(path, req, n, a);
}

/* Data-on-MDT (fs/mdt_wire.py) */
static int mdt_write_inline(const char *path, uint64_t off, const uint8_t *data, size_t len, attr_t *a) {
    uint8_t hdr[9] = {7};
    memcpy(hdr + 1, &off, 8);
    resp_t r;
    int rc = rpc(mdt_for(path), OP_MDT, path, hdr, 9, data, len, &r);
    if (rc) return rc;
    if (r.op == OP_MDT_OK) rc = decode_entry(r.val, r.vlen, a) ? 0 : -EIO;
    else rc = r.op == OP_ERROR ? mdt_errno(r.code) : -EIO;
    resp_free(&r);
    return rc;
}

/* read_inline (op 8, off/len) or freeze (op 9): reply u64 iver + entry +
 * bytes. On success *out holds the frame; data and dlen point into it. */
int mdt_inline_read(const char *path, int freeze, uint64_t off, uint32_t len, attr_t *a, resp_t *out,
                           const uint8_t **data, size_t *dlen) {
    uint8_t req[13] = {(uint8_t)(freeze ? 9 : 8)};
    memcpy(req + 1, &off, 8);
    memcpy(req + 9, &len, 4);
    int rc = rpc(mdt_for(path), OP_MDT, path, req, freeze ? 1 : 13, NULL, 0, out);
    if (rc) return rc;
    if (out->op != OP_MDT_OK || out->vlen < 8) {
        rc = out->op == OP_ERROR ? mdt_errno(out->code) : -EIO;
        resp_free(out);
        return rc;
    }
    size_t used = decode_entry(out->val + 8, out->vlen - 8, a);
    if (!used) { resp_free(out); return -EIO; }
    *data = out->val + 8 + used;
    *dlen = out->vlen - 8 - used;
    return 0;
}

static int mdt_spill_commit(const char *path, attr_t *a) {
    uint8_t req[1] = {10};
    return mdt_entry_call(path, req, 1, a);
}

int mdt_set_size(const char *path, uint64_t size, int exact, attr_t *a) {
    uint8_t req[10] = {3, (uint8_t)(exact ? 1 : 0)};
    memcpy(req + 2, &size, 8);
    return mdt_entry_call(path, req, 10, a);
}

static int mdt_unlink(const char *path, int is_dir, attr_t *a) {
    uint8_t req[2] = {4, (uint8_t)(is_dir ? 1 : 0)};
    return mdt_entry_call(path, req, 2, a);
}

static int mdt_child_count_one(peer_t *p, const char *dir, uint64_t *n) {
    uint8_t req[1] = {6};
    resp_t r;
    int rc = rpc(p, OP_MDT, dir, req, 1, NULL, 0, &r);
    if (rc) return rc;
    if (r.op == OP_MDT_OK && r.vlen >= 8) { memcpy(n, r.val, 8); rc = 0; }
    else rc = r.op == OP_ERROR ? mdt_errno(r.code) : -EIO;
    resp_free(&r);
    return rc;
}


/* One MDT's entries of dir, page by page (a page at most half a frame):
 * a directory of millions of entries does not fit one reply. Each page
 * asks for the names after the last one received. */
static int mdt_readdir_one(peer_t *p, const char *dir, dirent_t **out, size_t *count) {
    size_t cap = 0, i = 0;
    dirent_t *v = NULL;
    char last[4096];
    size_t last_len = 0;
    uint32_t budget = (uint32_t)(rp_frame_bytes / 2 > 0xffffffffu ? 0xffffffffu : rp_frame_bytes / 2);
    for (;;) {
        uint8_t req[7 + sizeof(last)];
        req[0] = 5;
        memcpy(req + 1, &budget, 4);
        req[5] = (uint8_t)(last_len & 0xff);
        req[6] = (uint8_t)(last_len >> 8);
        memcpy(req + 7, last, last_len);
        resp_t r;
        int rc = rpc(p, OP_MDT, dir, req, 7 + last_len, NULL, 0, &r);
        if (rc == 0 && (r.op != OP_MDT_OK || r.vlen < 4)) {
            rc = r.op == OP_ERROR ? mdt_errno(r.code) : -EIO;
            resp_free(&r);
        }
        if (rc) {
            for (size_t k = 0; k < i; k++) free(v[k].name);
            free(v);
            return rc;
        }
        uint32_t n;
        memcpy(&n, r.val, 4);
        int more = (n & 0x80000000u) != 0;
        n &= 0x7fffffffu;
        if (i + n > cap) {
            cap = (i + n) * 2 + 16;
            v = xrealloc(v, cap * sizeof(*v));
        }
        size_t off = 4, got = 0;
        for (; got < n; got++) {
            dirent_t *d = &v[i + got];
            if (off + 2 > r.vlen) break;
            uint16_t ln;
            memcpy(&ln, r.val + off, 2);
            off += 2;
            if (off + ln > r.vlen) break;
            d->name = xmalloc(ln + 1u);
            memcpy(d->name, r.val + off, ln);
            d->name[ln] = 0;
            off += ln;
            size_t used = decode_entry(r.val + off, r.vlen - off, &d->a);
            if (!used) { free(d->name); break; }
            off += used;
        }
        resp_free(&r);
        i += got;
        if (!more || !got) break;  /* (a server without paging sends everything, top bit clear) */
        last_len = strlen(v[i - 1].name);
        if (last_len > sizeof(last)) last_len = sizeof(last);
        memcpy(last, v[i - 1].name, last_len);
    }
    *out = v ? v : calloc(1, sizeof(*v));
    *count = i;
    return 0;
}

/* A striped directory spans every MDT: readdir / child_count go to all of
 * them in parallel and merge (the by-parent rule: one MDT, one RPC). The
 * fan-out helpers live after the thread pool below. */
int mdt_readdir(const char *dir, dirent_t **out, size_t *count);
static int mdt_child_count(const char *dir, uint64_t *n);

/* ---------------------------------------------------------------- attr cache
 * fs/attr_cache.py: path -> (expires, attr | negative). Own mutations update
 * it immediately; other clients' changes show within attr_ttl. */
#define AC_BUCKETS 65536
typedef struct ac_ent {
    struct ac_ent *next;
    char *path;
    double expires;
    int negative;
    attr_t a;
} ac_ent;

static ac_ent *g_ac[AC_BUCKETS];
static pthread_mutex_t g_ac_lk = PTHREAD_MUTEX_INITIALIZER;
static size_t g_ac_n;

uint32_t strhash(const char *s) {
    uint32_t h = 2166136261u;
    for (; *s; s++) h = (h ^ (uint8_t)*s) * 16777619u;
    return h;
}

static void ac_clear_locked(void) {
    for (int i = 0; i < AC_BUCKETS; i++) {
        ac_ent *e = g_ac[i];
        while (e) { ac_ent *n = e->next; free(e->path); free(e); e = n; }
        g_ac[i] = NULL;
    }
    g_ac_n = 0;
}

/* 1 = hit (a filled or *negative set), 0 = miss */
int ac_get(const char *path, attr_t *a, int *negative) {
    if (cfg.attr_ttl <= 0) return 0;
    uint32_t b = strhash(path) % AC_BUCKETS;
    int hit = 0;
    double now = mono_s();
    pthread_mutex_lock(&g_ac_lk);
    for (ac_ent **pp = &g_ac[b]; *pp; pp = &(*pp)->next) {
        ac_ent *e = *pp;
        if (strcmp(e->path, path)) continue;
        if (e->expires > now) {
            hit = 1;
            *negative = e->negative;
            if (!e->negative) *a = e->a;
        } else {
            *pp = e->next;
            free(e->path);
            free(e);
            g_ac_n--;
        }
        break;
    }
    pthread_mutex_unlock(&g_ac_lk);
    return hit;
}

void ac_put(const char *path, const attr_t *a) {  /* a == NULL: negative entry */
    if (cfg.attr_ttl <= 0) return;
    uint32_t b = strhash(path) % AC_BUCKETS;
    pthread_mutex_lock(&g_ac_lk);
    ac_ent *e;
    for (e = g_ac[b]; e; e = e->next)
        if (!strcmp(e->path, path)) break;
    if (!e) {
        if (g_ac_n >= 100000) ac_clear_locked();
        e = calloc(1, sizeof(*e));
        e->path = xstrdup(path);
        e->next = g_ac[b];
        g_ac[b] = e;
        g_ac_n++;
    }
    e->expires = mono_s() + cfg.attr_ttl;
    e->negative = a == NULL;
    if (a) e->a = *a;
    pthread_mutex_unlock(&g_ac_lk);
}

/* exact = truncate. Otherwise a write's size update only ever grows the
 * cached size: two flushes can get their MDT set_size(max) replies back in
 * either order, and letting the older, smaller one land last made stat and
 * read see a truncated file (IOR, 8 ranks writing one shared file). */
void ac_del(const char *path) {
    uint32_t b = strhash(path) % AC_BUCKETS;
    pthread_mutex_lock(&g_ac_lk);
    for (ac_ent **pp = &g_ac[b]; *pp; pp = &(*pp)->next)
        if (!strcmp((*pp)->path, path)) {
            ac_ent *e = *pp;
            *pp = e->next;
            free(e->path);
            free(e);
            g_ac_n--;
            break;
        }
    pthread_mutex_unlock(&g_ac_lk);
}

void ac_update_size(const char *path, uint64_t size, int exact) {
    uint32_t b = strhash(path) % AC_BUCKETS;
    pthread_mutex_lock(&g_ac_lk);
    for (ac_ent *e = g_ac[b]; e; e = e->next)
        if (!strcmp(e->path, path)) {
            if (!e->negative) {
                if (exact || size > e->a.size) e->a.size = size;
                e->a.mtime = wall_s();
            }
            break;
        }
    pthread_mutex_unlock(&g_ac_lk);
}

/* ---------------------------------------------------------------- task pool
 * Stripe RPCs fan out across OSTs concurrently. FUSE threads only wait on
 * tasks; tasks never wait on other tasks, so a fixed pool cannot deadlock. */
typedef struct task {
    void (*fn)(void *);
    void *arg;
    struct task *next;
} task_t;

static struct {
    pthread_mutex_t lk;
    pthread_cond_t cv;
    task_t *head, *tail;
} g_pool = {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, NULL, NULL};

void *pool_worker(void *arg) {
    pthread_setname_np(pthread_self(), "fuse-io");
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&g_pool.lk);
        while (!g_pool.head) pthread_cond_wait(&g_pool.cv, &g_pool.lk);
        task_t *t = g_pool.head;
        g_pool.head = t->next;
        if (!g_pool.head) g_pool.tail = NULL;
        pthread_mutex_unlock(&g_pool.lk);
        t->fn(t->arg);
        free(t);
    }
    return NULL;
}

void pool_submit(void (*fn)(void *), void *arg) {
    task_t *t = xmalloc(sizeof(*t));
    t->fn = fn;
    t->arg = arg;
    t->next = NULL;
    pthread_mutex_lock(&g_pool.lk);
    if (g_pool.tail) g_pool.tail->next = t; else g_pool.head = t;
    g_pool.tail = t;
    pthread_cond_signal(&g_pool.cv);
    pthread_mutex_unlock(&g_pool.lk);
}

typedef struct {
    pthread_mutex_t lk;
    pthread_cond_t cv;
    int remaining;
    int err;
} group_t;

static void group_init(group_t *g, int n) {
    pthread_mutex_init(&g->lk, NULL);
    pthread_cond_init(&g->cv, NULL);
    g->remaining = n;
    g->err = 0;
}

static void group_done(group_t *g, int err) {
    pthread_mutex_lock(&g->lk);
    if (err && !g->err) g->err = err;
    if (--g->remaining == 0) pthread_cond_broadcast(&g->cv);
    pthread_mutex_unlock(&g->lk);
}

static int group_wait(group_t *g) {
    pthread_mutex_lock(&g->lk);
    while (g->remaining > 0) pthread_cond_wait(&g->cv, &g->lk);
    int e = g->err;
    pthread_mutex_unlock(&g->lk);
    pthread_mutex_destroy(&g->lk);
    pthread_cond_destroy(&g->cv);
    return e;
}

/* run fn over n args: n-1 on the pool, the last inline, then wait */
static int run_all(void (*fn)(void *), void **args, int n, group_t *g) {
    for (int i = 0; i < n - 1; i++) pool_submit(fn, args[i]);
    if (n > 0) fn(args[n - 1]);
    return group_wait(g);
}

/* ---- directory fan-out (striped directories, fs/namespace_shard.py) ---- */
typedef struct {
    group_t *g;
    peer_t *p;
    const char *dir;
    dirent_t *ents;
    size_t n;
    uint64_t count;
} dir_task_t;

static void readdir_task(void *arg) {
    dir_task_t *t = arg;
    group_done(t->g, mdt_readdir_one(t->p, t->dir, &t->ents, &t->n));
}

static void child_count_task(void *arg) {
    dir_task_t *t = arg;
    group_done(t->g, mdt_child_count_one(t->p, t->dir, &t->count));
}

static int cmp_dirent(const void *a, const void *b) {
    return strcmp(((const dirent_t *)a)->name, ((const dirent_t *)b)->name);
}

static int mdt_fan_out(const char *dir, void (*fn)(void *), dir_task_t *tasks, int *nmdt) {
    peer_t *peers[MAX_PEERS];
    int n = mdts_for_children(dir, peers);
    void *args[MAX_PEERS];
    group_t g;
    group_init(&g, n);
    for (int i = 0; i < n; i++) { tasks[i] = (dir_task_t){&g, peers[i], dir, NULL, 0, 0}; args[i] = &tasks[i]; }
    *nmdt = n;
    return run_all(fn, args, n, &g);
}

int mdt_readdir(const char *dir, dirent_t **out, size_t *count) {
    dir_task_t tasks[MAX_PEERS];
    int n;
    int rc = mdt_fan_out(dir, readdir_task, tasks, &n);
    size_t total = 0;
    for (int i = 0; i < n; i++) total += tasks[i].n;
    if (rc) {
        for (int i = 0; i < n; i++) { for (size_t k = 0; k < tasks[i].n; k++) free(tasks[i].ents[k].name); free(tasks[i].ents); }
        return rc;
    }
    if (n == 1) { *out = tasks[0].ents; *count = tasks[0].n; return 0; }
    dirent_t *all = calloc(total ? total : 1, sizeof(*all));
    size_t w = 0;
    for (int i = 0; i < n; i++) {
        if (tasks[i].n) memcpy(all + w, tasks[i].ents, tasks[i].n * sizeof(*all));
        w += tasks[i].n;
        free(tasks[i].ents);
    }
    qsort(all, total, sizeof(*all), cmp_dirent);
    *out = all;
    *count = total;
    return 0;
}

static int mdt_child_count(const char *dir, uint64_t *n) {
    dir_task_t tasks[MAX_PEERS];
    int k;
    int rc = mdt_fan_out(dir, child_count_task, tasks, &k);
    if (rc) return rc;
    uint64_t total = 0;
    for (int i = 0; i < k; i++) total += tasks[i].count;
    *n = total;
    return 0;
}

/* ---------------------------------------------------------------- layout */
int ost_for_stripe(const attr_t *a, uint64_t idx) {
    uint64_t obj = idx % a->sc;
    return (int)((a->so + obj) % (uint64_t)cfg.nost);
}

void stripe_key(char *out, size_t n, const char *path, uint64_t idx) {
    snprintf(out, n, "%s.stripe.%08llu", path, (unsigned long long)idx);
}

static void choose_layout(const char *path, attr_t *lay) {
    memset(lay, 0, sizeof(*lay));
    int count = cfg.stripe_count <= 0 ? cfg.nost : (cfg.stripe_count > cfg.nost ? cfg.nost : cfg.stripe_count);
    lay->has_layout = 1;
    lay->sc = (uint32_t)count;
    lay->ss = cfg.stripe_size;
    lay->so = (uint32_t)(sha256_u64(path) % (uint64_t)cfg.nost);
}

/* ---------------------------------------------------------------- namespace */
int fs_getattr(const char *path, attr_t *a) {
    if (!*path) {
        memset(a, 0, sizeof(*a));
        a->is_dir = 1;
        return 0;
    }
    int neg = 0;
    if (ac_get(path, a, &neg)) return neg ? -ENOENT : 0;
    int rc = mdt_lookup(path, a);
    if (rc == 0) ac_put(path, a);
    else if (rc == -ENOENT) ac_put(path, NULL);
    return rc;
}

static int require_parent_dir(const char *path) {
    char par[4096];
    attr_t pa;
    int rc = fs_getattr(parent_of(path, par, sizeof(par)), &pa);
    if (rc) return rc;
    return pa.is_dir ? 0 : -ENOTDIR;
}

int fs_mkdir(const char *path, attr_t *a) {
    int rc = require_parent_dir(path);
    if (rc) return rc;
    rc = mdt_create(path, 1, 1, NULL, a);
    if (rc == 0) ac_put(path, a);
    return rc;
}

int fs_create_file_excl(const char *path, int exclusive, attr_t *a) {
    int rc = require_parent_dir(path);
    if (rc) return rc;
    attr_t lay;
    choose_layout(path, &lay);
    rc = mdt_create(path, 0, exclusive, &lay, a);
    if (rc == -EEXIST && !exclusive) return -EISDIR;  /* exists as a directory */
    if (rc == 0) ac_put(path, a);
    return rc;
}

int fs_create_file(const char *path, attr_t *a) { return fs_create_file_excl(path, 0, a); }

int fs_rmdir(const char *path) {
    attr_t a;
    int rc = fs_getattr(path, &a);
    if (rc) return rc;
    if (!a.is_dir) return -ENOTDIR;
    uint64_t n = 0;
    rc = mdt_child_count(path, &n);
    if (rc) return rc;
    if (n) return -ENOTEMPTY;
    rc = mdt_unlink(path, 1, &a);
    if (rc == 0 || rc == -ENOENT) ac_put(path, NULL);
    return rc;
}

/* ---------------------------------------------------------------- striped data
 * A write is a list of segments, contiguous in file order: the kernel's
 * WRITE requests as they sit in their arena buffers (rocepool_client.h
 * rp_bulk_*), so a stripe assembled from up to 4 of them goes out with no
 * copy at all -- the OST RDMA READs each segment where it is. More pieces
 * than that (ior-hard: 22 x 47 KB per stripe) are gathered once into an
 * arena buffer, which is still read in place by the OST. */
#define WSEG_MAX 4
typedef struct { const uint8_t *data; size_t len; roce_mem_t *mem; } wseg_t;  /* mem: device memory, else NULL */

typedef struct {
    group_t *g;
    const char *path;
    const attr_t *a;
    uint64_t idx;
    uint64_t lo, hi;       /* within the stripe */
    rp_seg_t seg[WSEG_MAX];
    int nseg;
    uint8_t *gathered;     /* arena copy when the stripe had more than WSEG_MAX pieces */
} stripe_io_t;

static void stripe_write_task(void *arg) {
    stripe_io_t *t = arg;
    char key[4200];
    stripe_key(key, sizeof(key), t->path, t->idx);
    peer_t *p = &g_osts[ost_for_stripe(t->a, t->idx)];
    int rc;
    if (t->lo == 0 && t->hi == t->a->ss)
        rc = rp_ost_put_v(p, key, t->seg, t->nseg, -1, NULL);  /* whole stripe: blind PUT */
    else  /* one atomic WRITE_AT (was: stat/get/stat + CAS put, retried) */
        rc = rp_ost_write_at_v(p, key, t->lo, t->seg, t->nseg, NULL, NULL);
    rp_bulk_free(t->gathered);
    group_done(t->g, rc);
}

/* the segments of a stripe's [lo, hi) (file offsets) out of the write's
 * segment list; gathers into arena memory when there are too many */
static void stripe_segments(stripe_io_t *t, uint64_t s_lo, uint64_t s_hi, const wseg_t *segs, int nsegs,
                            uint64_t off) {
    uint64_t pos = off;
    int n = 0, over = 0;
    for (int i = 0; i < nsegs; i++) {
        uint64_t a = pos, b = pos + segs[i].len;
        pos = b;
        if (b <= s_lo || a >= s_hi) continue;
        uint64_t lo = a > s_lo ? a : s_lo, hi = b < s_hi ? b : s_hi;
        if (n == WSEG_MAX) { over = 1; break; }
        t->seg[n++] = (rp_seg_t){ segs[i].data + (lo - a), (size_t)(hi - lo), segs[i].mem };
    }
    if (!over) { t->nseg = n; return; }
    size_t total = (size_t)(s_hi - s_lo);
    t->gathered = rp_bulk_alloc(total);
    if (!t->gathered) t->gathered = xmalloc(total);
    pos = off;
    for (int i = 0; i < nsegs; i++) {
        uint64_t a = pos, b = pos + segs[i].len;
        pos = b;
        if (b <= s_lo || a >= s_hi) continue;
        uint64_t lo = a > s_lo ? a : s_lo, hi = b < s_hi ? b : s_hi;
        memcpy(t->gathered + (lo - s_lo), segs[i].data + (lo - a), (size_t)(hi - lo));
    }
    t->seg[0] = (rp_seg_t){ t->gathered, total, NULL };
    t->nseg = 1;
}

/* One BATCH frame per OST, all OSTs in parallel. ok_missing: NOT_FOUND
 * sub-replies are fine (holes). */
typedef struct {
    group_t *g;
    int ost;
    rp_batch_t *b;
    int ok_missing;
} batch_task_t;

static void batch_task(void *arg) {
    batch_task_t *t = arg;
    /* A huge file's truncate/unlink can list more stripes than fit in one
     * frame: send the items in chunks of at most half a frame each. */
    size_t limit = rp_frame_bytes / 2, off = 4;
    int rc = 0;
    while (off < t->b->len && !rc) {
        rp_batch_t c;
        rp_batch_init(&c);
        while (off < t->b->len) {
            uint32_t kl, vl;
            memcpy(&kl, t->b->buf + off + 1, 4);
            memcpy(&vl, t->b->buf + off + 5, 4);
            size_t item = 9 + (size_t)kl + vl;
            if (c.n && c.len + item > limit) break;
            if (c.len + item > c.cap) {
                while (c.cap < c.len + item) c.cap *= 2;
                c.buf = xrealloc(c.buf, c.cap);
            }
            memcpy(c.buf + c.len, t->b->buf + off, item);
            c.len += item;
            c.n++;
            off += item;
        }
        rp_resp_t r;
        rc = rp_batch_send(&g_osts[t->ost], &c, &r);
        rp_batch_free(&c);
        if (rc) break;
        size_t pos = 0;
        uint8_t op;
        const uint8_t *v;
        uint32_t vl;
        char code[32];
        while (rp_batch_next(&r, &pos, &op, &v, &vl, code, sizeof(code)) == 0)
            if (op == RP_ERROR && !(t->ok_missing && !strcmp(code, "NOT_FOUND"))) { rc = -EIO; break; }
        rp_resp_free(&r);
    }
    group_done(t->g, rc);
}

static int run_batches(rp_batch_t *per_ost, int ok_missing) {
    batch_task_t *tasks = calloc((size_t)cfg.nost, sizeof(*tasks));
    void **args = xmalloc((size_t)cfg.nost * sizeof(void *));
    int n = 0;
    group_t g;
    for (int i = 0; i < cfg.nost; i++)
        if (per_ost[i].n) { tasks[n] = (batch_task_t){&g, i, &per_ost[i], ok_missing}; args[n] = &tasks[n]; n++; }
    int rc = 0;
    if (n) {
        group_init(&g, n);
        rc = run_all(batch_task, args, n, &g);
    }
    free(tasks);
    free(args);
    return rc;
}

static uint64_t inline_hint(const attr_t *a) {
    static long lim = -1;
    if (lim < 0) {
        const char *v = getenv("ROCEPOOL_INLINE_MAX");
        lim = v && *v ? atol(v) : 32768;
        if (lim < 0) lim = 0;
    }
    return a->ss && a->ss < (uint64_t)lim ? a->ss : (uint64_t)lim;
}

/* Move an inline file to its stripes (protocol: fs/mdt_wire.py). Any number
 * of clients may run this at once, or finish a spill another one began. */
int fs_spill(const char *path) {
    int done = 0;
    for (int i = 0; i < CAS_MAX_ATTEMPTS && !done; i++) {
        attr_t a;
        resp_t r;
        const uint8_t *d;
        size_t n;
        int rc = mdt_inline_read(path, 1, 0, 0, &a, &r, &d, &n);
        if (rc) return rc;
        resp_free(&r);
        if (!a.inl) { done = 1; break; }  /* someone committed already */
        char key[4200];
        stripe_key(key, sizeof(key), path, 0);
        peer_t *p = &g_osts[ost_for_stripe(&a, 0)];
        /* a version TOKEN, not "expected 0 = absent": a plain CAS could land
         * after this file was truncated/unlinked (stripe deleted -> absent
         * again) and recreated in between -- ABA */
        uint64_t v0 = 0;
        rc = rp_ost_vtoken(p, key, &v0);
        if (rc) return rc;
        rc = mdt_inline_read(path, 1, 0, 0, &a, &r, &d, &n);  /* still frozen AFTER reading v0? */
        if (rc) return rc;
        if (!a.inl) { resp_free(&r); done = 1; break; }
        rc = rp_ost_put_token(p, key, d, n, v0, NULL);
        resp_free(&r);
        if (rc == 0) done = 1;
        else if (rc != -EAGAIN) return rc;
    }
    /* Never commit without our PUT (or someone's commit) behind it: that
     * would publish a striped file whose stripe 0 lacks the inline bytes. */
    if (!done) return -EIO;
    attr_t a;
    int rc = mdt_spill_commit(path, &a);
    if (rc == 0) ac_put(path, &a);
    return rc;
}

static int fs_write_segs(const char *path, uint64_t off, const wseg_t *segs, int nsegs) {
    size_t len = 0;
    for (int i = 0; i < nsegs; i++) len += segs[i].len;
    if (!len) return 0;
    attr_t a;
    int rc = fs_getattr(path, &a);
    if (rc == -ENOENT) rc = fs_create_file(path, &a);
    if (rc) return rc;
    if (a.is_dir) return -EISDIR;
    while (a.inl) {  /* one MDT round trip, no OST at all */
        attr_t na;
        /* Only offer the MDT what could fit: a write ending past the inline
         * limit (same ROCEPOOL_INLINE_MAX rule as the servers, never more
         * than a stripe) spills first instead of shipping the data to the
         * MDT just to hear INLINE_FULL -- that doubled the bytes of every
         * new large file, and a run over one frame failed with EFBIG. */
        int device = 0;
        for (int i = 0; i < nsegs; i++) if (segs[i].mem && roce_mem_is_device(segs[i].mem)) device = 1;
        if (off + len > inline_hint(&a) || device) {  /* device memory: never through the CPU */
            rc = E_INLINE_FULL;
        } else if (nsegs == 1) {
            rc = mdt_write_inline(path, off, segs[0].data, len, &na);
        } else {  /* small (inline-sized): gather */
            uint8_t *tmp = xmalloc(len);
            size_t o = 0;
            for (int i = 0; i < nsegs; i++) { memcpy(tmp + o, segs[i].data, segs[i].len); o += segs[i].len; }
            rc = mdt_write_inline(path, off, tmp, len, &na);
            free(tmp);
        }
        if (rc == 0) { ac_put(path, &na); return 0; }
        if (rc == E_INLINE_FULL || rc == E_FROZEN) {
            rc = fs_spill(path);
            if (rc) return rc;
        } else if (rc != E_NOT_INLINE) {
            return rc;
        }
        ac_del(path);
        rc = fs_getattr(path, &a);
        if (rc) return rc;
    }
    if (!a.has_layout || !a.sc || !a.ss) return -EIO;
    uint64_t end = off + len, first = off / a.ss, last = (end - 1) / a.ss;
    int n = (int)(last - first + 1);
    stripe_io_t *tasks = calloc((size_t)n, sizeof(*tasks));
    void **args = xmalloc((size_t)n * sizeof(void *));
    group_t g;
    group_init(&g, n);
    for (int i = 0; i < n; i++) {
        uint64_t idx = first + (uint64_t)i, s0 = idx * a.ss;
        uint64_t lo = off > s0 ? off : s0, hi = end < s0 + a.ss ? end : s0 + a.ss;
        tasks[i] = (stripe_io_t){.g = &g, .path = path, .a = &a, .idx = idx, .lo = lo - s0, .hi = hi - s0};
        stripe_segments(&tasks[i], lo, hi, segs, nsegs, off);
        args[i] = &tasks[i];
    }
    rc = run_all(stripe_write_task, args, n, &g);
    free(tasks);
    free(args);
    if (rc) return rc;
    if (end > a.size) {
        attr_t na;
        rc = mdt_set_size(path, end, 0, &na);
        if (rc == E_INLINE || rc == E_FROZEN) {
            /* cached attributes said "striped" but the file is inline
             * (recreated by another client) or mid-spill: finish any spill,
             * then redo the write where the file now lives */
            if (rc == E_FROZEN && (rc = fs_spill(path)) != 0) return rc;
            ac_del(path);
            return fs_write_segs(path, off, segs, nsegs);
        }
        if (rc == 0) ac_update_size(path, na.size, 0);
    }
    return rc;
}

int fs_write_range(const char *path, uint64_t off, const uint8_t *data, size_t len) {
    wseg_t seg = { data, len, NULL };
    return fs_write_segs(path, off, &seg, 1);
}

int fs_truncate(const char *path, uint64_t size) {
    attr_t a;
    int rc = fs_getattr(path, &a);
    if (rc) return rc;
    if (a.is_dir) return -EISDIR;
    while (a.inl) {
        attr_t na;
        rc = mdt_set_size(path, size, 1, &na);  /* resizes the inline bytes */
        if (rc == 0) { ac_put(path, &na); return 0; }
        if (rc == E_INLINE_FULL || rc == E_FROZEN) {
            rc = fs_spill(path);
            if (rc) return rc;
        } else if (rc != E_NOT_INLINE) {
            return rc;
        }
        ac_del(path);
        rc = fs_getattr(path, &a);
        if (rc) return rc;
    }
    if (size < a.size && a.has_layout && a.ss) {
        uint64_t ss = a.ss, last_old = (a.size - 1) / ss, first_gone = (size + ss - 1) / ss;
        uint64_t start = size % ss ? size / ss : first_gone;
        rp_batch_t *per = calloc((size_t)cfg.nost, sizeof(*per));
        for (int i = 0; i < cfg.nost; i++) rp_batch_init(&per[i]);
        for (uint64_t idx = start; idx <= last_old; idx++) {
            char key[4200];
            stripe_key(key, sizeof(key), path, idx);
            rp_batch_t *b = &per[ost_for_stripe(&a, idx)];
            if (idx >= first_gone) {
                rp_batch_add(b, RP_DELETE, key, NULL, 0, NULL, 0);
            } else {
                uint64_t keep = size - idx * ss;  /* server-side trim: no read-modify-write */
                rp_batch_add(b, RP_TRUNCATE, key, &keep, 8, NULL, 0);
            }
        }
        rc = run_batches(per, 1);
        for (int i = 0; i < cfg.nost; i++) rp_batch_free(&per[i]);
        free(per);
        if (rc) return rc;
    }
    attr_t na;
    rc = mdt_set_size(path, size, 1, &na);
    if (rc == 0) ac_update_size(path, na.size, 1);
    return rc;
}

int fs_remove_file(const char *path) {
    attr_t a;
    int rc = fs_getattr(path, &a);
    if (rc) return rc;
    if (a.is_dir) return -EISDIR;
    attr_t e;
    rc = mdt_unlink(path, 0, &e);
    if (rc == 0 || rc == -ENOENT) ac_put(path, NULL);
    if (rc) return rc;
    if (e.inl && !e.frozen) return 0;  /* the bytes went away with the MDT entry */
    if ((e.size > 0 || e.frozen) && e.has_layout && e.ss) {
        uint64_t n = (e.size + e.ss - 1) / e.ss;
        if (!n) n = 1;
        rp_batch_t *per = calloc((size_t)cfg.nost, sizeof(*per));
        for (int i = 0; i < cfg.nost; i++) rp_batch_init(&per[i]);
        for (uint64_t idx = 0; idx < n; idx++) {  /* one BATCH per OST, not one request per stripe */
            char key[4200];
            stripe_key(key, sizeof(key), path, idx);
            rp_batch_add(&per[ost_for_stripe(&e, idx)], RP_DELETE, key, NULL, 0, NULL, 0);
        }
        rc = run_batches(per, 1);
        for (int i = 0; i < cfg.nost; i++) rp_batch_free(&per[i]);
        free(per);
    }
    return rc;
}

/* ---- one-sided reads ----
 * A lease (rp_lease_t) per stripe object, in a direct-mapped cache keyed
 * by the stripe key; a read RDMA READs the bytes straight out of the OST's
 * registered copy and validates its seqlock. Anything stale (the object
 * rewritten, truncated, deleted, evicted) fails validation, the lease is
 * dropped and taken again once, then the read goes by RPC. The OST's CPU
 * sees one small LEASE request per stripe instead of every read. */
#define LEASE_SLOTS 65536
typedef struct { char *key; rp_lease_t l; } lease_slot_t;
static lease_slot_t g_lease[LEASE_SLOTS];
static pthread_mutex_t g_lease_lk[256];
static pthread_once_t g_lease_once = PTHREAD_ONCE_INIT;
static void lease_init(void) { for (int i = 0; i < 256; i++) pthread_mutex_init(&g_lease_lk[i], NULL); }
static uint32_t lease_idx(const char *key) { return strhash(key) & (LEASE_SLOTS - 1); }

static int lease_get(const char *key, rp_lease_t *out) {
    uint32_t i = lease_idx(key);
    pthread_mutex_lock(&g_lease_lk[i & 255]);
    int ok = g_lease[i].key && !strcmp(g_lease[i].key, key);
    if (ok) *out = g_lease[i].l;
    pthread_mutex_unlock(&g_lease_lk[i & 255]);
    return ok ? 0 : -ENOENT;
}
static void lease_put(const char *key, const rp_lease_t *l) {
    uint32_t i = lease_idx(key);
    pthread_mutex_lock(&g_lease_lk[i & 255]);
    if (!g_lease[i].key || strcmp(g_lease[i].key, key)) { free(g_lease[i].key); g_lease[i].key = xstrdup(key); }
    g_lease[i].l = *l;
    pthread_mutex_unlock(&g_lease_lk[i & 255]);
}
static void lease_drop(const char *key) {
    uint32_t i = lease_idx(key);
    pthread_mutex_lock(&g_lease_lk[i & 255]);
    if (g_lease[i].key && !strcmp(g_lease[i].key, key)) { free(g_lease[i].key); g_lease[i].key = NULL; }
    pthread_mutex_unlock(&g_lease_lk[i & 255]);
}

/* bytes [off, off+len) of the object into dst (arena memory): 0 and *got
 * (short at the object's end), -ENOENT (no such object), or -EAGAIN: no
 * one-sided path for this read, use the RPC */
static int direct_read_mem(rp_peer_t *p, const char *key, uint64_t off, size_t len, uint8_t *dst, roce_mem_t *dmem,
                           size_t *got);
static int direct_read(rp_peer_t *p, const char *key, uint64_t off, size_t len, uint8_t *dst, size_t *got) {
    return direct_read_mem(p, key, off, len, dst, NULL, got);
}
static int direct_read_mem(rp_peer_t *p, const char *key, uint64_t off, size_t len, uint8_t *dst, roce_mem_t *dmem,
                           size_t *got) {
    if (!cfg.direct_read || atomic_load(&p->no_lease)) return -EAGAIN;
    pthread_once(&g_lease_once, lease_init);
    rp_lease_t l;
    int have = lease_get(key, &l) == 0;
    for (int attempt = 0; attempt < 2; attempt++) {
        if (!have) {
            int rc = rp_ost_lease(p, key, &l);
            if (rc == -ENOTSUP) { atomic_store(&p->no_lease, 1); return -EAGAIN; }
            if (rc == -ENOENT) return -ENOENT;
            if (rc) return -EAGAIN;
            lease_put(key, &l);
            have = 1;
        }
        size_t n = off >= l.size ? 0 : (size_t)(l.size - off < len ? l.size - off : len);
        int rc = rp_ost_read_direct_mem(p, &l, off, n, dst, dmem);
        if (rc == 0) { *got = n; return 0; }
        if (rc != -EAGAIN) return -EAGAIN;
        lease_drop(key);
        have = 0;
    }
    return -EAGAIN;
}

/* Exact-range reads for non-sequential access (ior-hard: 47008-byte pieces
 * all over a shared file): READ_AT moves only the bytes asked for, instead
 * of whole 1 MiB stripes into a cache that the next read will not use. */
typedef struct {
    group_t *g;
    const char *path;
    const attr_t *a;
    uint64_t idx, lo, hi;
    uint8_t *dst;
} range_read_t;

static void range_read_task(void *arg) {
    range_read_t *t = arg;
    char key[4200];
    stripe_key(key, sizeof(key), t->path, t->idx);
    rp_resp_t r;
    uint64_t ver;
    size_t want = (size_t)(t->hi - t->lo), got = 0;
    rp_peer_t *peer = &g_osts[ost_for_stripe(t->a, t->idx)];
    int rc = -EAGAIN;
    uint8_t *tmp = cfg.direct_read ? rp_bulk_alloc(want) : NULL;
    if (tmp) {
        rc = direct_read(peer, key, t->lo, want, tmp, &got);
        if (rc == 0) memcpy(t->dst, tmp, got);
        rp_bulk_free(tmp);
    }
    if (rc == -EAGAIN) {
        got = 0;
        rc = rp_ost_read_at(peer, key, t->lo, (uint32_t)want, 0, &ver, &r);
        if (rc == 0) {
            got = r.vlen - 8 < want ? r.vlen - 8 : want;
            memcpy(t->dst, r.val + 8, got);
            rp_resp_free(&r);
        }
    }
    if (rc == -ENOENT) rc = 0;  /* a hole */
    if (got < want) memset(t->dst + got, 0, want - got);
    group_done(t->g, rc);
}

int fs_read_direct(const char *path, const attr_t *a, uint64_t off, size_t len, uint8_t *out) {
    uint64_t end = off + len, first = off / a->ss, last = (end - 1) / a->ss;
    int n = (int)(last - first + 1);
    range_read_t *tasks = calloc((size_t)n, sizeof(*tasks));
    void **args = xmalloc((size_t)n * sizeof(void *));
    group_t g;
    group_init(&g, n);
    for (int i = 0; i < n; i++) {
        uint64_t idx = first + (uint64_t)i, s0 = idx * a->ss;
        uint64_t lo = off > s0 ? off : s0, hi = end < s0 + a->ss ? end : s0 + a->ss;
        tasks[i] = (range_read_t){&g, path, a, idx, lo - s0, hi - s0, out + (lo - off)};
        args[i] = &tasks[i];
    }
    int rc = run_all(range_read_task, args, n, &g);
    free(tasks);
    free(args);
    return rc;
}

/* ---------------------------------------------------------------- device (GPU) memory
 * GPUDirect-Storage-style I/O: a file range straight into / out of device
 * memory. Every stripe goes one-sided (an OST lease + an RDMA READ that
 * lands in the GPU; the OST RDMA READs a write's data out of the GPU), all
 * stripes in parallel across the OSTs. What cannot go that way -- inline
 * files (bytes in the MDT entry), an OST without leases, a buffer the NIC
 * could not register -- bounces through host memory with the runtime's
 * copy (cuMemcpy), counted separately so a benchmark shows which it got. */
static atomic_ullong g_dev_direct, g_dev_bounced;
void fs_dev_stats(uint64_t *direct, uint64_t *bounced) {
    *direct = atomic_load(&g_dev_direct);
    *bounced = atomic_load(&g_dev_bounced);
}

typedef struct {
    group_t *g;
    const char *path;
    const attr_t *a;
    uint64_t idx, lo, hi;
    const rp_devbuf_t *b;
    size_t boff;
} dev_read_t;

static void dev_read_task(void *arg) {
    dev_read_t *t = arg;
    char key[4200];
    stripe_key(key, sizeof(key), t->path, t->idx);
    rp_peer_t *peer = &g_osts[ost_for_stripe(t->a, t->idx)];
    size_t want = (size_t)(t->hi - t->lo), got = 0;
    uint8_t *dst = (uint8_t *)t->b->dev + t->boff;
    int rc = -EAGAIN;
    if (t->b->mem) {
        rc = direct_read_mem(peer, key, t->lo, want, dst, t->b->mem, &got);
        if (rc == 0) atomic_fetch_add(&g_dev_direct, got);
    }
    if (rc == -EAGAIN) {
        rp_resp_t r;
        uint64_t ver;
        got = 0;
        rc = rp_ost_read_at(peer, key, t->lo, (uint32_t)want, 0, &ver, &r);
        if (rc == 0) {
            got = r.vlen - 8 < want ? r.vlen - 8 : want;
            if (got) rc = t->b->ops->to_dev(dst, r.val + 8, got, t->b->ops->ctx);
            rp_resp_free(&r);
            atomic_fetch_add(&g_dev_bounced, got);
        }
    }
    if (rc == -ENOENT) rc = 0;  /* a hole */
    if (rc == 0 && got < want) rc = t->b->ops->zero(dst + got, want - got, t->b->ops->ctx);
    group_done(t->g, rc);
}

ssize_t fs_read_dev(const char *path, uint64_t off, size_t len, const rp_devbuf_t *b, size_t boff) {
    if (boff > b->len || len > b->len - boff) return -EINVAL;
    attr_t a;
    int rc = fs_getattr(path, &a);
    if (rc) return rc;
    if (a.is_dir) return -EISDIR;
    if (off >= a.size || !len) return 0;
    if (len > a.size - off) len = (size_t)(a.size - off);
    if (a.inl) {  /* Data-on-MDT: the bytes come with the entry, into host memory */
        rp_resp_t r;
        const uint8_t *d;
        size_t n;
        rc = mdt_inline_read(path, 0, off, (uint32_t)(len > UINT32_MAX ? UINT32_MAX : len), &a, &r, &d, &n);
        if (rc) return rc;
        if (a.inl) {
            if (n > len) n = len;
            rc = n ? b->ops->to_dev((uint8_t *)b->dev + boff, d, n, b->ops->ctx) : 0;
            rp_resp_free(&r);
            atomic_fetch_add(&g_dev_bounced, n);
            return rc ? rc : (ssize_t)n;
        }
        rp_resp_free(&r);  /* spilled meanwhile: read the stripes */
        ac_del(path);
        if ((rc = fs_getattr(path, &a)) != 0) return rc;
        if (off >= a.size) return 0;
        if (len > a.size - off) len = (size_t)(a.size - off);
    }
    if (!a.has_layout || !a.ss) return -EIO;
    uint64_t end = off + len, first = off / a.ss, last = (end - 1) / a.ss;
    int n = (int)(last - first + 1);
    dev_read_t *tasks = calloc((size_t)n, sizeof(*tasks));
    void **args = xmalloc((size_t)n * sizeof(void *));
    group_t g;
    group_init(&g, n);
    for (int i = 0; i < n; i++) {
        uint64_t idx = first + (uint64_t)i, s0 = idx * a.ss;
        uint64_t lo = off > s0 ? off : s0, hi = end < s0 + a.ss ? end : s0 + a.ss;
        tasks[i] = (dev_read_t){&g, path, &a, idx, lo - s0, hi - s0, b, boff + (size_t)(lo - off)};
        args[i] = &tasks[i];
    }
    rc = run_all(dev_read_task, args, n, &g);
    free(tasks);
    free(args);
    return rc ? rc : (ssize_t)len;
}

/* through host memory: the device range copied out in pieces, written as usual */
static ssize_t write_dev_bounced(const char *path, uint64_t off, size_t len, const rp_devbuf_t *b, size_t boff) {
    size_t chunk = 16u << 20, done = 0;
    uint8_t *tmp = rp_bulk_alloc(len < chunk ? len : chunk);
    if (!tmp) return -ENOMEM;
    int rc = 0;
    while (done < len && !rc) {
        size_t n = len - done < chunk ? len - done : chunk;
        rc = b->ops->from_dev(tmp, (const uint8_t *)b->dev + boff + done, n, b->ops->ctx);
        if (!rc) rc = fs_write_range(path, off + done, tmp, n);
        if (!rc) { done += n; atomic_fetch_add(&g_dev_bounced, n); }
    }
    rp_bulk_free(tmp);
    return rc ? rc : (ssize_t)len;
}

ssize_t fs_write_dev(const char *path, uint64_t off, size_t len, const rp_devbuf_t *b, size_t boff) {
    if (boff > b->len || len > b->len - boff) return -EINVAL;
    if (!len) return 0;
    attr_t a;
    int rc = fs_getattr(path, &a);
    if (rc == -ENOENT) rc = fs_create_file(path, &a);
    if (rc) return rc;
    if (a.is_dir) return -EISDIR;
    if (!b->mem || (a.inl && off + len <= inline_hint(&a)))
        return write_dev_bounced(path, off, len, b, boff);  /* small: into the MDT entry */
    /* striped: one segment per stripe, read by each OST out of the device
     * (fs_write_segs spills an inline file first: device bytes never go
     * through the CPU) */
    wseg_t seg = { (const uint8_t *)b->dev + boff, len, b->mem };
    rc = fs_write_segs(path, off, &seg, 1);
    if (rc == 0) { atomic_fetch_add(&g_dev_direct, len); return (ssize_t)len; }
    /* the NIC could not reach the device memory (its registration failed on
     * this host): the same bytes again through host memory -- rewriting the
     * range is idempotent */
    if (rc == -EIO || rc == -EFAULT) return write_dev_bounced(path, off, len, b, boff);
    return rc;
}

/* ---------------------------------------------------------------- stripe read cache
 * Per open file (fs/posix_api.py StripeReadCache): whole stripes, LRU, with
 * readahead into the stripes that follow. Refcounted: readahead tasks may
 * outlive the handle. clear() bumps the generation so in-flight fetches
 * from before a write are dropped. */

scache_t *sc_new(const char *path, const attr_t *a) {
    scache_t *c = calloc(1, sizeof(*c));
    pthread_mutex_init(&c->lk, NULL);
    pthread_cond_init(&c->cv, NULL);
    atomic_init(&c->refs, 1);
    c->n = cfg.scache_slots > 0 ? cfg.scache_slots : SCACHE_SLOTS;
    c->s = xmalloc(sizeof(sc_slot) * (size_t)c->n);
    memset(c->s, 0, sizeof(sc_slot) * (size_t)c->n);
    for (int i = 0; i < c->n; i++) c->s[i].idx = -1;
    c->path = xstrdup(path);
    c->a = *a;
    return c;
}

void sc_unref(scache_t *c) {
    if (atomic_fetch_sub(&c->refs, 1) != 1) return;
    for (int i = 0; i < c->n; i++) rp_bulk_free(c->s[i].data);
    free(c->s);
    free(c->path);
    pthread_mutex_destroy(&c->lk);
    pthread_cond_destroy(&c->cv);
    free(c);
}

void sc_clear(scache_t *c) {
    pthread_mutex_lock(&c->lk);
    c->gen++;
    for (int i = 0; i < c->n; i++) {
        rp_bulk_free(c->s[i].data);
        c->s[i].data = NULL;
        c->s[i].idx = -1;
        c->s[i].state = 0;
    }
    pthread_cond_broadcast(&c->cv);
    pthread_mutex_unlock(&c->lk);
}

static sc_slot *sc_find(scache_t *c, int64_t idx) {
    for (int i = 0; i < c->n; i++)
        if (c->s[i].idx == idx) return &c->s[i];
    return NULL;
}

/* caller holds lk: a slot to mark in flight for idx, evicting the LRU ready
 * one; NULL if every slot is in flight (then fetch without caching) */
static sc_slot *sc_claim(scache_t *c, int64_t idx) {
    sc_slot *victim = NULL;
    for (int i = 0; i < c->n; i++) {
        sc_slot *s = &c->s[i];
        if (s->idx == -1) { victim = s; break; }
        if (s->state == 2 && (!victim || s->lru < victim->lru)) victim = s;
    }
    if (!victim) return NULL;
    rp_bulk_free(victim->data);
    victim->data = NULL;
    victim->len = 0;
    victim->idx = idx;
    victim->state = 1;
    victim->gen = c->gen;
    return victim;
}

/* fetch stripe idx of path (hole -> empty) */
static int fetch_stripe(const char *path, const attr_t *a, uint64_t idx, uint8_t **data, size_t *len) {
    char key[4200];
    stripe_key(key, sizeof(key), path, idx);
    rp_peer_t *peer = &g_osts[ost_for_stripe(a, idx)];
    if (cfg.direct_read && !atomic_load(&peer->no_lease)) {
        /* whole object, one-sided: the lease says how big it is */
        uint8_t *buf = rp_bulk_alloc(a->ss);
        if (buf) {
            size_t got = 0;
            int rc = direct_read(peer, key, 0, (size_t)a->ss, buf, &got);
            if (rc == 0) { *data = buf; *len = got; return 0; }
            rp_bulk_free(buf);
            if (rc == -ENOENT) { *data = NULL; *len = 0; return 0; }
        }
    }
    resp_t r;
    int rc = ost_get(peer, key, &r);
    if (rc == -ENOENT) { *data = NULL; *len = 0; return 0; }
    if (rc) return rc;
    *data = xmalloc(r.vlen);
    memcpy(*data, r.val, r.vlen);
    *len = r.vlen;
    resp_free(&r);
    return 0;
}

static void sc_store(scache_t *c, int64_t idx, uint64_t gen, uint8_t *data, size_t len, int ok) {
    pthread_mutex_lock(&c->lk);
    sc_slot *s = sc_find(c, idx);
    if (s && s->state == 1 && s->gen == gen && gen == c->gen) {
        if (ok) {
            s->data = data;
            s->len = len;
            s->state = 2;
            s->lru = ++c->tick;
            data = NULL;
        } else {
            s->idx = -1;  /* failed: let a reader retry and see the error */
            s->state = 0;
        }
    }
    pthread_cond_broadcast(&c->cv);
    pthread_mutex_unlock(&c->lk);
    rp_bulk_free(data);
}

typedef struct {
    scache_t *c;
    int64_t idx;
    uint64_t gen;
    group_t *g;   /* NULL for readahead */
} sc_fetch_t;

static void sc_fetch_task(void *arg) {
    sc_fetch_t *f = arg;
    uint8_t *d = NULL;
    size_t n = 0;
    pthread_mutex_lock(&f->c->lk);
    attr_t a = f->c->a;  /* the layout never changes; the size may */
    pthread_mutex_unlock(&f->c->lk);
    int rc = fetch_stripe(f->c->path, &a, (uint64_t)f->idx, &d, &n);
    sc_store(f->c, f->idx, f->gen, d, n, rc == 0);
    if (f->g) {
        group_done(f->g, rc);
    } else {
        sc_unref(f->c);
        free(f);
    }
}

/* Serve [off, off+len) of the file into out (len already clamped to size). */
int sc_read(scache_t *c, const attr_t *a, uint64_t off, size_t len, uint8_t *out) {
    uint64_t end = off + len, first = off / a->ss, last = (end - 1) / a->ss;
    int n = (int)(last - first + 1);
    sc_fetch_t *fs = calloc((size_t)n, sizeof(*fs));
    void **args = xmalloc((size_t)n * sizeof(void *));
    uint8_t **direct = calloc((size_t)n, sizeof(uint8_t *));  /* uncached fetches */
    size_t *dlen = calloc((size_t)n, sizeof(size_t));
    int nfetch = 0, rc = 0;
    group_t g;
    /* 1. claim what is missing */
    pthread_mutex_lock(&c->lk);
    for (int i = 0; i < n; i++) {
        int64_t idx = (int64_t)(first + (uint64_t)i);
        sc_slot *s = sc_find(c, idx);
        if (s) continue;
        s = sc_claim(c, idx);
        if (!s) continue;  /* every slot busy: step 3 fetches it directly */
        fs[nfetch] = (sc_fetch_t){c, idx, c->gen, &g};
        args[nfetch] = &fs[nfetch];
        nfetch++;
    }
    pthread_mutex_unlock(&c->lk);
    /* 2. fetch them in parallel */
    if (nfetch) {
        group_init(&g, nfetch);
        rc = run_all(sc_fetch_task, args, nfetch, &g);
    }
    /* 3. assemble, waiting for stripes someone else (readahead) is fetching */
    for (int i = 0; i < n && rc == 0; i++) {
        uint64_t idx = first + (uint64_t)i, s0 = idx * a->ss;
        uint64_t lo = off > s0 ? off : s0, hi = end < s0 + a->ss ? end : s0 + a->ss;
        uint8_t *dst = out + (lo - off);
        pthread_mutex_lock(&c->lk);
        sc_slot *s;
        for (;;) {
            s = sc_find(c, (int64_t)idx);
            if (!s || s->state == 2) break;
            pthread_cond_wait(&c->cv, &c->lk);
        }
        if (s) {
            s->lru = ++c->tick;
            size_t have = s->len > lo - s0 ? s->len - (lo - s0) : 0;
            size_t want = hi - lo, take = have < want ? have : want;
            if (take) memcpy(dst, s->data + (lo - s0), take);
            if (take < want) memset(dst + take, 0, want - take);
            pthread_mutex_unlock(&c->lk);
            continue;
        }
        pthread_mutex_unlock(&c->lk);
        /* not cacheable (all slots busy) or dropped by a clear: fetch directly */
        rc = fetch_stripe(c->path, a, idx, &direct[i], &dlen[i]);
        if (rc) break;
        size_t have = dlen[i] > lo - s0 ? dlen[i] - (lo - s0) : 0;
        size_t want = hi - lo, take = have < want ? have : want;
        if (take) memcpy(dst, direct[i] + (lo - s0), take);
        if (take < want) memset(dst + take, 0, want - take);
    }
    for (int i = 0; i < n; i++) rp_bulk_free(direct[i]);
    free(direct);
    free(dlen);
    free(fs);
    free(args);
    if (rc) return rc;
    /* 4. readahead the following stripes (they live on other OSTs) */
    int ra = cfg.readahead < 0 ? 2 * cfg.nost : cfg.readahead;
    if (ra > c->n - 1) ra = c->n - 1;  /* never more in flight than the cache can hold */
    if (ra > 0 && a->size) {
        uint64_t last_stripe = (a->size - 1) / a->ss;
        pthread_mutex_lock(&c->lk);
        for (uint64_t idx = last + 1; idx <= last_stripe && idx <= last + (uint64_t)ra; idx++) {
            if (sc_find(c, (int64_t)idx)) continue;
            sc_slot *s = sc_claim(c, (int64_t)idx);
            if (!s) break;
            sc_fetch_t *f = xmalloc(sizeof(*f));
            *f = (sc_fetch_t){c, (int64_t)idx, c->gen, NULL};
            atomic_fetch_add(&c->refs, 1);
            pool_submit(sc_fetch_task, f);
        }
        pthread_mutex_unlock(&c->lk);
    }
    return 0;
}

/* ---------------------------------------------------------------- handles */

/* open file handles, so a truncate through any path/fd drops every open
 * handle's cached stripes of that file */
static handle_t **g_open;
static size_t g_nopen, g_capopen;
static pthread_mutex_t g_open_lk = PTHREAD_MUTEX_INITIALIZER;

void open_register(handle_t *h) {
    pthread_mutex_lock(&g_open_lk);
    if (g_nopen == g_capopen) {
        g_capopen = g_capopen ? 2 * g_capopen : 256;
        g_open = realloc(g_open, g_capopen * sizeof(*g_open));
    }
    g_open[g_nopen++] = h;
    pthread_mutex_unlock(&g_open_lk);
}

static void open_unregister(handle_t *h) {
    pthread_mutex_lock(&g_open_lk);
    for (size_t i = 0; i < g_nopen; i++)
        if (g_open[i] == h) { g_open[i] = g_open[--g_nopen]; break; }
    pthread_mutex_unlock(&g_open_lk);
}

void invalidate_open_caches(const char *path) {
    pthread_mutex_lock(&g_open_lk);
    for (size_t i = 0; i < g_nopen; i++)
        if (g_open[i]->sc && !strcmp(g_open[i]->path, path)) sc_clear(g_open[i]->sc);
    pthread_mutex_unlock(&g_open_lk);
}

handle_t *handle_new(const char *path, int is_dir) {
    handle_t *h = calloc(1, sizeof(*h));
    h->is_dir = is_dir;
    h->path = xstrdup(path);
    pthread_mutex_init(&h->lk, NULL);
    return h;
}

void handle_free(handle_t *h) {
    if (h->sc) open_unregister(h);
    for (size_t i = 0; i < h->ndirty; i++) rp_bulk_free(h->dirty[i].base ? h->dirty[i].base : h->dirty[i].data);
    free(h->dirty);
    for (size_t i = 0; i < h->ndents; i++) free(h->dents[i].name);
    free(h->dents);
    if (h->sc) sc_unref(h->sc);
    free(h->path);
    pthread_mutex_destroy(&h->lk);
    free(h);
}

/* caller holds h->lk. Coalesce this fd's writes into runs (in write order
 * for overlaps: later writes win) and send each run with one striped
 * write_range, like fs/fuse_lowlevel.py _flush_handle. */
int handle_flush(handle_t *h) {
    if (!h->ndirty) return 0;
    dirty_t *v = h->dirty;
    size_t n = h->ndirty;
    /* stable merge sort of indices by offset */
    size_t *order = xmalloc(n * sizeof(size_t));
    for (size_t i = 0; i < n; i++) order[i] = i;
    /* simple stable merge sort on indices */
    size_t *tmp = xmalloc(n * sizeof(size_t));
    for (size_t w = 1; w < n; w *= 2)
        for (size_t lo = 0; lo < n; lo += 2 * w) {
            size_t mid = lo + w < n ? lo + w : n, hi = lo + 2 * w < n ? lo + 2 * w : n;
            size_t i = lo, j = mid, k = lo;
            while (i < mid && j < hi) tmp[k++] = v[order[j]].off < v[order[i]].off ? order[j++] : order[i++];
            while (i < mid) tmp[k++] = order[i++];
            while (j < hi) tmp[k++] = order[j++];
            memcpy(order + lo, tmp + lo, (hi - lo) * sizeof(size_t));
        }
    free(tmp);
    int rc = 0;
    size_t i = 0;
    while (i < n && rc == 0) {
        uint64_t run_start = v[order[i]].off, run_end = run_start + v[order[i]].len;
        size_t j = i + 1;
        while (j < n && v[order[j]].off <= run_end) {
            uint64_t e = v[order[j]].off + v[order[j]].len;
            if (e > run_end) run_end = e;
            j++;
        }
        size_t rl = (size_t)(run_end - run_start);
        size_t cnt = j - i;
        int clean = 1;  /* pieces exactly adjacent, no overlap: send them where they are */
        for (size_t a = i + 1; a < j && clean; a++)
            if (v[order[a]].off != v[order[a - 1]].off + v[order[a - 1]].len) clean = 0;
        if (clean) {
            wseg_t *segs = xmalloc(cnt * sizeof(*segs));
            for (size_t a = 0; a < cnt; a++) segs[a] = (wseg_t){ v[order[i + a]].data, v[order[i + a]].len, NULL };
            rc = fs_write_segs(h->path, run_start, segs, (int)cnt);
            free(segs);
        } else {
            /* overlapping writes: apply in original order so a later one
             * wins, into arena memory (read in place by the OSTs) */
            uint8_t *run = rp_bulk_alloc(rl);
            if (!run) run = xmalloc(rl);
            memset(run, 0, rl);
            size_t *byorig = xmalloc(cnt * sizeof(size_t));
            memcpy(byorig, order + i, cnt * sizeof(size_t));
            for (size_t a = 1; a < cnt; a++) {  /* sort the run's members by original index */
                size_t x = byorig[a], b = a;
                while (b > 0 && byorig[b - 1] > x) { byorig[b] = byorig[b - 1]; b--; }
                byorig[b] = x;
            }
            for (size_t a = 0; a < cnt; a++) {
                dirty_t *d = &v[byorig[a]];
                memcpy(run + (d->off - run_start), d->data, d->len);
            }
            free(byorig);
            rc = fs_write_range(h->path, run_start, run, rl);
            rp_bulk_free(run);
        }
        if (rc == 0) i = j;
    }
    /* Runs order[0..i) were written; anything from the failed run on stays
     * buffered (in original order), so FLUSH/RELEASE retries it and reports
     * the error at close instead of the data vanishing silently. */
    uint8_t *keep = calloc(n, 1);
    for (size_t k = i; k < n; k++) keep[order[k]] = 1;
    free(order);
    size_t w = 0;
    h->dirty_bytes = 0;
    for (size_t k = 0; k < n; k++) {
        if (keep[k]) { v[w++] = v[k]; h->dirty_bytes += v[k].len; }
        else rp_bulk_free(v[k].base ? v[k].base : v[k].data);
    }
    free(keep);
    h->ndirty = w;
    if (h->sc) sc_clear(h->sc);
    return rc;
}


/* ---------------------------------------------------------------- handle I/O
 * The library's own read/write through a handle (the interception library
 * uses these; the FUSE daemon has zero-copy variants of the same logic). */
int handle_write(handle_t *h, uint64_t off, const uint8_t *data, size_t len, uint8_t *base) {
    if (h->ndirty == h->capdirty) {
        h->capdirty = h->capdirty ? 2 * h->capdirty : 64;
        h->dirty = xrealloc(h->dirty, h->capdirty * sizeof(dirty_t));
    }
    dirty_t *d = &h->dirty[h->ndirty++];
    d->off = off;
    d->len = len;
    if (base && len >= FUSE_BUF_SIZE / 2) {
        d->data = (uint8_t *)data;
        d->base = base;
    } else {
        d->data = rp_bulk_alloc(len);
        if (!d->data) d->data = xmalloc(len);
        memcpy(d->data, data, len);
        d->base = NULL;
        if (base) rp_bulk_free(base);
    }
    h->dirty_bytes += len;
    return h->dirty_bytes >= DIRTY_CAP ? handle_flush(h) : 0;  /* bound memory per handle */
}

ssize_t handle_read(handle_t *h, uint64_t off, uint8_t *out, size_t len) {
    int rc = handle_flush(h);  /* same-fd read-after-write */
    attr_t a;
    if (!rc) rc = fs_getattr(h->path, &a);
    if (!rc && a.is_dir) rc = -EISDIR;
    if (rc) return rc;
    if (a.inl) {  /* Data-on-MDT: one MDT round trip, which also says if it is still inline */
        resp_t r;
        const uint8_t *d;
        size_t n;
        rc = mdt_inline_read(h->path, 0, off, (uint32_t)(len > UINT32_MAX ? UINT32_MAX : len), &a, &r, &d, &n);
        if (rc) return rc;
        ac_put(h->path, &a);
        if (a.inl) {
            if (n > len) n = len;
            memcpy(out, d, n);
            resp_free(&r);
            h->next_off = off + n;
            return (ssize_t)n;
        }
        resp_free(&r);  /* spilled meanwhile: read the stripes */
    }
    if (off >= a.size) return 0;
    if (off + len > a.size) len = (size_t)(a.size - off);
    if (!a.has_layout || !a.ss) return -EIO;
    if (off != h->next_off && len < a.ss) {
        rc = fs_read_direct(h->path, &a, off, len, out);  /* random/strided: exact bytes only */
    } else {  /* sequential: whole-stripe cache + readahead */
        if (!h->sc) h->sc = sc_new(h->path, &a);
        pthread_mutex_lock(&h->sc->lk);
        h->sc->a = a;
        pthread_mutex_unlock(&h->sc->lk);
        rc = sc_read(h->sc, &a, off, len, out);
    }
    if (rc) return rc;
    h->next_off = off + len;
    return (ssize_t)len;
}

/* ---------------------------------------------------------------- init */
int rpfs_init(void) {
    const char *ie = getenv("ROCEPOOL_CLIENT_INLINE");
    if (ie && !strcmp(ie, "0")) cfg.inline_ok = 0;
    const char *dc = getenv("ROCEPOOL_DIRTY_CAP_MIB");
    if (dc && atol(dc) > 0) g_dirty_cap = (size_t)atol(dc) << 20;
    const char *ds = getenv("ROCEPOOL_DIR_STRIPE");
    if (ds && (!strcmp(ds, "0") || !strcmp(ds, "false") || !strcmp(ds, "no") || !strcmp(ds, "off"))) cfg.dir_stripe = 0;
    /* per-RPC reply timeout (60 s, rocepool_client.c): a request that gets no reply in time
     * fails the operation (EIO), it is never re-sent blindly. The
     * simulated cluster oversubscribes its host and raises it. */
    const char *to = getenv("ROCEPOOL_RPC_TIMEOUT_MS");
    if (to && atoi(to) > 0) rp_timeout_ms = atoi(to);
    const char *dr = getenv("ROCEPOOL_DIRECT_READ");
    if (dr && (!strcmp(dr, "0") || !strcmp(dr, "off"))) cfg.direct_read = 0;
    const char *dl = getenv("ROCEPOOL_DIRECT_LOOKUP");
    if (dl && *dl) g_direct_lookup = strcmp(dl, "0") && strcmp(dl, "off");
    const char *ss = getenv("ROCEPOOL_SCACHE_SLOTS");
    if (ss && atoi(ss) > 0) cfg.scache_slots = atoi(ss);
    if (cfg.nmdt < 1 || cfg.nost < 1) return -EINVAL;
    if (cfg.threads < 1) cfg.threads = 1;
    if (cfg.io_threads < 1) cfg.io_threads = 1;
    if (!cfg.stripe_size) cfg.stripe_size = 1u << 20;
    for (int i = 0; i < cfg.nmdt; i++) peer_init(&g_mdts[i], cfg.mdt[i]);
    for (int i = 0; i < cfg.nost; i++) peer_init(&g_osts[i], cfg.ost[i]);
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    for (int i = 0; i < cfg.io_threads; i++) {
        pthread_t t;
        if (pthread_create(&t, &at, pool_worker, NULL)) { pthread_attr_destroy(&at); return -EAGAIN; }
    }
    pthread_attr_destroy(&at);
    return 0;
}
