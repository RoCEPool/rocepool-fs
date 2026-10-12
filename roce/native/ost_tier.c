/* See ost_tier.h. */
#define _GNU_SOURCE
#include "ost_tier.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef ROCEPOOL_SPDK
#include <spdk/env.h>
#include <spdk/nvme.h>
#endif

/* ---------------------------------------------------------------- slabs */
#define N_CLASSES 7
static const size_t CLASS_BYTES[N_CLASSES] = {4u << 10, 16u << 10, 64u << 10, 256u << 10, 1u << 20, 4u << 20, 16u << 20};
#define ZONE_BYTES (64u << 20)

size_t ost_tier_class_bytes(uint32_t cls) { return cls < N_CLASSES ? CLASS_BYTES[cls] : 0; }
uint32_t ost_tier_class_for(size_t size) {
    for (uint32_t c = 0; c < N_CLASSES; c++) if (size <= CLASS_BYTES[c]) return c;
    return UINT32_MAX;
}

typedef struct { uint64_t *offs; size_t n, cap; } freelist_t;

struct ost_tier {
    char kind[8];
    uint64_t capacity, used;
    pthread_mutex_t lock;        /* allocator */
    uint64_t next_zone;          /* bump pointer over the device */
    freelist_t free[N_CLASSES];
    /* file backend */
    int fd, dfd;                 /* buffered and O_DIRECT descriptors (dfd < 0: no direct I/O) */
    uint32_t lba;                /* device sector size */
#ifdef ROCEPOOL_SPDK
    struct spdk_nvme_ctrlr *ctrlr;
    struct spdk_nvme_ns *ns;
    char traddr[64];
#endif
};

static int fl_push(freelist_t *f, uint64_t off) {
    if (f->n == f->cap) {
        size_t nc = f->cap ? f->cap * 2 : 256;
        uint64_t *p = realloc(f->offs, nc * sizeof(*p));
        if (!p) return -ENOMEM;
        f->offs = p;
        f->cap = nc;
    }
    f->offs[f->n++] = off;
    return 0;
}

int ost_tier_alloc(ost_tier_t *t, size_t size, tier_ext_t *ext) {
    uint32_t cls = ost_tier_class_for(size);
    if (cls == UINT32_MAX) return -EFBIG;
    pthread_mutex_lock(&t->lock);
    freelist_t *f = &t->free[cls];
    if (f->n == 0) {
        if (t->next_zone + ZONE_BYTES > t->capacity) { pthread_mutex_unlock(&t->lock); return -ENOSPC; }
        uint64_t z = t->next_zone;
        t->next_zone += ZONE_BYTES;
        size_t sb = CLASS_BYTES[cls];
        /* push in reverse so the zone is handed out front to back */
        for (uint64_t off = z + ZONE_BYTES - sb; ; off -= sb) {
            if (fl_push(f, off)) break;
            if (off == z) break;
        }
        if (f->n == 0) { pthread_mutex_unlock(&t->lock); return -ENOMEM; }
    }
    ext->off = f->offs[--f->n];
    ext->cls = cls;
    t->used += CLASS_BYTES[cls];
    pthread_mutex_unlock(&t->lock);
    return 0;
}

void ost_tier_free(ost_tier_t *t, const tier_ext_t *ext) {
    if (ext->cls >= N_CLASSES) return;
    pthread_mutex_lock(&t->lock);
    fl_push(&t->free[ext->cls], ext->off);  /* on ENOMEM the slab leaks: acceptable */
    t->used -= CLASS_BYTES[ext->cls];
    pthread_mutex_unlock(&t->lock);
}

const char *ost_tier_kind(const ost_tier_t *t) { return t->kind; }
uint64_t ost_tier_capacity(const ost_tier_t *t) { return t->capacity; }
uint64_t ost_tier_used(const ost_tier_t *t) { return t->used; }

/* ------------------------------------------------------------ file backend */
static int file_open(ost_tier_t *t, const char *spec, char *err, size_t err_len) {
    char path[4096];
    uint64_t size_gib = 16;
    const char *colon = strrchr(spec, ':');
    if (colon && colon != spec && strspn(colon + 1, "0123456789") == strlen(colon + 1) && colon[1]) {
        size_gib = strtoull(colon + 1, NULL, 10);
        snprintf(path, sizeof(path), "%.*s", (int)(colon - spec), spec);
    } else {
        snprintf(path, sizeof(path), "%s", spec);
    }
    t->fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (t->fd < 0) { snprintf(err, err_len, "tier: open %s: %s", path, strerror(errno)); return -1; }
    struct stat st;
    if (fstat(t->fd, &st)) { snprintf(err, err_len, "tier: fstat: %s", strerror(errno)); return -1; }
    t->lba = 512;
    if (S_ISBLK(st.st_mode)) {
        uint64_t bytes = 0;
        int lbs = 0;
        if (ioctl(t->fd, BLKGETSIZE64, &bytes)) { snprintf(err, err_len, "tier: BLKGETSIZE64: %s", strerror(errno)); return -1; }
        if (!ioctl(t->fd, BLKSSZGET, &lbs) && lbs > 0) t->lba = (uint32_t)lbs;
        t->capacity = bytes;
    } else {
        t->capacity = size_gib << 30;
        if ((uint64_t)st.st_size < t->capacity && ftruncate(t->fd, (off_t)t->capacity)) {
            snprintf(err, err_len, "tier: ftruncate %s to %llu GiB: %s", path, (unsigned long long)size_gib, strerror(errno));
            return -1;
        }
        if ((uint64_t)st.st_size > t->capacity) t->capacity = (uint64_t)st.st_size;
        /* allocate the blocks now (unwritten extents: no data written, a
         * second or two even for hundreds of GiB): a sparse file makes every
         * first write to a region allocate, under the file system's inode
         * lock -- on site an xfs-on-LVM tier took 50-100 ms per 1 MiB write,
         * seconds at the tail. No room or no support: stays sparse. */
        if (fallocate(t->fd, 0, 0, (off_t)t->capacity))
            fprintf(stderr, "rocepool OST: tier %s: blocks not preallocated (%s): first writes allocate\n", path,
                    strerror(errno));
    }
    t->capacity &= ~(uint64_t)(ZONE_BYTES - 1);
    /* a second descriptor for direct transfers; tmpfs and some file
     * systems refuse O_DIRECT: then everything goes buffered */
    t->dfd = open(path, O_RDWR | O_DIRECT | O_CLOEXEC);
    strcpy(t->kind, "file");
    return 0;
}

static int xpwrite(int fd, const void *buf, size_t len, uint64_t off) {
    const uint8_t *p = buf;
    while (len) {
        ssize_t w = pwrite(fd, p, len, (off_t)off);
        if (w < 0) { if (errno == EINTR) continue; return -errno; }
        if (w == 0) return -EIO;
        p += w; len -= (size_t)w; off += (uint64_t)w;
    }
    return 0;
}
static int xpread(int fd, void *buf, size_t len, uint64_t off) {
    uint8_t *p = buf;
    while (len) {
        ssize_t r = pread(fd, p, len, (off_t)off);
        if (r < 0) { if (errno == EINTR) continue; return -errno; }
        if (r == 0) { memset(p, 0, len); return 0; }  /* past the end of a sparse file: zeros */
        p += r; len -= (size_t)r; off += (uint64_t)r;
    }
    return 0;
}

/* ------------------------------------------------------------ SPDK backend */
#ifdef ROCEPOOL_SPDK
/* One I/O queue pair per server thread; requests complete by polling it
 * (SPDK has no interrupts): a thread submits one command and polls until
 * its completion callback fires. */
static __thread struct spdk_nvme_qpair *tl_qp;
static __thread ost_tier_t *tl_qp_owner;
static __thread void *tl_bounce;
static __thread size_t tl_bounce_len;

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr_opts *opts) {
    (void)opts;
    ost_tier_t *t = cb_ctx;
    return strcmp(trid->traddr, t->traddr) == 0;
}
static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr *ctrlr,
                      const struct spdk_nvme_ctrlr_opts *opts) {
    (void)trid; (void)opts;
    ((ost_tier_t *)cb_ctx)->ctrlr = ctrlr;
}

static int spdk_open(ost_tier_t *t, const char *spec, char *err, size_t err_len) {
    uint32_t nsid = 1;
    const char *colon = strrchr(spec, ':');
    if (colon && colon[1] && strspn(colon + 1, "0123456789") == strlen(colon + 1) && (size_t)(colon - spec) < sizeof(t->traddr)) {
        nsid = (uint32_t)strtoul(colon + 1, NULL, 10);
        snprintf(t->traddr, sizeof(t->traddr), "%.*s", (int)(colon - spec), spec);
    } else {
        snprintf(t->traddr, sizeof(t->traddr), "%s", spec);
    }
    struct spdk_env_opts opts = {0};
    opts.opts_size = sizeof(opts);   /* SPDK >= 23.x: set before spdk_env_opts_init */
    spdk_env_opts_init(&opts);
    opts.name = "rocepool_ost";
    /* several OST processes per host: each its own DPDK shm id */
    opts.shm_id = (int)(getpid() & 0x7fff);
    if (spdk_env_init(&opts) < 0) { snprintf(err, err_len, "tier: spdk_env_init failed (hugepages? vfio?)"); return -1; }
    struct spdk_nvme_transport_id trid = {0};
    spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
    snprintf(trid.traddr, sizeof(trid.traddr), "%s", t->traddr);
    if (spdk_nvme_probe(&trid, t, probe_cb, attach_cb, NULL) || !t->ctrlr) {
        snprintf(err, err_len, "tier: no NVMe controller at %s (bound to vfio-pci/uio?)", t->traddr);
        return -1;
    }
    t->ns = spdk_nvme_ctrlr_get_ns(t->ctrlr, nsid);
    if (!t->ns || !spdk_nvme_ns_is_active(t->ns)) { snprintf(err, err_len, "tier: namespace %u not active", nsid); return -1; }
    t->lba = spdk_nvme_ns_get_sector_size(t->ns);
    t->capacity = spdk_nvme_ns_get_size(t->ns) & ~(uint64_t)(ZONE_BYTES - 1);
    t->fd = t->dfd = -1;
    strcpy(t->kind, "spdk");
    return 0;
}

static struct spdk_nvme_qpair *spdk_qp(ost_tier_t *t) {
    if (!tl_qp || tl_qp_owner != t) {
        tl_qp = spdk_nvme_ctrlr_alloc_io_qpair(t->ctrlr, NULL, 0);
        tl_qp_owner = t;
    }
    return tl_qp;
}
static void io_done(void *arg, const struct spdk_nvme_cpl *cpl) {
    *(int *)arg = spdk_nvme_cpl_is_error(cpl) ? -EIO : 1;
}
/* one command, polled to completion; lba-aligned byte offset and length */
static int spdk_io(ost_tier_t *t, int is_write, uint64_t off, void *buf, size_t len) {
    struct spdk_nvme_qpair *qp = spdk_qp(t);
    if (!qp) return -EIO;
    int done = 0;
    uint64_t lba = off / t->lba;
    uint32_t n = (uint32_t)(len / t->lba);
    int rc = is_write ? spdk_nvme_ns_cmd_write(t->ns, qp, buf, lba, n, io_done, &done, 0)
                      : spdk_nvme_ns_cmd_read(t->ns, qp, buf, lba, n, io_done, &done, 0);
    if (rc) return -EIO;
    while (!done) spdk_nvme_qpair_process_completions(qp, 0);
    return done < 0 ? done : 0;
}
static void *spdk_bounce(size_t len) {
    if (tl_bounce_len < len) {
        spdk_dma_free(tl_bounce);
        tl_bounce = spdk_dma_malloc(len, TIER_PAGE, NULL);
        tl_bounce_len = tl_bounce ? len : 0;
    }
    return tl_bounce;
}
#endif

/* ------------------------------------------------------------- common ops */
ost_tier_t *ost_tier_open(const char *spec, char *err, size_t err_len) {
    ost_tier_t *t = calloc(1, sizeof(*t));
    if (!t) { snprintf(err, err_len, "tier: out of memory"); return NULL; }
    pthread_mutex_init(&t->lock, NULL);
    t->fd = t->dfd = -1;
    int rc;
    if (!strncmp(spec, "file:", 5)) rc = file_open(t, spec + 5, err, err_len);
#ifdef ROCEPOOL_SPDK
    else if (!strncmp(spec, "spdk:", 5)) rc = spdk_open(t, spec + 5, err, err_len);
#else
    else if (!strncmp(spec, "spdk:", 5)) { snprintf(err, err_len, "tier: this build has no SPDK (build.sh with SPDK_DIR=...)"); rc = -1; }
#endif
    else { snprintf(err, err_len, "tier: unknown backend in %s (file:PATH[:GIB] | spdk:TRADDR[:NSID])", spec); rc = -1; }
    if (rc) { ost_tier_close(t); return NULL; }
    if (t->capacity < ZONE_BYTES) { snprintf(err, err_len, "tier: device smaller than one 64 MiB zone"); ost_tier_close(t); return NULL; }
    return t;
}

void ost_tier_close(ost_tier_t *t) {
    if (!t) return;
    if (t->fd >= 0) close(t->fd);
    if (t->dfd >= 0) close(t->dfd);
    for (int i = 0; i < N_CLASSES; i++) free(t->free[i].offs);
    free(t);
}

int ost_tier_direct(const ost_tier_t *t) {
#ifdef ROCEPOOL_SPDK
    if (t->ns) return 1;
#endif
    return t->dfd >= 0;
}

static int bounds(const ost_tier_t *t, const tier_ext_t *ext, uint64_t off, size_t len) {
    (void)t;
    if (ext->cls >= N_CLASSES || off + len > CLASS_BYTES[ext->cls]) return -EINVAL;
    return 0;
}

int ost_tier_write(ost_tier_t *t, const tier_ext_t *ext, uint64_t off, const void *buf, size_t len) {
    int rc = bounds(t, ext, off, len);
    if (rc || !len) return rc;
#ifdef ROCEPOOL_SPDK
    if (t->ns) {
        /* round to LBAs; the edges come from the device (read-modify-write)
         * through a DMA bounce buffer */
        uint64_t s = off & ~(uint64_t)(t->lba - 1), e = (off + len + t->lba - 1) & ~(uint64_t)(t->lba - 1);
        uint8_t *b = spdk_bounce(e - s);
        if (!b) return -ENOMEM;
        if (s != off && (rc = spdk_io(t, 0, ext->off + s, b, t->lba))) return rc;
        if (e != off + len && e - s > t->lba && (rc = spdk_io(t, 0, ext->off + e - t->lba, b + (e - s) - t->lba, t->lba))) return rc;
        memcpy(b + (off - s), buf, len);
        return spdk_io(t, 1, ext->off + s, b, e - s);
    }
#endif
    return xpwrite(t->fd, buf, len, ext->off + off);
}

int ost_tier_read(ost_tier_t *t, const tier_ext_t *ext, uint64_t off, void *buf, size_t len) {
    int rc = bounds(t, ext, off, len);
    if (rc || !len) return rc;
#ifdef ROCEPOOL_SPDK
    if (t->ns) {
        uint64_t s = off & ~(uint64_t)(t->lba - 1), e = (off + len + t->lba - 1) & ~(uint64_t)(t->lba - 1);
        uint8_t *b = spdk_bounce(e - s);
        if (!b) return -ENOMEM;
        if ((rc = spdk_io(t, 0, ext->off + s, b, e - s))) return rc;
        memcpy(buf, b + (off - s), len);
        return 0;
    }
#endif
    return xpread(t->fd, buf, len, ext->off + off);
}

static int aligned(const void *buf, uint64_t off, size_t len) {
    return ((uintptr_t)buf % TIER_PAGE) == 0 && off % TIER_PAGE == 0 && len % TIER_PAGE == 0;
}

int ost_tier_write_direct(ost_tier_t *t, const tier_ext_t *ext, uint64_t off, const void *buf, size_t len) {
    int rc = bounds(t, ext, off, len);
    if (rc || !len) return rc;
    if (!aligned(buf, off, len)) return -EINVAL;
#ifdef ROCEPOOL_SPDK
    if (t->ns) return spdk_io(t, 1, ext->off + off, (void *)buf, len);
#endif
    if (t->dfd < 0) return xpwrite(t->fd, buf, len, ext->off + off);
    rc = xpwrite(t->dfd, buf, len, ext->off + off);
    return rc == -EINVAL ? xpwrite(t->fd, buf, len, ext->off + off) : rc;  /* the fs refused direct I/O after all */
}

int ost_tier_read_direct(ost_tier_t *t, const tier_ext_t *ext, uint64_t off, void *buf, size_t len) {
    int rc = bounds(t, ext, off, len);
    if (rc || !len) return rc;
    if (!aligned(buf, off, len)) return -EINVAL;
#ifdef ROCEPOOL_SPDK
    if (t->ns) return spdk_io(t, 0, ext->off + off, buf, len);
#endif
    if (t->dfd < 0) return xpread(t->fd, buf, len, ext->off + off);
    rc = xpread(t->dfd, buf, len, ext->off + off);
    return rc == -EINVAL ? xpread(t->fd, buf, len, ext->off + off) : rc;
}

void *ost_tier_buf_alloc(ost_tier_t *t, size_t size) {
#ifdef ROCEPOOL_SPDK
    if (t->ns) return spdk_dma_malloc(size ? size : 1, TIER_PAGE, NULL);
#endif
    (void)t;
    return malloc(size ? size : 1);
}
void ost_tier_buf_free(ost_tier_t *t, void *p, size_t size) {
    (void)size;
#ifdef ROCEPOOL_SPDK
    if (t->ns) { spdk_dma_free(p); return; }
#endif
    (void)t;
    free(p);
}
void ost_tier_notify_chunk(ost_tier_t *t, void *base, size_t len) {
#ifdef ROCEPOOL_SPDK
    if (t->ns) spdk_mem_register(base, len);
    return;
#endif
    (void)t; (void)base; (void)len;
}
