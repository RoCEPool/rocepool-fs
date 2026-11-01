/* See ost_tier.h. */
#define _GNU_SOURCE
#include "ost_tier.h"

#include <errno.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <pthread.h>
#include <stdatomic.h>
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
    freelist_t free[N_CLASSES];  /* slabs freed (written before) */
    freelist_t zfree[N_CLASSES]; /* slabs freed and zeroed again (fallocate ZERO_RANGE: read zeros) */
    uint64_t bump[N_CLASSES], bump_end[N_CLASSES];  /* the class's zone being carved, never handed out */
    /* offsets from here on were never written: the tier file was empty
     * when opened (fallocated / sparse: reads zeros). capacity: nothing
     * known (an existing file, a device, after a restore) */
    uint64_t zero_from;
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
    size_t sb = CLASS_BYTES[cls];
    if (t->zfree[cls].n) {  /* freed, zeroed again: reads zeros */
        ext->off = t->zfree[cls].offs[--t->zfree[cls].n];
        ext->zero_tail = 1;
    } else if (f->n) {  /* a slab freed earlier: written before */
        ext->off = f->offs[--f->n];
        ext->zero_tail = 0;
    } else {
        if (t->bump[cls] + sb > t->bump_end[cls]) {  /* a new zone for the class, carved front to back */
            if (t->next_zone + ZONE_BYTES > t->capacity) { pthread_mutex_unlock(&t->lock); return -ENOSPC; }
            t->bump[cls] = t->next_zone;
            t->bump_end[cls] = t->next_zone + ZONE_BYTES;
            t->next_zone += ZONE_BYTES;
        }
        ext->off = t->bump[cls];
        t->bump[cls] += sb;
        ext->zero_tail = ext->off >= t->zero_from;
    }
    ext->cls = cls;
    t->used += CLASS_BYTES[cls];
    pthread_mutex_unlock(&t->lock);
    return 0;
}

void ost_tier_free(ost_tier_t *t, const tier_ext_t *ext) {
    if (ext->cls >= N_CLASSES) return;
    /* a file tier's slab made to read zeros again -- a metadata change, its
     * blocks stay allocated (unwritten): the next object on it skips the
     * gaps its writes leave, as on a slab never written. The harness's
     * client probe frees slabs before the run: without this the run's
     * objects landed on them and every gap was written. */
    int zeroed = !strcmp(t->kind, "file") && t->fd >= 0 &&
                 fallocate(t->fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE, (off_t)ext->off,
                           (off_t)CLASS_BYTES[ext->cls]) == 0;
    pthread_mutex_lock(&t->lock);
    fl_push(zeroed ? &t->zfree[ext->cls] : &t->free[ext->cls], ext->off);  /* on ENOMEM the slab leaks: acceptable */
    t->used -= CLASS_BYTES[ext->cls];
    pthread_mutex_unlock(&t->lock);
}

int ost_tier_sync_fd(const ost_tier_t *t) { return strcmp(t->kind, "file") ? -1 : t->fd; }

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

int ost_tier_restore(ost_tier_t *t, const tier_ext_t *used, size_t n) {
    pthread_mutex_lock(&t->lock);
    for (int c = 0; c < N_CLASSES; c++) t->free[c].n = t->zfree[c].n = 0;
    for (int c = 0; c < N_CLASSES; c++) t->bump[c] = t->bump_end[c] = 0;
    t->used = 0;
    t->next_zone = 0;
    /* what the last life wrote past its logged objects is unknown */
    t->zero_from = t->capacity;
    if (!n) { pthread_mutex_unlock(&t->lock); return 0; }
    uint64_t nz = 0;
    for (size_t i = 0; i < n; i++) {
        if (used[i].cls >= N_CLASSES || used[i].off + CLASS_BYTES[used[i].cls] > t->capacity) {
            pthread_mutex_unlock(&t->lock);
            return -EINVAL;
        }
        uint64_t z = used[i].off / ZONE_BYTES + 1;
        if (z > nz) nz = z;
    }
    int8_t *zcls = malloc(nz);
    uint64_t *offs = malloc(n * sizeof(*offs));
    if (!zcls || !offs) { free(zcls); free(offs); pthread_mutex_unlock(&t->lock); return -ENOMEM; }
    memset(zcls, -1, nz);
    for (size_t i = 0; i < n; i++) {
        uint64_t z = used[i].off / ZONE_BYTES;
        if (zcls[z] >= 0 && zcls[z] != (int8_t)used[i].cls) { free(zcls); free(offs); pthread_mutex_unlock(&t->lock); return -EINVAL; }
        zcls[z] = (int8_t)used[i].cls;
        offs[i] = used[i].off;
        t->used += CLASS_BYTES[used[i].cls];
    }
    qsort(offs, n, sizeof(*offs), cmp_u64);
    /* every slab of a zone in use that no object holds goes back on its
     * class's free list (in reverse: handed out front to back) */
    for (uint64_t z = nz; z-- > 0;) {
        if (zcls[z] < 0) continue;
        size_t sb = CLASS_BYTES[(int)zcls[z]];
        uint64_t base = z * ZONE_BYTES;
        for (uint64_t off = base + ZONE_BYTES - sb; ; off -= sb) {
            if (!bsearch(&off, offs, n, sizeof(*offs), cmp_u64)) fl_push(&t->free[(int)zcls[z]], off);
            if (off == base) break;
        }
    }
    t->next_zone = nz * ZONE_BYTES;
    free(zcls);
    free(offs);
    pthread_mutex_unlock(&t->lock);
    return 0;
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
        /* a file created just now (empty): every block reads zeros until
         * written -- fallocated (unwritten extents) or sparse alike */
        if (st.st_size == 0) t->zero_from = 0;
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
    t->zero_from = UINT64_MAX;  /* nothing known to read zeros unless the backend says so */
    int rc;
    if (!strncmp(spec, "file:", 5)) rc = file_open(t, spec + 5, err, err_len);
#ifdef ROCEPOOL_SPDK
    else if (!strncmp(spec, "spdk:", 5)) rc = spdk_open(t, spec + 5, err, err_len);
#else
    else if (!strncmp(spec, "spdk:", 5)) { snprintf(err, err_len, "tier: this build has no SPDK (build.sh with SPDK_DIR=...)"); rc = -1; }
#endif
    else { snprintf(err, err_len, "tier: unknown backend in %s (file:PATH[:GIB] | spdk:TRADDR[:NSID])", spec); rc = -1; }
    if (rc) { ost_tier_close(t); return NULL; }
    if (t->zero_from > t->capacity) t->zero_from = t->capacity;
    if (t->capacity < ZONE_BYTES) { snprintf(err, err_len, "tier: device smaller than one 64 MiB zone"); ost_tier_close(t); return NULL; }
    return t;
}

void ost_tier_close(ost_tier_t *t) {
    if (!t) return;
    if (t->fd >= 0) close(t->fd);
    if (t->dfd >= 0) close(t->dfd);
    for (int i = 0; i < N_CLASSES; i++) { free(t->free[i].offs); free(t->zfree[i].offs); }
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

/* Tier reads bypass the page cache (O_DIRECT, as the writes do): through
 * it, a 1 MiB GET cost the OST ~1 ms of CPU on i3en (6 x 48 cores, the
 * ior-easy-read phase: its threads 93-99% busy) -- pages allocated, and
 * reclaimed under the memory pressure of terabytes read through 384 GiB
 * hosts, then copied; data read once gains nothing from the cache. The
 * page-aligned span is read into this thread's aligned buffer (or straight
 * into the caller's when it is aligned itself) and the bytes copied out.
 * ROCEPOOL_OST_TIER_DIRECT_READ=0: through the page cache, as before. */
static atomic_int g_direct_reads_off;
static int direct_reads_on(void) {
    static int v = -1;
    if (v < 0) { const char *e = getenv("ROCEPOOL_OST_TIER_DIRECT_READ"); v = !(e && !strcmp(e, "0")); }
    return v && !atomic_load(&g_direct_reads_off);
}
static atomic_ullong g_direct_reads;
unsigned long long ost_tier_direct_reads(void) { return atomic_load(&g_direct_reads); }
static __thread uint8_t *tl_dbuf;
static __thread size_t tl_dcap;
static int read_direct_any(ost_tier_t *t, const tier_ext_t *ext, uint64_t off, void *buf, size_t len) {
    uint64_t s = off & ~(uint64_t)(TIER_PAGE - 1), e = (off + len + TIER_PAGE - 1) & ~(uint64_t)(TIER_PAGE - 1);
    if (e > CLASS_BYTES[ext->cls] || ext->off % TIER_PAGE) return 1;  /* not this one: the buffered path */
    atomic_fetch_add(&g_direct_reads, 1);
    if (s == off && e == off + len && ((uintptr_t)buf % TIER_PAGE) == 0)
        return xpread(t->dfd, buf, len, ext->off + off);
    size_t n = (size_t)(e - s);
    if (tl_dcap < n) {
        void *b = NULL;
        size_t cap = n < (1u << 20) ? (1u << 20) : n;
        if (posix_memalign(&b, TIER_PAGE, cap)) return -ENOMEM;
        free(tl_dbuf);
        tl_dbuf = b;
        tl_dcap = cap;
    }
    int rc = xpread(t->dfd, tl_dbuf, n, ext->off + s);
    if (!rc) memcpy(buf, tl_dbuf + (off - s), len);
    return rc;
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
    if (t->dfd >= 0 && direct_reads_on()) {
        rc = read_direct_any(t, ext, off, buf, len);
        if (rc <= 0 && rc != -EINVAL) return rc;
        if (rc == -EINVAL) atomic_store(&g_direct_reads_off, 1);  /* the file system refused: buffered from now on */
        if (rc == -EINVAL) fprintf(stderr, "rocepool OST: direct tier reads refused (EINVAL): through the page cache\n");
    }
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
