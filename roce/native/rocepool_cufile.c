/* rocepool_cufile.c -- GPUDirect Storage for rocepool: NVIDIA's cuFile API
 * (the interface of libcufile.so, which PyTorch/kvikio, RAPIDS, DALI and
 * GDS-enabled renderers call), served by rocepool's client core. A file
 * read lands in GPU memory straight from the OSTs' RAM -- the NIC writes
 * into the GPU (RDMA READ into a dma-buf / peer-memory registration of the
 * device buffer), no host bounce buffer, no CPU copy; a write is RDMA-read
 * by each OST out of the GPU. Stripes go to all OSTs in parallel.
 *
 * Deployment: this library is also installed as libcufile.so.0 in a
 * directory of its own; putting that directory first in LD_LIBRARY_PATH
 * makes every cuFile application use it. Descriptors it serves:
 *   - files under $ROCEPOOL_PREFIX opened through the rocepool FUSE mount
 *     (the descriptor's path, /proc/self/fd), and
 *   - descriptors of the LD_PRELOAD interception library in the same
 *     process (rp_intercept_fd_path);
 * with ROCEPOOL_MDTS / ROCEPOOL_OSTS naming the servers, as for the
 * interception library. Any other descriptor goes to NVIDIA's real
 * libcufile when ROCEPOOL_CUFILE_REAL names it (or one is found in the
 * usual places), else through host memory (pread + copy to the device).
 *
 * Device runtime (rocepool_devmem.c): CUDA through libcuda.so.1 (dlopen):
 * the buffer's allocation is registered with the NIC as a dma-buf
 * (cuMemGetHandleForAddressRange, CUDA 11.7+ with the open kernel modules)
 * or, failing that, by its device pointer (nvidia-peermem); failing both,
 * transfers bounce through host memory (cuMemcpy) and are counted as such.
 * ROCEPOOL_DEV_EMU=1 replaces CUDA with an emulated device for tests: its
 * "device memory" is a memfd whose device-pointer range is mapped PROT_NONE
 * in the process -- any CPU access faults -- and the RDMA emulator reaches
 * it through the dma-buf registration, as a NIC reaches a GPU.
 */
#define _GNU_SOURCE
#include "rocepool_fs.h"
#include "rocepool_devmem.h"

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#define API __attribute__((visibility("default")))

/* ------------------------------------------------------------ the cuFile ABI
 * Types and values as in NVIDIA's GPUDirect Storage API reference
 * (cufile.h): the error struct, the descriptor, the batch structures. */
typedef int CUresult_rp;
typedef enum {
    CU_FILE_SUCCESS = 0,
    CU_FILE_DRIVER_NOT_INITIALIZED = 5001,
    CU_FILE_PLATFORM_NOT_SUPPORTED = 5007,
    CU_FILE_IO_NOT_SUPPORTED = 5008,
    CU_FILE_CUDA_DRIVER_ERROR = 5011,
    CU_FILE_CUDA_POINTER_INVALID = 5012,
    CU_FILE_INVALID_FILE_TYPE = 5018,
    CU_FILE_INVALID_VALUE = 5022,
    CU_FILE_MEMORY_ALREADY_REGISTERED = 5023,
    CU_FILE_MEMORY_NOT_REGISTERED = 5024,
    CU_FILE_HANDLE_ALREADY_REGISTERED = 5028,
    CU_FILE_INTERNAL_ERROR = 5030,
} CUfileOpError;
typedef struct { CUfileOpError err; CUresult_rp cu_err; } CUfileError_t;
typedef enum { CU_FILE_HANDLE_TYPE_OPAQUE_FD = 1, CU_FILE_HANDLE_TYPE_OPAQUE_WIN32 = 2,
               CU_FILE_HANDLE_TYPE_USERSPACE_FS = 3 } CUfileFileHandleType;
typedef struct {
    CUfileFileHandleType type;
    union { int fd; void *handle; } handle;
    const void *fs_ops;
} CUfileDescr_t;
typedef void *CUfileHandle_t;
typedef struct {
    struct {
        unsigned int major_version, minor_version;
        size_t poll_thresh_size, max_direct_io_size;
        unsigned int dstatusflags, dcontrolflags;
    } nvfs;
    unsigned int fflags, max_device_cache_size, per_buffer_cache_size, max_device_pinned_mem_size;
    unsigned int max_batch_io_size, max_batch_io_timeout_msecs;
} CUfileDrvProps_t;
typedef enum { CUFILE_READ = 0, CUFILE_WRITE = 1 } CUfileOpcode_t;
typedef enum { CUFILE_WAITING = 0x1, CUFILE_PENDING = 0x2, CUFILE_INVALID = 0x4, CUFILE_CANCELED = 0x8,
               CUFILE_COMPLETE = 0x10, CUFILE_TIMEOUT = 0x20, CUFILE_FAILED = 0x40 } CUfileStatus_t;
typedef enum { CUFILE_BATCH = 1 } CUfileBatchMode_t;
typedef struct {
    CUfileBatchMode_t mode;
    union { struct { void *devPtr_base; off_t file_offset; off_t devPtr_offset; size_t size; } batch; } u;
    CUfileHandle_t fh;
    CUfileOpcode_t opcode;
    void *cookie;
} CUfileIOParams_t;
typedef struct { void *cookie; CUfileStatus_t status; size_t ret; } CUfileIOEvents_t;
typedef void *CUfileBatchHandle_t;

static CUfileError_t ok(void) { return (CUfileError_t){CU_FILE_SUCCESS, 0}; }
static CUfileError_t fail(CUfileOpError e) { return (CUfileError_t){e, 0}; }

static const rp_devrt_t *g_rt;

/* ------------------------------------------------------------ state */
static int g_served;   /* the rocepool client core is up (ROCEPOOL_MDTS / OSTS given) */
static char g_prefix[PATH_MAX];
static size_t g_prefix_len;
static long g_use;
static pthread_mutex_t g_lk = PTHREAD_MUTEX_INITIALIZER;
static atomic_ullong g_posix_bytes;

/* NVIDIA's own libcufile, for descriptors that are not ours */
static struct {
    void *h;
    CUfileError_t (*driver_open)(void);
    CUfileError_t (*handle_register)(CUfileHandle_t *, CUfileDescr_t *);
    void (*handle_deregister)(CUfileHandle_t);
    CUfileError_t (*buf_register)(const void *, size_t, int);
    CUfileError_t (*buf_deregister)(const void *);
    ssize_t (*read)(CUfileHandle_t, void *, size_t, off_t, off_t);
    ssize_t (*write)(CUfileHandle_t, const void *, size_t, off_t, off_t);
} real;

static void load_real(void) {
    const char *cands[] = {getenv("ROCEPOOL_CUFILE_REAL"), "/usr/local/cuda/lib64/libcufile.so.0",
                           "/usr/local/cuda/targets/x86_64-linux/lib/libcufile.so.0", "/usr/lib/x86_64-linux-gnu/libcufile.so.0"};
    Dl_info me;
    char mine[PATH_MAX] = "";
    if (dladdr((void *)load_real, &me) && me.dli_fname && !realpath(me.dli_fname, mine)) mine[0] = 0;
    for (size_t i = 0; i < sizeof(cands) / sizeof(*cands); i++) {
        char rp[PATH_MAX];
        if (!cands[i] || !*cands[i] || !realpath(cands[i], rp)) continue;
        if (!strcmp(rp, mine)) continue;  /* never ourselves (we may be installed under that name) */
        void *h = dlopen(rp, RTLD_NOW | RTLD_LOCAL);
        if (!h) continue;
        *(void **)&real.driver_open = dlsym(h, "cuFileDriverOpen");
        *(void **)&real.handle_register = dlsym(h, "cuFileHandleRegister");
        *(void **)&real.handle_deregister = dlsym(h, "cuFileHandleDeregister");
        *(void **)&real.buf_register = dlsym(h, "cuFileBufRegister");
        *(void **)&real.buf_deregister = dlsym(h, "cuFileBufDeregister");
        *(void **)&real.read = dlsym(h, "cuFileRead");
        *(void **)&real.write = dlsym(h, "cuFileWrite");
        if (real.driver_open && real.handle_register && real.read && real.write && real.driver_open().err == 0) {
            real.h = h;
            return;
        }
        dlclose(h);
        memset(&real, 0, sizeof(real));
    }
}

static int split_list(char *s, char **out, int max) {
    int n = 0;
    char *save = NULL;
    for (char *tok = strtok_r(s, ",", &save); tok && n < max; tok = strtok_r(NULL, ",", &save)) out[n++] = tok;
    return n;
}

static int g_inited;
static int driver_init(void) {
    pthread_mutex_lock(&g_lk);
    if (!g_inited) {
        g_rt = rp_devrt_get();
        const char *prefix = getenv("ROCEPOOL_PREFIX"), *mdts = getenv("ROCEPOOL_MDTS"), *osts = getenv("ROCEPOOL_OSTS");
        if (prefix && *prefix && mdts && osts) {
            if (!realpath(prefix, g_prefix)) snprintf(g_prefix, sizeof(g_prefix), "%s", prefix);
            g_prefix_len = strlen(g_prefix);
            while (g_prefix_len > 1 && g_prefix[g_prefix_len - 1] == '/') g_prefix[--g_prefix_len] = 0;
            cfg.nmdt = split_list(strdup(mdts), cfg.mdt, MAX_PEERS);
            cfg.nost = split_list(strdup(osts), cfg.ost, MAX_PEERS);
            /* one GPU stream: enough threads to keep every OST busy */
            cfg.io_threads = 16;
            const char *v = getenv("ROCEPOOL_IO_THREADS");
            if (v && atoi(v) > 0) cfg.io_threads = atoi(v);
            if ((v = getenv("ROCEPOOL_ATTR_TTL")) && *v) cfg.attr_ttl = atof(v);
            if ((v = getenv("ROCEPOOL_STRIPE_SIZE")) && atoll(v) > 0) cfg.stripe_size = (uint64_t)atoll(v);
            if ((v = getenv("ROCEPOOL_STRIPE_COUNT")) && atoi(v) > 0) cfg.stripe_count = atoi(v);
            g_served = rpfs_init() == 0;
            if (!g_served) fprintf(stderr, "rocepool_cufile: client init failed; rocepool files go through host memory\n");
        }
        load_real();
        g_inited = 1;
    }
    pthread_mutex_unlock(&g_lk);
    return g_rt ? 0 : -1;
}

/* ------------------------------------------------------------ device buffers */
typedef struct dbuf {
    struct dbuf *next;
    rp_devbuf_t b;
    int fd, explicit_reg;
} dbuf_t;
static dbuf_t *g_bufs;
static pthread_mutex_t g_buf_lk = PTHREAD_MUTEX_INITIALIZER;

/* the registered allocation containing p, registering it on first use */
static dbuf_t *buf_for(const void *p, int explicit_reg) {
    pthread_mutex_lock(&g_buf_lk);
    for (dbuf_t *d = g_bufs; d; d = d->next)
        if ((const uint8_t *)p >= (uint8_t *)d->b.dev && (const uint8_t *)p < (uint8_t *)d->b.dev + d->b.len) {
            if (explicit_reg) d->explicit_reg = 1;
            pthread_mutex_unlock(&g_buf_lk);
            return d;
        }
    void *base;
    size_t size;
    int dev, fd;
    uint64_t off;
    if (!g_rt || g_rt->info(p, &base, &size, &dev, &fd, &off)) { pthread_mutex_unlock(&g_buf_lk); return NULL; }
    dbuf_t *d = calloc(1, sizeof(*d));
    d->b = (rp_devbuf_t){base, size, NULL, &g_rt->ops};
    d->fd = fd;
    d->explicit_reg = explicit_reg;
    /* the NIC registration (lazy, per device, at first transfer) */
    d->b.mem = fd == -2 || getenv("ROCEPOOL_GDS_BOUNCE") ? NULL : roce_mem_register_device(base, size, fd, off, dev);
    d->next = g_bufs;
    g_bufs = d;
    pthread_mutex_unlock(&g_buf_lk);
    return d;
}

/* ------------------------------------------------------------ file handles */
typedef struct {
    int fd;
    int ours;        /* served by the rocepool core */
    int via_icp;     /* a descriptor of the interception library */
    int writable;    /* opened for writing through the FUSE mount: fsync before reads */
    char path[PATH_MAX];
    CUfileHandle_t real_fh;
} fh_t;

static int fd_to_rocepool(int fd, fh_t *h) {
    int (*icp_path)(int, char *, size_t) = (int (*)(int, char *, size_t))dlsym(RTLD_DEFAULT, "rp_intercept_fd_path");
    if (icp_path && icp_path(fd, h->path, sizeof(h->path)) == 0) { h->via_icp = 1; return 1; }
    if (!g_prefix_len) return 0;
    char link[64], abs[PATH_MAX];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, abs, sizeof(abs) - 1);
    if (n <= 0) return 0;
    abs[n] = 0;
    if (strncmp(abs, g_prefix, g_prefix_len) || (abs[g_prefix_len] != '/' && abs[g_prefix_len])) return 0;
    const char *rel = abs + g_prefix_len;
    while (*rel == '/') rel++;
    snprintf(h->path, sizeof(h->path), "%s", rel);
    int fl = fcntl(fd, F_GETFL);
    h->writable = fl >= 0 && (fl & O_ACCMODE) != O_RDONLY;
    return 1;
}

/* before device I/O reads the file: writes still buffered in this process
 * (interception library) or in the FUSE daemon (an fd opened for writing)
 * reach the servers */
static void settle(fh_t *h) {
    if (h->via_icp) {
        int (*flush)(int) = (int (*)(int))dlsym(RTLD_DEFAULT, "rp_intercept_fd_flush");
        if (flush) flush(h->fd);
    } else if (h->writable) {
        fdatasync(h->fd);
    }
}

/* ------------------------------------------------------------ the API */
API CUfileError_t cuFileDriverOpen(void) {
    if (driver_init()) return (CUfileError_t){CU_FILE_CUDA_DRIVER_ERROR, 0};
    __atomic_add_fetch(&g_use, 1, __ATOMIC_RELAXED);
    return ok();
}
API CUfileError_t cuFileDriverClose(void) {
    __atomic_sub_fetch(&g_use, 1, __ATOMIC_RELAXED);
    return ok();
}
API CUfileError_t cuFileDriverClose_v2(void) { return cuFileDriverClose(); }
API long cuFileUseCount(void) { return __atomic_load_n(&g_use, __ATOMIC_RELAXED); }
API CUfileError_t cuFileGetVersion(int *version) {
    if (!version) return fail(CU_FILE_INVALID_VALUE);
    *version = 1130;
    return ok();
}
API CUfileError_t cuFileDriverGetProperties(CUfileDrvProps_t *p) {
    if (!p) return fail(CU_FILE_INVALID_VALUE);
    memset(p, 0, sizeof(*p));
    p->nvfs.major_version = 2;
    p->nvfs.minor_version = 17;
    p->nvfs.poll_thresh_size = 4;             /* KiB */
    p->nvfs.max_direct_io_size = 16384;       /* KiB */
    p->max_device_cache_size = 128 * 1024;    /* KiB */
    p->per_buffer_cache_size = 1024;          /* KiB */
    p->max_device_pinned_mem_size = 32u << 20;  /* KiB */
    p->max_batch_io_size = 128;
    p->max_batch_io_timeout_msecs = 5;
    return ok();
}
API CUfileError_t cuFileDriverSetPollMode(bool poll, size_t thresh) { (void)poll; (void)thresh; return ok(); }
API CUfileError_t cuFileDriverSetMaxDirectIOSize(size_t kb) { (void)kb; return ok(); }
API CUfileError_t cuFileDriverSetMaxCacheSize(size_t kb) { (void)kb; return ok(); }
API CUfileError_t cuFileDriverSetMaxPinnedMemSize(size_t kb) { (void)kb; return ok(); }

API CUfileError_t cuFileHandleRegister(CUfileHandle_t *fhp, CUfileDescr_t *descr) {
    if (!fhp || !descr || descr->type != CU_FILE_HANDLE_TYPE_OPAQUE_FD) return fail(CU_FILE_INVALID_FILE_TYPE);
    if (driver_init()) return (CUfileError_t){CU_FILE_CUDA_DRIVER_ERROR, 0};
    fh_t *h = calloc(1, sizeof(*h));
    h->fd = descr->handle.fd;
    h->ours = g_served && fd_to_rocepool(h->fd, h);
    if (!h->ours && real.h) {
        CUfileError_t e = real.handle_register(&h->real_fh, descr);
        if (e.err) h->real_fh = NULL;  /* POSIX through host memory, then */
    }
    *fhp = h;
    return ok();
}
API void cuFileHandleDeregister(CUfileHandle_t fhv) {
    fh_t *h = fhv;
    if (!h) return;
    if (h->real_fh && real.handle_deregister) real.handle_deregister(h->real_fh);
    free(h);
}

API CUfileError_t cuFileBufRegister(const void *base, size_t length, int flags) {
    (void)flags;
    if (driver_init()) return (CUfileError_t){CU_FILE_CUDA_DRIVER_ERROR, 0};
    if (!base || !length) return fail(CU_FILE_INVALID_VALUE);
    if (real.h && real.buf_register) real.buf_register(base, length, flags);  /* for its descriptors */
    return buf_for(base, 1) ? ok() : fail(CU_FILE_CUDA_POINTER_INVALID);
}
API CUfileError_t cuFileBufDeregister(const void *base) {
    if (real.h && real.buf_deregister) real.buf_deregister(base);
    pthread_mutex_lock(&g_buf_lk);
    for (dbuf_t **pp = &g_bufs; *pp; pp = &(*pp)->next)
        if ((*pp)->b.dev == base || ((const uint8_t *)base >= (uint8_t *)(*pp)->b.dev &&
                                     (const uint8_t *)base < (uint8_t *)(*pp)->b.dev + (*pp)->b.len)) {
            dbuf_t *d = *pp;
            *pp = d->next;
            roce_mem_release(d->b.mem);
            rp_devrt_close_fd(d->fd);
            free(d);
            pthread_mutex_unlock(&g_buf_lk);
            return ok();
        }
    pthread_mutex_unlock(&g_buf_lk);
    return fail(CU_FILE_MEMORY_NOT_REGISTERED);
}

/* any descriptor through host memory: pread / pwrite + a device copy */
static ssize_t posix_io(int fd, int write, uint8_t *dev, size_t size, off_t off) {
    size_t chunk = 8u << 20, done = 0;
    uint8_t *tmp = malloc(size < chunk ? size : chunk);
    if (!tmp) { errno = ENOMEM; return -1; }
    while (done < size) {
        size_t n = size - done < chunk ? size - done : chunk;
        ssize_t r;
        if (write) {
            if (g_rt->ops.from_dev(tmp, dev + done, n, NULL)) { free(tmp); errno = EFAULT; return -1; }
            r = pwrite(fd, tmp, n, off + (off_t)done);
        } else {
            r = pread(fd, tmp, n, off + (off_t)done);
            if (r > 0 && g_rt->ops.to_dev(dev + done, tmp, (size_t)r, NULL)) { free(tmp); errno = EFAULT; return -1; }
        }
        if (r < 0) { free(tmp); return -1; }
        done += (size_t)r;
        atomic_fetch_add(&g_posix_bytes, (size_t)r);
        if ((size_t)r < n) break;
    }
    free(tmp);
    return (ssize_t)done;
}

static ssize_t do_io(fh_t *h, int write, const void *base, size_t size, off_t foff, off_t boff) {
    if (!h || !base || foff < 0 || boff < 0) { errno = EINVAL; return -1; }
    if (driver_init()) { errno = ENODEV; return -1; }
    uint8_t *dev = (uint8_t *)base + boff;
    if (!h->ours) {
        if (h->real_fh) return write ? real.write(h->real_fh, base, size, foff, boff)
                                     : real.read(h->real_fh, (void *)base, size, foff, boff);
        return posix_io(h->fd, write, dev, size, foff);
    }
    dbuf_t *d = buf_for(dev, 0);
    if (!d || dev + size > (uint8_t *)d->b.dev + d->b.len) { errno = EFAULT; return -1; }
    settle(h);
    size_t bo = (size_t)(dev - (uint8_t *)d->b.dev);
    ssize_t r = write ? fs_write_dev(h->path, (uint64_t)foff, size, &d->b, bo)
                      : fs_read_dev(h->path, (uint64_t)foff, size, &d->b, bo);
    if (r < 0) { errno = (int)-r; return -1; }
    if (write && r > 0) {
        ac_del(h->path);  /* our view of the size moved on */
        /* and the descriptor's: the FUSE mount (or the interception library)
         * caches the file's size, which the direct write went around. The
         * last byte once more, through the descriptor, moves it on there. */
        uint8_t last;
        if (g_rt->ops.from_dev(&last, dev + r - 1, 1, NULL) == 0) {
            if (pwrite(h->fd, &last, 1, foff + r - 1) != 1) { /* the data is on the servers either way */ }
        }
    }
    return r;
}

API ssize_t cuFileRead(CUfileHandle_t fh, void *base, size_t size, off_t foff, off_t boff) {
    return do_io(fh, 0, base, size, foff, boff);
}
API ssize_t cuFileWrite(CUfileHandle_t fh, const void *base, size_t size, off_t foff, off_t boff) {
    return do_io(fh, 1, base, size, foff, boff);
}

/* ---- streams: done in order on the calling thread once the stream is idle */
API CUfileError_t cuFileStreamRegister(void *stream, unsigned flags) { (void)stream; (void)flags; return ok(); }
API CUfileError_t cuFileStreamDeregister(void *stream) { (void)stream; return ok(); }
static void stream_wait(void *stream) { rp_devrt_stream_sync(stream); }
API CUfileError_t cuFileReadAsync(CUfileHandle_t fh, void *base, size_t *size_p, off_t *foff_p, off_t *boff_p,
                                  ssize_t *done_p, void *stream) {
    if (!size_p || !foff_p || !boff_p || !done_p) return fail(CU_FILE_INVALID_VALUE);
    stream_wait(stream);
    *done_p = do_io(fh, 0, base, *size_p, *foff_p, *boff_p);
    return ok();
}
API CUfileError_t cuFileWriteAsync(CUfileHandle_t fh, void *base, size_t *size_p, off_t *foff_p, off_t *boff_p,
                                   ssize_t *done_p, void *stream) {
    if (!size_p || !foff_p || !boff_p || !done_p) return fail(CU_FILE_INVALID_VALUE);
    stream_wait(stream);
    *done_p = do_io(fh, 1, base, *size_p, *foff_p, *boff_p);
    return ok();
}

/* ---- batches: every entry of a submission in parallel, results polled */
typedef struct {
    unsigned cap, n, next_ev;
    CUfileIOParams_t *io;
    CUfileIOEvents_t *ev;
    unsigned done, claimed, workers;
    pthread_mutex_t lk;
    pthread_cond_t cv;
} batch_t;

API CUfileError_t cuFileBatchIOSetUp(CUfileBatchHandle_t *bp, unsigned nr) {
    if (!bp || !nr) return fail(CU_FILE_INVALID_VALUE);
    batch_t *b = calloc(1, sizeof(*b));
    b->cap = nr;
    b->io = calloc(nr, sizeof(*b->io));
    b->ev = calloc(nr, sizeof(*b->ev));
    pthread_mutex_init(&b->lk, NULL);
    pthread_cond_init(&b->cv, NULL);
    *bp = b;
    return ok();
}
static void *batch_worker(void *arg) {
    batch_t *b = arg;
    for (;;) {
        pthread_mutex_lock(&b->lk);
        unsigned i = b->claimed < b->n ? b->claimed++ : UINT32_MAX;
        pthread_mutex_unlock(&b->lk);
        if (i == UINT32_MAX) {
            pthread_mutex_lock(&b->lk);
            b->workers--;
            pthread_cond_broadcast(&b->cv);
            pthread_mutex_unlock(&b->lk);
            return NULL;
        }
        CUfileIOParams_t *p = &b->io[i];
        ssize_t r = do_io(p->fh, p->opcode == CUFILE_WRITE, p->u.batch.devPtr_base, p->u.batch.size,
                          p->u.batch.file_offset, p->u.batch.devPtr_offset);
        pthread_mutex_lock(&b->lk);
        b->ev[i] = (CUfileIOEvents_t){p->cookie, r < 0 ? CUFILE_FAILED : CUFILE_COMPLETE, r < 0 ? 0 : (size_t)r};
        b->done++;
        pthread_cond_broadcast(&b->cv);
        pthread_mutex_unlock(&b->lk);
    }
}
API CUfileError_t cuFileBatchIOSubmit(CUfileBatchHandle_t bh, unsigned nr, CUfileIOParams_t *iocbp, unsigned flags) {
    (void)flags;
    batch_t *b = bh;
    if (!b || !iocbp || nr > b->cap) return fail(CU_FILE_INVALID_VALUE);
    pthread_mutex_lock(&b->lk);
    if (b->done < b->n || b->workers) { pthread_mutex_unlock(&b->lk); return fail(CU_FILE_INVALID_VALUE); }  /* one at a time */
    memcpy(b->io, iocbp, nr * sizeof(*iocbp));
    for (unsigned i = 0; i < nr; i++) b->ev[i] = (CUfileIOEvents_t){iocbp[i].cookie, CUFILE_PENDING, 0};
    b->n = nr;
    b->done = b->claimed = b->next_ev = 0;
    unsigned workers = nr < 8 ? nr : 8;
    for (unsigned w = 0; w < workers; w++) {
        pthread_t t;
        if (pthread_create(&t, NULL, batch_worker, b) == 0) { pthread_detach(t); b->workers++; }
    }
    int started = b->workers > 0;
    pthread_mutex_unlock(&b->lk);
    return started ? ok() : fail(CU_FILE_INTERNAL_ERROR);
}
API CUfileError_t cuFileBatchIOGetStatus(CUfileBatchHandle_t bh, unsigned min_nr, unsigned *nr, CUfileIOEvents_t *ev,
                                         struct timespec *timeout) {
    batch_t *b = bh;
    if (!b || !nr || !ev) return fail(CU_FILE_INVALID_VALUE);
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    if (timeout) {
        dl.tv_sec += timeout->tv_sec;
        dl.tv_nsec += timeout->tv_nsec;
        if (dl.tv_nsec >= 1000000000L) { dl.tv_sec++; dl.tv_nsec -= 1000000000L; }
    }
    unsigned want = *nr;
    pthread_mutex_lock(&b->lk);
    while (b->done - b->next_ev < min_nr && b->done < b->n) {
        if (timeout) { if (pthread_cond_timedwait(&b->cv, &b->lk, &dl)) break; }
        else pthread_cond_wait(&b->cv, &b->lk);
    }
    unsigned out = 0;
    for (unsigned i = 0; i < b->n && out < want; i++)
        if (b->ev[i].status & (CUFILE_COMPLETE | CUFILE_FAILED)) { ev[out++] = b->ev[i]; b->ev[i].status = CUFILE_INVALID; }
    b->next_ev += out;
    pthread_mutex_unlock(&b->lk);
    *nr = out;
    return ok();
}
API CUfileError_t cuFileBatchIOCancel(CUfileBatchHandle_t bh) { (void)bh; return ok(); }
API void cuFileBatchIODestroy(CUfileBatchHandle_t bh) {
    batch_t *b = bh;
    if (!b) return;
    pthread_mutex_lock(&b->lk);
    while (b->done < b->n || b->workers) pthread_cond_wait(&b->cv, &b->lk);
    pthread_mutex_unlock(&b->lk);
    pthread_mutex_destroy(&b->lk);
    pthread_cond_destroy(&b->cv);
    free(b->io);
    free(b->ev);
    free(b);
}

/* ---- rocepool extras: what went where (benchmarks, tests, on-site report) */
API void rp_cufile_stats(uint64_t *direct, uint64_t *bounced, uint64_t *posix) {
    fs_dev_stats(direct, bounced);
    *posix = atomic_load(&g_posix_bytes);
}
API const char *rp_cufile_runtime(void) { driver_init(); return g_rt ? g_rt->name : "none"; }
