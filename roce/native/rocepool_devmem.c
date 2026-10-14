/* rocepool_devmem.c -- see rocepool_devmem.h */
#define _GNU_SOURCE
#include "rocepool_devmem.h"

#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

typedef int CUresult_rp;


/* ---- emulated device (tests) ---- */
typedef struct emu_alloc { struct emu_alloc *next; uint8_t *base, *view; size_t size; int fd; } emu_alloc_t;
static emu_alloc_t *g_emu;
static pthread_mutex_t g_emu_lk = PTHREAD_MUTEX_INITIALIZER;

static emu_alloc_t *emu_find(const void *p) {
    pthread_mutex_lock(&g_emu_lk);
    emu_alloc_t *a = g_emu;
    for (; a; a = a->next)
        if ((const uint8_t *)p >= a->base && (const uint8_t *)p < a->base + a->size) break;
    pthread_mutex_unlock(&g_emu_lk);
    return a;
}
/* device memory of the emulated GPU: the returned pointer faults on any CPU access */
RP_DEV_API void *rp_devemu_alloc(size_t size) {
    size = (size + 4095) & ~(size_t)4095;
    int fd = (int)syscall(SYS_memfd_create, "rocepool-devemu", 0);
    if (fd < 0 || ftruncate(fd, (off_t)size)) return NULL;
    uint8_t *base = mmap(NULL, size, PROT_NONE, MAP_SHARED, fd, 0);
    uint8_t *view = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED || view == MAP_FAILED) { close(fd); return NULL; }
    emu_alloc_t *a = calloc(1, sizeof(*a));
    *a = (emu_alloc_t){NULL, base, view, size, fd};
    pthread_mutex_lock(&g_emu_lk);
    a->next = g_emu;
    g_emu = a;
    pthread_mutex_unlock(&g_emu_lk);
    return base;
}
RP_DEV_API void rp_devemu_free(void *p) {
    pthread_mutex_lock(&g_emu_lk);
    for (emu_alloc_t **pp = &g_emu; *pp; pp = &(*pp)->next)
        if ((*pp)->base == p) {
            emu_alloc_t *a = *pp;
            *pp = a->next;
            munmap(a->base, a->size);
            munmap(a->view, a->size);
            close(a->fd);
            free(a);
            break;
        }
    pthread_mutex_unlock(&g_emu_lk);
}
static int emu_to(void *dev, const void *host, size_t n, void *ctx) {
    (void)ctx;
    emu_alloc_t *a = emu_find(dev);
    if (!a || (uint8_t *)dev + n > a->base + a->size) return -EFAULT;
    memcpy(a->view + ((uint8_t *)dev - a->base), host, n);
    return 0;
}
static int emu_from(void *host, const void *dev, size_t n, void *ctx) {
    (void)ctx;
    emu_alloc_t *a = emu_find(dev);
    if (!a || (const uint8_t *)dev + n > a->base + a->size) return -EFAULT;
    memcpy(host, a->view + ((const uint8_t *)dev - a->base), n);
    return 0;
}
static int emu_zero(void *dev, size_t n, void *ctx) {
    (void)ctx;
    emu_alloc_t *a = emu_find(dev);
    if (!a || (uint8_t *)dev + n > a->base + a->size) return -EFAULT;
    memset(a->view + ((uint8_t *)dev - a->base), 0, n);
    return 0;
}
/* the test's (and the benchmark's) view of the emulated device */
RP_DEV_API int rp_devemu_copy_to(void *dev, const void *host, size_t n) { return emu_to(dev, host, n, NULL); }
RP_DEV_API int rp_devemu_copy_from(void *host, const void *dev, size_t n) { return emu_from(host, dev, n, NULL); }
static int emu_info(const void *p, void **base, size_t *size, int *dev, int *fd, uint64_t *fd_off) {
    emu_alloc_t *a = emu_find(p);
    if (!a) return -EFAULT;
    *base = a->base;
    *size = a->size;
    *dev = 0;
    *fd = getenv("ROCEPOOL_DEV_EMU_NO_DMABUF") ? -2 : a->fd;  /* tests: the no-registration fallback */
    *fd_off = 0;
    return 0;
}
static const rp_devrt_t g_emu_rt = {"emulated", emu_info, {emu_to, emu_from, emu_zero, NULL}};

/* ---- CUDA (driver API, dlopen'ed) ---- */
typedef int CUdevice_rp;
typedef void *CUcontext_rp;
typedef unsigned long long CUdeviceptr_rp;
static struct {
    CUresult_rp (*init)(unsigned);
    CUresult_rp (*ptr_attr)(void *, int, CUdeviceptr_rp);
    CUresult_rp (*dev_get)(CUdevice_rp *, int);
    CUresult_rp (*dev_attr)(int *, int, CUdevice_rp);
    CUresult_rp (*ctx_retain)(CUcontext_rp *, CUdevice_rp);
    CUresult_rp (*ctx_push)(CUcontext_rp);
    CUresult_rp (*ctx_pop)(CUcontext_rp *);
    CUresult_rp (*htod)(CUdeviceptr_rp, const void *, size_t);
    CUresult_rp (*dtoh)(void *, CUdeviceptr_rp, size_t);
    CUresult_rp (*memset8)(CUdeviceptr_rp, unsigned char, size_t);
    CUresult_rp (*range_handle)(void *, CUdeviceptr_rp, size_t, int, unsigned long long);
    CUresult_rp (*stream_sync)(void *);
    CUcontext_rp ctx[64];
} cu;
enum { CU_PTR_CONTEXT = 1, CU_PTR_MEMORY_TYPE = 2, CU_PTR_DEVICE_ORDINAL = 9, CU_PTR_RANGE_START = 11,
       CU_PTR_RANGE_SIZE = 12, CU_MEMORYTYPE_DEVICE = 2, CU_DEV_ATTR_DMA_BUF_SUPPORTED = 124,
       CU_RANGE_HANDLE_DMA_BUF_FD = 1 };

static int cuda_load(void) {
    void *h = dlopen("libcuda.so.1", RTLD_NOW | RTLD_GLOBAL);
    if (!h) return -1;
#define LOAD(field, name) (*(void **)&cu.field = dlsym(h, name))
    LOAD(init, "cuInit");
    LOAD(ptr_attr, "cuPointerGetAttribute");
    LOAD(dev_get, "cuDeviceGet");
    LOAD(dev_attr, "cuDeviceGetAttribute");
    LOAD(ctx_retain, "cuDevicePrimaryCtxRetain");
    LOAD(ctx_push, "cuCtxPushCurrent_v2");
    LOAD(ctx_pop, "cuCtxPopCurrent_v2");
    LOAD(htod, "cuMemcpyHtoD_v2");
    LOAD(dtoh, "cuMemcpyDtoH_v2");
    LOAD(memset8, "cuMemsetD8_v2");
    LOAD(range_handle, "cuMemGetHandleForAddressRange");  /* CUDA 11.7+; may be NULL */
    LOAD(stream_sync, "cuStreamSynchronize");
#undef LOAD
    if (!cu.init || !cu.ptr_attr || !cu.dev_get || !cu.ctx_retain || !cu.ctx_push || !cu.ctx_pop || !cu.htod ||
        !cu.dtoh || !cu.memset8)
        return -1;
    return cu.init(0) == 0 ? 0 : -1;
}
static int cuda_dev_of(const void *p) {
    int dev = -1;
    return cu.ptr_attr(&dev, CU_PTR_DEVICE_ORDINAL, (CUdeviceptr_rp)(uintptr_t)p) == 0 ? dev : -1;
}
static pthread_mutex_t g_cu_lk = PTHREAD_MUTEX_INITIALIZER;
/* run the copy with the device's primary context current in this thread */
static int cuda_enter(const void *p) {
    int d = cuda_dev_of(p);
    if (d < 0 || d >= 64) return -EFAULT;
    pthread_mutex_lock(&g_cu_lk);
    if (!cu.ctx[d]) {
        CUdevice_rp dev;
        if (cu.dev_get(&dev, d) || cu.ctx_retain(&cu.ctx[d], dev)) cu.ctx[d] = NULL;
    }
    CUcontext_rp c = cu.ctx[d];
    pthread_mutex_unlock(&g_cu_lk);
    return c && cu.ctx_push(c) == 0 ? 0 : -EIO;
}
static void cuda_leave(void) { CUcontext_rp c; cu.ctx_pop(&c); }
static int cuda_to(void *dev, const void *host, size_t n, void *ctx) {
    (void)ctx;
    if (cuda_enter(dev)) return -EFAULT;
    int rc = cu.htod((CUdeviceptr_rp)(uintptr_t)dev, host, n) ? -EIO : 0;
    cuda_leave();
    return rc;
}
static int cuda_from(void *host, const void *dev, size_t n, void *ctx) {
    (void)ctx;
    if (cuda_enter(dev)) return -EFAULT;
    int rc = cu.dtoh(host, (CUdeviceptr_rp)(uintptr_t)dev, n) ? -EIO : 0;
    cuda_leave();
    return rc;
}
static int cuda_zero(void *dev, size_t n, void *ctx) {
    (void)ctx;
    if (cuda_enter(dev)) return -EFAULT;
    int rc = cu.memset8((CUdeviceptr_rp)(uintptr_t)dev, 0, n) ? -EIO : 0;
    cuda_leave();
    return rc;
}
static int cuda_info(const void *p, void **base, size_t *size, int *dev, int *fd, uint64_t *fd_off) {
    CUdeviceptr_rp dp = (CUdeviceptr_rp)(uintptr_t)p, start = 0;
    unsigned int type = 0;
    size_t sz = 0;
    if (cu.ptr_attr(&type, CU_PTR_MEMORY_TYPE, dp) || type != CU_MEMORYTYPE_DEVICE) return -EFAULT;
    if (cu.ptr_attr(&start, CU_PTR_RANGE_START, dp) || cu.ptr_attr(&sz, CU_PTR_RANGE_SIZE, dp)) return -EFAULT;
    *base = (void *)(uintptr_t)start;
    *size = sz;
    *dev = cuda_dev_of(p);
    /* without a dma-buf: the device pointer itself, if nvidia-peermem is loaded */
    *fd = access("/sys/module/nvidia_peermem", F_OK) == 0 || access("/sys/module/nv_peer_mem", F_OK) == 0 ? -1 : -2;
    *fd_off = 0;
    int supported = 0;
    CUdevice_rp d;
    if (cu.range_handle && !cu.dev_get(&d, *dev) && cu.dev_attr &&
        !cu.dev_attr(&supported, CU_DEV_ATTR_DMA_BUF_SUPPORTED, d) && supported && !(start & 4095)) {
        int f = -1;
        size_t asz = (sz + 4095) & ~(size_t)4095;  /* the export must cover whole host pages */
        if (!cuda_enter(p)) {
            if (cu.range_handle(&f, start, asz, CU_RANGE_HANDLE_DMA_BUF_FD, 0) == 0) { *fd = f; *size = asz; }
            cuda_leave();
        }
    }
    return 0;
}
static const rp_devrt_t g_cuda_rt = {"cuda", cuda_info, {cuda_to, cuda_from, cuda_zero, NULL}};


static const rp_devrt_t *g_rt;
static pthread_once_t g_rt_once = PTHREAD_ONCE_INIT;
static void rt_init(void) {
    const char *e = getenv("ROCEPOOL_DEV_EMU");
    g_rt = e && !strcmp(e, "1") ? &g_emu_rt : cuda_load() == 0 ? &g_cuda_rt : NULL;
}
const rp_devrt_t *rp_devrt_get(void) {
    pthread_once(&g_rt_once, rt_init);
    return g_rt;
}
void rp_devrt_close_fd(int fd) {
    /* the emulator's dma-buf is its allocation's own memfd, closed by rp_devemu_free */
    if (fd >= 0 && rp_devrt_get() == &g_cuda_rt) close(fd);
}
void rp_devrt_stream_sync(void *stream) {
    if (rp_devrt_get() == &g_cuda_rt && cu.stream_sync) cu.stream_sync(stream);
}
