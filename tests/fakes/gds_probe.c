/* gds_probe.c -- a cuFile application, as a GDS-enabled program is one: it
 * links against libcufile.so.0 and moves file data to and from device
 * memory. Run with rocepool's libcufile.so.0 first in LD_LIBRARY_PATH and
 * ROCEPOOL_DEV_EMU=1, its device buffers are the emulated GPU's (any CPU
 * access to them faults). Commands:
 *   read  PATH OFF LEN OUT [noreg]   cuFileRead into the device, then copy the device bytes to OUT
 *   write PATH OFF IN [noreg]        IN's bytes into the device, then cuFileWrite
 *   batch PATH CHUNK N OUT           N reads of CHUNK bytes as one batch (entry i at file offset i*CHUNK)
 *   async PATH OFF LEN OUT           cuFileReadAsync
 *   bench PATH LEN ITERS             GB/s of whole-file reads into one device buffer
 * Device memory: the emulated GPU's when ROCEPOOL_DEV_EMU=1, else real GPU
 * memory (cuMemAlloc on device $GDS_DEVICE, default 0, through libcuda.so.1),
 * so the same probe is the on-site GPU benchmark.
 * Each command ends with: STATS <direct> <bounced> <posix> <runtime>
 * The device buffer starts 4096 bytes into its allocation (devPtr_offset). */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

typedef struct { int err; int cu_err; } CUfileError_t;
typedef struct { int type; union { int fd; void *handle; } handle; const void *fs_ops; } CUfileDescr_t;
typedef void *CUfileHandle_t;
typedef struct {
    int mode;
    union { struct { void *devPtr_base; off_t file_offset; off_t devPtr_offset; size_t size; } batch; } u;
    CUfileHandle_t fh;
    int opcode;
    void *cookie;
} CUfileIOParams_t;
typedef struct { void *cookie; int status; size_t ret; } CUfileIOEvents_t;

CUfileError_t cuFileDriverOpen(void);
CUfileError_t cuFileDriverClose(void);
CUfileError_t cuFileHandleRegister(CUfileHandle_t *, CUfileDescr_t *);
void cuFileHandleDeregister(CUfileHandle_t);
CUfileError_t cuFileBufRegister(const void *, size_t, int);
CUfileError_t cuFileBufDeregister(const void *);
ssize_t cuFileRead(CUfileHandle_t, void *, size_t, off_t, off_t);
ssize_t cuFileWrite(CUfileHandle_t, const void *, size_t, off_t, off_t);
CUfileError_t cuFileReadAsync(CUfileHandle_t, void *, size_t *, off_t *, off_t *, ssize_t *, void *);
CUfileError_t cuFileBatchIOSetUp(void **, unsigned);
CUfileError_t cuFileBatchIOSubmit(void *, unsigned, CUfileIOParams_t *, unsigned);
CUfileError_t cuFileBatchIOGetStatus(void *, unsigned, unsigned *, CUfileIOEvents_t *, struct timespec *);
void cuFileBatchIODestroy(void *);
/* rocepool extras */
void *rp_devemu_alloc(size_t);
void rp_devemu_free(void *);
int rp_devemu_copy_to(void *, const void *, size_t);
int rp_devemu_copy_from(void *, const void *, size_t);
void rp_cufile_stats(uint64_t *, uint64_t *, uint64_t *);
const char *rp_cufile_runtime(void);

static int g_emu;
static struct {
    int (*init)(unsigned);
    int (*dev_get)(int *, int);
    int (*retain)(void **, int);
    int (*set)(void *);
    int (*alloc)(unsigned long long *, size_t);
    int (*free_)(unsigned long long);
    int (*htod)(unsigned long long, const void *, size_t);
    int (*dtoh)(void *, unsigned long long, size_t);
} cu;
static int dev_setup(void) {
    const char *e = getenv("ROCEPOOL_DEV_EMU");
    g_emu = e && !strcmp(e, "1");
    if (g_emu) return 0;
    void *h = dlopen("libcuda.so.1", RTLD_NOW | RTLD_GLOBAL);
    if (!h) return -1;
    *(void **)&cu.init = dlsym(h, "cuInit");
    *(void **)&cu.dev_get = dlsym(h, "cuDeviceGet");
    *(void **)&cu.retain = dlsym(h, "cuDevicePrimaryCtxRetain");
    *(void **)&cu.set = dlsym(h, "cuCtxSetCurrent");
    *(void **)&cu.alloc = dlsym(h, "cuMemAlloc_v2");
    *(void **)&cu.free_ = dlsym(h, "cuMemFree_v2");
    *(void **)&cu.htod = dlsym(h, "cuMemcpyHtoD_v2");
    *(void **)&cu.dtoh = dlsym(h, "cuMemcpyDtoH_v2");
    int d = 0;
    void *ctx;
    const char *gd = getenv("GDS_DEVICE");
    if (!cu.init || !cu.alloc || cu.init(0) || cu.dev_get(&d, gd ? atoi(gd) : 0) || cu.retain(&ctx, d) || cu.set(ctx))
        return -1;
    return 0;
}
static void *dev_alloc(size_t n) {
    if (g_emu) return rp_devemu_alloc(n);
    unsigned long long p = 0;
    return cu.alloc(&p, n) ? NULL : (void *)(uintptr_t)p;
}
static void dev_free(void *p) { if (g_emu) rp_devemu_free(p); else cu.free_((unsigned long long)(uintptr_t)p); }
static int dev_to(void *d, const void *h, size_t n) {
    return g_emu ? rp_devemu_copy_to(d, h, n) : cu.htod((unsigned long long)(uintptr_t)d, h, n);
}
static int dev_from(void *h, const void *d, size_t n) {
    return g_emu ? rp_devemu_copy_from(h, d, n) : cu.dtoh(h, (unsigned long long)(uintptr_t)d, n);
}

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "FAIL %s:%d %s\n", __FILE__, __LINE__, #c); exit(2); } } while (0)
#define BOFF 4096

static CUfileHandle_t reg_file(int fd) {
    CUfileDescr_t d;
    memset(&d, 0, sizeof(d));
    d.type = 1;
    d.handle.fd = fd;
    CUfileHandle_t fh;
    CUfileError_t e = cuFileHandleRegister(&fh, &d);
    CHECK(e.err == 0);
    return fh;
}

static void dump(const char *out, const void *dev, size_t n) {
    uint8_t *h = malloc(n ? n : 1);
    CHECK(dev_from(h, dev, n) == 0);
    FILE *f = fopen(out, "wb");
    CHECK(f && fwrite(h, 1, n, f) == n);
    fclose(f);
    free(h);
}

static void stats(void) {
    uint64_t d, b, p;
    rp_cufile_stats(&d, &b, &p);
    printf("STATS %llu %llu %llu %s\n", (unsigned long long)d, (unsigned long long)b, (unsigned long long)p,
           rp_cufile_runtime());
}

static double now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

int main(int argc, char **argv) {
    CHECK(argc >= 3);
    CHECK(dev_setup() == 0);
    CHECK(cuFileDriverOpen().err == 0);
    const char *cmd = argv[1], *path = argv[2];
    int noreg = argc > 0 && !strcmp(argv[argc - 1], "noreg");
    if (!strcmp(cmd, "read") || !strcmp(cmd, "async")) {
        CHECK(argc >= 6);
        off_t off = atoll(argv[3]);
        size_t len = (size_t)atoll(argv[4]);
        int fd = open(path, O_RDONLY);
        CHECK(fd >= 0);
        CUfileHandle_t fh = reg_file(fd);
        uint8_t *dev = dev_alloc(len + BOFF);
        CHECK(dev);
        if (!noreg) CHECK(cuFileBufRegister(dev, len + BOFF, 0).err == 0);
        ssize_t r;
        if (!strcmp(cmd, "read")) {
            r = cuFileRead(fh, dev, len, off, BOFF);
        } else {
            size_t sz = len;
            off_t fo = off, bo = BOFF;
            CHECK(cuFileReadAsync(fh, dev, &sz, &fo, &bo, &r, NULL).err == 0);
        }
        CHECK(r >= 0);
        printf("READ %zd\n", r);
        dump(argv[5], dev + BOFF, (size_t)r);
        if (!noreg) CHECK(cuFileBufDeregister(dev).err == 0);
        cuFileHandleDeregister(fh);
        close(fd);
        dev_free(dev);
    } else if (!strcmp(cmd, "write")) {
        CHECK(argc >= 5);
        off_t off = atoll(argv[3]);
        FILE *f = fopen(argv[4], "rb");
        CHECK(f);
        fseek(f, 0, SEEK_END);
        size_t len = (size_t)ftell(f);
        fseek(f, 0, SEEK_SET);
        uint8_t *h = malloc(len ? len : 1);
        CHECK(fread(h, 1, len, f) == len);
        fclose(f);
        int fd = open(path, O_RDWR | O_CREAT, 0644);
        CHECK(fd >= 0);
        CUfileHandle_t fh = reg_file(fd);
        uint8_t *dev = dev_alloc(len + BOFF);
        CHECK(dev && dev_to(dev + BOFF, h, len) == 0);
        if (!noreg) CHECK(cuFileBufRegister(dev, len + BOFF, 0).err == 0);
        ssize_t r = cuFileWrite(fh, dev, len, off, BOFF);
        CHECK(r == (ssize_t)len);
        printf("WROTE %zd\n", r);
        if (!noreg) CHECK(cuFileBufDeregister(dev).err == 0);
        cuFileHandleDeregister(fh);
        CHECK(close(fd) == 0);
        dev_free(dev);
        free(h);
    } else if (!strcmp(cmd, "batch")) {
        CHECK(argc >= 6);
        size_t chunk = (size_t)atoll(argv[3]);
        unsigned n = (unsigned)atoi(argv[4]);
        int fd = open(path, O_RDONLY);
        CHECK(fd >= 0);
        CUfileHandle_t fh = reg_file(fd);
        uint8_t *dev = dev_alloc(chunk * n + BOFF);
        CHECK(dev && cuFileBufRegister(dev, chunk * n + BOFF, 0).err == 0);
        void *bh;
        CHECK(cuFileBatchIOSetUp(&bh, n).err == 0);
        CUfileIOParams_t *io = calloc(n, sizeof(*io));
        for (unsigned i = 0; i < n; i++) {
            io[i].mode = 1;
            io[i].u.batch.devPtr_base = dev;
            io[i].u.batch.file_offset = (off_t)(i * chunk);
            io[i].u.batch.devPtr_offset = (off_t)(BOFF + i * chunk);
            io[i].u.batch.size = chunk;
            io[i].fh = fh;
            io[i].opcode = 0;
            io[i].cookie = (void *)(uintptr_t)(i + 1);
        }
        CHECK(cuFileBatchIOSubmit(bh, n, io, 0).err == 0);
        CUfileIOEvents_t *ev = calloc(n, sizeof(*ev));
        unsigned got = 0;
        size_t total = 0;
        while (got < n) {
            unsigned k = n - got;
            CHECK(cuFileBatchIOGetStatus(bh, 1, &k, ev + got, NULL).err == 0);
            for (unsigned i = got; i < got + k; i++) {
                CHECK(ev[i].status == 0x10);
                CHECK(ev[i].cookie != NULL);
                total += ev[i].ret;
            }
            got += k;
        }
        cuFileBatchIODestroy(bh);
        printf("BATCH %zu\n", total);
        dump(argv[5], dev + BOFF, chunk * n);
        cuFileBufDeregister(dev);
        cuFileHandleDeregister(fh);
        close(fd);
    } else if (!strcmp(cmd, "bench")) {
        CHECK(argc >= 5);
        size_t len = (size_t)atoll(argv[3]);
        int iters = atoi(argv[4]);
        int fd = open(path, O_RDONLY);
        CHECK(fd >= 0);
        CUfileHandle_t fh = reg_file(fd);
        uint8_t *dev = dev_alloc(len);
        CHECK(dev && cuFileBufRegister(dev, len, 0).err == 0);
        double t0 = now();
        size_t total = 0;
        for (int i = 0; i < iters; i++) {
            ssize_t r = cuFileRead(fh, dev, len, 0, 0);
            CHECK(r == (ssize_t)len);
            total += (size_t)r;
        }
        double dt = now() - t0;
        printf("BENCH %zu %.6f %.3f\n", total, dt, total / dt / 1e9);
        cuFileBufDeregister(dev);
        cuFileHandleDeregister(fh);
        close(fd);
    } else {
        CHECK(!"unknown command");
    }
    stats();
    CHECK(cuFileDriverClose().err == 0);
    return 0;
}
