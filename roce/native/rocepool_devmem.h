/* rocepool_devmem.h -- device (GPU) memory for rocepool's GPU-direct paths
 * (the cuFile library, rocepool_cufile.c; the KV API's *_dev calls,
 * rocepool_kv.c): which allocation a device pointer belongs to, a dma-buf
 * of it for the NIC, and the runtime's copies for transfers that cannot go
 * NIC <-> device directly.
 *
 * Runtimes: CUDA through libcuda.so.1 (dlopen'ed: nothing to link, nothing
 * needed at build time), or with ROCEPOOL_DEV_EMU=1 an emulated device for
 * tests: its memory is a memfd whose device-pointer range is mapped
 * PROT_NONE in the process (any CPU access faults); the RDMA emulator
 * reaches it through the dma-buf registration, as a NIC reaches a GPU. */
#ifndef ROCEPOOL_DEVMEM_H
#define ROCEPOOL_DEVMEM_H

#include <stddef.h>
#include <stdint.h>

#define RP_DEV_API __attribute__((visibility("default")))

/* the runtime's copies, 0 or -errno */
typedef struct {
    int (*to_dev)(void *dev, const void *host, size_t n, void *ctx);
    int (*from_dev)(void *host, const void *dev, size_t n, void *ctx);
    int (*zero)(void *dev, size_t n, void *ctx);
    void *ctx;
} rp_devops_t;

typedef struct {
    const char *name;   /* "cuda" / "emulated" */
    /* the allocation containing p: base, size, device ordinal, and how the
     * NIC can reach it: *fd >= 0 a dma-buf of [base, base+size) (offset
     * *fd_off; the caller closes it with rp_devrt_close_fd); -1 by the device
     * pointer itself (nvidia-peermem); -2 not at all (transfers bounce) */
    int (*info)(const void *p, void **base, size_t *size, int *dev, int *fd, uint64_t *fd_off);
    rp_devops_t ops;
} rp_devrt_t;

/* ROCEPOOL_DEV_EMU=1: the emulator; else CUDA when libcuda.so.1 loads and
 * initialises; else NULL (no device runtime). Decided once per process. */
const rp_devrt_t *rp_devrt_get(void);
void rp_devrt_close_fd(int fd);           /* a dma-buf from info() */
void rp_devrt_stream_sync(void *stream);  /* CUDA: cuStreamSynchronize; else nothing */

/* the emulated device (tests, benchmarks without a GPU) */
RP_DEV_API void *rp_devemu_alloc(size_t size);
RP_DEV_API void rp_devemu_free(void *p);
RP_DEV_API int rp_devemu_copy_to(void *dev, const void *host, size_t n);
RP_DEV_API int rp_devemu_copy_from(void *host, const void *dev, size_t n);

#endif
