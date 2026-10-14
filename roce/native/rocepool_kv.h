/* rocepool_kv.h -- native KV client API over the OST servers.
 *
 * No POSIX, no FUSE, no kernel crossing: one call = one RDMA round trip to
 * the OST that owns the key (sha256(namespace + key) mod #OSTs), and the
 * batched calls send ONE frame per OST for a whole set of keys, all OSTs in
 * parallel. Thread-safe: share one handle across threads.
 *
 * Return values: 0 on success, or a negative errno: -ENOENT (no such key),
 * -EAGAIN (put_if version mismatch), -EIO (transport/server failure),
 * -EFBIG (larger than one frame). Buffers returned via void** are malloc'd:
 * release them with rpkv_free.
 */
#ifndef ROCEPOOL_KV_H
#define ROCEPOOL_KV_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rpkv rpkv_t;

/* osts: "host:port,host:port,..." (the same OST list, in the same order, on
 * every client); ns: key namespace prefix (NULL = "kv/"), keeps KV keys apart
 * from file-system stripe objects on the same OSTs. */
rpkv_t *rpkv_open(const char *osts, const char *ns);
void rpkv_close(rpkv_t *kv);
void rpkv_free(void *p);

int rpkv_put(rpkv_t *kv, const char *key, const void *val, size_t len, uint64_t *version);
/* compare-and-swap: applies only if the key's version == expected (0 = must not exist) */
int rpkv_put_if(rpkv_t *kv, const char *key, const void *val, size_t len, uint64_t expected, uint64_t *version);
int rpkv_get(rpkv_t *kv, const char *key, void **val, size_t *len, uint64_t *version);
int rpkv_delete(rpkv_t *kv, const char *key);
/* partial update / read of a value without moving the rest of it */
int rpkv_write_at(rpkv_t *kv, const char *key, uint64_t off, const void *data, size_t len);
int rpkv_read_at(rpkv_t *kv, const char *key, uint64_t off, size_t len, void **val, size_t *got);

/* Batched: status[i] gets each key's own result (0 / -ENOENT / ...); the
 * call returns 0 unless the batch itself could not be delivered. */
int rpkv_mput(rpkv_t *kv, size_t n, const char *const *keys, const void *const *vals, const size_t *lens,
              int *status);
int rpkv_mget(rpkv_t *kv, size_t n, const char *const *keys, void **vals, size_t *lens, int *status);
int rpkv_mdelete(rpkv_t *kv, size_t n, const char *const *keys, int *status);

/* round trips this handle has made (for tests/benchmarks) */
uint64_t rpkv_rpcs(rpkv_t *kv);

/* ---------------------------------------------------------------- GPU memory
 * Values moved straight between the OSTs' RAM and GPU memory, for KV-cache
 * blocks (prefill writes them, decode / a later request with the same
 * prefix reads them): a get is a one-sided RDMA READ by this host's NIC
 * into the GPU buffer, a put is an RDMA READ by the OST out of it -- no host
 * bounce buffer, no CPU copy, no metadata server (a key maps to its OST by
 * hash). Leases (where a value sits in the OST's RAM) are cached per
 * handle and validated on every read by the object's seqlock, so a value
 * that is re-read costs one RDMA READ and no RPC.
 *
 * Device runtime: CUDA (libcuda.so.1, loaded at run time), or the emulated
 * device under ROCEPOOL_DEV_EMU=1. When the NIC cannot reach the GPU's
 * memory (no dma-buf export, no nvidia-peermem), the calls still work,
 * through host memory, and rpkv_dev_direct() says 0.
 *
 * Values must fit one frame (16 MiB by default, ROCEPOOL_FRAME_BYTES):
 * store a KV block per layer, or per block of all layers. */
typedef struct rpkv_dev rpkv_dev_t;

/* registers the GPU allocation containing [base, base+len) (the whole
 * allocation: one registration serves every block in it). NULL: base is
 * not device memory of the runtime, or no runtime. */
rpkv_dev_t *rpkv_dev_register(rpkv_t *kv, void *base, size_t len);
void rpkv_dev_deregister(rpkv_dev_t *d);
int rpkv_dev_direct(const rpkv_dev_t *d);   /* 1: the NIC reaches it; 0: transfers bounce */

/* the value <-> d at byte offset off (an offset from the base given to
 * rpkv_dev_register). get: cap = room at off; -EFBIG if the value is
 * larger (*len set to its size). */
int rpkv_put_dev(rpkv_t *kv, const char *key, rpkv_dev_t *d, size_t off, size_t len, uint64_t *version);
int rpkv_get_dev(rpkv_t *kv, const char *key, rpkv_dev_t *d, size_t off, size_t cap, size_t *len);

/* many keys at once, every OST in parallel: lease lookups go one BATCH
 * frame per OST, the transfers run concurrently. status[i] per key. */
int rpkv_mput_dev(rpkv_t *kv, size_t n, const char *const *keys, rpkv_dev_t *d, const size_t *offs,
                  const size_t *lens, int *status);
int rpkv_mget_dev(rpkv_t *kv, size_t n, const char *const *keys, rpkv_dev_t *d, const size_t *offs,
                  const size_t *caps, size_t *lens, int *status);
/* which keys exist (present[i] = 1/0), one BATCH round trip per OST; it
 * also takes and caches their leases, so an rpkv_mget_dev right after
 * moves data only. For prefix-cache lookups: how many leading blocks of a
 * prompt are stored. Returns 0 or the batch's own failure. */
int rpkv_mcontains(rpkv_t *kv, size_t n, const char *const *keys, int *present);

/* bytes moved NIC <-> GPU directly / through host memory, leases taken /
 * reused from the cache, by this handle */
void rpkv_dev_stats(rpkv_t *kv, uint64_t *direct, uint64_t *bounced, uint64_t *leases, uint64_t *lease_hits);

#ifdef __cplusplus
}
#endif
#endif
