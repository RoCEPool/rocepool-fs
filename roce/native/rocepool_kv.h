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

#ifdef __cplusplus
}
#endif
#endif
