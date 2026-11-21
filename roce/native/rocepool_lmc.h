/* rocepool_lmc.h -- asynchronous batched KV-cache store for inference
 * engines, on the native KV client (rocepool_kv.h).
 *
 * It implements the contract of LMCache's native storage connectors
 * (event fd + batched get/set/exists/delete + drained completions); the
 * Python side is roce/lmcache_connector.py. Nothing in it is specific to
 * LMCache: any engine that offloads KV-cache blocks by key can drive it.
 *
 * Values: one KV-cache chunk per key, of any size. A chunk is stored as
 * parts of at most part_bytes ("<key>#0", "<key>#1", ...) spread over the
 * OSTs by hash -- a large chunk is striped and moved by all OSTs in
 * parallel -- and, once every part is stored, a 24-byte commit record
 * under "<key>" itself. Only committed chunks exist for EXISTS, so a
 * half-written chunk is never reported; a GET reads the parts straight
 * into the caller's buffer (its size says how many parts there are).
 *
 * Zero copy: rplmc_register() a buffer region (the engine's pinned
 * KV-cache arena) once; every transfer from/to it is then a one-sided RDMA
 * READ into it (get) or an RDMA READ by the OST out of it (set). Buffers
 * outside registered regions work too, through a copy.
 *
 * Thread-safe. Every submitted operation completes exactly once (also on
 * close: queued operations are finished first). */
#ifndef ROCEPOOL_LMC_H
#define ROCEPOOL_LMC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rplmc rplmc_t;

enum { RPLMC_GET = 0, RPLMC_SET = 1, RPLMC_EXISTS = 2, RPLMC_DELETE = 3 };

/* osts: as for rpkv_open; ns: key namespace (NULL = "lmcache/"); workers:
 * operations run at once (<= 0: 4); part_bytes: largest part (0: 4 MiB;
 * capped below half a frame). NULL on a bad OST list. */
rplmc_t *rplmc_open(const char *osts, const char *ns, int workers, size_t part_bytes);
/* finishes the queued operations, then frees everything (also the
 * completions nobody popped) */
void rplmc_close(rplmc_t *c);

/* readable (counter > 0) while completions wait to be popped */
int rplmc_event_fd(const rplmc_t *c);
/* consume the event fd's counter (call before popping) */
void rplmc_ack(rplmc_t *c);

/* 1: registered with the NIC (zero copy); 0: accepted, transfers copy;
 * negative errno on bad arguments or too many regions */
int rplmc_register(rplmc_t *c, void *base, size_t len);

/* queue an operation; returns its id (>= 0) or a negative errno. keys are
 * copied; bufs/lens (GET, SET) must stay valid until it completes. */
int64_t rplmc_submit(rplmc_t *c, int op, size_t n, const char *const *keys, void *const *bufs, const size_t *lens);

/* pop one completion: 1 popped, 0 none waiting, -ENOSPC results too small
 * (*n set to what is needed, nothing popped). results[k] = 1 / 0 per key
 * (GET: loaded; SET: stored; EXISTS: present; DELETE: deleted); *ok = 1
 * when every key succeeded (EXISTS: when the lookup itself worked). */
int rplmc_pop(rplmc_t *c, uint64_t *id, int *op, int *ok, size_t *n, uint8_t *results, size_t cap, char *err,
              size_t errcap);

/* counters: [0] operations done, [1] keys, [2] bytes stored, [3] bytes
 * loaded, [4] bytes moved zero-copy, [5] bytes copied, [6] parts failed,
 * [7] registered regions, [8] chunks committed, [9] part size in bytes */
#define RPLMC_NSTATS 10
void rplmc_stats(rplmc_t *c, uint64_t out[RPLMC_NSTATS]);

#ifdef __cplusplus
}
#endif
#endif
