/* rocepool_client.h -- the native client core shared by the FUSE daemon
 * (rocepool_fuse.c) and the KV library (rocepool_kv.c): pooled RPC to one
 * peer over roce_transport, the OST operations incl. the "thick KV" ones,
 * BATCH frames, and the sha256 placement hash. Wire formats: roce/protocol.py.
 */
#ifndef ROCEPOOL_CLIENT_H
#define ROCEPOOL_CLIENT_H

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include "roce_transport.h"

enum {
    RP_PUT = 1, RP_PUT_OK = 2, RP_GET = 3, RP_GET_OK = 4, RP_DELETE = 5, RP_DELETE_OK = 6,
    RP_STAT = 7, RP_STAT_OK = 8,
    RP_WRITE_AT = 11, RP_WRITE_AT_OK = 12, RP_GET_V = 13, RP_GET_V_OK = 14,
    RP_READ_AT = 15, RP_READ_AT_OK = 16, RP_TRUNCATE = 17, RP_TRUNCATE_OK = 18,
    RP_MDT = 30, RP_MDT_OK = 31, RP_MDT_OK_TX = 32, RP_BATCH = 40, RP_BATCH_OK = 41, RP_VTOKEN = 42, RP_VTOKEN_OK = 43,
    RP_LEASE = 44, RP_LEASE_OK = 45,
    RP_ERROR = 255,
};

#define RP_POOL_IDLE_MAX 16

typedef struct {
    char addr[128];
    pthread_mutex_t lk;
    roce_conn_t *idle[RP_POOL_IDLE_MAX];
    double idle_at[RP_POOL_IDLE_MAX];
    int nidle;
    atomic_ullong rpcs;
    atomic_int no_lease;   /* this server has no one-sided read path (set on first NO_LEASE/UNSUPPORTED) */
} rp_peer_t;

typedef struct {
    roce_reply_t reply; /* the reply's registered buffer (the OST wrote it in place); rp_resp_free */
    uint8_t *raw;       /* == reply.data: the whole reply frame */
    uint8_t op;
    const uint8_t *val;
    uint32_t vlen;
    char code[32];      /* error code when op == RP_ERROR */
} rp_resp_t;

/* a request body segment; mem NULL = look it up (arena memory) or copy */
typedef struct { const void *data; size_t len; roce_mem_t *mem; } rp_seg_t;

/* Registered bulk memory: a request body allocated here is RDMA READ by
 * the OST straight from this address (no copy on the client). Power-of-two
 * blocks from 4 KiB to 16 MiB out of 64 MiB chunks registered once. */
void *rp_bulk_alloc(size_t n);
void rp_bulk_free(void *p);              /* also accepts ordinary malloc'd memory */
roce_mem_t *rp_bulk_mem(const void *p, size_t n);
/* arena size / in use, and request-body bytes sent in place vs copied first */
void rp_bulk_stats(uint64_t *bytes, uint64_t *in_use, uint64_t *zero_copy_bytes, uint64_t *copied_bytes);

extern size_t rp_frame_bytes;   /* max frame, default 16 MiB (env ROCE_MAX_FRAME_BYTES) */
extern int rp_timeout_ms;       /* per request, default 5000 */

void rp_peer_init(rp_peer_t *p, const char *addr);
void rp_peer_close_idle(rp_peer_t *p);
/* the request header's flag byte for this thread's next rp_rpc / rp_rpc_v
 * (then cleared): RP_RQF_ASYNC, RP_RQF_REPLAY (rocepool_servers.c) */
#define RP_RQF_ASYNC 1
#define RP_RQF_REPLAY 2
extern __thread uint8_t rp_next_flags;

/* value = v1 ++ v2. 0 = reply in *out (maybe RP_ERROR), -EIO/-EFBIG otherwise. */
int rp_rpc(rp_peer_t *p, uint8_t op, const char *key, const void *v1, size_t n1, const void *v2, size_t n2,
           rp_resp_t *out);
/* value = v1 ++ segs[0] ++ ... ++ segs[nsegs-1] (nsegs <= 4) */
int rp_rpc_v(rp_peer_t *p, uint8_t op, const char *key, const void *v1, size_t n1, const rp_seg_t *segs, int nsegs,
             rp_resp_t *out);
void rp_resp_free(rp_resp_t *r);
uint64_t rp_sha256_u64(const char *s);   /* int.from_bytes(sha256(s)[:8], "big") */
double rp_mono_s(void);

/* OST ops. Return 0, -ENOENT, -EAGAIN (version conflict) or -EIO. */
int rp_ost_put(rp_peer_t *p, const char *key, const void *data, size_t len, int64_t expected, uint64_t *ver);
/* ABA-safe CAS (roce/protocol.py VTOKEN): the token names one state of the
 * key -- its version, or its delete watermark when absent -- and is never
 * reused, so put_token cannot land after a delete/recreate in between. */
int rp_ost_vtoken(rp_peer_t *p, const char *key, uint64_t *token);
int rp_ost_put_token(rp_peer_t *p, const char *key, const void *data, size_t len, uint64_t token, uint64_t *ver);
int rp_ost_get(rp_peer_t *p, const char *key, rp_resp_t *out);                 /* data = out->val */
int rp_ost_stat_version(rp_peer_t *p, const char *key, uint64_t *ver);
int rp_ost_delete(rp_peer_t *p, const char *key);
int rp_ost_write_at(rp_peer_t *p, const char *key, uint64_t off, const void *data, size_t len,
                    uint64_t *ver, uint64_t *size);
/* the same with the data in up to 4 segments (a stripe assembled from several writes) */
int rp_ost_put_v(rp_peer_t *p, const char *key, const rp_seg_t *segs, int nsegs, int64_t expected, uint64_t *ver);
int rp_ost_write_at_v(rp_peer_t *p, const char *key, uint64_t off, const rp_seg_t *segs, int nsegs,
                      uint64_t *ver, uint64_t *size);
/* READ_AT (len = bytes wanted) / GET_V (len ignored when whole = 1): data = out->val + 8, out->vlen - 8 bytes */
int rp_ost_read_at(rp_peer_t *p, const char *key, uint64_t off, uint32_t len, int whole, uint64_t *ver,
                   rp_resp_t *out);
int rp_ost_truncate(rp_peer_t *p, const char *key, uint64_t size);

/* One-sided reads (the OST's CPU is not involved). A lease describes the
 * object's registered RAM copy; rp_ost_read_direct RDMA READs a range of
 * it into arena memory and checks the object's seqlock (header, data,
 * trailer read in order): 0 = bytes valid; -EAGAIN = the object changed
 * or moved (take a new lease, or read by RPC); -ENOTSUP from rp_ost_lease
 * = this server has no one-sided path (the Python one). */
typedef struct { uint64_t version, size, addr, seq, keyhash; uint32_t rkey; } rp_lease_t;
void rp_stride_ra_stats(uint64_t *issued, uint64_t *hits, uint64_t *wasted);  /* (rocepool_fs.c) stride readahead */
void rp_lookup_stats(uint64_t *direct, uint64_t *misses);   /* (rocepool_fs.c) one-sided MDT lookups / misses */
int rp_ost_lease(rp_peer_t *p, const char *key, rp_lease_t *out);
int rp_ost_read_direct(rp_peer_t *p, const rp_lease_t *l, uint64_t off, size_t len, void *dst);
int rp_ost_read_direct_mem(rp_peer_t *p, const rp_lease_t *l, uint64_t off, size_t len, void *dst, roce_mem_t *dmem);
/* raw one-sided reads over a peer's pooled connection (roce_read_t) */
typedef roce_read_t rp_read_t;
int rp_rdma_read(rp_peer_t *p, const rp_read_t *reads, int n);
/* one-sided reads done / bytes / leases taken / stale leases */
void rp_direct_stats(uint64_t *reads, uint64_t *bytes, uint64_t *leases, uint64_t *stale);

/* BATCH: build with rp_batch_add, send with rp_batch_send, read reply i with rp_batch_reply. */
typedef struct {
    uint8_t *buf;
    size_t len, cap;
    uint32_t n;
} rp_batch_t;
void rp_batch_init(rp_batch_t *b);
void rp_batch_add(rp_batch_t *b, uint8_t op, const char *key, const void *v1, size_t n1, const void *v2, size_t n2);
void rp_batch_free(rp_batch_t *b);
int rp_batch_send(rp_peer_t *p, rp_batch_t *b, rp_resp_t *out);
/* walks the reply: *pos starts at 0; returns 0 and fills op/val/vlen/code, or -1 at the end */
int rp_batch_next(const rp_resp_t *r, size_t *pos, uint8_t *op, const uint8_t **val, uint32_t *vlen, char *code,
                  size_t code_len);

#endif
