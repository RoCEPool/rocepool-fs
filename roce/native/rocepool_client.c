/* rocepool_client.c -- native client core, see rocepool_client.h. */
#define _GNU_SOURCE
#include "rocepool_client.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>

size_t rp_frame_bytes = 16u << 20;
/* per-RPC reply timeout. A dead server is noticed by the transport (the
 * connection closes, or RC retries run out) long before this; the timeout
 * only catches a server that is alive but not answering. It must outlast
 * a busy one: on site an OST writing to a slow drive queued 1 MiB writes
 * for > 5 s (the old value) and every one of them became EIO. */
int rp_timeout_ms = 60000;
static atomic_ullong g_reqid = 1;

double rp_mono_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* ---------------------------------------------------------------- sha256
 * Needed bit-for-bit: MDT placement, stripe offsets and KV placement are
 * sha256-based on the Python side (hashlib). FIPS 180-4. */
static const uint32_t K256[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

#define ROR(x, n) (((x) >> (n)) | ((x) << (32 - (n))))

static void sha256_block(uint32_t h[8], const uint8_t *p) {
    uint32_t w[64];
    for (int i = 0; i < 16; i++)
        w[i] = (uint32_t)p[4 * i] << 24 | (uint32_t)p[4 * i + 1] << 16 | (uint32_t)p[4 * i + 2] << 8 | p[4 * i + 3];
    for (int i = 16; i < 64; i++) {
        uint32_t s0 = ROR(w[i - 15], 7) ^ ROR(w[i - 15], 18) ^ (w[i - 15] >> 3);
        uint32_t s1 = ROR(w[i - 2], 17) ^ ROR(w[i - 2], 19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16] + s0 + w[i - 7] + s1;
    }
    uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], hh = h[7];
    for (int i = 0; i < 64; i++) {
        uint32_t t1 = hh + (ROR(e, 6) ^ ROR(e, 11) ^ ROR(e, 25)) + ((e & f) ^ (~e & g)) + K256[i] + w[i];
        uint32_t t2 = (ROR(a, 2) ^ ROR(a, 13) ^ ROR(a, 22)) + ((a & b) ^ (a & c) ^ (b & c));
        hh = g; g = f; f = e; e = d + t1; d = c; c = b; b = a; a = t1 + t2;
    }
    h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e; h[5] += f; h[6] += g; h[7] += hh;
}

uint64_t rp_sha256_u64(const char *s) {
    uint32_t h[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    size_t n = strlen(s), i = 0;
    uint8_t blk[64];
    for (; i + 64 <= n; i += 64) sha256_block(h, (const uint8_t *)s + i);
    size_t rem = n - i;
    memset(blk, 0, 64);
    memcpy(blk, s + i, rem);
    blk[rem] = 0x80;
    if (rem >= 56) { sha256_block(h, blk); memset(blk, 0, 64); }
    uint64_t bits = (uint64_t)n * 8;
    for (int k = 0; k < 8; k++) blk[63 - k] = (uint8_t)(bits >> (8 * k));
    sha256_block(h, blk);
    return (uint64_t)h[0] << 32 | h[1];
}

/* ---------------------------------------------------------------- bulk arena
 * Registered memory for request bodies (rocepool_client.h rp_bulk_*): a
 * body that lives here is RDMA READ by the OST straight from where it is,
 * so a client that keeps its data in arena memory never copies it on the
 * way out. Chunks are mmap'd 64 MiB regions registered once per RDMA
 * device (lazily, by the transport); each chunk serves one power-of-two
 * size class from 4 KiB up, so a chunk's blocks never fragment. */
#define RP_ARENA_CHUNK (64u << 20)
#define RP_ARENA_MAX_CHUNKS 4096
/* block sizes: powers of two 4 KiB .. 16 MiB (one frame), plus one class
 * for the FUSE daemon's request buffer (1 MiB write + headers) so those do
 * not round up to 2 MiB */
static const size_t g_classes[] = { 4u << 10, 8u << 10, 16u << 10, 32u << 10, 64u << 10, 128u << 10, 256u << 10,
                                    512u << 10, 1u << 20, (1u << 20) + (64u << 10), 2u << 20, 4u << 20, 8u << 20,
                                    16u << 20 };
#define RP_ARENA_CLASSES ((int)(sizeof(g_classes) / sizeof(g_classes[0])))

typedef struct { uint8_t *base; size_t len; int cls; roce_mem_t *mem; } rp_chunk_t;
typedef struct rp_free { struct rp_free *next; } rp_free_t;

static pthread_mutex_t g_arena_lk = PTHREAD_MUTEX_INITIALIZER;
static rp_chunk_t g_chunks[RP_ARENA_MAX_CHUNKS];
static int g_nchunks;
static rp_free_t *g_free[RP_ARENA_CLASSES];
static atomic_ullong g_arena_bytes, g_arena_in_use;
static atomic_ullong g_zc_bytes, g_copied_bytes;  /* request body bytes sent in place / copied first */

static int arena_class(size_t n) {
    int c = 0;
    while (c < RP_ARENA_CLASSES - 1 && g_classes[c] < n) c++;
    return c;
}

/* caller holds g_arena_lk */
static int arena_grow(int cls) {
    if (g_nchunks == RP_ARENA_MAX_CHUNKS) return -1;
    size_t blk = g_classes[cls];
    size_t len = blk > RP_ARENA_CHUNK ? blk : RP_ARENA_CHUNK;
    uint8_t *base = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (base == MAP_FAILED) return -1;
    roce_mem_t *mem = roce_mem_register(base, len);
    if (!mem) { munmap(base, len); return -1; }
    g_chunks[g_nchunks++] = (rp_chunk_t){ base, len, cls, mem };
    for (size_t off = 0; off + blk <= len; off += blk) {
        rp_free_t *f = (rp_free_t *)(base + off);
        f->next = g_free[cls];
        g_free[cls] = f;
    }
    atomic_fetch_add(&g_arena_bytes, len);
    return 0;
}

void *rp_bulk_alloc(size_t n) {
    if (n > g_classes[RP_ARENA_CLASSES - 1]) return NULL;
    int cls = arena_class(n ? n : 1);
    pthread_mutex_lock(&g_arena_lk);
    if (!g_free[cls] && arena_grow(cls)) { pthread_mutex_unlock(&g_arena_lk); return NULL; }
    rp_free_t *f = g_free[cls];
    g_free[cls] = f->next;
    pthread_mutex_unlock(&g_arena_lk);
    atomic_fetch_add(&g_arena_in_use, g_classes[cls]);
    return f;
}

static rp_chunk_t *arena_chunk_of(const void *p) {  /* caller holds g_arena_lk */
    const uint8_t *a = p;
    for (int i = 0; i < g_nchunks; i++)
        if (a >= g_chunks[i].base && a < g_chunks[i].base + g_chunks[i].len) return &g_chunks[i];
    return NULL;
}

void rp_bulk_free(void *p) {
    if (!p) return;
    pthread_mutex_lock(&g_arena_lk);
    rp_chunk_t *c = arena_chunk_of(p);
    if (c) {
        rp_free_t *f = p;
        f->next = g_free[c->cls];
        g_free[c->cls] = f;
        atomic_fetch_sub(&g_arena_in_use, g_classes[c->cls]);
    }
    pthread_mutex_unlock(&g_arena_lk);
    if (!c) free(p);  /* not arena memory: an ordinary allocation */
}

/* the registration covering [p, p+n), or NULL when p is not arena memory */
roce_mem_t *rp_bulk_mem(const void *p, size_t n) {
    pthread_mutex_lock(&g_arena_lk);
    rp_chunk_t *c = arena_chunk_of(p);
    roce_mem_t *m = c && (const uint8_t *)p + n <= c->base + c->len ? c->mem : NULL;
    pthread_mutex_unlock(&g_arena_lk);
    return m;
}

void rp_bulk_stats(uint64_t *bytes, uint64_t *in_use, uint64_t *zero_copy_bytes, uint64_t *copied_bytes) {
    if (bytes) *bytes = atomic_load(&g_arena_bytes);
    if (in_use) *in_use = atomic_load(&g_arena_in_use);
    if (zero_copy_bytes) *zero_copy_bytes = atomic_load(&g_zc_bytes);
    if (copied_bytes) *copied_bytes = atomic_load(&g_copied_bytes);
}

/* ---------------------------------------------------------------- pooled RPC */
void rp_peer_init(rp_peer_t *p, const char *addr) {
    memset(p, 0, sizeof(*p));
    snprintf(p->addr, sizeof(p->addr), "%s", addr);
    pthread_mutex_init(&p->lk, NULL);
    const char *fe = getenv("ROCE_MAX_FRAME_BYTES");
    if (fe && atol(fe) > 0) rp_frame_bytes = (size_t)atol(fe);
}

void rp_peer_close_idle(rp_peer_t *p) {
    pthread_mutex_lock(&p->lk);
    while (p->nidle > 0) roce_client_close(p->idle[--p->nidle]);
    pthread_mutex_unlock(&p->lk);
}

static roce_conn_t *peer_checkout(rp_peer_t *p) {
    roce_conn_t *c = NULL, *stale[RP_POOL_IDLE_MAX];
    int ns = 0;
    double now = rp_mono_s();
    pthread_mutex_lock(&p->lk);
    while (p->nidle > 0) {
        p->nidle--;
        if (now - p->idle_at[p->nidle] > 30.0) { stale[ns++] = p->idle[p->nidle]; continue; }
        c = p->idle[p->nidle];
        break;
    }
    pthread_mutex_unlock(&p->lk);
    for (int i = 0; i < ns; i++) roce_client_close(stale[i]);
    return c;
}

static void peer_checkin(rp_peer_t *p, roce_conn_t *c) {
    pthread_mutex_lock(&p->lk);
    if (p->nidle < RP_POOL_IDLE_MAX) {
        p->idle[p->nidle] = c;
        p->idle_at[p->nidle] = rp_mono_s();
        p->nidle++;
        c = NULL;
    }
    pthread_mutex_unlock(&p->lk);
    if (c) roce_client_close(c);
}

void rp_resp_free(rp_resp_t *r) {
    roce_reply_release(&r->reply);
    r->raw = NULL;
}

/* Retry rule (same as the Python client): only a request that provably
 * never left this host (ROCE_ERR_NOT_DELIVERED) is re-sent, on a fresh
 * connection; anything after the SEND was posted is reported.
 *
 * The frame is head (header + key + v1, copied into the SEND) followed by
 * the body segments. A segment in arena memory (rp_bulk_alloc) goes
 * without a copy: the OST RDMA READs it in place; any other segment is
 * copied once into a registered buffer by the transport. The reply lands
 * in a registered buffer the caller keeps until rp_resp_free. */
__thread uint8_t rp_next_flags;

int rp_rpc_v(rp_peer_t *p, uint8_t op, const char *key, const void *v1, size_t n1, const rp_seg_t *segs, int nsegs,
             rp_resp_t *out) {
    size_t klen = strlen(key), body = 0;
    for (int i = 0; i < nsegs; i++) body += segs[i].len;
    size_t head_len = 24 + klen + n1, len = head_len + body;
    if (len > rp_frame_bytes || nsegs > 4) return -EFBIG;
    uint8_t stack[4096];
    uint8_t *buf = head_len <= sizeof(stack) ? stack : malloc(head_len);
    if (!buf) return -ENOMEM;
    uint64_t id = atomic_fetch_add(&g_reqid, 1);
    uint32_t kl = (uint32_t)klen, vl = (uint32_t)(n1 + body);
    memcpy(buf, "RCP1", 4);
    buf[4] = op;
    buf[5] = rp_next_flags;
    rp_next_flags = 0;
    buf[6] = buf[7] = 0;
    memcpy(buf + 8, &id, 8);   /* little-endian hosts only (x86_64/aarch64) */
    memcpy(buf + 16, &kl, 4);
    memcpy(buf + 20, &vl, 4);
    memcpy(buf + 24, key, klen);
    if (n1) memcpy(buf + 24 + klen, v1, n1);
    roce_seg_t rs[4];
    for (int i = 0; i < nsegs; i++) {
        rs[i] = (roce_seg_t){ segs[i].data, segs[i].len, segs[i].mem ? segs[i].mem : rp_bulk_mem(segs[i].data, segs[i].len) };
        if (segs[i].len > 0) atomic_fetch_add(rs[i].mem ? &g_zc_bytes : &g_copied_bytes, segs[i].len);
    }
    atomic_fetch_add(&p->rpcs, 1);
    int rc = -EIO;
    for (int attempt = 0; attempt < 1 + RP_POOL_IDLE_MAX; attempt++) {
        roce_conn_t *c = peer_checkout(p);
        if (!c) c = roce_client_connect_shared(p->addr, rp_timeout_ms < 10000 ? rp_timeout_ms : 10000, rp_frame_bytes);
        if (!c) {
            fprintf(stderr, "rocepool: connect %s: %s\n", p->addr, roce_last_error());
            break;
        }
        roce_reply_t reply;
        int s = roce_client_request(c, buf, head_len, rs, nsegs, &reply, rp_timeout_ms);
        if (s != 0) {
            roce_client_close(c);
            if (s == ROCE_ERR_NOT_DELIVERED) continue;
            fprintf(stderr, "rocepool: %s op %u key %.80s: %s\n", p->addr, op, key, roce_last_error());
            break;
        }
        uint8_t *r = reply.data;
        size_t rl = reply.len;
        uint64_t rid;
        uint32_t rkl, rvl;
        if (rl < 24 || memcmp(r, "RCP1", 4)) { roce_reply_release(&reply); roce_client_close(c); break; }
        memcpy(&rid, r + 8, 8);
        memcpy(&rkl, r + 16, 4);
        memcpy(&rvl, r + 20, 4);
        if (rid != id || 24 + (size_t)rkl + rvl != rl) { roce_reply_release(&reply); roce_client_close(c); break; }
        peer_checkin(p, c);
        out->reply = reply;
        out->raw = r;
        out->op = r[4];
        out->val = r + 24 + rkl;
        out->vlen = rvl;
        out->code[0] = 0;
        if (out->op == RP_ERROR) {
            size_t n = 0;
            while (n < rvl && n < sizeof(out->code) - 1 && out->val[n]) { out->code[n] = (char)out->val[n]; n++; }
            out->code[n] = 0;
        }
        rc = 0;
        break;
    }
    if (buf != stack) free(buf);
    return rc;
}

int rp_rpc(rp_peer_t *p, uint8_t op, const char *key, const void *v1, size_t n1, const void *v2, size_t n2,
           rp_resp_t *out) {
    rp_seg_t seg = { v2, n2, NULL };
    return rp_rpc_v(p, op, key, v1, n1, n2 ? &seg : NULL, n2 ? 1 : 0, out);
}

/* ---------------------------------------------------------------- OST ops */
static int err_of(const rp_resp_t *r) {
    if (r->op != RP_ERROR) return -EIO;
    if (!strcmp(r->code, "NOT_FOUND")) return -ENOENT;
    if (!strcmp(r->code, "VERSION_CONFLICT")) return -EAGAIN;
    if (!strcmp(r->code, "NO_SPACE")) return -ENOSPC;  /* the OST's tier is full */
    return -EIO;
}

static uint64_t be64(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = v << 8 | p[i];
    return v;
}

static int ost_put_marker_v(rp_peer_t *p, const char *key, const rp_seg_t *segs, int nsegs, uint8_t marker,
                            uint64_t e, uint64_t *ver) {
    uint8_t hdr[9];
    hdr[0] = marker;
    for (int i = 0; i < 8; i++) hdr[1 + i] = (uint8_t)(e >> (56 - 8 * i));  /* big-endian */
    rp_resp_t r;
    int rc = rp_rpc_v(p, RP_PUT, key, hdr, 9, segs, nsegs, &r);
    if (rc) return rc;
    if (r.op == RP_PUT_OK && r.vlen >= 8) {
        if (ver) *ver = be64(r.val);
        rc = 0;
    } else {
        rc = err_of(&r);
    }
    rp_resp_free(&r);
    return rc;
}

static int ost_put_marker(rp_peer_t *p, const char *key, const void *data, size_t len, uint8_t marker,
                          uint64_t e, uint64_t *ver) {
    rp_seg_t seg = { data, len, NULL };
    return ost_put_marker_v(p, key, len ? &seg : NULL, len ? 1 : 0, marker, e, ver);
}

int rp_ost_put(rp_peer_t *p, const char *key, const void *data, size_t len, int64_t expected, uint64_t *ver) {
    return ost_put_marker(p, key, data, len, expected < 0 ? 0 : 1, expected < 0 ? 0 : (uint64_t)expected, ver);
}

int rp_ost_put_v(rp_peer_t *p, const char *key, const rp_seg_t *segs, int nsegs, int64_t expected, uint64_t *ver) {
    return ost_put_marker_v(p, key, segs, nsegs, expected < 0 ? 0 : 1, expected < 0 ? 0 : (uint64_t)expected, ver);
}

int rp_ost_put_token(rp_peer_t *p, const char *key, const void *data, size_t len, uint64_t token, uint64_t *ver) {
    return ost_put_marker(p, key, data, len, 2, token, ver);
}

int rp_ost_vtoken(rp_peer_t *p, const char *key, uint64_t *token) {
    rp_resp_t r;
    int rc = rp_rpc(p, RP_VTOKEN, key, NULL, 0, NULL, 0, &r);
    if (rc) return rc;
    if (r.op == RP_VTOKEN_OK && r.vlen >= 8) {
        *token = be64(r.val);
        rc = 0;
    } else {
        rc = err_of(&r);
    }
    rp_resp_free(&r);
    return rc;
}

int rp_ost_get(rp_peer_t *p, const char *key, rp_resp_t *out) {
    int rc = rp_rpc(p, RP_GET, key, NULL, 0, NULL, 0, out);
    if (rc) return rc;
    if (out->op == RP_GET_OK) return 0;
    rc = err_of(out);
    rp_resp_free(out);
    return rc;
}

int rp_ost_stat_version(rp_peer_t *p, const char *key, uint64_t *ver) {
    rp_resp_t r;
    int rc = rp_rpc(p, RP_STAT, key, NULL, 0, NULL, 0, &r);
    if (rc) return rc;
    if (r.op == RP_STAT_OK) {
        /* JSON body (Python json.dumps or the native server): find "version" */
        char tmp[512];
        size_t n = r.vlen < sizeof(tmp) - 1 ? r.vlen : sizeof(tmp) - 1;
        memcpy(tmp, r.val, n);
        tmp[n] = 0;
        char *v = strstr(tmp, "\"version\"");
        rc = -EIO;
        if (v && (v = strchr(v, ':'))) {
            *ver = strtoull(v + 1, NULL, 10);
            rc = 0;
        }
    } else {
        rc = err_of(&r);
    }
    rp_resp_free(&r);
    return rc;
}

int rp_ost_delete(rp_peer_t *p, const char *key) {
    rp_resp_t r;
    int rc = rp_rpc(p, RP_DELETE, key, NULL, 0, NULL, 0, &r);
    if (rc) return rc;
    rc = r.op == RP_DELETE_OK ? 0 : err_of(&r);
    rp_resp_free(&r);
    return rc;
}

int rp_ost_write_at_v(rp_peer_t *p, const char *key, uint64_t off, const rp_seg_t *segs, int nsegs,
                      uint64_t *ver, uint64_t *size) {
    rp_resp_t r;
    int rc = rp_rpc_v(p, RP_WRITE_AT, key, &off, 8, segs, nsegs, &r);
    if (rc) return rc;
    if (r.op == RP_WRITE_AT_OK && r.vlen >= 16) {
        if (ver) *ver = be64(r.val);
        if (size) *size = be64(r.val + 8);
        rc = 0;
    } else {
        rc = err_of(&r);
    }
    rp_resp_free(&r);
    return rc;
}

int rp_ost_write_at(rp_peer_t *p, const char *key, uint64_t off, const void *data, size_t len,
                    uint64_t *ver, uint64_t *size) {
    rp_seg_t seg = { data, len, NULL };
    return rp_ost_write_at_v(p, key, off, len ? &seg : NULL, len ? 1 : 0, ver, size);
}

int rp_ost_read_at(rp_peer_t *p, const char *key, uint64_t off, uint32_t len, int whole, uint64_t *ver,
                   rp_resp_t *out) {
    uint8_t v[12];
    memcpy(v, &off, 8);
    memcpy(v + 8, &len, 4);
    int rc = whole ? rp_rpc(p, RP_GET_V, key, NULL, 0, NULL, 0, out) : rp_rpc(p, RP_READ_AT, key, v, 12, NULL, 0, out);
    if (rc) return rc;
    if ((out->op == RP_READ_AT_OK || out->op == RP_GET_V_OK) && out->vlen >= 8) {
        if (ver) *ver = be64(out->val);
        return 0;
    }
    rc = err_of(out);
    rp_resp_free(out);
    return rc;
}

int rp_ost_truncate(rp_peer_t *p, const char *key, uint64_t size) {
    rp_resp_t r;
    int rc = rp_rpc(p, RP_TRUNCATE, key, &size, 8, NULL, 0, &r);
    if (rc) return rc;
    rc = r.op == RP_TRUNCATE_OK ? 0 : err_of(&r);
    rp_resp_free(&r);
    return rc;
}

/* ------------------------------------------------------ one-sided reads */
static atomic_ullong g_direct_reads, g_direct_bytes, g_leases, g_stale;

void rp_direct_stats(uint64_t *reads, uint64_t *bytes, uint64_t *leases, uint64_t *stale) {
    *reads = atomic_load(&g_direct_reads);
    *bytes = atomic_load(&g_direct_bytes);
    *leases = atomic_load(&g_leases);
    *stale = atomic_load(&g_stale);
}

int rp_ost_lease(rp_peer_t *p, const char *key, rp_lease_t *out) {
    rp_resp_t r;
    int rc = rp_rpc(p, RP_LEASE, key, NULL, 0, NULL, 0, &r);
    if (rc) return rc;
    if (r.op == RP_LEASE_OK && r.vlen >= 44) {
        out->version = be64(r.val);
        out->size = be64(r.val + 8);
        out->addr = be64(r.val + 16);
        out->seq = be64(r.val + 24);
        out->keyhash = be64(r.val + 32);
        out->rkey = ((uint32_t)r.val[40] << 24) | ((uint32_t)r.val[41] << 16) | ((uint32_t)r.val[42] << 8) | r.val[43];
        rp_resp_free(&r);
        atomic_fetch_add(&g_leases, 1);
        return 0;
    }
    rc = r.op == RP_ERROR && (!strcmp(r.code, "UNSUPPORTED_OP") || !strcmp(r.code, "NO_LEASE")) ? -ENOTSUP : err_of(&r);
    rp_resp_free(&r);
    return rc;
}

int rp_rdma_read(rp_peer_t *p, const rp_read_t *reads, int n) {
    for (int attempt = 0; attempt < 1 + RP_POOL_IDLE_MAX; attempt++) {
        roce_conn_t *c = peer_checkout(p);
        if (!c) c = roce_client_connect_shared(p->addr, rp_timeout_ms < 10000 ? rp_timeout_ms : 10000, rp_frame_bytes);
        if (!c) return -EIO;
        int s = roce_client_rdma_read(c, reads, n, rp_timeout_ms);
        if (s != 0) {
            roce_client_close(c);
            if (s == ROCE_ERR_NOT_DELIVERED) continue;
            return -EIO;
        }
        peer_checkin(p, c);
        return 0;
    }
    return -EIO;
}

/* header (32 B) + trailer (8 B) scratch in registered memory, per thread */
static __thread uint8_t *tl_scratch;

int rp_ost_read_direct(rp_peer_t *p, const rp_lease_t *l, uint64_t off, size_t len, void *dst) {
    return rp_ost_read_direct_mem(p, l, off, len, dst, NULL);
}

/* dmem: the registered region dst lies in -- device (GPU) memory included:
 * the data lands there straight from the OST's RAM, only the 40 bytes of
 * seqlock header and trailer come into host scratch. NULL: arena memory. */
int rp_ost_read_direct_mem(rp_peer_t *p, const rp_lease_t *l, uint64_t off, size_t len, void *dst, roce_mem_t *dmem) {
    if (off + len > l->size) return -EAGAIN;
    if (!tl_scratch) { tl_scratch = rp_bulk_alloc(4096); if (!tl_scratch) return -ENOMEM; }
    roce_mem_t *smem = rp_bulk_mem(tl_scratch, 4096);
    if (!dmem) dmem = rp_bulk_mem(dst, len);
    if (!smem || !dmem) return -EINVAL;  /* dst must be arena memory */
    memset(tl_scratch, 0, 48);
    roce_read_t reads[3] = {
        { l->addr, l->rkey, 32, smem, tl_scratch },
        { l->addr + 32 + off, l->rkey, (uint32_t)len, dmem, dst },
        { l->addr + 32 + l->size, l->rkey, 8, smem, tl_scratch + 32 },
    };
    int n = len ? 3 : 1;
    if (!len) reads[0].len = 32;
    int rc = -EIO;
    /* An ordered transport (RC) executes the chain header, data, trailer
       in order: header == trailer == lease.seq brackets the data. An
       unordered one (EFA's SRD) may run the three in any order, so read
       the data first and the header only after it completed: the lease's
       seq was observed by the OST before the data read, seqs are never
       reused, so an unchanged header afterwards means no writer touched
       the object in between. */
    int ordered = roce_transport_reads_ordered();
    for (int attempt = 0; attempt < 1 + RP_POOL_IDLE_MAX; attempt++) {
        roce_conn_t *c = peer_checkout(p);
        if (!c) c = roce_client_connect_shared(p->addr, rp_timeout_ms < 10000 ? rp_timeout_ms : 10000, rp_frame_bytes);
        if (!c) break;
        int s;
        if (ordered || !len) {
            s = roce_client_rdma_read(c, reads, n, rp_timeout_ms);
        } else {
            s = roce_client_rdma_read(c, &reads[1], 1, rp_timeout_ms);
            if (s == 0) s = roce_client_rdma_read(c, &reads[0], 1, rp_timeout_ms);
        }
        if (s != 0) {
            roce_client_close(c);
            if (s == ROCE_ERR_NOT_DELIVERED) continue;
            break;
        }
        peer_checkin(p, c);
        rc = 0;
        break;
    }
    if (rc) return rc;
    uint64_t seq, keyhash, size, tail;
    memcpy(&seq, tl_scratch, 8);
    memcpy(&keyhash, tl_scratch + 8, 8);
    memcpy(&size, tl_scratch + 16, 8);
    memcpy(&tail, tl_scratch + 32, 8);
    if (!ordered) tail = seq;  /* not read: the header after the data is the proof */
    if (seq != l->seq || (seq & 1) || keyhash != l->keyhash || size != l->size || (len && tail != seq)) {
        atomic_fetch_add(&g_stale, 1);
        return -EAGAIN;
    }
    atomic_fetch_add(&g_direct_reads, 1);
    atomic_fetch_add(&g_direct_bytes, len);
    return 0;
}

/* ---------------------------------------------------------------- BATCH */
void rp_batch_init(rp_batch_t *b) {
    b->cap = 4096;
    b->buf = malloc(b->cap);
    b->len = 4;
    b->n = 0;
}

void rp_batch_add(rp_batch_t *b, uint8_t op, const char *key, const void *v1, size_t n1, const void *v2, size_t n2) {
    uint32_t kl = (uint32_t)strlen(key), vl = (uint32_t)(n1 + n2);
    size_t need = b->len + 9 + kl + vl;
    if (need > b->cap) {
        while (b->cap < need) b->cap *= 2;
        b->buf = realloc(b->buf, b->cap);
    }
    uint8_t *q = b->buf + b->len;
    q[0] = op;
    memcpy(q + 1, &kl, 4);
    memcpy(q + 5, &vl, 4);
    memcpy(q + 9, key, kl);
    if (n1) memcpy(q + 9 + kl, v1, n1);
    if (n2) memcpy(q + 9 + kl + n1, v2, n2);
    b->len = need;
    b->n++;
}

void rp_batch_free(rp_batch_t *b) { free(b->buf); b->buf = NULL; }

int rp_batch_send(rp_peer_t *p, rp_batch_t *b, rp_resp_t *out) {
    memcpy(b->buf, &b->n, 4);
    int rc = rp_rpc(p, RP_BATCH, "", b->buf, b->len, NULL, 0, out);
    if (rc) return rc;
    if (out->op == RP_BATCH_OK && out->vlen >= 4) return 0;
    rc = err_of(out);
    rp_resp_free(out);
    return rc;
}

int rp_batch_next(const rp_resp_t *r, size_t *pos, uint8_t *op, const uint8_t **val, uint32_t *vlen, char *code,
                  size_t code_len) {
    size_t off = *pos ? *pos : 4;
    if (off + 5 > r->vlen) return -1;
    *op = r->val[off];
    memcpy(vlen, r->val + off + 1, 4);
    if (off + 5 + *vlen > r->vlen) return -1;
    *val = r->val + off + 5;
    if (code && code_len) {
        size_t n = 0;
        if (*op == RP_ERROR)
            while (n < *vlen && n < code_len - 1 && (*val)[n]) { code[n] = (char)(*val)[n]; n++; }
        code[n] = 0;
    }
    *pos = off + 5 + *vlen;
    return 0;
}
