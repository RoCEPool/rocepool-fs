#ifndef _GNU_SOURCE
#define _GNU_SOURCE  /* pthread_setname_np */
#endif
/* roce_transport.c -- RDMA/RoCE transport implementation.
 *
 * *** TESTING STATUS: compiled and RUN -- between two network nodes over
 * a back-to-back link, against softverbs (roce/native/softverbs/), a
 * strict userspace model of RC queue-pair semantics -- and compiled with
 * -Wall -Wextra -Werror and linked against the REAL rdma-core (v59.2)
 * headers and libraries. Not yet run on a real RDMA NIC: that is what
 * tools/roce_onsite.py does on site (docs/ROCE_DEPLOYMENT_NOTES.md).
 * Tier-B design (docs/ABC_UPGRADE.md): pooled persistent client
 * connections, servers receiving through a shared receive queue, frame
 * buffers from a process-wide registered pool, completion channels instead
 * of sleep-polling, CM events with timeouts, and a draining stop(). ***
 *
 * Written against:
 *   <rdma/rdma_cma.h>     -- librdmacm connection management
 *   <infiniband/verbs.h>  -- libibverbs queue pairs / memory regions / CQs
 * Both are part of the standard Linux rdma-core project and work
 * identically over RoCEv2 (Ethernet) or InfiniBand fabrics -- rdma_cma's
 * whole purpose is to make the connection-setup code fabric-agnostic; the
 * fabric type only affects which physical NIC gets selected during
 * address resolution.
 *
 * Pattern followed: the well-established rdma_cma client/server flow
 * used throughout rdma-core's own examples (librdmacm/examples/rping.c)
 * and standard RDMA programming references:
 *
 *   client: create_event_channel -> create_id -> resolve_addr (wait
 *           ADDR_RESOLVED) -> resolve_route (wait ROUTE_RESOLVED) ->
 *           alloc_pd -> create_cq -> create_qp -> reg_mr -> post initial
 *           recv buffers -> connect (wait ESTABLISHED)
 *
 *   server: create_event_channel -> create_id -> bind_addr -> listen ->
 *           [per incoming CONNECT_REQUEST: migrate the new id to its own
 *           event channel so it can be serviced by its own thread ->
 *           alloc_pd -> create_cq -> create_qp -> reg_mr -> post initial
 *           recv buffers -> accept (wait ESTABLISHED)]
 *
 * A RECV must always be posted before the peer's matching SEND can
 * arrive -- this is a hard RDMA correctness rule, not a style choice;
 * violating it drops the message with no error on the sender's side.
 * Every code path below posts a fresh RECV immediately after consuming
 * one, before the peer could plausibly send again.
 */
#include "roce_transport.h"

#define ROCE_LISTEN_BACKLOG 1024
/* Max connections one server holds open at once (override:
 * ROCE_SERVER_MAX_CONNS). Beyond it, clients wait at connection setup --
 * see struct conn_gate. Was 64 while every request opened its own
 * connection (then open connections == in-flight requests); with pooled
 * persistent client connections an open connection is mostly idle, so the
 * cap is on connections here and load is bounded by MAX_INFLIGHT. */
#define ROCE_SERVER_MAX_CONNS_DEFAULT 4096
/* Max requests being handled at once across all connections (override:
 * ROCE_SERVER_MAX_INFLIGHT). */
#define ROCE_SERVER_MAX_INFLIGHT_DEFAULT 64

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <sched.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <poll.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

/* ---------------------------------------------------------------------
 * Bulk data: server-driven RDMA (the Lustre/LNet bulk shape).
 *
 * Every request/response SEND from a client starts with an envelope
 * (struct rt_env): the bytes of the application frame that ride inline in
 * the SEND, then up to RT_MAX_SRC *source* descriptors (address, rkey,
 * length in the CLIENT's registered memory) for the rest of the frame,
 * and one *sink* descriptor: the client's reply buffer. The server
 *   - RDMA READs the sources straight into the buffer it hands its
 *     handler (a large PUT/WRITE_AT body never passes through a receive
 *     buffer, and the client never copies it into one either when the
 *     caller registered it: roce_mem_register + roce_client_request);
 *   - RDMA WRITEs a large reply into the sink, with immediate data, so
 *     the client's posted RECV completes with the reply already in its
 *     own buffer (roce_reply_t, handed to the caller as is), and a
 *     handler that built its reply in registered memory
 *     (roce_reply_alloc) is never copied.
 * Small frames go inline in the SEND / reply SEND as before. The receiver
 * of a SEND tells the two apart by the envelope's magic (an application
 * frame starts with its own), so stream sends and old clients still work.
 * On real hardware RDMA READ needs responder_resources / initiator_depth
 * on both sides (set in the CM parameters; softverbs enforces it too).
 * --------------------------------------------------------------------- */
#define RT_ENV_MAGIC 0x31564e45u  /* "ENV1" */
#define RT_MAX_SRC 4
struct rt_seg { uint64_t addr; uint32_t rkey; uint32_t len; };
struct rt_env {
    uint32_t magic;
    uint32_t inline_len;   /* frame bytes right after this header */
    uint32_t total_len;    /* whole frame: inline_len + every src len */
    uint32_t nsrc;
    struct rt_seg sink;    /* len 0: no sink (the reply must fit a SEND) */
    struct rt_seg src[RT_MAX_SRC];
};
#define RT_ENV_SIZE ((uint32_t)sizeof(struct rt_env))

#define ROCE_MAX_SEND_WR 4
#define ROCE_MAX_RECV_WR 4
#define ROCE_CQ_DEPTH 16
/* A server connection's CQ can hold as many receive completions as its
 * SRQ has buffers (a pipelining peer); SRQ depth is capped so every CQ and
 * the completion stash are sized for the worst case -- a CQ overrun is a
 * fatal async error on real hardware. */
#define ROCE_SRQ_DEPTH_MAX 256

/* rnr_retry_count = 7 means "retry indefinitely" on RNR (Receiver Not
 * Ready). Each connection has exactly one registered RECV buffer, re-posted
 * only after the previous message is consumed, so a peer that pipelines a
 * second SEND (RoCEStream does; so would any WLP-style stream) will
 * routinely arrive before the RECV is re-armed. With the default of 0 that
 * SEND fails with IBV_WC_RNR_RETRY_EXC_ERR and the QP goes to error --
 * which is exactly what happened the first time this ran between two
 * nodes (tools/run_crossover_rdma.py, stream phase). 7 is also what
 * rdma-core's own rping example uses. */
#ifndef ROCE_RNR_RETRY_INFINITE  /* overridable only so tests can build the old, broken value on purpose */
#define ROCE_RNR_RETRY_INFINITE 7
#endif

static __thread char g_last_error[512];

const char *roce_last_error(void) { return g_last_error; }

static void set_error(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_last_error, sizeof(g_last_error), fmt, ap);
    va_end(ap);
}

/* ---------------------------------------------------------------------
 * Shared per-connection RDMA resources (used by both client and each
 * server-side accepted connection).
 * --------------------------------------------------------------------- */
struct bufpool;
struct rbuf;

struct roce_qp_resources {
    struct rdma_cm_id *id;
    struct ibv_pd *pd;
    int own_pd;  /* dedicated mode allocates its own PD; shared mode uses the pool's */
    struct ibv_cq *cq;
    /* Completion channel: callers sleep in poll() on its fd until the CQ
     * has work (or a timeout), instead of the old 1 ms usleep loop -- which
     * cost every idle pooled connection ~1000 wakeups/s on the server and
     * added up to 1 ms to every request on both sides. */
    struct ibv_comp_channel *comp;
    int armed;  /* ibv_req_notify_cq issued and its event not consumed yet */
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    uint8_t *send_buf;
    uint8_t *recv_buf;
    size_t max_frame_bytes;
    /* One CQ serves both the send and the recv queue, so polling for one
     * can dequeue the other's completion first. Such completions are
     * parked here (indexed by wr_id: 1 = recv, 2 = send) instead of being
     * dropped -- dropping them was a real bug, found the first time a
     * pipelined stream ran between two nodes (the reply's RECV completion
     * was consumed while waiting for the request's SEND completion, and
     * the caller then waited for it forever). */
    /* FIFO per kind: with an SRQ several receives can complete on one
     * connection while its thread waits for a SEND completion (a pipelining
     * peer), so one slot per kind silently lost requests. */
#define ROCE_STASH (ROCE_SRQ_DEPTH_MAX + ROCE_CQ_DEPTH)
    int stash_head[4], stash_count[4];
    int stashed_len[4][ROCE_STASH];
    uint64_t stashed_wr[4][ROCE_STASH];
    int last_wc_status;  /* status of the completion that made poll_for_completion fail */
    uint64_t last_wc_wr_id;
    /* Shared-buffer mode (see "registered buffer pool" below): no
     * per-connection max_frame_bytes buffers. Large frames use buffers from
     * a process-wide pool registered once; small sends use this
     * connection's small buffer; servers receive through an SRQ. */
    struct bufpool *pool;
    struct ibv_srq *srq;
    struct rbuf **srq_bufs;  /* server: the SRQ's own buffers, indexed by WR_IDX */
    uint32_t n_srq_bufs;
    uint8_t *small_buf;
    struct ibv_mr *small_mr;
    struct rbuf *held[2];  /* client: pool buffers owned by an unfinished request (0 recv, 1 send) */
    /* The connection's CM event channel, watched while waiting for
     * completions: with an SRQ a disconnect produces NO receive completion
     * (SRQ receives are not flushed), so a server connection thread that
     * only watched its CQ would never notice its peer -- or stop() -- going
     * away. The CM DISCONNECTED event is the signal on real hardware too. */
    struct rdma_event_channel *cm_channel;
};

/* wr_id layout: low byte = kind, upper bits = pool buffer index */
#define WR_RECV 1
#define WR_SEND 2
#define WR_READ 3   /* an RDMA READ we posted (server: fetching a request's bulk) */
#define WR_KIND(w) ((int)((w) & 0xff))
#define WR_IDX(w) ((uint32_t)((w) >> 8))
#define WR_MAKE(kind, idx) ((((uint64_t)(idx)) << 8) | (uint64_t)(kind))
/* Sends up to this size go from the connection's own small buffer. */
#define ROCE_SMALL_FRAME (16 * 1024)

static uint32_t env_u32(const char *name, uint32_t dflt) {
    const char *e = getenv(name);
    long v = (e && *e) ? strtol(e, NULL, 10) : 0;
    return v > 0 ? (uint32_t)v : dflt;
}

/* ---------------------------------------------------------------------
 * Registered buffer pool.
 *
 * Every connection used to register two max_frame_bytes buffers (16 MiB
 * each by default). On a real NIC ibv_reg_mr PINS those pages, so each
 * connection cost 32 MiB of locked memory plus a multi-millisecond
 * registration -- with pooled persistent connections that is 32 MiB x
 * every open connection, held indefinitely. Instead, per (device, frame
 * size) there is one protection domain and one pool of frame buffers,
 * registered in chunks as the pool grows (to peak concurrency, capped by
 * ROCE_BUFPOOL_MAX, default 512) and reused forever; connections borrow a
 * buffer only for the duration of one request.
 * --------------------------------------------------------------------- */
struct rbuf {
    uint8_t *addr;
    uint32_t lkey, rkey;
    uint32_t idx;
    struct rbuf *next;
};

struct chunk { uint8_t *base; size_t len; struct ibv_mr *mr; };

struct bufpool {
    struct bufpool *next;
    struct ibv_context *ctx;
    size_t frame;
    int role;  /* POOL_CLIENT or POOL_SERVER: server replies never compete with same-process clients */
    struct ibv_pd *pd;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    struct rbuf **all;
    uint32_t n, max, nfree;
    struct rbuf *free_list;
    uint64_t registered_bytes;
    struct chunk *chunks;  /* registered regions, for pool_find */
    uint32_t nchunks;
};

static struct bufpool *g_pools;
static pthread_mutex_t g_pools_lock = PTHREAD_MUTEX_INITIALIZER;
static atomic_llong g_srq_registered;  /* bytes registered for server SRQ slabs */

#define POOL_CLIENT 0
#define POOL_SERVER 1
#define RT_PAGE 4096u

/* One PD per device, shared by every pool on it (and so by every QP and
 * SRQ slab on it): a QP can only use lkeys from its own PD. */
static struct ibv_pd *device_pd(struct ibv_context *ctx) {  /* caller holds g_pools_lock */
    for (struct bufpool *q = g_pools; q; q = q->next)
        if (q->ctx == ctx) return q->pd;
    return ibv_alloc_pd(ctx);
}

static struct bufpool *pool_for(struct ibv_context *ctx, size_t frame, int role) {
    pthread_mutex_lock(&g_pools_lock);
    struct bufpool *p;
    for (p = g_pools; p; p = p->next)
        if (p->ctx == ctx && p->frame == frame && p->role == role) break;
    if (!p) {
        p = calloc(1, sizeof(*p));
        if (p) {
            p->ctx = ctx;
            p->frame = frame;
            p->role = role;
            p->pd = device_pd(ctx);
            /* A server never needs more than one buffer for the request
             * being fetched and one for the reply per in-flight request, so
             * its pool is capped there (a buffer's touched pages stay
             * resident: 512 x 1 MiB replies would be 512 MiB per OST, which
             * OOM-killed OSTs in the 10-node rehearsal); a client's is capped
             * high, it only ever holds buffers for its own outstanding calls. */
            uint32_t inflight = env_u32("ROCE_SERVER_MAX_INFLIGHT", ROCE_SERVER_MAX_INFLIGHT_DEFAULT);
            p->max = env_u32("ROCE_BUFPOOL_MAX", role == POOL_SERVER ? 2 * inflight + 8 : 512);
            p->all = calloc(p->max, sizeof(*p->all));
            if (!p->pd || !p->all) {
                free(p->all);
                free(p);
                p = NULL;
            } else {
                pthread_mutex_init(&p->lock, NULL);
                pthread_cond_init(&p->cond, NULL);
                p->next = g_pools;
                g_pools = p;
            }
        }
    }
    pthread_mutex_unlock(&g_pools_lock);
    if (!p) set_error("could not create registered buffer pool (ibv_alloc_pd failed?)");
    return p;
}

/* ibv_reg_mr failures on real hardware are almost always RLIMIT_MEMLOCK. */
static void set_reg_mr_error(const char *what, size_t len) {
    int e = errno;
    set_error("ibv_reg_mr (%s, %zu bytes) failed: %s%s", what, len, strerror(e),
              (e == ENOMEM || e == EPERM || e == EAGAIN)
                  ? " -- raise the locked-memory limit (ulimit -l unlimited / systemd LimitMEMLOCK=infinity);"
                    " tools/roce_preflight.py checks this"
                  : "");
}

/* Per-connection RDMA-CM options for real fabrics, from the environment
 * (tools/roce_preflight.py derives them from the NIC's DCB setup):
 *   ROCE_TOS          IP ToS byte for RoCEv2 packets (DSCP << 2), which is
 *                     what puts traffic in the switch's lossless PFC class
 *   ROCE_ACK_TIMEOUT  local ACK timeout exponent (4.096 us * 2^n) */
static void apply_cm_options(struct rdma_cm_id *id) {
    const char *e = getenv("ROCE_TOS");
    if (e && *e) {
        uint8_t tos = (uint8_t)strtol(e, NULL, 0);
        if (rdma_set_option(id, RDMA_OPTION_ID, RDMA_OPTION_ID_TOS, &tos, sizeof(tos)))
            fprintf(stderr, "roce_transport: rdma_set_option(TOS=%u) failed: %s\n", tos, strerror(errno));
    }
    e = getenv("ROCE_ACK_TIMEOUT");
    if (e && *e) {
        uint8_t t = (uint8_t)strtol(e, NULL, 0);
        if (rdma_set_option(id, RDMA_OPTION_ID, RDMA_OPTION_ID_ACK_TIMEOUT, &t, sizeof(t)))
            fprintf(stderr, "roce_transport: rdma_set_option(ACK_TIMEOUT=%u) failed: %s\n", t, strerror(errno));
    }
}

/* Where a server pool's chunks come from (roce_set_chunk_hooks): by default
 * anonymous memory; an OST may supply P2P memory (an NVMe's CMB, so the
 * NIC's bulk lands in the drive) and wants to hear of every chunk (SPDK
 * registers it for its DMA). Set before the server starts. */
static roce_chunk_alloc_fn g_chunk_alloc;
static roce_chunk_free_fn g_chunk_free;
static roce_chunk_notify_fn g_chunk_notify;
static void *g_chunk_user;
static int g_chunk_fallback_warned;

void roce_set_chunk_hooks(roce_chunk_alloc_fn alloc, roce_chunk_free_fn free_fn, roce_chunk_notify_fn notify, void *user) {
    g_chunk_alloc = alloc;
    g_chunk_free = free_fn;
    g_chunk_notify = notify;
    g_chunk_user = user;
}

/* caller holds p->lock. Registers one more chunk; 0 on success. */
static int pool_grow(struct bufpool *p) {
    uint32_t k = p->max - p->n;
    if (k > 4) k = 4;
    if (k == 0) return -1;
    size_t len = p->frame * k;
    /* client buffers are read (request bulk) and written (replies) by the
     * server's NIC; server buffers are only ever local */
    int access = p->role == POOL_CLIENT ? IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
                                        : IBV_ACCESS_LOCAL_WRITE;
    void *base = NULL;
    struct ibv_mr *mr = NULL;
    int special = 0;
    if (p->role == POOL_SERVER && g_chunk_alloc) {
        base = g_chunk_alloc(len, g_chunk_user);
        if (base) {
            mr = ibv_reg_mr(p->pd, base, len, access);
            if (mr) special = 1;
            else {
                /* the NIC's driver cannot register this memory (no P2P
                 * support): say so once and fall back to host memory */
                if (!g_chunk_fallback_warned++)
                    fprintf(stderr, "roce_transport: ibv_reg_mr refused the supplied (P2P) chunk memory (%s): "
                                    "using host memory instead\n", strerror(errno));
                g_chunk_free(base, len, g_chunk_user);
                base = NULL;
            }
        }
    }
    if (!base) {
        base = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (base == MAP_FAILED) return -1;
        mr = ibv_reg_mr(p->pd, base, len, access);
        if (!mr) { set_reg_mr_error("buffer pool chunk", len); munmap(base, len); return -1; }
    }
    (void)special;
    if (p->role == POOL_SERVER && g_chunk_notify) g_chunk_notify(base, len, g_chunk_user);
    struct chunk *nc = realloc(p->chunks, (p->nchunks + 1) * sizeof(*nc));
    if (!nc) { ibv_dereg_mr(mr); munmap(base, len); return -1; }
    p->chunks = nc;
    p->chunks[p->nchunks++] = (struct chunk){ base, len, mr };
    for (uint32_t i = 0; i < k; i++) {
        struct rbuf *b = calloc(1, sizeof(*b));
        if (!b) return p->n ? 0 : -1;  /* the chunk stays registered; fine */
        b->addr = (uint8_t *)base + (size_t)i * p->frame;
        b->lkey = mr->lkey;
        b->rkey = mr->rkey;
        b->idx = p->n;
        p->all[p->n++] = b;
        b->next = p->free_list;
        p->free_list = b;
        p->nfree++;
    }
    p->registered_bytes += len;
    return 0;
}

/* Take n (1 or 2) buffers at once -- all or nothing, so callers that need
 * two never hold one while waiting for the other (with every thread doing
 * that at the pool cap, nobody could proceed). timeout_ms < 0 waits
 * forever. Returns 0, or -1 on timeout / growth failure (error set). */
static int pool_get_n(struct bufpool *p, int n, struct rbuf **out, int timeout_ms) {
    struct timespec dl;
    if (timeout_ms >= 0) {
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_sec += timeout_ms / 1000;
        dl.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if (dl.tv_nsec >= 1000000000L) { dl.tv_sec++; dl.tv_nsec -= 1000000000L; }
    }
    pthread_mutex_lock(&p->lock);
    while (p->nfree < (uint32_t)n) {
        if (pool_grow(p) == 0) continue;
        if (p->n < p->max) {  /* below the cap, so growth itself failed (e.g. memlock): error already set */
            pthread_mutex_unlock(&p->lock);
            return -1;
        }
        int rc = timeout_ms >= 0 ? pthread_cond_timedwait(&p->cond, &p->lock, &dl)
                                 : pthread_cond_wait(&p->cond, &p->lock);
        if (rc) {
            pthread_mutex_unlock(&p->lock);
            set_error("registered buffer pool exhausted (%u buffers of %zu bytes in use; ROCE_BUFPOOL_MAX)",
                      p->n, p->frame);
            return -1;
        }
    }
    for (int i = 0; i < n; i++) {
        out[i] = p->free_list;
        p->free_list = out[i]->next;
        p->nfree--;
    }
    pthread_mutex_unlock(&p->lock);
    return 0;
}

static struct rbuf *pool_get(struct bufpool *p, int timeout_ms) {
    struct rbuf *b;
    return pool_get_n(p, 1, &b, timeout_ms) ? NULL : b;
}

static void pool_put(struct bufpool *p, struct rbuf *b);

/* the pool buffer that starts at addr, or NULL */
static struct rbuf *pool_find(struct bufpool *p, const void *addr) {
    struct rbuf *hit = NULL;
    pthread_mutex_lock(&p->lock);
    for (uint32_t c = 0; c < p->nchunks && !hit; c++) {
        const uint8_t *a = addr;
        struct chunk *ch = &p->chunks[c];
        if (a < ch->base || a >= ch->base + ch->len) continue;
        size_t off = (size_t)(a - ch->base);
        if (off % p->frame) break;
        for (uint32_t i = 0; i < p->n; i++)
            if (p->all[i]->addr == a) { hit = p->all[i]; break; }
    }
    pthread_mutex_unlock(&p->lock);
    return hit;
}

/* ---- reply buffers for server handlers (roce_reply_alloc / _free) ----
 * A handler that builds a reply larger than a SEND in a registered pool
 * buffer lets the connection thread RDMA WRITE it to the client as is. The
 * connection thread names its pool in a thread-local before calling the
 * handler; outside a handler (or for small replies) this is malloc. */
static __thread struct bufpool *tl_reply_pool;

void *roce_reply_alloc(size_t len) {
    struct bufpool *p = tl_reply_pool;
    if (p && len > ROCE_SMALL_FRAME && len <= p->frame) {
        struct rbuf *b = pool_get(p, 30000);
        if (b) return b->addr;
    }
    return malloc(len ? len : 1);
}

void roce_reply_free(void *ptr) {
    if (!ptr) return;
    pthread_mutex_lock(&g_pools_lock);
    struct bufpool *p = g_pools;
    pthread_mutex_unlock(&g_pools_lock);
    for (; p; p = p->next) {
        struct rbuf *b = pool_find(p, ptr);
        if (b) { pool_put(p, b); return; }
    }
    free(ptr);
}

/* ---- registered caller memory (roce_mem_register) ----
 * One registration per device PD the memory is used with, made on first
 * use; every pool of a device shares that device's PD, so a region needs
 * at most one MR per RDMA device in the process. */
#define ROCE_MEM_PDS 8
struct roce_mem {
    void *addr;
    size_t len;
    pthread_mutex_t lock;
    int n;
    struct { struct ibv_pd *pd; struct ibv_mr *mr; } e[ROCE_MEM_PDS];
};

roce_mem_t *roce_mem_register(void *addr, size_t len) {
    struct roce_mem *m = calloc(1, sizeof(*m));
    if (!m) { set_error("out of memory"); return NULL; }
    m->addr = addr;
    m->len = len;
    pthread_mutex_init(&m->lock, NULL);
    return m;
}

void roce_mem_release(roce_mem_t *m) {
    if (!m) return;
    for (int i = 0; i < m->n; i++) ibv_dereg_mr(m->e[i].mr);
    pthread_mutex_destroy(&m->lock);
    free(m);
}

/* keys of m for pd, registering on first use; -1 if it cannot be registered */
static int mem_keys(struct roce_mem *m, struct ibv_pd *pd, uint32_t *lkey, uint32_t *rkey) {
    int rc = -1;
    pthread_mutex_lock(&m->lock);
    for (int i = 0; i < m->n; i++)
        if (m->e[i].pd == pd) { *lkey = m->e[i].mr->lkey; *rkey = m->e[i].mr->rkey; rc = 0; break; }
    if (rc && m->n < ROCE_MEM_PDS) {
        struct ibv_mr *mr = ibv_reg_mr(pd, m->addr, m->len,
                                       IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
        if (mr) {
            m->e[m->n].pd = pd;
            m->e[m->n].mr = mr;
            m->n++;
            *lkey = mr->lkey;
            *rkey = mr->rkey;
            rc = 0;
        } else {
            set_reg_mr_error("caller memory", m->len);
        }
    }
    pthread_mutex_unlock(&m->lock);
    return rc;
}

static void pool_put(struct bufpool *p, struct rbuf *b) {
    if (!b) return;
    pthread_mutex_lock(&p->lock);
    b->next = p->free_list;
    p->free_list = b;
    p->nfree++;
    pthread_cond_broadcast(&p->cond);  /* a waiter may need two */
    pthread_mutex_unlock(&p->lock);
}

void roce_bufpool_stats(uint64_t *registered_bytes, uint32_t *buffers, uint32_t *in_use) {
    uint64_t rb = 0;
    uint32_t n = 0, used = 0;
    pthread_mutex_lock(&g_pools_lock);
    for (struct bufpool *p = g_pools; p; p = p->next) {
        pthread_mutex_lock(&p->lock);
        rb += p->registered_bytes;
        n += p->n;
        used += p->n - p->nfree;
        pthread_mutex_unlock(&p->lock);
    }
    pthread_mutex_unlock(&g_pools_lock);
    rb += (uint64_t)atomic_load(&g_srq_registered);
    if (registered_bytes) *registered_bytes = rb;
    if (buffers) *buffers = n;
    if (in_use) *in_use = used;
}

static int post_recv(struct roce_qp_resources *r) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)r->recv_buf,
        .length = (uint32_t)r->max_frame_bytes,
        .lkey = r->recv_mr->lkey,
    };
    struct ibv_recv_wr wr = { .wr_id = WR_RECV, .sg_list = &sge, .num_sge = 1 };
    struct ibv_recv_wr *bad = NULL;
    int rc = ibv_post_recv(r->id->qp, &wr, &bad);
    if (rc) { set_error("ibv_post_recv failed: %s", strerror(rc)); return -rc; }
    return 0;
}

/* Post a pool buffer as a receive: on the QP (client) or on an SRQ (server). */
static int post_recv_rbuf(struct ibv_qp *qp, struct ibv_srq *srq, struct bufpool *p, struct rbuf *b) {
    struct ibv_sge sge = { .addr = (uintptr_t)b->addr, .length = (uint32_t)p->frame, .lkey = b->lkey };
    struct ibv_recv_wr wr = { .wr_id = WR_MAKE(WR_RECV, b->idx), .sg_list = &sge, .num_sge = 1 };
    struct ibv_recv_wr *bad = NULL;
    int rc = srq ? ibv_post_srq_recv(srq, &wr, &bad) : ibv_post_recv(qp, &wr, &bad);
    if (rc) { set_error("%s failed: %s", srq ? "ibv_post_srq_recv" : "ibv_post_recv", strerror(rc)); return -rc; }
    return 0;
}

static int post_send_from(struct roce_qp_resources *r, const void *addr, uint32_t lkey, size_t len) {
    struct ibv_sge sge = { .addr = (uintptr_t)addr, .length = (uint32_t)len, .lkey = lkey };
    struct ibv_send_wr wr = {
        .wr_id = WR_SEND, .sg_list = &sge, .num_sge = 1,
        .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED,
    };
    struct ibv_send_wr *bad = NULL;
    int rc = ibv_post_send(r->id->qp, &wr, &bad);
    if (rc) { set_error("ibv_post_send failed: %s", strerror(rc)); return -rc; }
    return 0;
}

/* RDMA READ remote [raddr, raddr+len) into local addr (lkey); wr_id kind WR_READ, index i */
static int post_read(struct roce_qp_resources *r, void *addr, uint32_t lkey, uint64_t raddr, uint32_t rkey,
                     uint32_t len, uint32_t i) {
    struct ibv_sge sge = { .addr = (uintptr_t)addr, .length = len, .lkey = lkey };
    struct ibv_send_wr wr = {
        .wr_id = WR_MAKE(WR_READ, i), .sg_list = &sge, .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ, .send_flags = IBV_SEND_SIGNALED,
        .wr.rdma = { .remote_addr = raddr, .rkey = rkey },
    };
    struct ibv_send_wr *bad = NULL;
    int rc = ibv_post_send(r->id->qp, &wr, &bad);
    if (rc) { set_error("ibv_post_send (RDMA READ) failed: %s", strerror(rc)); return -rc; }
    return 0;
}

/* RDMA WRITE local [addr, addr+len) to the peer's sink, with immediate =
 * len (network order): the peer's posted RECV completes with the data in place */
static int post_write_imm(struct roce_qp_resources *r, const void *addr, uint32_t lkey, const struct rt_seg *sink,
                          uint32_t len) {
    struct ibv_sge sge = { .addr = (uintptr_t)addr, .length = len, .lkey = lkey };
    struct ibv_send_wr wr = {
        .wr_id = WR_SEND, .sg_list = &sge, .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM, .send_flags = IBV_SEND_SIGNALED,
        .imm_data = htonl(len),
        .wr.rdma = { .remote_addr = sink->addr, .rkey = sink->rkey },
    };
    struct ibv_send_wr *bad = NULL;
    int rc = ibv_post_send(r->id->qp, &wr, &bad);
    if (rc) { set_error("ibv_post_send (RDMA WRITE) failed: %s", strerror(rc)); return -rc; }
    return 0;
}

static int post_send(struct roce_qp_resources *r, size_t len) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)r->send_buf,
        .length = (uint32_t)len,
        .lkey = r->send_mr->lkey,
    };
    struct ibv_send_wr wr = {
        .wr_id = WR_SEND, .sg_list = &sge, .num_sge = 1,
        .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED,
    };
    struct ibv_send_wr *bad = NULL;
    int rc = ibv_post_send(r->id->qp, &wr, &bad);
    if (rc) { set_error("ibv_post_send failed: %s", strerror(rc)); return -rc; }
    return 0;
}

/* Polls until one completion for wr_id arrives, or timeout_ms elapses.
 * Returns the completed byte length for a RECV completion (>=0), or a
 * negative value on error/timeout. Simple busy-poll with a sleep back-off
 * -- a production version should use a completion channel + epoll instead
 * of spinning, but the poll_cq() call itself is the real, correct verb
 * either way; the difference is only how the caller waits for work to
 * show up, which does not change wire behavior or correctness. */
static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}

/* Optional busy-poll before sleeping on the completion channel (microseconds,
 * env ROCE_POLL_SPIN_US, default 0). On a real NIC a short spin (20-50 us)
 * saves the wakeup latency for requests that complete in a few microseconds;
 * on the TCP emulator or an oversubscribed host it only burns CPU. */
static int spin_us(void) {
    static atomic_int v = -1;  /* lazily cached; any thread may initialise it (same value) */
    int x = atomic_load_explicit(&v, memory_order_relaxed);
    if (x < 0) {
        const char *e = getenv("ROCE_POLL_SPIN_US");
        x = (e && atoi(e) > 0) ? atoi(e) : 0;
        atomic_store_explicit(&v, x, memory_order_relaxed);
    }
    return x;
}

/* Sleep until the completion channel fires or timeout_ms (-1 = forever).
 * Returns 0 on an event, 1 on timeout, -1 on error. */
static int wait_comp_event(struct roce_qp_resources *r, int timeout_ms) {
    struct pollfd pfd[2] = { { .fd = r->comp->fd, .events = POLLIN },
                             { .fd = r->cm_channel ? r->cm_channel->fd : -1, .events = POLLIN } };
    int rc;
    do rc = poll(pfd, r->cm_channel ? 2 : 1, timeout_ms); while (rc < 0 && errno == EINTR);
    if (rc < 0) { set_error("poll on completion channel failed: %s", strerror(errno)); return -1; }
    if (rc == 0) return 1;
    if (r->cm_channel && (pfd[1].revents & POLLIN) && !(pfd[0].revents & POLLIN)) {
        struct rdma_cm_event *ev;
        if (rdma_get_cm_event(r->cm_channel, &ev)) { set_error("rdma_get_cm_event failed"); return -1; }
        enum rdma_cm_event_type t = ev->event;
        rdma_ack_cm_event(ev);
        if (t == RDMA_CM_EVENT_ESTABLISHED) return 0;  /* the accept side's own event: not a failure */
        r->last_wc_wr_id = 0;
        set_error("connection closed (%s)", rdma_event_str(t));
        return -1;
    }
    if (!(pfd[0].revents & POLLIN)) return 0;
    struct ibv_cq *ev_cq;
    void *ev_ctx;
    if (ibv_get_cq_event(r->comp, &ev_cq, &ev_ctx)) { set_error("ibv_get_cq_event failed"); return -1; }
    ibv_ack_cq_events(ev_cq, 1);
    r->armed = 0;
    return 0;
}

/* Handle one polled completion. Returns 1 if it is the one being waited
 * for (kind matches; len_out and wr_out set), 0 if it was stashed for the
 * other queue's waiter, -1 on an error completion. */
static int take_wc(struct roce_qp_resources *r, const struct ibv_wc *wc, int kind, int *len_out, uint64_t *wr_out) {
    if (wc->status != IBV_WC_SUCCESS) {
        /* any error puts the QP in the error state; every other WR will be
         * flushed, so fail the caller now either way */
        r->last_wc_status = (int)wc->status;
        r->last_wc_wr_id = wc->wr_id;
        set_error("completion error: %s", ibv_wc_status_str(wc->status));
        return -1;
    }
    int k = WR_KIND(wc->wr_id);
    if (k == kind) {
        *len_out = (int)wc->byte_len;  /* meaningful for RECV completions */
        if (wr_out) *wr_out = wc->wr_id;
        return 1;
    }
    if (k == WR_RECV || k == WR_SEND || k == WR_READ) {  /* another waiter's completion: keep it */
        if (r->stash_count[k] == ROCE_STASH) {  /* cannot happen: > CQ depth outstanding */
            set_error("completion stash overflow");
            return -1;
        }
        int slot = (r->stash_head[k] + r->stash_count[k]) % ROCE_STASH;
        r->stashed_len[k][slot] = (int)wc->byte_len;
        r->stashed_wr[k][slot] = wc->wr_id;
        r->stash_count[k]++;
    }
    return 0;
}

/* Waits until one completion of `kind` (WR_RECV / WR_SEND) arrives, or
 * timeout_ms elapses (-1 = no timeout). Returns the completed byte length
 * for a RECV completion (>=0), or a negative value on error/timeout; the
 * completion's full wr_id goes to *wr_out when non-NULL. Event-driven:
 * poll the CQ; if empty, arm it (ibv_req_notify_cq), poll AGAIN to close
 * the race with a completion that landed before the arm, then sleep on the
 * completion channel fd. */
static int poll_for_completion_wr(struct roce_qp_resources *r, int kind, int timeout_ms, uint64_t *wr_out) {
    if (r->stash_count[kind]) {
        int slot = r->stash_head[kind];
        r->stash_head[kind] = (slot + 1) % ROCE_STASH;
        r->stash_count[kind]--;
        if (wr_out) *wr_out = r->stashed_wr[kind][slot];
        return r->stashed_len[kind][slot];
    }
    struct ibv_wc wc;
    long deadline = timeout_ms >= 0 ? now_ms() + timeout_ms : 0;
    int spun = 0, len = 0;
    for (;;) {
        int n = ibv_poll_cq(r->cq, 1, &wc);
        if (n < 0) { set_error("ibv_poll_cq failed"); return -1; }
        if (n == 1) {
            int t = take_wc(r, &wc, kind, &len, wr_out);
            if (t < 0) return -1;
            if (t > 0) return len;
            continue;
        }
        if (!spun && spin_us() > 0) {
            spun = 1;
            long until = 0;
            struct timespec ts;
            clock_gettime(CLOCK_MONOTONIC, &ts);
            until = ts.tv_sec * 1000000L + ts.tv_nsec / 1000 + spin_us();
            do {
                n = ibv_poll_cq(r->cq, 1, &wc);
                if (n != 0) break;
                clock_gettime(CLOCK_MONOTONIC, &ts);
            } while (ts.tv_sec * 1000000L + ts.tv_nsec / 1000 < until);
            if (n < 0) { set_error("ibv_poll_cq failed"); return -1; }
            if (n == 1) {
                int t = take_wc(r, &wc, kind, &len, wr_out);
                if (t < 0) return -1;
                if (t > 0) return len;
            }
            continue;
        }
        if (!r->armed) {
            if (ibv_req_notify_cq(r->cq, 0)) { set_error("ibv_req_notify_cq failed"); return -1; }
            r->armed = 1;
            continue;  /* re-poll: a completion may have landed before the arm */
        }
        int wait_ms = -1;
        if (timeout_ms >= 0) {
            long left = deadline - now_ms();
            if (left <= 0) { set_error("timed out waiting for completion"); return -1; }
            wait_ms = (int)left;
        }
        if (wait_comp_event(r, wait_ms) < 0) return -1;
    }
}

static int poll_for_completion(struct roce_qp_resources *r, int kind, int timeout_ms) {
    return poll_for_completion_wr(r, kind, timeout_ms, NULL);
}

/* timeout_ms < 0 waits forever. rdma_get_cm_event() itself has no timeout:
 * on a real fabric a lost CM reply (bad route, peer host down, a switch
 * dropping MADs) blocked the caller forever. poll() on the channel fd first. */
static int wait_cm_event(struct rdma_event_channel *channel, enum rdma_cm_event_type expected, int timeout_ms) {
    struct rdma_cm_event *event;
    struct pollfd pfd = { .fd = channel->fd, .events = POLLIN };
    int prc;
    do prc = poll(&pfd, 1, timeout_ms); while (prc < 0 && errno == EINTR);
    if (prc == 0) { set_error("timed out after %d ms waiting for CM event %s", timeout_ms, rdma_event_str(expected)); return -1; }
    int rc = rdma_get_cm_event(channel, &event);
    if (rc) { set_error("rdma_get_cm_event failed: %s", strerror(errno)); return -1; }
    enum rdma_cm_event_type got = event->event;
    int status = event->status;
    rdma_ack_cm_event(event);
    if (got != expected) {
        set_error("unexpected CM event: got %s, expected %s (status=%d)",
                   rdma_event_str(got), rdma_event_str(expected), status);
        return -1;
    }
    return 0;
}

/* Per-connection frame buffers come straight from mmap and go straight back
 * with munmap. With malloc, glibc's DYNAMIC mmap threshold rises to 16 MiB
 * after the first such buffer is freed, so every later connection's two
 * max_frame_bytes buffers were carved out of per-thread heap arenas instead
 * -- and freed back INTO those arenas, touched pages still resident, when
 * the connection closed. Under connect-per-request load with hundreds of
 * connection threads that retained memory is what OOM-killed OST servers at
 * 500 clients (tools/stress_concurrency.py --mem-probe-at: 22-48 MB of
 * stored data, but 1-2.4 GB of freed-yet-retained arena memory per OST in
 * glibc mallinfo2). mmap'd pages are only faulted in as a frame actually
 * uses them and are released the moment the connection ends. */
static void *frame_buf_alloc(size_t n) {
    void *p = mmap(NULL, n, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? NULL : p;
}

static void frame_buf_free(void *p, size_t n) {
    if (p) munmap(p, n);
}

/* mode: dedicated buffers (streams: a RECV is always posted), shared-pool
 * client (a RECV is posted only while a request is outstanding), or
 * shared-pool server (receives come from the server's SRQ). */
enum qp_mode { QP_DEDICATED, QP_SHARED_CLIENT, QP_SHARED_SERVER };

static int setup_qp_resources(struct roce_qp_resources *r, size_t max_frame_bytes, enum qp_mode mode,
                              struct bufpool *pool, struct ibv_srq *srq) {
    r->max_frame_bytes = max_frame_bytes;
    if (mode == QP_DEDICATED) {
        r->pd = ibv_alloc_pd(r->id->verbs);
        r->own_pd = 1;
        if (!r->pd) { set_error("ibv_alloc_pd failed"); return -1; }
    } else {
        r->pool = pool;
        r->pd = pool->pd;
        r->srq = srq;
    }

    r->comp = ibv_create_comp_channel(r->id->verbs);
    if (!r->comp) { set_error("ibv_create_comp_channel failed: %s", strerror(errno)); return -1; }
    int cq_depth = ROCE_CQ_DEPTH + (srq ? ROCE_SRQ_DEPTH_MAX : 0);
    r->cq = ibv_create_cq(r->id->verbs, cq_depth, NULL, r->comp, 0);
    if (!r->cq) { set_error("ibv_create_cq failed"); return -1; }

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = r->cq, .recv_cq = r->cq, .qp_type = IBV_QPT_RC, .srq = srq,
        .cap = { .max_send_wr = ROCE_MAX_SEND_WR, .max_recv_wr = srq ? 0 : ROCE_MAX_RECV_WR,
                 .max_send_sge = 1, .max_recv_sge = 1 },
    };
    if (rdma_create_qp(r->id, r->pd, &qp_attr)) { set_error("rdma_create_qp failed: %s", strerror(errno)); return -1; }

    r->small_buf = frame_buf_alloc(ROCE_SMALL_FRAME);
    if (!r->small_buf) { set_error("out of memory allocating small frame buffer"); return -1; }
    r->small_mr = ibv_reg_mr(r->pd, r->small_buf, ROCE_SMALL_FRAME, IBV_ACCESS_LOCAL_WRITE);
    if (!r->small_mr) { set_reg_mr_error("small buffer", ROCE_SMALL_FRAME); return -1; }
    if (mode != QP_DEDICATED) return 0;

    /* mmap/munmap, not malloc/free: see frame_buf_alloc(). The server reads
     * request bulk from send_buf and writes large replies into recv_buf. */
    r->send_buf = frame_buf_alloc(max_frame_bytes);
    r->recv_buf = frame_buf_alloc(max_frame_bytes);
    if (!r->send_buf || !r->recv_buf) { set_error("out of memory allocating frame buffers"); return -1; }

    r->send_mr = ibv_reg_mr(r->pd, r->send_buf, max_frame_bytes, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
    r->recv_mr = ibv_reg_mr(r->pd, r->recv_buf, max_frame_bytes, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!r->send_mr || !r->recv_mr) { set_reg_mr_error("connection frame buffers", max_frame_bytes); return -1; }

    /* Must be posted before the peer can send us anything. */
    return post_recv(r);
}

static void teardown_qp_resources(struct roce_qp_resources *r) {
    if (r->send_mr) ibv_dereg_mr(r->send_mr);
    if (r->recv_mr) ibv_dereg_mr(r->recv_mr);
    if (r->small_mr) ibv_dereg_mr(r->small_mr);
    frame_buf_free(r->send_buf, r->max_frame_bytes);
    frame_buf_free(r->recv_buf, r->max_frame_bytes);
    frame_buf_free(r->small_buf, ROCE_SMALL_FRAME);
    if (r->id && r->id->qp) rdma_destroy_qp(r->id);
    /* only now, with the QP gone, can no more data land in borrowed buffers */
    for (int i = 0; i < 2; i++)
        if (r->held[i]) { pool_put(r->pool, r->held[i]); r->held[i] = NULL; }
    if (r->cq) ibv_destroy_cq(r->cq);  /* every retrieved CQ event was acked in wait_comp_event */
    if (r->comp) ibv_destroy_comp_channel(r->comp);
    if (r->pd && r->own_pd) ibv_dealloc_pd(r->pd);
}

/* RDMA READ needs responder resources at the target and initiator depth at
 * the requester; the CM takes the minimum of what both sides offer. Ask for
 * RT_MAX_SRC + a margin, within what the device supports. */
static void rd_atomic_params(struct ibv_context *ctx, struct rdma_conn_param *cp) {
    int want = 2 * RT_MAX_SRC;
    struct ibv_device_attr a;
    if (ctx && ibv_query_device(ctx, &a) == 0) {
        if (a.max_qp_rd_atom > 0 && a.max_qp_rd_atom < want) want = a.max_qp_rd_atom;
        if (a.max_qp_init_rd_atom > 0 && a.max_qp_init_rd_atom < want) want = a.max_qp_init_rd_atom;
    }
    if (want < 1) want = 1;
    cp->responder_resources = (uint8_t)want;
    cp->initiator_depth = (uint8_t)want;
}

static int resolve_peer(const char *peer, struct sockaddr_storage *out) {
    char host[256], *colon;
    strncpy(host, peer, sizeof(host) - 1);
    host[sizeof(host) - 1] = 0;
    colon = strrchr(host, ':');
    if (!colon) { set_error("peer address missing ':port': %s", peer); return -1; }
    *colon = 0;
    const char *port = colon + 1;

    struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM };
    struct addrinfo *res;
    if (getaddrinfo(host, port, &hints, &res)) { set_error("getaddrinfo failed for %s", peer); return -1; }
    memcpy(out, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return 0;
}

/* ---------------------------------------------------------------------
 * Client
 * --------------------------------------------------------------------- */
struct roce_conn {
    struct rdma_event_channel *channel;
    struct roce_qp_resources qp;
};

static roce_conn_t *client_connect(const char *peer, int timeout_ms, size_t max_frame_bytes, int shared) {
    struct roce_conn *conn = calloc(1, sizeof(*conn));
    if (!conn) { set_error("out of memory"); return NULL; }

    conn->channel = rdma_create_event_channel();
    if (!conn->channel) { set_error("rdma_create_event_channel failed"); goto fail; }

    if (rdma_create_id(conn->channel, &conn->qp.id, NULL, RDMA_PS_TCP)) {
        set_error("rdma_create_id failed: %s", strerror(errno)); goto fail;
    }
    apply_cm_options(conn->qp.id);

    struct sockaddr_storage addr;
    if (resolve_peer(peer, &addr)) goto fail;

    if (rdma_resolve_addr(conn->qp.id, NULL, (struct sockaddr *)&addr, timeout_ms)) {
        set_error("rdma_resolve_addr failed: %s", strerror(errno)); goto fail;
    }
    if (wait_cm_event(conn->channel, RDMA_CM_EVENT_ADDR_RESOLVED, timeout_ms + 1000)) goto fail;

    if (rdma_resolve_route(conn->qp.id, timeout_ms)) {
        set_error("rdma_resolve_route failed: %s", strerror(errno)); goto fail;
    }
    if (wait_cm_event(conn->channel, RDMA_CM_EVENT_ROUTE_RESOLVED, timeout_ms + 1000)) goto fail;

    if (shared) {
        struct bufpool *pool = pool_for(conn->qp.id->verbs, max_frame_bytes, POOL_CLIENT);
        if (!pool || setup_qp_resources(&conn->qp, max_frame_bytes, QP_SHARED_CLIENT, pool, NULL)) goto fail;
    } else if (setup_qp_resources(&conn->qp, max_frame_bytes, QP_DEDICATED, NULL, NULL)) {
        goto fail;
    }

    struct rdma_conn_param conn_param = { .retry_count = 7, .rnr_retry_count = ROCE_RNR_RETRY_INFINITE };
    rd_atomic_params(conn->qp.id->verbs, &conn_param);
    if (rdma_connect(conn->qp.id, &conn_param)) {
        set_error("rdma_connect failed: %s", strerror(errno)); goto fail;
    }
    if (wait_cm_event(conn->channel, RDMA_CM_EVENT_ESTABLISHED, timeout_ms)) goto fail;
    conn->qp.cm_channel = conn->channel;

    return conn;

fail:
    roce_client_close(conn);
    return NULL;
}

/* Dedicated buffers, a RECV always posted: for streams (RoCEStream), where
 * the peer may send at any time. */
roce_conn_t *roce_client_connect(const char *peer, int timeout_ms, size_t max_frame_bytes) {
    return client_connect(peer, timeout_ms, max_frame_bytes, 0);
}

/* Request/response connections: no per-connection frame buffers (see the
 * registered buffer pool). */
roce_conn_t *roce_client_connect_shared(const char *peer, int timeout_ms, size_t max_frame_bytes) {
    return client_connect(peer, timeout_ms, max_frame_bytes, 1);
}

/* An idle, established connection has nothing to report: any pending CM
 * event (DISCONNECTED, DEVICE_REMOVAL, ...) or any completion on its CQ
 * (its posted RECV flushed because the peer went away) means the QP is
 * already dead -- known BEFORE anything is posted on it. */
static int conn_known_dead(struct roce_conn *conn) {
    struct pollfd pfd = { .fd = conn->channel->fd, .events = POLLIN };
    if (poll(&pfd, 1, 0) > 0) return 1;
    struct ibv_wc wc;
    int n = ibv_poll_cq(conn->qp.cq, 1, &wc);
    if (n < 0) return 1;
    if (n == 1) {
        int len;
        if (take_wc(&conn->qp, &wc, -1 /* match nothing: stash */, &len, NULL) < 0) return 1;
    }
    return 0;
}

int roce_client_is_alive(roce_conn_t *conn) {
    return conn && !conn_known_dead(conn);
}

/* One request: SEND = envelope + inline part; bulk = registered segments
 * the server RDMA READs; the reply lands in the sink (a pool buffer in
 * shared mode, recv_buf in dedicated mode) either by SEND or by RDMA WRITE
 * with immediate -- the posted RECV completes either way, byte_len = reply
 * length. On success the shared-mode reply buffer is handed to the caller
 * (reply->buf/pool; roce_reply_release), dedicated mode copies it out. */
static int client_request(struct roce_conn *conn, const void *head, size_t head_len, const roce_seg_t *segs,
                          int nsegs, roce_reply_t *reply, int timeout_ms) {
    struct roce_qp_resources *r = &conn->qp;
    size_t body = 0;
    int registered = nsegs > 0 && nsegs <= RT_MAX_SRC;
    for (int i = 0; i < nsegs; i++) { body += segs[i].len; if (!segs[i].mem) registered = 0; }
    size_t total = head_len + body;
    if (total > r->max_frame_bytes) {
        set_error("request_len %zu exceeds max_frame_bytes %zu", total, r->max_frame_bytes);
        return -1;
    }
    int shared = r->pool != NULL;
    struct rbuf *bufs[2] = { NULL, NULL };
    struct rt_env env = { .magic = RT_ENV_MAGIC, .total_len = (uint32_t)total };
    uint8_t *inl = r->small_buf + RT_ENV_SIZE;  /* inline bytes go after the envelope */
    size_t inline_cap = ROCE_SMALL_FRAME - RT_ENV_SIZE;
    /* decide the shape: all inline / head inline + registered segments / head inline + one copied bulk */
    int all_inline = total <= inline_cap;
    int need_bulk = !all_inline && !registered;   /* the bulk goes through a pool / frame buffer */
    if (!all_inline && head_len > inline_cap) need_bulk = 1;  /* head too big: everything through the bulk buffer */
    if (shared) {
        if (pool_get_n(r->pool, need_bulk ? 2 : 1, bufs, timeout_ms)) return ROCE_ERR_NOT_DELIVERED;
        r->held[0] = bufs[0];
        r->held[1] = bufs[1];
        env.sink = (struct rt_seg){ (uint64_t)(uintptr_t)bufs[0]->addr, bufs[0]->rkey, (uint32_t)r->pool->frame };
        if (post_recv_rbuf(r->id->qp, NULL, r->pool, bufs[0])) return ROCE_ERR_NOT_DELIVERED;
    } else {
        env.sink = (struct rt_seg){ (uint64_t)(uintptr_t)r->recv_buf, r->recv_mr->rkey, (uint32_t)r->max_frame_bytes };
    }
    if (all_inline) {
        memcpy(inl, head, head_len);
        size_t o = head_len;
        for (int i = 0; i < nsegs; i++) { memcpy(inl + o, segs[i].addr, segs[i].len); o += segs[i].len; }
        env.inline_len = (uint32_t)total;
    } else if (!need_bulk) {
        memcpy(inl, head, head_len);
        env.inline_len = (uint32_t)head_len;
        for (int i = 0; i < nsegs; i++) {
            uint32_t lkey, rkey;
            if (mem_keys(segs[i].mem, r->pd, &lkey, &rkey)) return ROCE_ERR_NOT_DELIVERED;
            env.src[i] = (struct rt_seg){ (uint64_t)(uintptr_t)segs[i].addr, rkey, (uint32_t)segs[i].len };
        }
        env.nsrc = (uint32_t)nsegs;
    } else {
        /* one contiguous bulk copy: head (if it does not fit inline) + every segment */
        uint8_t *bulk = shared ? bufs[1]->addr : r->send_buf;
        uint32_t bkey = shared ? bufs[1]->rkey : r->send_mr->rkey;
        size_t o = 0;
        if (head_len <= inline_cap) {
            memcpy(inl, head, head_len);
            env.inline_len = (uint32_t)head_len;
        } else {
            memcpy(bulk, head, head_len);
            o = head_len;
        }
        for (int i = 0; i < nsegs; i++) { memcpy(bulk + o, segs[i].addr, segs[i].len); o += segs[i].len; }
        env.src[0] = (struct rt_seg){ (uint64_t)(uintptr_t)bulk, bkey, (uint32_t)o };
        env.nsrc = 1;
    }
    memcpy(r->small_buf, &env, RT_ENV_SIZE);
    if (post_send_from(r, r->small_buf, r->small_mr->lkey, RT_ENV_SIZE + env.inline_len)) return ROCE_ERR_NOT_DELIVERED;
    if (poll_for_completion(r, WR_SEND, timeout_ms) < 0) return -1;
    int recv_len = poll_for_completion(r, WR_RECV, timeout_ms);
    if (recv_len < 0) return -1;
    /* the server is done with our bulk (its reply came after its READs) */
    if (r->held[1]) { pool_put(r->pool, r->held[1]); r->held[1] = NULL; }
    if (shared) {
        reply->data = bufs[0]->addr;
        reply->len = (size_t)recv_len;
        reply->pool = r->pool;
        reply->buf = bufs[0];
        r->held[0] = NULL;
        return 0;
    }
    uint8_t *out = malloc(recv_len > 0 ? (size_t)recv_len : 1);
    if (!out) { set_error("out of memory"); return -1; }
    memcpy(out, r->recv_buf, (size_t)recv_len);
    reply->data = out;
    reply->len = (size_t)recv_len;
    reply->pool = NULL;
    reply->buf = NULL;
    return post_recv(r);  /* re-arm for the connection's next use */
}

/* RC executes a chain of READs in order. ROCE_READS_UNORDERED=1 makes
 * callers treat it as unordered (tests drive the EFA validation paths). */
int roce_transport_reads_ordered(void) {
    static int v = -1;
    if (v < 0) { const char *e = getenv("ROCE_READS_UNORDERED"); v = !(e && !strcmp(e, "1")); }
    return v;
}

int roce_client_rdma_read(roce_conn_t *conn, const roce_read_t *reads, int n, int timeout_ms) {
    struct roce_qp_resources *r = &conn->qp;
    if (n < 1 || n > 3) { set_error("roce_client_rdma_read: 1..3 ranges"); return -1; }
    if (conn_known_dead(conn)) return ROCE_ERR_NOT_DELIVERED;
    struct ibv_sge sge[3];
    struct ibv_send_wr wr[3];
    for (int i = 0; i < n; i++) {
        uint32_t lkey, rkey_unused;
        if (mem_keys(reads[i].mem, r->pd, &lkey, &rkey_unused)) return ROCE_ERR_NOT_DELIVERED;
        sge[i] = (struct ibv_sge){ .addr = (uintptr_t)reads[i].local, .length = reads[i].len, .lkey = lkey };
        wr[i] = (struct ibv_send_wr){
            .wr_id = WR_MAKE(WR_READ, (uint32_t)i), .sg_list = &sge[i], .num_sge = 1,
            .opcode = IBV_WR_RDMA_READ, .send_flags = IBV_SEND_SIGNALED,
            .wr.rdma = { .remote_addr = reads[i].raddr, .rkey = reads[i].rkey },
            .next = i + 1 < n ? &wr[i + 1] : NULL,
        };
    }
    struct ibv_send_wr *bad = NULL;
    int rc = ibv_post_send(r->id->qp, wr, &bad);
    if (rc) { set_error("ibv_post_send (RDMA READ) failed: %s", strerror(rc)); return ROCE_ERR_NOT_DELIVERED; }
    for (int i = 0; i < n; i++)
        if (poll_for_completion(r, WR_READ, timeout_ms) < 0) return -1;
    return 0;
}

void roce_reply_release(roce_reply_t *reply) {
    if (!reply || !reply->data) return;
    if (reply->pool) pool_put(reply->pool, reply->buf);
    else free(reply->data);
    reply->data = NULL;
    reply->len = 0;
    reply->pool = NULL;
    reply->buf = NULL;
}

int roce_client_request(roce_conn_t *conn, const void *head, size_t head_len, const roce_seg_t *segs, int nsegs,
                        roce_reply_t *reply, int timeout_ms) {
    /* ROCE_ERR_NOT_DELIVERED: the request provably never left this host --
     * the connection was already known dead before posting (CM event or a
     * flushed RECV, see conn_known_dead), or posting itself failed. Only
     * then may a caller retry a non-idempotent request on a new connection.
     * Once the SEND is posted every failure is ambiguous and stays -1 -- a
     * WR_FLUSH_ERR on the SEND included: a QP entering the error state
     * flushes WRs that were already transmitted but not yet ACKed, and the
     * responder may have received and executed such a request. */
    conn->qp.last_wc_status = 0;
    memset(reply, 0, sizeof(*reply));
    if (conn_known_dead(conn)) {
        set_error("connection already closed by peer (detected before sending)");
        return ROCE_ERR_NOT_DELIVERED;
    }
    return client_request(conn, head, head_len, segs, nsegs, reply, timeout_ms);
}

int roce_client_send_request(roce_conn_t *conn, const uint8_t *request, size_t request_len,
                              uint8_t **response_out, size_t *response_len_out, int timeout_ms) {
    roce_reply_t reply;
    int rc = roce_client_request(conn, request, request_len, NULL, 0, &reply, timeout_ms);
    if (rc) return rc;
    if (!reply.pool) {  /* dedicated mode: already a malloc'd copy */
        *response_out = reply.data;
        *response_len_out = reply.len;
        return 0;
    }
    size_t n = reply.len;
    uint8_t *out = malloc(n ? n : 1);
    if (!out) { roce_reply_release(&reply); set_error("out of memory"); return -1; }
    memcpy(out, reply.data, n);
    roce_reply_release(&reply);
    *response_out = out;
    *response_len_out = n;
    return 0;
}

int roce_client_stream_send(roce_conn_t *conn, const uint8_t *data, size_t len, int timeout_ms) {
    if (conn->qp.pool) { set_error("stream I/O needs a connection from roce_client_connect()"); return -1; }
    if (len > conn->qp.max_frame_bytes) {
        set_error("stream_send len %zu exceeds max_frame_bytes %zu", len, conn->qp.max_frame_bytes);
        return -1;
    }
    memcpy(conn->qp.send_buf, data, len);
    if (post_send(&conn->qp, len)) return -1;
    if (poll_for_completion(&conn->qp, WR_SEND, timeout_ms) < 0) return -1;
    return 0;
}

int roce_client_stream_recv(roce_conn_t *conn, uint8_t **data_out, size_t *len_out, int timeout_ms) {
    if (conn->qp.pool) { set_error("stream I/O needs a connection from roce_client_connect()"); return -1; }
    int recv_len = poll_for_completion(&conn->qp, WR_RECV, timeout_ms);
    if (recv_len < 0) return -1;

    uint8_t *out = malloc((size_t)recv_len > 0 ? (size_t)recv_len : 1);
    if (!out) { set_error("out of memory"); return -1; }
    memcpy(out, conn->qp.recv_buf, (size_t)recv_len);

    if (post_recv(&conn->qp)) { free(out); return -1; }  /* re-arm before returning */

    *data_out = out;
    *len_out = (size_t)recv_len;
    return 0;
}

void roce_client_close(roce_conn_t *conn) {
    if (!conn) return;
    if (conn->qp.id) rdma_disconnect(conn->qp.id);
    teardown_qp_resources(&conn->qp);
    if (conn->qp.id) rdma_destroy_id(conn->qp.id);
    if (conn->channel) rdma_destroy_event_channel(conn->channel);
    free(conn);
}

/* ---------------------------------------------------------------------
 * Server
 * --------------------------------------------------------------------- */
/* Admission gate: caps how many connections one server holds open at once.
 * Without it every incoming connection was accepted immediately and its
 * request received into memory, however far behind the handler was --
 * under overload (tools/stress_concurrency.py, 500 clients, 1 MiB writes on
 * a 2-core host) OSTs queued hundreds of received requests, clients timed
 * out and moved on while the server still executed their queued writes
 * later (orphaned stripes: 281-376 objects stored per OST where ~125 were
 * live), memory grew until the OOM killer took OST processes. With the
 * gate, excess clients wait at connection setup -- nothing is sent or
 * buffered until the server has a slot -- so server memory is bounded and
 * a request is only transmitted once it can be served promptly.
 * Refcounted separately from struct roce_server because detached
 * connection threads can outlive roce_server_stop(). */
struct conn_gate {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int live, max, closed, refs;
    /* In-flight requests (handler executing) across all connections. With
     * pooled persistent connections the connection count no longer tracks
     * load -- idle connections sit open -- so the backpressure that stops
     * a flood of queued requests from exhausting memory is this bound. */
    pthread_cond_t inflight_cond;
    int inflight, max_inflight;
    /* Requests received but whose reply SEND has not completed yet. A
     * handler can wake the application (e.g. an MPI recv) before its reply
     * is on the wire; if the process then stops the server and exits, the
     * client sees the request delivered but the reply flushed -- an
     * ambiguous failure. roce_server_stop() drains this to zero first. */
    int busy;
    pthread_cond_t drain_cond;
    /* Live connections, so roce_server_stop() can disconnect them. Pooled
     * clients keep connections open indefinitely; before this, a stopped
     * server's connection threads went on serving them -- calling a handler
     * whose owner had already been freed (a SIGSEGV in
     * tests/test_conn_pool.py's restart case). */
    struct roce_connection_thread *conns;
};

static void gate_release(struct conn_gate *g) {
    pthread_mutex_lock(&g->lock);
    int refs = --g->refs;
    pthread_mutex_unlock(&g->lock);
    if (refs == 0) {
        pthread_mutex_destroy(&g->lock);
        pthread_cond_destroy(&g->cond);
        pthread_cond_destroy(&g->inflight_cond);
        pthread_cond_destroy(&g->drain_cond);
        free(g);
    }
}

struct roce_connection_thread {
    struct roce_qp_resources qp;
    struct rdma_event_channel *channel;  /* private, via rdma_migrate_id */
    roce_request_handler handler;
    void *user_data;
    pthread_t thread;
    struct conn_gate *gate;  /* one slot + one ref held for this connection's lifetime */
    struct roce_connection_thread *prev, *next;  /* gate->conns, guarded by gate->lock */
    int linked;
};

/* caller holds g->lock */
static void conn_link(struct conn_gate *g, struct roce_connection_thread *ct) {
    ct->prev = NULL;
    ct->next = g->conns;
    if (g->conns) g->conns->prev = ct;
    g->conns = ct;
    ct->linked = 1;
}

/* caller holds g->lock */
static void conn_unlink(struct conn_gate *g, struct roce_connection_thread *ct) {
    if (!ct->linked) return;
    if (ct->prev) ct->prev->next = ct->next; else g->conns = ct->next;
    if (ct->next) ct->next->prev = ct->prev;
    ct->linked = 0;
}

static void conn_done(struct roce_connection_thread *ct) {
    struct conn_gate *g = ct->gate;
    pthread_mutex_lock(&g->lock);
    g->live--;
    pthread_cond_broadcast(&g->cond);  /* accept_loop waiting for a slot, or stop waiting for zero */
    pthread_mutex_unlock(&g->lock);
    gate_release(g);
}

/* A receive buffer consumed from the SRQ goes back to it once its request
 * has been handled (or its completion reported an error). */
static void srq_repost(struct roce_qp_resources *r, uint64_t wr_id) {
    if (WR_KIND(wr_id) != WR_RECV || WR_IDX(wr_id) >= r->n_srq_bufs) return;
    struct rbuf *b = r->srq_bufs[WR_IDX(wr_id)];
    /* cannot fail short of a broken SRQ; the buffer is owned by the SRQ's
     * posted[] list either way, so never hand it to the pool from here */
    (void)post_recv_rbuf(NULL, r->srq, r->pool, b);
}

/* Give back every SRQ buffer this connection still holds before its QP
 * goes: the receive whose error ended the loop, receives stashed while the
 * thread waited for a reply's SEND (a pipelining peer), and completions
 * still in the CQ. The QP is in error (rdma_disconnect) and, per the verbs
 * spec, receives it had already taken from the SRQ complete with a flush
 * status; drain until the CQ has been quiet for 10 ms (bounded, 200 ms).
 * Without this each such exit lost buffers for good, and once the SRQ ran
 * dry every SEND to the server was RNR-NAKed forever. */
static void srq_reclaim(struct roce_qp_resources *r) {
    if (!r->srq) return;
    if (r->last_wc_wr_id) { srq_repost(r, r->last_wc_wr_id); r->last_wc_wr_id = 0; }
    while (r->stash_count[WR_RECV]) {
        int slot = r->stash_head[WR_RECV];
        r->stash_head[WR_RECV] = (slot + 1) % ROCE_STASH;
        r->stash_count[WR_RECV]--;
        srq_repost(r, r->stashed_wr[WR_RECV][slot]);
    }
    long deadline = now_ms() + 200;
    int quiet_ms = 0;
    while (quiet_ms < 10 && now_ms() < deadline) {
        struct ibv_wc wc;
        int n = ibv_poll_cq(r->cq, 1, &wc);
        if (n < 0) break;
        if (n == 0) { usleep(1000); quiet_ms++; continue; }
        quiet_ms = 0;
        if (WR_KIND(wc.wr_id) == WR_RECV) srq_repost(r, wc.wr_id);
    }
}

/* ---- thread placement ----
 * A server's connection threads are spread over the cores the process may
 * run on (its affinity mask: the NUMA node rocepool_servers.c pinned it to,
 * or all cores), one thread per core round-robin, so a thread's CQ, its
 * request buffers and its handler's cache lines stay on one core and one
 * NUMA node (ROCE_PIN_THREADS=0 turns it off). */
static int thread_pin_enabled(void) {
    static int on = -1;
    if (on < 0) { const char *e = getenv("ROCE_PIN_THREADS"); on = !(e && !strcmp(e, "0")); }
    return on;
}
static void pin_to_next_core(void) {
    static atomic_uint next;
    if (!thread_pin_enabled()) return;
    cpu_set_t allowed;
    if (sched_getaffinity(0, sizeof(allowed), &allowed)) return;
    int n = CPU_COUNT(&allowed);
    if (n <= 1) return;
    unsigned k = atomic_fetch_add(&next, 1) % (unsigned)n;
    cpu_set_t one;
    CPU_ZERO(&one);
    for (int c = 0, seen = 0; c < CPU_SETSIZE; c++)
        if (CPU_ISSET(c, &allowed) && seen++ == (int)k) { CPU_SET(c, &one); break; }
    pthread_setaffinity_np(pthread_self(), sizeof(one), &one);
}

static void *serve_one_connection(void *arg) {
    pthread_setname_np(pthread_self(), "rt-conn");
    pin_to_next_core();
    struct roce_connection_thread *ct = arg;
    struct roce_qp_resources *r = &ct->qp;
    for (;;) {
        uint64_t wr = 0;
        int recv_len = poll_for_completion_wr(r, WR_RECV, -1 /* no timeout: block until closed */, &wr);
        if (recv_len < 0) break;  /* peer disconnected or error; srq_reclaim returns an errored receive's buffer */
        if (WR_IDX(wr) >= r->n_srq_bufs) { set_error("receive completion for unknown buffer"); break; }
        struct rbuf *req = r->srq_bufs[WR_IDX(wr)];

        /* the request: inline in the SEND, or (envelope) partly in the
         * client's memory, RDMA READ into a pool buffer of our own */
        const uint8_t *frame = req->addr;
        size_t frame_len = (size_t)recv_len;
        struct rt_env env = { 0 };
        int enveloped = recv_len >= (int)RT_ENV_SIZE && memcmp(req->addr, &(uint32_t){ RT_ENV_MAGIC }, 4) == 0;
        struct rbuf *fb = NULL;
        int fetch_ok = 1;
        if (enveloped) {
            memcpy(&env, req->addr, RT_ENV_SIZE);
            if (env.nsrc > RT_MAX_SRC || (size_t)env.inline_len + RT_ENV_SIZE > (size_t)recv_len ||
                env.total_len > r->max_frame_bytes) {
                set_error("malformed request envelope");
                srq_repost(r, wr);
                break;
            }
            if (env.nsrc == 0) {
                frame = req->addr + RT_ENV_SIZE;
                frame_len = env.inline_len;
            } else {
                fb = pool_get(r->pool, 30000);
                if (!fb) { srq_repost(r, wr); break; }
                /* the inline part is placed so that the first bulk source
                 * (a PUT's or WRITE_AT's data) starts on a page boundary:
                 * an OST with a capacity tier can then hand the NIC's
                 * buffer to the NVMe as it is (O_DIRECT / SPDK / P2P) */
                size_t pad = (RT_PAGE - env.inline_len % RT_PAGE) % RT_PAGE;
                if (pad + env.total_len > r->pool->frame) pad = 0;
                uint8_t *fbase = fb->addr + pad;
                memcpy(fbase, req->addr + RT_ENV_SIZE, env.inline_len);
                srq_repost(r, wr);  /* the SEND's bytes are consumed: give the SRQ its buffer back now */
                wr = 0;
                size_t off = env.inline_len;
                uint32_t posted = 0;
                for (uint32_t i = 0; i < env.nsrc && fetch_ok; i++) {
                    if (off + env.src[i].len > env.total_len) { set_error("malformed request envelope"); fetch_ok = 0; break; }
                    if (post_read(r, fbase + off, fb->lkey, env.src[i].addr, env.src[i].rkey, env.src[i].len, i))
                        fetch_ok = 0;
                    else posted++;
                    off += env.src[i].len;
                }
                for (uint32_t i = 0; i < posted; i++)
                    if (poll_for_completion(r, WR_READ, 30000) < 0) fetch_ok = 0;
                if (!fetch_ok) { pool_put(r->pool, fb); break; }
                frame = fbase;
                frame_len = env.total_len;
            }
        }

        size_t response_len = 0;
        struct conn_gate *g = ct->gate;
        pthread_mutex_lock(&g->lock);
        if (g->closed) {  /* server stopping: do not start new work */
            pthread_mutex_unlock(&g->lock);
            if (wr) srq_repost(r, wr);
            if (fb) pool_put(r->pool, fb);
            break;
        }
        g->busy++;
        while (g->inflight >= g->max_inflight) pthread_cond_wait(&g->inflight_cond, &g->lock);
        g->inflight++;
        pthread_mutex_unlock(&g->lock);
        tl_reply_pool = r->pool;
        uint8_t *response = ct->handler(frame, frame_len, &response_len, ct->user_data);
        tl_reply_pool = NULL;
        pthread_mutex_lock(&g->lock);
        g->inflight--;
        pthread_cond_signal(&g->inflight_cond);
        pthread_mutex_unlock(&g->lock);
        if (wr) srq_repost(r, wr);  /* the handler is done with the request bytes */
        if (fb) pool_put(r->pool, fb);
        int ok = 0;
        if (response) {
            struct rbuf *pooled = pool_find(r->pool, response);
            if (response_len > r->max_frame_bytes) {
                set_error("handler response_len %zu exceeds max_frame_bytes %zu", response_len, r->max_frame_bytes);
            } else if (response_len <= ROCE_SMALL_FRAME) {
                memcpy(r->small_buf, response, response_len);
                ok = !post_send_from(r, r->small_buf, r->small_mr->lkey, response_len) &&
                     poll_for_completion(r, WR_SEND, -1) >= 0;
            } else {
                /* large reply: RDMA WRITE into the client's sink when it
                 * offered one big enough, else a SEND; from the handler's
                 * own pool buffer when it used roce_reply_alloc (no copy) */
                struct rbuf *sb = pooled ? pooled : pool_get(r->pool, 30000);
                if (sb) {
                    if (!pooled) memcpy(sb->addr, response, response_len);
                    int prc = env.sink.len >= response_len
                        ? post_write_imm(r, sb->addr, sb->lkey, &env.sink, (uint32_t)response_len)
                        : post_send_from(r, sb->addr, sb->lkey, response_len);
                    ok = !prc && poll_for_completion(r, WR_SEND, -1) >= 0;
                    if (ok) { if (!pooled) pool_put(r->pool, sb); }
                    else { r->held[1] = sb; if (pooled) pooled = NULL; }  /* returned by teardown, after the QP is destroyed */
                }
            }
            if (pooled) pool_put(r->pool, pooled);
            else if (!pool_find(r->pool, response)) free(response);
        }
        pthread_mutex_lock(&g->lock);
        if (--g->busy == 0) pthread_cond_broadcast(&g->drain_cond);
        pthread_mutex_unlock(&g->lock);
        if (!ok) break;
    }
    /* unlink first: from here on roce_server_stop() no longer touches this id */
    pthread_mutex_lock(&ct->gate->lock);
    conn_unlink(ct->gate, ct);
    pthread_mutex_unlock(&ct->gate->lock);
    rdma_disconnect(ct->qp.id);
    srq_reclaim(r);
    teardown_qp_resources(&ct->qp);
    rdma_destroy_id(ct->qp.id);
    rdma_destroy_event_channel(ct->channel);
    conn_done(ct);
    free(ct);
    return NULL;
}

/* One shared receive queue per device the server accepts on: receive
 * memory is ROCE_SRQ_DEPTH (default 32) frame buffers per server however
 * many clients hold connections open. When all are in use a new request is
 * NAKed RNR and the client's NIC retransmits it -- flow control in the
 * transport, instead of unbounded per-connection receive buffers. */
#define ROCE_MAX_DEVS 8
struct srv_dev {
    struct ibv_context *ctx;
    struct bufpool *pool;    /* its PD (shared with the QPs) and reply buffers */
    struct ibv_srq *srq;
    struct rbuf **posted;    /* the SRQ's own buffers: one slab, registered once */
    uint32_t nposted;
    void *slab;
    size_t slab_len;
    struct ibv_mr *slab_mr;
};

struct roce_server {
    struct rdma_event_channel *listen_channel;
    struct rdma_cm_id *listen_id;
    roce_request_handler handler;
    void *user_data;
    size_t max_frame_bytes;
    pthread_t accept_thread;
    atomic_int stopping;
    char bound_address[128];
    struct conn_gate *gate;
    struct srv_dev devs[ROCE_MAX_DEVS];  /* touched by accept_loop only, and by stop after it joined */
    int ndevs;
};

static struct srv_dev *server_dev_for(struct roce_server *server, struct ibv_context *ctx) {
    for (int i = 0; i < server->ndevs; i++)
        if (server->devs[i].ctx == ctx) return &server->devs[i];
    if (server->ndevs == ROCE_MAX_DEVS) { set_error("too many RDMA devices"); return NULL; }
    struct bufpool *pool = pool_for(ctx, server->max_frame_bytes, POOL_SERVER);
    if (!pool) return NULL;
    uint32_t depth = env_u32("ROCE_SRQ_DEPTH", 32);
    if (depth > ROCE_SRQ_DEPTH_MAX) depth = ROCE_SRQ_DEPTH_MAX;
    struct ibv_device_attr dattr;
    if (ibv_query_device(ctx, &dattr) == 0) {
        if (dattr.max_srq <= 0 || dattr.max_srq_wr <= 0) {
            set_error("RDMA device has no shared receive queue support (max_srq=%d)", dattr.max_srq);
            return NULL;
        }
        if ((int)depth > dattr.max_srq_wr - 1) depth = (uint32_t)(dattr.max_srq_wr - 1);
        if ((uint64_t)pool->frame > dattr.max_mr_size) {
            set_error("max_frame_bytes %zu exceeds the device's max_mr_size %llu", pool->frame,
                      (unsigned long long)dattr.max_mr_size);
            return NULL;
        }
    }
    struct ibv_srq_init_attr attr = { .attr = { .max_wr = depth, .max_sge = 1 } };
    struct ibv_srq *srq = ibv_create_srq(pool->pd, &attr);
    if (!srq) { set_error("ibv_create_srq failed: %s", strerror(errno)); return NULL; }
    struct srv_dev *d = &server->devs[server->ndevs];
    memset(d, 0, sizeof(*d));
    /* The SRQ's buffers are the server's own slab (same PD as the pool, so
     * the QPs can use both), not borrowed from the process pool: several
     * servers in one process must not starve each other's clients. */
    d->slab_len = pool->frame * depth;
    d->slab = mmap(NULL, d->slab_len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    d->posted = calloc(depth, sizeof(*d->posted));
    if (d->slab == MAP_FAILED || !d->posted) {
        if (d->slab != MAP_FAILED) munmap(d->slab, d->slab_len);
        free(d->posted);
        ibv_destroy_srq(srq);
        set_error("out of memory for SRQ buffers");
        return NULL;
    }
    d->slab_mr = ibv_reg_mr(pool->pd, d->slab, d->slab_len, IBV_ACCESS_LOCAL_WRITE);
    if (!d->slab_mr) {
        set_reg_mr_error("SRQ buffers", d->slab_len);
        munmap(d->slab, d->slab_len);
        free(d->posted);
        ibv_destroy_srq(srq);
        return NULL;
    }
    atomic_fetch_add(&g_srq_registered, (long long)d->slab_len);
    for (uint32_t i = 0; i < depth; i++) {
        struct rbuf *b = calloc(1, sizeof(*b));
        if (!b) break;
        b->addr = (uint8_t *)d->slab + (size_t)i * pool->frame;
        b->lkey = d->slab_mr->lkey;
        b->idx = i;
        d->posted[d->nposted++] = b;
        if (post_recv_rbuf(NULL, srq, pool, b)) break;
    }
    d->ctx = ctx;
    d->pool = pool;
    d->srq = srq;
    server->ndevs++;
    return d;
}

static void *accept_loop(void *arg) {
    pthread_setname_np(pthread_self(), "rt-accept");
    struct roce_server *server = arg;
    struct conn_gate *g = server->gate;
    while (!atomic_load(&server->stopping)) {
        /* wait for a free slot BEFORE taking the next connection request */
        pthread_mutex_lock(&g->lock);
        while (g->live >= g->max && !g->closed) pthread_cond_wait(&g->cond, &g->lock);
        int closed = g->closed;
        pthread_mutex_unlock(&g->lock);
        if (closed) break;

        struct rdma_cm_event *event;
        /* rdma_get_cm_event blocks; roce_server_stop() unblocks it by
         * destroying listen_id, which the RDMA CM surfaces as a
         * DEVICE_REMOVAL-shaped event or an error return here. */
        /* poll() with a short timeout instead of blocking in
         * rdma_get_cm_event(): with real rdma-core, destroying the listen id
         * does NOT wake a thread blocked on its channel, so stop() would
         * hang in pthread_join. The emulator happened to push a wake-up
         * event; real hardware does not. */
        struct pollfd pfd = { .fd = server->listen_channel->fd, .events = POLLIN };
        int prc = poll(&pfd, 1, 100);
        if (prc < 0 && errno != EINTR) break;
        if (prc <= 0) continue;
        if (rdma_get_cm_event(server->listen_channel, &event)) break;

        if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            rdma_ack_cm_event(event);
            continue;
        }
        struct rdma_cm_id *new_id = event->id;
        rdma_ack_cm_event(event);

        struct roce_connection_thread *ct = calloc(1, sizeof(*ct));
        ct->handler = server->handler;
        ct->user_data = server->user_data;
        ct->qp.id = new_id;
        ct->gate = g;

        /* Move this connection's events onto its own channel so its
         * lifecycle (and any future DISCONNECTED event) can be handled
         * by its own thread without blocking the shared listener. */
        ct->channel = rdma_create_event_channel();
        if (!ct->channel || rdma_migrate_id(new_id, ct->channel)) {
            rdma_destroy_id(new_id);
            if (ct->channel) rdma_destroy_event_channel(ct->channel);
            free(ct);
            continue;
        }
        struct srv_dev *dev = server_dev_for(server, new_id->verbs);
        ct->qp.cm_channel = ct->channel;
        if (dev) { ct->qp.srq_bufs = dev->posted; ct->qp.n_srq_bufs = dev->nposted; }
        if (!dev || setup_qp_resources(&ct->qp, server->max_frame_bytes, QP_SHARED_SERVER, dev->pool, dev->srq)) {
            teardown_qp_resources(&ct->qp);
            rdma_destroy_id(new_id);
            rdma_destroy_event_channel(ct->channel);
            free(ct);
            continue;
        }
        struct rdma_conn_param conn_param = { .rnr_retry_count = ROCE_RNR_RETRY_INFINITE };
        rd_atomic_params(new_id->verbs, &conn_param);
        if (rdma_accept(new_id, &conn_param)) {
            teardown_qp_resources(&ct->qp);
            rdma_destroy_id(new_id);
            rdma_destroy_event_channel(ct->channel);
            free(ct);
            continue;
        }
        /* Create the connection thread already DETACHED. The old
         * `pthread_create(&ct->thread, ...); pthread_detach(ct->thread);`
         * was a use-after-free: with connect-per-request clients the new
         * thread can serve its one request, see the disconnect and
         * free(ct) before this thread gets to read ct->thread -- the
         * detach then ran on garbage and SIGSEGV'd the whole server.
         * Found by tools/stress_concurrency.py: MDT processes died at
         * ~50 concurrent clients; the core dump showed
         * pthread_detach(th=128) called from here. */
        pthread_mutex_lock(&g->lock);
        g->live++;
        g->refs++;
        conn_link(g, ct);
        pthread_mutex_unlock(&g->lock);
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_t tid;
        int rc = pthread_create(&tid, &attr, serve_one_connection, ct);
        pthread_attr_destroy(&attr);
        if (rc != 0) {
            /* out of threads: drop this one connection, keep serving */
            set_error("pthread_create for connection failed: %s", strerror(rc));
            pthread_mutex_lock(&g->lock);
            conn_unlink(g, ct);
            pthread_mutex_unlock(&g->lock);
            rdma_disconnect(new_id);
            teardown_qp_resources(&ct->qp);
            rdma_destroy_id(new_id);
            rdma_destroy_event_channel(ct->channel);
            conn_done(ct);
            free(ct);
        }
    }
    return NULL;
}

roce_server_t *roce_server_start(const char *bind_address, roce_request_handler handler,
                                  void *user_data, size_t max_frame_bytes) {
    struct roce_server *server = calloc(1, sizeof(*server));
    if (!server) { set_error("out of memory"); return NULL; }
    server->handler = handler;
    server->user_data = user_data;
    server->max_frame_bytes = max_frame_bytes;

    server->gate = calloc(1, sizeof(*server->gate));
    if (!server->gate) { set_error("out of memory"); free(server); return NULL; }
    pthread_mutex_init(&server->gate->lock, NULL);
    pthread_cond_init(&server->gate->cond, NULL);
    pthread_cond_init(&server->gate->inflight_cond, NULL);
    pthread_cond_init(&server->gate->drain_cond, NULL);
    server->gate->refs = 1;
    server->gate->max = ROCE_SERVER_MAX_CONNS_DEFAULT;
    const char *env_max = getenv("ROCE_SERVER_MAX_CONNS");
    if (env_max && atoi(env_max) > 0) server->gate->max = atoi(env_max);
    server->gate->max_inflight = ROCE_SERVER_MAX_INFLIGHT_DEFAULT;
    const char *env_inf = getenv("ROCE_SERVER_MAX_INFLIGHT");
    if (env_inf && atoi(env_inf) > 0) server->gate->max_inflight = atoi(env_inf);

    server->listen_channel = rdma_create_event_channel();
    if (!server->listen_channel) { set_error("rdma_create_event_channel failed"); goto fail; }
    if (rdma_create_id(server->listen_channel, &server->listen_id, NULL, RDMA_PS_TCP)) {
        set_error("rdma_create_id failed: %s", strerror(errno)); goto fail;
    }
    apply_cm_options(server->listen_id);  /* accepted connections inherit it */

    struct sockaddr_storage addr;
    if (resolve_peer(bind_address, &addr)) goto fail;
    if (rdma_bind_addr(server->listen_id, (struct sockaddr *)&addr)) {
        set_error("rdma_bind_addr failed: %s", strerror(errno)); goto fail;
    }
    /* Accept backlog: 64 overflowed at 200 concurrent connect-per-request
     * clients (tools/stress_concurrency.py: kernel ListenOverflows > 0, i.e.
     * SYNs dropped and retried after 1 s -- the ~1.1 s p99 at N=200). The
     * kernel still caps this at net.core.somaxconn. */
    if (rdma_listen(server->listen_id, ROCE_LISTEN_BACKLOG)) {
        set_error("rdma_listen failed: %s", strerror(errno)); goto fail;
    }

    struct sockaddr *bound = rdma_get_local_addr(server->listen_id);
    struct sockaddr_in *bound_in = (struct sockaddr_in *)bound;
    snprintf(server->bound_address, sizeof(server->bound_address), "%s:%d",
             inet_ntoa(bound_in->sin_addr), ntohs(bound_in->sin_port));

    atomic_init(&server->stopping, 0);
    if (pthread_create(&server->accept_thread, NULL, accept_loop, server)) {
        set_error("pthread_create for accept loop failed"); goto fail;
    }
    return server;

fail:
    if (server->listen_id) rdma_destroy_id(server->listen_id);
    if (server->listen_channel) rdma_destroy_event_channel(server->listen_channel);
    gate_release(server->gate);
    free(server);
    return NULL;
}

int roce_server_mem_rkey(roce_server_t *server, roce_mem_t *mem, uint32_t *rkey) {
    /* the PD every accepted connection of this device uses: the server's
     * process-wide pool for it. Bound to an address, the listen id already
     * knows the device, so a server can register (pin) large regions at
     * start-up instead of inside the first request that needs them --
     * pinning 16 GiB takes seconds, longer than a client's RPC timeout. */
    struct ibv_pd *pd = NULL;
    if (server->ndevs >= 1) pd = server->devs[0].pool->pd;
    else if (server->listen_id && server->listen_id->verbs) {
        struct bufpool *p = pool_for(server->listen_id->verbs, server->max_frame_bytes, POOL_SERVER);
        if (p) pd = p->pd;
    }
    if (!pd) { set_error("no RDMA device yet (no client has connected)"); return -1; }
    uint32_t lkey;
    return mem_keys(mem, pd, &lkey, rkey);
}

uint64_t roce_server_mem_addr(struct roce_server *server, roce_mem_t *mem, const void *va) {
    (void)server; (void)mem;
    return (uint64_t)(uintptr_t)va;
}

int roce_server_local_address(roce_server_t *server, char *out, size_t out_len) {
    if (!server->bound_address[0]) { set_error("server not bound"); return -1; }
    strncpy(out, server->bound_address, out_len - 1);
    out[out_len - 1] = 0;
    return 0;
}

static void deadline_after(struct timespec *dl, long ms) {
    clock_gettime(CLOCK_REALTIME, dl);
    dl->tv_sec += ms / 1000;
    dl->tv_nsec += (ms % 1000) * 1000000L;
    if (dl->tv_nsec >= 1000000000L) { dl->tv_sec++; dl->tv_nsec -= 1000000000L; }
}

/* Stop sequence:
 *   1. stop accepting (accept thread joined, listen id destroyed);
 *   2. drain: requests already received finish sending their replies
 *      (bounded by ROCE_SERVER_DRAIN_MS, default 2000) -- a handler can
 *      wake the application (an MPI recv, say) before its reply is on the
 *      wire, and a process exiting right after stop() used to turn that
 *      into a delivered request with a lost reply;
 *   3. disconnect every remaining (idle) connection, which tells pooled
 *      clients through the CM (they drop the connection before reusing it);
 *   4. wait for those connection threads to exit, so the handler and
 *      user_data are never called after this returns (same bound). */
void roce_server_stop(roce_server_t *server) {
    if (!server) return;
    struct conn_gate *g = server->gate;
    long drain_ms = 2000;
    const char *e = getenv("ROCE_SERVER_DRAIN_MS");
    if (e && *e) drain_ms = strtol(e, NULL, 10);

    atomic_store(&server->stopping, 1);
    pthread_mutex_lock(&g->lock);
    g->closed = 1;
    pthread_cond_broadcast(&g->cond);  /* wake accept_loop if it waits for a slot */
    pthread_mutex_unlock(&g->lock);
    pthread_join(server->accept_thread, NULL);
    rdma_destroy_id(server->listen_id);
    rdma_destroy_event_channel(server->listen_channel);

    struct timespec dl;
    deadline_after(&dl, drain_ms);
    pthread_mutex_lock(&g->lock);
    while (g->busy > 0)
        if (pthread_cond_timedwait(&g->drain_cond, &g->lock, &dl)) break;
    /* Disconnect under the lock: a connection thread unlinks itself (under
     * this lock) before it touches its own id, so every id on the list is
     * still valid here. On the emulator this wakes the thread's blocked
     * poll with a flushed RECV; on hardware the QP goes to error likewise. */
    for (struct roce_connection_thread *ct = g->conns; ct; ct = ct->next) rdma_disconnect(ct->qp.id);
    deadline_after(&dl, drain_ms);
    while (g->live > 0)
        if (pthread_cond_timedwait(&g->cond, &g->lock, &dl)) break;
    int all_gone = g->live == 0;
    pthread_mutex_unlock(&g->lock);

    /* With every QP gone, SRQ buffers can go back to the process pool
     * (reused by the next server). If some connection thread outlived the
     * bound, leave the SRQ alone: it may still receive into it. */
    for (int i = 0; all_gone && i < server->ndevs; i++) {
        struct srv_dev *d = &server->devs[i];
        ibv_destroy_srq(d->srq);
        ibv_dereg_mr(d->slab_mr);
        munmap(d->slab, d->slab_len);
        atomic_fetch_sub(&g_srq_registered, (long long)d->slab_len);
        for (uint32_t j = 0; j < d->nposted; j++) free(d->posted[j]);
        free(d->posted);
    }

    gate_release(g);  /* freed once the last live connection also lets go */
    free(server);
}
