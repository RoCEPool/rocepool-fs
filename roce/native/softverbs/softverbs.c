/* softverbs.c -- a userspace software RDMA device, so roce_transport.c can
 * run UNMODIFIED between two real network nodes when no RDMA NIC and no
 * Soft-RoCE (rdma_rxe) kernel module is available.
 *
 * It implements exactly the rdma_cma / ibverbs subset roce_transport.c
 * uses (the same subset declared in ../test_stubs/), carrying RC traffic
 * over a TCP connection per queue pair across whatever real IP link joins
 * the two nodes -- in this project, the veth "crossover cable" built by
 * tools/netlink_veth.py between two network namespaces.
 *
 * What it is: a strict model of Reliable Connected (RC) queue-pair
 * semantics, written so that the bugs RDMA code typically has are
 * FAILURES here rather than things that happen to work over TCP:
 *
 *   - A SEND's completion is generated only after the responder ACKs it
 *     (RC), not when it leaves the sender.
 *   - A SEND that arrives when the responder has no RECV posted is NAKed
 *     RNR (Receiver Not Ready) immediately. The requester retransmits
 *     after an RNR timer per ITS rnr_retry_count (set via
 *     rdma_conn_param; 7 = retry forever, 0 = fail on the first RNR) and
 *     otherwise completes with IBV_WC_RNR_RETRY_EXC_ERR and its QP goes to
 *     the error state -- the same division of labour as real RC hardware.
 *   - A SEND larger than the posted RECV buffer: responder gets
 *     IBV_WC_LOC_LEN_ERR, requester gets IBV_WC_REM_INV_REQ_ERR.
 *   - Every sge's lkey/address range is checked against registered MRs
 *     (IBV_WC_LOC_PROT_ERR otherwise).
 *   - Posting more than max_send_wr / max_recv_wr outstanding WRs fails
 *     with ENOMEM from ibv_post_*.
 *   - Once a QP is in error (or the peer disconnects), every outstanding
 *     and newly-posted WR completes with IBV_WC_WR_FLUSH_ERR.
 *   - rdma_connect() is asynchronous: a missing listener surfaces as an
 *     RDMA_CM_EVENT_REJECTED event, not a return code.
 *
 * What it is NOT: it does not exercise a real RNIC, real kernel verbs,
 * DMA, zero-copy, RoCEv2 UDP/4791 encapsulation, PFC/ECN, or real RDMA
 * performance. Latency/throughput numbers from this are TCP numbers. It
 * validates the *logic* of roce_transport.c (handshake ordering, buffer
 * ownership, completion handling, RECV-before-SEND discipline, teardown)
 * and of everything above it -- not the hardware path.
 *
 * Wire format between two softverbs instances is private to this file
 * (both ends are always this same file on the same architecture).
 */
#define _GNU_SOURCE
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <time.h>
#include <sys/mman.h>
#include <unistd.h>

#define SV_RNR_TIMER_MS 10   /* per-retry RNR back-off (models min_rnr_timer) */
#define SV_MAX_MRS 65536  /* slot index lives in the lkey's low 16 bits */

static int sv_trace(void) {
    static int v = -1;
    if (v < 0) v = getenv("SOFTVERBS_TRACE") ? 1 : 0;
    return v;
}
#define TRACE(...) do { if (sv_trace()) { fprintf(stderr, "[softverbs] " __VA_ARGS__); fputc('\n', stderr); } } while (0)

/* ------------------------------------------------------------------ */
/* device / PD / MR                                                    */
/* ------------------------------------------------------------------ */
struct ibv_context { int dummy; };
static struct ibv_context g_ctx;

struct sv_mr { struct ibv_mr pub; struct ibv_pd *pd; int live; int access;
                uint8_t *view; size_t view_len;   /* dma-buf MR: the emulator's own mapping of the buffer */ };
/* MR table, O(1) in every operation. The lkey encodes the slot in its low
 * 16 bits and a per-slot generation in the high bits, so a stale lkey whose
 * slot was reused still fails validation (a real HCA also rejects a
 * deregistered key). It used to be a fixed 256-entry array scanned linearly
 * under the global lock on every post_send/post_recv: each connection
 * registers 2 MRs, so one server process could hold at most ~127 live
 * connections, and the 128th failed setup_qp_resources and was dropped --
 * found by tools/stress_concurrency.py at 200 clients (hot MDT pinned at
 * 253-254 threads = ~127 connections x 2 threads, clients seeing
 * "unexpected CM event"). Real NICs report max_mr in the millions. */
static struct sv_mr *g_mrs[SV_MAX_MRS];
static uint16_t g_mr_gen[SV_MAX_MRS];
static int g_mr_free[SV_MAX_MRS];
static int g_mr_free_top = -1;  /* -1: free stack not built yet */
static pthread_mutex_t g_mr_lock = PTHREAD_MUTEX_INITIALIZER;

struct ibv_pd *ibv_alloc_pd(struct ibv_context *ctx) {
    struct ibv_pd *pd = calloc(1, sizeof(*pd));
    if (pd) pd->context = ctx;
    return pd;
}
int ibv_dealloc_pd(struct ibv_pd *pd) { free(pd); return 0; }

struct ibv_mr *ibv_reg_mr(struct ibv_pd *pd, void *addr, size_t length, int access) {
    struct sv_mr *mr = calloc(1, sizeof(*mr));
    if (!mr) { errno = ENOMEM; return NULL; }
    pthread_mutex_lock(&g_mr_lock);
    if (g_mr_free_top < 0) {  /* first use: every slot free, lowest on top */
        for (int i = 0; i < SV_MAX_MRS; i++) g_mr_free[i] = SV_MAX_MRS - 1 - i;
        g_mr_free_top = SV_MAX_MRS;
    }
    if (g_mr_free_top == 0) { pthread_mutex_unlock(&g_mr_lock); free(mr); errno = ENOMEM; return NULL; }
    int slot = g_mr_free[--g_mr_free_top];
    uint16_t gen = ++g_mr_gen[slot];
    if (gen == 0) gen = g_mr_gen[slot] = 1;  /* keep lkey != 0 */
    mr->pub.addr = addr; mr->pub.length = length;
    mr->pub.lkey = mr->pub.rkey = ((uint32_t)gen << 16) | (uint32_t)slot;
    mr->pd = pd; mr->live = 1; mr->access = access;
    g_mrs[slot] = mr;
    pthread_mutex_unlock(&g_mr_lock);
    return &mr->pub;
}

/* A dma-buf MR (rdma-core v34+), the way a NIC reaches GPU memory: the
 * buffer is the dma-buf `fd` from `offset`, addressed in work requests at
 * `iova` (the device virtual address). The emulator maps the fd itself and
 * moves bytes through that mapping, so nothing ever dereferences `iova`:
 * a test's "device memory" can be a memfd whose iova range is PROT_NONE in
 * the process, and any CPU access to it by the code under test faults. */
struct ibv_mr *ibv_reg_dmabuf_mr(struct ibv_pd *pd, uint64_t offset, size_t length, uint64_t iova, int fd, int access) {
    void *view = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, (off_t)offset);
    if (view == MAP_FAILED) return NULL;
    struct ibv_mr *pub = ibv_reg_mr(pd, (void *)(uintptr_t)iova, length, access);
    if (!pub) { munmap(view, length); return NULL; }
    struct sv_mr *mr = (struct sv_mr *)pub;
    mr->view = view;
    mr->view_len = length;
    return pub;
}

/* where the bytes of [addr, ...) under key really are: the dma-buf
 * mapping for a dma-buf MR, addr itself otherwise */
static void *sv_host(uint32_t key, uint64_t addr) {
    int slot = (int)(key & 0xFFFF);
    void *p = (void *)(uintptr_t)addr;
    pthread_mutex_lock(&g_mr_lock);
    struct sv_mr *m = slot < SV_MAX_MRS ? g_mrs[slot] : NULL;
    if (m && m->pub.lkey == key && m->view) p = m->view + (addr - (uint64_t)(uintptr_t)m->pub.addr);
    pthread_mutex_unlock(&g_mr_lock);
    return p;
}

int ibv_dereg_mr(struct ibv_mr *pub) {
    int slot = (int)(pub->lkey & 0xFFFF);
    pthread_mutex_lock(&g_mr_lock);
    if (slot < SV_MAX_MRS && g_mrs[slot] && &g_mrs[slot]->pub == pub) {
        g_mrs[slot] = NULL;
        g_mr_free[g_mr_free_top++] = slot;
    }
    pthread_mutex_unlock(&g_mr_lock);
    struct sv_mr *mr = (struct sv_mr *)pub;
    if (mr->view) munmap(mr->view, mr->view_len);
    free(pub);
    return 0;
}

/* sge must fall entirely inside one live MR with that lkey, registered on
 * the same protection domain as the QP/SRQ it is posted to (real HCAs fail
 * a cross-PD lkey with a protection error; the shared-PD buffer pool in
 * roce_transport.c depends on getting this right). pd NULL = don't check. */
static int sge_valid_pd(const struct ibv_sge *sge, const struct ibv_pd *pd) {
    int ok = 0;
    int slot = (int)(sge->lkey & 0xFFFF);
    pthread_mutex_lock(&g_mr_lock);
    struct sv_mr *m = slot < SV_MAX_MRS ? g_mrs[slot] : NULL;
    if (m && m->pub.lkey == sge->lkey) {
        uintptr_t lo = (uintptr_t)m->pub.addr, hi = lo + m->pub.length;
        if (sge->addr >= lo && sge->addr + sge->length <= hi && (!pd || m->pd == pd)) ok = 1;
    }
    pthread_mutex_unlock(&g_mr_lock);
    return ok;
}


/* Remote access (RDMA READ / WRITE from the peer): the rkey must name a
 * live MR that covers [addr, addr+len) and was registered with the access
 * the operation needs (IBV_ACCESS_REMOTE_READ / _WRITE) -- a real HCA
 * answers anything else with a NAK and IBV_WC_REM_ACCESS_ERR at the
 * requester. The PD is not checked: rkeys are device-wide. */
static int rkey_valid(uint32_t rkey, uint64_t addr, uint32_t len, int access) {
    int ok = 0;
    int slot = (int)(rkey & 0xFFFF);
    pthread_mutex_lock(&g_mr_lock);
    struct sv_mr *m = slot < SV_MAX_MRS ? g_mrs[slot] : NULL;
    if (m && m->pub.rkey == rkey && (m->access & access) == access) {
        uintptr_t lo = (uintptr_t)m->pub.addr, hi = lo + m->pub.length;
        if (addr >= lo && addr + len <= hi && addr + len >= addr) ok = 1;
    }
    pthread_mutex_unlock(&g_mr_lock);
    return ok;
}

/* ------------------------------------------------------------------ */
/* CQ                                                                  */
/* ------------------------------------------------------------------ */
/* Completion channel: an eventfd (semaphore mode) that is readable while
 * CQ events are pending, like rdma-core's channel fd -- so callers can
 * poll()/epoll() it with a timeout instead of spinning on ibv_poll_cq. */
struct sv_cq;
struct sv_comp_channel {
    struct ibv_comp_channel pub;
    pthread_mutex_t lock;
    struct sv_cq *q[64];   /* pending events (one per armed CQ at most) */
    int head, count;
};

struct sv_cq {
    struct ibv_cq pub;
    pthread_mutex_t lock;
    struct ibv_wc *ring;
    int cap, head, count, overflowed;
    int armed;
};

static void comp_channel_signal(struct sv_comp_channel *ch, struct sv_cq *cq) {
    pthread_mutex_lock(&ch->lock);
    if (ch->count < 64) {
        ch->q[(ch->head + ch->count) % 64] = cq;
        ch->count++;
        uint64_t one = 1;
        if (write(ch->pub.fd, &one, sizeof(one)) < 0) { /* eventfd full: cannot happen at 64 events */ }
    }
    pthread_mutex_unlock(&ch->lock);
}

struct ibv_comp_channel *ibv_create_comp_channel(struct ibv_context *ctx) {
    struct sv_comp_channel *ch = calloc(1, sizeof(*ch));
    if (!ch) { errno = ENOMEM; return NULL; }
    ch->pub.context = ctx;
    ch->pub.fd = eventfd(0, EFD_SEMAPHORE | EFD_CLOEXEC);
    if (ch->pub.fd < 0) { free(ch); return NULL; }
    pthread_mutex_init(&ch->lock, NULL);
    return &ch->pub;
}

int ibv_destroy_comp_channel(struct ibv_comp_channel *pub) {
    struct sv_comp_channel *ch = (struct sv_comp_channel *)pub;
    if (!ch) return 0;
    close(ch->pub.fd);
    pthread_mutex_destroy(&ch->lock);
    free(ch);
    return 0;
}

int ibv_get_cq_event(struct ibv_comp_channel *pub, struct ibv_cq **cq_out, void **ctx_out) {
    struct sv_comp_channel *ch = (struct sv_comp_channel *)pub;
    uint64_t v;
    if (read(ch->pub.fd, &v, sizeof(v)) != (ssize_t)sizeof(v)) return -1;  /* blocks until an event */
    pthread_mutex_lock(&ch->lock);
    struct sv_cq *cq = ch->count ? ch->q[ch->head] : NULL;
    if (ch->count) { ch->head = (ch->head + 1) % 64; ch->count--; }
    pthread_mutex_unlock(&ch->lock);
    if (!cq) { errno = EAGAIN; return -1; }
    *cq_out = &cq->pub;
    if (ctx_out) *ctx_out = cq->pub.cq_context;
    return 0;
}

void ibv_ack_cq_events(struct ibv_cq *cq, unsigned int n) { (void)cq; (void)n; }

int ibv_req_notify_cq(struct ibv_cq *pub, int solicited_only) {
    (void)solicited_only;
    struct sv_cq *cq = (struct sv_cq *)pub;
    pthread_mutex_lock(&cq->lock);
    cq->armed = 1;   /* next completion added after this call raises one event */
    pthread_mutex_unlock(&cq->lock);
    return 0;
}

struct ibv_cq *ibv_create_cq(struct ibv_context *ctx, int cqe, void *cq_context, struct ibv_comp_channel *channel, int vec) {
    (void)vec;
    struct sv_cq *cq = calloc(1, sizeof(*cq));
    if (!cq) return NULL;
    cq->pub.context = ctx;
    cq->pub.channel = channel;
    cq->pub.cq_context = cq_context;
    cq->cap = cqe > 0 ? cqe : 1;
    cq->ring = calloc((size_t)cq->cap, sizeof(struct ibv_wc));
    pthread_mutex_init(&cq->lock, NULL);
    return &cq->pub;
}

int ibv_destroy_cq(struct ibv_cq *pub) {
    struct sv_cq *cq = (struct sv_cq *)pub;
    struct sv_comp_channel *ch = (struct sv_comp_channel *)pub->channel;
    if (ch) {  /* drop any undelivered event that still points at this CQ */
        pthread_mutex_lock(&ch->lock);
        for (int i = 0; i < ch->count; i++)
            if (ch->q[(ch->head + i) % 64] == cq) ch->q[(ch->head + i) % 64] = NULL;
        pthread_mutex_unlock(&ch->lock);
    }
    pthread_mutex_destroy(&cq->lock);
    free(cq->ring);
    free(cq);
    return 0;
}

static void cq_push_ex(struct ibv_cq *pub, uint64_t wr_id, enum ibv_wc_status st, uint32_t len,
                       enum ibv_wc_opcode opcode, unsigned flags, uint32_t imm) {
    struct sv_cq *cq = (struct sv_cq *)pub;
    int fire = 0;
    pthread_mutex_lock(&cq->lock);
    if (cq->count == cq->cap) {
        cq->overflowed = 1;  /* real HW: IBV_EVENT_CQ_ERR; we surface it as poll_cq < 0 */
    } else {
        struct ibv_wc *wc = &cq->ring[(cq->head + cq->count) % cq->cap];
        memset(wc, 0, sizeof(*wc));
        wc->wr_id = wr_id; wc->status = st; wc->byte_len = len;
        wc->opcode = opcode; wc->wc_flags = flags; wc->imm_data = imm;
        cq->count++;
    }
    if (cq->armed && pub->channel) { cq->armed = 0; fire = 1; }
    pthread_mutex_unlock(&cq->lock);
    if (fire) comp_channel_signal((struct sv_comp_channel *)pub->channel, cq);
    TRACE("cq %p <- wr_id=%llu status=%s len=%u", (void *)pub, (unsigned long long)wr_id, ibv_wc_status_str(st), len);
}

static void cq_push(struct ibv_cq *pub, uint64_t wr_id, enum ibv_wc_status st, uint32_t len) {
    cq_push_ex(pub, wr_id, st, len, IBV_WC_SEND, 0, 0);
}

int ibv_poll_cq(struct ibv_cq *pub, int n, struct ibv_wc *out) {
    struct sv_cq *cq = (struct sv_cq *)pub;
    pthread_mutex_lock(&cq->lock);
    if (cq->overflowed) { pthread_mutex_unlock(&cq->lock); return -1; }
    int got = 0;
    while (got < n && cq->count > 0) {
        out[got++] = cq->ring[cq->head];
        cq->head = (cq->head + 1) % cq->cap;
        cq->count--;
    }
    pthread_mutex_unlock(&cq->lock);
    return got;
}

const char *ibv_wc_status_str(enum ibv_wc_status s) {
    switch (s) {
    case IBV_WC_SUCCESS: return "success";
    case IBV_WC_LOC_LEN_ERR: return "local length error";
    case IBV_WC_LOC_QP_OP_ERR: return "local QP operation error";
    case IBV_WC_LOC_PROT_ERR: return "local protection error";
    case IBV_WC_WR_FLUSH_ERR: return "Work Request Flushed Error";
    case IBV_WC_REM_INV_REQ_ERR: return "remote invalid request error";
    case IBV_WC_REM_ACCESS_ERR: return "remote access error";
    case IBV_WC_RETRY_EXC_ERR: return "transport retry counter exceeded";
    case IBV_WC_RNR_RETRY_EXC_ERR: return "RNR retry counter exceeded";
    }
    return "unknown";
}

/* ------------------------------------------------------------------ */
/* CM event channel                                                    */
/* ------------------------------------------------------------------ */
struct sv_event_node { struct rdma_cm_event ev; struct sv_event_node *next; };

struct sv_channel {
    struct rdma_event_channel pub;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    struct sv_event_node *head, *tail;
};

struct rdma_event_channel *rdma_create_event_channel(void) {
    struct sv_channel *ch = calloc(1, sizeof(*ch));
    if (!ch) return NULL;
    /* readable (semaphore count = queued events) exactly while events are
     * pending, like rdma-core's channel fd, so callers can poll() it */
    ch->pub.fd = eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK | EFD_CLOEXEC);
    if (ch->pub.fd < 0) { free(ch); return NULL; }
    pthread_mutex_init(&ch->lock, NULL);
    pthread_cond_init(&ch->cond, NULL);
    return &ch->pub;
}

void rdma_destroy_event_channel(struct rdma_event_channel *pub) {
    struct sv_channel *ch = (struct sv_channel *)pub;
    if (!ch) return;
    struct sv_event_node *n = ch->head;
    while (n) { struct sv_event_node *nx = n->next; free(n); n = nx; }
    if (ch->pub.fd >= 0) close(ch->pub.fd);
    pthread_mutex_destroy(&ch->lock);
    pthread_cond_destroy(&ch->cond);
    free(ch);
}

static void channel_push(struct rdma_event_channel *pub, struct rdma_cm_id *id,
                          enum rdma_cm_event_type type, int status) {
    struct sv_channel *ch = (struct sv_channel *)pub;
    if (!ch) return;
    struct sv_event_node *n = calloc(1, sizeof(*n));
    n->ev.id = id; n->ev.event = type; n->ev.status = status;
    pthread_mutex_lock(&ch->lock);
    if (ch->tail) ch->tail->next = n; else ch->head = n;
    ch->tail = n;
    uint64_t one = 1;
    if (write(ch->pub.fd, &one, sizeof(one)) < 0) { /* counter overflow: unreachable */ }
    pthread_cond_broadcast(&ch->cond);
    pthread_mutex_unlock(&ch->lock);
    TRACE("cm event %s -> channel %p", rdma_event_str(type), (void *)pub);
}

int rdma_get_cm_event(struct rdma_event_channel *pub, struct rdma_cm_event **event) {
    struct sv_channel *ch = (struct sv_channel *)pub;
    pthread_mutex_lock(&ch->lock);
    while (!ch->head) pthread_cond_wait(&ch->cond, &ch->lock);
    struct sv_event_node *n = ch->head;
    ch->head = n->next;
    if (!ch->head) ch->tail = NULL;
    uint64_t v;
    if (read(ch->pub.fd, &v, sizeof(v)) < 0) { /* count mirrors the list: cannot be empty here */ }
    pthread_mutex_unlock(&ch->lock);
    *event = &n->ev;  /* ev is the first member: ack frees the node */
    return 0;
}

int rdma_ack_cm_event(struct rdma_cm_event *event) { free(event); return 0; }

const char *rdma_event_str(enum rdma_cm_event_type e) {
    static const char *names[] = {
        "RDMA_CM_EVENT_ADDR_RESOLVED", "RDMA_CM_EVENT_ADDR_ERROR", "RDMA_CM_EVENT_ROUTE_RESOLVED",
        "RDMA_CM_EVENT_ROUTE_ERROR", "RDMA_CM_EVENT_CONNECT_REQUEST", "RDMA_CM_EVENT_CONNECT_RESPONSE",
        "RDMA_CM_EVENT_CONNECT_ERROR", "RDMA_CM_EVENT_UNREACHABLE", "RDMA_CM_EVENT_REJECTED",
        "RDMA_CM_EVENT_ESTABLISHED", "RDMA_CM_EVENT_DISCONNECTED", "RDMA_CM_EVENT_DEVICE_REMOVAL",
    };
    return (e >= 0 && (size_t)e < sizeof(names) / sizeof(names[0])) ? names[e] : "RDMA_CM_EVENT_UNKNOWN";
}

/* ------------------------------------------------------------------ */
/* QP + cm_id                                                          */
/* ------------------------------------------------------------------ */
/* F_DATA: a SEND. F_WRITE: an RDMA WRITE (flags & SVF_IMM: with immediate,
 * consumes a RECV at the responder like a SEND). F_READ_REQ / F_READ_RESP:
 * an RDMA READ and its data. */
enum { F_CONNREQ = 1, F_CONNREP = 2, F_DATA = 3, F_ACK = 4, F_NAK = 5, F_WRITE = 6, F_READ_REQ = 7,
       F_READ_RESP = 8 };
#define SVF_IMM 1

struct sv_frame {           /* private wire header, host byte order */
    uint8_t type, status, rnr_retry, flags;
    uint32_t len;
    uint64_t seq;
    uint64_t raddr;         /* F_WRITE / F_READ_REQ: remote address + rkey */
    uint32_t rkey;
    uint32_t imm;           /* F_WRITE with SVF_IMM: immediate data (network order, opaque here) */
    uint8_t rd_atomic;      /* F_CONNREQ / F_CONNREP: responder_resources the sender offers */
    uint8_t pad[7];
};

struct sv_id;
struct sv_recv { uint64_t wr_id; struct ibv_sge sge; };
struct sv_send {
    uint64_t wr_id, seq;
    int signaled;
    int rnr_left;             /* remaining RNR retries; 7 == infinite */
    uint8_t *payload; uint32_t len;   /* kept for RNR retransmission */
    enum ibv_wr_opcode opcode;
    uint64_t raddr; uint32_t rkey; uint32_t imm;  /* RDMA ops */
    struct ibv_sge sge;       /* RDMA READ: where the data lands */
    struct sv_send *next;
};
#define SV_NAK_RNR 0xFE       /* private wire code: responder had no RECV posted */

/* Shared receive queue: many QPs consume one pool of posted receives. A
 * SEND arriving while it is empty is NAKed RNR exactly like a QP with no
 * RECV posted (the requester's rnr_retry decides what happens next), and a
 * QP going to error does NOT flush SRQ receives -- they belong to the SRQ. */
struct sv_srq {
    struct ibv_srq pub;
    pthread_mutex_t lock;
    struct sv_recv *ring;
    uint32_t max_wr, head, count;
};

struct sv_qp {
    struct ibv_qp pub;
    struct ibv_cq *send_cq, *recv_cq;
    uint32_t max_send_wr, max_recv_wr;
    struct sv_recv *rq;     /* ring of posted receives */
    uint32_t rq_head, rq_count;
    struct sv_send *outstanding;  /* sends awaiting ACK/NAK */
    uint32_t sq_count;
    uint64_t next_seq;
    int error;
    struct sv_id *owner;
    struct sv_srq *srq;     /* non-NULL: receives are taken from this SRQ */
};

struct sv_id {
    struct rdma_cm_id pub;        /* must be first */
    pthread_mutex_t lock;         /* guards qp state + socket writes */
    pthread_cond_t rq_cond;       /* signalled when a RECV is posted */
    int fd;
    int listening;
    pthread_t thread; int thread_started;
    struct sockaddr_storage dst, local;
    uint8_t my_rnr_retry, peer_rnr_retry;
    /* RDMA READ needs resources on both sides, negotiated by the CM:
     * my_rd_atomic = READs I serve (my responder_resources), my_init_depth
     * = READs I may have in flight (my initiator_depth), peer_rd_atomic =
     * READs the peer serves. A READ posted or received without them fails
     * the way real RC hardware fails it. */
    uint8_t my_rd_atomic, my_init_depth, peer_rd_atomic;
    struct sv_qp *qp;
};

static struct sv_id *ID(struct rdma_cm_id *pub) { return (struct sv_id *)pub; }

static int write_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = buf;
    while (len) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n <= 0) { if (n < 0 && errno == EINTR) continue; return -1; }
        p += n; len -= (size_t)n;
    }
    return 0;
}
static int read_all(int fd, void *buf, size_t len) {
    uint8_t *p = buf;
    while (len) {
        ssize_t n = recv(fd, p, len, 0);
        if (n <= 0) { if (n < 0 && errno == EINTR) continue; return -1; }
        p += n; len -= (size_t)n;
    }
    return 0;
}

static int send_frame_ex(struct sv_id *id, struct sv_frame *f, const void *payload) {
    f->rnr_retry = id->my_rnr_retry;
    f->rd_atomic = id->my_rd_atomic;
    if (write_all(id->fd, f, sizeof(*f))) return -1;
    if (f->len && write_all(id->fd, payload, f->len)) return -1;
    return 0;
}

static int send_frame(struct sv_id *id, uint8_t type, uint8_t status, uint64_t seq,
                      const void *payload, uint32_t len) {
    struct sv_frame f = { .type = type, .status = status, .len = len, .seq = seq };
    return send_frame_ex(id, &f, payload);
}

/* (re)transmit an outstanding work request; caller holds id->lock */
static int send_wr(struct sv_id *id, const struct sv_send *s) {
    struct sv_frame f = { .seq = s->seq, .raddr = s->raddr, .rkey = s->rkey, .imm = s->imm };
    switch (s->opcode) {
    case IBV_WR_SEND: f.type = F_DATA; f.len = s->len; break;
    case IBV_WR_RDMA_WRITE: f.type = F_WRITE; f.len = s->len; break;
    case IBV_WR_RDMA_WRITE_WITH_IMM: f.type = F_WRITE; f.flags = SVF_IMM; f.len = s->len; break;
    case IBV_WR_RDMA_READ: f.type = F_READ_REQ; f.len = 0; f.imm = s->sge.length; break;
    default: return -1;
    }
    return send_frame_ex(id, &f, s->payload);
}

/* caller holds id->lock */
static void qp_flush(struct sv_id *id) {
    struct sv_qp *qp = id->qp;
    if (!qp) return;
    qp->error = 1;
    while (qp->rq_count) {
        cq_push(qp->recv_cq, qp->rq[qp->rq_head].wr_id, IBV_WC_WR_FLUSH_ERR, 0);
        qp->rq_head = (qp->rq_head + 1) % qp->max_recv_wr;
        qp->rq_count--;
    }
    while (qp->outstanding) {
        struct sv_send *s = qp->outstanding;
        qp->outstanding = s->next;
        cq_push(qp->send_cq, s->wr_id, IBV_WC_WR_FLUSH_ERR, 0);
        free(s->payload);
        free(s);
    }
    qp->sq_count = 0;
    pthread_cond_broadcast(&id->rq_cond);
}

/* caller holds id->lock; completes the outstanding send with this seq */
static void complete_send(struct sv_id *id, uint64_t seq, enum ibv_wc_status st) {
    struct sv_qp *qp = id->qp;
    if (!qp) return;
    for (struct sv_send **pp = &qp->outstanding; *pp; pp = &(*pp)->next) {
        struct sv_send *s = *pp;
        if (s->seq != seq) continue;
        *pp = s->next;
        qp->sq_count--;
        enum ibv_wc_opcode op = s->opcode == IBV_WR_RDMA_READ ? IBV_WC_RDMA_READ
                              : s->opcode == IBV_WR_SEND ? IBV_WC_SEND : IBV_WC_RDMA_WRITE;
        if (st != IBV_WC_SUCCESS || s->signaled)
            cq_push_ex(qp->send_cq, s->wr_id, st, s->opcode == IBV_WR_RDMA_READ ? s->sge.length : 0, op, 0, 0);
        free(s->payload);
        free(s);
        break;
    }
    if (st != IBV_WC_SUCCESS) qp_flush(id);
}

/* responder side of one incoming SEND (F_DATA) or RDMA WRITE (F_WRITE) */
static void deliver(struct sv_id *id, const struct sv_frame *f, const uint8_t *payload) {
    pthread_mutex_lock(&id->lock);
    struct sv_qp *qp = id->qp;
    if (!qp || qp->error) {
        send_frame(id, F_NAK, IBV_WC_REM_INV_REQ_ERR, f->seq, NULL, 0);
        pthread_mutex_unlock(&id->lock);
        return;
    }
    if (f->type == F_WRITE) {
        if (!rkey_valid(f->rkey, f->raddr, f->len, IBV_ACCESS_REMOTE_WRITE)) {
            TRACE("RDMA WRITE seq=%llu: bad rkey/range/access -> REM_ACCESS_ERR", (unsigned long long)f->seq);
            send_frame(id, F_NAK, IBV_WC_REM_ACCESS_ERR, f->seq, NULL, 0);
            qp_flush(id);  /* the responder's QP goes to error too */
            pthread_mutex_unlock(&id->lock);
            return;
        }
        if (!(f->flags & SVF_IMM)) {  /* plain WRITE: no RECV involved, no completion here */
            memcpy(sv_host(f->rkey, f->raddr), payload, f->len);
            send_frame(id, F_ACK, 0, f->seq, NULL, 0);
            pthread_mutex_unlock(&id->lock);
            return;
        }
    }
    /* RNR: real RC responders NAK immediately; the REQUESTER decides
     * whether to retransmit, per its own rnr_retry_count. */
    struct sv_recv r;
    int have = 0;
    if (qp->srq) {
        pthread_mutex_lock(&qp->srq->lock);
        if (qp->srq->count) {
            r = qp->srq->ring[qp->srq->head];
            qp->srq->head = (qp->srq->head + 1) % qp->srq->max_wr;
            qp->srq->count--;
            have = 1;
        }
        pthread_mutex_unlock(&qp->srq->lock);
    } else if (qp->rq_count) {
        r = qp->rq[qp->rq_head];
        qp->rq_head = (qp->rq_head + 1) % qp->max_recv_wr;
        qp->rq_count--;
        have = 1;
    }
    if (!have) {
        TRACE("RNR: %s seq=%llu arrived with no RECV posted", f->type == F_WRITE ? "WRITE_WITH_IMM" : "SEND",
              (unsigned long long)f->seq);
        send_frame(id, F_NAK, SV_NAK_RNR, f->seq, NULL, 0);
        pthread_mutex_unlock(&id->lock);
        return;
    }
    if (f->type == F_WRITE) {  /* with immediate: data to raddr, the RECV's buffer untouched */
        TRACE("RDMA WRITE with immediate placed: %u bytes", f->len);
        memcpy(sv_host(f->rkey, f->raddr), payload, f->len);
        cq_push_ex(qp->recv_cq, r.wr_id, IBV_WC_SUCCESS, f->len, IBV_WC_RECV_RDMA_WITH_IMM, IBV_WC_WITH_IMM, f->imm);
        send_frame(id, F_ACK, 0, f->seq, NULL, 0);
        pthread_mutex_unlock(&id->lock);
        return;
    }
    if (f->len > r.sge.length) {
        cq_push_ex(qp->recv_cq, r.wr_id, IBV_WC_LOC_LEN_ERR, 0, IBV_WC_RECV, 0, 0);
        qp_flush(id);
        send_frame(id, F_NAK, IBV_WC_REM_INV_REQ_ERR, f->seq, NULL, 0);
        pthread_mutex_unlock(&id->lock);
        return;
    }
    memcpy(sv_host(r.sge.lkey, r.sge.addr), payload, f->len);
    cq_push_ex(qp->recv_cq, r.wr_id, IBV_WC_SUCCESS, f->len, IBV_WC_RECV, 0, 0);
    send_frame(id, F_ACK, 0, f->seq, NULL, 0);
    pthread_mutex_unlock(&id->lock);
}

/* responder side of an RDMA READ: answer with the bytes, or NAK */
static void serve_read(struct sv_id *id, const struct sv_frame *f) {
    pthread_mutex_lock(&id->lock);
    struct sv_qp *qp = id->qp;
    uint32_t len = f->imm;  /* F_READ_REQ carries the length in imm */
    if (!qp || qp->error) {
        send_frame(id, F_NAK, IBV_WC_REM_INV_REQ_ERR, f->seq, NULL, 0);
    } else if (id->my_rd_atomic == 0) {
        TRACE("RDMA READ seq=%llu with responder_resources 0 -> REM_INV_REQ_ERR", (unsigned long long)f->seq);
        send_frame(id, F_NAK, IBV_WC_REM_INV_REQ_ERR, f->seq, NULL, 0);
        qp_flush(id);
    } else if (!rkey_valid(f->rkey, f->raddr, len, IBV_ACCESS_REMOTE_READ)) {
        TRACE("RDMA READ seq=%llu: bad rkey/range/access -> REM_ACCESS_ERR", (unsigned long long)f->seq);
        send_frame(id, F_NAK, IBV_WC_REM_ACCESS_ERR, f->seq, NULL, 0);
        qp_flush(id);
    } else {
        TRACE("RDMA READ served: %u bytes", len);
        struct sv_frame r = { .type = F_READ_RESP, .len = len, .seq = f->seq };
        send_frame_ex(id, &r, sv_host(f->rkey, f->raddr));
    }
    pthread_mutex_unlock(&id->lock);
}

/* requester side: the data of one of our RDMA READs arrived */
static void read_response(struct sv_id *id, const struct sv_frame *f, const uint8_t *payload) {
    pthread_mutex_lock(&id->lock);
    struct sv_send *s = NULL;
    if (id->qp) for (s = id->qp->outstanding; s && s->seq != f->seq; s = s->next) {}
    if (s && s->opcode == IBV_WR_RDMA_READ && f->len == s->sge.length) {
        memcpy(sv_host(s->sge.lkey, s->sge.addr), payload, f->len);
        complete_send(id, f->seq, IBV_WC_SUCCESS);
    } else if (s) {
        complete_send(id, f->seq, IBV_WC_LOC_LEN_ERR);
    }
    pthread_mutex_unlock(&id->lock);
}

static void handle_rnr_nak(struct sv_id *id, uint64_t seq) {
    pthread_mutex_lock(&id->lock);
    struct sv_send *s = NULL;
    if (id->qp) for (s = id->qp->outstanding; s && s->seq != seq; s = s->next) {}
    if (!s) { pthread_mutex_unlock(&id->lock); return; }
    if (s->rnr_left == 0) {
        TRACE("RNR retries exhausted for seq=%llu", (unsigned long long)seq);
        complete_send(id, seq, IBV_WC_RNR_RETRY_EXC_ERR);
        pthread_mutex_unlock(&id->lock);
        return;
    }
    if (s->rnr_left != 7) s->rnr_left--;
    pthread_mutex_unlock(&id->lock);
    usleep(SV_RNR_TIMER_MS * 1000);  /* RNR timer; bounded, never waits on the app */
    pthread_mutex_lock(&id->lock);
    if (id->qp && !id->qp->error) {
        for (s = id->qp->outstanding; s && s->seq != seq; s = s->next) {}
        if (s && send_wr(id, s))
            complete_send(id, seq, IBV_WC_RETRY_EXC_ERR);
    }
    pthread_mutex_unlock(&id->lock);
}

static void *reader_thread(void *arg) {
    pthread_setname_np(pthread_self(), "sv-reader");  /* emulator cost: tools/procstat.py */
    struct sv_id *id = arg;
    for (;;) {
        struct sv_frame f;
        if (read_all(id->fd, &f, sizeof(f))) break;
        uint8_t *payload = NULL;
        if (f.len) {
            payload = malloc(f.len);
            if (!payload || read_all(id->fd, payload, f.len)) { free(payload); break; }
        }
        switch (f.type) {
        case F_CONNREP:
            id->peer_rnr_retry = f.rnr_retry;
            id->peer_rd_atomic = f.rd_atomic;
            channel_push(id->pub.channel, &id->pub, RDMA_CM_EVENT_ESTABLISHED, 0);
            break;
        case F_DATA:
        case F_WRITE:
            deliver(id, &f, payload);
            break;
        case F_READ_REQ:
            serve_read(id, &f);
            break;
        case F_READ_RESP:
            read_response(id, &f, payload);
            break;
        case F_NAK:
            if (f.status == SV_NAK_RNR) { handle_rnr_nak(id, f.seq); break; }
            /* fall through */
        case F_ACK:
            pthread_mutex_lock(&id->lock);
            complete_send(id, f.seq, f.type == F_ACK ? IBV_WC_SUCCESS : (enum ibv_wc_status)f.status);
            pthread_mutex_unlock(&id->lock);
            break;
        }
        free(payload);
    }
    pthread_mutex_lock(&id->lock);
    qp_flush(id);
    pthread_mutex_unlock(&id->lock);
    channel_push(id->pub.channel, &id->pub, RDMA_CM_EVENT_DISCONNECTED, 0);
    return NULL;
}

int rdma_create_id(struct rdma_event_channel *channel, struct rdma_cm_id **out, void *context,
                    enum rdma_port_space ps) {
    (void)context;
    if (ps != RDMA_PS_TCP) { errno = EINVAL; return -1; }
    struct sv_id *id = calloc(1, sizeof(*id));
    if (!id) { errno = ENOMEM; return -1; }
    id->pub.channel = channel;
    id->fd = -1;
    pthread_mutex_init(&id->lock, NULL);
    pthread_cond_init(&id->rq_cond, NULL);
    *out = &id->pub;
    return 0;
}

int rdma_resolve_addr(struct rdma_cm_id *pub, struct sockaddr *src, struct sockaddr *dst, int timeout_ms) {
    (void)src; (void)timeout_ms;
    struct sv_id *id = ID(pub);
    memcpy(&id->dst, dst, sizeof(struct sockaddr_in));
    pub->verbs = &g_ctx;  /* the "RDMA device" that routes to dst */
    channel_push(pub->channel, pub, RDMA_CM_EVENT_ADDR_RESOLVED, 0);
    return 0;
}

int rdma_resolve_route(struct rdma_cm_id *pub, int timeout_ms) {
    (void)timeout_ms;
    channel_push(pub->channel, pub, RDMA_CM_EVENT_ROUTE_RESOLVED, 0);
    return 0;
}

int ibv_query_device(struct ibv_context *ctx, struct ibv_device_attr *a) {
    (void)ctx;
    memset(a, 0, sizeof(*a));
    a->max_mr_size = ~0ULL;
    a->max_qp_wr = 16384;
    a->max_cqe = 4194303;
    a->max_mr = SV_MAX_MRS;
    a->max_srq = 1 << 20;
    a->max_srq_wr = 16384;
    a->max_srq_sge = 1;
    a->max_qp_rd_atom = 16;
    a->max_qp_init_rd_atom = 16;
    return 0;
}

/* ToS / ACK timeout have no meaning over TCP; accepted so the transport's
 * real-fabric settings are exercised on this path too. */
int rdma_set_option(struct rdma_cm_id *id, int level, int optname, void *optval, size_t optlen) {
    (void)id;
    if (level != RDMA_OPTION_ID || !optval || optlen != 1 ||
        (optname != RDMA_OPTION_ID_TOS && optname != RDMA_OPTION_ID_ACK_TIMEOUT)) { errno = EINVAL; return -1; }
    return 0;
}

struct ibv_srq *ibv_create_srq(struct ibv_pd *pd, struct ibv_srq_init_attr *attr) {
    if (!pd || !attr || !attr->attr.max_wr) { errno = EINVAL; return NULL; }
    struct sv_srq *q = calloc(1, sizeof(*q));
    if (!q) { errno = ENOMEM; return NULL; }
    q->ring = calloc(attr->attr.max_wr, sizeof(struct sv_recv));
    if (!q->ring) { free(q); errno = ENOMEM; return NULL; }
    q->max_wr = attr->attr.max_wr;
    q->pub.context = pd->context;
    q->pub.pd = pd;
    q->pub.srq_context = attr->srq_context;
    pthread_mutex_init(&q->lock, NULL);
    return &q->pub;
}

int ibv_destroy_srq(struct ibv_srq *pub) {
    struct sv_srq *q = (struct sv_srq *)pub;
    pthread_mutex_destroy(&q->lock);
    free(q->ring);
    free(q);
    return 0;
}

int ibv_post_srq_recv(struct ibv_srq *pub, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad) {
    struct sv_srq *q = (struct sv_srq *)pub;
    int rc = 0;
    pthread_mutex_lock(&q->lock);
    for (; wr; wr = wr->next) {
        if (wr->num_sge != 1 || !sge_valid_pd(wr->sg_list, q->pub.pd)) { *bad = wr; rc = EINVAL; break; }
        if (q->count >= q->max_wr) { *bad = wr; rc = ENOMEM; break; }
        struct sv_recv *slot = &q->ring[(q->head + q->count) % q->max_wr];
        slot->wr_id = wr->wr_id;
        slot->sge = wr->sg_list[0];
        q->count++;
    }
    pthread_mutex_unlock(&q->lock);
    return rc;
}

int rdma_create_qp(struct rdma_cm_id *pub, struct ibv_pd *pd, struct ibv_qp_init_attr *attr) {
    if (attr->qp_type != IBV_QPT_RC || !attr->send_cq || !attr->recv_cq || (!attr->cap.max_recv_wr && !attr->srq)) {
        errno = EINVAL; return -1;
    }
    if (attr->srq && attr->srq->pd != pd) { errno = EINVAL; return -1; }
    struct sv_qp *qp = calloc(1, sizeof(*qp));
    if (!qp) { errno = ENOMEM; return -1; }
    qp->pub.context = pub->verbs;
    qp->pub.pd = pd;
    qp->send_cq = attr->send_cq;
    qp->recv_cq = attr->recv_cq;
    qp->max_send_wr = attr->cap.max_send_wr;
    qp->max_recv_wr = attr->cap.max_recv_wr ? attr->cap.max_recv_wr : 1;
    qp->rq = calloc(qp->max_recv_wr, sizeof(struct sv_recv));
    qp->srq = (struct sv_srq *)attr->srq;
    struct sv_id *id = ID(pub);
    qp->owner = id;
    pthread_mutex_lock(&id->lock);
    id->qp = qp;
    pub->qp = &qp->pub;
    pthread_mutex_unlock(&id->lock);
    return 0;
}

static void stop_reader(struct sv_id *id) {
    pthread_mutex_lock(&id->lock);
    if (id->qp) id->qp->error = 1;          /* wakes a responder parked in RNR wait */
    pthread_cond_broadcast(&id->rq_cond);
    pthread_mutex_unlock(&id->lock);
    /* SHUT_RD, not SHUT_RDWR: on Linux it still wakes the reader thread's
     * blocking recv() (returns 0), but sends no FIN. With SHUT_RDWR our FIN
     * made the peer close back first, and its FIN put THIS socket into
     * TIME_WAIT before close() could abort it -- defeating the SO_LINGER
     * {1,0} set in tune() (still ~50k TIME_WAIT sockets in the stress test
     * after that change alone). The close() that follows sends an RST. */
    if (id->fd >= 0) shutdown(id->fd, SHUT_RD);
    if (id->thread_started) { pthread_join(id->thread, NULL); id->thread_started = 0; }
}

int rdma_destroy_qp(struct rdma_cm_id *pub) {
    struct sv_id *id = ID(pub);
    stop_reader(id);  /* no more completions may land in CQs about to be destroyed */
    pthread_mutex_lock(&id->lock);
    struct sv_qp *qp = id->qp;
    id->qp = NULL;
    pub->qp = NULL;
    pthread_mutex_unlock(&id->lock);
    if (qp) {
        while (qp->outstanding) { struct sv_send *s = qp->outstanding; qp->outstanding = s->next; free(s->payload); free(s); }
        free(qp->rq);
        free(qp);
    }
    return 0;
}

int ibv_post_recv(struct ibv_qp *pubqp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad) {
    struct sv_qp *qp = (struct sv_qp *)pubqp;
    struct sv_id *id = qp->owner;
    int rc = 0;
    pthread_mutex_lock(&id->lock);
    for (; wr; wr = wr->next) {
        if (qp->srq) { *bad = wr; rc = EINVAL; break; }  /* receives come from the SRQ */
        if (wr->num_sge != 1 || !sge_valid_pd(wr->sg_list, qp->pub.pd)) { *bad = wr; rc = EINVAL; break; }
        if (qp->rq_count >= qp->max_recv_wr) { *bad = wr; rc = ENOMEM; break; }
        if (qp->error) { cq_push(qp->recv_cq, wr->wr_id, IBV_WC_WR_FLUSH_ERR, 0); continue; }
        struct sv_recv *slot = &qp->rq[(qp->rq_head + qp->rq_count) % qp->max_recv_wr];
        slot->wr_id = wr->wr_id;
        slot->sge = wr->sg_list[0];
        qp->rq_count++;
    }
    pthread_cond_broadcast(&id->rq_cond);
    pthread_mutex_unlock(&id->lock);
    return rc;
}

int ibv_post_send(struct ibv_qp *pubqp, struct ibv_send_wr *wr, struct ibv_send_wr **bad) {
    struct sv_qp *qp = (struct sv_qp *)pubqp;
    struct sv_id *id = qp->owner;
    int rc = 0;
    pthread_mutex_lock(&id->lock);
    for (; wr; wr = wr->next) {
        int rdma = wr->opcode == IBV_WR_RDMA_WRITE || wr->opcode == IBV_WR_RDMA_WRITE_WITH_IMM ||
                   wr->opcode == IBV_WR_RDMA_READ;
        if ((wr->opcode != IBV_WR_SEND && !rdma) || wr->num_sge != 1) { *bad = wr; rc = EINVAL; break; }
        if (qp->sq_count >= qp->max_send_wr) { *bad = wr; rc = ENOMEM; break; }
        if (qp->error) { cq_push(qp->send_cq, wr->wr_id, IBV_WC_WR_FLUSH_ERR, 0); continue; }
        if (!sge_valid_pd(wr->sg_list, qp->pub.pd)) {
            cq_push(qp->send_cq, wr->wr_id, IBV_WC_LOC_PROT_ERR, 0);
            qp_flush(id);
            continue;
        }
        if (wr->opcode == IBV_WR_RDMA_READ && (id->my_init_depth == 0 || id->peer_rd_atomic == 0)) {
            /* no initiator depth / the peer offered no responder resources:
             * real hardware fails the READ and the QP goes to error */
            TRACE("RDMA READ posted with initiator_depth=%u peer responder_resources=%u", id->my_init_depth,
                  id->peer_rd_atomic);
            cq_push_ex(qp->send_cq, wr->wr_id, IBV_WC_REM_INV_REQ_ERR, 0, IBV_WC_RDMA_READ, 0, 0);
            qp_flush(id);
            continue;
        }
        struct sv_send *s = calloc(1, sizeof(*s));
        s->wr_id = wr->wr_id;
        s->seq = ++qp->next_seq;
        s->signaled = (wr->send_flags & IBV_SEND_SIGNALED) != 0;
        s->rnr_left = id->my_rnr_retry;
        s->opcode = wr->opcode;
        s->sge = wr->sg_list[0];
        if (rdma) { s->raddr = wr->wr.rdma.remote_addr; s->rkey = wr->wr.rdma.rkey; s->imm = wr->imm_data; }
        if (wr->opcode != IBV_WR_RDMA_READ) {
            s->len = wr->sg_list[0].length;
            s->payload = malloc(s->len ? s->len : 1);
            memcpy(s->payload, sv_host(wr->sg_list[0].lkey, wr->sg_list[0].addr), s->len);
        }
        s->next = qp->outstanding;
        qp->outstanding = s;
        qp->sq_count++;
        if (send_wr(id, s)) {
            complete_send(id, s->seq, IBV_WC_RETRY_EXC_ERR);  /* link gone: transport retries exhausted */
        }
    }
    pthread_mutex_unlock(&id->lock);
    return rc;
}

/* ------------------------------------------------------------------ */
/* connection management                                               */
/* ------------------------------------------------------------------ */
static void tune(int fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    /* Abortive close (RST, no TIME_WAIT). A real RC QP torn down with
     * rdma_disconnect leaves nothing behind like TCP's 60 s TIME_WAIT --
     * but this emulator carries each QP on a TCP socket, so every
     * connect-per-request RPC used to park one ephemeral port and one
     * TIME_WAIT bucket for a minute. tools/stress_concurrency.py showed the
     * result at a sustained ~1-2k RPC/s: both namespaces pinned at
     * tcp_max_tw_buckets (32768), connect() crawling through a near-full
     * port range, and throughput collapsing to ~38% in a ~60 s cycle.
     * Safe here: by the time either side closes, the request/response it
     * carried has completed at the application level (the reply was
     * delivered, or the peer is already gone); the RST only drops TCP
     * teardown state, which RDMA hardware wouldn't have had. */
    struct linger lg = { .l_onoff = 1, .l_linger = 0 };
    setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
}

int rdma_connect(struct rdma_cm_id *pub, struct rdma_conn_param *param) {
    struct sv_id *id = ID(pub);
    if (!id->qp) { errno = EINVAL; return -1; }
    id->my_rnr_retry = param ? param->rnr_retry_count : 0;
    id->my_rd_atomic = param ? param->responder_resources : 0;
    id->my_init_depth = param ? param->initiator_depth : 0;
    id->fd = socket(AF_INET, SOCK_STREAM, 0);
    if (id->fd < 0) return -1;
    tune(id->fd);
    if (connect(id->fd, (struct sockaddr *)&id->dst, sizeof(struct sockaddr_in))) {
        /* asynchronous in the real API: surfaces as a REJECTED event */
        channel_push(pub->channel, pub, RDMA_CM_EVENT_REJECTED, ECONNREFUSED);
        return 0;
    }
    socklen_t sl = sizeof(id->local);
    getsockname(id->fd, (struct sockaddr *)&id->local, &sl);
    if (send_frame(id, F_CONNREQ, 0, 0, NULL, 0)) {
        channel_push(pub->channel, pub, RDMA_CM_EVENT_REJECTED, EPIPE);
        return 0;
    }
    /* thread_started only on success: stop_reader() joins id->thread when it is set */
    if (pthread_create(&id->thread, NULL, reader_thread, id)) {
        channel_push(pub->channel, pub, RDMA_CM_EVENT_REJECTED, EAGAIN);
        return 0;
    }
    id->thread_started = 1;
    return 0;
}

int rdma_bind_addr(struct rdma_cm_id *pub, struct sockaddr *addr) {
    struct sv_id *id = ID(pub);
    id->fd = socket(AF_INET, SOCK_STREAM, 0);
    if (id->fd < 0) return -1;
    int one = 1;
    setsockopt(id->fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (bind(id->fd, addr, sizeof(struct sockaddr_in))) return -1;
    socklen_t sl = sizeof(id->local);
    getsockname(id->fd, (struct sockaddr *)&id->local, &sl);
    pub->verbs = &g_ctx;
    return 0;
}

static void *acceptor_thread(void *arg) {
    pthread_setname_np(pthread_self(), "sv-accept");
    struct sv_id *lid = arg;
    for (;;) {
        int fd = accept(lid->fd, NULL, NULL);
        if (fd < 0) break;  /* listening socket shut down by rdma_destroy_id */
        tune(fd);
        struct sv_frame f;
        if (read_all(fd, &f, sizeof(f)) || f.type != F_CONNREQ) { close(fd); continue; }
        struct rdma_cm_id *npub = NULL;
        if (rdma_create_id(lid->pub.channel, &npub, NULL, RDMA_PS_TCP) || !npub) { close(fd); continue; }
        struct sv_id *nid = ID(npub);
        nid->fd = fd;
        nid->peer_rnr_retry = f.rnr_retry;
        nid->peer_rd_atomic = f.rd_atomic;
        npub->verbs = &g_ctx;
        socklen_t sl = sizeof(nid->local);
        getsockname(fd, (struct sockaddr *)&nid->local, &sl);
        channel_push(lid->pub.channel, npub, RDMA_CM_EVENT_CONNECT_REQUEST, 0);
    }
    return NULL;
}

int rdma_listen(struct rdma_cm_id *pub, int backlog) {
    struct sv_id *id = ID(pub);
    if (listen(id->fd, backlog)) return -1;
    id->listening = 1;
    if (pthread_create(&id->thread, NULL, acceptor_thread, id)) { errno = EAGAIN; return -1; }
    id->thread_started = 1;
    return 0;
}

int rdma_accept(struct rdma_cm_id *pub, struct rdma_conn_param *param) {
    struct sv_id *id = ID(pub);
    if (!id->qp) { errno = EINVAL; return -1; }
    id->my_rnr_retry = param ? param->rnr_retry_count : 0;
    id->my_rd_atomic = param ? param->responder_resources : 0;
    id->my_init_depth = param ? param->initiator_depth : 0;
    pthread_mutex_lock(&id->lock);
    int rc = send_frame(id, F_CONNREP, 0, 0, NULL, 0);
    pthread_mutex_unlock(&id->lock);
    if (rc) { errno = ECONNRESET; return -1; }
    if (pthread_create(&id->thread, NULL, reader_thread, id)) { errno = EAGAIN; return -1; }
    id->thread_started = 1;
    channel_push(pub->channel, pub, RDMA_CM_EVENT_ESTABLISHED, 0);
    return 0;
}

int rdma_migrate_id(struct rdma_cm_id *pub, struct rdma_event_channel *channel) {
    pub->channel = channel;
    return 0;
}

struct sockaddr *rdma_get_local_addr(struct rdma_cm_id *pub) {
    return (struct sockaddr *)&ID(pub)->local;
}

int rdma_disconnect(struct rdma_cm_id *pub) {
    /* After this returns no further completion or buffer write can happen
     * for this id -- callers (roce_transport.c) free registered buffers
     * right after disconnecting. */
    stop_reader(ID(pub));
    return 0;
}

int rdma_destroy_id(struct rdma_cm_id *pub) {
    struct sv_id *id = ID(pub);
    if (id->listening) {
        shutdown(id->fd, SHUT_RDWR);
        if (id->thread_started) { pthread_join(id->thread, NULL); id->thread_started = 0; }
        /* unblock anyone waiting in rdma_get_cm_event on this listener */
        channel_push(pub->channel, NULL, RDMA_CM_EVENT_DEVICE_REMOVAL, 0);
    } else {
        stop_reader(id);
    }
    if (id->qp) rdma_destroy_qp(pub);
    if (id->fd >= 0) close(id->fd);
    pthread_mutex_destroy(&id->lock);
    pthread_cond_destroy(&id->rq_cond);
    free(id);
    return 0;
}
