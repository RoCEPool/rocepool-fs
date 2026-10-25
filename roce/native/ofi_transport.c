#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
/* ofi_transport.c -- the transport API of roce_transport.h on libfabric
 * (OFI) reliable-datagram endpoints, for fabrics without RoCE: AWS EFA
 * (provider "efa": SRD, hardware RDMA read on Nitro v4+) above all, and
 * any other libfabric provider with FI_EP_RDM + FI_MSG + FI_RMA (the
 * "tcp" provider is how this file is tested without an EFA).
 *
 * The servers, clients, interception library and tools are unchanged:
 * this file replaces roce_transport.c at build time (build.sh picks it
 * when the host has an EFA device, or ROCE_BACKEND=ofi).
 *
 * Shape (why it differs from the RC transport):
 *  - RDM endpoints are connectionless and EFA has few queue pairs per
 *    device, so endpoints are pooled, not per connection: a server owns K
 *    endpoints (ROCE_OFI_SERVER_EPS, default one per 4 cores, 2..16), each
 *    served by one pinned worker thread -- thread-per-core; a client
 *    process owns E endpoints per domain (ROCE_OFI_CLIENT_EPS, default 2)
 *    that its threads check out per request. A "connection" is a TCP
 *    socket to the server's port (the bootstrap: endpoint names are
 *    exchanged on it, and its EOF is the liveness signal rdma_cm events
 *    gave the RC transport) plus the addresses learnt on it.
 *  - Requests: a small SEND carries a header (reply-to address, a tag,
 *    up to 4 source descriptors) and the inline bytes; the server RDMA
 *    READs the sources (the client's registered memory) into a pool
 *    buffer -- the same envelope protocol as the RC transport.
 *  - Replies: the client posts a receive into a registered pool buffer
 *    before sending; the server SENDs the reply from the registered buffer
 *    its handler built it in (roce_reply_alloc) -- the provider moves
 *    large messages zero-copy (EFA: device RDMA read of the sender's
 *    buffer). Each reply echoes the request's tag, so a reply that
 *    arrives after its request timed out is recognised and dropped.
 *  - RDM does not order operations: roce_client_rdma_read issues its
 *    ranges concurrently, and roce_transport_reads_ordered() returns 0 so
 *    callers validate one-sided reads accordingly (rocepool_client.c).
 *  - Addresses in descriptors are virtual addresses when the provider
 *    has FI_MR_VIRT_ADDR (EFA), offsets into the region otherwise (tcp);
 *    roce_server_mem_addr() gives servers the right one. Keys must fit
 *    32 bits (EFA's do).
 *
 * Environment: ROCE_OFI_PROVIDER (default "efa" when an EFA device exists,
 * else "tcp"), ROCE_OFI_SERVER_EPS, ROCE_OFI_CLIENT_EPS, ROCE_POLL_SPIN_US,
 * ROCE_BUFPOOL_MAX, ROCE_PIN_THREADS, ROCE_SERVER_DRAIN_MS.
 */
#include "roce_transport.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <sys/prctl.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>

#define OFI_API FI_VERSION(1, 18)
#define RT_MAX_SRC 4
#define SMALL_FRAME (16 * 1024)      /* request header + inline bytes, and small replies */
#define HEADROOM 64                  /* before a pooled reply: room for the reply header */
#define RX_DEPTH_DEFAULT 64          /* posted request buffers per server endpoint */
#define MAX_DOMS 8
#define MAX_CEPS 64

#define REQ_MAGIC 0x3151464fu  /* "OFQ1" */
#define REP_MAGIC 0x3152464fu  /* "OFR1" */
#define HS_MAGIC 0x3148464fu   /* "OFH1": bootstrap */

struct ofi_src { uint64_t addr; uint32_t key; uint32_t len; };
struct req_hdr {
    uint32_t magic, inline_len, total_len, nsrc;
    uint64_t reply_to, tag;
    struct ofi_src src[RT_MAX_SRC];
};
struct rep_hdr { uint32_t magic, status; uint64_t tag; };
#define REQ_HDR ((uint32_t)sizeof(struct req_hdr))
#define REP_HDR ((uint32_t)sizeof(struct rep_hdr))

/* ------------------------------------------------------------- errors */
static __thread char g_last_error[512];
const char *roce_last_error(void) { return g_last_error; }
static void set_error(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(g_last_error, sizeof(g_last_error), fmt, ap);
    va_end(ap);
}
static void set_fi_error(const char *what, ssize_t rc) {
    set_error("%s: %s (%zd)", what, fi_strerror((int)(rc < 0 ? -rc : rc)), rc);
}
static uint32_t env_u32(const char *name, uint32_t dflt) {
    const char *e = getenv(name);
    long v = (e && *e) ? strtol(e, NULL, 10) : 0;
    return v > 0 ? (uint32_t)v : dflt;
}
static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
static long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000L + ts.tv_nsec / 1000000L;
}
static long now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000L + ts.tv_nsec / 1000L;
}
int roce_transport_reads_ordered(void) { return 0; }

/* ------------------------------------------------------------ domains */
struct bufpool;
struct ofi_ep;

struct ofi_dom {
    char prov[32], name[128];
    struct fi_info *info;
    struct fid_fabric *fabric;
    struct fid_domain *domain;
    struct fid_av *av;            /* clients' AV (server endpoints) */
    pthread_mutex_t av_lk;
    /* client endpoint pool */
    pthread_mutex_t lk;
    pthread_cond_t cv;
    int virt;                     /* FI_MR_VIRT_ADDR: remote addresses are virtual, else offsets */
    struct ofi_ep *ceps[MAX_CEPS];
    int nceps;
    int ceps_cap;                 /* endpoint creation failed at this count (device out of QPs); 0 = none yet */
    struct ofi_ep *cfree;
    atomic_ullong gen;
};
static struct ofi_dom *g_doms[MAX_DOMS];
static int g_ndoms;
static pthread_mutex_t g_doms_lk = PTHREAD_MUTEX_INITIALIZER;

static const char *ofi_provider(void) {
    const char *p = getenv("ROCE_OFI_PROVIDER");
    if (p && *p) return p;
    DIR *d = opendir("/sys/class/infiniband");
    int efa = 0;
    if (d) {
        struct dirent *e;
        /* named efa* on older kernels, rdmap<bus>s<slot> on newer ones: the
         * PCI driver tells */
        while ((e = readdir(d))) {
            if (!strncmp(e->d_name, "efa", 3)) efa = 1;
            if (e->d_name[0] == '.') continue;
            char path[512], tgt[512];
            snprintf(path, sizeof(path), "/sys/class/infiniband/%s/device/driver", e->d_name);
            ssize_t n = readlink(path, tgt, sizeof(tgt) - 1);
            if (n > 0) {
                tgt[n] = 0;
                const char *b = strrchr(tgt, '/');
                if (!strcmp(b ? b + 1 : tgt, "efa")) efa = 1;
            }
        }
        closedir(d);
    }
    return efa ? "efa" : "tcp";
}
static int ip_provider(const char *prov) { return strcmp(prov, "efa") != 0; }

/* the local IPv4 address the kernel would use to reach host (for IP-based providers) */
static int local_ip_for(const char *host, char *out, size_t n) {
    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_DGRAM}, *ai = NULL;
    if (getaddrinfo(host, "9", &hints, &ai) || !ai) return -1;
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    int rc = -1;
    if (s >= 0 && connect(s, ai->ai_addr, ai->ai_addrlen) == 0) {
        struct sockaddr_in sa;
        socklen_t sl = sizeof(sa);
        if (getsockname(s, (struct sockaddr *)&sa, &sl) == 0 && inet_ntop(AF_INET, &sa.sin_addr, out, (socklen_t)n)) rc = 0;
    }
    if (s >= 0) close(s);
    freeaddrinfo(ai);
    return rc;
}

/* the domain for local address `src_ip` (NULL: the provider's first) */
static struct ofi_dom *dom_get(const char *src_ip) {
    const char *prov = ofi_provider();
    /* EFA hands same-host peers to libfabric's shm provider, whose large
     * messages and RMA reads stalled with our MR mode on c5n (2026-10-23:
     * "unidentified operation type", requests timing out at 1 MiB). The
     * EFA device serves same-host peers itself: use it unless asked not to.
     * Read by libfabric when it first initialises the providers. */
    if (!strcmp(prov, "efa")) setenv("FI_EFA_ENABLE_SHM_TRANSFER", "0", 0);
    struct fi_info *hints = fi_allocinfo();
    if (!hints) { set_error("fi_allocinfo failed"); return NULL; }
    hints->ep_attr->type = FI_EP_RDM;
    hints->caps = FI_MSG | FI_RMA | FI_READ | FI_REMOTE_READ | FI_SEND | FI_RECV;
    /* device (GPU) memory in RDMA: requested only where asked for
     * (tools/roce_preflight.py sets it on EFA hosts with GPUs), since a
     * provider without it would otherwise not be offered at all */
    const char *hm = getenv("ROCE_OFI_HMEM");
    if (hm && !strcmp(hm, "1")) hints->caps |= FI_HMEM;
    hints->mode = FI_CONTEXT | FI_CONTEXT2;
    hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY;
    hints->domain_attr->threading = FI_THREAD_SAFE;
    hints->fabric_attr->prov_name = strdup(prov);
    struct fi_info *list = NULL;
    const char *node = (src_ip && ip_provider(prov)) ? src_ip : NULL;
    int rc = fi_getinfo(OFI_API, node, NULL, node ? FI_SOURCE : 0, hints, &list);
    fi_freeinfo(hints);
    if (rc || !list) {
        set_error("fi_getinfo(provider %s%s%s, RDM, MSG+RMA): %s -- is libfabric's %s provider usable here?", prov,
                  node ? ", source " : "", node ? node : "", fi_strerror(-rc), prov);
        return NULL;
    }
    struct fi_info *info = list;
    pthread_mutex_lock(&g_doms_lk);
    for (int i = 0; i < g_ndoms; i++)
        if (!strcmp(g_doms[i]->prov, prov) && !strcmp(g_doms[i]->name, info->domain_attr->name)) {
            struct ofi_dom *d = g_doms[i];
            pthread_mutex_unlock(&g_doms_lk);
            fi_freeinfo(list);
            return d;
        }
    struct ofi_dom *d = NULL;
    if (g_ndoms == MAX_DOMS) { set_error("too many fabric domains"); goto out; }
    d = calloc(1, sizeof(*d));
    if (!d) { set_error("out of memory"); goto out; }
    snprintf(d->prov, sizeof(d->prov), "%s", prov);
    snprintf(d->name, sizeof(d->name), "%s", info->domain_attr->name);
    d->virt = (info->domain_attr->mr_mode & FI_MR_VIRT_ADDR) != 0;
    d->info = fi_dupinfo(info);
    pthread_mutex_init(&d->lk, NULL);
    pthread_mutex_init(&d->av_lk, NULL);
    pthread_cond_init(&d->cv, NULL);
    if ((rc = fi_fabric(d->info->fabric_attr, &d->fabric, NULL))) { set_fi_error("fi_fabric", rc); goto fail; }
    if ((rc = fi_domain(d->fabric, d->info, &d->domain, NULL))) { set_fi_error("fi_domain", rc); goto fail; }
    struct fi_av_attr av_attr = {.type = FI_AV_TABLE};
    if ((rc = fi_av_open(d->domain, &av_attr, &d->av, NULL))) { set_fi_error("fi_av_open", rc); goto fail; }
    g_doms[g_ndoms++] = d;
    goto out;
fail:
    if (d->av) fi_close(&d->av->fid);
    if (d->domain) fi_close(&d->domain->fid);
    if (d->fabric) fi_close(&d->fabric->fid);
    fi_freeinfo(d->info);
    free(d);
    d = NULL;
out:
    pthread_mutex_unlock(&g_doms_lk);
    fi_freeinfo(list);
    return d;
}

/* --------------------------------------------- registered memory (MRs) */
static atomic_ullong g_req_key = 1;

static int mr_reg(struct ofi_dom *d, void *addr, size_t len, struct fid_mr **mr, void **desc, uint64_t *key) {
    uint64_t access = FI_SEND | FI_RECV | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    int rc = fi_mr_reg(d->domain, addr, len, access, 0, atomic_fetch_add(&g_req_key, 1), 0, mr, NULL);
    if (rc) { set_fi_error("fi_mr_reg", rc); return -1; }
    *desc = fi_mr_desc(*mr);
    *key = fi_mr_key(*mr);
    if (*key == FI_KEY_NOTAVAIL || *key > UINT32_MAX) {
        set_error("memory key %llu does not fit 32 bits", (unsigned long long)*key);
        fi_close(&(*mr)->fid);
        return -1;
    }
    return 0;
}

#define ROCE_MEM_DOMS MAX_DOMS
struct roce_mem {
    void *addr;
    size_t len;
    pthread_mutex_t lock;
    int n;
    int dev, dmabuf_fd, cuda_device;   /* device memory (roce_mem_register_device) */
    uint64_t dmabuf_off;
    struct { struct ofi_dom *d; struct fid_mr *mr; void *desc; uint64_t key; } e[ROCE_MEM_DOMS];
};

/* device memory: FI_HMEM_CUDA, as a dma-buf when there is one */
static int mr_reg_dev(struct ofi_dom *d, const struct roce_mem *m, struct fid_mr **mr, void **desc, uint64_t *key) {
    struct iovec iov = {m->addr, m->len};
    struct fi_mr_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mr_iov = &iov;
    attr.iov_count = 1;
    attr.access = FI_SEND | FI_RECV | FI_READ | FI_WRITE | FI_REMOTE_READ | FI_REMOTE_WRITE;
    attr.requested_key = atomic_fetch_add(&g_req_key, 1);
    attr.iface = FI_HMEM_CUDA;
    attr.device.cuda = m->cuda_device;
    uint64_t flags = 0;
#ifdef FI_MR_DMABUF
    struct fi_mr_dmabuf dmabuf = {.fd = m->dmabuf_fd, .offset = m->dmabuf_off, .len = m->len, .base_addr = m->addr};
    if (m->dmabuf_fd >= 0) { attr.dmabuf = &dmabuf; flags |= FI_MR_DMABUF; }
#endif
    int rc = fi_mr_regattr(d->domain, &attr, flags, mr);
    if (rc) { set_fi_error("fi_mr_regattr (device memory; ROCE_OFI_HMEM=1 needed)", rc); return -1; }
    *desc = fi_mr_desc(*mr);
    *key = fi_mr_key(*mr);
    if (*key == FI_KEY_NOTAVAIL || *key > UINT32_MAX) {
        set_error("memory key %llu does not fit 32 bits", (unsigned long long)*key);
        fi_close(&(*mr)->fid);
        return -1;
    }
    return 0;
}

roce_mem_t *roce_mem_register(void *addr, size_t len) {
    struct roce_mem *m = calloc(1, sizeof(*m));
    if (!m) { set_error("out of memory"); return NULL; }
    m->addr = addr;
    m->len = len;
    m->dmabuf_fd = -1;
    pthread_mutex_init(&m->lock, NULL);
    return m;
}
roce_mem_t *roce_mem_register_device(void *addr, size_t len, int dmabuf_fd, uint64_t dmabuf_offset, int cuda_device) {
    /* the endpoints were opened with FI_HMEM only under ROCE_OFI_HMEM=1 (EFA
     * with GPUs); without it no provider registers device memory */
    const char *hm = getenv("ROCE_OFI_HMEM");
    if (!hm || strcmp(hm, "1")) { set_error("device memory needs ROCE_OFI_HMEM=1 (a provider with FI_HMEM)"); return NULL; }
    struct roce_mem *m = roce_mem_register(addr, len);
    if (!m) return NULL;
    m->dev = 1;
    m->dmabuf_fd = dmabuf_fd;
    m->dmabuf_off = dmabuf_offset;
    m->cuda_device = cuda_device;
    return m;
}
int roce_mem_is_device(const roce_mem_t *m) { return m && m->dev; }
void roce_mem_release(roce_mem_t *m) {
    if (!m) return;
    for (int i = 0; i < m->n; i++) fi_close(&m->e[i].mr->fid);
    pthread_mutex_destroy(&m->lock);
    free(m);
}
static int mem_keys(struct roce_mem *m, struct ofi_dom *d, void **desc, uint32_t *key) {
    int rc = -1;
    pthread_mutex_lock(&m->lock);
    for (int i = 0; i < m->n; i++)
        if (m->e[i].d == d) { *desc = m->e[i].desc; *key = (uint32_t)m->e[i].key; rc = 0; break; }
    if (rc && m->n < ROCE_MEM_DOMS) {
        uint64_t k;
        if (!(m->dev ? mr_reg_dev(d, m, &m->e[m->n].mr, &m->e[m->n].desc, &k)
                     : mr_reg(d, m->addr, m->len, &m->e[m->n].mr, &m->e[m->n].desc, &k))) {
            m->e[m->n].d = d;
            m->e[m->n].key = k;
            *desc = m->e[m->n].desc;
            *key = (uint32_t)k;
            m->n++;
            rc = 0;
        }
    }
    pthread_mutex_unlock(&m->lock);
    return rc;
}

/* ------------------------------------------------- registered buffer pools
 * Per (domain, frame, role): buffers of `frame` bytes registered in chunks,
 * borrowed for one request. Server pools capped (2 per worker + 8). */
static roce_chunk_alloc_fn g_chunk_alloc;
static roce_chunk_free_fn g_chunk_free;
static roce_chunk_notify_fn g_chunk_notify;
static void *g_chunk_user;
void roce_set_chunk_hooks(roce_chunk_alloc_fn alloc, roce_chunk_free_fn free_fn, roce_chunk_notify_fn notify, void *user) {
    g_chunk_alloc = alloc;
    g_chunk_free = free_fn;
    g_chunk_notify = notify;
    g_chunk_user = user;
}

struct rbuf { uint8_t *addr; uint8_t *mr_base; void *desc; uint32_t key; uint32_t idx; struct rbuf *next; };
struct pchunk { uint8_t *base; size_t len; struct fid_mr *mr; void *desc; uint32_t first; };
#define POOL_CLIENT 0
#define POOL_SERVER 1
struct bufpool {
    struct bufpool *next;
    struct ofi_dom *d;
    size_t frame;
    int role;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    struct rbuf **all;
    uint32_t n, max, nfree;
    struct rbuf *free_list;
    uint64_t registered_bytes;
    struct pchunk *chunks;
    uint32_t nchunks;
};
static struct bufpool *g_pools;
static pthread_mutex_t g_pools_lk = PTHREAD_MUTEX_INITIALIZER;

static struct bufpool *pool_for(struct ofi_dom *d, size_t frame, int role, uint32_t max_dflt) {
    pthread_mutex_lock(&g_pools_lk);
    struct bufpool *p;
    for (p = g_pools; p; p = p->next)
        if (p->d == d && p->frame == frame && p->role == role) break;
    if (!p && (p = calloc(1, sizeof(*p)))) {
        p->d = d;
        p->frame = frame;
        p->role = role;
        p->max = env_u32("ROCE_BUFPOOL_MAX", max_dflt);
        p->all = calloc(p->max, sizeof(*p->all));
        if (!p->all) { free(p); p = NULL; }
        else {
            pthread_mutex_init(&p->lock, NULL);
            pthread_cond_init(&p->cond, NULL);
            p->next = g_pools;
            g_pools = p;
        }
    }
    pthread_mutex_unlock(&g_pools_lk);
    if (!p) set_error("out of memory");
    return p;
}

static int pool_grow(struct bufpool *p) {  /* caller holds p->lock */
    uint32_t k = p->max - p->n;
    if (k > 4) k = 4;
    if (!k) return -1;
    size_t len = p->frame * k;
    void *base = NULL;
    if (p->role == POOL_SERVER && g_chunk_alloc) base = g_chunk_alloc(len, g_chunk_user);
    struct pchunk c = {0};
    uint64_t key;
    if (base && mr_reg(p->d, base, len, &c.mr, &c.desc, &key)) {
        fprintf(stderr, "ofi_transport: the provider refused the supplied (P2P) chunk memory: using host memory\n");
        g_chunk_free(base, len, g_chunk_user);
        base = NULL;
    }
    if (!base) {
        base = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (base == MAP_FAILED) { set_error("mmap %zu: %s", len, strerror(errno)); return -1; }
        if (mr_reg(p->d, base, len, &c.mr, &c.desc, &key)) { munmap(base, len); return -1; }
    }
    if (p->role == POOL_SERVER && g_chunk_notify) g_chunk_notify(base, len, g_chunk_user);
    c.base = base;
    c.len = len;
    c.first = p->n;
    struct pchunk *nc = realloc(p->chunks, (p->nchunks + 1) * sizeof(*nc));
    if (!nc) { fi_close(&c.mr->fid); munmap(base, len); return -1; }
    p->chunks = nc;
    p->chunks[p->nchunks++] = c;
    for (uint32_t i = 0; i < k; i++) {
        struct rbuf *b = calloc(1, sizeof(*b));
        if (!b) break;
        b->addr = (uint8_t *)base + (size_t)i * p->frame;
        b->mr_base = base;
        b->desc = c.desc;
        b->key = (uint32_t)key;
        b->idx = p->n;
        p->all[p->n++] = b;
        b->next = p->free_list;
        p->free_list = b;
        p->nfree++;
    }
    p->registered_bytes += len;
    return 0;
}

static struct rbuf *pool_get(struct bufpool *p, int timeout_ms) {
    pthread_mutex_lock(&p->lock);
    long deadline = now_ms() + timeout_ms;
    while (!p->free_list) {
        if (p->n < p->max) { if (pool_grow(p) == 0) continue; }
        if (p->n == 0) { pthread_mutex_unlock(&p->lock); return NULL; }
        long left = deadline - now_ms();
        if (left <= 0) { pthread_mutex_unlock(&p->lock); set_error("buffer pool exhausted"); return NULL; }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        long ms = left > 50 ? 50 : left;
        ts.tv_nsec += ms * 1000000L;
        ts.tv_sec += ts.tv_nsec / 1000000000L;
        ts.tv_nsec %= 1000000000L;
        pthread_cond_timedwait(&p->cond, &p->lock, &ts);
    }
    struct rbuf *b = p->free_list;
    p->free_list = b->next;
    p->nfree--;
    pthread_mutex_unlock(&p->lock);
    return b;
}
/* n buffers at once: callers that need two must never hold one while
 * waiting for the other (at the cap, concurrent callers would each hold one
 * and wait forever) */
static int pool_get_n(struct bufpool *p, int n, struct rbuf **out, int timeout_ms) {
    if (n == 1) return (out[0] = pool_get(p, timeout_ms)) ? 0 : -1;
    pthread_mutex_lock(&p->lock);
    long deadline = now_ms() + timeout_ms;
    while (p->nfree < (uint32_t)n) {
        if (p->n < p->max) { if (pool_grow(p) == 0) continue; }
        if (p->max < (uint32_t)n) { pthread_mutex_unlock(&p->lock); set_error("buffer pool cap below %d", n); return -1; }
        long left = deadline - now_ms();
        if (left <= 0) { pthread_mutex_unlock(&p->lock); set_error("buffer pool exhausted"); return -1; }
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        long ms = left > 50 ? 50 : left;
        ts.tv_nsec += ms * 1000000L;
        ts.tv_sec += ts.tv_nsec / 1000000000L;
        ts.tv_nsec %= 1000000000L;
        pthread_cond_timedwait(&p->cond, &p->lock, &ts);
    }
    for (int i = 0; i < n; i++) {
        out[i] = p->free_list;
        p->free_list = out[i]->next;
        p->nfree--;
    }
    pthread_mutex_unlock(&p->lock);
    return 0;
}
static void pool_put(struct bufpool *p, struct rbuf *b) {
    if (!b) return;
    pthread_mutex_lock(&p->lock);
    b->next = p->free_list;
    p->free_list = b;
    p->nfree++;
    pthread_cond_broadcast(&p->cond);
    pthread_mutex_unlock(&p->lock);
}
/* the buffer of p that contains addr (interior pointers too) */
static struct rbuf *pool_find(struct bufpool *p, const void *addr) {
    struct rbuf *hit = NULL;
    const uint8_t *a = addr;
    pthread_mutex_lock(&p->lock);
    for (uint32_t c = 0; c < p->nchunks; c++) {
        struct pchunk *ch = &p->chunks[c];
        if (a >= ch->base && a < ch->base + ch->len) {
            uint32_t k = ch->first + (uint32_t)((size_t)(a - ch->base) / p->frame);
            if (k < p->n) hit = p->all[k];
            break;
        }
    }
    pthread_mutex_unlock(&p->lock);
    return hit;
}

void roce_bufpool_stats(uint64_t *registered_bytes, uint32_t *buffers, uint32_t *in_use) {
    uint64_t rb = 0;
    uint32_t n = 0, used = 0;
    pthread_mutex_lock(&g_pools_lk);
    for (struct bufpool *p = g_pools; p; p = p->next) {
        pthread_mutex_lock(&p->lock);
        rb += p->registered_bytes;
        n += p->n;
        used += p->n - p->nfree;
        pthread_mutex_unlock(&p->lock);
    }
    pthread_mutex_unlock(&g_pools_lk);
    if (registered_bytes) *registered_bytes = rb;
    if (buffers) *buffers = n;
    if (in_use) *in_use = used;
}

static __thread struct bufpool *tl_reply_pool;
void *roce_reply_alloc(size_t len) {
    struct bufpool *p = tl_reply_pool;
    if (p && len > SMALL_FRAME - REP_HDR && len + HEADROOM <= p->frame) {
        struct rbuf *b = pool_get(p, 30000);
        if (b) return b->addr + HEADROOM;
    }
    return malloc(len ? len : 1);
}
static struct rbuf *pool_find_any(const void *ptr, struct bufpool **pp) {
    pthread_mutex_lock(&g_pools_lk);
    struct bufpool *p = g_pools;
    pthread_mutex_unlock(&g_pools_lk);
    for (; p; p = p->next) {
        struct rbuf *b = pool_find(p, ptr);
        if (b) { *pp = p; return b; }
    }
    return NULL;
}
void roce_reply_free(void *ptr) {
    if (!ptr) return;
    struct bufpool *p;
    struct rbuf *b = pool_find_any(ptr, &p);
    if (b) pool_put(p, b);
    else free(ptr);
}

/* the address a peer uses for va in a region registered at base */
static uint64_t raddr_of(const struct ofi_dom *d, const void *base, const void *va) {
    return d->virt ? (uint64_t)(uintptr_t)va : (uint64_t)((const uint8_t *)va - (const uint8_t *)base);
}

/* ------------------------------------------------------------ endpoints */
struct op {
    struct fi_context2 ctx;    /* first: the context pointer is the op */
    int kind;                  /* 1 tx, 2 rx (client reply), 3 rma, 4 server request slot */
    volatile int done;
    int err;
    size_t len;
    uint64_t seq;              /* server request slots: arrival order */
    uint64_t t_ns;             /* server request slots: when it arrived */
};
#define OP_TX 1
#define OP_RX 2
#define OP_RMA 3
#define OP_SLOT 4

struct ofi_ep {
    struct ofi_dom *d;
    struct fid_ep *ep;
    struct fid_cq *cq;
    struct fid *wait_fid;      /* the CQ if it has an fd */
    int waitfd;
    int sread;                 /* the CQ supports fi_cq_sread */
    uint8_t name[256];
    size_t namelen;
    uint64_t gen;              /* client pool endpoints: identity, bumped on replace */
    int index;                 /* client pool slot, -1 private */
    struct ofi_ep *next;
    uint64_t next_tag;
    /* small registered buffer: client requests / server replies */
    uint8_t *small;
    struct fid_mr *small_mr;
    void *small_desc;
    /* server: posted request buffers */
    uint8_t *slab;
    struct fid_mr *slab_mr;
    void *slab_desc;
    struct op *slots;
    int nslots;
    uint64_t arrivals;
};

static void ep_destroy(struct ofi_ep *e) {
    if (!e) return;
    if (e->ep) { int r = fi_close(&e->ep->fid); if (r && getenv("ROCE_OFI_DEBUG")) fprintf(stderr, "fi_close(ep) %d\n", r); }
    if (e->cq) { int r = fi_close(&e->cq->fid); if (r && getenv("ROCE_OFI_DEBUG")) fprintf(stderr, "fi_close(cq) %d\n", r); }
    if (e->small_mr) fi_close(&e->small_mr->fid);
    if (e->slab_mr) fi_close(&e->slab_mr->fid);
    free(e->small);
    if (e->slab) munmap(e->slab, (size_t)e->nslots * SMALL_FRAME);
    free(e->slots);
    free(e);
}

static struct ofi_ep *ep_create(struct ofi_dom *d, struct fid_av *av) {
    struct ofi_ep *e = calloc(1, sizeof(*e));
    if (!e) { set_error("out of memory"); return NULL; }
    e->d = d;
    e->index = -1;
    e->waitfd = -1;
    int rc;
    /* a CQ we can sleep on: an fd (polled with the bootstrap socket), else
     * fi_cq_sread, else plain polling */
    static const enum fi_wait_obj objs[] = {FI_WAIT_FD, FI_WAIT_UNSPEC, FI_WAIT_NONE};
    /* ROCE_OFI_CQ_WAIT=none: no wait object, as on EFA ("wait objects are
     * not currently supported", fi_efa(7)) -- reproduces EFA's waiting on
     * the tcp provider */
    const char *cw = getenv("ROCE_OFI_CQ_WAIT");
    for (int i = cw && !strcmp(cw, "none") ? 2 : 0; i < 3; i++) {
        struct fi_cq_attr ca = {.size = 1024, .format = FI_CQ_FORMAT_DATA, .wait_obj = objs[i]};
        if (fi_cq_open(d->domain, &ca, &e->cq, e) == 0) {
            if (objs[i] == FI_WAIT_FD && fi_control(&e->cq->fid, FI_GETWAIT, &e->waitfd) == 0) e->wait_fid = &e->cq->fid;
            else e->waitfd = -1;
            e->sread = objs[i] != FI_WAIT_NONE;
            break;
        }
    }
    if (!e->cq) { set_error("fi_cq_open failed"); goto fail; }
    if ((rc = fi_endpoint(d->domain, d->info, &e->ep, NULL))) { set_fi_error("fi_endpoint", rc); goto fail; }
    if ((rc = fi_ep_bind(e->ep, &e->cq->fid, FI_TRANSMIT | FI_RECV))) { set_fi_error("fi_ep_bind(cq)", rc); goto fail; }
    if ((rc = fi_ep_bind(e->ep, &av->fid, 0))) { set_fi_error("fi_ep_bind(av)", rc); goto fail; }
    if ((rc = fi_enable(e->ep))) { set_fi_error("fi_enable", rc); goto fail; }
    e->namelen = sizeof(e->name);
    if ((rc = fi_getname(&e->ep->fid, e->name, &e->namelen))) { set_fi_error("fi_getname", rc); goto fail; }
    e->small = aligned_alloc(4096, SMALL_FRAME);
    uint64_t key;
    if (!e->small || mr_reg(d, e->small, SMALL_FRAME, &e->small_mr, &e->small_desc, &key)) goto fail;
    return e;
fail:
    ep_destroy(e);
    return NULL;
}

/* how long this thread's recent waits lasted past the spin (EWMA, us):
 * decides whether it yields or sleeps (ep_wait) */
static __thread long tl_wait_ewma_us;
/* a server endpoint's worker: the waits that yield (a client's waits
 * spin, then sleep: its program's threads need the cores) */
static __thread int tl_server_thread;
static void wait_note(long us) { tl_wait_ewma_us = (7 * tl_wait_ewma_us + us) / 8; }

/* after the busy-poll, how long a thread with no CQ wait object (EFA)
 * yields its core between polls before it sleeps (ROCE_POLL_YIELD_US) */
static long yield_us(void) {
    static long v = -1;
    if (v < 0) { const char *s = getenv("ROCE_POLL_YIELD_US"); v = s && *s ? atol(s) : 2000; if (v < 0) v = 0; }
    return v;
}

static int spin_us(void) {
    static int v = -1;
    if (v < 0) { const char *s = getenv("ROCE_POLL_SPIN_US"); v = s && *s ? atoi(s) : 20; if (v < 0) v = 0; }
    return v;
}

static void dispatch(struct ofi_ep *e, const struct fi_cq_data_entry *ent) {
    struct op *o = ent->op_context;
    if (!o) return;
    o->len = ent->len;
    o->err = 0;
    if (o->kind == OP_SLOT) { o->seq = __atomic_add_fetch(&e->arrivals, 1, __ATOMIC_RELAXED); o->t_ns = now_ns(); }
    __atomic_store_n(&o->done, 1, __ATOMIC_RELEASE);  /* several threads may drain one CQ */
}

/* Read and dispatch completions. Returns the number handled, or -1 on a
 * CQ failure. */
static int ep_drain(struct ofi_ep *e) {
    struct fi_cq_data_entry ent[16];
    int handled = 0;
    for (;;) {
        ssize_t n = fi_cq_read(e->cq, ent, 16);
        if (n == -FI_EAGAIN) return handled;
        if (n == -FI_EAVAIL) {
            struct fi_cq_err_entry err = {0};
            if (fi_cq_readerr(e->cq, &err, 0) > 0 && err.op_context) {
                struct op *o = err.op_context;
                o->err = err.err ? err.err : FI_EIO;
                o->len = err.len;
                if (o->kind == OP_SLOT) { o->seq = __atomic_add_fetch(&e->arrivals, 1, __ATOMIC_RELAXED); o->t_ns = now_ns(); }
                __atomic_store_n(&o->done, 1, __ATOMIC_RELEASE);
                /* a receive we cancelled ourselves: keep the error that made us cancel it */
                if (o->err != FI_ECANCELED)
                    set_error("completion error: %s (prov %d%s%s)", fi_strerror(o->err), err.prov_errno,
                              err.err_data_size ? ": " : "",
                              err.err_data_size ? fi_cq_strerror(e->cq, err.prov_errno, err.err_data, NULL, 0) : "");
                handled++;
            }
            continue;
        }
        if (n < 0) { set_fi_error("fi_cq_read", n); return -1; }
        for (ssize_t i = 0; i < n; i++) { dispatch(e, &ent[i]); handled++; }
    }
}

/* Wait up to ms for completions on e (progressing it), watching `sock`
 * for EOF (the peer gone). 1 = something completed, 0 = timeout, -1 =
 * failure, -2 = the peer's socket closed. */
/* why sock_dead() said so, for the error message */
static __thread char tl_dead_why[96];
static int sock_dead(int sock) {
    if (sock < 0) return 0;
    struct pollfd p = {.fd = sock, .events = POLLIN};
    if (poll(&p, 1, 0) <= 0) return 0;
    int soerr = 0;
    socklen_t sl = sizeof(soerr);
    if (p.revents & (POLLHUP | POLLERR | POLLNVAL)) {
        getsockopt(sock, SOL_SOCKET, SO_ERROR, &soerr, &sl);
        snprintf(tl_dead_why, sizeof(tl_dead_why), "socket %s%s%s%s%s", p.revents & POLLHUP ? "hangup" : "",
                 p.revents & POLLERR ? " error" : "", p.revents & POLLNVAL ? " invalid" : "",
                 soerr ? ": " : "", soerr ? strerror(soerr) : "");
        return 1;
    }
    char c;
    ssize_t r = recv(sock, &c, 1, MSG_PEEK | MSG_DONTWAIT);
    if (r == 0) { snprintf(tl_dead_why, sizeof(tl_dead_why), "EOF from the server"); return 1; }
    if (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        snprintf(tl_dead_why, sizeof(tl_dead_why), "recv: %s", strerror(errno));
        return 1;
    }
    return 0;
}
static int ep_wait(struct ofi_ep *e, int ms, int sock) {
    static __thread int slack_set;
    if (!slack_set && !e->wait_fid && !e->sread) {
        /* the sleep below is 50 us; the default 50 us timer slack made it ~100 */
        prctl(PR_SET_TIMERSLACK, 1000UL, 0, 0, 0);
        slack_set = 1;
    }
    int h = ep_drain(e);
    if (h) return h > 0 ? 1 : -1;
    long until = now_us() + spin_us();
    while (now_us() < until) { h = ep_drain(e); if (h) return h > 0 ? 1 : -1; }
    /* Yield only when this thread's waits have been short: a server
     * thread between metadata requests, a client waiting for a lookup.
     * A thread whose waits are long (bulk I/O: milliseconds) sleeps --
     * yielding there kept every waiting thread on a CPU: on 2 x i3en the
     * hosts went from 16% to 83% busy in ior-hard-write and every IOR
     * phase lost 12-23% (here, 2 cores: ior-hard write 192 -> 62 MiB/s). */
    long t_wait = now_us();
    long win = 0;
    if (!e->wait_fid && !e->sread && tl_server_thread && tl_wait_ewma_us < yield_us() / 4) {
        win = 4 * tl_wait_ewma_us + 20;
        if (win > yield_us()) win = yield_us();
    }
    long yield_until = win ? t_wait + win : 0;
    long end = now_ms() + ms;
    for (;;) {
        if (sock_dead(sock)) return -2;
        long left = end - now_ms();
        if (left <= 0) return 0;
        int chunk = left > 20 ? 20 : (int)left;
        if (e->wait_fid) {
            struct fid *f = e->wait_fid;
            if (fi_trywait(e->d->fabric, &f, 1) == FI_SUCCESS) {
                struct pollfd p[2] = {{.fd = e->waitfd, .events = POLLIN}, {.fd = sock, .events = POLLIN}};
                poll(p, sock >= 0 ? 2 : 1, chunk);
            }
        } else if (e->sread) {
            struct fi_cq_data_entry ent;
            ssize_t n = fi_cq_sread(e->cq, &ent, 1, NULL, chunk);
            if (n == 1) { dispatch(e, &ent); return 1; }
        } else if (now_us() < yield_until) {
            /* No wait object (EFA): a nanosleep(50 us) is ~100 us on Linux
             * and nothing progresses meanwhile (manual progress), so every
             * request that reached an idle thread, and every reply a client
             * waited for longer than the spin, paid it (calibration on
             * i3en: p50 127 us without the spin, 35 us with). For a while
             * after the spin, give the core away instead of sleeping:
             * sched_yield returns at once when nobody else wants it. */
            sched_yield();
        } else {
            struct timespec ts = {0, 50000};
            nanosleep(&ts, NULL);
        }
        h = ep_drain(e);
        if (h) {
            if (h > 0) wait_note(now_us() - t_wait);
            return h > 0 ? 1 : -1;
        }
    }
}

/* wait until every op is done (or one failed). 0, -1 (error set), -2 peer gone, -3 timeout */
static int ep_wait_ops(struct ofi_ep *e, struct op **ops, int n, int timeout_ms, int sock) {
    long end = timeout_ms < 0 ? -1 : now_ms() + timeout_ms;
    for (;;) {
        int all = 1;
        for (int i = 0; i < n; i++) {
            if (!ops[i]->done) { all = 0; continue; }
            if (ops[i]->err) return -1;
        }
        if (all) return 0;
        long left = end < 0 ? 1000 : end - now_ms();
        if (end >= 0 && left <= 0) { set_error("timed out waiting for completion"); return -3; }
        int w = ep_wait(e, (int)(left > 1000 ? 1000 : left), sock);
        if (w == -1) return -1;
        if (w == -2) { set_error("peer closed the connection (%s)", tl_dead_why); return -2; }
    }
}

/* A post that keeps returning EAGAIN: a full queue drains by progress, but
 * a peer that is gone keeps it there for good (the tcp provider retries
 * its connection on every attempt). So between attempts: progress, back
 * off after a few, give up when the peer is known dead (tl_alive, set by
 * a server worker for the request at hand) or after ROCE_OFI_POST_TIMEOUT_MS. */
static __thread int (*tl_alive)(void *);
static __thread void *tl_alive_arg;

static int post_retry_wait(struct ofi_ep *e, int tries, long *deadline) {
    if (ep_drain(e) < 0) return -1;
    if (tl_alive && !tl_alive(tl_alive_arg)) { set_error("peer gone"); return -1; }
    if (tries < 64) return 0;
    if (!*deadline) *deadline = now_ms() + (long)env_u32("ROCE_OFI_POST_TIMEOUT_MS", 10000);
    else if (now_ms() > *deadline) { set_error("cannot post: provider queue stays full (peer unreachable?)"); return -1; }
    usleep(tries < 1000 ? 50 : 1000);
    return 0;
}

static int do_send(struct ofi_ep *e, const void *buf, size_t len, void *desc, fi_addr_t to, struct op *o) {
    o->kind = OP_TX; o->done = 0; o->err = 0;
    long deadline = 0;
    for (int tries = 0;; tries++) {
        ssize_t rc = fi_send(e->ep, buf, len, desc, to, &o->ctx);
        if (rc == 0) return 0;
        if (rc != -FI_EAGAIN) { set_fi_error("fi_send", rc); return -1; }
        if (post_retry_wait(e, tries, &deadline)) return -1;
    }
}
static int do_recv(struct ofi_ep *e, void *buf, size_t len, void *desc, struct op *o, int kind) {
    o->kind = kind; o->done = 0; o->err = 0;
    long deadline = 0;
    for (int tries = 0;; tries++) {
        ssize_t rc = fi_recv(e->ep, buf, len, desc, FI_ADDR_UNSPEC, &o->ctx);
        if (rc == 0) return 0;
        if (rc != -FI_EAGAIN) { set_fi_error("fi_recv", rc); return -1; }
        if (post_retry_wait(e, tries, &deadline)) return -1;
    }
}
static int do_read(struct ofi_ep *e, void *buf, size_t len, void *desc, fi_addr_t from, uint64_t raddr, uint64_t key,
                   struct op *o) {
    o->kind = OP_RMA; o->done = 0; o->err = 0;
    long deadline = 0;
    for (int tries = 0;; tries++) {
        ssize_t rc = fi_read(e->ep, buf, len, desc, from, raddr, key, &o->ctx);
        if (rc == 0) return 0;
        if (rc != -FI_EAGAIN) { set_fi_error("fi_read", rc); return -1; }
        if (post_retry_wait(e, tries, &deadline)) return -1;
    }
}
/* drop a posted receive we no longer wait for; 0 when the provider gave it
 * back (the buffer is ours again) */
static int cancel_recv(struct ofi_ep *e, struct op *o) {
    if (o->done) return 0;
    fi_cancel(&e->ep->fid, &o->ctx);
    long end = now_ms() + 200;
    while (!o->done && now_ms() < end) ep_wait(e, 10, -1);
    return o->done ? 0 : -1;
}

/* ----------------------------------------------------- socket helpers */
static int write_all(int s, const void *b, size_t n) {
    const uint8_t *p = b;
    while (n) {
        ssize_t w = send(s, p, n, MSG_NOSIGNAL);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        p += w; n -= (size_t)w;
    }
    return 0;
}
static int read_all(int s, void *b, size_t n, int timeout_ms) {
    uint8_t *p = b;
    long end = now_ms() + timeout_ms;
    while (n) {
        struct pollfd pf = {.fd = s, .events = POLLIN};
        long left = end - now_ms();
        if (left <= 0) { errno = ETIMEDOUT; return -1; }
        int pr = poll(&pf, 1, (int)left);
        if (pr < 0) { if (errno == EINTR) continue; return -1; }
        if (pr == 0) continue;
        ssize_t r = recv(s, p, n, 0);
        if (r == 0) { errno = ECONNRESET; return -1; }
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        p += r; n -= (size_t)r;
    }
    return 0;
}
static int split_hostport(const char *s, char *host, size_t hn, char *port, size_t pn) {
    const char *c = strrchr(s, ':');
    if (!c) { set_error("address %s: expected host:port", s); return -1; }
    snprintf(host, hn, "%.*s", (int)(c - s), s);
    snprintf(port, pn, "%s", c + 1);
    return 0;
}

/* ------------------------------------------------------- client side */
struct roce_conn {
    int sock;
    struct ofi_dom *d;
    size_t max_frame;
    fi_addr_t server;              /* in d->av */
    int dedicated;
    struct ofi_ep *own;            /* dedicated: a private endpoint */
    uint64_t own_reply_to;
    uint64_t reply_to[MAX_CEPS];   /* server-AV address of each pool endpoint */
    uint64_t reply_gen[MAX_CEPS];  /* the endpoint generation it was learnt for */
    /* dedicated: always-posted receive + a registered send buffer */
    uint8_t *rbuf, *sbuf;
    struct fid_mr *rmr, *smr;
    void *rdesc, *sdesc;
    uint64_t skey;
    struct op rop;
    int rposted;
    struct bufpool *pool;
};

static uint32_t client_eps(void) {
    uint32_t n = env_u32("ROCE_OFI_CLIENT_EPS", 2);
    return n > MAX_CEPS ? MAX_CEPS : n;
}

/* a pool endpoint for one request (created on demand up to E) */
static struct ofi_ep *ep_checkout(struct ofi_dom *d) {
    pthread_mutex_lock(&d->lk);
    for (;;) {
        if (d->cfree) {
            struct ofi_ep *e = d->cfree;
            d->cfree = e->next;
            pthread_mutex_unlock(&d->lk);
            return e;
        }
        if (d->nceps < (int)client_eps() && (!d->ceps_cap || d->nceps < d->ceps_cap)) {
            int idx = d->nceps++;
            pthread_mutex_unlock(&d->lk);
            struct ofi_ep *e = ep_create(d, d->av);
            pthread_mutex_lock(&d->lk);
            if (!e) {
                d->nceps--;
                /* the device has no queue pair left (EFA: a few hundred per
                 * device, shared by every process on the host): wait for one
                 * of the endpoints this process already has */
                if (d->nceps > 0) { d->ceps_cap = d->nceps; pthread_cond_wait(&d->cv, &d->lk); continue; }
                pthread_mutex_unlock(&d->lk);
                return NULL;
            }
            e->index = idx;
            e->gen = atomic_fetch_add(&d->gen, 1) + 1;
            d->ceps[idx] = e;
            pthread_mutex_unlock(&d->lk);
            return e;
        }
        pthread_cond_wait(&d->cv, &d->lk);
    }
}
static void ep_checkin(struct ofi_dom *d, struct ofi_ep *e) {
    pthread_mutex_lock(&d->lk);
    e->next = d->cfree;
    d->cfree = e;
    pthread_cond_signal(&d->cv);
    pthread_mutex_unlock(&d->lk);
}
/* an endpoint in an unknown state (a receive we could not cancel): close
 * it -- no DMA after fi_close -- and put a fresh one in its slot */
static struct ofi_ep *ep_replace(struct ofi_dom *d, struct ofi_ep *e) {
    int idx = e->index;
    ep_destroy(e);
    struct ofi_ep *n = ep_create(d, d->av);
    pthread_mutex_lock(&d->lk);
    if (n) { n->index = idx; n->gen = atomic_fetch_add(&d->gen, 1) + 1; d->ceps[idx] = n; }
    else { d->ceps[idx] = d->ceps[--d->nceps]; if (d->ceps[idx]) d->ceps[idx]->index = idx; }
    pthread_mutex_unlock(&d->lk);
    return n;
}

/* bootstrap message: client -> server: magic, n, n x (len, name);
 * server -> client: magic, status, server name (len, bytes), n x u64 addr */
static int handshake(struct roce_conn *c, struct ofi_ep **eps, int n, uint64_t *addrs, int timeout_ms, int first) {
    uint8_t buf[MAX_CEPS * 260 + 16];
    size_t o = 0;
    uint32_t v = HS_MAGIC;
    memcpy(buf + o, &v, 4); o += 4;
    v = (uint32_t)n;
    memcpy(buf + o, &v, 4); o += 4;
    for (int i = 0; i < n; i++) {
        v = (uint32_t)eps[i]->namelen;
        memcpy(buf + o, &v, 4); o += 4;
        memcpy(buf + o, eps[i]->name, eps[i]->namelen); o += eps[i]->namelen;
    }
    if (write_all(c->sock, buf, o)) { set_error("bootstrap send: %s", strerror(errno)); return -1; }
    uint32_t hdr[3];
    if (read_all(c->sock, hdr, sizeof(hdr), timeout_ms)) { set_error("bootstrap reply: %s", strerror(errno)); return -1; }
    if (hdr[0] != HS_MAGIC || hdr[1] != 0 || hdr[2] > 256) { set_error("bootstrap refused (status %u)", hdr[1]); return -1; }
    uint8_t sname[256];
    if (read_all(c->sock, sname, hdr[2], timeout_ms) || read_all(c->sock, addrs, (size_t)n * 8, timeout_ms)) {
        set_error("bootstrap reply: %s", strerror(errno));
        return -1;
    }
    if (first) {
        pthread_mutex_lock(&c->d->av_lk);
        int rc = fi_av_insert(c->d->av, sname, 1, &c->server, 0, NULL);
        pthread_mutex_unlock(&c->d->av_lk);
        if (rc != 1) { set_error("fi_av_insert(server): %d", rc); return -1; }
    }
    return 0;
}

/* the server's address for pool endpoint e (registering e on first use) */
static int conn_reply_to(struct roce_conn *c, struct ofi_ep *e, uint64_t *rt, int timeout_ms) {
    if (c->reply_gen[e->index] == e->gen) { *rt = c->reply_to[e->index]; return 0; }
    uint64_t a;
    struct ofi_ep *one[1] = {e};
    if (handshake(c, one, 1, &a, timeout_ms, 0)) return -1;
    c->reply_to[e->index] = a;
    c->reply_gen[e->index] = e->gen;
    *rt = a;
    return 0;
}

static roce_conn_t *client_connect(const char *peer, int timeout_ms, size_t max_frame, int dedicated) {
    char host[256], port[32], src[64];
    if (split_hostport(peer, host, sizeof(host), port, sizeof(port))) return NULL;
    struct roce_conn *c = calloc(1, sizeof(*c));
    if (!c) { set_error("out of memory"); return NULL; }
    c->sock = -1;
    c->max_frame = max_frame;
    c->dedicated = dedicated;
    c->d = dom_get(local_ip_for(host, src, sizeof(src)) == 0 ? src : NULL);
    if (!c->d) goto fail;
    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM}, *ai = NULL;
    int gr = getaddrinfo(host, port, &hints, &ai);
    if (gr || !ai) { set_error("resolve %s: %s", peer, gai_strerror(gr)); goto fail; }
    c->sock = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    int crc = c->sock < 0 ? -1 : connect(c->sock, ai->ai_addr, ai->ai_addrlen);
    freeaddrinfo(ai);
    if (crc && errno != EINPROGRESS) { set_error("connect %s: %s", peer, strerror(errno)); goto fail; }
    struct pollfd pf = {.fd = c->sock, .events = POLLOUT};
    if (poll(&pf, 1, timeout_ms) <= 0) { set_error("connect %s: timed out after %d ms", peer, timeout_ms); goto fail; }
    int soerr = 0;
    socklen_t sl = sizeof(soerr);
    getsockopt(c->sock, SOL_SOCKET, SO_ERROR, &soerr, &sl);
    if (soerr) { set_error("connect %s: %s", peer, strerror(soerr)); goto fail; }
    fcntl(c->sock, F_SETFL, fcntl(c->sock, F_GETFL) & ~O_NONBLOCK);
    int one = 1;
    setsockopt(c->sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    if (dedicated) {
        c->own = ep_create(c->d, c->d->av);
        if (!c->own) goto fail;
        struct ofi_ep *eps[1] = {c->own};
        if (handshake(c, eps, 1, &c->own_reply_to, timeout_ms, 1)) goto fail;
        size_t fl = max_frame + 4096;
        c->rbuf = aligned_alloc(4096, fl);
        c->sbuf = aligned_alloc(4096, fl);
        uint64_t k;
        if (!c->rbuf || !c->sbuf || mr_reg(c->d, c->rbuf, fl, &c->rmr, &c->rdesc, &k) ||
            mr_reg(c->d, c->sbuf, fl, &c->smr, &c->sdesc, &c->skey))
            goto fail;
        if (do_recv(c->own, c->rbuf, fl, c->rdesc, &c->rop, OP_RX)) goto fail;
        c->rposted = 1;
    } else {
        /* learn the server; pool endpoints register lazily (conn_reply_to) */
        struct ofi_ep *e = ep_checkout(c->d);
        if (!e) goto fail;
        struct ofi_ep *eps[1] = {e};
        uint64_t a;
        int rc = handshake(c, eps, 1, &a, timeout_ms, 1);
        if (!rc) { c->reply_to[e->index] = a; c->reply_gen[e->index] = e->gen; }
        ep_checkin(c->d, e);
        if (rc) goto fail;
        c->pool = pool_for(c->d, max_frame + 4096, POOL_CLIENT, 512);
        if (!c->pool) goto fail;
    }
    return c;
fail:
    roce_client_close(c);
    return NULL;
}

roce_conn_t *roce_client_connect(const char *peer, int timeout_ms, size_t max_frame_bytes) {
    return client_connect(peer, timeout_ms, max_frame_bytes, 1);
}
roce_conn_t *roce_client_connect_shared(const char *peer, int timeout_ms, size_t max_frame_bytes) {
    return client_connect(peer, timeout_ms, max_frame_bytes, 0);
}

int roce_client_is_alive(roce_conn_t *c) { return c && c->sock >= 0 && !sock_dead(c->sock); }

void roce_client_close(roce_conn_t *c) {
    if (!c) return;
    if (c->own) {
        if (c->rposted && cancel_recv(c->own, &c->rop)) { /* the endpoint is closed below: safe */ }
        ep_destroy(c->own);
    }
    if (c->rmr) fi_close(&c->rmr->fid);
    if (c->smr) fi_close(&c->smr->fid);
    free(c->rbuf);
    free(c->sbuf);
    if (c->sock >= 0) close(c->sock);
    /* the server's AV entry stays in d->av (FI_AV_TABLE indexes are cheap);
     * the server drops ours when it sees the socket close */
    free(c);
}

/* build the request header + inline part in e->small; sources from segs */
/* does the request need a copied bulk buffer (build_request's last case)? */
static int needs_bulk(const struct roce_conn *c, size_t head_len, const roce_seg_t *segs, int nsegs) {
    size_t body = 0;
    int registered = nsegs > 0 && nsegs <= RT_MAX_SRC;
    int device = 0;
    for (int i = 0; i < nsegs; i++) {
        body += segs[i].len;
        if (!segs[i].mem) registered = 0;
        else if (segs[i].mem->dev) device = 1;
    }
    size_t total = head_len + body, cap = SMALL_FRAME - REQ_HDR;
    (void)c;
    if (device) return 0;  /* registered segments or an error, never a copy */
    return !(total <= cap || (registered && head_len <= cap));
}

/* *bulk: a buffer the caller took when needs_bulk() (else NULL) */
static int build_request(struct roce_conn *c, struct ofi_ep *e, uint64_t reply_to, uint64_t tag, const void *head,
                         size_t head_len, const roce_seg_t *segs, int nsegs, struct rbuf **bulk, size_t *send_len) {
    struct req_hdr h = {.magic = REQ_MAGIC, .reply_to = reply_to, .tag = tag};
    size_t body = 0;
    int registered = nsegs > 0 && nsegs <= RT_MAX_SRC, device = 0;
    for (int i = 0; i < nsegs; i++) {
        body += segs[i].len;
        if (!segs[i].mem) registered = 0;
        else if (segs[i].mem->dev) device = 1;
    }
    size_t total = head_len + body, cap = SMALL_FRAME - REQ_HDR;
    if (total > c->max_frame) { set_error("request_len %zu exceeds max_frame_bytes %zu", total, c->max_frame); return -1; }
    if (device && (!registered || head_len > cap)) {
        set_error("device memory segments must all be registered, with a head that fits inline");
        return -1;
    }
    h.total_len = (uint32_t)total;
    uint8_t *inl = e->small + REQ_HDR;
    struct rbuf *given = *bulk;
    *bulk = NULL;
    if (total <= cap && !device) {  /* all inline */
        memcpy(inl, head, head_len);
        size_t o = head_len;
        for (int i = 0; i < nsegs; i++) { memcpy(inl + o, segs[i].addr, segs[i].len); o += segs[i].len; }
        h.inline_len = (uint32_t)total;
    } else if (registered && head_len <= cap) {  /* head inline, segments read in place */
        memcpy(inl, head, head_len);
        h.inline_len = (uint32_t)head_len;
        for (int i = 0; i < nsegs; i++) {
            void *desc;
            uint32_t key;
            if (mem_keys(segs[i].mem, c->d, &desc, &key)) return -1;
            h.src[i] = (struct ofi_src){raddr_of(c->d, segs[i].mem->addr, segs[i].addr), key, (uint32_t)segs[i].len};
        }
        h.nsrc = (uint32_t)nsegs;
    } else {  /* one copied bulk */
        struct rbuf *b = given;
        if (!b) b = pool_get(c->pool ? c->pool : pool_for(c->d, c->max_frame + 4096, POOL_CLIENT, 512), 30000);
        if (!b) return -1;
        size_t o = 0;
        if (head_len <= cap) { memcpy(inl, head, head_len); h.inline_len = (uint32_t)head_len; }
        else { memcpy(b->addr, head, head_len); o = head_len; }
        for (int i = 0; i < nsegs; i++) { memcpy(b->addr + o, segs[i].addr, segs[i].len); o += segs[i].len; }
        h.src[0] = (struct ofi_src){raddr_of(c->d, b->mr_base, b->addr), b->key, (uint32_t)o};
        h.nsrc = 1;
        *bulk = b;
    }
    memcpy(e->small, &h, REQ_HDR);
    *send_len = REQ_HDR + h.inline_len;
    return 0;
}

static void release_bulk(struct roce_conn *c, struct rbuf *b) {
    if (!b) return;
    struct bufpool *bp;
    if (pool_find_any(b->addr, &bp)) pool_put(bp, b);
    (void)c;
}

/* shared-pool request */
static int shared_request(struct roce_conn *c, const void *head, size_t head_len, const roce_seg_t *segs, int nsegs,
                          roce_reply_t *reply, int timeout_ms) {
    if (sock_dead(c->sock)) { set_error("peer closed the connection (%s)", tl_dead_why); return ROCE_ERR_NOT_DELIVERED; }
    struct ofi_ep *e = ep_checkout(c->d);
    if (!e) return ROCE_ERR_NOT_DELIVERED;
    uint64_t rt;
    if (conn_reply_to(c, e, &rt, timeout_ms)) { ep_checkin(c->d, e); return ROCE_ERR_NOT_DELIVERED; }
    struct rbuf *two[2] = {NULL, NULL};
    int nb = needs_bulk(c, head_len, segs, nsegs) ? 2 : 1;
    if (pool_get_n(c->pool, nb, two, 30000)) { ep_checkin(c->d, e); return ROCE_ERR_NOT_DELIVERED; }
    struct rbuf *rb = two[0], *bulk = two[1];
    uint64_t tag = ++e->next_tag;
    size_t send_len;
    struct op tx, rx;
    if (build_request(c, e, rt, tag, head, head_len, segs, nsegs, &bulk, &send_len) ||
        do_recv(e, rb->addr, c->pool->frame, rb->desc, &rx, OP_RX)) {
        pool_put(c->pool, rb);
        release_bulk(c, bulk);
        ep_checkin(c->d, e);
        return ROCE_ERR_NOT_DELIVERED;
    }
    if (do_send(e, e->small, send_len, e->small_desc, c->server, &tx)) {
        if (cancel_recv(e, &rx)) { e = ep_replace(c->d, e); rb = NULL; }
        if (rb) pool_put(c->pool, rb);
        release_bulk(c, bulk);
        if (e) ep_checkin(c->d, e);
        return ROCE_ERR_NOT_DELIVERED;
    }
    struct op *both[2] = {&tx, &rx};
    int rc;
    for (;;) {
        rc = ep_wait_ops(e, both, 2, timeout_ms, c->sock);
        if (rc) break;
        struct rep_hdr *rh = (struct rep_hdr *)rb->addr;
        if (rx.len >= REP_HDR && rh->magic == REP_MAGIC && rh->tag == tag) break;
        /* a late reply to an earlier, timed-out request: drop it, wait on */
        if (do_recv(e, rb->addr, c->pool->frame, rb->desc, &rx, OP_RX)) { rc = -1; break; }
    }
    if (rc) {
        /* the send and the receive may still be in flight: get the buffers
         * back before they are reused, or retire the endpoint */
        int ok = tx.done && cancel_recv(e, &rx) == 0;
        if (!ok) { e = ep_replace(c->d, e); }
        else pool_put(c->pool, rb);
        if (ok) release_bulk(c, bulk);
        if (e) ep_checkin(c->d, e);
        return -1;
    }
    struct rep_hdr *rh = (struct rep_hdr *)rb->addr;
    release_bulk(c, bulk);  /* the server has read it (its reply came after) */
    ep_checkin(c->d, e);
    if (rh->status) { pool_put(c->pool, rb); set_error("server could not handle the request (status %u)", rh->status); return -1; }
    reply->data = rb->addr + REP_HDR;
    reply->len = rx.len - REP_HDR;
    reply->pool = c->pool;
    reply->buf = rb;
    return 0;
}

/* dedicated connection: its own endpoint, an always-posted receive */
static int own_send(struct roce_conn *c, const void *head, size_t head_len, const roce_seg_t *segs, int nsegs,
                    uint64_t tag, int timeout_ms) {
    struct ofi_ep *e = c->own;
    size_t body = 0;
    for (int i = 0; i < nsegs; i++) body += segs[i].len;
    size_t total = head_len + body;
    if (total > c->max_frame) { set_error("request_len %zu exceeds max_frame_bytes %zu", total, c->max_frame); return -1; }
    for (int i = 0; i < nsegs; i++)
        if (segs[i].mem && segs[i].mem->dev) { set_error("device memory needs a shared-pool connection"); return ROCE_ERR_NOT_DELIVERED; }
    struct req_hdr h = {.magic = REQ_MAGIC, .reply_to = c->own_reply_to, .tag = tag, .total_len = (uint32_t)total};
    if (total <= SMALL_FRAME - REQ_HDR) {
        uint8_t *inl = e->small + REQ_HDR;
        memcpy(inl, head, head_len);
        size_t o = head_len;
        for (int i = 0; i < nsegs; i++) { memcpy(inl + o, segs[i].addr, segs[i].len); o += segs[i].len; }
        h.inline_len = (uint32_t)total;
    } else {
        memcpy(c->sbuf, head, head_len);
        size_t o = head_len;
        for (int i = 0; i < nsegs; i++) { memcpy(c->sbuf + o, segs[i].addr, segs[i].len); o += segs[i].len; }
        h.src[0] = (struct ofi_src){raddr_of(c->d, c->sbuf, c->sbuf), (uint32_t)c->skey, (uint32_t)o};
        h.nsrc = 1;
    }
    memcpy(e->small, &h, REQ_HDR);
    struct op tx;
    if (do_send(e, e->small, REQ_HDR + h.inline_len, e->small_desc, c->server, &tx)) return ROCE_ERR_NOT_DELIVERED;
    struct op *o1[1] = {&tx};
    int rc = ep_wait_ops(e, o1, 1, timeout_ms, c->sock);
    return rc ? -1 : 0;
}
/* the next reply on a dedicated connection: payload in c->rbuf + REP_HDR */
static int own_recv(struct roce_conn *c, uint64_t want_tag, size_t *len, int timeout_ms) {
    struct op *o1[1] = {&c->rop};
    for (;;) {
        int rc = ep_wait_ops(c->own, o1, 1, timeout_ms, c->sock);
        if (rc) return -1;
        struct rep_hdr *rh = (struct rep_hdr *)c->rbuf;
        size_t n = c->rop.len;
        int ok = n >= REP_HDR && rh->magic == REP_MAGIC && (!want_tag || rh->tag == want_tag);
        if (ok) { *len = n - REP_HDR; return rh->status ? -1 : 0; }
        if (do_recv(c->own, c->rbuf, c->max_frame + 4096, c->rdesc, &c->rop, OP_RX)) return -1;  /* stale: drop */
    }
}
static int own_rearm(struct roce_conn *c) {
    return do_recv(c->own, c->rbuf, c->max_frame + 4096, c->rdesc, &c->rop, OP_RX);
}

int roce_client_request(roce_conn_t *c, const void *head, size_t head_len, const roce_seg_t *segs, int nsegs,
                        roce_reply_t *reply, int timeout_ms) {
    if (!c->dedicated) return shared_request(c, head, head_len, segs, nsegs, reply, timeout_ms);
    uint64_t tag = ++c->own->next_tag;
    int rc = own_send(c, head, head_len, segs, nsegs, tag, timeout_ms);
    if (rc) return rc;
    size_t n;
    if (own_recv(c, tag, &n, timeout_ms)) return -1;
    uint8_t *out = malloc(n ? n : 1);
    if (!out) { set_error("out of memory"); return -1; }
    memcpy(out, c->rbuf + REP_HDR, n);
    if (own_rearm(c)) { free(out); return -1; }
    reply->data = out;
    reply->len = n;
    reply->pool = NULL;
    reply->buf = NULL;
    return 0;
}

void roce_reply_release(roce_reply_t *reply) {
    if (!reply || !reply->data) return;
    if (reply->pool) pool_put(reply->pool, reply->buf);
    else free(reply->data);
    memset(reply, 0, sizeof(*reply));
}

int roce_client_send_request(roce_conn_t *conn, const uint8_t *request, size_t request_len, uint8_t **response_out,
                             size_t *response_len_out, int timeout_ms) {
    roce_reply_t reply;
    int rc = roce_client_request(conn, request, request_len, NULL, 0, &reply, timeout_ms);
    if (rc) return rc;
    if (!reply.pool) { *response_out = reply.data; *response_len_out = reply.len; return 0; }
    uint8_t *out = malloc(reply.len ? reply.len : 1);
    if (!out) { roce_reply_release(&reply); set_error("out of memory"); return -1; }
    memcpy(out, reply.data, reply.len);
    *response_out = out;
    *response_len_out = reply.len;
    roce_reply_release(&reply);
    return 0;
}

int roce_client_stream_send(roce_conn_t *c, const uint8_t *data, size_t len, int timeout_ms) {
    if (!c->dedicated) { set_error("stream I/O needs a connection from roce_client_connect()"); return -1; }
    return own_send(c, data, len, NULL, 0, ++c->own->next_tag, timeout_ms) ? -1 : 0;
}
int roce_client_stream_recv(roce_conn_t *c, uint8_t **data_out, size_t *len_out, int timeout_ms) {
    if (!c->dedicated) { set_error("stream I/O needs a connection from roce_client_connect()"); return -1; }
    size_t n;
    if (own_recv(c, 0, &n, timeout_ms)) return -1;
    uint8_t *out = malloc(n ? n : 1);
    if (!out) { set_error("out of memory"); return -1; }
    memcpy(out, c->rbuf + REP_HDR, n);
    if (own_rearm(c)) { free(out); return -1; }
    *data_out = out;
    *len_out = n;
    return 0;
}

/* one-sided reads: issued concurrently (RDM does not order them) */
int roce_client_rdma_read(roce_conn_t *c, const roce_read_t *reads, int n, int timeout_ms) {
    if (n < 1 || n > 3) { set_error("roce_client_rdma_read: 1..3 ranges"); return -1; }
    if (sock_dead(c->sock)) { set_error("peer closed the connection (%s)", tl_dead_why); return ROCE_ERR_NOT_DELIVERED; }
    struct ofi_ep *e = c->dedicated ? c->own : ep_checkout(c->d);
    if (!e) return ROCE_ERR_NOT_DELIVERED;
    struct op ops[3];
    struct op *pp[3];
    int posted = 0, rc = 0;
    for (int i = 0; i < n; i++) {
        void *desc;
        uint32_t key;
        if (mem_keys(reads[i].mem, c->d, &desc, &key) ||
            do_read(e, reads[i].local, reads[i].len, desc, c->server, reads[i].raddr, reads[i].rkey, &ops[i])) {
            rc = posted ? -1 : ROCE_ERR_NOT_DELIVERED;
            break;
        }
        pp[posted++] = &ops[i];
    }
    if (posted && ep_wait_ops(e, pp, posted, timeout_ms, c->sock)) rc = -1;
    if (rc == -1) {
        /* reads still in flight write into the caller's memory: wait them out */
        long end = now_ms() + 2000;
        int all;
        do { all = 1; for (int i = 0; i < posted; i++) if (!pp[i]->done) all = 0; if (!all) ep_wait(e, 10, -1); }
        while (!all && now_ms() < end);
        if (!all && !c->dedicated) e = ep_replace(c->d, e);
    }
    if (!c->dedicated && e) ep_checkin(c->d, e);
    return rc;
}

/* ------------------------------------------------------- server side */
/* A client endpoint's AV entry is kept after its last connection closes
 * (refs 0, dormant) and revived when the same endpoint connects again:
 * EFA keeps per-peer message ordering state, and removing a live client
 * endpoint and re-inserting it reset the server's side only -- the next
 * packet aborted the process ("EFA internal error: Packet already
 * processed", 2026-10-23, both OSTs in the first IO500 on EFA). A new
 * client endpoint has a new name (EFA names carry a per-QP qkey). */
struct name_ent { uint8_t name[256]; size_t len; fi_addr_t addr; int refs; int valid; long dormant_since; };
/* Dormant entries are evicted oldest first -- by then their client process
 * has normally exited -- when there are more than ROCE_OFI_DORMANT_MAX
 * (default 4096) or the AV refuses an insert. Kept forever they filled
 * the AV within an IO500 run (every rank of every phase has new endpoints)
 * and new clients were refused: "peer closed the connection", 2026-10-23. */
/* Per server-AV index: a client endpoint that went away must not be
 * replied to or read from by a worker still holding one of its requests,
 * and its index, once removed, may be handed to a new client. So: the
 * handshake gives clients reply_to = epoch << 32 | index; a worker admits
 * a request only while that index is live in that epoch (busy counts the
 * admitted ones), and fi_av_remove waits until none is busy. */
struct aent { uint32_t epoch; int live; int busy; int owed; };
struct sconn { int sock; fi_addr_t *addrs; int naddrs; int w; };

/* One per server endpoint; served by `nthreads` threads (ROCE_OFI_SERVER_
 * THREADS): an endpoint is a queue pair, of which EFA has few per host, so
 * concurrency comes from threads sharing one -- with one thread per
 * endpoint the first EFA IO500 ran at most 9 requests at a time per server
 * on 36 cores (inflight_max 9), its hosts half idle in the metadata phases.
 * A thread claims an arrived request under `pick` and replies from its own
 * registered buffer (tl_small). */
struct worker {
    struct roce_server *s;
    struct ofi_ep *e;
    pthread_t *threads;
    int nthreads;
    pthread_mutex_t pick;
    uint8_t *claimed;
    struct bufpool *pool;
    /* STATS "transport": requests handled; arrival -> picked up (queued
     * behind others); picked up -> reply posted / sent (handler, bulk
     * reads, and for a synchronous reply its completion); waiting for a
     * reply to complete; async replies; times every async slot was busy */
    uint64_t st_reqs, st_queue_ns, st_busy_ns, st_txwait_ns, st_async, st_ring_full;
    uint64_t st_t0;
};
static __thread uint8_t *tl_small;
static __thread void *tl_small_desc;

/* This thread's small replies in flight. A reply used to be sent and then
 * waited for (until EFA's SRD acknowledged it, a network round trip) before
 * the endpoint's thread took the next request: on 2 x i3en the MDTs topped
 * out near 92 k requests/s each with their threads 40% busy, the same in
 * every metadata phase (stat, delete, read). Now the thread goes on at once
 * and recycles the buffer, and releases the client's address entry, when
 * the send completes. Replies too big for a slot still go synchronously. */
#define TX_RING 32
struct txslot { struct op op; int busy; fi_addr_t to; };
struct txring {
    uint8_t *buf;          /* TX_RING x SMALL_FRAME, registered */
    struct fid_mr *mr;
    void *desc;
    struct txslot s[TX_RING];
    int nbusy;
    uint64_t sent, waited;  /* async sends; times all slots were busy */
};
static __thread struct txring *tl_tx;

struct roce_server {
    roce_request_handler handler;
    void *user;
    size_t max_frame;
    struct ofi_dom *d;
    struct fid_av *av;
    pthread_mutex_t av_lk;
    struct aent *ae;
    size_t nae;
    struct name_ent *names;
    int nnames, cnames;
    int ndormant;
    struct worker *w;
    int nw;
    int lsock;
    pthread_t boot;
    atomic_int stopping;
    char bound[300];
    uint32_t rr;
    struct sconn *conns;
    int nconns, cconns;
};

static void pin_to_next_core(void) {
    static atomic_uint next;
    const char *e = getenv("ROCE_PIN_THREADS");
    if (e && !strcmp(e, "0")) return;
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

static int slot_post(struct ofi_ep *e, int i) {
    return do_recv(e, e->slab + (size_t)i * SMALL_FRAME, SMALL_FRAME, e->slab_desc, &e->slots[i], OP_SLOT);
}

static void peer_release(struct roce_server *s, fi_addr_t a);

/* finished async replies: free their slots and their clients' AV entries */
static void tx_reap(struct worker *w) {
    struct txring *t = tl_tx;
    if (!t || !t->nbusy) return;
    for (int i = 0; i < TX_RING; i++) {
        struct txslot *x = &t->s[i];
        if (!x->busy || !__atomic_load_n(&x->op.done, __ATOMIC_ACQUIRE)) continue;
        if (x->op.err && x->op.err != FI_ECANCELED) {
            static _Atomic int said;
            if (atomic_fetch_add(&said, 1) < 5) fprintf(stderr, "ofi_transport: a reply failed: %s\n", fi_strerror(x->op.err));
        }
        x->busy = 0;
        t->nbusy--;
        peer_release(w->s, x->to);
    }
}

/* a free slot of this thread's ring, waiting (progressing the endpoint)
 * while all are in flight; NULL after 30 s */
static struct txslot *tx_slot(struct worker *w) {
    struct txring *t = tl_tx;
    long end = 0;
    for (;;) {
        tx_reap(w);
        if (t->nbusy < TX_RING)
            for (int i = 0; i < TX_RING; i++) if (!t->s[i].busy) return &t->s[i];
        if (!end) { end = now_ms() + 30000; t->waited++; __atomic_add_fetch(&w->st_ring_full, 1, __ATOMIC_RELAXED); }
        else if (now_ms() > end) { set_error("replies not completing (peer gone?)"); return NULL; }
        if (ep_wait(w->e, 10, -1) == -1) return NULL;
    }
}

/* Send a reply (hdr + payload) to `to`. Releases the peer (peer_admit's
 * hold) once the reply has left -- later, for a small reply sent async. */
static int send_reply(struct worker *w, fi_addr_t to, uint64_t tag, uint32_t status, uint8_t *resp, size_t rlen) {
    struct ofi_ep *e = w->e;
    struct rep_hdr h = {.magic = REP_MAGIC, .status = status, .tag = tag};
    struct rbuf *own = NULL;
    uint8_t *start;
    void *desc;
    struct bufpool *bp = NULL;
    struct rbuf *pb = resp ? pool_find_any(resp, &bp) : NULL;
    int pooled = pb && resp - pb->addr >= (ptrdiff_t)REP_HDR;
    if (!pooled && rlen + REP_HDR <= SMALL_FRAME && tl_tx) {
        struct txslot *x = tx_slot(w);
        if (!x) { peer_release(w->s, to); return -1; }
        start = tl_tx->buf + (size_t)(x - tl_tx->s) * SMALL_FRAME;
        memcpy(start, &h, REP_HDR);
        if (rlen) memcpy(start + REP_HDR, resp, rlen);
        if (do_send(e, start, REP_HDR + rlen, tl_tx->desc, to, &x->op)) { peer_release(w->s, to); return -1; }
        x->busy = 1;
        x->to = to;
        tl_tx->nbusy++;
        tl_tx->sent++;
        __atomic_add_fetch(&w->st_async, 1, __ATOMIC_RELAXED);
        return 0;
    }
    if (pooled) {
        start = resp - REP_HDR;
        desc = pb->desc;
    } else if (rlen + REP_HDR <= SMALL_FRAME) {
        start = tl_small ? tl_small : e->small;
        desc = tl_small ? tl_small_desc : e->small_desc;
        if (rlen) memcpy(start + REP_HDR, resp, rlen);
    } else {
        own = pool_get(w->pool, 30000);
        if (!own) { peer_release(w->s, to); return -1; }
        start = own->addr;
        desc = own->desc;
        memcpy(start + REP_HDR, resp, rlen);
    }
    memcpy(start, &h, REP_HDR);
    struct op tx;
    int rc = do_send(e, start, REP_HDR + rlen, desc, to, &tx);
    if (!rc) {
        struct op *o1[1] = {&tx};
        uint64_t t0 = now_ns();
        rc = ep_wait_ops(e, o1, 1, 30000, -1);
        __atomic_add_fetch(&w->st_txwait_ns, now_ns() - t0, __ATOMIC_RELAXED);
    }
    if (own) pool_put(w->pool, own);
    peer_release(w->s, to);
    return rc;
}

static int peer_admit(struct roce_server *s, uint64_t reply_to, fi_addr_t *to) {
    fi_addr_t a = (fi_addr_t)(reply_to & 0xffffffffu);
    int ok = 0;
    pthread_mutex_lock(&s->av_lk);
    if (a < s->nae && s->ae[a].live && s->ae[a].epoch == (uint32_t)(reply_to >> 32)) { s->ae[a].busy++; ok = 1; }
    pthread_mutex_unlock(&s->av_lk);
    *to = a;
    return ok;
}
struct alive_arg { struct roce_server *s; fi_addr_t a; };
static int peer_alive(void *p) {
    struct alive_arg *x = p;
    pthread_mutex_lock(&x->s->av_lk);
    int live = x->s->ae[x->a].live;
    pthread_mutex_unlock(&x->s->av_lk);
    return live;
}
static void peer_release(struct roce_server *s, fi_addr_t a) {
    pthread_mutex_lock(&s->av_lk);
    struct aent *x = &s->ae[a];
    if (--x->busy == 0) for (; x->owed; x->owed--) fi_av_remove(s->av, &a, 1, 0);
    pthread_mutex_unlock(&s->av_lk);
}

static void handle_slot_admitted(struct worker *w, int i, const struct req_hdr *hp, fi_addr_t to);

static void handle_slot(struct worker *w, int i) {
    struct ofi_ep *e = w->e;
    struct op *slot = &e->slots[i];
    uint8_t *msg = e->slab + (size_t)i * SMALL_FRAME;
    struct req_hdr h;
    if (slot->err || slot->len < REQ_HDR) return;
    memcpy(&h, msg, REQ_HDR);
    if (h.magic != REQ_MAGIC || h.nsrc > RT_MAX_SRC || REQ_HDR + (size_t)h.inline_len > slot->len ||
        h.total_len > w->s->max_frame) {
        fprintf(stderr, "ofi_transport: malformed request dropped\n");
        return;
    }
    fi_addr_t to;
    if (!peer_admit(w->s, h.reply_to, &to)) return;  /* its client is gone: nobody to answer */
    struct alive_arg aa = {w->s, to};
    tl_alive = peer_alive;
    tl_alive_arg = &aa;
    handle_slot_admitted(w, i, &h, to);  /* sends exactly one reply, which releases the peer */
    tl_alive = NULL;
}

static void handle_slot_admitted(struct worker *w, int i, const struct req_hdr *hp, fi_addr_t to) {
    struct ofi_ep *e = w->e;
    uint8_t *msg = e->slab + (size_t)i * SMALL_FRAME;
    struct req_hdr h = *hp;
    const uint8_t *frame = msg + REQ_HDR;
    struct rbuf *fb = NULL;
    if (h.nsrc) {
        fb = pool_get(w->pool, 30000);
        if (!fb) { send_reply(w, to, h.tag, 2, NULL, 0); return; }
        /* like the RC transport: the inline part is placed so that the
         * first bulk source (a PUT's or WRITE_AT's data) starts on a page
         * boundary -- an OST with a capacity tier hands that buffer to the
         * NVMe as it is (O_DIRECT / SPDK) */
        size_t pad = (4096 - h.inline_len % 4096) % 4096;
        if (pad + h.total_len > w->pool->frame) pad = 0;
        uint8_t *fbase = fb->addr + pad;
        memcpy(fbase, msg + REQ_HDR, h.inline_len);
        struct op ops[RT_MAX_SRC];
        struct op *pp[RT_MAX_SRC];
        size_t off = h.inline_len;
        int ok = 1;
        uint32_t posted = 0;
        for (uint32_t k = 0; k < h.nsrc && ok; k++) {
            if (off + h.src[k].len > h.total_len) { ok = 0; break; }
            if (do_read(e, fbase + off, h.src[k].len, fb->desc, to, h.src[k].addr, h.src[k].key, &ops[k])) ok = 0;
            else pp[posted++] = &ops[k];
            off += h.src[k].len;
        }
        if (posted && ep_wait_ops(e, pp, (int)posted, 30000, -1)) ok = 0;
        if (!ok) {
            /* never reuse a buffer a read may still write into */
            int all = 1;
            for (uint32_t k = 0; k < posted; k++) if (!pp[k]->done) all = 0;
            if (all) pool_put(w->pool, fb);
            send_reply(w, to, h.tag, 3, NULL, 0);
            return;
        }
        frame = fbase;
    }
    size_t rlen = 0;
    tl_reply_pool = w->pool;
    uint8_t *resp = w->s->handler(frame, h.total_len, &rlen, w->s->user);
    tl_reply_pool = NULL;
    if (fb) pool_put(w->pool, fb);
    if (!resp) { send_reply(w, to, h.tag, 1, NULL, 0); return; }
    if (rlen > w->s->max_frame) {
        fprintf(stderr, "ofi_transport: handler response_len %zu exceeds max_frame_bytes %zu\n", rlen, w->s->max_frame);
        send_reply(w, to, h.tag, 4, NULL, 0);
    } else {
        send_reply(w, to, h.tag, 0, resp, rlen);
    }
    roce_reply_free(resp);
}

static void *worker_main(void *arg) {
    struct worker *w = arg;
    pthread_setname_np(pthread_self(), "rt-ofi");
    tl_server_thread = 1;
    pin_to_next_core();
    struct ofi_ep *e = w->e;
    /* this thread's registered reply buffer */
    struct fid_mr *smr = NULL;
    uint64_t key;
    tl_small = aligned_alloc(4096, SMALL_FRAME);
    if (!tl_small || mr_reg(w->s->d, tl_small, SMALL_FRAME, &smr, &tl_small_desc, &key)) {
        fprintf(stderr, "ofi_transport: worker reply buffer: %s\n", roce_last_error());
        free(tl_small);
        tl_small = NULL;  /* falls back to the endpoint's (only safe alone) */
    }
    const char *ar = getenv("ROCE_OFI_ASYNC_REPLIES");  /* "0": every reply synchronous (as before) */
    struct txring *ring = !(ar && !strcmp(ar, "0")) ? calloc(1, sizeof(*ring)) : NULL;
    if (ring) {
        uint64_t k2;
        ring->buf = aligned_alloc(4096, (size_t)TX_RING * SMALL_FRAME);
        if (!ring->buf || mr_reg(w->s->d, ring->buf, (size_t)TX_RING * SMALL_FRAME, &ring->mr, &ring->desc, &k2)) {
            fprintf(stderr, "ofi_transport: async reply buffers: %s (replies stay synchronous)\n", roce_last_error());
            free(ring->buf);
            free(ring);
            ring = NULL;
        }
    }
    tl_tx = ring;
    while (!atomic_load(&w->s->stopping)) {
        tx_reap(w);
        /* the oldest arrived request nobody has claimed */
        int best = -1;
        pthread_mutex_lock(&w->pick);
        for (int i = 0; i < e->nslots; i++)
            if (!w->claimed[i] && __atomic_load_n(&e->slots[i].done, __ATOMIC_ACQUIRE) &&
                (best < 0 || e->slots[i].seq < e->slots[best].seq)) best = i;
        if (best >= 0) w->claimed[best] = 1;
        pthread_mutex_unlock(&w->pick);
        if (best < 0) { ep_wait(e, 50, -1); continue; }
        uint64_t t_pick = now_ns();
        __atomic_add_fetch(&w->st_queue_ns, t_pick - e->slots[best].t_ns, __ATOMIC_RELAXED);
        handle_slot(w, best);
        __atomic_add_fetch(&w->st_busy_ns, now_ns() - t_pick, __ATOMIC_RELAXED);
        __atomic_add_fetch(&w->st_reqs, 1, __ATOMIC_RELAXED);
        int bad = slot_post(e, best);
        pthread_mutex_lock(&w->pick);
        w->claimed[best] = 0;
        pthread_mutex_unlock(&w->pick);
        if (bad) {
            fprintf(stderr, "ofi_transport: cannot repost a receive: %s\n", roce_last_error());
            break;
        }
    }
    if (ring) {  /* let the replies in flight finish before their buffers go */
        long end = now_ms() + 2000;
        while (ring->nbusy && now_ms() < end) { ep_wait(e, 10, -1); tx_reap(w); }
        tl_tx = NULL;
        if (!ring->nbusy) { fi_close(&ring->mr->fid); free(ring->buf); free(ring); }
        /* else leaked on purpose: the NIC may still read them */
    }
    if (smr) fi_close(&smr->fid);
    free(tl_small);
    tl_small = NULL;
    return NULL;
}

static int bind_listen(const char *bind_address, char *bound, size_t bn) {
    char host[256], port[32];
    if (split_hostport(bind_address, host, sizeof(host), port, sizeof(port))) return -1;
    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM, .ai_flags = AI_PASSIVE}, *ai = NULL;
    int gr = getaddrinfo(strcmp(host, "0.0.0.0") ? host : NULL, port, &hints, &ai);
    if (gr || !ai) { set_error("resolve %s: %s", bind_address, gai_strerror(gr)); return -1; }
    int s = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    int one = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (s < 0 || bind(s, ai->ai_addr, ai->ai_addrlen) || listen(s, 1024)) {
        set_error("bind/listen %s: %s", bind_address, strerror(errno));
        if (s >= 0) close(s);
        freeaddrinfo(ai);
        return -1;
    }
    freeaddrinfo(ai);
    struct sockaddr_in sa;
    socklen_t sl = sizeof(sa);
    getsockname(s, (struct sockaddr *)&sa, &sl);
    snprintf(bound, bn, "%s:%u", host, ntohs(sa.sin_port));
    return s;
}

/* remove the longest-dormant client endpoint from the AV (once no worker
 * holds one of its requests); 0 = one evicted */
static int evict_oldest_dormant(struct roce_server *s) {
    int v = -1;
    for (int i = 0; i < s->nnames; i++)
        if (s->names[i].valid && s->names[i].refs == 0 &&
            (v < 0 || s->names[i].dormant_since < s->names[v].dormant_since)) v = i;
    if (v < 0) return -1;
    fi_addr_t a = s->names[v].addr;
    pthread_mutex_lock(&s->av_lk);
    struct aent *x = &s->ae[a];
    x->live = 0;
    x->owed++;
    if (!x->busy) for (; x->owed; x->owed--) fi_av_remove(s->av, &a, 1, 0);
    pthread_mutex_unlock(&s->av_lk);
    s->names[v].valid = 0;
    s->ndormant--;
    return 0;
}

/* insert (or reference) a client endpoint name in the server AV */
static int name_ref(struct roce_server *s, const uint8_t *name, size_t len, fi_addr_t *out) {
    for (int i = 0; i < s->nnames; i++)
        if (s->names[i].valid && s->names[i].len == len && !memcmp(s->names[i].name, name, len)) {
            if (s->names[i].refs++ == 0) {  /* dormant: live again, in a new epoch */
                s->ndormant--;
                pthread_mutex_lock(&s->av_lk);
                s->ae[s->names[i].addr].epoch++;
                s->ae[s->names[i].addr].live = 1;
                pthread_mutex_unlock(&s->av_lk);
            }
            *out = s->names[i].addr;
            return 0;
        }
    static int dormant_max = -1;
    if (dormant_max < 0) dormant_max = (int)env_u32("ROCE_OFI_DORMANT_MAX", 4096);
    while (s->ndormant > dormant_max && evict_oldest_dormant(s) == 0) {}
    fi_addr_t a;
    pthread_mutex_lock(&s->av_lk);
    int rc = fi_av_insert(s->av, name, 1, &a, 0, NULL);
    for (int tries = 0; rc != 1 && tries < 64 && s->ndormant > 0; tries++) {  /* AV full: make room */
        pthread_mutex_unlock(&s->av_lk);
        if (evict_oldest_dormant(s)) { pthread_mutex_lock(&s->av_lk); break; }
        pthread_mutex_lock(&s->av_lk);
        rc = fi_av_insert(s->av, name, 1, &a, 0, NULL);
    }
    if (rc == 1 && a >= s->nae) {
        size_t nn = s->nae ? s->nae : 256;
        while (nn <= a) nn *= 2;
        struct aent *na = realloc(s->ae, nn * sizeof(*na));
        if (!na) { fi_av_remove(s->av, &a, 1, 0); pthread_mutex_unlock(&s->av_lk); return -1; }
        memset(na + s->nae, 0, (nn - s->nae) * sizeof(*na));
        s->ae = na;
        s->nae = nn;
    }
    if (rc == 1) { s->ae[a].epoch++; s->ae[a].live = 1; }
    pthread_mutex_unlock(&s->av_lk);
    if (rc != 1) { set_error("fi_av_insert(client): %d", rc); return -1; }
    int slot = -1;
    for (int i = 0; i < s->nnames; i++) if (!s->names[i].valid) { slot = i; break; }
    if (slot < 0) {
        if (s->nnames == s->cnames) {
            int nc = s->cnames ? s->cnames * 2 : 64;
            struct name_ent *nn = realloc(s->names, (size_t)nc * sizeof(*nn));
            if (!nn) return -1;
            s->names = nn;
            s->cnames = nc;
        }
        slot = s->nnames++;
    }
    memcpy(s->names[slot].name, name, len);
    s->names[slot].len = len;
    s->names[slot].addr = a;
    s->names[slot].refs = 1;
    s->names[slot].valid = 1;
    *out = a;
    return 0;
}
static void name_unref(struct roce_server *s, fi_addr_t a) {
    for (int i = 0; i < s->nnames; i++)
        if (s->names[i].valid && s->names[i].refs > 0 && s->names[i].addr == a) {
            if (--s->names[i].refs == 0) {
                /* dormant: no more requests admitted, the AV entry (and EFA's
                 * per-peer state) kept for the endpoint's next connection */
                s->names[i].dormant_since = now_ms();
                s->ndormant++;
                pthread_mutex_lock(&s->av_lk);
                s->ae[a].live = 0;
                pthread_mutex_unlock(&s->av_lk);
            }
            return;
        }
}

/* one bootstrap message on conn ci; -1 = drop the connection */
static int boot_message(struct roce_server *s, struct sconn *sc) {
    uint32_t hdr[2];
    if (read_all(sc->sock, hdr, 8, 5000) || hdr[0] != HS_MAGIC || hdr[1] == 0 || hdr[1] > MAX_CEPS) return -1;
    uint32_t n = hdr[1];
    fi_addr_t addrs[MAX_CEPS];
    for (uint32_t i = 0; i < n; i++) {
        uint32_t len;
        uint8_t name[256];
        if (read_all(sc->sock, &len, 4, 5000) || len > sizeof(name) || read_all(sc->sock, name, len, 5000)) return -1;
        if (name_ref(s, name, len, &addrs[i])) return -1;
        fi_addr_t *na = realloc(sc->addrs, (size_t)(sc->naddrs + 1) * sizeof(*na));
        if (!na) return -1;
        sc->addrs = na;
        sc->addrs[sc->naddrs++] = addrs[i];
    }
    /* this connection's server endpoint: round robin over the workers,
     * chosen once per connection */
    if (sc->w < 0) sc->w = (int)(s->rr++ % (uint32_t)s->nw);
    struct ofi_ep *e = s->w[sc->w].e;
    uint32_t rh[3] = {HS_MAGIC, 0, (uint32_t)e->namelen};
    uint8_t out[16 + 256 + MAX_CEPS * 8];
    size_t o = 0;
    memcpy(out, rh, 12); o = 12;
    memcpy(out + o, e->name, e->namelen); o += e->namelen;
    pthread_mutex_lock(&s->av_lk);
    for (uint32_t i = 0; i < n; i++) {
        uint64_t a = (uint64_t)s->ae[addrs[i]].epoch << 32 | (uint64_t)addrs[i];
        memcpy(out + o, &a, 8);
        o += 8;
    }
    pthread_mutex_unlock(&s->av_lk);
    return write_all(sc->sock, out, o);
}

static void drop_conn(struct roce_server *s, int i) {
    struct sconn *sc = &s->conns[i];
    for (int k = 0; k < sc->naddrs; k++) name_unref(s, sc->addrs[k]);
    free(sc->addrs);
    close(sc->sock);
    s->conns[i] = s->conns[--s->nconns];
}

static void *boot_main(void *arg) {
    struct roce_server *s = arg;
    pthread_setname_np(pthread_self(), "rt-ofi-boot");
    /* every bootstrap connection is a descriptor: as many as the process
     * may open (raised to its hard limit), less a margin for the rest */
    struct rlimit rl;
    uint32_t fds = 4096;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur != RLIM_INFINITY)
        fds = rl.rlim_cur > 1024 + 256 ? (uint32_t)(rl.rlim_cur - 256) : 768;
    uint32_t max_conns = env_u32("ROCE_SERVER_MAX_CONNS", fds > 65536 ? 65536 : fds);
    int full_said = 0;
    while (!atomic_load(&s->stopping)) {
        int n = s->nconns;
        struct pollfd *pf = calloc((size_t)n + 1, sizeof(*pf));
        if (!pf) break;
        /* at the limit, stop accepting: new clients wait in the listen
         * backlog (as with the RC transport's gate) instead of being closed
         * -- closing them failed IO500's mdtest-hard with "peer closed the
         * connection" on EFA, 2026-10-23 */
        int full = (uint32_t)n >= max_conns;
        if (full && !full_said) {
            fprintf(stderr, "ofi_transport: %s: %u bootstrap connections (ROCE_SERVER_MAX_CONNS): new clients wait\n",
                    s->bound, max_conns);
            full_said = 1;
        }
        pf[0].fd = full ? -1 : s->lsock;
        pf[0].events = POLLIN;
        for (int i = 0; i < n; i++) { pf[i + 1].fd = s->conns[i].sock; pf[i + 1].events = POLLIN; }
        int pr = poll(pf, (nfds_t)n + 1, 100);
        if (pr > 0) {
            for (int i = n - 1; i >= 0; i--) {
                if (!pf[i + 1].revents) continue;
                char c;
                ssize_t r = recv(s->conns[i].sock, &c, 1, MSG_PEEK | MSG_DONTWAIT);
                /* EOF or a real error: the client is gone; a spurious wake-up
                 * (EAGAIN) is not a reason to drop a live client */
                if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) continue;
                if (r < 0 || (pf[i + 1].revents & (POLLERR | POLLNVAL))) {
                    static atomic_int said_err;
                    if (atomic_fetch_add(&said_err, 1) < 20)
                        fprintf(stderr, "ofi_transport: %s: client connection dropped: %s (revents 0x%x)\n", s->bound,
                                r < 0 ? strerror(errno) : "socket error", pf[i + 1].revents);
                }
                if (r <= 0) { drop_conn(s, i); continue; }
                if (boot_message(s, &s->conns[i])) {
                    static atomic_int said;
                    if (atomic_fetch_add(&said, 1) < 20)
                        fprintf(stderr, "ofi_transport: %s: bootstrap from a client failed (%s): its connection is "
                                "closed\n", s->bound, roce_last_error());
                    drop_conn(s, i);
                }
            }
            if (pf[0].revents & POLLIN) {
                int cs = accept4(s->lsock, NULL, NULL, SOCK_CLOEXEC);
                if (cs < 0 && (errno == EMFILE || errno == ENFILE)) {
                    static int said;
                    if (!said++) fprintf(stderr, "ofi_transport: %s: accept: %s (open-file limit): clients wait\n",
                                         s->bound, strerror(errno));
                    usleep(10000);
                }
                if (cs >= 0) {
                    int one = 1;
                    setsockopt(cs, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                    {
                        if (s->nconns == s->cconns) {
                            int nc = s->cconns ? s->cconns * 2 : 64;
                            struct sconn *ncs = realloc(s->conns, (size_t)nc * sizeof(*ncs));
                            if (ncs) { s->conns = ncs; s->cconns = nc; }
                        }
                        if (s->nconns < s->cconns) s->conns[s->nconns++] = (struct sconn){cs, NULL, 0, -1};
                        else close(cs);
                    }
                }
            }
        }
        free(pf);
    }
    return NULL;
}

/* a server holds a descriptor per client connection: the soft open-file
 * limit (1024 under ssh) is raised to the hard one */
static void raise_nofile(void) {
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 && rl.rlim_cur < rl.rlim_max) {
        rl.rlim_cur = rl.rlim_max == RLIM_INFINITY ? 1048576 : rl.rlim_max;
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

roce_server_t *roce_server_start(const char *bind_address, roce_request_handler handler, void *user_data,
                                 size_t max_frame_bytes) {
    raise_nofile();
    struct roce_server *s = calloc(1, sizeof(*s));
    if (!s) { set_error("out of memory"); return NULL; }
    s->handler = handler;
    s->user = user_data;
    s->max_frame = max_frame_bytes;
    s->lsock = -1;
    pthread_mutex_init(&s->av_lk, NULL);
    char host[256], port[32];
    if (split_hostport(bind_address, host, sizeof(host), port, sizeof(port))) goto fail;
    char src[64];
    const char *src_ip = strcmp(host, "0.0.0.0") ? host : (local_ip_for("8.8.8.8", src, sizeof(src)) ? NULL : src);
    s->d = dom_get(src_ip);
    if (!s->d) goto fail;
    struct fi_av_attr av_attr = {.type = FI_AV_TABLE, .count = env_u32("ROCE_OFI_AV_SIZE", 16384)};
    int rc = fi_av_open(s->d->domain, &av_attr, &s->av, NULL);
    if (rc) { set_fi_error("fi_av_open", rc); goto fail; }
    s->lsock = bind_listen(bind_address, s->bound, sizeof(s->bound));
    if (s->lsock < 0) goto fail;
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    cpu_set_t allowed;
    if (sched_getaffinity(0, sizeof(allowed), &allowed) == 0) ncpu = CPU_COUNT(&allowed);
    /* one endpoint (= one queue pair, one worker thread) per 4 cores, 2..16:
     * EFA has a few hundred QPs per device for ALL processes on the host
     * (several servers + every client rank), so one per core ran out */
    uint32_t k = env_u32("ROCE_OFI_SERVER_EPS", ncpu < 8 ? 2 : (ncpu > 64 ? 16 : (uint32_t)(ncpu / 4)));
    uint32_t depth = env_u32("ROCE_OFI_RX_DEPTH", RX_DEPTH_DEFAULT);
    /* threads per endpoint: 1 unless ROCE_OFI_SERVER_THREADS says more.
     * Several threads sharing an EFA endpoint broke on i3en (2026-10-25):
     * a 300 KB PUT was answered twice (ERROR, then PUT_OK) and the transport
     * checks ran 5x slower -- correct on libfabric's tcp provider, not yet
     * on EFA, so opt-in until it is */
    uint32_t per = env_u32("ROCE_OFI_SERVER_THREADS", 1);
    if (per < 1) per = 1;
    s->w = calloc(k, sizeof(*s->w));
    if (!s->w) goto fail;
    struct bufpool *pool = pool_for(s->d, max_frame_bytes + 4096, POOL_SERVER, 2 * k * per + 8);
    if (!pool) goto fail;
    for (uint32_t i = 0; i < k; i++) {
        struct worker *w = &s->w[i];
        w->s = s;
        w->pool = pool;
        w->e = ep_create(s->d, s->av);
        if (!w->e) {
            /* out of queue pairs (EFA: shared by every process on the host):
             * serve with the endpoints we have rather than not at all */
            if (s->nw > 0) {
                fprintf(stderr, "ofi_transport: %s: %u of %u endpoints (%s): serving with those\n", bind_address,
                        s->nw, k, roce_last_error());
                break;
            }
            goto fail;
        }
        s->nw++;
        w->st_t0 = now_ns();
        struct ofi_ep *e = w->e;
        pthread_mutex_init(&w->pick, NULL);
        w->nthreads = (int)per;
        w->threads = calloc(per, sizeof(*w->threads));
        w->claimed = calloc(depth, 1);
        if (!w->threads || !w->claimed) { set_error("out of memory"); goto fail; }
        e->nslots = (int)depth;
        e->slots = calloc(depth, sizeof(*e->slots));
        e->slab = mmap(NULL, (size_t)depth * SMALL_FRAME, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (!e->slots || e->slab == MAP_FAILED) { e->slab = NULL; set_error("out of memory"); goto fail; }
        uint64_t key;
        if (mr_reg(s->d, e->slab, (size_t)depth * SMALL_FRAME, &e->slab_mr, &e->slab_desc, &key)) goto fail;
        for (int j = 0; j < e->nslots; j++)
            if (slot_post(e, j)) goto fail;
    }
    for (int i = 0; i < s->nw; i++)
        for (int t = 0; t < s->w[i].nthreads; t++)
            if (pthread_create(&s->w[i].threads[t], NULL, worker_main, &s->w[i])) {
                set_error("pthread_create failed");
                goto fail_threads;
            }
    if (pthread_create(&s->boot, NULL, boot_main, s)) { set_error("pthread_create failed"); goto fail_threads; }
    return s;
fail_threads:
    atomic_store(&s->stopping, 1);
    for (int i = 0; i < s->nw; i++)
        for (int t = 0; t < s->w[i].nthreads; t++) if (s->w[i].threads[t]) pthread_join(s->w[i].threads[t], NULL);
fail:
    if (s->lsock >= 0) close(s->lsock);
    for (int i = 0; i < s->nw; i++) ep_destroy(s->w[i].e);
    free(s->w);
    if (s->av) fi_close(&s->av->fid);
    free(s);
    return NULL;
}

size_t roce_server_stats_json(roce_server_t *s, char *out, size_t out_len) {
    uint64_t r = 0, q = 0, b = 0, tw = 0, as = 0, rf = 0, up = 0;
    int threads = 0;
    uint64_t now = now_ns();
    for (int i = 0; i < s->nw; i++) {
        struct worker *w = &s->w[i];
        r += __atomic_load_n(&w->st_reqs, __ATOMIC_RELAXED);
        q += __atomic_load_n(&w->st_queue_ns, __ATOMIC_RELAXED);
        b += __atomic_load_n(&w->st_busy_ns, __ATOMIC_RELAXED);
        tw += __atomic_load_n(&w->st_txwait_ns, __ATOMIC_RELAXED);
        as += __atomic_load_n(&w->st_async, __ATOMIC_RELAXED);
        rf += __atomic_load_n(&w->st_ring_full, __ATOMIC_RELAXED);
        up += (now - w->st_t0) * (uint64_t)w->nthreads;
        threads += w->nthreads;
    }
    int n = snprintf(out, out_len,
                     "\"endpoints\": %d, \"threads\": %d, \"reqs\": %llu, \"queue_ns\": %llu, \"busy_ns\": %llu, "
                     "\"reply_wait_ns\": %llu, \"async_replies\": %llu, \"reply_ring_full\": %llu, \"thread_ns\": %llu",
                     s->nw, threads, (unsigned long long)r, (unsigned long long)q, (unsigned long long)b,
                     (unsigned long long)tw, (unsigned long long)as, (unsigned long long)rf, (unsigned long long)up);
    return n > 0 && (size_t)n < out_len ? (size_t)n : 0;
}

int roce_server_local_address(roce_server_t *s, char *out, size_t out_len) {
    if ((size_t)snprintf(out, out_len, "%s", s->bound) >= out_len) { set_error("buffer too small"); return -1; }
    return 0;
}

int roce_server_mem_rkey(struct roce_server *s, roce_mem_t *mem, uint32_t *rkey) {
    void *desc;
    return mem_keys(mem, s->d, &desc, rkey);
}
uint64_t roce_server_mem_addr(struct roce_server *s, roce_mem_t *mem, const void *va) {
    return raddr_of(s->d, mem->addr, va);
}

void roce_server_stop(roce_server_t *s) {
    if (!s) return;
    atomic_store(&s->stopping, 1);
    /* workers finish the request at hand (each wait is bounded) */
    pthread_join(s->boot, NULL);
    for (int i = 0; i < s->nw; i++)
        for (int t = 0; t < s->w[i].nthreads; t++) pthread_join(s->w[i].threads[t], NULL);
    close(s->lsock);
    s->lsock = -1;
    for (int i = s->nconns - 1; i >= 0; i--) drop_conn(s, i);
    /* endpoints, the AV and the store stay: like the RC transport, the
     * server lives for the process; closing the endpoints here would race
     * a provider progress thread for nothing */
}
