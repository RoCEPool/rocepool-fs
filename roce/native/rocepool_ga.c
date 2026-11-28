/* rocepool_ga.c -- the global address client library (rocepool_ga.h) over
 * the OSTs' global address regions (rocepool_servers.c, OP_GA). */
#define _GNU_SOURCE
#include "rocepool_ga.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rocepool_client.h"
#include "rocepool_devmem.h"
#include "rocepool_ga_exiom.h"

#define GA_OP 48
#define GA_OP_OK 49
#define GA_MAX_OSTS 256
#define GA_SLOTS 65536   /* region descriptors cached, per OST (direct-mapped by region number) */

typedef struct {
    uint32_t id;      /* 0: empty */
    uint64_t size, raddr;
    uint32_t rkey;
    int oneside;
} ga_desc_t;

struct rpga {
    rp_peer_t ost[GA_MAX_OSTS];
    gx_t *ex[GA_MAX_OSTS];           /* an exiom memory server (rocepool_ga_exiom.c), not an OST */
    int n;
    ga_desc_t *cache[GA_MAX_OSTS];   /* allocated on first use */
    pthread_mutex_t lk;
    atomic_uint rr;
    atomic_ullong rd_direct, rd_rpc, wr_direct, wr_bounced;
};

static void le32(uint8_t *p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (8 * i)); }
static void le64(uint8_t *p, uint64_t v) { for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i)); }
static uint32_t rd32(const uint8_t *p) { uint32_t v = 0; for (int i = 3; i >= 0; i--) v = v << 8 | p[i]; return v; }
static uint64_t rd64(const uint8_t *p) { uint64_t v = 0; for (int i = 7; i >= 0; i--) v = v << 8 | p[i]; return v; }

rpga_t *rpga_open(const char *osts) {
    rpga_t *ga = calloc(1, sizeof(*ga));
    if (!ga || !osts) { free(ga); return NULL; }
    char *list = strdup(osts), *save = NULL;
    int bad = 0;
    for (char *t = strtok_r(list, ",", &save); t && ga->n < GA_MAX_OSTS; t = strtok_r(NULL, ",", &save)) {
        if (gx_is_spec(t)) { if (!(ga->ex[ga->n] = gx_open(t))) { bad = 1; break; } }
        else rp_peer_init(&ga->ost[ga->n], t);
        ga->n++;
    }
    free(list);
    pthread_mutex_init(&ga->lk, NULL);
    if (!ga->n || bad) { rpga_close(ga); return NULL; }
    return ga;
}

void rpga_close(rpga_t *ga) {
    if (!ga) return;
    for (int i = 0; i < ga->n; i++) {
        if (ga->ex[i]) gx_close(ga->ex[i]);
        else rp_peer_close_idle(&ga->ost[i]);
        free(ga->cache[i]);
    }
    pthread_mutex_destroy(&ga->lk);
    free(ga);
}

int rpga_osts(const rpga_t *ga) { return ga ? ga->n : 0; }

static int err_code(const rp_resp_t *r) {
    if (r->op != RP_ERROR) return -EIO;
    if (!strcmp(r->code, "NOT_FOUND")) return -ENOENT;
    if (!strcmp(r->code, "EXISTS")) return -EEXIST;
    if (!strcmp(r->code, "RANGE")) return -ERANGE;
    if (!strcmp(r->code, "NO_SPACE")) return -ENOSPC;
    if (!strcmp(r->code, "BAD_REQUEST")) return -EINVAL;
    return -EIO;
}

/* one OP_GA request; *r filled on 0 (the caller frees it) */
static int ga_rpc(rpga_t *ga, int ost, const uint8_t *head, size_t hl, const rp_seg_t *segs, int nsegs, rp_resp_t *r) {
    int rc = rp_rpc_v(&ga->ost[ost], GA_OP, "", head, hl, segs, nsegs, r);
    if (rc) return rc;
    if (r->op == GA_OP_OK) return 0;
    rc = err_code(r);
    rp_resp_free(r);
    return rc;
}

static void cache_put(rpga_t *ga, int ost, const ga_desc_t *d) {
    pthread_mutex_lock(&ga->lk);
    if (!ga->cache[ost]) ga->cache[ost] = calloc(GA_SLOTS, sizeof(ga_desc_t));
    if (ga->cache[ost]) ga->cache[ost][d->id & 0xffff] = *d;
    pthread_mutex_unlock(&ga->lk);
}

static void cache_drop(rpga_t *ga, int ost, uint32_t id) {
    pthread_mutex_lock(&ga->lk);
    if (ga->cache[ost] && ga->cache[ost][id & 0xffff].id == id) ga->cache[ost][id & 0xffff].id = 0;
    pthread_mutex_unlock(&ga->lk);
}

static int parse_desc(const rp_resp_t *r, ga_desc_t *d) {
    if (r->vlen < 25) return -EIO;
    d->id = rd32(r->val);
    d->size = rd64(r->val + 4);
    d->raddr = rd64(r->val + 12);
    d->rkey = rd32(r->val + 20);
    d->oneside = r->val[24];
    return 0;
}

/* the region's descriptor: cached, or looked up by its number */
static int desc_of(rpga_t *ga, int ost, uint32_t id, ga_desc_t *d) {
    if (ost < 0 || ost >= ga->n || !id) return -EINVAL;
    pthread_mutex_lock(&ga->lk);
    int hit = ga->cache[ost] && ga->cache[ost][id].id == id;
    if (hit) *d = ga->cache[ost][id];
    pthread_mutex_unlock(&ga->lk);
    if (hit) return 0;
    uint8_t h[5 + 1];
    h[0] = 2;
    le32(h + 1, id);
    rp_resp_t r;
    int rc = ga_rpc(ga, ost, h, 5, NULL, 0, &r);
    if (rc) return rc;
    rc = parse_desc(&r, d);
    rp_resp_free(&r);
    if (!rc) cache_put(ga, ost, d);
    return rc;
}

static int ost_of_name(rpga_t *ga, const char *name) {
    if (name && *name) return (int)(rp_sha256_u64(name) % (uint64_t)ga->n);
    return (int)(atomic_fetch_add(&ga->rr, 1) % (unsigned)ga->n);
}

int rpga_alloc(rpga_t *ga, int ost, const char *name, uint64_t size, int exclusive, rpga_addr_t *addr) {
    if (!ga || !addr) return -EINVAL;
    size_t nl = name ? strlen(name) : 0;
    if (nl >= 128) return -ENAMETOOLONG;
    if (!size || size > (1ull << RPGA_OFFSET_BITS)) return -ERANGE;
    if (ost < 0) ost = ost_of_name(ga, name);
    if (ost >= ga->n) return -EINVAL;
    if (ga->ex[ost]) {
        uint32_t slot;
        int rc = gx_alloc(ga->ex[ost], name, size, exclusive, &slot);
        if (!rc) *addr = RPGA_MAKE(ost, slot, 0);
        return rc;
    }
    uint8_t h[10 + 128];
    h[0] = 1;
    le64(h + 1, size);
    h[9] = exclusive ? 1 : 0;
    if (nl) memcpy(h + 10, name, nl);
    rp_resp_t r;
    int rc = ga_rpc(ga, ost, h, 10 + nl, NULL, 0, &r);
    if (rc) return rc;
    ga_desc_t d;
    rc = parse_desc(&r, &d);
    rp_resp_free(&r);
    if (rc) return rc;
    cache_put(ga, ost, &d);
    *addr = RPGA_MAKE(ost, d.id, 0);
    return 0;
}

int rpga_lookup(rpga_t *ga, int ost, const char *name, rpga_addr_t *addr, uint64_t *size) {
    if (!ga || !name || !*name || !addr) return -EINVAL;
    size_t nl = strlen(name);
    if (nl >= 128) return -ENAMETOOLONG;
    if (ost < 0) ost = ost_of_name(ga, name);
    if (ost >= ga->n) return -EINVAL;
    if (ga->ex[ost]) {
        uint32_t slot;
        int rc = gx_lookup(ga->ex[ost], name, &slot, size);
        if (!rc) *addr = RPGA_MAKE(ost, slot, 0);
        return rc;
    }
    uint8_t h[5 + 128];
    h[0] = 2;
    le32(h + 1, 0xffffffffu);
    memcpy(h + 5, name, nl);
    rp_resp_t r;
    int rc = ga_rpc(ga, ost, h, 5 + nl, NULL, 0, &r);
    if (rc) return rc;
    ga_desc_t d;
    rc = parse_desc(&r, &d);
    rp_resp_free(&r);
    if (rc) return rc;
    cache_put(ga, ost, &d);
    *addr = RPGA_MAKE(ost, d.id, 0);
    if (size) *size = d.size;
    return 0;
}

int rpga_free(rpga_t *ga, rpga_addr_t addr) {
    if (!ga) return -EINVAL;
    int ost = RPGA_OST(addr);
    uint32_t id = RPGA_REGION(addr);
    if (ost >= ga->n || !id) return -EINVAL;
    if (ga->ex[ost]) return gx_free(ga->ex[ost], id);
    cache_drop(ga, ost, id);
    uint8_t h[5];
    h[0] = 3;
    le32(h + 1, id);
    rp_resp_t r;
    int rc = ga_rpc(ga, ost, h, 5, NULL, 0, &r);
    if (!rc) rp_resp_free(&r);
    return rc;
}

int rpga_region_size(rpga_t *ga, rpga_addr_t addr, uint64_t *size) {
    if (ga && RPGA_OST(addr) < ga->n && ga->ex[RPGA_OST(addr)])
        return gx_size(ga->ex[RPGA_OST(addr)], RPGA_REGION(addr), size);
    ga_desc_t d;
    int rc = ga ? desc_of(ga, RPGA_OST(addr), RPGA_REGION(addr), &d) : -EINVAL;
    if (!rc && size) *size = d.size;
    return rc;
}

/* ---------------------------------------------------------------- buffers */
rpga_mem_t *rpga_register_dev(rpga_t *ga, void *base, size_t len) {
    const rp_devrt_t *rt = rp_devrt_get();
    void *abase;
    size_t asize;
    int dev, fd;
    uint64_t foff;
    if (!ga || !base || !len || !rt || rt->info(base, &abase, &asize, &dev, &fd, &foff)) return NULL;
    if ((uint8_t *)base + len > (uint8_t *)abase + asize) { rp_devrt_close_fd(fd); return NULL; }
    rpga_mem_t *m = calloc(1, sizeof(*m));
    if (!m) { rp_devrt_close_fd(fd); return NULL; }
    m->base = base;
    m->len = len;
    m->fd = fd;
    m->rt = rt;
    const char *bounce = getenv("ROCEPOOL_GDS_BOUNCE");
    if (fd != -2 && !(bounce && !strcmp(bounce, "1"))) m->mem = roce_mem_register_device(abase, asize, fd, foff, dev);
    return m;
}

static int host_copy(void *dst, const void *src, size_t n, void *ctx) { (void)ctx; memcpy(dst, src, n); return 0; }
static int host_zero(void *dst, size_t n, void *ctx) { (void)ctx; memset(dst, 0, n); return 0; }
static const rp_devrt_t g_host_rt = {"host", NULL, {host_copy, host_copy, host_zero, NULL}};

rpga_mem_t *rpga_register_host(rpga_t *ga, void *base, size_t len) {
    if (!ga || !base || !len) return NULL;
    rpga_mem_t *m = calloc(1, sizeof(*m));
    if (!m) return NULL;
    m->base = base;
    m->len = len;
    m->fd = -1;
    m->rt = &g_host_rt;
    const char *bounce = getenv("ROCEPOOL_GDS_BOUNCE");
    if (!(bounce && !strcmp(bounce, "1"))) m->mem = roce_mem_register(base, len);
    return m;
}

void rpga_deregister(rpga_mem_t *m) {
    if (!m) return;
    roce_mem_release(m->mem);
    rp_devrt_close_fd(m->fd);
    free(m);
}

int rpga_mem_direct(const rpga_mem_t *m) { return m && m->mem && !atomic_load(&m->broken); }

/* ---------------------------------------------------------------- transfers */
#define ONE_READ_MAX (1u << 30)   /* one RDMA READ: at most 1 GiB (the work request's length is 32 bits) */

static size_t rpc_chunk(void) { return rp_frame_bytes > (1u << 20) ? rp_frame_bytes - 4096 : rp_frame_bytes / 2; }

static int read_rpc(rpga_t *ga, int ost, uint32_t id, uint64_t off, uint8_t *dst, size_t len, rpga_mem_t *m) {
    size_t ch = rpc_chunk();
    for (size_t done = 0; done < len;) {
        size_t n = len - done < ch ? len - done : ch;
        uint8_t h[17];
        h[0] = 5;
        le32(h + 1, id);
        le64(h + 5, off + done);
        le32(h + 13, (uint32_t)n);
        rp_resp_t r;
        int rc = ga_rpc(ga, ost, h, 17, NULL, 0, &r);
        if (rc) return rc;
        if (r.vlen != n) { rp_resp_free(&r); return -EIO; }
        rc = m->rt->ops.to_dev(dst + done, r.val, n, m->rt->ops.ctx);
        rp_resp_free(&r);
        if (rc) return rc;
        done += n;
    }
    atomic_fetch_add(&ga->rd_rpc, len);
    return 0;
}

int rpga_read(rpga_t *ga, rpga_addr_t src, rpga_mem_t *m, size_t off, size_t len) {
    if (!ga || !m || off > m->len || len > m->len - off) return -EINVAL;
    if (!len) return 0;
    int ost = RPGA_OST(src);
    uint32_t id = RPGA_REGION(src);
    uint64_t roff = RPGA_OFFSET(src);
    if (ost >= ga->n) return -EINVAL;
    if (ga->ex[ost]) {
        int direct = 0, rc = gx_read(ga->ex[ost], id, roff, m, off, len, &direct);
        if (!rc) atomic_fetch_add(direct ? &ga->rd_direct : &ga->rd_rpc, len);
        return rc;
    }
    uint8_t *dst = m->base + off;
    for (int attempt = 0; attempt < 2; attempt++) {
        ga_desc_t d;
        int rc = desc_of(ga, ost, id, &d);
        if (rc) return rc;
        if (roff > d.size || len > d.size - roff) return -ERANGE;
        if (!d.oneside || !rpga_mem_direct(m)) return read_rpc(ga, ost, id, roff, dst, len, m);
        for (size_t done = 0; done < len && !rc;) {
            uint32_t n = (uint32_t)(len - done < ONE_READ_MAX ? len - done : ONE_READ_MAX);
            rp_read_t rd = {d.raddr + roff + done, d.rkey, n, m->mem, dst + done};
            rc = rp_rdma_read(&ga->ost[ost], &rd, 1);
            done += n;
        }
        if (!rc) { atomic_fetch_add(&ga->rd_direct, len); return 0; }
        /* freed (deregistered) since it was cached, or the transfer failed:
         * look it up again once -- gone is -ENOENT, else by RPC */
        cache_drop(ga, ost, id);
        if (attempt) break;
    }
    ga_desc_t d;
    int rc = desc_of(ga, ost, id, &d);
    if (rc) return rc;
    atomic_store(&m->broken, 1);
    return read_rpc(ga, ost, id, roff, dst, len, m);
}

int rpga_write(rpga_t *ga, rpga_addr_t dstaddr, rpga_mem_t *m, size_t off, size_t len) {
    if (!ga || !m || off > m->len || len > m->len - off) return -EINVAL;
    if (!len) return 0;
    int ost = RPGA_OST(dstaddr);
    uint32_t id = RPGA_REGION(dstaddr);
    uint64_t roff = RPGA_OFFSET(dstaddr);
    if (ost >= ga->n) return -EINVAL;
    if (ga->ex[ost]) {
        int direct = 0, rc = gx_write(ga->ex[ost], id, roff, m, off, len, &direct);
        if (!rc) atomic_fetch_add(direct ? &ga->wr_direct : &ga->wr_bounced, len);
        return rc;
    }
    ga_desc_t d;
    int rc = desc_of(ga, ost, id, &d);
    if (rc) return rc;
    if (roff > d.size || len > d.size - roff) return -ERANGE;
    const uint8_t *src = m->base + off;
    size_t ch = rpc_chunk();
    int direct = rpga_mem_direct(m);
    uint8_t *tmp = direct ? NULL : rp_bulk_alloc(len < ch ? len : ch);
    if (!direct && !tmp) return -ENOMEM;
    for (size_t done = 0; done < len && !rc;) {
        size_t n = len - done < ch ? len - done : ch;
        uint8_t h[13];
        h[0] = 4;
        le32(h + 1, id);
        le64(h + 5, roff + done);
        rp_seg_t seg = {src + done, n, m->mem};
        if (!direct) {
            rc = m->rt->ops.from_dev(tmp, src + done, n, m->rt->ops.ctx);
            if (rc) break;
            seg = (rp_seg_t){tmp, n, NULL};
        }
        rp_resp_t r;
        rc = ga_rpc(ga, ost, h, 13, &seg, 1, &r);
        if (!rc) rp_resp_free(&r);
        else if (rc == -ENOENT) cache_drop(ga, ost, id);
        done += n;
    }
    if (tmp) rp_bulk_free(tmp);
    if (!rc) atomic_fetch_add(direct ? &ga->wr_direct : &ga->wr_bounced, len);
    return rc;
}

/* many: one thread per OST involved, each doing its OST's transfers */
typedef struct {
    rpga_t *ga;
    int write;
    size_t n, *idx;
    const rpga_addr_t *a;
    rpga_mem_t *m;
    const size_t *offs, *lens;
    int *status;
} ga_job_t;

static void *ga_job(void *arg) {
    ga_job_t *j = arg;
    for (size_t k = 0; k < j->n; k++) {
        size_t i = j->idx[k];
        int rc = j->write ? rpga_write(j->ga, j->a[i], j->m, j->offs[i], j->lens[i])
                          : rpga_read(j->ga, j->a[i], j->m, j->offs[i], j->lens[i]);
        if (j->status) j->status[i] = rc;
    }
    return NULL;
}

static int many(rpga_t *ga, int write, size_t n, const rpga_addr_t *a, rpga_mem_t *m, const size_t *offs,
                const size_t *lens, int *status) {
    if (!ga || !m || (n && (!a || !offs || !lens))) return -EINVAL;
    int *st = status ? status : calloc(n ? n : 1, sizeof(int));
    size_t *idx = malloc((n ? n : 1) * sizeof(size_t));
    ga_job_t *jobs = calloc((size_t)ga->n, sizeof(*jobs));
    pthread_t *th = calloc((size_t)ga->n, sizeof(*th));
    char *started = calloc((size_t)ga->n, 1);
    int rc = 0;
    if (!st || !idx || !jobs || !th || !started) { rc = -ENOMEM; goto out; }
    size_t pos = 0;
    for (int o = 0; o < ga->n; o++) {
        jobs[o] = (ga_job_t){ga, write, 0, idx + pos, a, m, offs, lens, st};
        for (size_t i = 0; i < n; i++)
            if (RPGA_OST(a[i]) == o) idx[pos + jobs[o].n++] = i;
        pos += jobs[o].n;
    }
    for (size_t i = 0; i < n; i++)
        if (RPGA_OST(a[i]) >= ga->n) st[i] = -EINVAL;
    for (int o = 0; o < ga->n; o++) {
        if (!jobs[o].n) continue;
        if (pthread_create(&th[o], NULL, ga_job, &jobs[o]) == 0) started[o] = 1;
        else ga_job(&jobs[o]);
    }
    for (int o = 0; o < ga->n; o++)
        if (started[o]) pthread_join(th[o], NULL);
    for (size_t i = 0; i < n && !rc; i++) rc = st[i];
out:
    if (st != status) free(st);
    free(idx);
    free(jobs);
    free(th);
    free(started);
    return rc;
}

int rpga_mread(rpga_t *ga, size_t n, const rpga_addr_t *src, rpga_mem_t *m, const size_t *offs, const size_t *lens,
               int *status) {
    return many(ga, 0, n, src, m, offs, lens, status);
}

int rpga_mwrite(rpga_t *ga, size_t n, const rpga_addr_t *dst, rpga_mem_t *m, const size_t *offs, const size_t *lens,
                int *status) {
    return many(ga, 1, n, dst, m, offs, lens, status);
}

void rpga_stats(rpga_t *ga, uint64_t *read_direct, uint64_t *read_rpc, uint64_t *write_direct, uint64_t *write_bounced,
                uint64_t *rpcs) {
    if (!ga) return;
    if (read_direct) *read_direct = atomic_load(&ga->rd_direct);
    if (read_rpc) *read_rpc = atomic_load(&ga->rd_rpc);
    if (write_direct) *write_direct = atomic_load(&ga->wr_direct);
    if (write_bounced) *write_bounced = atomic_load(&ga->wr_bounced);
    if (rpcs) {
        uint64_t t = 0;
        for (int i = 0; i < ga->n; i++) {
            uint64_t r = 0;
            if (ga->ex[i]) gx_path(ga->ex[i], NULL, NULL, &r);
            else r = atomic_load(&ga->ost[i].rpcs);
            t += r;
        }
        *rpcs = t;
    }
}

/* ---------------------------------------------------------------- exiom: values by key, paths */
int rpga_stage(rpga_t *ga, int ost, const char *name, uint64_t size, rpga_addr_t *addr) {
    if (!ga || !name || !*name || !addr) return -EINVAL;
    if (strlen(name) >= 128) return -ENAMETOOLONG;
    if (ost < 0) ost = ost_of_name(ga, name);
    if (ost >= ga->n) return -EINVAL;
    if (!ga->ex[ost]) return -EOPNOTSUPP;
    uint32_t slot;
    int rc = gx_stage(ga->ex[ost], name, size, &slot);
    if (!rc) *addr = RPGA_MAKE(ost, slot, 0);
    return rc;
}

int rpga_commit(rpga_t *ga, rpga_addr_t addr) {
    int ost = ga ? RPGA_OST(addr) : 0;
    if (!ga || ost >= ga->n) return -EINVAL;
    if (!ga->ex[ost]) return -EOPNOTSUPP;
    return gx_commit(ga->ex[ost], RPGA_REGION(addr));
}

int rpga_path(rpga_t *ga, int ost, int *endpoint, uint64_t *failovers) {
    if (!ga || ost < 0 || ost >= ga->n) return -EINVAL;
    if (!ga->ex[ost]) return -EOPNOTSUPP;
    gx_path(ga->ex[ost], endpoint, failovers, NULL);
    return 0;
}
