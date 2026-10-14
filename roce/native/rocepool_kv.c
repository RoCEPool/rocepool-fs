/* rocepool_kv.c -- native KV client API (rocepool_kv.h) on the shared
 * client core (rocepool_client.c). Build with -DRPKV_BENCH for the
 * rocepool-kv-bench driver (main at the bottom). */
#define _GNU_SOURCE
#include "rocepool_kv.h"
#include "rocepool_client.h"
#include "rocepool_devmem.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define KV_MAX_OSTS 256
#define LC_SLOTS 16384   /* cached leases per handle (direct-mapped by key hash) */

typedef struct { uint64_t h; char *key; rp_lease_t l; } lc_slot_t;

struct rpkv {
    rp_peer_t ost[KV_MAX_OSTS];
    int n;
    char ns[128];
    lc_slot_t *lc;
    pthread_mutex_t lc_lk;
    atomic_ullong dev_direct, dev_bounced, leases, lease_hits;
};

void rpkv_free(void *p) { free(p); }

rpkv_t *rpkv_open(const char *osts, const char *ns) {
    rpkv_t *kv = calloc(1, sizeof(*kv));
    if (!kv || !osts) { free(kv); return NULL; }
    char *list = strdup(osts), *save = NULL;
    for (char *t = strtok_r(list, ",", &save); t && kv->n < KV_MAX_OSTS; t = strtok_r(NULL, ",", &save))
        rp_peer_init(&kv->ost[kv->n++], t);
    free(list);
    snprintf(kv->ns, sizeof(kv->ns), "%s", ns ? ns : "kv/");
    if (!kv->n) { free(kv); return NULL; }
    kv->lc = calloc(LC_SLOTS, sizeof(lc_slot_t));
    pthread_mutex_init(&kv->lc_lk, NULL);
    return kv;
}

void rpkv_close(rpkv_t *kv) {
    if (!kv) return;
    for (int i = 0; i < kv->n; i++) rp_peer_close_idle(&kv->ost[i]);
    if (kv->lc)
        for (size_t i = 0; i < LC_SLOTS; i++) free(kv->lc[i].key);
    free(kv->lc);
    pthread_mutex_destroy(&kv->lc_lk);
    free(kv);
}

uint64_t rpkv_rpcs(rpkv_t *kv) {
    uint64_t t = 0;
    for (int i = 0; i < kv->n; i++) t += atomic_load(&kv->ost[i].rpcs);
    return t;
}

/* full key = ns + key; owner = sha256(full key) mod #OSTs (the same rule
 * roce/pool_node.py uses for its keyspace) */
static int full_key(const rpkv_t *kv, const char *key, char *out, size_t n) {
    int w = snprintf(out, n, "%s%s", kv->ns, key);
    return (w < 0 || (size_t)w >= n) ? -ENAMETOOLONG : 0;
}

static rp_peer_t *owner(rpkv_t *kv, const char *fk) { return &kv->ost[rp_sha256_u64(fk) % (uint64_t)kv->n]; }

int rpkv_put(rpkv_t *kv, const char *key, const void *val, size_t len, uint64_t *version) {
    char fk[4096];
    int rc = full_key(kv, key, fk, sizeof(fk));
    return rc ? rc : rp_ost_put(owner(kv, fk), fk, val, len, -1, version);
}

int rpkv_put_if(rpkv_t *kv, const char *key, const void *val, size_t len, uint64_t expected, uint64_t *version) {
    char fk[4096];
    int rc = full_key(kv, key, fk, sizeof(fk));
    return rc ? rc : rp_ost_put(owner(kv, fk), fk, val, len, (int64_t)expected, version);
}

int rpkv_get(rpkv_t *kv, const char *key, void **val, size_t *len, uint64_t *version) {
    char fk[4096];
    int rc = full_key(kv, key, fk, sizeof(fk));
    if (rc) return rc;
    rp_resp_t r;
    uint64_t ver;
    rc = rp_ost_read_at(owner(kv, fk), fk, 0, 0, 1, &ver, &r);  /* GET_V: value + version, one snapshot */
    if (rc) return rc;
    size_t n = r.vlen - 8;
    void *out = malloc(n ? n : 1);
    if (!out) { rp_resp_free(&r); return -ENOMEM; }
    memcpy(out, r.val + 8, n);
    rp_resp_free(&r);
    *val = out;
    if (len) *len = n;
    if (version) *version = ver;
    return 0;
}

int rpkv_delete(rpkv_t *kv, const char *key) {
    char fk[4096];
    int rc = full_key(kv, key, fk, sizeof(fk));
    return rc ? rc : rp_ost_delete(owner(kv, fk), fk);
}

int rpkv_write_at(rpkv_t *kv, const char *key, uint64_t off, const void *data, size_t len) {
    char fk[4096];
    int rc = full_key(kv, key, fk, sizeof(fk));
    return rc ? rc : rp_ost_write_at(owner(kv, fk), fk, off, data, len, NULL, NULL);
}

int rpkv_read_at(rpkv_t *kv, const char *key, uint64_t off, size_t len, void **val, size_t *got) {
    char fk[4096];
    int rc = full_key(kv, key, fk, sizeof(fk));
    if (rc) return rc;
    if (len > UINT32_MAX) len = UINT32_MAX;
    rp_resp_t r;
    rc = rp_ost_read_at(owner(kv, fk), fk, off, (uint32_t)len, 0, NULL, &r);
    if (rc) return rc;
    size_t n = r.vlen - 8;
    void *out = malloc(n ? n : 1);
    if (!out) { rp_resp_free(&r); return -ENOMEM; }
    memcpy(out, r.val + 8, n);
    rp_resp_free(&r);
    *val = out;
    *got = n;
    return 0;
}

/* ---------------------------------------------------------------- batches
 * Keys are grouped by owning OST; each group goes as BATCH frames, one
 * thread per OST group. Both directions stay under half a frame:
 *  - requests: split by encoded size; one item too big to share a batch is
 *    sent on its own (and fails alone, -EFBIG, if it exceeds a frame);
 *  - replies (mget): value sizes are unknown up front, so each key is read
 *    with READ_AT capped at budget/keys-in-batch -- a value that fills its
 *    cap may be longer and is re-read on its own with GET_V. */
#define KV_MGET_MIN_CAP 4096

typedef struct {
    rpkv_t *kv;
    int ost;
    uint8_t op;           /* RP_PUT / RP_GET_V / RP_DELETE */
    size_t *idx;          /* indexes into the caller's arrays */
    size_t cnt;
    const char *const *keys;
    const void *const *vals;
    const size_t *lens;
    void **outv;
    size_t *outl;
    int *status;
    int rc;
} group_job_t;

static int status_of(uint8_t op, const uint8_t *v, uint32_t vl) {
    if (op != RP_ERROR) return 0;
    char code[32];
    size_t n = 0;
    while (n < vl && n < sizeof(code) - 1 && v[n]) { code[n] = (char)v[n]; n++; }
    code[n] = 0;
    if (!strcmp(code, "NOT_FOUND")) return -ENOENT;
    if (!strcmp(code, "VERSION_CONFLICT")) return -EAGAIN;
    return -EIO;
}

/* one key on its own (oversized for a batch, or a truncated mget read) */
static int single(group_job_t *j, size_t k) {
    if (j->op == RP_PUT) return rpkv_put(j->kv, j->keys[k], j->vals[k], j->lens[k], NULL);
    if (j->op == RP_DELETE) return rpkv_delete(j->kv, j->keys[k]);
    free(j->outv[k]);
    j->outv[k] = NULL;
    j->outl[k] = 0;
    return rpkv_get(j->kv, j->keys[k], &j->outv[k], &j->outl[k], NULL);
}

static void *group_run(void *arg) {
    group_job_t *j = arg;
    size_t budget = rp_frame_bytes / 2;
    size_t max_keys = j->op == RP_GET_V ? budget / KV_MGET_MIN_CAP : SIZE_MAX;
    if (!max_keys) max_keys = 1;
    size_t i = 0;
    size_t *redo = malloc((j->cnt ? j->cnt : 1) * sizeof(size_t));
    size_t nredo = 0;
    while (i < j->cnt && !j->rc) {
        size_t first = i, last = i;
        /* pass 1: which items go in this batch */
        size_t len = 4;
        for (; last < j->cnt && last - first < max_keys; last++) {
            size_t k = j->idx[last];
            size_t add = 9 + strlen(j->kv->ns) + strlen(j->keys[k]) +
                         (j->op == RP_PUT ? 9 + j->lens[k] : j->op == RP_GET_V ? 12 : 0);
            if (add > budget) {
                if (last == first) { redo[nredo++] = k; first = ++last; len = 4; }  /* alone */
                break;
            }
            if (len + add > budget) break;
            len += add;
        }
        i = last;
        if (first == last) continue;
        size_t nk = last - first;
        uint32_t cap = 0;
        if (j->op == RP_GET_V) {
            size_t c = budget / nk;
            cap = c > UINT32_MAX ? UINT32_MAX : (uint32_t)c;
        }
        rp_batch_t b;
        rp_batch_init(&b);
        static const uint8_t no_cas[9] = {0};
        for (size_t q = first; q < last; q++) {
            size_t k = j->idx[q];
            char fk[4096];
            full_key(j->kv, j->keys[k], fk, sizeof(fk));  /* checked in run_grouped */
            if (j->op == RP_PUT) {
                rp_batch_add(&b, RP_PUT, fk, no_cas, 9, j->vals[k], j->lens[k]);
            } else if (j->op == RP_GET_V) {
                uint8_t ra[12] = {0};  /* offset 0 (u64 LE) + cap (u32 LE) */
                for (int x = 0; x < 4; x++) ra[8 + x] = (uint8_t)(cap >> (8 * x));
                rp_batch_add(&b, RP_READ_AT, fk, ra, 12, NULL, 0);
            } else {
                rp_batch_add(&b, j->op, fk, NULL, 0, NULL, 0);
            }
        }
        rp_resp_t r;
        int rc = rp_batch_send(&j->kv->ost[j->ost], &b, &r);
        rp_batch_free(&b);
        if (rc) { j->rc = rc; break; }
        size_t pos = 0;
        for (size_t q = first; q < last; q++) {
            size_t k = j->idx[q];
            uint8_t op;
            const uint8_t *v;
            uint32_t vl;
            if (rp_batch_next(&r, &pos, &op, &v, &vl, NULL, 0)) { j->status[k] = -EIO; continue; }
            j->status[k] = status_of(op, v, vl);
            if (j->op == RP_GET_V && !j->status[k]) {
                size_t n = vl >= 8 ? vl - 8 : 0;
                if (n >= cap) { redo[nredo++] = k; continue; }  /* may be longer than its cap */
                void *out = malloc(n ? n : 1);
                if (!out) { j->status[k] = -ENOMEM; continue; }
                memcpy(out, v + 8, n);
                j->outv[k] = out;
                j->outl[k] = n;
            }
        }
        rp_resp_free(&r);
    }
    for (size_t q = 0; q < nredo && !j->rc; q++) j->status[redo[q]] = single(j, redo[q]);
    free(redo);
    return NULL;
}

static int run_grouped(rpkv_t *kv, uint8_t op, size_t n, const char *const *keys, const void *const *vals,
                       const size_t *lens, void **outv, size_t *outl, int *status) {
    size_t *count = calloc((size_t)kv->n, sizeof(size_t));
    size_t **idx = calloc((size_t)kv->n, sizeof(size_t *));
    for (size_t k = 0; k < n; k++) {
        status[k] = -EIO;
        if (outv) { outv[k] = NULL; outl[k] = 0; }
        char fk[4096];
        if (full_key(kv, keys[k], fk, sizeof(fk))) { status[k] = -ENAMETOOLONG; continue; }
        int o = (int)(rp_sha256_u64(fk) % (uint64_t)kv->n);
        if (!idx[o]) idx[o] = malloc(n * sizeof(size_t));
        idx[o][count[o]++] = k;
    }
    group_job_t *jobs = calloc((size_t)kv->n, sizeof(*jobs));
    pthread_t *th = calloc((size_t)kv->n, sizeof(pthread_t));
    int *started = calloc((size_t)kv->n, sizeof(int));
    int last = -1;
    for (int o = 0; o < kv->n; o++) {
        if (!count[o]) continue;
        jobs[o] = (group_job_t){kv, o, op, idx[o], count[o], keys, vals, lens, outv, outl, status, 0};
        if (last >= 0 && pthread_create(&th[last], NULL, group_run, &jobs[last]) == 0) started[last] = 1;
        else if (last >= 0) group_run(&jobs[last]);
        last = o;
    }
    if (last >= 0) group_run(&jobs[last]);  /* the last group runs on the calling thread */
    int rc = 0;
    for (int o = 0; o < kv->n; o++) {
        if (started[o]) pthread_join(th[o], NULL);
        if (count[o] && jobs[o].rc && !rc) rc = jobs[o].rc;
        free(idx[o]);
    }
    free(count); free(idx); free(jobs); free(th); free(started);
    return rc;
}

int rpkv_mput(rpkv_t *kv, size_t n, const char *const *keys, const void *const *vals, const size_t *lens,
              int *status) {
    return run_grouped(kv, RP_PUT, n, keys, vals, lens, NULL, NULL, status);
}

int rpkv_mget(rpkv_t *kv, size_t n, const char *const *keys, void **vals, size_t *lens, int *status) {
    return run_grouped(kv, RP_GET_V, n, keys, NULL, NULL, vals, lens, status);
}

int rpkv_mdelete(rpkv_t *kv, size_t n, const char *const *keys, int *status) {
    return run_grouped(kv, RP_DELETE, n, keys, NULL, NULL, NULL, NULL, status);
}

/* ---------------------------------------------------------------- GPU memory */
struct rpkv_dev {
    rpkv_t *kv;
    uint8_t *base;          /* as given to rpkv_dev_register: offsets count from here */
    size_t len;
    roce_mem_t *mem;        /* the allocation's NIC registration; NULL: bounce */
    int fd;                 /* its dma-buf (ours to close), or -1 / -2 */
    const rp_devrt_t *rt;
    atomic_int broken;      /* a direct transfer failed where a bounced one worked: stop trying */
};

/* lease cache: a hit costs no RPC; every read re-checks it (seqlock) */
static int lc_get(rpkv_t *kv, const char *fk, rp_lease_t *l) {
    uint64_t h = rp_sha256_u64(fk);
    lc_slot_t *e = &kv->lc[h % LC_SLOTS];
    int hit = 0;
    pthread_mutex_lock(&kv->lc_lk);
    if (e->key && e->h == h && !strcmp(e->key, fk)) { *l = e->l; hit = 1; }
    pthread_mutex_unlock(&kv->lc_lk);
    return hit;
}
static void lc_put(rpkv_t *kv, const char *fk, const rp_lease_t *l) {
    uint64_t h = rp_sha256_u64(fk);
    lc_slot_t *e = &kv->lc[h % LC_SLOTS];
    char *k = strdup(fk);
    pthread_mutex_lock(&kv->lc_lk);
    free(e->key);
    *e = (lc_slot_t){h, k, *l};
    pthread_mutex_unlock(&kv->lc_lk);
}
static void lc_drop(rpkv_t *kv, const char *fk) {
    uint64_t h = rp_sha256_u64(fk);
    lc_slot_t *e = &kv->lc[h % LC_SLOTS];
    pthread_mutex_lock(&kv->lc_lk);
    if (e->key && e->h == h && !strcmp(e->key, fk)) { free(e->key); e->key = NULL; }
    pthread_mutex_unlock(&kv->lc_lk);
}

rpkv_dev_t *rpkv_dev_register(rpkv_t *kv, void *base, size_t len) {
    const rp_devrt_t *rt = rp_devrt_get();
    void *abase;
    size_t asize;
    int dev, fd;
    uint64_t foff;
    if (!kv || !base || !len || !rt || rt->info(base, &abase, &asize, &dev, &fd, &foff)) return NULL;
    if ((uint8_t *)base + len > (uint8_t *)abase + asize) { rp_devrt_close_fd(fd); return NULL; }
    rpkv_dev_t *d = calloc(1, sizeof(*d));
    d->kv = kv;
    d->base = base;
    d->len = len;
    d->fd = fd;
    d->rt = rt;
    const char *bounce = getenv("ROCEPOOL_GDS_BOUNCE");
    if (fd != -2 && !(bounce && !strcmp(bounce, "1")))
        d->mem = roce_mem_register_device(abase, asize, fd, foff, dev);
    return d;
}

void rpkv_dev_deregister(rpkv_dev_t *d) {
    if (!d) return;
    roce_mem_release(d->mem);
    rp_devrt_close_fd(d->fd);
    free(d);
}

int rpkv_dev_direct(const rpkv_dev_t *d) { return d && d->mem && !atomic_load(&d->broken); }

void rpkv_dev_stats(rpkv_t *kv, uint64_t *direct, uint64_t *bounced, uint64_t *leases, uint64_t *lease_hits) {
    if (direct) *direct = atomic_load(&kv->dev_direct);
    if (bounced) *bounced = atomic_load(&kv->dev_bounced);
    if (leases) *leases = atomic_load(&kv->leases);
    if (lease_hits) *lease_hits = atomic_load(&kv->lease_hits);
}

static uint64_t be64(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = (v << 8) | p[i];
    return v;
}

static int get_dev_one(rpkv_t *kv, const char *fk, rpkv_dev_t *d, size_t off, size_t cap, size_t *len, int count_hit) {
    if (off > d->len || cap > d->len - off) return -EINVAL;
    rp_peer_t *p = owner(kv, fk);
    uint8_t *dst = d->base + off;
    int tried_direct = 0;
    if (rpkv_dev_direct(d) && !atomic_load(&p->no_lease)) {
        rp_lease_t l;
        int have = lc_get(kv, fk, &l);
        if (have && count_hit) atomic_fetch_add(&kv->lease_hits, 1);
        for (int attempt = 0; attempt < 2; attempt++) {
            if (!have) {
                int rc = rp_ost_lease(p, fk, &l);
                if (rc == -ENOENT) return -ENOENT;
                if (rc == -ENOTSUP) break;  /* this object / server: by RPC */
                if (rc) return rc;
                atomic_fetch_add(&kv->leases, 1);
                lc_put(kv, fk, &l);
                have = 1;
            }
            if (len) *len = (size_t)l.size;
            if (l.size > cap) return -EFBIG;
            int rc = rp_ost_read_direct_mem(p, &l, 0, (size_t)l.size, dst, d->mem);
            if (rc == 0) { atomic_fetch_add(&kv->dev_direct, l.size); return 0; }
            lc_drop(kv, fk);
            have = 0;
            if (rc != -EAGAIN) { tried_direct = 1; break; }  /* not changed meanwhile: the transfer itself failed */
        }
    }
    /* through host memory: GET_V + a copy into the device */
    rp_resp_t r;
    uint64_t ver;
    int rc = rp_ost_read_at(p, fk, 0, 0, 1, &ver, &r);
    if (rc) return rc;
    size_t n = r.vlen - 8;
    if (len) *len = n;
    if (n > cap) { rp_resp_free(&r); return -EFBIG; }
    rc = n ? d->rt->ops.to_dev(dst, r.val + 8, n, d->rt->ops.ctx) : 0;
    rp_resp_free(&r);
    if (rc) return rc;
    atomic_fetch_add(&kv->dev_bounced, n);
    if (tried_direct) atomic_store(&d->broken, 1);
    return 0;
}

static int put_dev_one(rpkv_t *kv, const char *fk, rpkv_dev_t *d, size_t off, size_t len, uint64_t *version) {
    if (off > d->len || len > d->len - off) return -EINVAL;
    rp_peer_t *p = owner(kv, fk);
    const uint8_t *src = d->base + off;
    lc_drop(kv, fk);
    int tried_direct = 0;
    if (rpkv_dev_direct(d)) {
        /* the OST RDMA-reads the value out of the device */
        rp_seg_t seg = {src, len, d->mem};
        int rc = rp_ost_put_v(p, fk, &seg, 1, -1, version);
        if (rc == 0) { atomic_fetch_add(&kv->dev_direct, len); return 0; }
        if (rc != -EIO) return rc;
        tried_direct = 1;
    }
    uint8_t *tmp = rp_bulk_alloc(len ? len : 1);
    if (!tmp) return -ENOMEM;
    int rc = len ? d->rt->ops.from_dev(tmp, src, len, d->rt->ops.ctx) : 0;
    if (!rc) rc = rp_ost_put(p, fk, tmp, len, -1, version);
    rp_bulk_free(tmp);
    if (rc) return rc;
    atomic_fetch_add(&kv->dev_bounced, len);
    if (tried_direct) atomic_store(&d->broken, 1);
    return 0;
}

int rpkv_put_dev(rpkv_t *kv, const char *key, rpkv_dev_t *d, size_t off, size_t len, uint64_t *version) {
    char fk[4096];
    int rc = full_key(kv, key, fk, sizeof(fk));
    return rc ? rc : !d ? -EINVAL : put_dev_one(kv, fk, d, off, len, version);
}

int rpkv_get_dev(rpkv_t *kv, const char *key, rpkv_dev_t *d, size_t off, size_t cap, size_t *len) {
    char fk[4096];
    int rc = full_key(kv, key, fk, sizeof(fk));
    return rc ? rc : !d ? -EINVAL : get_dev_one(kv, fk, d, off, cap, len, 1);
}

/* leases for many keys: one BATCH of LEASE per OST (all OSTs in parallel),
 * cached; present[k] = 1 found, 0 not found, -1 unknown (error) */
typedef struct {
    rpkv_t *kv;
    size_t *idx, cnt;
    char **fks;
    int *present;
    int rc;
} lease_job_t;

static void *lease_group(void *arg) {
    lease_job_t *j = arg;
    const size_t per = 1024;
    for (size_t first = 0; first < j->cnt && !j->rc; first += per) {
        size_t last = first + per < j->cnt ? first + per : j->cnt;
        rp_batch_t b;
        rp_batch_init(&b);
        for (size_t q = first; q < last; q++) rp_batch_add(&b, RP_LEASE, j->fks[j->idx[q]], NULL, 0, NULL, 0);
        rp_resp_t r;
        int rc = rp_batch_send(owner(j->kv, j->fks[j->idx[first]]), &b, &r);
        rp_batch_free(&b);
        if (rc) { j->rc = rc; break; }
        size_t pos = 0;
        for (size_t q = first; q < last; q++) {
            size_t k = j->idx[q];
            uint8_t op;
            const uint8_t *v;
            uint32_t vl;
            if (rp_batch_next(&r, &pos, &op, &v, &vl, NULL, 0)) { j->present[k] = -1; continue; }
            if (op == RP_LEASE_OK && vl >= 44) {
                rp_lease_t l = {be64(v), be64(v + 8), be64(v + 16), be64(v + 24), be64(v + 32),
                                ((uint32_t)v[40] << 24) | ((uint32_t)v[41] << 16) | ((uint32_t)v[42] << 8) | v[43]};
                lc_put(j->kv, j->fks[k], &l);
                atomic_fetch_add(&j->kv->leases, 1);
                j->present[k] = 1;
            } else {
                int st = status_of(op, v, vl);
                /* NO_LEASE / UNSUPPORTED_OP: it exists, readable by RPC only */
                j->present[k] = st == -ENOENT ? 0 : st == -EIO && vl >= 8 && !memcmp(v, "NO_LEASE", 8) ? 1
                              : st == -EIO && vl >= 14 && !memcmp(v, "UNSUPPORTED_OP", 14) ? 1 : -1;
            }
        }
        rp_resp_free(&r);
    }
    return NULL;
}

static int batch_leases(rpkv_t *kv, size_t n, char **fks, const int *skip, int *present) {
    size_t *count = calloc((size_t)kv->n, sizeof(size_t));
    size_t **idx = calloc((size_t)kv->n, sizeof(size_t *));
    for (size_t k = 0; k < n; k++) {
        if (skip && skip[k]) continue;
        int o = (int)(owner(kv, fks[k]) - kv->ost);
        if (!idx[o]) idx[o] = malloc(n * sizeof(size_t));
        idx[o][count[o]++] = k;
    }
    lease_job_t *jobs = calloc((size_t)kv->n, sizeof(*jobs));
    pthread_t *th = calloc((size_t)kv->n, sizeof(pthread_t));
    int *started = calloc((size_t)kv->n, sizeof(int));
    for (int o = 0; o < kv->n; o++) {
        if (!count[o]) continue;
        jobs[o] = (lease_job_t){kv, idx[o], count[o], fks, present, 0};
        if (pthread_create(&th[o], NULL, lease_group, &jobs[o]) == 0) started[o] = 1;
        else lease_group(&jobs[o]);
    }
    int rc = 0;
    for (int o = 0; o < kv->n; o++) {
        if (started[o]) pthread_join(th[o], NULL);
        if (count[o] && jobs[o].rc && !rc) rc = jobs[o].rc;
        free(idx[o]);
    }
    free(count); free(idx); free(jobs); free(th); free(started);
    return rc;
}

static char **full_keys(rpkv_t *kv, size_t n, const char *const *keys, int *status) {
    char **fks = calloc(n ? n : 1, sizeof(char *));
    for (size_t k = 0; k < n; k++) {
        char fk[4096];
        if (full_key(kv, keys[k], fk, sizeof(fk))) { status[k] = -ENAMETOOLONG; continue; }
        fks[k] = strdup(fk);
    }
    return fks;
}
static void free_keys(char **fks, size_t n) {
    for (size_t k = 0; k < n; k++) free(fks[k]);
    free(fks);
}

int rpkv_mcontains(rpkv_t *kv, size_t n, const char *const *keys, int *present) {
    int *st = calloc(n ? n : 1, sizeof(int));
    char **fks = full_keys(kv, n, keys, st);
    int *skip = calloc(n ? n : 1, sizeof(int));
    for (size_t k = 0; k < n; k++) { present[k] = 0; skip[k] = !fks[k]; }
    int rc = batch_leases(kv, n, fks, skip, present);
    for (size_t k = 0; k < n; k++) if (present[k] < 0) present[k] = 0;
    free_keys(fks, n);
    free(st);
    free(skip);
    return rc;
}

/* the transfers of a multi-key call, in parallel (ROCEPOOL_KV_INFLIGHT
 * at a time, default 16): KV blocks are small, the wire is kept busy by
 * having many in flight across all OSTs */
typedef struct {
    rpkv_t *kv;
    rpkv_dev_t *d;
    size_t n;
    char **fks;
    const size_t *offs, *lens_in;
    size_t *lens_out;
    int *status;
    int put;
    atomic_size_t next;
} xfer_t;

static void *xfer_worker(void *arg) {
    xfer_t *x = arg;
    for (;;) {
        size_t k = atomic_fetch_add(&x->next, 1);
        if (k >= x->n) return NULL;
        if (!x->fks[k] || x->status[k] != 1) continue;  /* 1 = to do */
        x->status[k] = x->put ? put_dev_one(x->kv, x->fks[k], x->d, x->offs[k], x->lens_in[k], NULL)
                              : get_dev_one(x->kv, x->fks[k], x->d, x->offs[k], x->lens_in[k], &x->lens_out[k], 0);
    }
}

static void run_xfers(xfer_t *x) {
    const char *e = getenv("ROCEPOOL_KV_INFLIGHT");
    size_t t = e && atoi(e) > 0 ? (size_t)atoi(e) : 16;
    if (t > x->n) t = x->n;
    pthread_t *th = calloc(t ? t : 1, sizeof(pthread_t));
    int *ok = calloc(t ? t : 1, sizeof(int));
    for (size_t i = 1; i < t; i++) ok[i] = pthread_create(&th[i], NULL, xfer_worker, x) == 0;
    xfer_worker(x);
    for (size_t i = 1; i < t; i++) if (ok[i]) pthread_join(th[i], NULL);
    free(th);
    free(ok);
}

int rpkv_mput_dev(rpkv_t *kv, size_t n, const char *const *keys, rpkv_dev_t *d, const size_t *offs,
                  const size_t *lens, int *status) {
    if (!d) return -EINVAL;
    for (size_t k = 0; k < n; k++) status[k] = 1;
    char **fks = full_keys(kv, n, keys, status);
    xfer_t x = {kv, d, n, fks, offs, lens, NULL, status, 1, 0};
    run_xfers(&x);
    free_keys(fks, n);
    return 0;
}

int rpkv_mget_dev(rpkv_t *kv, size_t n, const char *const *keys, rpkv_dev_t *d, const size_t *offs,
                  const size_t *caps, size_t *lens, int *status) {
    if (!d) return -EINVAL;
    for (size_t k = 0; k < n; k++) { status[k] = 1; lens[k] = 0; }
    char **fks = full_keys(kv, n, keys, status);
    if (rpkv_dev_direct(d)) {
        /* the leases not cached yet: one round trip per OST for all of them */
        int *skip = calloc(n ? n : 1, sizeof(int)), *present = calloc(n ? n : 1, sizeof(int));
        rp_lease_t l;
        for (size_t k = 0; k < n; k++) {
            skip[k] = !fks[k] || lc_get(kv, fks[k], &l);
            if (fks[k] && skip[k]) atomic_fetch_add(&kv->lease_hits, 1);
            present[k] = -1;
        }
        if (batch_leases(kv, n, fks, skip, present) == 0)
            for (size_t k = 0; k < n; k++)
                if (!skip[k] && present[k] == 0) status[k] = -ENOENT;
        free(skip);
        free(present);
    }
    xfer_t x = {kv, d, n, fks, offs, caps, lens, status, 0, 0};
    run_xfers(&x);
    free_keys(fks, n);
    return 0;
}

#ifdef RPKV_BENCH
/* rocepool-kv-bench OSTS THREADS SECONDS VALUE_BYTES MODE(get|put|mget) [BATCH]
 * prints: RESULT {"ops_per_s": ..., "p50_us": ..., "p99_us": ...} */
#include <time.h>
typedef struct {
    rpkv_t *kv;
    int tid, mode, batch;
    size_t vbytes;
    double until;
    uint64_t ops;
    double *lat;
    size_t nlat, caplat;
} bench_t;

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static void *bench_run(void *arg) {
    bench_t *b = arg;
    char *val = malloc(b->vbytes ? b->vbytes : 1);
    memset(val, 'v', b->vbytes);
    char **keys = calloc((size_t)b->batch, sizeof(char *));
    for (int i = 0; i < b->batch; i++) keys[i] = malloc(64);
    void **outv = calloc((size_t)b->batch, sizeof(void *));
    size_t *outl = calloc((size_t)b->batch, sizeof(size_t));
    int *st = calloc((size_t)b->batch, sizeof(int));
    uint64_t i = 0;
    while (now_s() < b->until) {
        double t0 = now_s();
        if (b->mode == 2) {
            for (int k = 0; k < b->batch; k++) snprintf(keys[k], 64, "k%d-%llu", b->tid, (unsigned long long)((i + (uint64_t)k) % 1000));
            rpkv_mget(b->kv, (size_t)b->batch, (const char *const *)keys, outv, outl, st);
            for (int k = 0; k < b->batch; k++) free(outv[k]);
            b->ops += (uint64_t)b->batch;
        } else {
            snprintf(keys[0], 64, "k%d-%llu", b->tid, (unsigned long long)(i % 1000));
            if (b->mode == 1) {
                rpkv_put(b->kv, keys[0], val, b->vbytes, NULL);
            } else {
                void *v = NULL;
                size_t l;
                if (rpkv_get(b->kv, keys[0], &v, &l, NULL) == 0) free(v);
            }
            b->ops++;
        }
        i += (uint64_t)b->batch;
        if (b->nlat < b->caplat) b->lat[b->nlat++] = (now_s() - t0) * 1e6;
    }
    return NULL;
}

static int cmpd(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

int main(int argc, char **argv) {
    if (argc < 6) { fprintf(stderr, "usage: %s OSTS THREADS SECONDS VALUE_BYTES get|put|mget [BATCH]\n", argv[0]); return 2; }
    rpkv_t *kv = rpkv_open(argv[1], "bench/");
    int threads = atoi(argv[2]);
    double secs = atof(argv[3]);
    size_t vbytes = (size_t)atol(argv[4]);
    int mode = !strcmp(argv[5], "put") ? 1 : !strcmp(argv[5], "mget") ? 2 : 0;
    int batch = mode == 2 ? (argc > 6 ? atoi(argv[6]) : 64) : 1;
    /* preload the keys every mode reads */
    char *val = malloc(vbytes ? vbytes : 1);
    memset(val, 'v', vbytes);
    for (int t = 0; t < threads; t++)
        for (int k = 0; k < 1000; k++) {
            char key[64];
            snprintf(key, sizeof(key), "k%d-%d", t, k);
            rpkv_put(kv, key, val, vbytes, NULL);
        }
    bench_t *b = calloc((size_t)threads, sizeof(*b));
    pthread_t *th = calloc((size_t)threads, sizeof(pthread_t));
    double until = now_s() + secs;
    for (int t = 0; t < threads; t++) {
        b[t] = (bench_t){kv, t, mode, batch, vbytes, until, 0, NULL, 0, 1000000};
        b[t].lat = malloc(b[t].caplat * sizeof(double));
        pthread_create(&th[t], NULL, bench_run, &b[t]);
    }
    uint64_t ops = 0;
    size_t nl = 0;
    for (int t = 0; t < threads; t++) { pthread_join(th[t], NULL); ops += b[t].ops; nl += b[t].nlat; }
    double *all = malloc((nl ? nl : 1) * sizeof(double));
    size_t w = 0;
    for (int t = 0; t < threads; t++) { memcpy(all + w, b[t].lat, b[t].nlat * sizeof(double)); w += b[t].nlat; }
    qsort(all, nl, sizeof(double), cmpd);
    printf("RESULT {\"ops_per_s\": %.1f, \"p50_us\": %.1f, \"p99_us\": %.1f, \"rpcs\": %llu}\n", ops / secs,
           nl ? all[nl / 2] : 0.0, nl ? all[(size_t)(nl * 0.99)] : 0.0, (unsigned long long)rpkv_rpcs(kv));
    return 0;
}
#endif
