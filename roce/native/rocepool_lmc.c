/* rocepool_lmc.c -- asynchronous batched KV-cache store (rocepool_lmc.h). */
#define _GNU_SOURCE
#include "rocepool_lmc.h"
#include "rocepool_client.h"
#include "rocepool_kv.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

#define LMC_MAX_REGIONS 16
#define LMC_COMMIT_MAGIC "RPLMC001"
#define LMC_COMMIT_BYTES 24   /* magic (8) + chunk size (u64 LE) + part size (u64 LE) */

typedef struct job {
    struct job *next;
    uint64_t id;
    int op;
    size_t n;
    char **keys;
    uint8_t **bufs;
    size_t *lens;
    uint8_t *res;
    int ok;
    char err[160];
} job_t;

typedef struct { uint8_t *base; size_t len; rpkv_dev_t *d; } region_t;

struct rplmc {
    rpkv_t *kv;
    size_t part;
    int efd;
    pthread_mutex_t lk;
    pthread_cond_t cv;
    job_t *qhead, *qtail;     /* submitted, not started */
    job_t *dhead, *dtail;     /* completed, not popped */
    uint64_t next_id;
    int stop;
    int nworkers;
    pthread_t *th;
    region_t reg[LMC_MAX_REGIONS];
    int nreg;
    pthread_rwlock_t reglk;
    atomic_ullong ops, keys, bytes_set, bytes_get, copied, parts_failed;
};

/* ---------------------------------------------------------------- helpers */
static void put_u64(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i));
}
static uint64_t get_u64(const uint8_t *p) {
    uint64_t v = 0;
    for (int i = 7; i >= 0; i--) v = (v << 8) | p[i];
    return v;
}

static char *part_key(const char *key, size_t j) {
    size_t n = strlen(key) + 24;
    char *s = malloc(n);
    if (s) snprintf(s, n, "%s#%zu", key, j);
    return s;
}

static size_t nparts(size_t len, size_t part) { return len ? (len + part - 1) / part : 1; }

/* the region holding [p, p+len) entirely, or -1 */
static int find_region(rplmc_t *c, const uint8_t *p, size_t len) {
    int r = -1;
    pthread_rwlock_rdlock(&c->reglk);
    for (int i = 0; i < c->nreg; i++)
        if (p >= c->reg[i].base && len <= c->reg[i].len && (size_t)(p - c->reg[i].base) <= c->reg[i].len - len) {
            r = i;
            break;
        }
    pthread_rwlock_unlock(&c->reglk);
    return r;
}

static void set_err(job_t *j, const char *what, int rc, size_t bad, size_t of) {
    if (j->err[0]) return;
    snprintf(j->err, sizeof(j->err), "%s: %s on %zu of %zu parts", what, strerror(rc < 0 ? -rc : rc), bad, of);
}

/* ---------------------------------------------------------------- parts */
typedef struct {
    char **keys;      /* part keys */
    uint8_t **ptr;    /* where each part lives in the caller's buffer */
    size_t *len;
    size_t *owner;    /* chunk index */
    int *st;
    size_t n;
} parts_t;

static int parts_build(rplmc_t *c, job_t *j, parts_t *p) {
    size_t total = 0;
    for (size_t k = 0; k < j->n; k++) total += nparts(j->lens[k], c->part);
    p->n = total;
    p->keys = calloc(total ? total : 1, sizeof(char *));
    p->ptr = calloc(total ? total : 1, sizeof(uint8_t *));
    p->len = calloc(total ? total : 1, sizeof(size_t));
    p->owner = calloc(total ? total : 1, sizeof(size_t));
    p->st = calloc(total ? total : 1, sizeof(int));
    if (!p->keys || !p->ptr || !p->len || !p->owner || !p->st) return -ENOMEM;
    size_t q = 0;
    for (size_t k = 0; k < j->n; k++) {
        size_t np = nparts(j->lens[k], c->part);
        for (size_t x = 0; x < np; x++, q++) {
            size_t off = x * c->part;
            p->keys[q] = part_key(j->keys[k], x);
            if (!p->keys[q]) return -ENOMEM;
            p->ptr[q] = j->bufs[k] + off;
            p->len[q] = j->lens[k] - off < c->part ? j->lens[k] - off : c->part;
            p->owner[q] = k;
            p->st[q] = -EIO;
        }
    }
    return 0;
}

static void parts_free(parts_t *p) {
    if (p->keys)
        for (size_t q = 0; q < p->n; q++) free(p->keys[q]);
    free(p->keys); free(p->ptr); free(p->len); free(p->owner); free(p->st);
}

/* run the parts (set: store, else load), grouped by the region they sit
 * in: one batched zero-copy call per region, one copying call for the rest */
static void parts_run(rplmc_t *c, parts_t *p, int set) {
    int *where = malloc((p->n ? p->n : 1) * sizeof(int));
    const char **ks = malloc((p->n ? p->n : 1) * sizeof(char *));
    size_t *offs = malloc((p->n ? p->n : 1) * sizeof(size_t));
    size_t *ls = malloc((p->n ? p->n : 1) * sizeof(size_t));
    size_t *got = malloc((p->n ? p->n : 1) * sizeof(size_t));
    int *st = malloc((p->n ? p->n : 1) * sizeof(int));
    size_t *idx = malloc((p->n ? p->n : 1) * sizeof(size_t));
    void **vals = malloc((p->n ? p->n : 1) * sizeof(void *));
    if (!where || !ks || !offs || !ls || !got || !st || !idx || !vals) goto out;
    for (size_t q = 0; q < p->n; q++) where[q] = find_region(c, p->ptr[q], p->len[q]);
    for (int r = -1; r < LMC_MAX_REGIONS; r++) {
        size_t m = 0;
        for (size_t q = 0; q < p->n; q++)
            if (where[q] == r) {
                idx[m] = q;
                ks[m] = p->keys[q];
                ls[m] = p->len[q];
                if (r >= 0) offs[m] = (size_t)(p->ptr[q] - c->reg[r].base);
                vals[m] = p->ptr[q];
                m++;
            }
        if (!m) continue;
        if (r >= 0) {
            rpkv_dev_t *d = c->reg[r].d;
            if (set) {
                rpkv_mput_dev(c->kv, m, ks, d, offs, ls, st);
            } else {
                rpkv_mget_dev(c->kv, m, ks, d, offs, ls, got, st);
                for (size_t i = 0; i < m; i++)
                    if (!st[i] && got[i] != ls[i]) st[i] = -EBADMSG;  /* stored part of another size */
            }
        } else if (set) {
            rpkv_mput(c->kv, m, ks, (const void *const *)vals, ls, st);
            for (size_t i = 0; i < m; i++) if (!st[i]) atomic_fetch_add(&c->copied, ls[i]);
        } else {
            void **out = calloc(m, sizeof(void *));
            if (!out) { for (size_t i = 0; i < m; i++) st[i] = -ENOMEM; }
            else {
                rpkv_mget(c->kv, m, ks, out, got, st);
                for (size_t i = 0; i < m; i++) {
                    if (!st[i] && got[i] != ls[i]) st[i] = -EBADMSG;
                    if (!st[i]) { memcpy(vals[i], out[i], ls[i]); atomic_fetch_add(&c->copied, ls[i]); }
                    rpkv_free(out[i]);
                }
                free(out);
            }
        }
        for (size_t i = 0; i < m; i++) p->st[idx[i]] = st[i];
    }
out:
    free(where); free(ks); free(offs); free(ls); free(got); free(st); free(idx); free(vals);
}

/* ---------------------------------------------------------------- ops */
static void run_set(rplmc_t *c, job_t *j) {
    parts_t p = {0};
    int rc = parts_build(c, j, &p);
    if (rc) { set_err(j, "set", rc, j->n, j->n); parts_free(&p); return; }
    parts_run(c, &p, 1);
    uint8_t *chunk_ok = malloc(j->n ? j->n : 1);
    for (size_t k = 0; k < j->n; k++) chunk_ok[k] = 1;
    size_t bad = 0;
    int first = 0;
    for (size_t q = 0; q < p.n; q++)
        if (p.st[q]) { chunk_ok[p.owner[q]] = 0; bad++; if (!first) first = p.st[q]; }
    if (bad) { atomic_fetch_add(&c->parts_failed, bad); set_err(j, "set", first, bad, p.n); }
    /* commit the chunks whose parts are all in */
    size_t m = 0;
    const char **ks = malloc((j->n ? j->n : 1) * sizeof(char *));
    const void **vs = malloc((j->n ? j->n : 1) * sizeof(void *));
    size_t *ls = malloc((j->n ? j->n : 1) * sizeof(size_t));
    size_t *which = malloc((j->n ? j->n : 1) * sizeof(size_t));
    int *st = malloc((j->n ? j->n : 1) * sizeof(int));
    uint8_t *recs = malloc((j->n ? j->n : 1) * LMC_COMMIT_BYTES);
    for (size_t k = 0; k < j->n; k++) {
        if (!chunk_ok[k]) continue;
        uint8_t *rec = recs + m * LMC_COMMIT_BYTES;
        memcpy(rec, LMC_COMMIT_MAGIC, 8);
        put_u64(rec + 8, j->lens[k]);
        put_u64(rec + 16, c->part);
        ks[m] = j->keys[k];
        vs[m] = rec;
        ls[m] = LMC_COMMIT_BYTES;
        which[m++] = k;
    }
    if (m) {
        rpkv_mput(c->kv, m, ks, vs, ls, st);
        for (size_t i = 0; i < m; i++) {
            if (st[i]) { set_err(j, "commit", st[i], 1, 1); continue; }
            j->res[which[i]] = 1;
            atomic_fetch_add(&c->bytes_set, j->lens[which[i]]);
        }
    }
    j->ok = 1;
    for (size_t k = 0; k < j->n; k++) if (!j->res[k]) j->ok = 0;
    free(chunk_ok); free(ks); free(vs); free(ls); free(which); free(st); free(recs);
    parts_free(&p);
}

static void run_get(rplmc_t *c, job_t *j) {
    parts_t p = {0};
    int rc = parts_build(c, j, &p);
    if (rc) { set_err(j, "get", rc, j->n, j->n); parts_free(&p); return; }
    parts_run(c, &p, 0);
    for (size_t k = 0; k < j->n; k++) j->res[k] = 1;
    size_t bad = 0;
    int first = 0;
    for (size_t q = 0; q < p.n; q++)
        if (p.st[q]) {
            j->res[p.owner[q]] = 0;
            if (p.st[q] != -ENOENT) { bad++; if (!first) first = p.st[q]; }
        }
    if (bad) { atomic_fetch_add(&c->parts_failed, bad); set_err(j, "get", first, bad, p.n); }
    j->ok = 1;
    for (size_t k = 0; k < j->n; k++) {
        if (j->res[k]) atomic_fetch_add(&c->bytes_get, j->lens[k]);
        else j->ok = 0;
    }
    parts_free(&p);
}

static void run_exists(rplmc_t *c, job_t *j) {
    int *present = calloc(j->n ? j->n : 1, sizeof(int));
    int rc = rpkv_mcontains(c->kv, j->n, (const char *const *)j->keys, present);
    if (rc) { set_err(j, "exists", rc, j->n, j->n); free(present); return; }
    for (size_t k = 0; k < j->n; k++) j->res[k] = present[k] == 1;
    j->ok = 1;
    free(present);
}

static void run_delete(rplmc_t *c, job_t *j) {
    size_t n = j->n;
    void **vals = calloc(n ? n : 1, sizeof(void *));
    size_t *lens = calloc(n ? n : 1, sizeof(size_t));
    int *st = calloc(n ? n : 1, sizeof(int));
    size_t *np = calloc(n ? n : 1, sizeof(size_t));
    /* how many parts each chunk has: its commit record */
    rpkv_mget(c->kv, n, (const char *const *)j->keys, vals, lens, st);
    for (size_t k = 0; k < n; k++) {
        if (!st[k] && lens[k] == LMC_COMMIT_BYTES && !memcmp(vals[k], LMC_COMMIT_MAGIC, 8)) {
            const uint8_t *rec = vals[k];
            uint64_t part = get_u64(rec + 16);
            np[k] = part ? nparts((size_t)get_u64(rec + 8), (size_t)part) : 0;
        }
        rpkv_free(vals[k]);
    }
    /* commit first (the chunk stops existing), then its parts */
    rpkv_mdelete(c->kv, n, (const char *const *)j->keys, st);
    size_t total = 0;
    for (size_t k = 0; k < n; k++) {
        j->res[k] = st[k] == 0;
        if (st[k] && st[k] != -ENOENT) set_err(j, "delete", st[k], 1, 1);
        total += j->res[k] ? np[k] : 0;
    }
    if (total) {
        char **pk = calloc(total, sizeof(char *));
        int *pst = calloc(total, sizeof(int));
        size_t q = 0;
        for (size_t k = 0; k < n; k++)
            for (size_t x = 0; j->res[k] && x < np[k]; x++) pk[q++] = part_key(j->keys[k], x);
        rpkv_mdelete(c->kv, q, (const char *const *)pk, pst);
        for (size_t i = 0; i < q; i++) free(pk[i]);
        free(pk);
        free(pst);
    }
    j->ok = 1;
    free(vals); free(lens); free(st); free(np);
}

/* ---------------------------------------------------------------- queue */
static void job_free(job_t *j) {
    if (!j) return;
    for (size_t k = 0; j->keys && k < j->n; k++) free(j->keys[k]);
    free(j->keys); free(j->bufs); free(j->lens); free(j->res);
    free(j);
}

static void *worker(void *arg) {
    rplmc_t *c = arg;
    for (;;) {
        pthread_mutex_lock(&c->lk);
        while (!c->qhead && !c->stop) pthread_cond_wait(&c->cv, &c->lk);
        job_t *j = c->qhead;
        if (!j) { pthread_mutex_unlock(&c->lk); return NULL; }  /* stopping, queue empty */
        c->qhead = j->next;
        if (!c->qhead) c->qtail = NULL;
        pthread_mutex_unlock(&c->lk);

        switch (j->op) {
        case RPLMC_SET: run_set(c, j); break;
        case RPLMC_GET: run_get(c, j); break;
        case RPLMC_EXISTS: run_exists(c, j); break;
        default: run_delete(c, j); break;
        }
        atomic_fetch_add(&c->ops, 1);
        atomic_fetch_add(&c->keys, j->n);

        j->next = NULL;
        pthread_mutex_lock(&c->lk);
        if (c->dtail) c->dtail->next = j; else c->dhead = j;
        c->dtail = j;
        pthread_mutex_unlock(&c->lk);
        uint64_t one = 1;
        ssize_t w = write(c->efd, &one, sizeof(one));
        (void)w;
    }
}

rplmc_t *rplmc_open(const char *osts, const char *ns, int workers, size_t part_bytes) {
    rpkv_t *kv = rpkv_open(osts, ns ? ns : "lmcache/");
    if (!kv) return NULL;
    rplmc_t *c = calloc(1, sizeof(*c));
    if (!c) { rpkv_close(kv); return NULL; }
    c->kv = kv;
    size_t cap = rp_frame_bytes / 2 > 8192 ? rp_frame_bytes / 2 - 4096 : 4096;
    c->part = part_bytes ? part_bytes : (size_t)4 << 20;
    if (c->part > cap) c->part = cap;
    c->efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    pthread_mutex_init(&c->lk, NULL);
    pthread_cond_init(&c->cv, NULL);
    pthread_rwlock_init(&c->reglk, NULL);
    c->nworkers = workers > 0 ? workers : 4;
    c->th = calloc((size_t)c->nworkers, sizeof(pthread_t));
    if (c->efd < 0 || !c->th) { rplmc_close(c); return NULL; }
    for (int i = 0; i < c->nworkers; i++)
        if (pthread_create(&c->th[i], NULL, worker, c)) { c->nworkers = i; break; }
    if (!c->nworkers) { rplmc_close(c); return NULL; }
    return c;
}

void rplmc_close(rplmc_t *c) {
    if (!c) return;
    pthread_mutex_lock(&c->lk);
    c->stop = 1;
    pthread_cond_broadcast(&c->cv);
    pthread_mutex_unlock(&c->lk);
    for (int i = 0; i < c->nworkers; i++) pthread_join(c->th[i], NULL);
    free(c->th);
    for (job_t *j = c->qhead, *nx; j; j = nx) { nx = j->next; job_free(j); }  /* no workers ever started */
    for (job_t *j = c->dhead, *nx; j; j = nx) { nx = j->next; job_free(j); }
    for (int i = 0; i < c->nreg; i++) rpkv_dev_deregister(c->reg[i].d);
    if (c->efd >= 0) close(c->efd);
    rpkv_close(c->kv);
    pthread_mutex_destroy(&c->lk);
    pthread_cond_destroy(&c->cv);
    pthread_rwlock_destroy(&c->reglk);
    free(c);
}

int rplmc_event_fd(const rplmc_t *c) { return c ? c->efd : -EINVAL; }

void rplmc_ack(rplmc_t *c) {
    uint64_t v;
    ssize_t r = read(c->efd, &v, sizeof(v));
    (void)r;
}

int rplmc_register(rplmc_t *c, void *base, size_t len) {
    if (!c || !base || !len) return -EINVAL;
    uint8_t *b = base;
    pthread_rwlock_wrlock(&c->reglk);
    for (int i = 0; i < c->nreg; i++)
        if (c->reg[i].base == b && c->reg[i].len == len) {
            int direct = rpkv_dev_direct(c->reg[i].d);
            pthread_rwlock_unlock(&c->reglk);
            return direct;
        }
    if (c->nreg == LMC_MAX_REGIONS) { pthread_rwlock_unlock(&c->reglk); return -ENOSPC; }
    rpkv_dev_t *d = rpkv_host_register(c->kv, base, len);
    if (!d) { pthread_rwlock_unlock(&c->reglk); return -ENOMEM; }
    c->reg[c->nreg++] = (region_t){b, len, d};
    int direct = rpkv_dev_direct(d);
    pthread_rwlock_unlock(&c->reglk);
    return direct;
}

int64_t rplmc_submit(rplmc_t *c, int op, size_t n, const char *const *keys, void *const *bufs, const size_t *lens) {
    if (!c || op < RPLMC_GET || op > RPLMC_DELETE || (n && !keys)) return -EINVAL;
    int data = op == RPLMC_GET || op == RPLMC_SET;
    if (data && n && (!bufs || !lens)) return -EINVAL;
    job_t *j = calloc(1, sizeof(*j));
    if (!j) return -ENOMEM;
    j->op = op;
    j->n = n;
    j->keys = calloc(n ? n : 1, sizeof(char *));
    j->res = calloc(n ? n : 1, 1);
    if (data) {
        j->bufs = calloc(n ? n : 1, sizeof(uint8_t *));
        j->lens = calloc(n ? n : 1, sizeof(size_t));
    }
    if (!j->keys || !j->res || (data && (!j->bufs || !j->lens))) { job_free(j); return -ENOMEM; }
    for (size_t k = 0; k < n; k++) {
        if (!keys[k] || (data && !bufs[k] && lens[k])) { job_free(j); return -EINVAL; }
        j->keys[k] = strdup(keys[k]);
        if (!j->keys[k]) { job_free(j); return -ENOMEM; }
        if (data) { j->bufs[k] = bufs[k]; j->lens[k] = lens[k]; }
    }
    pthread_mutex_lock(&c->lk);
    if (c->stop) { pthread_mutex_unlock(&c->lk); job_free(j); return -ESHUTDOWN; }
    j->id = c->next_id++;
    if (c->qtail) c->qtail->next = j; else c->qhead = j;
    c->qtail = j;
    pthread_cond_signal(&c->cv);
    int64_t id = (int64_t)j->id;
    pthread_mutex_unlock(&c->lk);
    return id;
}

int rplmc_pop(rplmc_t *c, uint64_t *id, int *op, int *ok, size_t *n, uint8_t *results, size_t cap, char *err,
              size_t errcap) {
    pthread_mutex_lock(&c->lk);
    job_t *j = c->dhead;
    if (!j) { pthread_mutex_unlock(&c->lk); return 0; }
    if (j->n > cap) { *n = j->n; pthread_mutex_unlock(&c->lk); return -ENOSPC; }
    c->dhead = j->next;
    if (!c->dhead) c->dtail = NULL;
    pthread_mutex_unlock(&c->lk);
    *id = j->id;
    *op = j->op;
    *ok = j->ok;
    *n = j->n;
    if (j->n) memcpy(results, j->res, j->n);
    if (err && errcap) snprintf(err, errcap, "%s", j->err);
    job_free(j);
    return 1;
}

void rplmc_stats(rplmc_t *c, uint64_t out[8]) {
    uint64_t direct = 0, bounced = 0;
    rpkv_dev_stats(c->kv, &direct, &bounced, NULL, NULL);
    out[0] = atomic_load(&c->ops);
    out[1] = atomic_load(&c->keys);
    out[2] = atomic_load(&c->bytes_set);
    out[3] = atomic_load(&c->bytes_get);
    out[4] = direct;
    out[5] = bounced + atomic_load(&c->copied);
    out[6] = atomic_load(&c->parts_failed);
    pthread_rwlock_rdlock(&c->reglk);
    out[7] = (uint64_t)c->nreg;
    pthread_rwlock_unlock(&c->reglk);
}
