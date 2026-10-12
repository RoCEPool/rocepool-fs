/* rocepool_kv.c -- native KV client API (rocepool_kv.h) on the shared
 * client core (rocepool_client.c). Build with -DRPKV_BENCH for the
 * rocepool-kv-bench driver (main at the bottom). */
#define _GNU_SOURCE
#include "rocepool_kv.h"
#include "rocepool_client.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define KV_MAX_OSTS 256

struct rpkv {
    rp_peer_t ost[KV_MAX_OSTS];
    int n;
    char ns[128];
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
    return kv;
}

void rpkv_close(rpkv_t *kv) {
    if (!kv) return;
    for (int i = 0; i < kv->n; i++) rp_peer_close_idle(&kv->ost[i]);
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
