/* rocepool_kvcache_bench.c -- KV-cache reuse through rocepool, the part of
 * time-to-first-token (TTFT) that storage decides.
 *
 * A prefix-caching / prefill-decode-disaggregated server stores the KV
 * cache of every prompt it prefilled; a later request (or the decode node)
 * whose prompt starts with a stored prefix looks up how many leading blocks
 * are stored and loads them into GPU memory instead of recomputing them.
 * TTFT = lookup + load of the cached prefix + prefill of the rest. This
 * driver measures the first two for a model's KV-cache geometry:
 *
 *   store   every distinct prefix's blocks, put from GPU memory (what
 *           prefill writes, off the critical path)
 *   reuse   R requests, C at a time: rpkv_mcontains on the prompt's block
 *           keys (one BATCH round trip per OST), then rpkv_mget_dev of all
 *           its cached blocks into the request's GPU buffer
 *
 * Usage: rocepool-kvcache-bench OSTS key=value...
 *   layers=32 kv_heads=8 head_dim=128 dtype_bytes=2   (model; Llama-3-8B)
 *   block_tokens=16 granularity=block|layer           (key per block of all layers, or per block per layer)
 *   prompt_tokens=2048 prefix_tokens=1536 prefixes=4 requests=32 concurrency=4 repeat=2
 *   mode=gpu|host                                     (gpu: device memory, *_dev calls; host: rpkv_mget)
 *   device=0                                          (CUDA device; ROCEPOOL_DEV_EMU=1: the emulator)
 * Prints one line: RESULT {json}.
 */
#define _GNU_SOURCE
#include "rocepool_devmem.h"
#include "rocepool_kv.h"

#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* ---- device memory for the benchmark: the emulator, or CUDA ---- */
static int g_emu;
static struct {
    int (*init)(unsigned);
    int (*dev_get)(int *, int);
    int (*retain)(void **, int);
    int (*set)(void *);
    int (*alloc)(unsigned long long *, size_t);
    int (*htod)(unsigned long long, const void *, size_t);
    int (*dtoh)(void *, unsigned long long, size_t);
} cu;
static void *g_ctx;
static int dev_setup(int device) {
    const char *e = getenv("ROCEPOOL_DEV_EMU");
    g_emu = e && !strcmp(e, "1");
    if (g_emu) return 0;
    void *h = dlopen("libcuda.so.1", RTLD_NOW | RTLD_GLOBAL);
    if (!h) return -1;
    *(void **)&cu.init = dlsym(h, "cuInit");
    *(void **)&cu.dev_get = dlsym(h, "cuDeviceGet");
    *(void **)&cu.retain = dlsym(h, "cuDevicePrimaryCtxRetain");
    *(void **)&cu.set = dlsym(h, "cuCtxSetCurrent");
    *(void **)&cu.alloc = dlsym(h, "cuMemAlloc_v2");
    *(void **)&cu.htod = dlsym(h, "cuMemcpyHtoD_v2");
    *(void **)&cu.dtoh = dlsym(h, "cuMemcpyDtoH_v2");
    int d = 0;
    if (!cu.init || !cu.alloc || cu.init(0) || cu.dev_get(&d, device) || cu.retain(&g_ctx, d) || cu.set(g_ctx))
        return -1;
    return 0;
}
static void dev_thread(void) { if (!g_emu && cu.set) cu.set(g_ctx); }
static void *dev_alloc(size_t n) {
    if (g_emu) return rp_devemu_alloc(n);
    unsigned long long p = 0;
    return cu.alloc(&p, n) ? NULL : (void *)(uintptr_t)p;
}
static int dev_to(void *d, const void *h, size_t n) {
    return g_emu ? rp_devemu_copy_to(d, h, n) : cu.htod((unsigned long long)(uintptr_t)d, h, n);
}
static int dev_from(void *h, const void *d, size_t n) {
    return g_emu ? rp_devemu_copy_from(h, d, n) : cu.dtoh(h, (unsigned long long)(uintptr_t)d, n);
}

/* ---- parameters ---- */
static long P(int argc, char **argv, const char *name, long dflt) {
    size_t n = strlen(name);
    for (int i = 2; i < argc; i++)
        if (!strncmp(argv[i], name, n) && argv[i][n] == '=') return atol(argv[i] + n + 1);
    return dflt;
}
static const char *S(int argc, char **argv, const char *name, const char *dflt) {
    size_t n = strlen(name);
    for (int i = 2; i < argc; i++)
        if (!strncmp(argv[i], name, n) && argv[i][n] == '=') return argv[i] + n + 1;
    return dflt;
}

typedef struct {
    rpkv_t *kv;
    int gpu, per_layer;
    long layers, blocks_prefix, blocks_prompt, prefixes, requests;
    size_t val_bytes, nkeys_req;
    atomic_long next;
    double *lat, *lookup;      /* per request: total, lookup part (s) */
    atomic_long errors, misses;
    long repeat;
} job_t;

/* the key of prefix p's block b (layer l when per layer): as in vLLM / LMCache
 * a block's key names its prefix chain, so equal prefixes share blocks */
static void key_of(char *out, size_t n, long p, long b, long l, int per_layer) {
    if (per_layer) snprintf(out, n, "kvc/p%ld/b%ld/l%ld", p, b, l);
    else snprintf(out, n, "kvc/p%ld/b%ld", p, b);
}

/* a value's bytes: a pattern of its key, so a load is checked against it */
static void fill(uint8_t *buf, size_t n, const char *key) {
    uint64_t x = 1469598103934665603ULL;
    for (const char *c = key; *c; c++) x = (x ^ (uint8_t)*c) * 1099511628211ULL;
    for (size_t i = 0; i < n; i++) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; buf[i] = (uint8_t)x; }
}

typedef struct { job_t *j; int w; } worker_t;

static void *reuse_worker(void *arg) {
    worker_t *wk = arg;
    job_t *j = wk->j;
    dev_thread();
    size_t n = j->nkeys_req;
    char **keys = calloc(n, sizeof(char *));
    for (size_t k = 0; k < n; k++) keys[k] = malloc(96);
    size_t *offs = calloc(n, sizeof(size_t)), *caps = calloc(n, sizeof(size_t)), *lens = calloc(n, sizeof(size_t));
    int *st = calloc(n, sizeof(int)), *present = calloc(n, sizeof(int));
    void **hv = calloc(n, sizeof(void *));
    size_t bytes = n * j->val_bytes;
    uint8_t *buf = j->gpu ? dev_alloc(bytes) : NULL;
    rpkv_dev_t *d = buf ? rpkv_dev_register(j->kv, buf, bytes) : NULL;
    if (j->gpu && !d) { fprintf(stderr, "kvcache-bench: device buffer of %zu bytes: no\n", bytes); exit(3); }
    for (size_t k = 0; k < n; k++) { offs[k] = k * j->val_bytes; caps[k] = j->val_bytes; }
    uint8_t *want = malloc(j->val_bytes), *got = malloc(j->val_bytes);
    for (;;) {
        long r = atomic_fetch_add(&j->next, 1);
        if (r >= j->requests * j->repeat) break;
        long p = r % j->prefixes;
        size_t k = 0;
        for (long b = 0; b < j->blocks_prompt; b++)
            for (long l = 0; l < (j->per_layer ? j->layers : 1); l++) key_of(keys[k++], 96, p, b, l, j->per_layer);
        double t0 = now_s();
        /* how many leading blocks are cached: the lookup a scheduler does
         * before it decides what to prefill */
        rpkv_mcontains(j->kv, n, (const char *const *)keys, present);
        size_t hit = 0;
        while (hit < n && present[hit]) hit++;
        double t1 = now_s();
        if (j->gpu) rpkv_mget_dev(j->kv, hit, (const char *const *)keys, d, offs, caps, lens, st);
        else rpkv_mget(j->kv, hit, (const char *const *)keys, hv, lens, st);
        double t2 = now_s();
        j->lat[r] = t2 - t0;
        j->lookup[r] = t1 - t0;
        if (hit != (size_t)(j->blocks_prefix * (j->per_layer ? j->layers : 1))) atomic_fetch_add(&j->misses, 1);
        for (size_t q = 0; q < hit; q++)
            if (st[q] || lens[q] != j->val_bytes) atomic_fetch_add(&j->errors, 1);
        /* check one block of every request against its pattern */
        size_t q = (size_t)r % (hit ? hit : 1);
        if (hit) {
            fill(want, j->val_bytes, keys[q]);
            const uint8_t *src = got;
            if (j->gpu) dev_from(got, buf + offs[q], j->val_bytes);
            else src = hv[q];
            if (!src || memcmp(src, want, j->val_bytes)) atomic_fetch_add(&j->errors, 1);
        }
        if (!j->gpu) for (size_t x = 0; x < hit; x++) { rpkv_free(hv[x]); hv[x] = NULL; }
    }
    rpkv_dev_deregister(d);
    return NULL;
}

static int cmpd(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

int main(int argc, char **argv) {
    if (argc < 2) { fprintf(stderr, "usage: %s OSTS key=value...\n", argv[0]); return 2; }
    long layers = P(argc, argv, "layers", 32), heads = P(argc, argv, "kv_heads", 8);
    long hdim = P(argc, argv, "head_dim", 128), dbytes = P(argc, argv, "dtype_bytes", 2);
    long btok = P(argc, argv, "block_tokens", 16), ptok = P(argc, argv, "prompt_tokens", 2048);
    long pfx = P(argc, argv, "prefix_tokens", 1536), nprefix = P(argc, argv, "prefixes", 4);
    long reqs = P(argc, argv, "requests", 32), conc = P(argc, argv, "concurrency", 4);
    long repeat = P(argc, argv, "repeat", 2);
    int per_layer = !strcmp(S(argc, argv, "granularity", "block"), "layer");
    int gpu = !strcmp(S(argc, argv, "mode", "gpu"), "gpu");
    if (gpu && dev_setup((int)P(argc, argv, "device", 0))) { fprintf(stderr, "kvcache-bench: no CUDA device\n"); return 3; }
    size_t layer_block = (size_t)(2 * heads * hdim * dbytes * btok);   /* K and V of one block, one layer */
    size_t val = per_layer ? layer_block : layer_block * (size_t)layers;
    long bp = pfx / btok, bq = ptok / btok;
    if (bp < 1 || bq < bp || nprefix < 1 || reqs < 1 || conc < 1) { fprintf(stderr, "bad shape\n"); return 2; }
    rpkv_t *kv = rpkv_open(argv[1], "kvcache/");
    if (!kv) return 4;
    size_t per_req = (size_t)bq * (per_layer ? (size_t)layers : 1);

    /* store: every prefix's blocks (the cached part), from GPU memory */
    size_t nstore = (size_t)bp * (per_layer ? (size_t)layers : 1);
    char **keys = calloc(nstore, sizeof(char *));
    for (size_t k = 0; k < nstore; k++) keys[k] = malloc(96);
    size_t *offs = calloc(nstore, sizeof(size_t)), *lens = calloc(nstore, sizeof(size_t));
    int *st = calloc(nstore, sizeof(int));
    uint8_t *host = malloc(nstore * val);
    uint8_t *sbuf = gpu ? dev_alloc(nstore * val) : NULL;
    rpkv_dev_t *sd = sbuf ? rpkv_dev_register(kv, sbuf, nstore * val) : NULL;
    if (gpu && !sd) { fprintf(stderr, "kvcache-bench: store buffer: no device memory\n"); return 3; }
    double store_s = 0;
    long store_err = 0;
    for (long p = 0; p < nprefix; p++) {
        size_t k = 0;
        for (long b = 0; b < bp; b++)
            for (long l = 0; l < (per_layer ? layers : 1); l++) {
                key_of(keys[k], 96, p, b, l, per_layer);
                offs[k] = k * val;
                lens[k] = val;
                fill(host + offs[k], val, keys[k]);
                k++;
            }
        if (gpu) dev_to(sbuf, host, nstore * val);
        double t0 = now_s();
        if (gpu) {
            rpkv_mput_dev(kv, nstore, (const char *const *)keys, sd, offs, lens, st);
        } else {
            const void **vals = calloc(nstore, sizeof(void *));
            for (size_t q = 0; q < nstore; q++) vals[q] = host + offs[q];
            rpkv_mput(kv, nstore, (const char *const *)keys, (const void *const *)vals, lens, st);
            free(vals);
        }
        store_s += now_s() - t0;
        for (size_t q = 0; q < nstore; q++) if (st[q]) store_err++;
    }
    uint64_t s_direct = 0, s_bounced = 0;
    rpkv_dev_stats(kv, &s_direct, &s_bounced, NULL, NULL);

    /* reuse: R requests (x repeat), C at a time */
    job_t j = {kv, gpu, per_layer, layers, bp, bq, nprefix, reqs, val, per_req, 0, NULL, NULL, 0, 0, repeat};
    j.lat = calloc((size_t)(reqs * repeat), sizeof(double));
    j.lookup = calloc((size_t)(reqs * repeat), sizeof(double));
    pthread_t *th = calloc((size_t)conc, sizeof(pthread_t));
    worker_t *wk = calloc((size_t)conc, sizeof(worker_t));
    double t0 = now_s();
    for (long w = 0; w < conc; w++) {
        wk[w] = (worker_t){&j, (int)w};
        pthread_create(&th[w], NULL, reuse_worker, &wk[w]);
    }
    for (long w = 0; w < conc; w++) pthread_join(th[w], NULL);
    double wall = now_s() - t0;
    long nr = reqs * repeat;
    double *lat = malloc((size_t)nr * sizeof(double)), *lk = malloc((size_t)nr * sizeof(double));
    memcpy(lat, j.lat, (size_t)nr * sizeof(double));
    memcpy(lk, j.lookup, (size_t)nr * sizeof(double));
    qsort(lat, (size_t)nr, sizeof(double), cmpd);
    qsort(lk, (size_t)nr, sizeof(double), cmpd);
    uint64_t direct = 0, bounced = 0, leases = 0, hits = 0;
    rpkv_dev_stats(kv, &direct, &bounced, &leases, &hits);
    size_t req_bytes = (size_t)bp * (per_layer ? (size_t)layers : 1) * val;
    printf("RESULT {\"mode\": \"%s\", \"granularity\": \"%s\", \"value_bytes\": %zu, \"keys_per_request\": %zu, "
           "\"cached_bytes_per_request\": %zu, \"store_gbps\": %.3f, \"store_errors\": %ld, "
           "\"requests\": %ld, \"concurrency\": %ld, \"load_ms_p50\": %.3f, \"load_ms_p99\": %.3f, "
           "\"lookup_ms_p50\": %.3f, \"load_gbps\": %.3f, \"errors\": %ld, \"misses\": %ld, "
           "\"direct_bytes\": %llu, \"bounced_bytes\": %llu, \"store_direct_bytes\": %llu, "
           "\"leases\": %llu, \"lease_hits\": %llu, \"rpcs\": %llu, \"direct_capable\": %d}\n",
           gpu ? "gpu" : "host", per_layer ? "layer" : "block", val, (size_t)bp * (per_layer ? (size_t)layers : 1),
           req_bytes, store_s > 0 ? (double)(nstore * val * (size_t)nprefix) / store_s / 1e9 : 0.0, store_err,
           nr, conc, lat[nr / 2] * 1e3, lat[(size_t)(nr * 0.99)] * 1e3, lk[nr / 2] * 1e3,
           (double)req_bytes * (double)nr / wall / 1e9, (long)j.errors, (long)j.misses,
           (unsigned long long)(direct - s_direct), (unsigned long long)(bounced - s_bounced),
           (unsigned long long)s_direct, (unsigned long long)leases, (unsigned long long)hits,
           (unsigned long long)rpkv_rpcs(kv), sd ? rpkv_dev_direct(sd) : 0);
    rpkv_dev_deregister(sd);
    rpkv_close(kv);
    return j.errors || store_err ? 1 : 0;
}
