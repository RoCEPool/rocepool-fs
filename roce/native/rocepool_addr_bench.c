/* rocepool_addr_bench.c -- reading a KV-cache block by key vs by address.
 *
 * What would a global address layer (one address space over the memory of
 * all the OSTs, an address telling the server and the offset) change for
 * reads? This driver reads the same blocks, stored on one OST, four ways:
 *
 *   rpc      by key, RPC: the OST looks the key up and sends the value in its
 *            reply (what rpkv_get does: rp_ost_read_at + a copy out)
 *   lease    by key, one-sided, no cache: a LEASE RPC (where the value sits:
 *            address, rkey, seq) then the one-sided read (what rpkv_get_dev
 *            does for a key it has not read before)
 *   cached   by key, one-sided, lease cached: the one-sided read only, as
 *            rpkv_get_dev does for a key read before -- header, data and
 *            trailer in one RDMA READ chain, the seq bracketing the data
 *   addr     by address: the data alone, one RDMA READ at a known address --
 *            what a global address layer would do (no lookup, no check that
 *            the block is still there: the address is the contract)
 *
 * Usage: rocepool-addr-bench OST block_bytes blocks threads seconds
 * Prints one line per mode: RESULT {json}.
 */
#define _GNU_SOURCE
#include "rocepool_client.h"

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

enum { M_RPC, M_LEASE, M_CACHED, M_ADDR, M_COUNT };
static const char *mode_name[M_COUNT] = {"rpc", "lease", "cached", "addr"};

static rp_peer_t g_peer;
static size_t g_bytes;
static int g_blocks;
static rp_lease_t *g_leases;
static char (*g_keys)[64];

typedef struct {
    int mode, id;
    double until;
    uint64_t ops, errors;
    double *lat;
    size_t nlat, cap;
} job_t;

static void *run(void *arg) {
    job_t *j = arg;
    void *dst = rp_bulk_alloc(g_bytes);
    roce_mem_t *mem = dst ? rp_bulk_mem(dst, g_bytes) : NULL;
    if (!dst || !mem) { j->errors++; return NULL; }
    int i = j->id;
    while (now_s() < j->until) {
        int b = i++ % g_blocks;
        double t0 = now_s();
        int rc = 0;
        if (j->mode == M_RPC) {
            rp_resp_t r;
            uint64_t ver;
            rc = rp_ost_read_at(&g_peer, g_keys[b], 0, 0, 1, &ver, &r);
            if (!rc) {
                if (r.vlen - 8 == g_bytes) memcpy(dst, r.val + 8, g_bytes);
                else rc = -EIO;
                rp_resp_free(&r);
            }
        } else if (j->mode == M_LEASE) {
            rp_lease_t l;
            rc = rp_ost_lease(&g_peer, g_keys[b], &l);
            if (!rc) rc = rp_ost_read_direct_mem(&g_peer, &l, 0, g_bytes, dst, mem);
        } else if (j->mode == M_CACHED) {
            rc = rp_ost_read_direct_mem(&g_peer, &g_leases[b], 0, g_bytes, dst, mem);
        } else {
            rp_read_t rd = {g_leases[b].addr + 32, g_leases[b].rkey, (uint32_t)g_bytes, mem, dst};
            rc = rp_rdma_read(&g_peer, &rd, 1);
        }
        double us = (now_s() - t0) * 1e6;
        if (rc) { j->errors++; continue; }
        /* every block starts with its number: the right block was read */
        if (*(uint32_t *)dst != (uint32_t)b) { j->errors++; continue; }
        j->ops++;
        if (j->nlat == j->cap) {
            j->cap = j->cap ? 2 * j->cap : 4096;
            j->lat = realloc(j->lat, j->cap * sizeof(double));
        }
        j->lat[j->nlat++] = us;
    }
    rp_bulk_free(dst);
    return NULL;
}

static int cmp_d(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

int main(int argc, char **argv) {
    if (argc != 6) {
        fprintf(stderr, "usage: %s OST block_bytes blocks threads seconds\n", argv[0]);
        return 2;
    }
    rp_peer_init(&g_peer, argv[1]);
    g_bytes = strtoull(argv[2], NULL, 0);
    g_blocks = atoi(argv[3]);
    int threads = atoi(argv[4]);
    double secs = atof(argv[5]);
    if (g_bytes < 8 || g_blocks < 1 || threads < 1 || secs <= 0) return 2;
    g_keys = calloc((size_t)g_blocks, sizeof(*g_keys));
    g_leases = calloc((size_t)g_blocks, sizeof(*g_leases));
    uint8_t *val = malloc(g_bytes);
    for (int b = 0; b < g_blocks; b++) {
        snprintf(g_keys[b], sizeof(g_keys[b]), "kv/addrbench/%zu/%d", g_bytes, b);
        for (size_t k = 0; k < g_bytes; k++) val[k] = (uint8_t)(k * 131 + b);
        *(uint32_t *)val = (uint32_t)b;
        if (rp_ost_put(&g_peer, g_keys[b], val, g_bytes, -1, NULL)) {
            fprintf(stderr, "put %s failed\n", g_keys[b]);
            return 1;
        }
        int rc = rp_ost_lease(&g_peer, g_keys[b], &g_leases[b]);
        if (rc) {
            fprintf(stderr, "lease %s: %d (this OST has no one-sided read path)\n", g_keys[b], rc);
            return 1;
        }
    }
    free(val);
    for (int m = 0; m < M_COUNT; m++) {
        job_t *jobs = calloc((size_t)threads, sizeof(job_t));
        pthread_t *th = calloc((size_t)threads, sizeof(pthread_t));
        /* a short warm-up: connections made, buffers registered */
        double start = now_s();
        for (int t = 0; t < threads; t++) {
            jobs[t] = (job_t){.mode = m, .id = t, .until = start + 0.3};
            pthread_create(&th[t], NULL, run, &jobs[t]);
        }
        for (int t = 0; t < threads; t++) { pthread_join(th[t], NULL); free(jobs[t].lat); }
        start = now_s();
        for (int t = 0; t < threads; t++) {
            jobs[t] = (job_t){.mode = m, .id = t, .until = start + secs};
            pthread_create(&th[t], NULL, run, &jobs[t]);
        }
        uint64_t ops = 0, errors = 0;
        size_t n = 0;
        for (int t = 0; t < threads; t++) {
            pthread_join(th[t], NULL);
            ops += jobs[t].ops;
            errors += jobs[t].errors;
            n += jobs[t].nlat;
        }
        double elapsed = now_s() - start;
        double *all = malloc((n ? n : 1) * sizeof(double));
        size_t k = 0;
        for (int t = 0; t < threads; t++) {
            memcpy(all + k, jobs[t].lat, jobs[t].nlat * sizeof(double));
            k += jobs[t].nlat;
            free(jobs[t].lat);
        }
        qsort(all, n, sizeof(double), cmp_d);
        printf("RESULT {\"mode\": \"%s\", \"block_bytes\": %zu, \"threads\": %d, \"ops_per_s\": %.0f, "
               "\"mib_per_s\": %.1f, \"p50_us\": %.1f, \"p99_us\": %.1f, \"errors\": %llu}\n",
               mode_name[m], g_bytes, threads, ops / elapsed, ops * (double)g_bytes / elapsed / 1048576.0,
               n ? all[n / 2] : 0.0, n ? all[(size_t)(n * 0.99)] : 0.0, (unsigned long long)errors);
        fflush(stdout);
        free(all);
        free(jobs);
        free(th);
    }
    return 0;
}
