/* tests/test_wal.py's driver for roce/native/wal.c.
 *   wal_driver write PATH THREADS PER_THREAD   records "t<k>:<i>:<pad>" (sizes vary), each waited for
 *   wal_driver dump PATH                       replays: one line per record
 *   wal_driver stale PATH GAP N                after the log's end: GAP zero bytes, then N valid
 *                                              records "stale:<i>" (a crash's out-of-order commits)
 *   wal_driver big PATH                        a 3 MiB record: refused
 * Prints the log's stats after write. */
#define _GNU_SOURCE
#include "wal.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static wal_t *W;
static int PER;

static void *writer(void *arg) {
    long k = (long)arg;
    char rec[6000];
    for (int i = 0; i < PER; i++) {
        int pad = (int)((k * 7919 + i * 104729) % 4000);
        int n = snprintf(rec, sizeof(rec), "t%ld:%d:", k, i);
        memset(rec + n, 'a' + (int)(k % 26), (size_t)pad);
        uint64_t lsn = wal_append(W, rec, (size_t)n + (size_t)pad, NULL, 0, NULL, 0);
        if (!lsn || wal_wait(W, lsn)) { fprintf(stderr, "append/wait failed\n"); exit(2); }
    }
    return NULL;
}

static int dump_cb(void *u, const uint8_t *rec, size_t len) {
    (void)u;
    size_t n = 0;
    while (n < len && rec[n] != ':') n++;
    size_t m = n + 1;
    while (m < len && rec[m] != ':') m++;
    /* name, index, payload length, payload all one letter */
    int same = 1;
    for (size_t j = m + 1; j < len; j++) if (rec[j] != rec[m + 1]) same = 0;
    printf("%.*s %zu %d\n", (int)m, (const char *)rec, len - m - 1, same);
    return 0;
}

static void put_u32(uint8_t *p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (8 * i)); }
static uint32_t crc(const uint8_t *p, size_t n) {
    uint32_t c = ~0u;
    while (n--) { c ^= *p++; for (int k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320u ^ (c >> 1) : c >> 1; }
    return ~c;
}

int main(int argc, char **argv) {
    if (argc < 3) return 1;
    const char *cmd = argv[1], *path = argv[2];
    char err[256];
    if (!strcmp(cmd, "write")) {
        if (wal_replay(path, NULL, NULL) < 0) return 3;
        W = wal_open(path, err, sizeof(err));
        if (!W) { fprintf(stderr, "%s\n", err); return 3; }
        int threads = atoi(argv[3]);
        PER = atoi(argv[4]);
        pthread_t t[64];
        for (long k = 0; k < threads; k++) pthread_create(&t[k], NULL, writer, (void *)k);
        for (int k = 0; k < threads; k++) pthread_join(t[k], NULL);
        uint64_t r, c, b, ns, infl, pre;
        int direct, pipe;
        wal_stats(W, &r, &c, &b, &ns);
        wal_stats_more(W, &direct, &pipe, &infl, &pre);
        printf("records %llu commits %llu direct %d pipeline %d inflight_max %llu prefill %llu\n",
               (unsigned long long)r, (unsigned long long)c, direct, pipe, (unsigned long long)infl,
               (unsigned long long)pre);
        return 0;
    }
    if (!strcmp(cmd, "dump")) {
        long n = wal_replay(path, dump_cb, NULL);
        printf("replayed %ld\n", n);
        return n < 0;
    }
    if (!strcmp(cmd, "stale")) {
        /* where the log ends now (replay), then the crash's leftovers after a gap */
        long n = wal_replay(path, NULL, NULL);
        wal_t *w = wal_open(path, err, sizeof(err));
        if (n < 0 || !w) return 3;
        uint64_t lsn = wal_append(w, "x", 1, NULL, 0, NULL, 0);  /* one more record, durable */
        if (wal_wait(w, lsn)) return 3;
        /* the log's end: walk the headers to the first zero one */
        uint64_t end = 0;
        {
            int rfd = open(path, O_RDONLY);
            uint8_t h[8];
            while (pread(rfd, h, 8, (off_t)end) == 8) {
                uint32_t l = (uint32_t)h[0] | (uint32_t)h[1] << 8 | (uint32_t)h[2] << 16 | (uint32_t)h[3] << 24;
                uint32_t c = (uint32_t)h[4] | (uint32_t)h[5] << 8 | (uint32_t)h[6] << 16 | (uint32_t)h[7] << 24;
                if (!l && !c) break;
                end += 8 + (l & 0x7fffffffu);
            }
            close(rfd);
        }
        uint64_t gap = (uint64_t)atoll(argv[3]);
        int cnt = atoi(argv[4]);
        uint64_t at = end + gap;  /* the log's own end: a block in direct mode, anywhere after appends */
        int fd = open(path, O_WRONLY);
        for (int i = 0; i < cnt; i++) {
            uint8_t rec[64];
            int len = snprintf((char *)rec + 8, sizeof(rec) - 8, "stale:%d:", i);
            put_u32(rec, (uint32_t)len);
            put_u32(rec + 4, crc(rec + 8, (size_t)len));
            if (pwrite(fd, rec, 8 + (size_t)len, (off_t)at) < 0) return 4;
            at += 8 + (uint64_t)len;
        }
        fdatasync(fd);
        printf("stale at %llu..%llu\n", (unsigned long long)(end + gap), (unsigned long long)at);
        fflush(stdout);
        _exit(0);  /* a crash: no clean shutdown */
    }
    if (!strcmp(cmd, "exact")) {  /* records of exactly argv[3] payload bytes, one commit each */
        if (wal_replay(path, NULL, NULL) < 0) return 3;
        wal_t *w = wal_open(path, err, sizeof(err));
        if (!w) return 3;
        static char rec[1 << 16];
        size_t n = (size_t)atoi(argv[3]);
        for (int i = 0; i < atoi(argv[4]); i++) {
            int h = snprintf(rec, sizeof(rec), "e:%d:", i);
            memset(rec + h, 'q', n - (size_t)h);
            uint64_t lsn = wal_append(w, rec, n, NULL, 0, NULL, 0);
            if (!lsn || wal_wait(w, lsn)) return 4;
        }
        return 0;
    }
    if (!strcmp(cmd, "big")) {
        wal_t *w = wal_open(path, err, sizeof(err));
        static char big[3 << 20];
        printf("lsn %llu\n", (unsigned long long)wal_append(w, big, sizeof(big), NULL, 0, NULL, 0));
        return 0;
    }
    return 1;
}
