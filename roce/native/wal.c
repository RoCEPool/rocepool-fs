/* See wal.h. */
#define _GNU_SOURCE
#include "wal.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define WAL_MAX_SYNC 8

struct wal {
    int fd;
    pthread_mutex_t lk;
    pthread_cond_t cv;
    uint8_t *buf, *spare;      /* appended, not yet written / the leader's previous buffer */
    size_t len, cap, spare_cap;
    uint64_t next_lsn;         /* log length including everything appended */
    uint64_t durable_lsn;      /* written and synced up to here */
    int flushing;              /* a leader is writing + syncing */
    int err;                   /* first failure: every later wait fails */
    int kicked;                /* wal_kick: the flusher thread should commit */
    int flusher;               /* the flusher thread is running */
    _Atomic uint64_t durable_pub;  /* durable_lsn, for wal_ready without the lock */
    _Atomic int err_pub;
    int sync_fd[WAL_MAX_SYNC];
    int nsync;
    atomic_ullong records, commits, bytes, sync_ns;
};

static uint32_t crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;
static void crc_init(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
        crc_table[i] = c;
    }
}
static uint32_t crc32_of(uint32_t crc, const uint8_t *p, size_t n) {
    crc = ~crc;
    while (n--) crc = crc_table[(crc ^ *p++) & 0xff] ^ (crc >> 8);
    return ~crc;
}
static void put_u32(uint8_t *p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (8 * i)); }
static uint32_t get_u32(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static uint64_t mono_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

void wal_path(const char *spec, int n, char *out, size_t out_len) {
    const char *br = strstr(spec, "{n}");
    if (br) snprintf(out, out_len, "%.*s%d%s", (int)(br - spec), spec, n, br + 3);
    else snprintf(out, out_len, "%s", spec);
}

wal_t *wal_open(const char *path, char *err, size_t err_len) {
    pthread_once(&crc_once, crc_init);
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (fd < 0) { snprintf(err, err_len, "log %s: %s", path, strerror(errno)); return NULL; }
    struct stat st;
    if (fstat(fd, &st)) { snprintf(err, err_len, "log %s: fstat: %s", path, strerror(errno)); close(fd); return NULL; }
    wal_t *w = calloc(1, sizeof(*w));
    if (!w) { snprintf(err, err_len, "log: out of memory"); close(fd); return NULL; }
    w->fd = fd;
    pthread_mutex_init(&w->lk, NULL);
    pthread_cond_init(&w->cv, NULL);
    w->next_lsn = w->durable_lsn = (uint64_t)st.st_size;
    atomic_store(&w->durable_pub, w->durable_lsn);
    /* the directory entry of a new log file must survive a crash too */
    char dir[4096];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        if (slash == dir) slash[1] = 0; else *slash = 0;
        int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
    return w;
}

int wal_add_sync(wal_t *w, int fd) {
    if (fd < 0) return 0;
    pthread_mutex_lock(&w->lk);
    int rc = 0;
    if (w->nsync == WAL_MAX_SYNC) rc = -ENOSPC;
    else w->sync_fd[w->nsync++] = fd;
    pthread_mutex_unlock(&w->lk);
    return rc;
}

uint64_t wal_append(wal_t *w, const void *a, size_t alen, const void *b, size_t blen, const void *c, size_t clen) {
    size_t n = alen + blen + clen;
    if (n > UINT32_MAX) return 0;
    uint32_t crc = crc32_of(0, a, alen);
    if (blen) crc = crc32_of(crc, b, blen);
    if (clen) crc = crc32_of(crc, c, clen);
    pthread_mutex_lock(&w->lk);
    if (w->err) { pthread_mutex_unlock(&w->lk); return 0; }
    if (w->len + 8 + n > w->cap) {
        size_t nc = w->cap ? w->cap : 1 << 16;
        while (nc < w->len + 8 + n) nc *= 2;
        uint8_t *nb = realloc(w->buf, nc);
        if (!nb) { pthread_mutex_unlock(&w->lk); return 0; }
        w->buf = nb;
        w->cap = nc;
    }
    uint8_t *p = w->buf + w->len;
    put_u32(p, (uint32_t)n);
    put_u32(p + 4, crc);
    memcpy(p + 8, a, alen);
    if (blen) memcpy(p + 8 + alen, b, blen);
    if (clen) memcpy(p + 8 + alen + blen, c, clen);
    w->len += 8 + n;
    w->next_lsn += 8 + n;
    uint64_t lsn = w->next_lsn;
    pthread_mutex_unlock(&w->lk);
    atomic_fetch_add(&w->records, 1);
    return lsn;
}

/* one commit: write + sync everything appended so far. Called with w->lk
 * held and !w->flushing; drops the lock meanwhile, holds it on return. */
static void flush_round(wal_t *w) {
    w->flushing = 1;
    uint8_t *buf = w->buf;
    size_t len = w->len, cap = w->cap;
    w->buf = w->spare;
    w->cap = w->spare_cap;
    w->len = 0;
    uint64_t target = w->next_lsn;
    int nsync = w->nsync, fds[WAL_MAX_SYNC];
    memcpy(fds, w->sync_fd, sizeof(fds));
    pthread_mutex_unlock(&w->lk);
    uint64_t t0 = mono_ns();
    int rc = 0;
    /* the data the records point at first (an OST's tier file) */
    for (int i = 0; i < nsync && !rc; i++)
        if (fdatasync(fds[i])) rc = -errno;
    for (size_t off = 0; off < len && !rc;) {
        ssize_t k = write(w->fd, buf + off, len - off);
        if (k < 0) { if (errno == EINTR) continue; rc = -errno; break; }
        off += (size_t)k;
    }
    if (!rc && len && fdatasync(w->fd)) rc = -errno;
    atomic_fetch_add(&w->sync_ns, mono_ns() - t0);
    atomic_fetch_add(&w->commits, 1);
    atomic_fetch_add(&w->bytes, len);
    pthread_mutex_lock(&w->lk);
    w->spare = buf;
    w->spare_cap = cap;
    if (rc) {
        if (!w->err) fprintf(stderr, "rocepool: writing the log failed: %s -- nothing more is acknowledged\n",
                             strerror(-rc));
        w->err = rc;
        atomic_store(&w->err_pub, rc);
    } else {
        w->durable_lsn = target;
        atomic_store(&w->durable_pub, target);
    }
    w->flushing = 0;
    pthread_cond_broadcast(&w->cv);
}

int wal_wait(wal_t *w, uint64_t lsn) {
    pthread_mutex_lock(&w->lk);
    while (w->durable_lsn < lsn && !w->err) {
        if (w->flushing) { pthread_cond_wait(&w->cv, &w->lk); continue; }
        flush_round(w);  /* leader: commit what is appended so far */
    }
    int rc = w->durable_lsn >= lsn ? 0 : w->err;
    pthread_mutex_unlock(&w->lk);
    return rc;
}

int wal_ready(wal_t *w, uint64_t lsn) {
    if (atomic_load_explicit(&w->durable_pub, memory_order_acquire) >= lsn) return 1;
    int e = atomic_load_explicit(&w->err_pub, memory_order_acquire);
    return e ? e : 0;
}

/* Commits for the requests whose replies wait in their transport
 * (wal_kick): nobody blocks in wal_wait for them, so this thread does.
 * While it syncs, the next batch accumulates. */
static void *flusher_main(void *arg) {
    wal_t *w = arg;
    pthread_setname_np(pthread_self(), "rp-wal");
    pthread_mutex_lock(&w->lk);
    for (;;) {
        if (w->kicked && !w->flushing && !w->err) {
            w->kicked = 0;
            if (w->len) flush_round(w);
            continue;
        }
        pthread_cond_wait(&w->cv, &w->lk);
    }
    return NULL;
}

void wal_kick(wal_t *w) {
    pthread_mutex_lock(&w->lk);
    w->kicked = 1;
    if (!w->flusher) {
        pthread_t t;
        pthread_attr_t a;
        pthread_attr_init(&a);
        pthread_attr_setdetachstate(&a, PTHREAD_CREATE_DETACHED);
        if (!pthread_create(&t, &a, flusher_main, w)) w->flusher = 1;
        pthread_attr_destroy(&a);
        if (!w->flusher) {  /* no thread: commit here */
            uint64_t want = w->next_lsn;
            while (w->durable_lsn < want && !w->err) {
                if (w->flushing) pthread_cond_wait(&w->cv, &w->lk);
                else flush_round(w);
            }
        }
    }
    if (!w->flushing) pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->lk);
}

long wal_replay(const char *path, int (*cb)(void *user, const uint8_t *rec, size_t len), void *user) {
    pthread_once(&crc_once, crc_init);
    int fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0) return errno == ENOENT ? 0 : -errno;
    FILE *f = fdopen(fd, "r+b");
    if (!f) { int e = errno; close(fd); return -e; }
    long n = 0;
    uint64_t good = 0;
    uint8_t hdr[8];
    uint8_t *rec = NULL;
    size_t cap = 0;
    int stop = 0;
    while (!stop && fread(hdr, 1, 8, f) == 8) {
        uint32_t len = get_u32(hdr), crc = get_u32(hdr + 4);
        if (len > cap) {
            uint8_t *r = realloc(rec, len);
            if (!r) { free(rec); fclose(f); return -ENOMEM; }
            rec = r;
            cap = len;
        }
        if (fread(rec, 1, len, f) != len || crc32_of(0, rec, len) != crc) break;  /* torn tail */
        good += 8 + (uint64_t)len;
        n++;
        stop = cb(user, rec, len);
    }
    free(rec);
    if (!stop) {  /* cut a torn tail, so new records follow the last good one */
        fflush(f);
        struct stat st;
        if (!fstat(fd, &st) && (uint64_t)st.st_size > good) {
            fprintf(stderr, "rocepool: log %s: %llu bytes after the last whole record dropped (a write torn by a crash)\n",
                    path, (unsigned long long)((uint64_t)st.st_size - good));
            if (ftruncate(fd, (off_t)good) == 0) fdatasync(fd);
        }
    }
    fclose(f);
    return n;
}

void wal_stats(wal_t *w, uint64_t *records, uint64_t *commits, uint64_t *bytes, uint64_t *sync_ns) {
    *records = atomic_load(&w->records);
    *commits = atomic_load(&w->commits);
    *bytes = atomic_load(&w->bytes);
    *sync_ns = atomic_load(&w->sync_ns);
}
