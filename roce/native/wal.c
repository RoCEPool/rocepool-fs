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
#define WAL_BLOCK 4096u                 /* every commit starts on a block: O_DIRECT */
#define WAL_PAD 0x80000000u             /* length flag: a pad record, skipped by replay */
#define WAL_MAX_RECORD (2u << 20)       /* larger records are refused */
#define WAL_BATCH (256u << 10)          /* a commit takes up to this much (whole records) */
#define WAL_MAX_PIPE 8                  /* commits in flight at most */
/* After a crash, commits that were in flight may have reached the drive
 * out of order: the replay stops at the first one that did not, and the
 * ones after it -- never acknowledged -- must not be read as part of the
 * log once new records fill the gap. They lie within (pipeline - 1) x the
 * largest commit of the end; the replay clears everything up to a run of
 * this many zero bytes. */
#define WAL_CLEAR_RUN ((size_t)(WAL_MAX_PIPE * (WAL_MAX_RECORD + WAL_BATCH)))

struct inflight { uint64_t start, end, seq_end; int done; };

struct wal {
    int fd;                    /* buffered: prefill, fallback writes, fdatasync */
    int dfd;                   /* O_DIRECT | O_DSYNC, -1: none (tmpfs, ...) */
    pthread_mutex_t lk;
    pthread_cond_t cv;         /* waiters: the durable prefix moved */
    pthread_cond_t fcv;        /* flusher threads: something to commit */
    uint8_t *buf;              /* records appended, not yet taken by a flusher */
    size_t len, cap;
    uint64_t buf_start;        /* the file offset buf[0] goes to */
    uint64_t next_lsn;         /* buf_start + len: how far the prefill must stay ahead */
    /* An LSN is a record's sequence number (1, 2, ...), not a file offset:
     * a commit that takes part of what is appended pads to a block, which
     * moves the records after it -- an offset handed out at append time
     * would then be passed before they are written (an early reply) */
    uint64_t next_seq;         /* records appended */
    uint64_t buf_seq;          /* records taken by flushers (buf[0] is the next) */
    uint64_t durable_lsn;      /* records 1..durable_lsn are on the drive */
    int want;                  /* a waiter / kick wants what is appended committed */
    int err;
    struct inflight fl[WAL_MAX_PIPE];
    int nfl;                   /* commits in flight, in log order */
    int pipe;                  /* flusher threads */
    int stop;
    /* zeros written and synced up to here (dfd mode): a direct write below
     * it changes no file system metadata -- one drive write, no journal */
    uint64_t prefilled;
    uint64_t chunk;            /* prefill step */
    pthread_mutex_t ext_lk;
    pthread_cond_t ext_cv;     /* the prefill thread */
    int sync_fd[WAL_MAX_SYNC];
    int nsync;
    _Atomic uint64_t durable_pub;
    _Atomic int err_pub;
    atomic_ullong records, commits, bytes, sync_ns, inflight_max, prefill_bytes;
    pthread_t th[WAL_MAX_PIPE + 1];
    int nth;
    double cal_append, cal_direct, cal_direct4;  /* records/s the calibration measured (0: not run) */
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
static uint32_t crc32_zeros(size_t n) {
    uint32_t crc = ~0u;
    while (n--) crc = crc_table[crc & 0xff] ^ (crc >> 8);
    return ~crc;
}
static void put_u32(uint8_t *p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (8 * i)); }
static uint32_t get_u32(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static uint64_t mono_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}
static uint64_t align_up(uint64_t v) { return (v + WAL_BLOCK - 1) & ~(uint64_t)(WAL_BLOCK - 1); }

static int pwrite_all(int fd, const void *buf, size_t len, uint64_t off) {
    const uint8_t *p = buf;
    while (len) {
        ssize_t k = pwrite(fd, p, len, (off_t)off);
        if (k < 0) { if (errno == EINTR) continue; return -errno; }
        p += k; len -= (size_t)k; off += (uint64_t)k;
    }
    return 0;
}

/* a pad record of exactly n (>= 8) bytes at p: header + zeros */
static void put_pad(uint8_t *p, size_t n) {
    memset(p, 0, n);
    put_u32(p, (uint32_t)(n - 8) | WAL_PAD);
    put_u32(p + 4, crc32_zeros(n - 8));
}
/* bytes to append so that `len` ends on a block: 0, or a pad of >= 8 */
static size_t pad_for(uint64_t end) {
    size_t r = (size_t)(align_up(end) - end);
    if (r && r < 8) r += WAL_BLOCK;
    return r;
}

static int env_int(const char *name, int dflt, int lo, int hi) {
    const char *e = getenv(name);
    int v = e && *e ? atoi(e) : dflt;
    return v < lo ? lo : v > hi ? hi : v;
}

void wal_path(const char *spec, int n, char *out, size_t out_len) {
    const char *br = strstr(spec, "{n}");
    if (br) snprintf(out, out_len, "%.*s%d%s", (int)(br - spec), spec, n, br + 3);
    else snprintf(out, out_len, "%s", spec);
}

/* ------------------------------------------------------------ replay */
/* Scans the log: every whole record in order to cb (not pad records);
 * stops at a zero header (the end), a short or corrupt record, or when cb
 * says so. *good = where the next record goes. */
static long scan(int fd, int (*cb)(void *, const uint8_t *, size_t), void *user, uint64_t *good, int *torn) {
    FILE *f = fdopen(dup(fd), "rb");
    if (!f) return -errno;
    long n = 0;
    uint64_t pos = 0;
    uint8_t hdr[8], *rec = NULL;
    size_t cap = 0;
    *torn = 0;
    for (;;) {
        size_t h = fread(hdr, 1, 8, f);
        if (h == 0) break;                                  /* end of file */
        if (h < 8) { *torn = 1; break; }
        uint32_t lf = get_u32(hdr), crc = get_u32(hdr + 4);
        if (lf == 0 && crc == 0) break;                     /* zeros: the end */
        int pad = (lf & WAL_PAD) != 0;
        uint32_t len = lf & ~WAL_PAD;
        /* a record has a payload; a pad may not (exactly 8 bytes to the block) */
        if ((len == 0 && !pad) || len > WAL_MAX_RECORD || (pad && len > 2 * WAL_BLOCK)) { *torn = 1; break; }
        if (len > cap || !rec) {
            uint8_t *r = realloc(rec, len ? len : 1);
            if (!r) { free(rec); fclose(f); return -ENOMEM; }
            rec = r;
            cap = len ? len : 1;
        }
        if (fread(rec, 1, len, f) != len || crc32_of(0, rec, len) != crc) { *torn = 1; break; }
        if (pad && (pos + 8 + len) % WAL_BLOCK) { *torn = 1; break; }  /* a pad always ends a block */
        pos += 8 + (uint64_t)len;
        if (pad) continue;
        n++;
        if (cb && cb(user, rec, len)) break;
    }
    free(rec);
    fclose(f);
    *good = pos;
    return n;
}

/* Everything after `good` up to a long run of zeros: what a crash left of
 * commits that were never acknowledged. Cleared, so new records that fill
 * the gap are never followed by them. Returns the bytes found non-zero. */
static int clear_after(int fd, uint64_t good, uint64_t *nonzero) {
    struct stat st;
    if (fstat(fd, &st)) return -errno;
    uint64_t size = (uint64_t)st.st_size, pos = good, zrun = 0, last_nz = good;
    static uint8_t zero[1 << 16];
    uint8_t *b = malloc(1 << 16);
    if (!b) return -ENOMEM;
    while (pos < size && zrun < WAL_CLEAR_RUN) {
        size_t want = size - pos < (1 << 16) ? (size_t)(size - pos) : (1 << 16);
        ssize_t k = pread(fd, b, want, (off_t)pos);
        if (k <= 0) break;
        for (ssize_t i = 0; i < k; i++) {
            if (b[i]) { zrun = 0; last_nz = pos + (uint64_t)i + 1; }
            else zrun++;
        }
        pos += (uint64_t)k;
    }
    free(b);
    *nonzero = last_nz - good;
    for (uint64_t o = good; o < last_nz;) {
        size_t n = last_nz - o < sizeof(zero) ? (size_t)(last_nz - o) : sizeof(zero);
        int rc = pwrite_all(fd, zero, n, o);
        if (rc) return rc;
        o += n;
    }
    return last_nz > good && fdatasync(fd) ? -errno : 0;
}

/* the end each replay found, for the wal_open that follows it */
static pthread_mutex_t ends_lk = PTHREAD_MUTEX_INITIALIZER;
static struct { char path[1024]; uint64_t end; } ends[64];
static int nends;

static long replay_fd(int fd, const char *path, int (*cb)(void *, const uint8_t *, size_t), void *user,
                      uint64_t *end) {
    pthread_once(&crc_once, crc_init);
    uint64_t good = 0;
    int torn = 0;
    long n = scan(fd, cb, user, &good, &torn);
    if (n < 0) return n;
    uint64_t nz = 0;
    int rc = clear_after(fd, good, &nz);
    if (rc) return rc;
    if (nz)
        fprintf(stderr, "rocepool: log %s: %llu bytes after the last whole record cleared (commits a crash cut %s)\n",
                path, (unsigned long long)nz, torn ? "short" : "off");
    /* the next commit starts on a block: a pad record up to it */
    uint64_t start = align_up(good);
    size_t pad = pad_for(good);
    if (pad) {
        uint8_t p[2 * WAL_BLOCK];
        put_pad(p, pad);
        if ((rc = pwrite_all(fd, p, pad, good)) || fdatasync(fd)) return rc ? rc : -errno;
        start = good + pad;
    }
    *end = start;
    return n;
}

long wal_replay(const char *path, int (*cb)(void *user, const uint8_t *rec, size_t len), void *user) {
    int fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0) return errno == ENOENT ? 0 : -errno;
    uint64_t end = 0;
    long n = replay_fd(fd, path, cb, user, &end);
    close(fd);
    if (n >= 0) {
        pthread_mutex_lock(&ends_lk);
        int i = 0;
        while (i < nends && strcmp(ends[i].path, path)) i++;
        if (i == nends && nends < 64) nends++;
        if (i < 64) { snprintf(ends[i].path, sizeof(ends[i].path), "%s", path); ends[i].end = end; }
        pthread_mutex_unlock(&ends_lk);
    }
    return n;
}

/* ----------------------------------------------------------- prefill */
/* zeros from w->prefilled to at least `upto`, synced (ext_lk held) */
static int extend_locked(wal_t *w, uint64_t upto) {
    static uint8_t zero[1 << 20];
    while (w->prefilled < upto) {
        uint64_t from = w->prefilled, to = from + w->chunk;
        for (uint64_t o = from; o < to; o += sizeof(zero)) {
            int rc = pwrite_all(w->fd, zero, sizeof(zero), o);
            if (rc) return rc;
        }
        if (fdatasync(w->fd)) return -errno;
        atomic_fetch_add(&w->prefill_bytes, to - from);
        __atomic_store_n(&w->prefilled, to, __ATOMIC_RELEASE);
    }
    return 0;
}

/* keeps the zeros ahead of the log, off the commit path */
static void *prefill_main(void *arg) {
    wal_t *w = arg;
    pthread_setname_np(pthread_self(), "rp-wal-fill");
    pthread_mutex_lock(&w->ext_lk);
    while (!w->stop) {
        /* a chunk ahead (wal_open writes the first): extends only once the
         * log has used a chunk's worth -- not while it is being calibrated */
        uint64_t want = align_up(__atomic_load_n(&w->next_lsn, __ATOMIC_RELAXED)) + w->chunk;
        if (w->prefilled < want) {
            if (extend_locked(w, want)) {
                fprintf(stderr, "rocepool: log prefill failed: %s -- commits extend it themselves\n", strerror(errno));
                break;
            }
            continue;
        }
        pthread_cond_wait(&w->ext_cv, &w->ext_lk);
    }
    pthread_mutex_unlock(&w->ext_lk);
    return NULL;
}

/* ------------------------------------------------------------ commit */
static void fail_locked(wal_t *w, int rc) {
    if (!w->err) {
        fprintf(stderr, "rocepool: writing the log failed: %s -- nothing more is acknowledged\n", strerror(-rc));
        w->err = rc;
        atomic_store(&w->err_pub, rc);
    }
}

/* How much of buf one commit takes: whole records, up to WAL_BATCH (one
 * record at least). */
static size_t take_len(const wal_t *w, uint64_t *nrec) {
    size_t off = 0;
    *nrec = 0;
    while (off + 8 <= w->len) {
        size_t r = 8 + (get_u32(w->buf + off) & ~WAL_PAD);
        if (off && off + r > WAL_BATCH) break;
        off += r;
        ++*nrec;
    }
    return off;
}

static int grow(uint8_t **buf, size_t *cap, size_t need, size_t keep) {
    if (need <= *cap) return 0;
    size_t nc = *cap ? *cap : 1 << 16;
    while (nc < need) nc *= 2;
    void *nb;
    if (posix_memalign(&nb, WAL_BLOCK, nc)) return -ENOMEM;
    if (keep) memcpy(nb, *buf, keep);
    free(*buf);
    *buf = nb;
    *cap = nc;
    return 0;
}

/* One commit: whole records off the front of buf (padded to a block for a
 * direct write), the tier files synced, written, the durable prefix moved.
 * Called with w->lk held and a commit possible (len, nfl < pipe); drops
 * the lock for the I/O, holds it again on return. *mine: the caller's
 * own buffer to write from. */
static void commit_one(wal_t *w, uint8_t **mine, size_t *mine_cap) {
    uint64_t nrec;
    size_t n = take_len(w, &nrec), pad = w->dfd >= 0 ? pad_for(w->buf_start + n) : 0;
    if (grow(mine, mine_cap, n + pad, 0)) { fail_locked(w, -ENOMEM); pthread_cond_broadcast(&w->cv); return; }
    memcpy(*mine, w->buf, n);
    if (pad) put_pad(*mine + n, pad);
    memmove(w->buf, w->buf + n, w->len - n);
    w->len -= n;
    uint64_t start = w->buf_start, end = start + n + pad;
    w->buf_start = end;
    __atomic_store_n(&w->next_lsn, end + w->len, __ATOMIC_RELAXED);
    if (!w->len) w->want = 0;
    w->buf_seq += nrec;
    w->fl[w->nfl++] = (struct inflight){start, end, w->buf_seq, 0};
    if ((unsigned long long)w->nfl > atomic_load(&w->inflight_max)) atomic_store(&w->inflight_max, (unsigned long long)w->nfl);
    int nsync = w->nsync, fds[WAL_MAX_SYNC];
    memcpy(fds, w->sync_fd, sizeof(fds));
    int dfd = w->dfd;
    pthread_mutex_unlock(&w->lk);

    uint64_t t0 = mono_ns();
    int rc = 0;
    /* the data the records point at first (an OST's tier file) */
    for (int i = 0; i < nsync && !rc; i++)
        if (fdatasync(fds[i])) rc = -errno;
    if (!rc && dfd >= 0 && __atomic_load_n(&w->prefilled, __ATOMIC_ACQUIRE) < end) {
        pthread_mutex_lock(&w->ext_lk);  /* the prefill fell behind: extend here */
        rc = extend_locked(w, end);
        pthread_mutex_unlock(&w->ext_lk);
    }
    if (!rc && dfd >= 0) {
        rc = pwrite_all(dfd, *mine, end - start, start);  /* O_DSYNC: on the drive when it returns */
        if (rc == -EINVAL) {  /* the file system refused direct I/O after all */
            pthread_mutex_lock(&w->lk);
            w->dfd = -1;
            pthread_mutex_unlock(&w->lk);
            dfd = -1;
            rc = 0;
        }
    }
    if (!rc && dfd < 0) {
        rc = pwrite_all(w->fd, *mine, end - start, start);
        if (!rc && fdatasync(w->fd)) rc = -errno;
    }
    atomic_fetch_add(&w->sync_ns, mono_ns() - t0);
    atomic_fetch_add(&w->commits, 1);
    atomic_fetch_add(&w->bytes, end - start);

    pthread_mutex_lock(&w->lk);
    if (rc) fail_locked(w, rc);
    for (int i = 0; i < w->nfl; i++) if (w->fl[i].start == start) w->fl[i].done = 1;
    /* durable: the longest prefix of finished commits */
    int k = 0;
    while (k < w->nfl && w->fl[k].done) w->durable_lsn = w->fl[k++].seq_end;
    if (k) {
        memmove(w->fl, w->fl + k, (size_t)(w->nfl - k) * sizeof(w->fl[0]));
        w->nfl -= k;
        atomic_store_explicit(&w->durable_pub, w->durable_lsn, memory_order_release);
        pthread_cond_broadcast(&w->cv);  /* the waiters */
    }
    if (rc) pthread_cond_broadcast(&w->cv);
    /* more to commit and a flusher may be idle */
    if (w->want && w->len && w->nfl < w->pipe) pthread_cond_signal(&w->fcv);
    /* wake the prefill (busy already when its lock is held: never wait
     * for it -- it holds the lock through a whole chunk of zeros) */
    if (w->dfd >= 0 && !pthread_mutex_trylock(&w->ext_lk)) {
        pthread_cond_signal(&w->ext_cv);
        pthread_mutex_unlock(&w->ext_lk);
    }
}

/* commits for requests nobody waits in (wal_kick: the transport holds
 * their replies) */
static void *flusher_main(void *arg) {
    wal_t *w = arg;
    pthread_setname_np(pthread_self(), "rp-wal");
    uint8_t *mine = NULL;
    size_t mine_cap = 0;
    pthread_mutex_lock(&w->lk);
    for (;;) {
        while (!w->stop && !(w->want && w->len && !w->err && w->nfl < w->pipe)) pthread_cond_wait(&w->fcv, &w->lk);
        if (w->stop) break;
        commit_one(w, &mine, &mine_cap);
    }
    pthread_mutex_unlock(&w->lk);
    free(mine);
    return NULL;
}

/* -------------------------------------------------------------- open */
/* the log at path, its records ending at `end`, committed `direct`ly or by
 * appends, `pipe` commits at once at most */
static wal_t *open_core(const char *path, int fd, uint64_t end, int direct, int pipe, char *err, size_t err_len) {
    wal_t *w = calloc(1, sizeof(*w));
    if (!w) { snprintf(err, err_len, "log: out of memory"); return NULL; }
    w->fd = fd;
    pthread_mutex_init(&w->lk, NULL);
    pthread_cond_init(&w->cv, NULL);
    pthread_cond_init(&w->fcv, NULL);
    pthread_mutex_init(&w->ext_lk, NULL);
    pthread_cond_init(&w->ext_cv, NULL);
    w->buf_start = w->next_lsn = end;
    w->pipe = pipe;
    w->chunk = (uint64_t)env_int("ROCEPOOL_LOG_PREFILL_MIB", 32, 1, 4096) << 20;
    w->dfd = direct ? open(path, O_RDWR | O_DIRECT | O_DSYNC | O_CLOEXEC) : -1;
    struct stat st;
    if (!fstat(fd, &st)) w->prefilled = (uint64_t)st.st_size & ~(uint64_t)(WAL_BLOCK - 1);
    if (w->prefilled < end) w->prefilled = end;
    if (w->dfd >= 0) {
        pthread_mutex_lock(&w->ext_lk);
        int rc = extend_locked(w, end + w->chunk);
        pthread_mutex_unlock(&w->ext_lk);
        if (rc) { close(w->dfd); w->dfd = -1; }  /* no room for zeros: appends */
    }
    for (int i = 0; i < w->pipe; i++)
        if (!pthread_create(&w->th[w->nth], NULL, flusher_main, w)) w->nth++;
    if (!w->nth) { snprintf(err, err_len, "log: no flusher thread"); if (w->dfd >= 0) close(w->dfd); free(w); return NULL; }
    if (w->dfd >= 0) {
        pthread_t t;
        if (!pthread_create(&t, NULL, prefill_main, w)) w->th[w->nth++] = t;
    }
    return w;
}

void wal_close(wal_t *w) {
    pthread_mutex_lock(&w->lk);
    w->stop = 1;
    pthread_cond_broadcast(&w->fcv);
    pthread_cond_broadcast(&w->cv);
    pthread_mutex_unlock(&w->lk);
    pthread_mutex_lock(&w->ext_lk);
    pthread_cond_broadcast(&w->ext_cv);
    pthread_mutex_unlock(&w->ext_lk);
    for (int i = 0; i < w->nth; i++) pthread_join(w->th[i], NULL);
    if (w->dfd >= 0) close(w->dfd);
    close(w->fd);
    free(w->buf);
    free(w);
}

/* ------------------------------------------------------- calibration */
/* Which commit suits this drive is measured, not assumed. On 6 x i3en an
 * append + fdatasync took ~150 us (the file system's journal commit behind
 * each); on a VM's virtual disk every sync is a cache flush whatever the
 * write, and 4 commits at once -- though 4 raw writes at once ran at twice
 * the rate of one -- split the batches and gave the log 25% less than one
 * at a time. So each candidate runs the real thing for a moment, on a
 * scratch log next to the real one: 32 threads appending 160-byte records
 * and waiting for them. */
struct cal_load { wal_t *w; uint64_t until; _Atomic long done; };
static void *cal_thread(void *a) {
    struct cal_load *c = a;
    char rec[160];
    memset(rec, 'c', sizeof(rec));
    while (mono_ns() < c->until) {
        uint64_t l = wal_append(c->w, rec, sizeof(rec), NULL, 0, NULL, 0);
        if (!l || wal_wait(c->w, l)) break;
        atomic_fetch_add(&c->done, 1);
    }
    return NULL;
}
static double cal_rate(const char *scratch, int direct, int pipe, int ms) {
    unlink(scratch);
    int fd = open(scratch, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) return 0;
    char err[256];
    wal_t *w = open_core(scratch, fd, 0, direct, pipe, err, sizeof(err));
    if (!w) { close(fd); unlink(scratch); return 0; }
    if (direct && w->dfd < 0) { wal_close(w); unlink(scratch); return 0; }  /* no direct I/O here */
    struct cal_load c = {w, 0, 0};
    pthread_t t[32];
    int n = 0;
    uint64_t t0 = mono_ns();
    c.until = t0 + (uint64_t)ms * 1000000ull;
    for (; n < 32; n++) if (pthread_create(&t[n], NULL, cal_thread, &c)) break;
    for (int i = 0; i < n; i++) pthread_join(t[i], NULL);
    double rate = (double)atomic_load(&c.done) * 1e9 / (double)(mono_ns() - t0);
    wal_close(w);
    unlink(scratch);
    return rate;
}
static void calibrate(const char *path, int want_direct, int want_pipe, int *direct, int *pipe, double res[3]) {
    char scratch[1100];
    snprintf(scratch, sizeof(scratch), "%s.calibrate", path);
    int ms = env_int("ROCEPOOL_LOG_CALIBRATE_MS", 120, 10, 5000);
    static const int modes[3][2] = {{0, 1}, {1, 1}, {1, 4}};  /* append / direct, one at a time / 4 */
    int best = -1;
    for (int i = 0; i < 3; i++) {
        res[i] = 0;
        if (want_direct >= 0 && modes[i][0] != want_direct) continue;
        if (want_pipe > 0 && modes[i][1] != 1 && want_pipe == 1) continue;
        res[i] = cal_rate(scratch, modes[i][0], want_pipe > 0 ? want_pipe : modes[i][1], ms);
        /* a candidate wins only by 10%: the simpler one otherwise */
        if (res[i] > 0 && (best < 0 || res[i] > 1.1 * res[best])) best = i;
    }
    if (best < 0) best = 0;
    *direct = modes[best][0];
    *pipe = want_pipe > 0 ? want_pipe : modes[best][1];
    fprintf(stderr, "rocepool: log %s: commits/s measured here -- appends %.0f, direct writes %.0f, direct writes 4 at "
            "once %.0f records/s -> %s, %d in flight\n", path, res[0], res[1], res[2],
            *direct ? "direct writes" : "appends", *pipe);
}

wal_t *wal_open(const char *path, char *err, size_t err_len) {
    pthread_once(&crc_once, crc_init);
    int fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (fd < 0) { snprintf(err, err_len, "log %s: %s", path, strerror(errno)); return NULL; }
    /* where the log ends: the replay that just ran says; else scan it now */
    uint64_t end = 0;
    int known = 0;
    pthread_mutex_lock(&ends_lk);
    for (int i = 0; i < nends; i++)
        if (!strcmp(ends[i].path, path)) { end = ends[i].end; known = 1; ends[i] = ends[--nends]; break; }
    pthread_mutex_unlock(&ends_lk);
    if (!known) {
        long n = replay_fd(fd, path, NULL, NULL, &end);
        if (n < 0) { snprintf(err, err_len, "log %s: %s", path, strerror((int)-n)); close(fd); return NULL; }
    }
    /* ROCEPOOL_LOG_DIRECT / ROCEPOOL_LOG_PIPELINE set: as told; else measured */
    int want_pipe = env_int("ROCEPOOL_LOG_PIPELINE", 0, 0, WAL_MAX_PIPE);
    int want_direct = getenv("ROCEPOOL_LOG_DIRECT") && *getenv("ROCEPOOL_LOG_DIRECT")
                          ? env_int("ROCEPOOL_LOG_DIRECT", 1, 0, 1) : -1;
    int direct = want_direct > 0, pipe = want_pipe ? want_pipe : 1;
    double res[3] = {0, 0, 0};
    if (want_direct < 0 || !want_pipe) calibrate(path, want_direct, want_pipe, &direct, &pipe, res);
    wal_t *w = open_core(path, fd, end, direct, pipe, err, err_len);
    if (!w) { close(fd); return NULL; }
    w->cal_append = res[0];
    w->cal_direct = res[1];
    w->cal_direct4 = res[2];
    /* the directory entry of a new log file must survive a crash too */
    char dir[4096];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) {
        if (slash == dir) slash[1] = 0; else *slash = 0;
        int dfdir = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfdir >= 0) { fsync(dfdir); close(dfdir); }
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
    if (n == 0 || n > WAL_MAX_RECORD) return 0;
    uint32_t crc = crc32_of(0, a, alen);
    if (blen) crc = crc32_of(crc, b, blen);
    if (clen) crc = crc32_of(crc, c, clen);
    pthread_mutex_lock(&w->lk);
    if (w->err || grow(&w->buf, &w->cap, w->len + 8 + n, w->len)) { pthread_mutex_unlock(&w->lk); return 0; }
    uint8_t *p = w->buf + w->len;
    put_u32(p, (uint32_t)n);
    put_u32(p + 4, crc);
    memcpy(p + 8, a, alen);
    if (blen) memcpy(p + 8 + alen, b, blen);
    if (clen) memcpy(p + 8 + alen + blen, c, clen);
    w->len += 8 + n;
    __atomic_store_n(&w->next_lsn, w->buf_start + w->len, __ATOMIC_RELAXED);
    uint64_t lsn = ++w->next_seq;
    pthread_mutex_unlock(&w->lk);
    atomic_fetch_add(&w->records, 1);
    return lsn;
}

int wal_wait(wal_t *w, uint64_t lsn) {
    static __thread uint8_t *mine;
    static __thread size_t mine_cap;
    pthread_mutex_lock(&w->lk);
    if (w->durable_lsn < lsn && !w->err) w->want = 1;
    while (w->durable_lsn < lsn && !w->err) {
        /* a free slot: commit it here, no hand-off to another thread (the
         * records this one waits for are taken first) */
        if (w->len && w->nfl < w->pipe && w->buf_seq < lsn) commit_one(w, &mine, &mine_cap);
        else pthread_cond_wait(&w->cv, &w->lk);
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

void wal_kick(wal_t *w) {
    pthread_mutex_lock(&w->lk);
    w->want = 1;
    if (w->len && w->nfl < w->pipe) pthread_cond_signal(&w->fcv);
    pthread_mutex_unlock(&w->lk);
}

void wal_stats(wal_t *w, uint64_t *records, uint64_t *commits, uint64_t *bytes, uint64_t *sync_ns) {
    *records = atomic_load(&w->records);
    *commits = atomic_load(&w->commits);
    *bytes = atomic_load(&w->bytes);
    *sync_ns = atomic_load(&w->sync_ns);
}

void wal_calibration(wal_t *w, double *append, double *direct, double *direct4) {
    *append = w->cal_append;
    *direct = w->cal_direct;
    *direct4 = w->cal_direct4;
}

void wal_stats_more(wal_t *w, int *direct, int *pipeline, uint64_t *inflight_max, uint64_t *prefill_bytes) {
    pthread_mutex_lock(&w->lk);
    *direct = w->dfd >= 0;
    *pipeline = w->pipe;
    pthread_mutex_unlock(&w->lk);
    *inflight_max = atomic_load(&w->inflight_max);
    *prefill_bytes = atomic_load(&w->prefill_bytes);
}
