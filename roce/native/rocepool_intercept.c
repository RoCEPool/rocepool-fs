/* rocepool_intercept.c -- the kernel-bypass client: an LD_PRELOAD library
 * that takes the POSIX calls an application makes on paths under
 * $ROCEPOOL_PREFIX and serves them from the native client core
 * (rocepool_fs.c) inside the application's own process -- no kernel VFS,
 * no FUSE, no /dev/fuse channel; the data goes from the application's
 * buffer into registered arena memory and from there by RDMA to the OSTs.
 * Everything else is passed to libc untouched (dlsym RTLD_NEXT). The same
 * idea as DAOS's libioil and WEKA's client library; IO500 permits it.
 *
 *   LD_PRELOAD=librocepool_intercept.so ROCEPOOL_PREFIX=/mnt/rocepool \
 *   ROCEPOOL_MDTS=h1:19901,... ROCEPOOL_OSTS=h1:19951,... ior -a POSIX -o /mnt/rocepool/f
 *
 * What is served: open/openat/creat (O_CREAT, O_EXCL, O_TRUNC, O_APPEND),
 * close, read/write/pread/pwrite/readv/writev, lseek, fstat/stat/lstat/
 * fstatat (and the __xstat family of older glibc), unlink/unlinkat, mkdir,
 * rmdir, access, opendir/readdir/closedir/dirfd/rewinddir, fsync,
 * ftruncate/truncate, dup/dup2/dup3, fcntl(F_GETFL/F_SETFL/F_DUPFD),
 * realpath, statfs/statvfs, chmod/chown/utimes (accepted, no-op), rename
 * (EXDEV: not supported), the _FORTIFY_SOURCE variants (__open_2,
 * __read_chk, __realpath_chk, ...) and fopen/fopen64/fdopen (a FILE built
 * with fopencookie over our descriptor, since libc's own streams call
 * libc internally and cannot be interposed). Not served: mmap of these
 * files (ENODEV), symlinks, hard links.
 *
 * Rules learnt from running real programs under it: never call strtok
 * (the host may be inside its own strtok loop -- io500 is), resolve libc
 * lazily in every entry point (another library's constructor may call
 * through us before ours ran -- libselinux does under nsenter), decide by
 * the prefix string alone before touching anything else.
 *
 * File descriptors: every file or directory opened here gets a REAL
 * descriptor (a dup of /dev/null) so the number is reserved in the kernel
 * and dup/close/select behave; a table maps it to the handle. A fork
 * inherits the table; the child re-initialises the core (threads do not
 * survive fork) on its first call.
 */
#define _GNU_SOURCE
#include "rocepool_fs.h"

#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/statvfs.h>
#include <sys/uio.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

/* ---------------------------------------------------------------- libc */
#define REAL(name) static typeof(name) *real_##name
REAL(open); REAL(open64); REAL(openat); REAL(openat64); REAL(creat); REAL(creat64); REAL(close);
REAL(read); REAL(write); REAL(pread); REAL(pread64); REAL(pwrite); REAL(pwrite64); REAL(readv); REAL(writev);
REAL(lseek); REAL(lseek64); REAL(fstat); REAL(fstat64); REAL(stat); REAL(stat64); REAL(lstat); REAL(lstat64);
REAL(fstatat); REAL(fstatat64); REAL(unlink); REAL(unlinkat); REAL(mkdir); REAL(mkdirat); REAL(rmdir);
REAL(access); REAL(faccessat); REAL(opendir); REAL(fdopendir); REAL(readdir); REAL(readdir64); REAL(closedir);
REAL(dirfd); REAL(rewinddir); REAL(fsync); REAL(fdatasync); REAL(ftruncate); REAL(ftruncate64); REAL(truncate);
REAL(truncate64); REAL(dup); REAL(dup2); REAL(dup3); REAL(fcntl); REAL(realpath); REAL(statfs); REAL(statvfs);
REAL(chmod); REAL(fchmod); REAL(chown); REAL(rename); REAL(renameat); REAL(utimes); REAL(futimens); REAL(utimensat);
REAL(fchdir); REAL(chdir); REAL(fopen); REAL(fopen64); REAL(fdopen);

static void *sym(const char *name) {
    void *p = dlsym(RTLD_NEXT, name);
    if (!p) { fprintf(stderr, "rocepool_intercept: libc has no %s\n", name); abort(); }
    return p;
}

static int g_log;
static pthread_once_t g_libc_once = PTHREAD_ONCE_INIT;
static int g_libc_ok;
static void load_libc(void);
/* Another preloaded or dependent library's constructor may call libc through
 * us before our own constructor ran (libselinux's fopen("/proc/filesystems")
 * under nsenter): every entry point resolves libc first. */
#define ENSURE() do { if (__builtin_expect(!g_libc_ok, 0)) pthread_once(&g_libc_once, load_libc); } while (0)
/* Resolve libc before main() runs: every entry point below may be called
 * on a descriptor that is not ours before any path under the prefix is
 * seen (a plain `tail` reads stdin first). */
static void read_prefix(void);
__attribute__((constructor)) static void intercept_ctor(void) { pthread_once(&g_libc_once, load_libc); read_prefix(); }
static int g_ready;
static pid_t g_pid;
/* ROCEPOOL_INTERCEPT_LOG: a one-line summary at exit (tests read it) */
__attribute__((destructor)) static void intercept_dtor(void) {
    if (!g_log || g_ready != 1 || getpid() != g_pid) return;
    uint64_t dr, db, dl, ds, lk, lm;
    rp_direct_stats(&dr, &db, &dl, &ds);
    rp_lookup_stats(&lk, &lm);
    unsigned long long orr = 0, mr = 0;
    for (int i = 0; i < cfg.nost; i++) orr += atomic_load(&g_osts[i].rpcs);
    for (int i = 0; i < cfg.nmdt; i++) mr += atomic_load(&g_mdts[i].rpcs);
    fprintf(stderr, "rocepool_intercept[%d]: stats mdt_rpcs=%llu ost_rpcs=%llu direct_reads=%llu direct_bytes=%llu "
                    "leases=%llu stale_leases=%llu direct_lookups=%llu direct_lookup_misses=%llu\n", (int)getpid(), mr, orr,
            (unsigned long long)dr, (unsigned long long)db, (unsigned long long)dl, (unsigned long long)ds,
            (unsigned long long)lk, (unsigned long long)lm);
}
static void load_libc(void) {
#define LOAD(name) real_##name = sym(#name)
    LOAD(open); LOAD(open64); LOAD(openat); LOAD(openat64); LOAD(creat); LOAD(creat64); LOAD(close);
    LOAD(read); LOAD(write); LOAD(pread); LOAD(pread64); LOAD(pwrite); LOAD(pwrite64); LOAD(readv); LOAD(writev);
    LOAD(lseek); LOAD(lseek64); LOAD(fstat); LOAD(fstat64); LOAD(stat); LOAD(stat64); LOAD(lstat); LOAD(lstat64);
    LOAD(fstatat); LOAD(fstatat64); LOAD(unlink); LOAD(unlinkat); LOAD(mkdir); LOAD(mkdirat); LOAD(rmdir);
    LOAD(access); LOAD(faccessat); LOAD(opendir); LOAD(fdopendir); LOAD(readdir); LOAD(readdir64); LOAD(closedir);
    LOAD(dirfd); LOAD(rewinddir); LOAD(fsync); LOAD(fdatasync); LOAD(ftruncate); LOAD(ftruncate64); LOAD(truncate);
    LOAD(truncate64); LOAD(dup); LOAD(dup2); LOAD(dup3); LOAD(fcntl); LOAD(realpath); LOAD(statfs); LOAD(statvfs);
    LOAD(chmod); LOAD(fchmod); LOAD(chown); LOAD(rename); LOAD(renameat); LOAD(utimes); LOAD(futimens); LOAD(utimensat);
    LOAD(fchdir); LOAD(chdir); LOAD(fopen); LOAD(fopen64); LOAD(fdopen);
    g_libc_ok = 1;
#undef LOAD
}

/* ---------------------------------------------------------------- state */
static char g_prefix[PATH_MAX / 2];   /* "/mnt/rocepool", no trailing slash */
static size_t g_prefix_len;

/* The prefix alone decides whether a path concerns us; the client core
 * (connections, threads) is only brought up once a path under it is seen,
 * so a shell or mpirun carrying our environment stays untouched. */
static void read_prefix(void) {
    const char *prefix = getenv("ROCEPOOL_PREFIX");
    g_log = getenv("ROCEPOOL_INTERCEPT_LOG") ? atoi(getenv("ROCEPOOL_INTERCEPT_LOG")) : 0;
    if (!prefix || prefix[0] != '/' || strlen(prefix) >= sizeof(g_prefix)) return;
    snprintf(g_prefix, sizeof(g_prefix), "%s", prefix);
    g_prefix_len = strlen(g_prefix);
    while (g_prefix_len > 1 && g_prefix[g_prefix_len - 1] == '/') g_prefix[--g_prefix_len] = 0;
}

/* out = g_prefix + "/" + rel; -1 if it does not fit. */
static int join_prefix(char *out, size_t n, const char *rel) {
    size_t pl = g_prefix_len, rl = strlen(rel);
    if (pl + 1 + rl + 1 > n) return -1;
    memcpy(out, g_prefix, pl);
    if (rl) { out[pl] = '/'; memcpy(out + pl + 1, rel, rl + 1); }
    else out[pl] = 0;
    return 0;
}
static int g_ready = -1;          /* -1 unknown, 0 off (no config / init failed), 1 serving */
static pthread_mutex_t g_init_lk = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    handle_t *h;
    int refs;             /* descriptors sharing this (dup) */
    int is_dir;
    int flags;            /* O_APPEND / O_RDONLY ... */
    pthread_mutex_t lk;   /* position */
    uint64_t pos;
} ofile_t;

#define FD_TABLE 65536
static ofile_t *g_fds[FD_TABLE];
static pthread_rwlock_t g_fd_lk = PTHREAD_RWLOCK_INITIALIZER;

typedef struct {
    int fd;               /* reserved real descriptor (dirfd) */
    char *path;
    dirent_t *ents;
    size_t n, next;
    struct dirent64 cur64;
    struct dirent cur;
} odir_t;

#define DIR_TABLE 4096
static odir_t *g_dirs[DIR_TABLE];
static pthread_mutex_t g_dir_lk = PTHREAD_MUTEX_INITIALIZER;

static int split_list(char *s, char **out, int max) {
    int n = 0;
    /* strtok_r, never strtok: we run inside the host program's calls, and
     * io500 is in the middle of its own strtok() loop when it first stats
     * a path under the prefix (u_create_dir_recursive) */
    char *save = NULL;
    for (char *tok = strtok_r(s, ",", &save); tok && n < max; tok = strtok_r(NULL, ",", &save)) out[n++] = tok;
    return n;
}

static int init_locked(void) {
    const char *prefix = getenv("ROCEPOOL_PREFIX"), *mdts = getenv("ROCEPOOL_MDTS"), *osts = getenv("ROCEPOOL_OSTS");
    if (!prefix || !*prefix || !mdts || !osts) return 0;
    cfg.nmdt = split_list(strdup(mdts), cfg.mdt, MAX_PEERS);
    cfg.nost = split_list(strdup(osts), cfg.ost, MAX_PEERS);
    const char *v;
    /* one program = one I/O stream: a few threads for the stripe fan-out
     * of a large read or a readdir, not the FUSE daemon's 32 */
    cfg.io_threads = 4;
    if ((v = getenv("ROCEPOOL_IO_THREADS")) && atoi(v) > 0) cfg.io_threads = atoi(v);
    if ((v = getenv("ROCEPOOL_ATTR_TTL")) && *v) cfg.attr_ttl = atof(v);
    if ((v = getenv("ROCEPOOL_STRIPE_SIZE")) && atoll(v) > 0) cfg.stripe_size = (uint64_t)atoll(v);
    if ((v = getenv("ROCEPOOL_STRIPE_COUNT")) && atoi(v) > 0) cfg.stripe_count = atoi(v);
    int rc = rpfs_init();
    if (rc) { fprintf(stderr, "rocepool_intercept: init failed: %s\n", strerror(-rc)); return 0; }
    if (g_log) fprintf(stderr, "rocepool_intercept[%d]: serving %s (%d MDT, %d OST)\n", (int)getpid(), g_prefix, cfg.nmdt, cfg.nost);
    return 1;
}

/* 1 when paths under the prefix are served (initialising on first use) */
static int ready(void) {
    pthread_once(&g_libc_once, load_libc);
    pid_t pid = getpid();
    if (g_ready == 1 && pid == g_pid) return 1;
    if (g_ready == 0 && pid == g_pid) return 0;
    pthread_mutex_lock(&g_init_lk);
    if (g_ready != 1 || pid != g_pid) {
        if (g_ready == 1 && pid != g_pid) {
            /* a fork: the child inherited the table but not the core's
             * threads or connections -- start them again for this pid */
            for (int i = 0; i < cfg.nmdt; i++) rp_peer_init(&g_mdts[i], cfg.mdt[i]);
            for (int i = 0; i < cfg.nost; i++) rp_peer_init(&g_osts[i], cfg.ost[i]);
        }
        g_ready = (g_ready == -1 || pid != g_pid) ? (g_ready == 1 ? (rpfs_init() == 0) : init_locked()) : g_ready;
        g_pid = pid;
    }
    pthread_mutex_unlock(&g_init_lk);
    return g_ready == 1;
}

/* ---------------------------------------------------------------- paths */
/* Our path for `path` (absolute or relative to cwd / a directory fd), or
 * NULL when it is not under the prefix. `out` gets the normalised path
 * relative to the prefix (no leading slash, "" = the prefix itself). */
static const char *ours(int dirfd, const char *path, char *out, size_t n) {
    if (!path || !g_prefix_len) return NULL;
    char abs[PATH_MAX * 2];
    if (path[0] == '/') {
        snprintf(abs, sizeof(abs), "%s", path);
    } else if (dirfd == AT_FDCWD) {
        char cwd[PATH_MAX];
        if (!getcwd(cwd, sizeof(cwd))) return NULL;
        snprintf(abs, sizeof(abs), "%s/%s", cwd, path);
    } else {
        /* relative to one of our directory descriptors? */
        const char *base = NULL;
        pthread_mutex_lock(&g_dir_lk);
        for (int i = 0; i < DIR_TABLE; i++)
            if (g_dirs[i] && g_dirs[i]->fd == dirfd) { base = g_dirs[i]->path; break; }
        if (base) snprintf(abs, sizeof(abs), "%s/%s/%s", g_prefix, base, path);
        pthread_mutex_unlock(&g_dir_lk);
        if (!base) return NULL;
    }
    if (strncmp(abs, g_prefix, g_prefix_len) || (abs[g_prefix_len] && abs[g_prefix_len] != '/')) return NULL;
    if (!ready()) return NULL;   /* under the prefix: bring the client up (first use in this process) */
    /* normalise: drop "", ".", resolve ".." within the prefix */
    const char *p = abs + g_prefix_len;
    size_t o = 0;
    while (*p) {
        while (*p == '/') p++;
        const char *st = p;
        while (*p && *p != '/') p++;
        size_t cl = (size_t)(p - st);
        if (cl == 0 || (cl == 1 && st[0] == '.')) continue;
        if (cl == 2 && st[0] == '.' && st[1] == '.') {
            while (o > 0 && out[o - 1] != '/') o--;
            if (o > 0) o--;
            continue;
        }
        if (o + cl + 2 > n) return NULL;
        if (o) out[o++] = '/';
        memcpy(out + o, st, cl);
        o += cl;
    }
    out[o] = 0;
    return out;
}

/* ---------------------------------------------------------------- fds */
static ofile_t *fd_get(int fd) {
    if (fd < 0 || fd >= FD_TABLE) return NULL;
    /* fast path for the host program's own descriptors: no lock */
    if (!__atomic_load_n(&g_fds[fd], __ATOMIC_ACQUIRE)) return NULL;
    pthread_rwlock_rdlock(&g_fd_lk);
    ofile_t *f = g_fds[fd];
    pthread_rwlock_unlock(&g_fd_lk);
    return f;
}

static int reserve_fd(void) {
    int fd = real_open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (fd >= FD_TABLE) { real_close(fd); errno = EMFILE; return -1; }
    return fd;
}

static int seterr(int rc) {  /* -errno -> -1 + errno */
    if (rc >= 0) return rc;
    errno = rc <= -4096 ? EIO : -rc;
    return -1;
}

static void fill_stat(struct stat *st, const char *path, const attr_t *a) {
    memset(st, 0, sizeof(*st));
    st->st_ino = *path ? (ino_t)strhash(path) + 2 : 1;
    st->st_mode = a->is_dir ? (S_IFDIR | 0755) : (S_IFREG | 0644);
    st->st_nlink = a->is_dir ? 2 : 1;
    st->st_uid = getuid();
    st->st_gid = getgid();
    st->st_size = a->is_dir ? 4096 : (off_t)a->size;
    st->st_blksize = 4096;
    st->st_blocks = (blkcnt_t)((a->size + 511) / 512);
    st->st_mtim.tv_sec = (time_t)a->mtime;
    st->st_mtim.tv_nsec = (long)((a->mtime - (double)(time_t)a->mtime) * 1e9);
    st->st_atim = st->st_mtim;
    st->st_ctim.tv_sec = (time_t)a->ctime;
    st->st_ctim.tv_nsec = (long)((a->ctime - (double)(time_t)a->ctime) * 1e9);
}

/* ---------------------------------------------------------------- open/close */
static int do_open(const char *rel, int flags, mode_t mode) {
    (void)mode;
    attr_t a;
    int rc = fs_getattr(rel, &a);
    if (rc == -ENOENT) {
        if (!(flags & O_CREAT)) return seterr(-ENOENT);
        rc = fs_create_file_excl(rel, (flags & O_EXCL) != 0, &a);
        if (rc == -EEXIST && !(flags & O_EXCL)) rc = fs_getattr(rel, &a);  /* created meanwhile */
    } else if (rc == 0) {
        if ((flags & (O_CREAT | O_EXCL)) == (O_CREAT | O_EXCL)) return seterr(-EEXIST);
        if (a.is_dir && (flags & O_ACCMODE) != O_RDONLY) return seterr(-EISDIR);
        if (!a.is_dir && !a.inl) {  /* close-to-open: revalidate a striped file's size */
            ac_del(rel);
            rc = fs_getattr(rel, &a);
        }
    }
    if (rc) {
        if (g_log) fprintf(stderr, "rocepool_intercept[%d]: open %s flags %#x failed: %d\n", (int)getpid(), rel, flags, rc);
        return seterr(rc);
    }
    if (!a.is_dir && (flags & O_TRUNC) && a.size) {
        invalidate_open_caches(rel);
        rc = fs_truncate(rel, 0);
        if (rc) return seterr(rc);
    }
    int fd = reserve_fd();
    if (fd < 0) return -1;
    ofile_t *f = calloc(1, sizeof(*f));
    f->h = handle_new(rel, a.is_dir);
    if (!a.is_dir) { f->h->sc = sc_new(rel, &a); open_register(f->h); }
    f->refs = 1;
    f->is_dir = a.is_dir;
    f->flags = flags;
    pthread_mutex_init(&f->lk, NULL);
    pthread_rwlock_wrlock(&g_fd_lk);
    __atomic_store_n(&g_fds[fd], f, __ATOMIC_RELEASE);
    pthread_rwlock_unlock(&g_fd_lk);
    if (g_log) fprintf(stderr, "rocepool_intercept[%d]: open %s -> fd %d\n", (int)getpid(), rel, fd);
    return fd;
}

int open(const char *path, int flags, ...) {
    ENSURE();
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, flags); mode = va_arg(ap, mode_t); va_end(ap); }
    char rel[PATH_MAX];
    if (ours(AT_FDCWD, path, rel, sizeof(rel))) return do_open(rel, flags, mode);
    return real_open(path, flags, mode);
}

int open64(const char *path, int flags, ...) {
    ENSURE();
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, flags); mode = va_arg(ap, mode_t); va_end(ap); }
    char rel[PATH_MAX];
    if (ours(AT_FDCWD, path, rel, sizeof(rel))) return do_open(rel, flags | O_LARGEFILE, mode);
    return real_open64(path, flags, mode);
}

int openat(int dirfd, const char *path, int flags, ...) {
    ENSURE();
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, flags); mode = va_arg(ap, mode_t); va_end(ap); }
    char rel[PATH_MAX];
    if (ours(dirfd, path, rel, sizeof(rel))) return do_open(rel, flags, mode);
    return real_openat(dirfd, path, flags, mode);
}

int openat64(int dirfd, const char *path, int flags, ...) {
    ENSURE();
    mode_t mode = 0;
    if (flags & (O_CREAT | O_TMPFILE)) { va_list ap; va_start(ap, flags); mode = va_arg(ap, mode_t); va_end(ap); }
    char rel[PATH_MAX];
    if (ours(dirfd, path, rel, sizeof(rel))) return do_open(rel, flags | O_LARGEFILE, mode);
    return real_openat64(dirfd, path, flags, mode);
}

int creat(const char *path, mode_t mode) { return open(path, O_WRONLY | O_CREAT | O_TRUNC, mode); }
int creat64(const char *path, mode_t mode) { return open64(path, O_WRONLY | O_CREAT | O_TRUNC, mode); }

static int flush_file(ofile_t *f) {
    if (f->is_dir) return 0;
    pthread_mutex_lock(&f->h->lk);
    int rc = handle_flush(f->h);
    pthread_mutex_unlock(&f->h->lk);
    return rc;
}

int open(const char *path, int flags, ...);
int open64(const char *path, int flags, ...);
int openat(int dirfd, const char *path, int flags, ...);
int openat64(int dirfd, const char *path, int flags, ...);
/* _FORTIFY_SOURCE routes a 2-argument open()/open64()/openat() here */
int __open_2(const char *path, int flags) { return open(path, flags); }
int __open64_2(const char *path, int flags) { return open64(path, flags); }
int __openat_2(int dirfd, const char *path, int flags) { return openat(dirfd, path, flags); }
int __openat64_2(int dirfd, const char *path, int flags) { return openat64(dirfd, path, flags); }

int close(int fd) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    if (!f) return real_close(fd);
    pthread_rwlock_wrlock(&g_fd_lk);
    __atomic_store_n(&g_fds[fd], NULL, __ATOMIC_RELEASE);
    int last = --f->refs == 0;
    pthread_rwlock_unlock(&g_fd_lk);
    int rc = 0;
    if (last) {
        rc = flush_file(f);  /* the error of a failed flush is reported here, like FUSE's FLUSH */
        /* close-to-open for the attributes too: a stat after close asks the
         * MDT, so a shared file another rank extended is seen at its full
         * size (IOR's aggregate-size check after the write phase) */
        if (!f->is_dir) ac_del(f->h->path);
        handle_free(f->h);
        pthread_mutex_destroy(&f->lk);
        free(f);
    }
    real_close(fd);
    if (rc < 0) {  /* a program sees only "close failed": say why, a few times per process */
        static _Atomic int said;
        if (atomic_fetch_add(&said, 1) < 5)
            fprintf(stderr, "rocepool_intercept[%d]: close(%d): flushing the file failed: %s (%d)\n", (int)getpid(), fd,
                    rc <= -4096 ? "client error" : strerror(-rc), rc);
    }
    return seterr(rc);
}

/* ---------------------------------------------------------------- I/O */
static ssize_t do_pwrite(ofile_t *f, const void *buf, size_t n, uint64_t off) {
    if (f->is_dir) return seterr(-EISDIR);
    if ((f->flags & O_ACCMODE) == O_RDONLY) return seterr(-EBADF);
    if (!n) return 0;
    pthread_mutex_lock(&f->h->lk);
    int rc = handle_write(f->h, off, buf, n, NULL);
    pthread_mutex_unlock(&f->h->lk);
    return rc ? seterr(rc) : (ssize_t)n;
}

static ssize_t do_pread(ofile_t *f, void *buf, size_t n, uint64_t off) {
    if (f->is_dir) return seterr(-EISDIR);
    if ((f->flags & O_ACCMODE) == O_WRONLY) return seterr(-EBADF);
    if (!n) return 0;
    pthread_mutex_lock(&f->h->lk);
    ssize_t got = handle_read(f->h, off, buf, n);
    pthread_mutex_unlock(&f->h->lk);
    return got < 0 ? seterr((int)got) : got;
}

static int file_size(ofile_t *f, uint64_t *size) {
    int rc = flush_file(f);
    attr_t a;
    if (!rc) rc = fs_getattr(f->h->path, &a);
    if (!rc) *size = a.size;
    return rc;
}

ssize_t write(int fd, const void *buf, size_t n) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    if (!f) return real_write(fd, buf, n);
    pthread_mutex_lock(&f->lk);
    if (f->flags & O_APPEND) {
        uint64_t size;
        int rc = file_size(f, &size);
        if (rc) { pthread_mutex_unlock(&f->lk); return seterr(rc); }
        f->pos = size;
    }
    ssize_t w = do_pwrite(f, buf, n, f->pos);
    if (w > 0) f->pos += (uint64_t)w;
    pthread_mutex_unlock(&f->lk);
    return w;
}

ssize_t read(int fd, void *buf, size_t n) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    if (!f) return real_read(fd, buf, n);
    pthread_mutex_lock(&f->lk);
    ssize_t r = do_pread(f, buf, n, f->pos);
    if (r > 0) f->pos += (uint64_t)r;
    pthread_mutex_unlock(&f->lk);
    return r;
}

ssize_t pwrite(int fd, const void *buf, size_t n, off_t off) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? do_pwrite(f, buf, n, (uint64_t)off) : real_pwrite(fd, buf, n, off);
}
ssize_t pwrite64(int fd, const void *buf, size_t n, off64_t off) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? do_pwrite(f, buf, n, (uint64_t)off) : real_pwrite64(fd, buf, n, off);
}
ssize_t pread(int fd, void *buf, size_t n, off_t off) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? do_pread(f, buf, n, (uint64_t)off) : real_pread(fd, buf, n, off);
}
ssize_t pread64(int fd, void *buf, size_t n, off64_t off) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? do_pread(f, buf, n, (uint64_t)off) : real_pread64(fd, buf, n, off);
}

ssize_t writev(int fd, const struct iovec *iov, int cnt) {
    ENSURE();
    if (!fd_get(fd)) return real_writev(fd, iov, cnt);
    ssize_t total = 0;
    for (int i = 0; i < cnt; i++) {
        ssize_t w = write(fd, iov[i].iov_base, iov[i].iov_len);
        if (w < 0) return total ? total : w;
        total += w;
        if ((size_t)w < iov[i].iov_len) break;
    }
    return total;
}

ssize_t readv(int fd, const struct iovec *iov, int cnt) {
    ENSURE();
    if (!fd_get(fd)) return real_readv(fd, iov, cnt);
    ssize_t total = 0;
    for (int i = 0; i < cnt; i++) {
        ssize_t r = read(fd, iov[i].iov_base, iov[i].iov_len);
        if (r < 0) return total ? total : r;
        total += r;
        if ((size_t)r < iov[i].iov_len) break;
    }
    return total;
}

/* _FORTIFY_SOURCE variants: a read into a buffer of known size compiles to
 * these; without them our descriptors would be read as the /dev/null they
 * are backed by */
ssize_t __read_chk(int fd, void *buf, size_t n, size_t buflen) {
    ENSURE();
    if (n > buflen) { fprintf(stderr, "rocepool_intercept: read buffer overflow\n"); abort(); }
    return read(fd, buf, n);
}
ssize_t __pread_chk(int fd, void *buf, size_t n, off_t off, size_t buflen) {
    ENSURE();
    if (n > buflen) { fprintf(stderr, "rocepool_intercept: pread buffer overflow\n"); abort(); }
    return pread(fd, buf, n, off);
}
ssize_t __pread64_chk(int fd, void *buf, size_t n, off64_t off, size_t buflen) {
    ENSURE();
    if (n > buflen) { fprintf(stderr, "rocepool_intercept: pread64 buffer overflow\n"); abort(); }
    return pread64(fd, buf, n, off);
}

static off64_t do_lseek(ofile_t *f, off64_t off, int whence) {
    pthread_mutex_lock(&f->lk);
    int64_t base = 0;
    if (whence == SEEK_CUR) base = (int64_t)f->pos;
    else if (whence == SEEK_END) {
        uint64_t size;
        int rc = file_size(f, &size);
        if (rc) { pthread_mutex_unlock(&f->lk); return seterr(rc); }
        base = (int64_t)size;
    } else if (whence != SEEK_SET) { pthread_mutex_unlock(&f->lk); errno = EINVAL; return -1; }
    int64_t np = base + off;
    if (np < 0) { pthread_mutex_unlock(&f->lk); errno = EINVAL; return -1; }
    f->pos = (uint64_t)np;
    pthread_mutex_unlock(&f->lk);
    return (off64_t)np;
}

off_t lseek(int fd, off_t off, int whence) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? (off_t)do_lseek(f, off, whence) : real_lseek(fd, off, whence);
}
off64_t lseek64(int fd, off64_t off, int whence) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? do_lseek(f, off, whence) : real_lseek64(fd, off, whence);
}

/* For the GPU-direct library (rocepool_cufile.c) in the same process: the
 * file a descriptor of ours names (its path relative to the prefix), and
 * its buffered writes pushed out before device I/O reads the file. 0, or
 * -1 when fd is not ours. */
int rp_intercept_fd_path(int fd, char *out, size_t n) {
    if (!ready()) return -1;
    ofile_t *f = fd_get(fd);
    if (!f || f->is_dir || !f->h) return -1;
    snprintf(out, n, "%s", f->h->path);
    return 0;
}
int rp_intercept_fd_flush(int fd) {
    if (!ready()) return -1;
    ofile_t *f = fd_get(fd);
    return f ? flush_file(f) : -1;
}

int fsync(int fd) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? seterr(flush_file(f)) : real_fsync(fd);
}
int fdatasync(int fd) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? seterr(flush_file(f)) : real_fdatasync(fd);
}

static int do_ftruncate(ofile_t *f, uint64_t size) {
    if (f->is_dir) return seterr(-EISDIR);
    int rc = flush_file(f);
    if (!rc) { invalidate_open_caches(f->h->path); rc = fs_truncate(f->h->path, size); }
    return seterr(rc);
}
int ftruncate(int fd, off_t len) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? do_ftruncate(f, (uint64_t)len) : real_ftruncate(fd, len);
}
int ftruncate64(int fd, off64_t len) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? do_ftruncate(f, (uint64_t)len) : real_ftruncate64(fd, len);
}
int truncate(const char *path, off_t len) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_truncate(path, len);
    invalidate_open_caches(rel);
    return seterr(fs_truncate(rel, (uint64_t)len));
}
int truncate64(const char *path, off64_t len) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_truncate64(path, len);
    invalidate_open_caches(rel);
    return seterr(fs_truncate(rel, (uint64_t)len));
}

/* ---------------------------------------------------------------- dup / fcntl */
static int share_fd(int oldfd, int newfd) {  /* newfd now refers to oldfd's file */
    ofile_t *f = fd_get(oldfd);
    pthread_rwlock_wrlock(&g_fd_lk);
    if (newfd >= 0 && newfd < FD_TABLE) {
        ofile_t *prev = g_fds[newfd];
        if (prev && prev != f && --prev->refs == 0) { handle_free(prev->h); free(prev); }
        __atomic_store_n(&g_fds[newfd], f, __ATOMIC_RELEASE);
        f->refs++;
    }
    pthread_rwlock_unlock(&g_fd_lk);
    return newfd;
}

int dup(int fd) {
    ENSURE();
    if (!fd_get(fd)) return real_dup(fd);
    int nfd = real_dup(fd);
    return nfd < 0 ? nfd : share_fd(fd, nfd);
}
int dup2(int fd, int nfd) {
    ENSURE();
    if (!fd_get(fd)) { if (fd_get(nfd)) close(nfd); return real_dup2(fd, nfd); }
    if (fd == nfd) return nfd;
    if (fd_get(nfd)) close(nfd);
    int r = real_dup2(fd, nfd);
    return r < 0 ? r : share_fd(fd, nfd);
}
int dup3(int fd, int nfd, int flags) {
    ENSURE();
    if (!fd_get(fd)) { if (fd_get(nfd)) close(nfd); return real_dup3(fd, nfd, flags); }
    if (fd_get(nfd)) close(nfd);
    int r = real_dup3(fd, nfd, flags);
    return r < 0 ? r : share_fd(fd, nfd);
}

int fcntl(int fd, int cmd, ...) {
    ENSURE();
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *);
    va_end(ap);
    ofile_t *f = fd_get(fd);
    if (!f) return real_fcntl(fd, cmd, arg);
    switch (cmd) {
    case F_GETFL: return f->flags;
    case F_SETFL: f->flags = (f->flags & O_ACCMODE) | ((int)(intptr_t)arg & ~O_ACCMODE); return 0;
    case F_GETFD: return real_fcntl(fd, cmd, arg);
    case F_SETFD: return real_fcntl(fd, cmd, arg);
    case F_DUPFD: case F_DUPFD_CLOEXEC: {
        int nfd = real_fcntl(fd, cmd, arg);
        return nfd < 0 ? nfd : share_fd(fd, nfd);
    }
    default: return 0;  /* locks, hints: accepted */
    }
}
int fcntl64(int fd, int cmd, ...) {   /* what _FILE_OFFSET_BITS=64 programs (Python) call */
    va_list ap;
    va_start(ap, cmd);
    void *arg = va_arg(ap, void *);
    va_end(ap);
    return fcntl(fd, cmd, arg);
}

/* ---------------------------------------------------------------- stat */
static int do_stat_path(const char *rel, struct stat *st) {
    attr_t a;
    int rc = fs_getattr(rel, &a);
    if (g_log > 1) fprintf(stderr, "rocepool_intercept[%d]: stat %s -> %d\n", (int)getpid(), rel, rc);
    if (rc) return seterr(rc);
    fill_stat(st, rel, &a);
    return 0;
}

static int do_fstat(ofile_t *f, struct stat *st) {
    int rc = flush_file(f);
    attr_t a;
    if (!rc) rc = fs_getattr(f->h->path, &a);
    if (rc) return seterr(rc);
    fill_stat(st, f->h->path, &a);
    return 0;
}

int stat(const char *path, struct stat *st) {
    ENSURE();
    char rel[PATH_MAX];
    return ours(AT_FDCWD, path, rel, sizeof(rel)) ? do_stat_path(rel, st) : real_stat(path, st);
}
int stat64(const char *path, struct stat64 *st) {
    ENSURE();
    char rel[PATH_MAX];
    return ours(AT_FDCWD, path, rel, sizeof(rel)) ? do_stat_path(rel, (struct stat *)st) : real_stat64(path, st);
}
int lstat(const char *path, struct stat *st) {
    ENSURE();
    char rel[PATH_MAX];
    return ours(AT_FDCWD, path, rel, sizeof(rel)) ? do_stat_path(rel, st) : real_lstat(path, st);
}
int lstat64(const char *path, struct stat64 *st) {
    ENSURE();
    char rel[PATH_MAX];
    return ours(AT_FDCWD, path, rel, sizeof(rel)) ? do_stat_path(rel, (struct stat *)st) : real_lstat64(path, st);
}
int fstat(int fd, struct stat *st) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? do_fstat(f, st) : real_fstat(fd, st);
}
int fstat64(int fd, struct stat64 *st) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    return f ? do_fstat(f, (struct stat *)st) : real_fstat64(fd, st);
}
int fstatat(int dirfd, const char *path, struct stat *st, int flags) {
    ENSURE();
    char rel[PATH_MAX];
    if (ours(dirfd, path, rel, sizeof(rel))) return do_stat_path(rel, st);
    return real_fstatat(dirfd, path, st, flags);
}
int fstatat64(int dirfd, const char *path, struct stat64 *st, int flags) {
    ENSURE();
    char rel[PATH_MAX];
    if (ours(dirfd, path, rel, sizeof(rel))) return do_stat_path(rel, (struct stat *)st);
    return real_fstatat64(dirfd, path, st, flags);
}
/* older glibc / older binaries call these */
int __xstat(int ver, const char *path, struct stat *st) { (void)ver; return stat(path, st); }
int __xstat64(int ver, const char *path, struct stat64 *st) { (void)ver; return stat64(path, st); }
int __lxstat(int ver, const char *path, struct stat *st) { (void)ver; return lstat(path, st); }
int __lxstat64(int ver, const char *path, struct stat64 *st) { (void)ver; return lstat64(path, st); }
int __fxstat(int ver, int fd, struct stat *st) { (void)ver; return fstat(fd, st); }
int __fxstat64(int ver, int fd, struct stat64 *st) { (void)ver; return fstat64(fd, st); }
int __fxstatat(int ver, int dirfd, const char *path, struct stat *st, int flags) {
    ENSURE();
    (void)ver; return fstatat(dirfd, path, st, flags);
}
int __fxstatat64(int ver, int dirfd, const char *path, struct stat64 *st, int flags) {
    ENSURE();
    (void)ver; return fstatat64(dirfd, path, st, flags);
}

/* ---------------------------------------------------------------- namespace */
int unlink(const char *path) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_unlink(path);
    if (!*rel) return seterr(-EISDIR);
    invalidate_open_caches(rel);
    return seterr(fs_remove_file(rel));
}
int unlinkat(int dirfd, const char *path, int flags) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(dirfd, path, rel, sizeof(rel))) return real_unlinkat(dirfd, path, flags);
    if (!*rel) return seterr((flags & AT_REMOVEDIR) ? -EBUSY : -EISDIR);
    if (flags & AT_REMOVEDIR) return seterr(fs_rmdir(rel));
    invalidate_open_caches(rel);
    return seterr(fs_remove_file(rel));
}
int mkdir(const char *path, mode_t mode) {
    ENSURE();
    (void)mode;
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_mkdir(path, mode);
    if (!*rel) return seterr(-EEXIST);   /* the prefix itself is the pool's root */
    attr_t a;
    int rc = fs_mkdir(rel, &a);
    if (g_log) fprintf(stderr, "rocepool_intercept[%d]: mkdir %s -> %d\n", (int)getpid(), rel, rc);
    return seterr(rc);
}
int mkdirat(int dirfd, const char *path, mode_t mode) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(dirfd, path, rel, sizeof(rel))) return real_mkdirat(dirfd, path, mode);
    if (!*rel) return seterr(-EEXIST);
    attr_t a;
    return seterr(fs_mkdir(rel, &a));
}
int rmdir(const char *path) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_rmdir(path);
    if (!*rel) return seterr(-EBUSY);
    return seterr(fs_rmdir(rel));
}
int access(const char *path, int mode) {
    ENSURE();
    (void)mode;
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_access(path, mode);
    attr_t a;
    return seterr(fs_getattr(rel, &a));
}
int faccessat(int dirfd, const char *path, int mode, int flags) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(dirfd, path, rel, sizeof(rel))) return real_faccessat(dirfd, path, mode, flags);
    attr_t a;
    return seterr(fs_getattr(rel, &a));
}
int rename(const char *a, const char *b) {
    ENSURE();
    char ra[PATH_MAX], rb[PATH_MAX];
    int oa = ours(AT_FDCWD, a, ra, sizeof(ra)) != NULL, ob = ours(AT_FDCWD, b, rb, sizeof(rb)) != NULL;
    if (!oa && !ob) return real_rename(a, b);
    errno = EXDEV;  /* not supported on this file system (a client-side copy would not be atomic) */
    return -1;
}
int renameat(int da, const char *a, int db, const char *b) {
    ENSURE();
    char ra[PATH_MAX], rb[PATH_MAX];
    int oa = ours(da, a, ra, sizeof(ra)) != NULL, ob = ours(db, b, rb, sizeof(rb)) != NULL;
    if (!oa && !ob) return real_renameat(da, a, db, b);
    errno = EXDEV;
    return -1;
}
int chmod(const char *path, mode_t mode) {
    ENSURE();
    char rel[PATH_MAX];
    return ours(AT_FDCWD, path, rel, sizeof(rel)) ? 0 : real_chmod(path, mode);
}
int fchmod(int fd, mode_t mode) { ENSURE(); return fd_get(fd) ? 0 : real_fchmod(fd, mode); }
int chown(const char *path, uid_t u, gid_t g) {
    ENSURE();
    char rel[PATH_MAX];
    return ours(AT_FDCWD, path, rel, sizeof(rel)) ? 0 : real_chown(path, u, g);
}
int utimes(const char *path, const struct timeval tv[2]) {
    ENSURE();
    char rel[PATH_MAX];
    return ours(AT_FDCWD, path, rel, sizeof(rel)) ? 0 : real_utimes(path, tv);
}
int futimens(int fd, const struct timespec ts[2]) { ENSURE(); return fd_get(fd) ? 0 : real_futimens(fd, ts); }
int utimensat(int dirfd, const char *path, const struct timespec ts[2], int flags) {
    ENSURE();
    char rel[PATH_MAX];
    return ours(dirfd, path, rel, sizeof(rel)) ? 0 : real_utimensat(dirfd, path, ts, flags);
}

char *realpath(const char *path, char *resolved) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_realpath(path, resolved);
    attr_t a;
    int rc = fs_getattr(rel, &a);
    if (rc) { seterr(rc); return NULL; }
    char *out = resolved ? resolved : malloc(PATH_MAX);
    if (!out) return NULL;
    if (join_prefix(out, PATH_MAX, rel) < 0) { if (!resolved) free(out); errno = ENAMETOOLONG; return NULL; }
    return out;
}
char *__realpath_chk(const char *path, char *resolved, size_t resolvedlen) {  /* fortified (pfind) */
    if (resolvedlen < PATH_MAX) { fprintf(stderr, "rocepool_intercept: realpath buffer too small\n"); abort(); }
    return realpath(path, resolved);
}

/* ------------------------------------------------------------- stdio
 * libc's own FILE streams read and write through libc-internal entry
 * points that cannot be interposed, so a stream on one of our files is
 * built with fopencookie over our descriptor instead. */
static ssize_t ck_read(void *c, char *buf, size_t n) { return read((int)(intptr_t)c, buf, n); }
static ssize_t ck_write(void *c, const char *buf, size_t n) {
    ssize_t w = write((int)(intptr_t)c, buf, n);
    return w < 0 ? 0 : w;  /* fopencookie: a short write reports the error */
}
static int ck_seek(void *c, off64_t *pos, int whence) {
    off64_t r = lseek64((int)(intptr_t)c, *pos, whence);
    if (r < 0) return -1;
    *pos = r;
    return 0;
}
static int ck_close(void *c) { return close((int)(intptr_t)c); }

static FILE *stream_over(int fd, const char *mode) {
    cookie_io_functions_t io = {ck_read, ck_write, ck_seek, ck_close};
    FILE *fp = fopencookie((void *)(intptr_t)fd, mode, io);
    if (!fp) close(fd);
    return fp;
}

static int mode_flags(const char *mode) {
    int acc = 0, extra = 0;
    switch (mode[0]) {
    case 'r': acc = O_RDONLY; break;
    case 'w': acc = O_WRONLY; extra = O_CREAT | O_TRUNC; break;
    case 'a': acc = O_WRONLY; extra = O_CREAT | O_APPEND; break;
    default: errno = EINVAL; return -1;
    }
    for (const char *m = mode + 1; *m; m++) {
        if (*m == '+') acc = O_RDWR;
        else if (*m == 'x') extra |= O_EXCL;
    }
    return acc | extra;
}

FILE *fopen(const char *path, const char *mode) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_fopen(path, mode);
    int flags = mode_flags(mode);
    if (flags < 0) return NULL;
    int fd = do_open(rel, flags, 0666);
    return fd < 0 ? NULL : stream_over(fd, mode);
}
FILE *fopen64(const char *path, const char *mode) { return fopen(path, mode); }
FILE *fdopen(int fd, const char *mode) {
    ENSURE();
    return fd_get(fd) ? stream_over(fd, mode) : real_fdopen(fd, mode);
}

int chdir(const char *path) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_chdir(path);
    errno = ENOTSUP;  /* a cwd inside the prefix would need every relative path routed here; not supported */
    return -1;
}
int fchdir(int fd) { ENSURE(); if (fd_get(fd)) { errno = ENOTSUP; return -1; } return real_fchdir(fd); }

static void fake_statfs(struct statfs *st) {
    memset(st, 0, sizeof(*st));
    st->f_type = 0x52435031;  /* "RCP1" */
    st->f_bsize = 4096;
    st->f_frsize = 4096;
    st->f_blocks = (1ull << 40) / 4096;
    st->f_bfree = st->f_bavail = st->f_blocks / 2;
    st->f_files = 1ull << 30;
    st->f_ffree = st->f_files / 2;
    st->f_namelen = 255;
}
int statfs(const char *path, struct statfs *st) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_statfs(path, st);
    fake_statfs(st);
    return 0;
}
int statvfs(const char *path, struct statvfs *st) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_statvfs(path, st);
    struct statfs fs;
    fake_statfs(&fs);
    memset(st, 0, sizeof(*st));
    st->f_bsize = fs.f_bsize; st->f_frsize = fs.f_frsize; st->f_blocks = fs.f_blocks;
    st->f_bfree = fs.f_bfree; st->f_bavail = fs.f_bavail; st->f_files = fs.f_files; st->f_ffree = fs.f_ffree;
    st->f_favail = fs.f_ffree; st->f_namemax = 255;
    return 0;
}

/* ---------------------------------------------------------------- directories */
static odir_t *dir_of(DIR *d) {
    if (!d) return NULL;
    pthread_mutex_lock(&g_dir_lk);
    odir_t *hit = NULL;
    for (int i = 0; i < DIR_TABLE; i++)
        if ((DIR *)g_dirs[i] == d) { hit = g_dirs[i]; break; }
    pthread_mutex_unlock(&g_dir_lk);
    return hit;
}

static void dir_drop(odir_t *d) {
    for (size_t i = 0; i < d->n; i++) free(d->ents[i].name);
    free(d->ents);
    d->ents = NULL;
    d->n = d->next = 0;
}

/* fetch the listing (all MDTs, merged) and prime the attribute cache for
 * the stats that usually follow a readdir */
static int dir_list(odir_t *d) {
    int rc = mdt_readdir(d->path, &d->ents, &d->n);
    if (rc) return rc;
    for (size_t i = 0; i < d->n; i++) {
        char child[PATH_MAX];
        int w = *d->path ? snprintf(child, sizeof(child), "%s/%s", d->path, d->ents[i].name)
                         : snprintf(child, sizeof(child), "%s", d->ents[i].name);
        if (w > 0 && (size_t)w < sizeof(child)) ac_put(child, &d->ents[i].a);
    }
    return 0;
}

DIR *opendir(const char *path) {
    ENSURE();
    char rel[PATH_MAX];
    if (!ours(AT_FDCWD, path, rel, sizeof(rel))) return real_opendir(path);
    attr_t a;
    int rc = fs_getattr(rel, &a);
    if (!rc && !a.is_dir) rc = -ENOTDIR;
    if (rc) { seterr(rc); return NULL; }
    odir_t *d = calloc(1, sizeof(*d));
    d->fd = reserve_fd();
    if (d->fd < 0) { free(d); return NULL; }
    d->path = strdup(rel);
    rc = dir_list(d);  /* one listing, served to every readdir */
    if (rc) { real_close(d->fd); free(d->path); free(d); seterr(rc); return NULL; }
    pthread_mutex_lock(&g_dir_lk);
    int slot = -1;
    for (int i = 0; i < DIR_TABLE; i++) if (!g_dirs[i]) { g_dirs[i] = d; slot = i; break; }
    pthread_mutex_unlock(&g_dir_lk);
    if (slot < 0) { real_close(d->fd); dir_drop(d); free(d->path); free(d); errno = EMFILE; return NULL; }
    return (DIR *)d;
}

DIR *fdopendir(int fd) {
    ENSURE();
    ofile_t *f = fd_get(fd);
    if (!f) return real_fdopendir(fd);
    char full[PATH_MAX];
    if (join_prefix(full, sizeof(full), f->h->path) < 0) { errno = ENAMETOOLONG; return NULL; }
    DIR *d = opendir(full);
    if (d) close(fd);
    return d;
}

static void fill_dirent(odir_t *d, size_t i) {
    const dirent_t *e = &d->ents[i];
    char child[PATH_MAX];
    int w = *d->path ? snprintf(child, sizeof(child), "%s/%s", d->path, e->name) : snprintf(child, sizeof(child), "%s", e->name);
    ino_t ino = w > 0 && (size_t)w < sizeof(child) ? (ino_t)strhash(child) + 2 : 2;
    memset(&d->cur64, 0, sizeof(d->cur64));
    d->cur64.d_ino = ino;
    d->cur64.d_off = (off64_t)(i + 1);
    d->cur64.d_type = e->a.is_dir ? DT_DIR : DT_REG;
    snprintf(d->cur64.d_name, sizeof(d->cur64.d_name), "%s", e->name);
    d->cur64.d_reclen = (unsigned short)sizeof(d->cur64);
    memset(&d->cur, 0, sizeof(d->cur));
    d->cur.d_ino = ino;
    d->cur.d_off = (off_t)(i + 1);
    d->cur.d_type = d->cur64.d_type;
    snprintf(d->cur.d_name, sizeof(d->cur.d_name), "%s", e->name);
    d->cur.d_reclen = (unsigned short)sizeof(d->cur);
}

struct dirent *readdir(DIR *dp) {
    ENSURE();
    odir_t *d = dir_of(dp);
    if (!d) return real_readdir(dp);
    if (d->next >= d->n) return NULL;
    fill_dirent(d, d->next++);
    return &d->cur;
}
struct dirent64 *readdir64(DIR *dp) {
    ENSURE();
    odir_t *d = dir_of(dp);
    if (!d) return real_readdir64(dp);
    if (d->next >= d->n) return NULL;
    fill_dirent(d, d->next++);
    return &d->cur64;
}
void rewinddir(DIR *dp) {
    ENSURE();
    odir_t *d = dir_of(dp);
    if (!d) { real_rewinddir(dp); return; }
    dir_drop(d);
    dir_list(d);   /* POSIX: the stream reflects the directory as of the rewind */
    d->next = 0;
}
int dirfd(DIR *dp) {
    ENSURE();
    odir_t *d = dir_of(dp);
    return d ? d->fd : real_dirfd(dp);
}
int closedir(DIR *dp) {
    ENSURE();
    odir_t *d = dir_of(dp);
    if (!d) return real_closedir(dp);
    pthread_mutex_lock(&g_dir_lk);
    for (int i = 0; i < DIR_TABLE; i++) if (g_dirs[i] == d) { g_dirs[i] = NULL; break; }
    pthread_mutex_unlock(&g_dir_lk);
    real_close(d->fd);
    dir_drop(d);
    free(d->path);
    free(d);
    return 0;
}
