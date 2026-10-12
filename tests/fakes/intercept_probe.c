/* A plain C program run under LD_PRELOAD=librocepool_intercept.so by
 * tests/test_intercept.py: the libc entry points a Python host does not
 * reach (opendir/readdir, fstatat/unlinkat on a dirfd, dup2, the fortified
 * 2-argument open64, writev, realpath). argv[1] = the prefix.
 * Prints "OK" or "FAIL <what>" and exits non-zero on failure. */
#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#define CHECK(c) do { if (!(c)) { printf("FAIL %s (line %d, errno %s)\n", #c, __LINE__, strerror(errno)); return 1; } } while (0)

int main(int argc, char **argv) {
    if (argc < 2) return 2;
    char d[2048], p[4096];
    if (strlen(argv[1]) > 1000) return 2;
    snprintf(d, sizeof(d), "%s/cprobe", argv[1]);
    CHECK(mkdir(d, 0755) == 0 || errno == EEXIST);
    /* 3-argument create, writev, close */
    snprintf(p, sizeof(p), "%s/a", d);
    int fd = open(p, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    CHECK(fd >= 0);
    struct iovec iov[3] = {{"hel", 3}, {"lo ", 3}, {"world", 5}};
    CHECK(writev(fd, iov, 3) == 11);
    /* dup2 onto a descriptor of ours and a stranger */
    int fd2 = dup(fd);
    CHECK(fd2 >= 0 && fd2 != fd);
    CHECK(write(fd2, "!", 1) == 1);
    CHECK(close(fd) == 0);
    CHECK(fsync(fd2) == 0);
    CHECK(close(fd2) == 0);
    /* the fortified 2-argument open64 (what IOR compiles to) */
    volatile int ro = O_RDONLY;
    fd = open64(p, ro);
    CHECK(fd >= 0);
    char buf[64] = {0};
    CHECK(pread(fd, buf, sizeof(buf), 0) == 12 && strcmp(buf, "hello world!") == 0);
    CHECK(lseek(fd, 6, SEEK_SET) == 6);
    CHECK(read(fd, buf, 5) == 5 && memcmp(buf, "world", 5) == 0);
    CHECK(lseek(fd, 0, SEEK_END) == 12);
    struct stat st;
    CHECK(fstat(fd, &st) == 0 && st.st_size == 12 && S_ISREG(st.st_mode));
    CHECK(close(fd) == 0);
    CHECK(stat(p, &st) == 0 && st.st_size == 12);
    CHECK(stat(d, &st) == 0 && S_ISDIR(st.st_mode));
    /* a second file, then opendir/readdir/fstatat/unlinkat on the dirfd */
    snprintf(p, sizeof(p), "%s/b", d);
    fd = creat(p, 0644);
    CHECK(fd >= 0 && write(fd, "b", 1) == 1 && close(fd) == 0);
    DIR *dir = opendir(d);
    CHECK(dir);
    int seen_a = 0, seen_b = 0, other = 0;
    struct dirent *e;
    while ((e = readdir(dir))) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        if (!strcmp(e->d_name, "a")) seen_a = 1;
        else if (!strcmp(e->d_name, "b")) seen_b = 1;
        else other++;
    }
    CHECK(seen_a && seen_b && other == 0);
    int dfd = dirfd(dir);
    CHECK(dfd >= 0);
    CHECK(fstatat(dfd, "a", &st, 0) == 0 && st.st_size == 12);
    CHECK(fstatat(dfd, "b", &st, 0) == 0 && st.st_size == 1);
    CHECK(fstatat(dfd, "zz", &st, 0) == -1 && errno == ENOENT);
    CHECK(unlinkat(dfd, "b", 0) == 0);
    CHECK(fstatat(dfd, "b", &st, 0) == -1 && errno == ENOENT);
    rewinddir(dir);
    seen_b = 0;
    while ((e = readdir(dir))) if (!strcmp(e->d_name, "b")) seen_b = 1;
    CHECK(!seen_b);
    CHECK(closedir(dir) == 0);
    /* openat relative to our dirfd */
    dir = opendir(d);
    CHECK(dir);
    fd = openat(dirfd(dir), "c", O_CREAT | O_WRONLY, 0644);
    CHECK(fd >= 0 && write(fd, "ccc", 3) == 3 && close(fd) == 0);
    CHECK(closedir(dir) == 0);
    snprintf(p, sizeof(p), "%s/c", d);
    CHECK(stat(p, &st) == 0 && st.st_size == 3);
    /* realpath, access, truncate, unlink, rmdir */
    char *rp = realpath(p, NULL);
    CHECK(rp && strcmp(rp, p) == 0);
    free(rp);
    CHECK(access(p, F_OK) == 0 && access(p, R_OK | W_OK) == 0);
    CHECK(truncate(p, 1) == 0 && stat(p, &st) == 0 && st.st_size == 1);
    CHECK(unlink(p) == 0);
    snprintf(p, sizeof(p), "%s/a", d);
    CHECK(unlink(p) == 0);
    CHECK(rmdir(d) == 0);
    CHECK(stat(d, &st) == -1 && errno == ENOENT);
    /* a path outside the prefix still goes to the kernel */
    CHECK(stat("/", &st) == 0 && S_ISDIR(st.st_mode));
    fd = open("/dev/null", O_RDONLY);
    CHECK(fd >= 0 && read(fd, buf, 1) == 0 && close(fd) == 0);
    printf("OK\n");
    return 0;
}
