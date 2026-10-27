/* rocepool_fs.h -- the native client file-system core (rocepool_fs.c),
 * shared by the FUSE daemon and the interception library. */
#ifndef ROCEPOOL_FS_H
#define ROCEPOOL_FS_H

#include "rocepool_client.h"
#include "rocepool_devmem.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define MAX_PEERS 256
#define SCACHE_SLOTS 64
#define FUSE_MAX_WRITE (1u << 20)                     /* largest kernel WRITE/READ request */
#define FUSE_BUF_SIZE (FUSE_MAX_WRITE + 64 * 1024)    /* a request buffer: data + headers */
extern size_t g_dirty_cap;                            /* buffered writes per handle before a flush */
#define DIRTY_CAP g_dirty_cap

typedef struct {
    char *mount;
    char *mdt[MAX_PEERS];
    int nmdt;
    char *ost[MAX_PEERS];
    int nost;
    double attr_ttl;
    int threads, io_threads;
    int scache_slots;        /* whole-stripe read cache slots per open file (env ROCEPOOL_SCACHE_SLOTS) */
    int stripe_count;  /* 0 = all OSTs */
    uint64_t stripe_size;
    int kernel_cache_s;
    int readahead;     /* stripes; -1 = 2 x OSTs */
    int inline_ok;     /* create new files as Data-on-MDT (fs/mdt_wire.py); env ROCEPOOL_CLIENT_INLINE=0 off */
    int dir_stripe;    /* fs/namespace_shard.py: 1 = entry on hash(path), a directory spans every MDT;
                          0 = hash(parent), a directory on one MDT. Env ROCEPOOL_DIR_STRIPE. */
    int uring;         /* FUSE over io_uring (--uring 1 / ROCEPOOL_FUSE_URING=1; needs a 6.14+ kernel with
                          fuse.enable_uring=Y). Off by default: tools/io500_run.py measures both ways on the
                          hosts at hand and picks (it lost ~10% on this project's 2-core emulated rig). */
    int uring_entries; /* ring entries per queue (one queue per possible CPU) */
    int direct_read;   /* one-sided reads of stripes (OST LEASE + RDMA READ); env ROCEPOOL_DIRECT_READ=0 off */
} rpfs_cfg_t;
extern rpfs_cfg_t cfg;

/* a file's attributes as the MDT entry gives them (fs/mdt_wire.py) */
typedef struct {
    int is_dir, has_layout;
    int inl, frozen;   /* Data-on-MDT: bytes live in the MDT entry (fs/mdt_wire.py) */
    uint64_t size;
    double mtime, ctime;
    uint32_t sc, so;
    uint64_t ss;
} attr_t;

/* internal (non-errno) results of the Data-on-MDT ops; never reach the kernel */
#define E_NOT_INLINE (-10001)
#define E_INLINE_FULL (-10002)
#define E_FROZEN (-10003)
#define E_INLINE (-10004)

typedef struct {
    char *name;
    attr_t a;
} dirent_t;

/* per open file: whole stripes, LRU, readahead (fs/posix_api.py StripeReadCache) */
typedef struct {
    int64_t idx;       /* -1 = free */
    int state;         /* 1 in flight, 2 ready */
    uint64_t gen;
    uint8_t *data;
    size_t len;
    uint64_t lru;
} sc_slot;

typedef struct {
    pthread_mutex_t lk;
    pthread_cond_t cv;
    atomic_int refs;
    uint64_t gen, tick;
    sc_slot *s;        /* cfg.scache_slots of them (SCACHE_SLOTS by default) */
    int n;
    char *path;
    attr_t a;          /* layout snapshot for readahead */
} scache_t;

typedef struct {
    uint64_t off;
    size_t len;
    uint8_t *data;   /* inside base: the kernel's WRITE request buffer, kept as is */
    uint8_t *base;   /* rp_bulk_free */
} dirty_t;

/* an open file or directory */
typedef struct {
    int is_dir;
    char *path;
    pthread_mutex_t lk;
    dirty_t *dirty;
    size_t ndirty, capdirty, dirty_bytes;
    scache_t *sc;
    dirent_t *dents;     /* dir: listing snapshot taken at the first READDIR */
    size_t ndents;
    int listed;
    uint64_t next_off;   /* where a sequential reader's next read would start */
    /* a small inline file's whole content, fetched by the open itself
     * (fs_open_read): reads are served from it, no MDT round trip */
    uint8_t *pre;
    size_t pre_len;
    volatile int pre_ok;
} handle_t;

/* short names used throughout the client sources */
#define peer_t rp_peer_t
#define resp_t rp_resp_t
#define resp_free rp_resp_free
#define sha256_u64 rp_sha256_u64
uint32_t strhash(const char *s);

/* ---- lifecycle ---- */
/* reads the ROCEPOOL_* environment, connects the peers named in cfg.mdt /
 * cfg.ost, starts cfg.io_threads pool workers. Returns 0 or -errno. */
int rpfs_init(void);
void die(const char *fmt, ...);
void *xmalloc(size_t n);
void *xrealloc(void *old, size_t n);
char *xstrdup(const char *s);
double mono_s(void);
double wall_s(void);
extern rp_peer_t g_mdts[MAX_PEERS], g_osts[MAX_PEERS];

/* ---- namespace (paths are normalised: no leading slash; "" = root) ---- */
int fs_getattr(const char *path, attr_t *a);
/* open for reading: the attributes and, for a small inline file, its whole
 * content in one MDT request (*data malloc'd, NULL if not fetched).
 * -EISDIR for a directory. */
int fs_open_read(const char *path, attr_t *a, uint8_t **data, size_t *dlen);
int fs_mkdir(const char *path, attr_t *a);
int fs_create_file(const char *path, attr_t *a);
int fs_create_file_excl(const char *path, int exclusive, attr_t *a);  /* -EEXIST when exclusive and present */
int fs_rmdir(const char *path);
int fs_remove_file(const char *path);
int fs_truncate(const char *path, uint64_t size);
int fs_spill(const char *path);
int mdt_readdir(const char *dir, dirent_t **out, size_t *count);
/* positional listing: the MDTs' listings concatenated (rocepool_fs.c) */
int mdt_dir_nmdts(const char *dir);
int mdt_readdir_at(const char *dir, int k, uint64_t at, uint32_t budget,
                   dirent_t **out, size_t *count, uint64_t *total, int *more);
int mdt_readdir_first(const char *dir, uint32_t budget, dirent_t **ents, size_t *n,
                      uint64_t *total, int *more);
int mdt_set_size(const char *path, uint64_t size, int exact, attr_t *a);
int mdt_inline_read(const char *path, int freeze, uint64_t off, uint32_t len, attr_t *a, rp_resp_t *r,
                    const uint8_t **data, size_t *dlen);

/* ---- attribute cache ---- */
int ac_get(const char *path, attr_t *a, int *negative);
void ac_put(const char *path, const attr_t *a);   /* a == NULL: negative entry */
void ac_del(const char *path);
void ac_update_size(const char *path, uint64_t size, int exact);

/* ---- data ---- */
int fs_write_range(const char *path, uint64_t off, const uint8_t *data, size_t len);
int fs_read_direct(const char *path, const attr_t *a, uint64_t off, size_t len, uint8_t *out);
int ost_for_stripe(const attr_t *a, uint64_t idx);
void stripe_key(char *out, size_t n, const char *path, uint64_t idx);
scache_t *sc_new(const char *path, const attr_t *a);
void sc_unref(scache_t *c);
void sc_clear(scache_t *c);
int sc_read(scache_t *c, const attr_t *a, uint64_t off, size_t len, uint8_t *out);

/* ---- device (GPU) memory: GPUDirect-Storage-style I/O ----
 * rp_devops_t (rocepool_devmem.h): the runtime's copies, used only where a
 * transfer cannot go NIC <-> device directly. */
/* a device buffer: dev is the device pointer; mem its NIC registration
 * (roce_mem_register_device), or NULL when the NIC refused it: then every
 * transfer bounces through host memory */
typedef struct { void *dev; size_t len; roce_mem_t *mem; const rp_devops_t *ops; } rp_devbuf_t;
/* file range <-> b at boff; bytes moved (short at EOF for reads) or -errno */
ssize_t fs_read_dev(const char *path, uint64_t off, size_t len, const rp_devbuf_t *b, size_t boff);
ssize_t fs_write_dev(const char *path, uint64_t off, size_t len, const rp_devbuf_t *b, size_t boff);
/* bytes moved NIC <-> device directly, and through host memory */
void fs_dev_stats(uint64_t *direct, uint64_t *bounced);

/* ---- handles ---- */
handle_t *handle_new(const char *path, int is_dir);
void handle_free(handle_t *h);
int handle_flush(handle_t *h);          /* caller holds h->lk */
void open_register(handle_t *h);
void invalidate_open_caches(const char *path);
/* buffer a write: data stays in `base` (arena memory the handle now owns)
 * when base != NULL and the write is large, else it is copied. Caller
 * holds h->lk. Returns 0, or -errno from an early flush. */
int handle_write(handle_t *h, uint64_t off, const uint8_t *data, size_t len, uint8_t *base);
/* a read through the handle: flushes its own buffered writes first,
 * handles inline files, direct and cached stripe reads. Caller holds h->lk.
 * Returns bytes read (>= 0, short at EOF) or -errno. */
ssize_t handle_read(handle_t *h, uint64_t off, uint8_t *out, size_t len);

/* ---- the task pool ---- */
void *pool_worker(void *arg);
void pool_submit(void (*fn)(void *), void *arg);

#endif
