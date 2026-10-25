/* rocepool_servers.c -- native C OST and MDT servers.
 *
 * Why: in the Python servers every request crossed into the interpreter
 * (ctypes callback -> GIL -> Frame.decode -> PoolNode/MDTServer -> encode),
 * so one server process could never use more than one core, and a 1 MiB PUT
 * was copied 4-5 times on its way into the store. Measured ceiling on the
 * stress rig: ~3,000-3,600 MDT requests/s per process at a full core
 * (tools/stress_concurrency.py). Here the request is decoded straight out of
 * the connection's receive buffer and answered without Python at all; the
 * transport's one-thread-per-connection model now actually runs in parallel.
 *
 * Wire compatibility, byte for byte:
 *   OST = a single-node roce/pool_node.py PoolNode: PUT/GET/STAT/DELETE/
 *         LIST_PREFIX with PoolNode's sub-encodings (PUT value = u8 marker +
 *         u64 BE expected_version + data; PUT_OK = u64 BE version + u64 BE
 *         size; STAT_OK/LIST_PREFIX_OK = the same JSON PoolNode emits;
 *         ERROR = "CODE\0message"), so roce/remote_engine.py talks to it
 *         unchanged.
 *   MDT = fs/mdt_wire.py's binary layout, identical semantics to
 *         fs/mdt_node.py MDTServer.apply (the Python server stays as the
 *         reference; tests/test_native_servers.py runs one suite against both).
 */
#define _GNU_SOURCE
#include "rocepool_servers.h"
#include "ost_tier.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* frame codec (roce/protocol.py)                                      */
/* ------------------------------------------------------------------ */
enum {
    OP_PUT = 1, OP_PUT_OK = 2, OP_GET = 3, OP_GET_OK = 4, OP_DELETE = 5, OP_DELETE_OK = 6,
    OP_STAT = 7, OP_STAT_OK = 8, OP_LIST_PREFIX = 9, OP_LIST_PREFIX_OK = 10,
    /* "thick KV" ops (roce/protocol.py documents the encodings) */
    OP_WRITE_AT = 11, OP_WRITE_AT_OK = 12, OP_GET_V = 13, OP_GET_V_OK = 14,
    OP_READ_AT = 15, OP_READ_AT_OK = 16, OP_TRUNCATE = 17, OP_TRUNCATE_OK = 18,
    OP_BATCH = 40, OP_BATCH_OK = 41, OP_VTOKEN = 42, OP_VTOKEN_OK = 43,
    OP_LEASE = 44, OP_LEASE_OK = 45,
    OP_STATS = 50, OP_STATS_OK = 51,
    OP_MDT = 30, OP_MDT_OK = 31, OP_ERROR = 255,
};
#define HDR 24

typedef struct {
    uint8_t op;
    uint64_t req_id;
    const char *key;
    uint32_t key_len;
    const uint8_t *val;
    uint32_t val_len;
} frame_t;

static uint32_t rd_u32le(const uint8_t *p) { return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24; }
static uint64_t rd_u64le(const uint8_t *p) { return (uint64_t)rd_u32le(p) | (uint64_t)rd_u32le(p + 4) << 32; }
static uint64_t rd_u64be(const uint8_t *p) { uint64_t v = 0; for (int i = 0; i < 8; i++) v = v << 8 | p[i]; return v; }
static void wr_u32le(uint8_t *p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (8 * i)); }
static void wr_u64le(uint8_t *p, uint64_t v) { for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (8 * i)); }
static void wr_u64be(uint8_t *p, uint64_t v) { for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (56 - 8 * i)); }

static int frame_decode(const uint8_t *buf, size_t len, frame_t *f) {
    if (len < HDR || memcmp(buf, "RCP1", 4) != 0) return -1;
    f->op = buf[4];
    f->req_id = rd_u64le(buf + 8);
    f->key_len = rd_u32le(buf + 16);
    f->val_len = rd_u32le(buf + 20);
    if ((size_t)HDR + f->key_len + f->val_len != len) return -1;
    f->key = (const char *)buf + HDR;
    f->val = buf + HDR + f->key_len;
    return 0;
}

/* Allocates a whole response frame with room for `val_len` value bytes and
 * returns a pointer to where the value goes; out and out_len get the frame. */
static uint8_t *frame_alloc(uint8_t op, const frame_t *req, size_t val_len, uint8_t **out, size_t *out_len) {
    size_t total = HDR + req->key_len + val_len;
    /* a large reply (a GET's stripe) is built in a registered buffer the
     * transport RDMA WRITEs to the client as is -- no copy on the way out */
    uint8_t *b = roce_reply_alloc(total);
    if (!b) {
        /* every request of this server then fails ("could not handle the
         * request"): say why, once a minute, with the process's mapping
         * count (glibc's malloc fails at vm.max_map_count with RAM free) */
        static _Atomic long last;
        long now = (long)time(NULL), was = atomic_load(&last);
        if (now - was >= 60 && atomic_compare_exchange_strong(&last, &was, now)) {
            int e = errno;
            long maps = 0, max = -1;
            FILE *fm = fopen("/proc/self/maps", "r");
            if (fm) { int c; while ((c = fgetc(fm)) != EOF) if (c == '\n') maps++; fclose(fm); }
            FILE *fx = fopen("/proc/sys/vm/max_map_count", "r");
            if (fx) { if (fscanf(fx, "%ld", &max) != 1) max = -1; fclose(fx); }
            fprintf(stderr, "rocepool server: cannot allocate a %zu-byte reply (%s): %ld memory mappings, "
                    "vm.max_map_count %ld\n", total, strerror(e), maps, max);
        }
        return NULL;
    }
    memcpy(b, "RCP1", 4);
    b[4] = op; b[5] = b[6] = b[7] = 0;
    wr_u64le(b + 8, req->req_id);
    wr_u32le(b + 16, req->key_len);
    wr_u32le(b + 20, (uint32_t)val_len);
    memcpy(b + HDR, req->key, req->key_len);
    *out = b;
    *out_len = total;
    return b + HDR + req->key_len;
}

static uint8_t *frame_error(const frame_t *req, const char *code, const char *msg, size_t *out_len) {
    size_t cl = strlen(code), ml = strlen(msg);
    uint8_t *out;
    uint8_t *v = frame_alloc(OP_ERROR, req, cl + 1 + ml, &out, out_len);
    if (!v) return NULL;
    memcpy(v, code, cl);
    v[cl] = 0;
    memcpy(v + cl + 1, msg, ml);
    return out;
}

static uint8_t *frame_simple(uint8_t op, const frame_t *req, const void *val, size_t val_len, size_t *out_len) {
    uint8_t *out;
    uint8_t *v = frame_alloc(op, req, val_len, &out, out_len);
    if (!v) return NULL;
    if (val_len) memcpy(v, val, val_len);
    return out;
}

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static uint64_t mono_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t fnv1a(const char *s, size_t n) {
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; i++) { h ^= (uint8_t)s[i]; h *= 1099511628211ULL; }
    return h;
}

/* growable byte buffer for JSON/readdir bodies */
typedef struct { uint8_t *p; size_t len, cap; } buf_t;
static int buf_reserve(buf_t *b, size_t more) {
    if (b->len + more <= b->cap) return 0;
    size_t cap = b->cap ? b->cap : 256;
    while (cap < b->len + more) cap *= 2;
    uint8_t *p = realloc(b->p, cap);
    if (!p) return -1;
    b->p = p; b->cap = cap;
    return 0;
}
static int buf_put(buf_t *b, const void *d, size_t n) {
    if (buf_reserve(b, n)) return -1;
    memcpy(b->p + b->len, d, n);
    b->len += n;
    return 0;
}
static int buf_printf(buf_t *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static int buf_printf(buf_t *b, const char *fmt, ...) {
    /* 256 bytes truncated the MDT's stats line once its counters grew
     * (after ~16 M creates): STATS then failed ("could not handle") */
    char tmp[2048];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= sizeof(tmp)) return -1;
    return buf_put(b, tmp, (size_t)n);
}
/* JSON string with the escapes Python's json.loads requires */
static int buf_json_str(buf_t *b, const char *s, size_t n) {
    if (buf_put(b, "\"", 1)) return -1;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '"' || c == '\\') { char e[2] = {'\\', (char)c}; if (buf_put(b, e, 2)) return -1; }
        else if (c < 0x20) { if (buf_printf(b, "\\u%04x", c)) return -1; }
        else if (buf_put(b, &s[i], 1)) return -1;
    }
    return buf_put(b, "\"", 1);
}

/* ------------------------------------------------------------------ */
/* OST: striped-lock hash map, semantics of storage_engine/memory_engine */
/* ------------------------------------------------------------------ */
#define OST_STRIPES 64
/* Delete watermarks (roce/protocol.py VTOKEN): a key's versions are never
 * reused across delete/recreate -- a new object starts above its bucket's
 * watermark, and an absent key's version token is that watermark. Bucket b
 * is only touched under stripe lock b % OST_STRIPES (same hash). */
#define OST_WM_BUCKETS 4096

/* ---- registered object memory, for one-sided reads ----
 * An object's RAM copy lives in a slab of a chunk registered with the NIC
 * (and told to the tier: SPDK registers it for its DMA), laid out as
 *   [obj_hdr: seq, keyhash, size][data ...][tail: seq]
 * A client that holds a LEASE (addr, rkey, seq, keyhash, size) RDMA READs
 * header, data range and tail in one in-order chain and trusts the bytes
 * only if header.seq == tail == lease.seq (even) and keyhash matches:
 * writers bump seq to odd before touching the object and to a fresh even
 * number after (a seqlock the NIC reads), freeing leaves seq odd and the
 * memory registered, so a stale lease reads an invalid header, never
 * someone else's data. */
#define OBJ_HDR 32
typedef struct { atomic_ullong seq; uint64_t keyhash; uint64_t size; uint64_t pad; } obj_hdr_t;
#define OBJ_TAIL 8
#define ARENA_CHUNK (64u << 20)
#define ARENA_CLASSES 7
/* class 0 is one page with its header and tail (4096 bytes of data needed
 * two pages: 3901-byte mdtest-hard files took 8 KiB each) */
static const size_t ARENA_DATA[ARENA_CLASSES] = {4096 - OBJ_HDR - OBJ_TAIL, 16u << 10, 64u << 10, 256u << 10, 1u << 20,
                                                 4u << 20, 16u << 20};

typedef struct arena_chunk {
    struct arena_chunk *next;
    uint8_t *base;
    size_t len, used;
    roce_mem_t *mem;
    uint32_t rkey;
    int rkey_ok;
} arena_chunk_t;

typedef struct {
    pthread_mutex_t lock;
    arena_chunk_t *chunks;
    uint8_t **free[ARENA_CLASSES];
    size_t nfree[ARENA_CLASSES], cfree[ARENA_CLASSES];
    atomic_ullong bytes, seq_gen;
    void (*notify)(void *base, size_t len, void *user);
    void *notify_user;
} obj_arena_t;

static size_t arena_slab_bytes(uint32_t cls) {  /* header + data + tail, page multiple */
    return (OBJ_HDR + ARENA_DATA[cls] + OBJ_TAIL + 4095) & ~(size_t)4095;
}
static uint32_t arena_class(size_t size) {
    for (uint32_t c = 0; c < ARENA_CLASSES; c++) if (size <= ARENA_DATA[c]) return c;
    return UINT32_MAX;
}
static inline obj_hdr_t *obj_hdr(const uint8_t *data) { return (obj_hdr_t *)(data - OBJ_HDR); }
static inline uint64_t *obj_tail(uint8_t *data, size_t size) { return (uint64_t *)(data + size); }

/* the memory a RAM copy of capacity cap really holds (its whole slab):
 * what the RAM cache's budget counts */
static size_t cap_mem(size_t cap) { uint32_t c = arena_class(cap); return c == UINT32_MAX ? cap : arena_slab_bytes(c); }

static void arena_init(obj_arena_t *a) { memset(a, 0, sizeof(*a)); pthread_mutex_init(&a->lock, NULL); }

/* caller holds a->lock */
static int arena_grow(obj_arena_t *a) {
    arena_chunk_t *c = calloc(1, sizeof(*c));
    if (!c) return -ENOMEM;
    c->len = ARENA_CHUNK;
    c->base = mmap(NULL, c->len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (c->base == MAP_FAILED) { free(c); return -ENOMEM; }
    c->mem = roce_mem_register(c->base, c->len);
    if (!c->mem) { munmap(c->base, c->len); free(c); return -ENOMEM; }
    if (a->notify) a->notify(c->base, c->len, a->notify_user);
    c->next = a->chunks;
    a->chunks = c;
    atomic_fetch_add(&a->bytes, c->len);
    return 0;
}

/* data pointer (OBJ_HDR into the slab) for `size` bytes, header seq odd
 * (not yet published), or NULL */
static uint8_t *arena_alloc(obj_arena_t *a, size_t size, uint64_t keyhash, size_t *cap) {
    uint32_t cls = arena_class(size);
    if (cls == UINT32_MAX) return NULL;
    size_t slab = arena_slab_bytes(cls);
    uint8_t *p = NULL;
    pthread_mutex_lock(&a->lock);
    if (!a->nfree[cls]) {
        /* none free in this class: carve a free slab of a larger class
         * (IOR's evicted 1 MiB copies) into this class's slabs before growing
         * the arena -- free slabs never moved between classes, and an OST
         * whose cache turned from large objects to small ones grew past
         * its budget until the kernel killed it (EFA run, 2026-10-24).
         * Every piece's header is marked free (odd seq, no keyhash): a stale
         * lease on the old object reads its old header, now a free piece's */
        for (uint32_t big = cls + 1; big < ARENA_CLASSES && !a->nfree[cls]; big++) {
            if (!a->nfree[big]) continue;
            uint8_t *b = a->free[big][--a->nfree[big]];
            size_t n = arena_slab_bytes(big) / slab;
            if (a->cfree[cls] < a->nfree[cls] + n) {
                size_t nc = a->cfree[cls] ? a->cfree[cls] : 256;
                while (nc < a->nfree[cls] + n) nc *= 2;
                uint8_t **nf = realloc(a->free[cls], nc * sizeof(*nf));
                if (!nf) { a->free[big][a->nfree[big]++] = b; break; }
                a->free[cls] = nf;
                a->cfree[cls] = nc;
            }
            for (size_t i = n; i-- > 0;) {
                obj_hdr_t *ph = (obj_hdr_t *)(b + i * slab);
                atomic_store(&ph->seq, 1);
                ph->keyhash = 0;
                a->free[cls][a->nfree[cls]++] = (uint8_t *)ph;
            }
        }
    }
    if (a->nfree[cls]) p = a->free[cls][--a->nfree[cls]];
    else {
        if (!a->chunks || a->chunks->used + slab > a->chunks->len) {
            if (arena_grow(a)) { pthread_mutex_unlock(&a->lock); return NULL; }
        }
        p = a->chunks->base + a->chunks->used;
        a->chunks->used += slab;
    }
    pthread_mutex_unlock(&a->lock);
    obj_hdr_t *h = (obj_hdr_t *)p;
    atomic_store(&h->seq, 1);  /* odd: being written */
    h->keyhash = keyhash;
    h->size = size;
    *cap = ARENA_DATA[cls];
    return p + OBJ_HDR;
}

static void arena_free(obj_arena_t *a, uint8_t *data, size_t cap) {
    if (!data) return;
    obj_hdr_t *h = obj_hdr(data);
    atomic_store(&h->seq, 1);  /* a stale lease now reads an odd seq */
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    h->keyhash = 0;
    uint32_t cls = arena_class(cap);
    pthread_mutex_lock(&a->lock);
    if (a->nfree[cls] == a->cfree[cls]) {
        size_t nc = a->cfree[cls] ? a->cfree[cls] * 2 : 256;
        uint8_t **nf = realloc(a->free[cls], nc * sizeof(*nf));
        if (nf) { a->free[cls] = nf; a->cfree[cls] = nc; }
    }
    if (a->nfree[cls] < a->cfree[cls]) a->free[cls][a->nfree[cls]++] = (uint8_t *)h;  /* else the slab leaks */
    pthread_mutex_unlock(&a->lock);
}

/* seqlock: around every in-place change of a published object */
static void obj_begin(uint8_t *data) {
    obj_hdr_t *h = obj_hdr(data);
    atomic_store(&h->seq, atomic_load(&h->seq) | 1);
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
}
static uint64_t obj_publish(obj_arena_t *a, uint8_t *data, size_t size) {
    obj_hdr_t *h = obj_hdr(data);
    uint64_t seq = (atomic_fetch_add(&a->seq_gen, 1) + 1) << 1;  /* fresh, even, never 0 */
    h->size = size;
    *obj_tail(data, size) = seq;
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    atomic_store(&h->seq, seq);
    return seq;
}

/* the chunk holding data, for its rkey */
static arena_chunk_t *arena_chunk_of(obj_arena_t *a, const uint8_t *data) {
    for (arena_chunk_t *c = a->chunks; c; c = c->next)
        if (data >= c->base && data < c->base + c->len) return c;
    return NULL;
}

typedef struct kv_entry {
    struct kv_entry *next;
    uint64_t hash;
    char *key;
    size_t key_len;
    uint8_t *data;        /* RAM copy of the object, NULL when it only lives on the tier */
    size_t size;          /* logical size */
    size_t data_cap;      /* bytes allocated behind data */
    uint64_t version;
    double created_at, updated_at;
    /* capacity tier (ost_tier.h): the object's slab, and its place in the
     * RAM cache's LRU list (both only with a tier) */
    tier_ext_t ext;
    uint8_t on_tier;
    /* the RAM copy has bytes the tier copy lacks: written back once, when
     * evicted (WRITE_AT pieces of a stripe filled out of order) */
    uint8_t tier_dirty;
    struct kv_entry *lru_prev, *lru_next;
} kv_entry;

typedef struct {
    pthread_mutex_t lock;
    kv_entry **slots;
    size_t nslots, count;
} kv_stripe;

typedef struct {
    kv_stripe stripes[OST_STRIPES];
    uint64_t wm[OST_WM_BUCKETS];
    atomic_ullong bytes, objects, ops;
    /* capacity tier: every object is written through to it; RAM keeps at
     * most ram_budget bytes of copies (LRU), reads of evicted objects go
     * from the device straight into the reply buffer */
    ost_tier_t *tier;
    uint64_t ram_budget;
    atomic_ullong ram_bytes, direct_writes, tier_reads, evictions;
    atomic_ullong tier_bytes_written;  /* all writes to the tier: over bytes stored = write amplification */
    atomic_ullong writebacks;          /* dirty RAM copies written to the tier on eviction */
    /* background eviction: requests only wake it while the cache is
     * between its budget and the hard limit above it (budget + 1/8) */
    pthread_mutex_t ev_lk;
    pthread_cond_t ev_cv;
    int ev_started;
    atomic_ullong inline_evictions;    /* requests that had to evict themselves (past the hard limit) */
    atomic_int writeback_failed;       /* a write-back failed (tier full?): WRITE_AT writes through again */
    obj_arena_t arena;              /* every RAM copy: registered, one-sided readable */
    roce_server_t *server;          /* for the arena chunks' rkeys (set once the server runs) */
    atomic_ullong leases, lease_misses;
    pthread_mutex_t lru_lock;
    kv_entry *lru_head, *lru_tail;   /* head = most recently used */
} ost_store;

static void arena_tier_notify(void *base, size_t len, void *user) { ost_tier_notify_chunk(user, base, len); }

static void *ost_evictor(void *arg);
static ost_store *ost_new(void) {
    ost_store *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    for (int i = 0; i < OST_STRIPES; i++) {
        pthread_mutex_init(&s->stripes[i].lock, NULL);
        s->stripes[i].nslots = 64;
        s->stripes[i].slots = calloc(64, sizeof(kv_entry *));
        if (!s->stripes[i].slots) return NULL;
    }
    pthread_mutex_init(&s->lru_lock, NULL);
    arena_init(&s->arena);
    const char *spec = getenv("ROCEPOOL_OST_TIER");
    if (spec && *spec) {
        /* every OST needs a device or file of its own; "{n}" in the spec is
         * this process's OST number (several OSTs in one process: tests) */
        static atomic_int n_stores;
        char expanded[1024], err[512];
        const char *br = strstr(spec, "{n}");
        if (br) snprintf(expanded, sizeof(expanded), "%.*s%d%s", (int)(br - spec), spec, atomic_fetch_add(&n_stores, 1), br + 3);
        else snprintf(expanded, sizeof(expanded), "%s", spec);
        s->tier = ost_tier_open(expanded, err, sizeof(err));
        if (!s->tier) { fprintf(stderr, "rocepool OST: %s\n", err); free(s); return NULL; }
        const char *mib = getenv("ROCEPOOL_OST_RAM_CACHE_MIB");
        s->ram_budget = (mib && atoll(mib) >= 0 ? (uint64_t)atoll(mib) : 1024) << 20;
        s->arena.notify = arena_tier_notify;
        s->arena.notify_user = s->tier;
        pthread_mutex_init(&s->ev_lk, NULL);
        pthread_cond_init(&s->ev_cv, NULL);
        const char *bg = getenv("ROCEPOOL_OST_BG_EVICT");  /* "0": requests evict themselves, as before */
        pthread_t evt;
        if (s->ram_budget >= (16u << 20) && !(bg && !strcmp(bg, "0")) && !pthread_create(&evt, NULL, ost_evictor, s)) {
            pthread_detach(evt);
            s->ev_started = 1;
        }
        fprintf(stderr, "rocepool OST: tier %s, %llu GiB, direct I/O %s, RAM cache %llu MiB\n", ost_tier_kind(s->tier),
                (unsigned long long)(ost_tier_capacity(s->tier) >> 30), ost_tier_direct(s->tier) ? "on" : "off",
                (unsigned long long)(s->ram_budget >> 20));
    }
    return s;
}

static kv_stripe *ost_stripe(ost_store *s, uint64_t h) { return &s->stripes[h % OST_STRIPES]; }

/* ---- RAM copies and the tier (caller holds the entry's stripe lock) ---- */
static uint8_t *ent_alloc_h(ost_store *s, size_t size, uint64_t keyhash, size_t *cap) {
    return arena_alloc(&s->arena, size, keyhash, cap);
}
static void ent_free_data(ost_store *s, uint8_t *p, size_t cap) {
    arena_free(&s->arena, p, cap);
}

static void lru_unlink(ost_store *s, kv_entry *e) {  /* caller holds lru_lock */
    if (e->lru_prev) e->lru_prev->lru_next = e->lru_next; else if (s->lru_head == e) s->lru_head = e->lru_next;
    if (e->lru_next) e->lru_next->lru_prev = e->lru_prev; else if (s->lru_tail == e) s->lru_tail = e->lru_prev;
    e->lru_prev = e->lru_next = NULL;
}
static void lru_touch(ost_store *s, kv_entry *e) {  /* e is resident: to the head */
    pthread_mutex_lock(&s->lru_lock);
    lru_unlink(s, e);
    e->lru_next = s->lru_head;
    if (s->lru_head) s->lru_head->lru_prev = e;
    s->lru_head = e;
    if (!s->lru_tail) s->lru_tail = e;
    pthread_mutex_unlock(&s->lru_lock);
}
static void lru_drop(ost_store *s, kv_entry *e) {
    pthread_mutex_lock(&s->lru_lock);
    lru_unlink(s, e);
    pthread_mutex_unlock(&s->lru_lock);
}

/* Free RAM copies, least recently used first, until the cache is within
 * budget. An entry is only touched under its stripe lock, taken with
 * trylock (the caller may hold another stripe's lock): a busy one is
 * skipped this round. Every object is written through, so a copy is
 * always clean. */
static int ent_sync(ost_store *s, kv_entry *e, uint64_t off, size_t len, size_t reserve);
static void lru_touch(ost_store *s, kv_entry *e);
static void ost_evict_to(ost_store *s, uint64_t target);
static void *ost_evictor(void *arg) {
    ost_store *s = arg;
    pthread_setname_np(pthread_self(), "ost-evict");
    for (;;) {
        pthread_mutex_lock(&s->ev_lk);
        uint64_t low = s->ram_budget - s->ram_budget / 16;
        if (atomic_load(&s->ram_bytes) <= low) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_nsec += 20 * 1000000L;
            ts.tv_sec += ts.tv_nsec / 1000000000L;
            ts.tv_nsec %= 1000000000L;
            pthread_cond_timedwait(&s->ev_cv, &s->ev_lk, &ts);
        }
        pthread_mutex_unlock(&s->ev_lk);
        if (atomic_load(&s->ram_bytes) > low) ost_evict_to(s, low);
    }
    return NULL;
}

/* After a request that grew the cache. Eviction -- with write-backs of
 * dirty copies, a stripe's worth of NVMe writes each -- used to run here,
 * inside the request: in ior-hard-write on 2 x i3en every OST did 145 k of
 * them in its handlers. A background thread now keeps the cache a little
 * under budget; a request evicts itself only past the hard limit. */
static void ost_evict(ost_store *s) {
    if (!s->tier) return;
    uint64_t ram = atomic_load(&s->ram_bytes), hard = s->ram_budget + s->ram_budget / 8;
    if (ram <= s->ram_budget - s->ram_budget / 16) return;
    if (s->ev_started) pthread_cond_signal(&s->ev_cv);
    if (ram > hard || !s->ev_started) {
        if (s->ev_started) atomic_fetch_add(&s->inline_evictions, 1);
        ost_evict_to(s, s->ram_budget);
    }
}

static void ost_evict_to(ost_store *s, uint64_t target) {
    if (!s->tier) return;
    /* until within budget: only busy entries count against the attempts --
     * a fixed 64 evictions per call fell behind when 4 KiB copies (mdtest-
     * hard's files) made way for 1 MiB ones (ior-easy-read): ~250 to free
     * per read, the cache grew to 28 of 19 GiB and the host ran out
     * (EFA run, 2026-10-24) */
    int attempts = 256;
    while (atomic_load(&s->ram_bytes) > target && attempts > 0) {
        pthread_mutex_lock(&s->lru_lock);
        kv_entry *e = s->lru_tail;
        if (!e) { pthread_mutex_unlock(&s->lru_lock); return; }
        kv_stripe *st = ost_stripe(s, e->hash);
        if (pthread_mutex_trylock(&st->lock)) {
            attempts--;
            /* busy: rotate it to the head so the next candidate differs */
            lru_unlink(s, e);
            e->lru_next = s->lru_head;
            if (s->lru_head) s->lru_head->lru_prev = e;
            s->lru_head = e;
            if (!s->lru_tail) s->lru_tail = e;
            pthread_mutex_unlock(&s->lru_lock);
            continue;
        }
        lru_unlink(s, e);
        pthread_mutex_unlock(&s->lru_lock);
        if (e->data && e->tier_dirty) {  /* write it back first, whole, once */
            if (ent_sync(s, e, 0, e->size, 0)) {
                if (!atomic_exchange(&s->writeback_failed, 1))
                    fprintf(stderr, "rocepool OST: writing an object back to the tier failed; WRITE_AT writes through from now on\n");
                lru_touch(s, e);  /* stays cached: its bytes exist nowhere else */
                pthread_mutex_unlock(&st->lock);
                attempts--;
                continue;
            }
            e->tier_dirty = 0;
            atomic_fetch_add(&s->writebacks, 1);
        }
        if (e->data && e->on_tier) {
            atomic_fetch_sub(&s->ram_bytes, cap_mem(e->data_cap));
            ent_free_data(s, e->data, e->data_cap);
            e->data = NULL;
            e->data_cap = 0;
            atomic_fetch_add(&s->evictions, 1);
        }
        pthread_mutex_unlock(&st->lock);
    }
}

/* the object's bytes in RAM (read back from the tier when evicted) */
static int ent_resident(ost_store *s, kv_entry *e) {
    if (e->data) return 0;
    size_t cap;
    uint8_t *d = ent_alloc_h(s, e->size, e->hash, &cap);
    if (!d) return -ENOMEM;
    if (e->on_tier && e->size) {
        int rc = ost_tier_read(s->tier, &e->ext, 0, d, e->size);
        if (rc) { ent_free_data(s, d, cap); return rc; }
        atomic_fetch_add(&s->tier_reads, 1);
    }
    obj_publish(&s->arena, d, e->size);
    e->data = d;
    e->data_cap = cap;
    atomic_fetch_add(&s->ram_bytes, cap_mem(cap));
    lru_touch(s, e);
    return 0;
}

/* a slab that holds `size` bytes: keeps the current one when it fits, else
 * a new one (the caller copies the content it needs to keep) */
static int ent_slab(ost_store *s, kv_entry *e, size_t size, tier_ext_t *old, int *moved, size_t reserve) {
    *moved = 0;
    if (e->on_tier && size <= ost_tier_class_bytes(e->ext.cls)) return 0;
    tier_ext_t ext;
    /* reserve: room to take for an object expected to grow (a new slab
     * copies the whole object) */
    int rc = ost_tier_alloc(s->tier, size > reserve ? size : reserve, &ext);
    if (rc) return rc;
    if (e->on_tier) { *old = e->ext; *moved = 1; }
    e->ext = ext;
    e->on_tier = 1;
    return 0;
}

/* write the RAM copy's bytes [off, off+len) through to the tier */
static int ent_sync(ost_store *s, kv_entry *e, uint64_t off, size_t len, size_t reserve) {
    tier_ext_t old;
    int moved;
    int rc = ent_slab(s, e, e->size, &old, &moved, reserve);
    if (rc) return rc;
    if (moved) { off = 0; len = e->size; }  /* a new slab: the whole object */
    if (len) rc = ost_tier_write(s->tier, &e->ext, off, e->data + off, len);
    if (len && !rc) atomic_fetch_add(&s->tier_bytes_written, len);
    if (moved) ost_tier_free(s->tier, &old);
    return rc;
}

/* A WRITE_AT past an object's start is a piece of a stripe being filled
 * out of order (shared-file writes): take a slab for a whole default
 * stripe at once. Growing 64 K -> 256 K -> 1 M rewrote the object on every
 * step (each new slab gets the whole object). */
#define OST_GROW_RESERVE (1u << 20)
static size_t grow_reserve(uint64_t off, size_t new_size) {
    return off > 0 && new_size < OST_GROW_RESERVE ? OST_GROW_RESERVE : 0;
}

/* page-aligned source, offset and length: the NIC's buffer (host memory or
 * a P2P CMB) goes to the device without a RAM copy */
static int direct_ok(ost_store *s, const uint8_t *src, uint64_t off, size_t len) {
    return s->tier && ost_tier_direct(s->tier) && len && ((uintptr_t)src % TIER_PAGE) == 0 &&
           off % TIER_PAGE == 0 && len % TIER_PAGE == 0;
}

static void ent_destroy(ost_store *s, kv_entry *e) {  /* unlinked from its stripe already */
    if (s->tier) {
        lru_drop(s, e);
        if (e->on_tier) ost_tier_free(s->tier, &e->ext);
        if (e->data) atomic_fetch_sub(&s->ram_bytes, cap_mem(e->data_cap));
    }
    ent_free_data(s, e->data, e->data_cap);
    free(e->key);
    free(e);
}


/* caller holds the stripe lock */
static kv_entry **kv_find(kv_stripe *st, uint64_t h, const char *key, size_t kl) {
    kv_entry **pp = &st->slots[(h / OST_STRIPES) % st->nslots];
    for (; *pp; pp = &(*pp)->next)
        if ((*pp)->hash == h && (*pp)->key_len == kl && memcmp((*pp)->key, key, kl) == 0) return pp;
    return pp;
}

static void kv_grow(kv_stripe *st) {
    size_t n = st->nslots * 2;
    kv_entry **slots = calloc(n, sizeof(kv_entry *));
    if (!slots) return;  /* keep working at the old size */
    for (size_t i = 0; i < st->nslots; i++) {
        kv_entry *e = st->slots[i];
        while (e) {
            kv_entry *nx = e->next;
            size_t j = (e->hash / OST_STRIPES) % n;
            e->next = slots[j];
            slots[j] = e;
            e = nx;
        }
    }
    free(st->slots);
    st->slots = slots;
    st->nslots = n;
}

static int stat_json(buf_t *b, const kv_entry *e, int with_key) {
    if (buf_put(b, "{", 1)) return -1;
    if (with_key) {
        if (buf_put(b, "\"key\": ", 7) || buf_json_str(b, e->key, e->key_len) || buf_put(b, ", ", 2)) return -1;
    }
    return buf_printf(b, "\"size\": %zu, \"created_at\": %.17g, \"updated_at\": %.17g, \"version\": %llu, \"is_dir\": false}",
                      e->size, e->created_at, e->updated_at, (unsigned long long)e->version);
}

static int cmp_entry_key(const void *a, const void *b) {
    const kv_entry *x = *(kv_entry *const *)a, *y = *(kv_entry *const *)b;
    size_t n = x->key_len < y->key_len ? x->key_len : y->key_len;
    int c = memcmp(x->key, y->key, n);  /* byte order == Python str order for UTF-8 */
    return c ? c : (x->key_len > y->key_len) - (x->key_len < y->key_len);
}

static uint8_t *ost_handle(ost_store *s, const frame_t *f, size_t *out_len) {
    atomic_fetch_add(&s->ops, 1);
    uint64_t h = fnv1a(f->key, f->key_len);
    kv_stripe *st = ost_stripe(s, h);
    uint64_t *wm = &s->wm[h % OST_WM_BUCKETS];
    char msg[160];
    snprintf(msg, sizeof(msg), "no such object: %.*s", (int)(f->key_len > 120 ? 120 : f->key_len), f->key);

    switch (f->op) {
    case OP_PUT: {
        if (f->val_len < 9) return frame_error(f, "STORAGE_ERROR", "short PUT value", out_len);
        /* marker 1: CAS on the version (0 = absent); 2: CAS on a VTOKEN */
        int has_expected = f->val[0] == 1 || f->val[0] == 2, by_token = f->val[0] == 2;
        uint64_t expected = rd_u64be(f->val + 1);
        size_t dlen = f->val_len - 9;
        const uint8_t *src = f->val + 9;
        int direct = direct_ok(s, src, 0, dlen);
        uint8_t *data = NULL;
        size_t cap = 0;
        if (!direct) {
            data = ent_alloc_h(s, dlen, h, &cap);
            if (!data) return frame_error(f, "STORAGE_ERROR", "out of memory", out_len);
            memcpy(data, src, dlen);  /* the one copy into the store, done outside the lock */
        }
        double t = now_s();
        pthread_mutex_lock(&st->lock);
        kv_entry **pp = kv_find(st, h, f->key, f->key_len);
        kv_entry *e = *pp;
        uint64_t cur = e ? e->version : by_token ? *wm : 0;
        if (has_expected && expected != cur) {
            pthread_mutex_unlock(&st->lock);
            ent_free_data(s, data, cap);
            char vc[64];
            snprintf(vc, sizeof(vc), "%llu:%llu", (unsigned long long)expected, (unsigned long long)cur);
            return frame_error(f, "VERSION_CONFLICT", vc, out_len);
        }
        uint8_t *old = NULL;
        size_t old_size = 0, old_cap = 0;
        if (!e) {
            e = calloc(1, sizeof(*e));
            char *k = malloc(f->key_len ? f->key_len : 1);
            if (!e || !k) { pthread_mutex_unlock(&st->lock); free(e); free(k); ent_free_data(s, data, cap);
                            return frame_error(f, "STORAGE_ERROR", "out of memory", out_len); }
            memcpy(k, f->key, f->key_len);
            e->hash = h; e->key = k; e->key_len = f->key_len;
            e->version = *wm; e->created_at = t;
            e->next = *pp;
            *pp = e;
            st->count++;
            atomic_fetch_add(&s->objects, 1);
            if (st->count > st->nslots * 2) kv_grow(st);
        } else {
            old = e->data; old_size = e->size; old_cap = e->data_cap;
        }
        e->data = data; e->data_cap = cap; e->size = dlen; e->version++; e->updated_at = t;
        if (data) obj_publish(&s->arena, data, dlen);
        int rc = 0;
        if (s->tier) {
            tier_ext_t oldext;
            int moved;
            rc = ent_slab(s, e, dlen, &oldext, &moved, 0);
            if (!rc) rc = direct ? ost_tier_write_direct(s->tier, &e->ext, 0, src, dlen)
                                 : ost_tier_write(s->tier, &e->ext, 0, data, dlen);
            if (!rc) atomic_fetch_add(&s->tier_bytes_written, dlen);
            if (!rc && moved) ost_tier_free(s->tier, &oldext);
            if (!rc) e->tier_dirty = 0;
            if (!rc && direct) atomic_fetch_add(&s->direct_writes, 1);
            if (old) atomic_fetch_sub(&s->ram_bytes, cap_mem(old_cap));
            if (data) { atomic_fetch_add(&s->ram_bytes, cap_mem(cap)); lru_touch(s, e); } else lru_drop(s, e);
        }
        uint64_t version = e->version;
        pthread_mutex_unlock(&st->lock);
        ent_free_data(s, old, old_cap);
        if (rc) return frame_error(f, "STORAGE_ERROR", "tier write failed", out_len);
        atomic_fetch_add(&s->bytes, dlen);
        atomic_fetch_sub(&s->bytes, old_size);
        ost_evict(s);
        uint8_t ok[16];
        wr_u64be(ok, version);
        wr_u64be(ok + 8, dlen);
        return frame_simple(OP_PUT_OK, f, ok, 16, out_len);
    }
    case OP_GET: {
        pthread_mutex_lock(&st->lock);
        kv_entry *e = *kv_find(st, h, f->key, f->key_len);
        if (!e) { pthread_mutex_unlock(&st->lock); return frame_error(f, "NOT_FOUND", msg, out_len); }
        /* copy under the lock: a concurrent PUT may free e->data; an
         * evicted object is read from the tier straight into the reply */
        uint8_t *out = NULL;
        uint8_t *v = frame_alloc(OP_GET_OK, f, e->size, &out, out_len);
        int rc = 0;
        if (v && e->data) { memcpy(v, e->data, e->size); if (s->tier) lru_touch(s, e); }
        else if (v && e->size) { rc = ost_tier_read(s->tier, &e->ext, 0, v, e->size); atomic_fetch_add(&s->tier_reads, 1); }
        pthread_mutex_unlock(&st->lock);
        if (rc) { roce_reply_free(out); return frame_error(f, "STORAGE_ERROR", "tier read failed", out_len); }
        return out;
    }
    case OP_STAT: {
        buf_t b = {0};
        pthread_mutex_lock(&st->lock);
        kv_entry *e = *kv_find(st, h, f->key, f->key_len);
        int rc = e ? stat_json(&b, e, 0) : 1;
        pthread_mutex_unlock(&st->lock);
        if (rc == 1) return frame_error(f, "NOT_FOUND", msg, out_len);
        uint8_t *out = rc ? NULL : frame_simple(OP_STAT_OK, f, b.p, b.len, out_len);
        free(b.p);
        return out;
    }
    case OP_DELETE: {
        pthread_mutex_lock(&st->lock);
        kv_entry **pp = kv_find(st, h, f->key, f->key_len);
        kv_entry *e = *pp;
        if (!e) { pthread_mutex_unlock(&st->lock); return frame_error(f, "NOT_FOUND", msg, out_len); }
        *pp = e->next;
        st->count--;
        if (*wm < e->version + 1) *wm = e->version + 1;
        pthread_mutex_unlock(&st->lock);
        atomic_fetch_sub(&s->bytes, e->size);
        atomic_fetch_sub(&s->objects, 1);
        ent_destroy(s, e);
        return frame_simple(OP_DELETE_OK, f, NULL, 0, out_len);
    }
    case OP_LIST_PREFIX: {
        /* snapshot matching metadata stripe by stripe (like MemoryEngine,
         * consistent per key, not a global point-in-time view) */
        size_t n = 0, cap = 64;
        kv_entry **hits = malloc(cap * sizeof(*hits));
        if (!hits) return frame_error(f, "STORAGE_ERROR", "out of memory", out_len);
        for (int i = 0; i < OST_STRIPES; i++) {
            kv_stripe *sx = &s->stripes[i];
            pthread_mutex_lock(&sx->lock);
            for (size_t j = 0; j < sx->nslots; j++)
                for (kv_entry *e = sx->slots[j]; e; e = e->next) {
                    if (e->key_len < f->key_len || memcmp(e->key, f->key, f->key_len) != 0) continue;
                    if (n == cap) { cap *= 2; kv_entry **nh = realloc(hits, cap * sizeof(*hits)); if (!nh) continue; hits = nh; }
                    kv_entry *copy = malloc(sizeof(*copy));
                    if (!copy) continue;
                    *copy = *e;
                    copy->key = malloc(e->key_len ? e->key_len : 1);
                    if (!copy->key) { free(copy); continue; }
                    memcpy(copy->key, e->key, e->key_len);
                    copy->data = NULL;
                    hits[n++] = copy;
                }
            pthread_mutex_unlock(&sx->lock);
        }
        qsort(hits, n, sizeof(*hits), cmp_entry_key);
        buf_t b = {0};
        int rc = buf_put(&b, "[", 1);
        for (size_t i = 0; i < n && !rc; i++) {
            if (i) rc = buf_put(&b, ", ", 2);
            if (!rc) rc = stat_json(&b, hits[i], 1);
        }
        if (!rc) rc = buf_put(&b, "]", 1);
        for (size_t i = 0; i < n; i++) { free(hits[i]->key); free(hits[i]); }
        free(hits);
        uint8_t *out = rc ? NULL : frame_simple(OP_LIST_PREFIX_OK, f, b.p, b.len, out_len);
        free(b.p);
        return out;
    }
    case OP_WRITE_AT: {
        /* value: u64 LE offset + data. Atomic in-place write under the
         * stripe lock: creates the object if absent, zero-fills any gap,
         * bumps the version. Replaces the client's stat/get/stat/CAS-put
         * read-modify-write (4 round trips + a whole-stripe transfer). */
        if (f->val_len < 8) return frame_error(f, "STORAGE_ERROR", "short WRITE_AT value", out_len);
        uint64_t off = rd_u64le(f->val);
        size_t dlen = f->val_len - 8;
        const uint8_t *src = f->val + 8;
        if (off > (1ull << 40)) return frame_error(f, "STORAGE_ERROR", "offset too large", out_len);
        double t = now_s();
        pthread_mutex_lock(&st->lock);
        kv_entry **pp = kv_find(st, h, f->key, f->key_len);
        kv_entry *e = *pp;
        size_t old_size = e ? e->size : 0, end = (size_t)off + dlen;
        size_t new_size = end > old_size ? end : old_size;
        if (!e) {
            e = calloc(1, sizeof(*e));
            char *k = malloc(f->key_len ? f->key_len : 1);
            if (!e || !k) { pthread_mutex_unlock(&st->lock); free(e); free(k);
                            return frame_error(f, "STORAGE_ERROR", "out of memory", out_len); }
            memcpy(k, f->key, f->key_len);
            e->hash = h; e->key = k; e->key_len = f->key_len; e->created_at = t;
            e->version = *wm;  /* ++ below: starts above anything this key had */
            e->next = *pp;
            *pp = e;
            st->count++;
            atomic_fetch_add(&s->objects, 1);
            if (st->count > st->nslots * 2) kv_grow(st);
        }
        int rc = 0;
        /* Direct path: the NIC's buffer to the device, no RAM copy -- when
         * the object has no RAM copy to keep coherent and its slab (or a
         * fresh one for a new object) holds the new size. */
        if (direct_ok(s, src, off, dlen) && !e->data &&
            (!e->on_tier || new_size <= ost_tier_class_bytes(e->ext.cls))) {
            tier_ext_t oldext;
            int moved;
            rc = ent_slab(s, e, new_size, &oldext, &moved, grow_reserve(off, new_size));  /* never moves: checked above */
            (void)moved;
            if (!rc && off > old_size) {  /* the gap reads as zeros */
                size_t gap = (size_t)off - old_size;
                uint8_t *z = calloc(1, gap);
                rc = z ? ost_tier_write(s->tier, &e->ext, old_size, z, gap) : -ENOMEM;
                free(z);
            }
            if (!rc) rc = ost_tier_write_direct(s->tier, &e->ext, off, src, dlen);
            if (!rc) atomic_fetch_add(&s->tier_bytes_written, dlen);
            if (!rc) atomic_fetch_add(&s->direct_writes, 1);
        } else if (s->tier && !e->data && e->on_tier && new_size <= ost_tier_class_bytes(e->ext.cls)) {
            /* evicted, and the write fits its slab: write the tier in place
             * (was: read the whole object back into RAM first -- for
             * 47008 bytes, up to a stripe's worth of NVMe reads) */
            if (off > old_size) {  /* the gap reads as zeros */
                size_t gap = (size_t)off - old_size;
                uint8_t *z = calloc(1, gap);
                rc = z ? ost_tier_write(s->tier, &e->ext, old_size, z, gap) : -ENOMEM;
                free(z);
                if (!rc) atomic_fetch_add(&s->tier_bytes_written, gap);
            }
            if (!rc) rc = ost_tier_write(s->tier, &e->ext, off, src, dlen);
            if (!rc) atomic_fetch_add(&s->tier_bytes_written, dlen);
        } else {
            if (s->tier) rc = ent_resident(s, e);
            int fresh = 0;
            if (!rc && (new_size > e->data_cap || !e->data)) {
                size_t cap;
                uint8_t *nd = ent_alloc_h(s, new_size, h, &cap);
                if (!nd) rc = -ENOMEM;
                else {
                    if (e->data) memcpy(nd, e->data, old_size);
                    if (s->tier && e->data) atomic_fetch_sub(&s->ram_bytes, cap_mem(e->data_cap));
                    ent_free_data(s, e->data, e->data_cap);
                    e->data = nd; e->data_cap = cap;
                    fresh = 1;
                    if (s->tier) atomic_fetch_add(&s->ram_bytes, cap_mem(cap));
                }
            }
            if (!rc) {
                if (!fresh) obj_begin(e->data);
                if (new_size > old_size) memset(e->data + old_size, 0, new_size - old_size);
                memcpy(e->data + off, src, dlen);
                e->size = new_size;
                obj_publish(&s->arena, e->data, new_size);
                if (s->tier && !atomic_load(&s->writeback_failed)) {
                    /* write-back: the tier copy is brought up to date
                     * when the object leaves RAM, one whole-object write
                     * instead of one per piece (IOR hard: ~22 per stripe,
                     * and the zero-filled gaps in front of them) */
                    e->tier_dirty = 1;
                    lru_touch(s, e);
                } else if (s->tier) {
                    /* the touched range, plus the zeroed gap when it grew.
                     * (Was [min(off, old size), new size): a write into
                     * the middle of an object rewrote everything after it
                     * -- IOR hard's out-of-order 47008-byte pieces made
                     * ~10x their bytes in tier writes.) */
                    uint64_t from = off > old_size ? old_size : off;
                    rc = ent_sync(s, e, from, (size_t)(off + dlen - from), grow_reserve(off, new_size));
                    lru_touch(s, e);
                }
            }
        }
        if (rc) {
            pthread_mutex_unlock(&st->lock);
            return frame_error(f, "STORAGE_ERROR", rc == -ENOSPC ? "tier full" : "tier write failed", out_len);
        }
        e->size = new_size;
        e->version++;
        e->updated_at = t;
        uint64_t version = e->version;
        pthread_mutex_unlock(&st->lock);
        atomic_fetch_add(&s->bytes, new_size - old_size);
        ost_evict(s);
        uint8_t ok[16];
        wr_u64be(ok, version);
        wr_u64be(ok + 8, new_size);
        return frame_simple(OP_WRITE_AT_OK, f, ok, 16, out_len);
    }
    case OP_GET_V:
    case OP_READ_AT: {
        /* GET_V: u64 BE version + data, one snapshot (replaces stat/get/stat).
         * READ_AT: value u64 LE offset + u32 LE length -> u64 BE version +
         * the bytes in [offset, min(offset+length, size)) (may be short). */
        uint64_t off = 0, len = UINT64_MAX;
        if (f->op == OP_READ_AT) {
            if (f->val_len < 12) return frame_error(f, "STORAGE_ERROR", "short READ_AT value", out_len);
            off = rd_u64le(f->val);
            len = rd_u32le(f->val + 8);
        }
        pthread_mutex_lock(&st->lock);
        kv_entry *e = *kv_find(st, h, f->key, f->key_len);
        if (!e) { pthread_mutex_unlock(&st->lock); return frame_error(f, "NOT_FOUND", msg, out_len); }
        size_t from = off < e->size ? (size_t)off : e->size;
        size_t n = e->size - from;
        if (len < n) n = (size_t)len;
        uint8_t *out = NULL;
        uint8_t *v = frame_alloc(f->op == OP_GET_V ? OP_GET_V_OK : OP_READ_AT_OK, f, 8 + n, &out, out_len);
        int rc = 0;
        if (v) {
            wr_u64be(v, e->version);
            if (n && e->data) { memcpy(v + 8, e->data + from, n); if (s->tier) lru_touch(s, e); }
            else if (n) { rc = ost_tier_read(s->tier, &e->ext, from, v + 8, n); atomic_fetch_add(&s->tier_reads, 1); }
        }
        pthread_mutex_unlock(&st->lock);
        if (rc) { roce_reply_free(out); return frame_error(f, "STORAGE_ERROR", "tier read failed", out_len); }
        return out;
    }
    case OP_TRUNCATE: {
        /* value: u64 LE size. Shrinks the object if it is longer (never
         * extends: a short or missing stripe object reads as zeros).
         * Reply: u64 BE version + u64 BE size (0/0 if absent). */
        if (f->val_len < 8) return frame_error(f, "STORAGE_ERROR", "short TRUNCATE value", out_len);
        uint64_t size = rd_u64le(f->val);
        uint64_t version = 0, cur = 0, freed = 0;
        pthread_mutex_lock(&st->lock);
        kv_entry *e = *kv_find(st, h, f->key, f->key_len);
        if (e) {
            if (e->size > size) {
                freed = e->size - size;
                e->size = (size_t)size;
                e->version++;
                e->updated_at = now_s();
                if (e->data) { obj_begin(e->data); obj_publish(&s->arena, e->data, e->size); }
            }
            version = e->version;
            cur = e->size;
        }
        pthread_mutex_unlock(&st->lock);
        atomic_fetch_sub(&s->bytes, freed);
        uint8_t ok[16];
        wr_u64be(ok, version);
        wr_u64be(ok + 8, cur);
        return frame_simple(OP_TRUNCATE_OK, f, ok, 16, out_len);
    }
    case OP_LEASE: {
        /* a descriptor for one-sided reads of this object's RAM copy:
         * u64 BE version, size, addr, seq, keyhash + u32 BE rkey.
         * NO_LEASE when the copy cannot be published (no server device yet,
         * registration refused): the client then reads by RPC. */
        pthread_mutex_lock(&st->lock);
        kv_entry *e = *kv_find(st, h, f->key, f->key_len);
        if (!e) { pthread_mutex_unlock(&st->lock); return frame_error(f, "NOT_FOUND", msg, out_len); }
        int rc = s->tier ? ent_resident(s, e) : (e->data ? 0 : -ENOENT);
        arena_chunk_t *c = rc ? NULL : arena_chunk_of(&s->arena, e->data);
        if (c && !c->rkey_ok) {
            if (!s->server || roce_server_mem_rkey(s->server, c->mem, &c->rkey)) c = NULL;
            else c->rkey_ok = 1;
        }
        if (!c) {
            pthread_mutex_unlock(&st->lock);
            atomic_fetch_add(&s->lease_misses, 1);
            ost_evict(s);
            return frame_error(f, "NO_LEASE", "object not readable one-sided", out_len);
        }
        if (s->tier) lru_touch(s, e);
        uint8_t ok[44];
        wr_u64be(ok, e->version);
        wr_u64be(ok + 8, e->size);
        wr_u64be(ok + 16, roce_server_mem_addr(s->server, c->mem, obj_hdr(e->data)));
        wr_u64be(ok + 24, atomic_load(&obj_hdr(e->data)->seq));
        wr_u64be(ok + 32, e->hash);
        uint32_t rk = c->rkey;
        ok[40] = rk >> 24; ok[41] = rk >> 16; ok[42] = rk >> 8; ok[43] = rk;
        pthread_mutex_unlock(&st->lock);
        atomic_fetch_add(&s->leases, 1);
        /* the copy just made resident counts against the RAM budget like a
         * write's: without this, one-sided reads of a tier-backed OST grew
         * its cache without bound (site: 29 GiB budget, 117 GiB, OOM kill).
         * This entry is at the LRU head; a copy evicted under a lease reads
         * an odd seq and the client falls back to an RPC read. */
        ost_evict(s);
        return frame_simple(OP_LEASE_OK, f, ok, 44, out_len);
    }
    case OP_VTOKEN: {
        pthread_mutex_lock(&st->lock);
        kv_entry *e = *kv_find(st, h, f->key, f->key_len);
        uint64_t tok = e ? e->version : *wm;
        pthread_mutex_unlock(&st->lock);
        uint8_t ok[8];
        wr_u64be(ok, tok);
        return frame_simple(OP_VTOKEN_OK, f, ok, 8, out_len);
    }
    case OP_BATCH: {
        /* value: u32 LE n + n x (u8 op, u32 LE key_len, u32 LE val_len, key, val).
         * Reply: u32 LE n + n x (u8 op, u32 LE val_len, val) -- each the
         * sub-request's own reply op/value (ERROR included). Executed in
         * order; one frame instead of n round trips. No nesting. The whole
         * frame is validated first: a malformed batch runs nothing. */
        if (f->val_len < 4) return frame_error(f, "STORAGE_ERROR", "short BATCH", out_len);
        uint32_t n = rd_u32le(f->val);
        size_t off = 4;
        for (uint32_t i = 0; i < n; i++) {
            if (off + 9 > f->val_len || f->val[off] == OP_BATCH)
                return frame_error(f, "STORAGE_ERROR", "malformed BATCH", out_len);
            size_t kl = rd_u32le(f->val + off + 1), vl = rd_u32le(f->val + off + 5);
            off += 9;
            if (kl > f->val_len - off || vl > f->val_len - off - kl)
                return frame_error(f, "STORAGE_ERROR", "malformed BATCH", out_len);
            off += kl + vl;
        }
        off = 4;
        buf_t b = {0};
        uint8_t hdr4[4];
        wr_u32le(hdr4, n);
        int rc = buf_put(&b, hdr4, 4);
        for (uint32_t i = 0; i < n && !rc; i++) {
            frame_t sub = {.op = f->val[off], .req_id = f->req_id};
            sub.key_len = rd_u32le(f->val + off + 1);
            sub.val_len = rd_u32le(f->val + off + 5);
            off += 9;
            sub.key = (const char *)f->val + off;
            sub.val = f->val + off + sub.key_len;
            off += (size_t)sub.key_len + sub.val_len;
            size_t rl = 0;
            uint8_t *r = ost_handle(s, &sub, &rl);
            if (!r) { rc = -1; break; }
            frame_t rf;
            if (frame_decode(r, rl, &rf)) { roce_reply_free(r); rc = -1; break; }
            uint8_t sh[5];
            sh[0] = rf.op;
            wr_u32le(sh + 1, rf.val_len);
            rc = buf_put(&b, sh, 5) || buf_put(&b, rf.val, rf.val_len);
            roce_reply_free(r);
        }
        uint8_t *out = rc ? NULL : frame_simple(OP_BATCH_OK, f, b.p, b.len, out_len);
        free(b.p);
        return out;
    }
    default: {
        char m[48];
        snprintf(m, sizeof(m), "unsupported op %u", f->op);
        return frame_error(f, "UNSUPPORTED_OP", m, out_len);
    }
    }
}

/* ------------------------------------------------------------------ */
/* MDT: parent-dir -> (name -> entry), fs/mdt_node.py semantics          */
/* ------------------------------------------------------------------ */
typedef struct md_entry {
    struct md_entry *next;
    uint64_t hash;
    char *name;
    size_t name_len;
    int is_dir, has_layout;
    uint64_t size;
    double mtime, ctime;
    uint32_t sc, so;
    uint64_t ss;
    /* Data-on-MDT: a small file's bytes live in its entry (size bytes) until
     * it outgrows the inline limit; then it is frozen, copied to the OSTs by
     * a client and committed (see fs/mdt_wire.py for the protocol). */
    int inl, frozen;
    uint8_t *idata;
    uint64_t iver;
} md_entry;

/* Locking (was: one mutex for the whole MDT, which serialized every
 * request of a hot directory): the directory table is under a read-write
 * lock taken for reading on every request and for writing only to add a
 * directory; each directory's entries are split by name hash into
 * MD_BUCKETS independently locked hash tables, so requests for different
 * names in the same directory run in parallel. An md_dir is never freed
 * (rmdir removes the entry in its parent; the empty table stays), so a
 * pointer obtained under the read lock stays valid after it is released. */
#define MD_BUCKETS 64

typedef struct {
    pthread_mutex_t lock;
    md_entry **slots;
    size_t nslots, count;
} md_bucket;

/* A paged listing's sorted snapshot of a directory: records in wire form
 * (u16 LE name length, name, entry) in name order. Built by a listing's
 * first page and served to its later pages by binary search on the cursor.
 * Before it, every page copied and sorted all entries after the cursor: a
 * 13 M-entry mdtest-hard directory at ~150 k entries per page was ~90 full
 * copy-and-sorts per MDT, and IO500 find took 1130 s (i3en, 2026-10-25). */
typedef struct rd_snap {
    uint8_t *buf;
    size_t *off;   /* n + 1 record offsets into buf */
    size_t n;
    uint64_t built_ns;
    int refs;      /* under the directory's snap_lk */
    int attached;  /* still the directory's current snapshot */
} rd_snap;

typedef struct md_dir {
    struct md_dir *next;  /* in the directory hash chain */
    uint64_t hash;
    char *path;
    size_t path_len;
    md_bucket b[MD_BUCKETS];
    pthread_mutex_t snap_lk;
    rd_snap *snap;
} md_dir;

/* ---- the slot table: entries readable one-sided ----
 * A registered open-addressing table keyed by the hash of the full path:
 * every entry's wire encoding is kept in a 256-byte slot under a seqlock
 * (seq 0 empty, odd being written / deleted, even valid; the tail repeats
 * the seq). A client hashes the path, RDMA READs MD_PROBE consecutive
 * slots and takes a valid matching one as the lookup's answer; an empty
 * slot in the run means ENOENT; anything else (a run with no room, a slot
 * mid-write) sends it to the RPC. Fixed size (ROCEPOOL_MDT_SLOTS, default
 * 1 M = 256 MiB); when a run is full the entry is simply not published. */
#define MD_SLOT 256
#define MD_PROBE 8
#define MD_SLOT_TAIL (MD_SLOT - 8)
#define MD_SLOT_LOCKS 256
typedef struct {
    uint8_t *base;
    size_t nslots;
    roce_mem_t *mem;
    uint32_t rkey;
    int rkey_ok;
    pthread_mutex_t lk[MD_SLOT_LOCKS];
    atomic_ullong seq_gen, published, unpublished, used;
} md_table;

typedef struct {
    pthread_rwlock_t dirs_lock;
    md_dir **dirs;
    size_t ndirs_slots, ndirs;
    atomic_ullong ops;
    md_table tab;
    roce_server_t *server;
    /* STATS: wall time waiting for / holding a bucket lock; with MD_BUCKETS
     * stripes per directory, hold time per request / MD_BUCKETS bounds the
     * MDT's throughput on one directory */
    atomic_ullong lock_wait_ns, lock_hold_ns;
    atomic_ullong by_op[16];
    /* readdir: entries copied out of the buckets (snapshot builds) and
     * snapshots built; a listing of N entries costs ~N, not N x pages */
    atomic_ullong rd_copied, rd_snaps;
} mdt_store;

static mdt_store *mdt_new(void) {
    mdt_store *m = calloc(1, sizeof(*m));
    if (!m) return NULL;
    pthread_rwlock_init(&m->dirs_lock, NULL);
    m->ndirs_slots = 256;
    m->dirs = calloc(m->ndirs_slots, sizeof(md_dir *));
    if (!m->dirs) return NULL;
    const char *v = getenv("ROCEPOOL_MDT_SLOTS");
    size_t n = v && atoll(v) > 0 ? (size_t)atoll(v) : (1u << 20);
    if (n < MD_PROBE * 2) n = MD_PROBE * 2;
    m->tab.nslots = n;
    m->tab.base = mmap(NULL, n * MD_SLOT, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (m->tab.base == MAP_FAILED) { m->tab.base = NULL; m->tab.nslots = 0; }
    else m->tab.mem = roce_mem_register(m->tab.base, n * MD_SLOT);
    for (int i = 0; i < MD_SLOT_LOCKS; i++) pthread_mutex_init(&m->tab.lk[i], NULL);
    return m;
}

static size_t entry_len(const md_entry *e);
static void entry_write(uint8_t *p, const md_entry *e);

/* publish the entry at `path` (NULL: it was deleted) to the slot table */
static void md_slot_sync(mdt_store *m, const char *path, size_t pl, const md_entry *e) {
    md_table *t = &m->tab;
    if (!t->base) return;
    uint64_t h = fnv1a(path, pl);
    size_t start = (size_t)(h % t->nslots);
    uint8_t *hit = NULL, *empty = NULL;
    /* the run's locks, in index order (a run may wrap) */
    for (size_t i = 0; i < MD_PROBE; i++) {
        size_t k = (start + i) % t->nslots;
        uint8_t *sl = t->base + k * MD_SLOT;
        pthread_mutex_lock(&t->lk[k % MD_SLOT_LOCKS]);
        uint64_t seq, ph;
        memcpy(&seq, sl, 8);
        memcpy(&ph, sl + 8, 8);
        if (seq != 0 && ph == h) { hit = sl; pthread_mutex_unlock(&t->lk[k % MD_SLOT_LOCKS]); break; }
        if (seq == 0 && !empty) { empty = sl; pthread_mutex_unlock(&t->lk[k % MD_SLOT_LOCKS]); continue; }
        pthread_mutex_unlock(&t->lk[k % MD_SLOT_LOCKS]);
    }
    uint8_t *sl = hit ? hit : empty;
    if (!sl) { atomic_fetch_add(&t->unpublished, 1); return; }
    size_t k = (size_t)(sl - t->base) / MD_SLOT;
    pthread_mutex_lock(&t->lk[k % MD_SLOT_LOCKS]);
    uint64_t seq;
    memcpy(&seq, sl, 8);
    if (!hit && seq != 0) { pthread_mutex_unlock(&t->lk[k % MD_SLOT_LOCKS]); atomic_fetch_add(&t->unpublished, 1); return; }
    uint64_t odd = (seq | 1);
    if (seq == 0) { odd = 1; atomic_fetch_add(&t->used, 1); }
    __atomic_store_n((uint64_t *)sl, odd, __ATOMIC_SEQ_CST);
    memcpy(sl + 8, &h, 8);
    if (e) {
        uint32_t elen = (uint32_t)entry_len(e);
        memcpy(sl + 16, &elen, 4);
        entry_write(sl + 24, e);
        uint64_t nseq = (atomic_fetch_add(&t->seq_gen, 1) + 1) << 1;
        memcpy(sl + MD_SLOT_TAIL, &nseq, 8);
        __atomic_thread_fence(__ATOMIC_SEQ_CST);
        __atomic_store_n((uint64_t *)sl, nseq, __ATOMIC_SEQ_CST);
        atomic_fetch_add(&t->published, 1);
    }
    /* deleted: stays odd (a tombstone keyed by the path, reused on recreate) */
    pthread_mutex_unlock(&t->lk[k % MD_SLOT_LOCKS]);
}

/* normalise like fs/namespace_shard.normalize: drop "", "." components and
 * surrounding slashes. Writes into `out` (>= len+1 bytes). */
static size_t norm_path(const char *in, size_t len, char *out) {
    size_t o = 0, i = 0;
    while (i < len) {
        while (i < len && in[i] == '/') i++;
        size_t st = i;
        while (i < len && in[i] != '/') i++;
        size_t cl = i - st;
        if (cl == 0 || (cl == 1 && in[st] == '.')) continue;
        if (o) out[o++] = '/';
        memcpy(out + o, in + st, cl);
        o += cl;
    }
    out[o] = 0;
    return o;
}

static md_dir *md_dir_lookup(mdt_store *m, const char *p, size_t pl, uint64_t h) {
    for (md_dir *d = m->dirs[h % m->ndirs_slots]; d; d = d->next)
        if (d->hash == h && d->path_len == pl && memcmp(d->path, p, pl) == 0) return d;
    return NULL;
}

/* returns a stable pointer (or NULL); holds dirs_lock only inside */
static md_dir *md_get_dir(mdt_store *m, const char *p, size_t pl, int create) {
    uint64_t h = fnv1a(p, pl);
    pthread_rwlock_rdlock(&m->dirs_lock);
    md_dir *d = md_dir_lookup(m, p, pl, h);
    pthread_rwlock_unlock(&m->dirs_lock);
    if (d || !create) return d;
    d = calloc(1, sizeof(*d));
    char *cp = malloc(pl + 1);
    if (!d || !cp) { free(d); free(cp); return NULL; }
    for (int i = 0; i < MD_BUCKETS; i++) {
        pthread_mutex_init(&d->b[i].lock, NULL);
        d->b[i].nslots = 4;
        d->b[i].slots = calloc(4, sizeof(md_entry *));
        if (!d->b[i].slots) { for (int k = 0; k < i; k++) free(d->b[k].slots); free(d); free(cp); return NULL; }
    }
    memcpy(cp, p, pl); cp[pl] = 0;
    d->hash = h; d->path = cp; d->path_len = pl;
    pthread_mutex_init(&d->snap_lk, NULL);
    pthread_rwlock_wrlock(&m->dirs_lock);
    md_dir *race = md_dir_lookup(m, p, pl, h);  /* another thread added it meanwhile? */
    if (race) {
        pthread_rwlock_unlock(&m->dirs_lock);
        for (int i = 0; i < MD_BUCKETS; i++) free(d->b[i].slots);
        free(cp); free(d);
        return race;
    }
    md_dir **pp = &m->dirs[h % m->ndirs_slots];
    d->next = *pp;
    *pp = d;
    if (++m->ndirs > m->ndirs_slots * 2) {  /* grow the directory table */
        size_t n = m->ndirs_slots * 2;
        md_dir **nd = calloc(n, sizeof(md_dir *));
        if (nd) {
            for (size_t i = 0; i < m->ndirs_slots; i++) {
                md_dir *x = m->dirs[i];
                while (x) { md_dir *nx = x->next; x->next = nd[x->hash % n]; nd[x->hash % n] = x; x = nx; }
            }
            free(m->dirs);
            m->dirs = nd;
            m->ndirs_slots = n;
        }
    }
    pthread_rwlock_unlock(&m->dirs_lock);
    return d;
}

static md_bucket *md_bucket_of(md_dir *d, uint64_t h) { return &d->b[h % MD_BUCKETS]; }

/* caller holds the bucket's lock */
static md_entry **md_find(md_bucket *b, const char *name, size_t nl, uint64_t h) {
    md_entry **pp = &b->slots[(h / MD_BUCKETS) % b->nslots];
    for (; *pp; pp = &(*pp)->next)
        if ((*pp)->hash == h && (*pp)->name_len == nl && memcmp((*pp)->name, name, nl) == 0) return pp;
    return pp;
}

static void md_grow(md_bucket *b) {
    size_t n = b->nslots * 2;
    md_entry **slots = calloc(n, sizeof(md_entry *));
    if (!slots) return;
    for (size_t i = 0; i < b->nslots; i++) {
        md_entry *e = b->slots[i];
        while (e) { md_entry *nx = e->next; size_t j = (e->hash / MD_BUCKETS) % n; e->next = slots[j]; slots[j] = e; e = nx; }
    }
    free(b->slots);
    b->slots = slots;
    b->nslots = n;
}

#define ENTRY_HEAD 26  /* u8 is_dir, u8 has_layout, u64 size, f64 mtime, f64 ctime */
#define ENTRY_LAYOUT 16  /* u32 sc, u64 ss, u32 so */

static size_t entry_len(const md_entry *e) { return ENTRY_HEAD + (e->has_layout ? ENTRY_LAYOUT : 0); }

/* entry flags byte: bit0 has_layout, bit1 inline (data in the MDT), bit2 frozen */
static void entry_write(uint8_t *p, const md_entry *e) {
    p[0] = (uint8_t)e->is_dir;
    p[1] = (uint8_t)((e->has_layout ? 1 : 0) | (e->inl ? 2 : 0) | (e->frozen ? 4 : 0));
    wr_u64le(p + 2, e->size);
    memcpy(p + 10, &e->mtime, 8);  /* little-endian IEEE754 on every target we build for */
    memcpy(p + 18, &e->ctime, 8);
    if (e->has_layout) {
        wr_u32le(p + 26, e->sc);
        wr_u64le(p + 30, e->ss);
        wr_u32le(p + 38, e->so);
    }
}

static uint8_t *entry_frame(const frame_t *f, const md_entry *e, size_t *out_len) {
    uint8_t *out;
    uint8_t *v = frame_alloc(OP_MDT_OK, f, entry_len(e), &out, out_len);
    if (v) entry_write(v, e);
    return out;
}

/* inline limit: ROCEPOOL_INLINE_MAX (default 32 KiB, 0 disables), never
 * more than one stripe, so a spill only ever fills stripe 0 */
static uint64_t inline_limit(const md_entry *e) {
    static long lim = -1;
    if (lim < 0) {
        const char *v = getenv("ROCEPOOL_INLINE_MAX");
        lim = v && *v ? atol(v) : 32768;
        if (lim < 0) lim = 0;
    }
    uint64_t l = (uint64_t)lim;
    return e->has_layout && e->ss && e->ss < l ? e->ss : l;
}

/* All inline file bytes this MDT process holds, against a budget:
 * ROCEPOOL_MDT_INLINE_BUDGET_MIB, default a fifth of the host's memory.
 * Without one, IO500's mdtest-hard (3901-byte files, millions of them)
 * grew an MDT past 50 GiB on a 92 GiB host (EFA run, 2026-10-24) and the
 * run failed there. Beyond the budget new files start striped and an
 * inline file that would grow answers INLINE_FULL -- the client then
 * spills it to the OSTs, the path every file above the inline limit takes. */
static _Atomic uint64_t g_inline_bytes;
static uint64_t inline_budget(void) {
    static uint64_t b = UINT64_MAX;
    if (b == UINT64_MAX) {
        const char *v = getenv("ROCEPOOL_MDT_INLINE_BUDGET_MIB");
        const char *vb = getenv("ROCEPOOL_MDT_INLINE_BUDGET_BYTES");  /* tests */
        if (vb && *vb) b = (uint64_t)atoll(vb);
        else if (v && *v) b = (uint64_t)atoll(v) << 20;
        else {
            long pages = sysconf(_SC_PHYS_PAGES), psz = sysconf(_SC_PAGESIZE);
            b = pages > 0 && psz > 0 ? (uint64_t)pages * (uint64_t)psz / 5 : (uint64_t)8 << 30;
        }
    }
    return b;
}
static int inline_room(uint64_t more) { return atomic_load(&g_inline_bytes) + more <= inline_budget(); }

/* resize the inline buffer to n bytes (zero-filling growth); 0 or -1
 * (-2: over the MDT's inline budget) */
static int inline_resize(md_entry *e, uint64_t n) {
    if (n > e->size && !inline_room(n - e->size)) return -2;
    uint8_t *d = realloc(e->idata, n ? n : 1);
    if (!d) return -1;
    if (n >= e->size) atomic_fetch_add(&g_inline_bytes, n - e->size);
    else atomic_fetch_sub(&g_inline_bytes, e->size - n);
    if (n > e->size) memset(d + e->size, 0, n - e->size);
    e->idata = d;
    e->size = n;
    return 0;
}

/* READ_INLINE / FREEZE reply: u64 LE inline_ver + entry + inline bytes [off, off+len) */
static uint8_t *inline_frame(const frame_t *f, const md_entry *e, uint64_t off, uint64_t len, size_t *out_len) {
    size_t n = 0;
    if (e->inl && off < e->size) n = (size_t)(e->size - off < len ? e->size - off : len);
    uint8_t *out;
    uint8_t *v = frame_alloc(OP_MDT_OK, f, 8 + entry_len(e) + n, &out, out_len);
    if (!v) return NULL;
    wr_u64le(v, e->iver);
    entry_write(v + 8, e);
    if (n) memcpy(v + 8 + entry_len(e), e->idata + off, n);
    return out;
}

static int cmp_md_name(const void *a, const void *b) {
    const md_entry *x = *(md_entry *const *)a, *y = *(md_entry *const *)b;
    size_t n = x->name_len < y->name_len ? x->name_len : y->name_len;
    int c = memcmp(x->name, y->name, n);
    return c ? c : (x->name_len > y->name_len) - (x->name_len < y->name_len);
}

static void rd_snap_unref(md_dir *d, rd_snap *s) {
    pthread_mutex_lock(&d->snap_lk);
    int last = --s->refs == 0 && !s->attached;
    pthread_mutex_unlock(&d->snap_lk);
    if (last) { free(s->buf); free(s->off); free(s); }
}

/* replace the directory's snapshot with s (NULL: just drop it) */
static void rd_snap_attach(md_dir *d, rd_snap *s) {
    pthread_mutex_lock(&d->snap_lk);
    rd_snap *old = d->snap;
    d->snap = s;
    if (s) s->attached = 1;
    int free_old = 0;
    if (old) { old->attached = 0; free_old = old->refs == 0; }
    pthread_mutex_unlock(&d->snap_lk);
    if (free_old) { free(old->buf); free(old->off); free(old); }
}

/* copy every entry of d (bucket by bucket, each under its lock) into a
 * sorted snapshot; NULL when out of memory */
static rd_snap *rd_snap_build(mdt_store *m, md_dir *d) {
    size_t n = 0, cap = 1024, bytes = 0, bcap = 1 << 16;
    md_entry *list = malloc(cap * sizeof(*list));
    uint8_t *names = malloc(bcap);  /* names packed back to back */
    size_t *noff = malloc(cap * sizeof(*noff));
    int oom = !list || !names || !noff;
    for (int bi = 0; !oom && d && bi < MD_BUCKETS; bi++) {
        md_bucket *b = &d->b[bi];
        uint64_t t0 = mono_ns();
        pthread_mutex_lock(&b->lock);
        uint64_t t1 = mono_ns();
        for (size_t i = 0; !oom && i < b->nslots; i++)
            for (md_entry *e = b->slots[i]; e; e = e->next) {
                if (n == cap) {
                    cap *= 2;
                    md_entry *nl = realloc(list, cap * sizeof(*list));
                    if (nl) list = nl;
                    size_t *no = realloc(noff, cap * sizeof(*noff));
                    if (no) noff = no;
                    if (!nl || !no) { oom = 1; break; }
                }
                if (bytes + e->name_len > bcap) {
                    while (bytes + e->name_len > bcap) bcap *= 2;
                    uint8_t *nb = realloc(names, bcap);
                    if (!nb) { oom = 1; break; }
                    names = nb;
                }
                list[n] = *e;
                list[n].idata = NULL;
                list[n].next = NULL;
                noff[n] = bytes;
                memcpy(names + bytes, e->name, e->name_len);
                bytes += e->name_len;
                n++;
            }
        pthread_mutex_unlock(&b->lock);
        atomic_fetch_add(&m->lock_wait_ns, t1 - t0);
        atomic_fetch_add(&m->lock_hold_ns, mono_ns() - t1);
    }
    atomic_fetch_add(&m->rd_copied, n);
    rd_snap *s = NULL;
    md_entry **ptrs = oom ? NULL : malloc((n ? n : 1) * sizeof(*ptrs));
    if (ptrs) {
        for (size_t i = 0; i < n; i++) { list[i].name = (char *)(names + noff[i]); ptrs[i] = &list[i]; }
        qsort(ptrs, n, sizeof(*ptrs), cmp_md_name);
        size_t total = 0;
        for (size_t i = 0; i < n; i++) total += 2 + ptrs[i]->name_len + entry_len(ptrs[i]);
        s = calloc(1, sizeof(*s));
        if (s) { s->buf = malloc(total ? total : 1); s->off = malloc((n + 1) * sizeof(size_t)); }
        if (s && s->buf && s->off) {
            uint8_t *v = s->buf;
            for (size_t i = 0; i < n; i++) {
                s->off[i] = (size_t)(v - s->buf);
                v[0] = (uint8_t)(ptrs[i]->name_len & 0xff);
                v[1] = (uint8_t)(ptrs[i]->name_len >> 8);
                memcpy(v + 2, ptrs[i]->name, ptrs[i]->name_len);
                v += 2 + ptrs[i]->name_len;
                entry_write(v, ptrs[i]);
                v += entry_len(ptrs[i]);
            }
            s->off[n] = total;
            s->n = n;
            s->built_ns = mono_ns();
            atomic_fetch_add(&m->rd_snaps, 1);
        } else if (s) { free(s->buf); free(s->off); free(s); s = NULL; }
    }
    free(ptrs);
    free(list);
    free(names);
    free(noff);
    return s;
}

/* index of the first record whose name sorts after the cursor */
static size_t rd_snap_seek(const rd_snap *s, const uint8_t *cur, size_t clen) {
    size_t lo = 0, hi = s->n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        const uint8_t *r = s->buf + s->off[mid];
        size_t nl = (size_t)r[0] | (size_t)r[1] << 8;
        size_t k = nl < clen ? nl : clen;
        int c = memcmp(r + 2, cur, k);
        if (c < 0 || (c == 0 && nl <= clen)) lo = mid + 1; else hi = mid;
    }
    return lo;
}

/* a snapshot older than this is rebuilt rather than served to a later page */
#define RD_SNAP_MAX_AGE_NS (60ull * 1000000000ull)

static uint8_t *mdt_handle(mdt_store *m, const frame_t *f, size_t *out_len) {
    if (f->op != OP_MDT) {
        char msg[48];
        snprintf(msg, sizeof(msg), "%u", f->op);
        return frame_error(f, "UNSUPPORTED_OP", msg, out_len);
    }
    if (f->val_len < 1) return frame_error(f, "MDT_INTERNAL", "empty request", out_len);
    atomic_fetch_add(&m->ops, 1);
    char *path = malloc(f->key_len + 1);
    if (!path) return frame_error(f, "MDT_INTERNAL", "out of memory", out_len);
    size_t pl = norm_path(f->key, f->key_len, path);
    const char *slash = memrchr(path, '/', pl);
    const char *parent = path;
    size_t parent_len = slash ? (size_t)(slash - path) : 0;
    const char *name = slash ? slash + 1 : path;
    size_t name_len = pl - (size_t)(name - path);
    uint64_t nh = fnv1a(name, name_len);
    uint8_t op = f->val[0];
    uint8_t *out = NULL;

    atomic_fetch_add(&m->by_op[op & 15], 1);
    /* entry ops lock the name's bucket in the parent directory; readdir /
     * child_count (5, 6) walk the directory's buckets one at a time */
    md_dir *pd = NULL;
    md_bucket *bk = NULL;
    uint64_t tw = mono_ns(), th = tw;
    if (op != 5 && op != 6) {
        pd = md_get_dir(m, parent, parent_len, op == 2);
        if (pd) {
            bk = md_bucket_of(pd, nh);
            pthread_mutex_lock(&bk->lock);
            th = mono_ns();
        }
    }
    switch (op) {
    case 1: {  /* lookup */
        md_entry *e = bk ? *md_find(bk, name, name_len, nh) : NULL;
        out = e ? entry_frame(f, e, out_len) : frame_error(f, "NOT_FOUND", path, out_len);
        break;
    }
    case 2: {  /* create */
        if (f->val_len < 4) { out = frame_error(f, "MDT_INTERNAL", "short create", out_len); break; }
        int is_dir = f->val[1], exclusive = f->val[2], has_layout = f->val[3] & 1, inline_ok = f->val[3] & 2;
        if (has_layout && f->val_len < 4 + ENTRY_LAYOUT) { out = frame_error(f, "MDT_INTERNAL", "short layout", out_len); break; }
        if (!bk) { out = frame_error(f, "MDT_INTERNAL", "out of memory", out_len); break; }
        md_entry **pp = md_find(bk, name, name_len, nh);
        if (*pp) {
            if (exclusive || (*pp)->is_dir != is_dir) out = frame_error(f, "EXISTS", path, out_len);
            else out = entry_frame(f, *pp, out_len);
            break;
        }
        md_entry *e = calloc(1, sizeof(*e));
        char *nm = malloc(name_len + 1);
        if (!e || !nm) { free(e); free(nm); out = frame_error(f, "MDT_INTERNAL", "out of memory", out_len); break; }
        memcpy(nm, name, name_len); nm[name_len] = 0;
        e->hash = nh; e->name = nm; e->name_len = name_len;
        e->is_dir = is_dir ? 1 : 0;
        e->has_layout = has_layout ? 1 : 0;
        if (has_layout) { e->sc = rd_u32le(f->val + 4); e->ss = rd_u64le(f->val + 8); e->so = rd_u32le(f->val + 16); }
        e->inl = !is_dir && inline_ok && inline_limit(e) > 0 && inline_room(inline_limit(e));
        e->mtime = e->ctime = now_s();
        e->next = *pp;
        *pp = e;
        if (++bk->count > bk->nslots * 2) md_grow(bk);
        out = entry_frame(f, e, out_len);
        break;
    }
    case 3: {  /* set_size */
        if (f->val_len < 10) { out = frame_error(f, "MDT_INTERNAL", "short set_size", out_len); break; }
        md_entry *e = bk ? *md_find(bk, name, name_len, nh) : NULL;
        if (!e) { out = frame_error(f, "NOT_FOUND", path, out_len); break; }
        if (e->is_dir) { out = frame_error(f, "IS_DIR", path, out_len); break; }
        uint64_t size = rd_u64le(f->val + 2);
        uint64_t want = f->val[1] == 0 ? (size > e->size ? size : e->size) : size;
        if (e->inl) {  /* the inline bytes ARE the file: resize them (truncate only) */
            if (e->frozen) { out = frame_error(f, "FROZEN", path, out_len); break; }
            /* a size-growing write that went to the OSTs: the writer's cached
             * attributes were stale -- tell it to redo the write inline */
            if (f->val[1] == 0) { out = frame_error(f, "INLINE", path, out_len); break; }
            if (want > inline_limit(e)) { out = frame_error(f, "INLINE_FULL", path, out_len); break; }
            int rr = inline_resize(e, want);
            if (rr == -2) { out = frame_error(f, "INLINE_FULL", path, out_len); break; }
            if (rr) { out = frame_error(f, "MDT_INTERNAL", "out of memory", out_len); break; }
            e->iver++;
        }
        e->size = want;
        e->mtime = now_s();
        out = entry_frame(f, e, out_len);
        break;
    }
    case 4: {  /* unlink */
        if (f->val_len < 2) { out = frame_error(f, "MDT_INTERNAL", "short unlink", out_len); break; }
        md_entry **pp = bk ? md_find(bk, name, name_len, nh) : NULL;
        md_entry *e = pp ? *pp : NULL;
        if (!e) { out = frame_error(f, "NOT_FOUND", path, out_len); break; }
        if (e->is_dir != (f->val[1] ? 1 : 0)) { out = frame_error(f, e->is_dir ? "IS_DIR" : "NOT_DIR", path, out_len); break; }
        *pp = e->next;
        bk->count--;
        out = entry_frame(f, e, out_len);
        if (e->inl && e->idata) atomic_fetch_sub(&g_inline_bytes, e->size);
        free(e->idata);
        free(e->name);
        free(e);
        break;
    }
    case 5: {  /* readdir: entries of the directory `path` itself, sorted by
                * name. Bucket by bucket under that bucket's lock -- a
                * consistent snapshot per name, not of the whole directory
                * (same as the OST's LIST_PREFIX and POSIX readdir). Entries
                * are copied out (name + wire form) so no lock is held while
                * the reply is assembled.
                * Paged (request: u8 5, u32 LE max reply bytes, u16 LE cursor
                * length, cursor): only names after the cursor, at most the
                * reply budget of them; the count's top bit says more follow.
                * A one-byte request gets the whole directory in one reply
                * (older clients): a directory of millions of entries does
                * not fit one frame -- the site's IO500 find saw none of
                * mdtest-hard's 5.4 M files that way. */
        size_t budget = SIZE_MAX, clen = 0;
        const uint8_t *cursor = NULL;
        if (f->val_len >= 7) {
            budget = rd_u32le(f->val + 1);
            clen = (size_t)f->val[5] | (size_t)f->val[6] << 8;
            if (f->val_len < 7 + clen) { out = frame_error(f, "MDT_INTERNAL", "short readdir cursor", out_len); break; }
            cursor = f->val + 7;
            if (budget < 4096) budget = 4096;
        }
        md_dir *d = md_get_dir(m, path, pl, 0);
        if (budget != SIZE_MAX) {  /* paged: from the listing's snapshot */
            if (!d) {
                uint8_t *v = frame_alloc(OP_MDT_OK, f, 4, &out, out_len);
                if (v) wr_u32le(v, 0);
                break;
            }
            rd_snap *sn = NULL;
            if (clen) {  /* a later page: the current snapshot, if fresh */
                pthread_mutex_lock(&d->snap_lk);
                sn = d->snap;
                if (sn && mono_ns() - sn->built_ns > RD_SNAP_MAX_AGE_NS) sn = NULL;
                if (sn) sn->refs++;
                pthread_mutex_unlock(&d->snap_lk);
            }
            if (!sn) {  /* a first page (or none left): a new one */
                sn = rd_snap_build(m, d);
                if (!sn) { out = frame_error(f, "MDT_INTERNAL", "out of memory", out_len); break; }
                sn->refs = 1;
                rd_snap_attach(d, sn);
            }
            size_t i0 = clen ? rd_snap_seek(sn, cursor, clen) : 0, i1 = i0;
            size_t tot = 4;
            for (; i1 < sn->n; i1++) {  /* at least one record, then the budget */
                size_t add = sn->off[i1 + 1] - sn->off[i1];
                if (i1 > i0 && tot + add > budget) break;
                tot += add;
            }
            uint8_t *v = frame_alloc(OP_MDT_OK, f, tot, &out, out_len);
            if (v) {
                wr_u32le(v, (uint32_t)(i1 - i0) | (i1 < sn->n ? 0x80000000u : 0));
                memcpy(v + 4, sn->buf + sn->off[i0], sn->off[i1] - sn->off[i0]);
            }
            int done = i1 >= sn->n;
            if (done) {  /* the listing's last page: free it unless replaced */
                pthread_mutex_lock(&d->snap_lk);
                int mine = d->snap == sn;
                pthread_mutex_unlock(&d->snap_lk);
                if (mine) rd_snap_attach(d, NULL);
            }
            rd_snap_unref(d, sn);
            break;
        }
        size_t n = 0, cap = 64, total = 4;
        md_entry *list = malloc(cap * sizeof(*list));
        if (!list) { out = frame_error(f, "MDT_INTERNAL", "out of memory", out_len); break; }
        for (int bi = 0; d && bi < MD_BUCKETS; bi++) {
            md_bucket *b = &d->b[bi];
            uint64_t t0 = mono_ns();
            pthread_mutex_lock(&b->lock);
            uint64_t t1 = mono_ns();
            for (size_t i = 0; i < b->nslots; i++)
                for (md_entry *e = b->slots[i]; e; e = e->next) {
                    if (cursor) {  /* only names after the cursor (the cmp_md_name order) */
                        size_t k = e->name_len < clen ? e->name_len : clen;
                        int c = memcmp(e->name, cursor, k);
                        if (c < 0 || (c == 0 && e->name_len <= clen)) continue;
                    }
                    if (n == cap) { cap *= 2; md_entry *nl = realloc(list, cap * sizeof(*list)); if (!nl) continue; list = nl; }
                    list[n] = *e;
                    list[n].idata = NULL;
                    list[n].name = malloc(e->name_len ? e->name_len : 1);
                    if (!list[n].name) continue;
                    memcpy(list[n].name, e->name, e->name_len);
                    n++;
                }
            pthread_mutex_unlock(&b->lock);
            atomic_fetch_add(&m->lock_wait_ns, t1 - t0);
            atomic_fetch_add(&m->lock_hold_ns, mono_ns() - t1);
        }
        md_entry **ptrs = malloc((n ? n : 1) * sizeof(*ptrs));
        if (!ptrs) { for (size_t i = 0; i < n; i++) free(list[i].name); free(list); out = frame_error(f, "MDT_INTERNAL", "out of memory", out_len); break; }
        for (size_t i = 0; i < n; i++) ptrs[i] = &list[i];
        qsort(ptrs, n, sizeof(*ptrs), cmp_md_name);
        size_t emit = 0;  /* the first entries that fit the budget (at least one) */
        for (; emit < n; emit++) {
            size_t add = 2 + ptrs[emit]->name_len + entry_len(ptrs[emit]);
            if (emit && total + add > budget) break;
            total += add;
        }
        uint8_t *v = frame_alloc(OP_MDT_OK, f, total, &out, out_len);
        if (v) {
            wr_u32le(v, (uint32_t)emit | (emit < n ? 0x80000000u : 0));
            v += 4;
            for (size_t i = 0; i < emit; i++) {
                v[0] = (uint8_t)(ptrs[i]->name_len & 0xff);
                v[1] = (uint8_t)(ptrs[i]->name_len >> 8);
                memcpy(v + 2, ptrs[i]->name, ptrs[i]->name_len);
                v += 2 + ptrs[i]->name_len;
                entry_write(v, ptrs[i]);
                v += entry_len(ptrs[i]);
            }
        }
        for (size_t i = 0; i < n; i++) free(list[i].name);
        free(list);
        free(ptrs);
        break;
    }
    case 6: {  /* child_count */
        md_dir *d = md_get_dir(m, path, pl, 0);
        uint64_t cnt = 0;
        for (int bi = 0; d && bi < MD_BUCKETS; bi++) {
            pthread_mutex_lock(&d->b[bi].lock);
            cnt += d->b[bi].count;
            pthread_mutex_unlock(&d->b[bi].lock);
        }
        uint8_t c[8];
        wr_u64le(c, cnt);
        out = frame_simple(OP_MDT_OK, f, c, 8, out_len);
        break;
    }
    case 7:    /* write_inline: u64 LE offset + data */
    case 8:    /* read_inline: u64 LE offset + u32 LE length */
    case 9:    /* freeze */
    case 10: { /* spill_commit */
        md_entry *e = bk ? *md_find(bk, name, name_len, nh) : NULL;
        if (!e) { out = frame_error(f, "NOT_FOUND", path, out_len); break; }
        if (e->is_dir) { out = frame_error(f, "IS_DIR", path, out_len); break; }
        if (op == 8) {
            if (f->val_len < 13) { out = frame_error(f, "MDT_INTERNAL", "short read_inline", out_len); break; }
            out = inline_frame(f, e, rd_u64le(f->val + 1), rd_u32le(f->val + 9), out_len);
        } else if (op == 9) {
            if (e->inl) e->frozen = 1;  /* inline bytes immutable from here on */
            out = inline_frame(f, e, 0, UINT64_MAX, out_len);
        } else if (op == 10) {
            if (e->inl && !e->frozen) { out = frame_error(f, "NOT_FROZEN", path, out_len); break; }
            if (e->inl) {
                if (e->idata) atomic_fetch_sub(&g_inline_bytes, e->size);
                e->inl = 0; e->frozen = 0; free(e->idata); e->idata = NULL;
            }
            out = entry_frame(f, e, out_len);  /* idempotent: every helper may commit */
        } else {
            if (f->val_len < 9) { out = frame_error(f, "MDT_INTERNAL", "short write_inline", out_len); break; }
            if (!e->inl) { out = frame_error(f, "NOT_INLINE", path, out_len); break; }
            if (e->frozen) { out = frame_error(f, "FROZEN", path, out_len); break; }
            uint64_t off = rd_u64le(f->val + 1), n = f->val_len - 9, end = off + n;
            if (end > inline_limit(e) || end < off) { out = frame_error(f, "INLINE_FULL", path, out_len); break; }
            int rr = end > e->size ? inline_resize(e, end) : 0;
            if (rr == -2) { out = frame_error(f, "INLINE_FULL", path, out_len); break; }
            if (rr) { out = frame_error(f, "MDT_INTERNAL", "out of memory", out_len); break; }
            if (n) memcpy(e->idata + off, f->val + 9, n);
            e->iver++;
            e->mtime = now_s();
            out = entry_frame(f, e, out_len);
        }
        break;
    }
    case 12: {  /* the slot table: u64 LE addr, u64 LE nslots, u32 LE rkey (NO_TABLE if none) */
        md_table *t = &m->tab;
        if (t->base && !t->rkey_ok && m->server && !roce_server_mem_rkey(m->server, t->mem, &t->rkey)) t->rkey_ok = 1;
        if (!t->base || !t->rkey_ok) { out = frame_error(f, "NO_TABLE", "no one-sided lookup table", out_len); break; }
        uint8_t v[20];
        wr_u64le(v, roce_server_mem_addr(m->server, t->mem, t->base));
        wr_u64le(v + 8, t->nslots);
        wr_u32le(v + 16, t->rkey);
        out = frame_simple(OP_MDT_OK, f, v, 20, out_len);
        break;
    }
    default: {
        char msg[32];
        snprintf(msg, sizeof(msg), "op %u", op);
        out = frame_error(f, "UNSUPPORTED_OP", msg, out_len);
    }
    }
    if (bk && (op == 2 || op == 3 || op == 4 || op == 7 || op == 9 || op == 10)) {
        /* keep the one-sided table in step while the bucket is still ours */
        md_entry *e = op == 4 ? NULL : *md_find(bk, name, name_len, nh);
        if (op == 4 || e) md_slot_sync(m, path, pl, e);
    }
    if (bk) {
        uint64_t tu = mono_ns();
        pthread_mutex_unlock(&bk->lock);
        atomic_fetch_add(&m->lock_wait_ns, th - tw);
        atomic_fetch_add(&m->lock_hold_ns, tu - th);
    }
    free(path);
    return out;
}

/* ------------------------------------------------------------------ */
/* server glue                                                         */
/* ------------------------------------------------------------------ */
struct rocepool_native {
    roce_server_t *server;
    int kind;
    ost_store *ost;
    mdt_store *mdt;
    /* STATS (op 50): what a bottleneck analysis needs from each server --
     * requests and bytes each way, CPU spent inside the handler (the
     * server's own work, excluding the transport), and per-op counts */
    atomic_ullong reqs, cpu_ns, wall_ns, bytes_in, bytes_out, inflight, inflight_max;
    atomic_ullong ost_by_op[64];
};

static uint64_t thread_cpu_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* this process's resident memory, and what is free on the NUMA node it is
 * bound to (ROCEPOOL_NUMA_NODE): a server that dies of the OOM killer
 * shows the way there in the run's snapshots */
static void mem_facts(unsigned long long *rss, long long *node_free) {
    *rss = 0;
    *node_free = -1;
    FILE *f = fopen("/proc/self/statm", "r");
    if (f) {
        unsigned long long size, res;
        if (fscanf(f, "%llu %llu", &size, &res) == 2) *rss = res * (unsigned long long)sysconf(_SC_PAGESIZE);
        fclose(f);
    }
    const char *v = getenv("ROCEPOOL_NUMA_NODE");
    if (!v || !*v) return;
    char path[96], line[256];
    snprintf(path, sizeof(path), "/sys/devices/system/node/node%d/meminfo", atoi(v));
    if (!(f = fopen(path, "r"))) return;
    while (fgets(line, sizeof(line), f)) {
        char *p = strstr(line, "MemFree:");
        if (p) { *node_free = atoll(p + 8) * 1024LL; break; }
    }
    fclose(f);
}

static uint8_t *stats_frame(struct rocepool_native *n, const frame_t *f, size_t *out_len) {
    buf_t b = {0};
    unsigned long long rss;
    long long node_free;
    mem_facts(&rss, &node_free);
    int rc = buf_printf(&b, "{\"kind\": \"%s\", \"reqs\": %llu, \"handler_cpu_ns\": %llu, \"handler_wall_ns\": %llu, "
                        "\"bytes_in\": %llu, \"bytes_out\": %llu, \"inflight_max\": %llu",
                        n->kind == ROCEPOOL_NATIVE_OST ? "ost" : "mdt",
                        (unsigned long long)atomic_load(&n->reqs), (unsigned long long)atomic_load(&n->cpu_ns),
                        (unsigned long long)atomic_load(&n->wall_ns), (unsigned long long)atomic_load(&n->bytes_in),
                        (unsigned long long)atomic_load(&n->bytes_out), (unsigned long long)atomic_load(&n->inflight_max));
    if (!rc && n->kind == ROCEPOOL_NATIVE_MDT) {
        rc = buf_printf(&b, ", \"lock_wait_ns\": %llu, \"lock_hold_ns\": %llu, \"lock_stripes\": %d, "
                        "\"table_slots\": %llu, \"table_registered\": %s, \"table_used\": %llu, \"slots_published\": %llu, "
                        "\"slots_unpublished\": %llu, \"inline_bytes\": %llu, \"inline_budget\": %llu, "
                        "\"readdir_copied\": %llu, \"readdir_snapshots\": %llu, \"by_op\": {",
                        (unsigned long long)atomic_load(&n->mdt->lock_wait_ns),
                        (unsigned long long)atomic_load(&n->mdt->lock_hold_ns), MD_BUCKETS,
                        (unsigned long long)n->mdt->tab.nslots, n->mdt->tab.rkey_ok ? "true" : "false",
                        (unsigned long long)atomic_load(&n->mdt->tab.used),
                        (unsigned long long)atomic_load(&n->mdt->tab.published),
                        (unsigned long long)atomic_load(&n->mdt->tab.unpublished),
                        (unsigned long long)atomic_load(&g_inline_bytes), (unsigned long long)inline_budget(),
                        (unsigned long long)atomic_load(&n->mdt->rd_copied),
                        (unsigned long long)atomic_load(&n->mdt->rd_snaps));
        int first = 1;
        for (int i = 0; i < 16 && !rc; i++) {
            unsigned long long c = atomic_load(&n->mdt->by_op[i]);
            if (c) { rc = buf_printf(&b, "%s\"%d\": %llu", first ? "" : ", ", i, c); first = 0; }
        }
        if (!rc) rc = buf_put(&b, "}", 1);
    } else if (!rc) {
        rc = buf_printf(&b, ", \"objects\": %llu, \"bytes\": %llu",
                        (unsigned long long)atomic_load(&n->ost->objects), (unsigned long long)atomic_load(&n->ost->bytes));
        if (!rc)
            rc = buf_printf(&b, ", \"leases\": %llu, \"lease_misses\": %llu, \"arena_bytes\": %llu",
                            (unsigned long long)atomic_load(&n->ost->leases), (unsigned long long)atomic_load(&n->ost->lease_misses),
                            (unsigned long long)atomic_load(&n->ost->arena.bytes));
        if (!rc && n->ost->tier)
            rc = buf_printf(&b, ", \"tier\": \"%s\", \"tier_capacity\": %llu, \"tier_used\": %llu, \"tier_direct\": %s, "
                            "\"ram_cache_bytes\": %llu, \"ram_cache_budget\": %llu, \"direct_writes\": %llu, "
                            "\"tier_reads\": %llu, \"evictions\": %llu, \"tier_bytes_written\": %llu, \"writebacks\": %llu, \"inline_evictions\": %llu",
                            ost_tier_kind(n->ost->tier), (unsigned long long)ost_tier_capacity(n->ost->tier),
                            (unsigned long long)ost_tier_used(n->ost->tier), ost_tier_direct(n->ost->tier) ? "true" : "false",
                            (unsigned long long)atomic_load(&n->ost->ram_bytes), (unsigned long long)n->ost->ram_budget,
                            (unsigned long long)atomic_load(&n->ost->direct_writes),
                            (unsigned long long)atomic_load(&n->ost->tier_reads), (unsigned long long)atomic_load(&n->ost->evictions),
                            (unsigned long long)atomic_load(&n->ost->tier_bytes_written),
                            (unsigned long long)atomic_load(&n->ost->writebacks),
                            (unsigned long long)atomic_load(&n->ost->inline_evictions));
        if (!rc) rc = buf_put(&b, ", \"by_op\": {", 12);
        int first = 1;
        for (int i = 0; i < 64 && !rc; i++) {
            unsigned long long c = atomic_load(&n->ost_by_op[i]);
            if (c) { rc = buf_printf(&b, "%s\"%d\": %llu", first ? "" : ", ", i, c); first = 0; }
        }
        if (!rc) rc = buf_put(&b, "}", 1);
    }
    if (!rc && n->server) {  /* where a request's time goes outside the handler */
        char tx[1024];
        if (roce_server_stats_json(n->server, tx, sizeof(tx)) > 0) rc = buf_printf(&b, ", \"transport\": {%s}", tx);
    }
    if (!rc) rc = buf_printf(&b, ", \"rss_bytes\": %llu, \"numa_free_bytes\": %lld}", rss, node_free);
    uint8_t *out = rc ? NULL : frame_simple(OP_STATS_OK, f, b.p, b.len, out_len);
    free(b.p);
    return out;
}

static uint8_t *native_handler(const uint8_t *req, size_t req_len, size_t *resp_len, void *user) {
    struct rocepool_native *n = user;
    frame_t f;
    if (frame_decode(req, req_len, &f)) return NULL;  /* malformed: drop this connection */
    if (f.op == OP_STATS) return stats_frame(n, &f, resp_len);
    unsigned long long cur = atomic_fetch_add(&n->inflight, 1) + 1, mx = atomic_load(&n->inflight_max);
    while (cur > mx && !atomic_compare_exchange_weak(&n->inflight_max, &mx, cur)) {}
    uint64_t c0 = thread_cpu_ns(), w0 = mono_ns();
    if (n->kind == ROCEPOOL_NATIVE_OST) atomic_fetch_add(&n->ost_by_op[f.op & 63], 1);
    /* tests only: an OST whose writes take this long (a slow drive) */
    static long slow_ms = -1;
    if (slow_ms < 0) { const char *d = getenv("ROCEPOOL_TEST_OST_WRITE_DELAY_MS"); slow_ms = d ? atol(d) : 0; }
    if (slow_ms > 0 && n->kind == ROCEPOOL_NATIVE_OST && (f.op == OP_PUT || f.op == OP_WRITE_AT)) usleep(slow_ms * 1000);
    uint8_t *out = n->kind == ROCEPOOL_NATIVE_OST ? ost_handle(n->ost, &f, resp_len) : mdt_handle(n->mdt, &f, resp_len);
    atomic_fetch_add(&n->cpu_ns, thread_cpu_ns() - c0);
    atomic_fetch_add(&n->wall_ns, mono_ns() - w0);
    atomic_fetch_add(&n->reqs, 1);
    atomic_fetch_add(&n->bytes_in, req_len);
    if (out) atomic_fetch_add(&n->bytes_out, *resp_len);
    atomic_fetch_sub(&n->inflight, 1);
    return out;
}

/* P2P memory for the transport's server pool (ROCEPOOL_P2PMEM=<sysfs
 * p2pmem dir of an NVMe with a CMB>, Linux 6.2+: mmap of its "allocate"
 * file hands out the drive's own memory). The NIC then RDMA-READs a
 * write's data straight into the drive, and the tier's O_DIRECT write from
 * that buffer is a DMA inside the drive -- host memory never sees the
 * bytes. Whether the NIC's driver can register it (ibv_reg_mr on P2P pages
 * needs a driver with FOLL_PCI_P2PDMA support, mlx5 since 6.2) is found
 * out at the first chunk; the transport falls back to host memory. */
static char g_p2pmem_dir[512];
static void *p2pmem_alloc(size_t len, void *user) {
    (void)user;
    char path[600];
    snprintf(path, sizeof(path), "%s/allocate", g_p2pmem_dir);
    int fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0) { fprintf(stderr, "rocepool OST: p2pmem: open %s: %s\n", path, strerror(errno)); return NULL; }
    void *p = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { fprintf(stderr, "rocepool OST: p2pmem: mmap %zu bytes: %s\n", len, strerror(errno)); return NULL; }
    return p;
}
static void p2pmem_free(void *base, size_t len, void *user) { (void)user; munmap(base, len); }
static void tier_notify(void *base, size_t len, void *user) { ost_tier_notify_chunk(((ost_store *)user)->tier, base, len); }

/* ---- NUMA placement ----
 * ROCEPOOL_NUMA_NODE=<n>: this process runs on node n's cores only and
 * allocates its memory there first (MPOL_PREFERRED): the store, the
 * registered arena, the transport's buffer pools and every connection
 * thread stay local to the node whose NIC and NVMe it serves while it has
 * room, and spill to the other node when it has not. (MPOL_BIND made one
 * full node an OOM kill: on site a node with ~23 GiB free next to a 30 GiB
 * RAM cache lost its OST to the OOM killer mid-run.) tools/io500_run.py starts one MDT
 * and one OST per NUMA node on real hosts and sets this for each. */
static void numa_pin(void) {
    const char *v = getenv("ROCEPOOL_NUMA_NODE");
    if (!v || !*v) return;
    int node = atoi(v);
    char path[128];
    snprintf(path, sizeof(path), "/sys/devices/system/node/node%d/cpulist", node);
    FILE *f = fopen(path, "r");
    if (!f) { fprintf(stderr, "rocepool: NUMA node %d: no %s (ignored)\n", node, path); return; }
    char list[4096] = {0};
    if (!fgets(list, sizeof(list), f)) list[0] = 0;
    fclose(f);
    cpu_set_t set;
    CPU_ZERO(&set);
    int ncpu = 0;
    for (char *p = list; *p; ) {
        char *end;
        long a = strtol(p, &end, 10), b = a;
        if (end == p) break;
        if (*end == '-') { b = strtol(end + 1, &end, 10); }
        for (long c = a; c <= b && c < CPU_SETSIZE; c++) { CPU_SET((int)c, &set); ncpu++; }
        p = *end == ',' ? end + 1 : end;
    }
    if (ncpu == 0) return;
    if (sched_setaffinity(0, sizeof(set), &set)) fprintf(stderr, "rocepool: sched_setaffinity: %s\n", strerror(errno));
    /* MPOL_PREFERRED = 1: allocations from this node while it has room
     * (set_mempolicy, no libnuma) */
    unsigned long mask[16] = {0};
    if (node >= 0 && node < 1024) {
        mask[node / 64] = 1ul << (node % 64);
        if (syscall(SYS_set_mempolicy, 1, mask, 1024) && errno != ENOSYS)
            fprintf(stderr, "rocepool: set_mempolicy(MPOL_PREFERRED node %d): %s\n", node, strerror(errno));
    }
    char *nl = strchr(list, '\n');
    if (nl) *nl = 0;
    /* read the policy back: what the kernel actually applies (Mems_allowed
     * in /proc is the cpuset, not this) */
    int mode = -1;
    unsigned long got[16] = {0};
    const char *mem = "memory policy unknown";
    if (syscall(SYS_get_mempolicy, &mode, got, 1024, (void *)0, 0UL) == 0)
        mem = (mode == 1 && node >= 0 && node < 1024 && (got[node / 64] >> (node % 64) & 1)) ? "memory preferred from it"
                                                                                           : "memory policy NOT set";
    fprintf(stderr, "rocepool: pinned to NUMA node %d (%d cores: %s; %s)\n", node, ncpu, list, mem);
}

rocepool_native_t *rocepool_native_start(int kind, const char *bind_address, size_t max_frame_bytes) {
    static pthread_once_t pin_once = PTHREAD_ONCE_INIT;
    pthread_once(&pin_once, numa_pin);
    struct rocepool_native *n = calloc(1, sizeof(*n));
    if (!n) return NULL;
    n->kind = kind;
    if (kind == ROCEPOOL_NATIVE_OST) n->ost = ost_new();
    else if (kind == ROCEPOOL_NATIVE_MDT) n->mdt = mdt_new();
    if (!n->ost && !n->mdt) { free(n); return NULL; }
    if (n->ost && n->ost->tier) {
        const char *p2p = getenv("ROCEPOOL_P2PMEM");
        if (p2p && *p2p) {
            snprintf(g_p2pmem_dir, sizeof(g_p2pmem_dir), "%s", p2p);
            fprintf(stderr, "rocepool OST: request buffers from P2P memory %s\n", p2p);
        }
        roce_set_chunk_hooks(p2p && *p2p ? p2pmem_alloc : NULL, p2pmem_free, tier_notify, n->ost);
    }
    n->server = roce_server_start(bind_address, native_handler, n, max_frame_bytes);
    if (!n->server) return NULL;  /* store intentionally not freed: see rocepool_native_stop */
    if (n->ost) n->ost->server = n->server;
    if (n->mdt) n->mdt->server = n->server;
    if (n->mdt && n->mdt->tab.base && n->mdt->tab.mem) {
        /* pin the one-sided lookup table now, before READY: done lazily by
         * the first request that asks for it, a 16 GiB table took ~8 s on
         * site -- longer than the clients' 5 s RPC timeout (EIO) */
        md_table *t = &n->mdt->tab;
        uint64_t t0 = mono_ns();
        uint32_t rk;
        if (!roce_server_mem_rkey(n->server, t->mem, &rk)) {
            t->rkey = rk;
            t->rkey_ok = 1;
            double s = (double)(mono_ns() - t0) / 1e9;
            if (s > 0.5)
                fprintf(stderr, "rocepool MDT: lookup table %.1f GiB registered in %.1f s\n",
                        (double)t->nslots * MD_SLOT / (1u << 30), s);
        }
    }
    return n;
}

int rocepool_native_local_address(rocepool_native_t *n, char *out, size_t out_len) {
    return roce_server_local_address(n->server, out, out_len);
}

void rocepool_native_stats(rocepool_native_t *n, uint64_t *objects, uint64_t *bytes, uint64_t *ops) {
    if (n->kind == ROCEPOOL_NATIVE_OST) {
        *objects = atomic_load(&n->ost->objects);
        *bytes = atomic_load(&n->ost->bytes);
        *ops = atomic_load(&n->ost->ops);
    } else {
        pthread_rwlock_rdlock(&n->mdt->dirs_lock);
        uint64_t count = 0;
        for (size_t i = 0; i < n->mdt->ndirs_slots; i++)
            for (md_dir *d = n->mdt->dirs[i]; d; d = d->next)
                for (int bi = 0; bi < MD_BUCKETS; bi++) {
                    pthread_mutex_lock(&d->b[bi].lock);
                    count += d->b[bi].count;
                    pthread_mutex_unlock(&d->b[bi].lock);
                }
        pthread_rwlock_unlock(&n->mdt->dirs_lock);
        *objects = count;
        *bytes = 0;
        *ops = atomic_load(&n->mdt->ops);
    }
}

void rocepool_native_stop(rocepool_native_t *n) {
    /* Stops accepting; the store is deliberately NOT freed. Connection
     * threads are detached and may still be inside a handler (see
     * roce_server_stop), so freeing it here would be a use-after-free;
     * these servers live for the process lifetime anyway. */
    roce_server_stop(n->server);
}
