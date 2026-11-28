/* rocepool_ga_exiom.c -- the exiom backend of the global address client
 * library: an entry of the list given to rpga_open that names an exiom
 * memory server instead of an OST.
 *
 *     exiom://tenant@host:port|host:port|...
 *
 * The endpoints are the control ports (TCP) of the paths to the same memory:
 * the primary memory server first, then the backup, then a path through the
 * switch -- exiom/connect.py on each. The client takes the first that
 * answers and moves to the next when a request or a transfer fails.
 *
 * Control (TCP, 4-byte big-endian length + a JSON object, exiom/connect.py):
 * connect (this host's queue pair, its tenant) -> the target's queue pair
 * and the tenant's region (R_Key, virtual address, length, path MTU);
 * alloc / commit / lookup / free by key. Data: one-sided RDMA READ and
 * WRITE on a queue pair connected by hand (the FPGA has no connection
 * manager: roce_raw_qp_*), straight into and out of the registered buffer
 * -- GPU memory included -- or through a host bounce buffer when the NIC
 * cannot reach it.
 *
 * A region number of an address is a slot here: a key's value (its place,
 * version), from rpga_alloc / rpga_stage (this process placed it: no lease)
 * or rpga_lookup (someone else did: valid for the lease the server gave,
 * then looked up again -- a key whose value changed since reads -ENOENT,
 * never another value's bytes, as the server does not give the space of an
 * old value out again before the leases end). */
#define _GNU_SOURCE
#include "rocepool_ga_exiom.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define GX_ENDPOINTS 4
#define GX_SLOTS 65536
#define GX_KEY 128
#define GX_ONE_OP (1u << 30)        /* one RDMA operation: at most 1 GiB */
#define GX_BOUNCE (4u << 20)        /* host bounce buffer */
#define GX_TIMEOUT_MS 30000

typedef struct {
    int used;
    int owner;          /* placed by this process (alloc, stage): no lease */
    int pending;        /* staged: not committed yet */
    char key[GX_KEY];
    uint64_t va, size, version;
    double lease_until; /* looked up: valid until (CLOCK_MONOTONIC seconds) */
} gx_slot_t;

struct gx {
    char tenant[64];
    char host[GX_ENDPOINTS][64];
    int port[GX_ENDPOINTS];
    int n;
    int cur;            /* endpoint in use, -1: none */
    int fd;             /* its control connection */
    roce_raw_qp_t *qp;  /* its queue pair */
    uint32_t rkey;
    uint64_t region_va, region_len;
    uint32_t pmtu;
    pthread_mutex_t lk; /* one request or transfer at a time */
    gx_slot_t *slot;    /* [GX_SLOTS], slot 0 unused */
    uint32_t hint;
    uint64_t anon;
    uint8_t *bounce;
    roce_mem_t *bounce_mem;
    uint64_t failovers, requests;
};

static double mono(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + ts.tv_nsec / 1e9;
}

int gx_is_spec(const char *spec) { return spec && !strncmp(spec, "exiom://", 8); }

gx_t *gx_open(const char *spec) {
    if (!gx_is_spec(spec)) return NULL;
    gx_t *g = calloc(1, sizeof(*g));
    if (!g) return NULL;
    g->slot = calloc(GX_SLOTS, sizeof(gx_slot_t));
    if (!g->slot) { free(g); return NULL; }
    const char *p = spec + 8, *at = strchr(p, '@');
    if (!at || at == p || (size_t)(at - p) >= sizeof(g->tenant)) goto bad;
    memcpy(g->tenant, p, (size_t)(at - p));
    char *list = strdup(at + 1), *save = NULL;
    for (char *t = strtok_r(list, "|", &save); t && g->n < GX_ENDPOINTS; t = strtok_r(NULL, "|", &save)) {
        char *colon = strrchr(t, ':');
        if (!colon || colon == t || (size_t)(colon - t) >= sizeof(g->host[0])) continue;
        memcpy(g->host[g->n], t, (size_t)(colon - t));
        g->port[g->n] = atoi(colon + 1);
        if (g->port[g->n] > 0) g->n++;
    }
    free(list);
    if (!g->n) goto bad;
    g->cur = -1;
    g->fd = -1;
    g->hint = 1;
    pthread_mutex_init(&g->lk, NULL);
    return g;
bad:
    free(g->slot);
    free(g);
    return NULL;
}

static void disconnect(gx_t *g) {
    if (g->qp) { roce_raw_qp_destroy(g->qp); g->qp = NULL; }
    if (g->fd >= 0) { close(g->fd); g->fd = -1; }
    g->cur = -1;
}

void gx_close(gx_t *g) {
    if (!g) return;
    disconnect(g);
    if (g->bounce_mem) roce_mem_release(g->bounce_mem);
    free(g->bounce);
    pthread_mutex_destroy(&g->lk);
    free(g->slot);
    free(g);
}

/* ------------------------------------------------------------ messages */
/* The answers are flat JSON objects: strings, integers, true/false/null. */
typedef struct { char key[32]; const char *val; size_t len; int str; } jfield_t;
#define J_FIELDS 24

static const char *skip(const char *p) { while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++; return p; }

static const char *jstring_end(const char *p) {   /* p after the opening quote: the closing one */
    while (*p && *p != '"') p += (*p == '\\' && p[1]) ? 2 : 1;
    return *p ? p : NULL;
}

static int jparse(const char *s, jfield_t *f, int max) {
    int n = 0;
    const char *p = skip(s);
    if (*p++ != '{') return -1;
    for (p = skip(p); *p && *p != '}';) {
        if (*p != '"') return -1;
        const char *ke = jstring_end(p + 1);
        if (!ke) return -1;
        jfield_t x = {0};
        size_t kl = (size_t)(ke - p - 1);
        memcpy(x.key, p + 1, kl < sizeof(x.key) - 1 ? kl : sizeof(x.key) - 1);
        p = skip(ke + 1);
        if (*p++ != ':') return -1;
        p = skip(p);
        if (*p == '"') {
            const char *ve = jstring_end(p + 1);
            if (!ve) return -1;
            x.val = p + 1, x.len = (size_t)(ve - p - 1), x.str = 1;
            p = ve + 1;
        } else {
            x.val = p;
            while (*p && *p != ',' && *p != '}' && *p != ' ') p++;
            x.len = (size_t)(p - x.val);
        }
        if (n < max) f[n++] = x;
        p = skip(p);
        if (*p == ',') p = skip(p + 1);
    }
    return n;
}

static const jfield_t *jget(const jfield_t *f, int n, const char *key) {
    for (int i = 0; i < n; i++)
        if (!strcmp(f[i].key, key)) return &f[i];
    return NULL;
}

static int jint(const jfield_t *f, int n, const char *key, uint64_t *v) {
    const jfield_t *x = jget(f, n, key);
    if (!x || x->str || !x->len) return -1;
    char tmp[32];
    size_t l = x->len < sizeof(tmp) - 1 ? x->len : sizeof(tmp) - 1;
    memcpy(tmp, x->val, l);
    tmp[l] = 0;
    char *end;
    double d = strtod(tmp, &end);   /* a float (a lease of 1.5 s) or an integer */
    if (strchr(tmp, '.') || strchr(tmp, 'e')) { *v = (uint64_t)d; return 0; }
    *v = strtoull(tmp, &end, 10);
    return *end ? -1 : 0;
}

static double jnum(const jfield_t *f, int n, const char *key, double dflt) {
    const jfield_t *x = jget(f, n, key);
    if (!x || x->str) return dflt;
    char tmp[32];
    size_t l = x->len < sizeof(tmp) - 1 ? x->len : sizeof(tmp) - 1;
    memcpy(tmp, x->val, l);
    tmp[l] = 0;
    return strtod(tmp, NULL);
}

static int jok(const jfield_t *f, int n) {
    const jfield_t *x = jget(f, n, "ok");
    return x && !x->str && x->len == 4 && !memcmp(x->val, "true", 4);
}

/* the server's error text -> errno (exiom/connect.py's messages) */
static int jerrno(const jfield_t *f, int n) {
    const jfield_t *x = jget(f, n, "error");
    if (!x) return -EIO;
    char e[160];
    size_t l = x->len < sizeof(e) - 1 ? x->len : sizeof(e) - 1;
    memcpy(e, x->val, l);
    e[l] = 0;
    if (strstr(e, "No such")) return -ENOENT;
    if (strstr(e, "No space")) return -ENOSPC;
    if (strstr(e, "No queue pair")) return -EBUSY;
    if (strstr(e, "Unknown tenant")) return -EACCES;
    if (strstr(e, "multiple of 4") || strstr(e, "Unknown operation")) return -EINVAL;
    return -EIO;
}

/* a JSON string of s (quotes and escapes) into out */
static void jquote(char *out, size_t cap, const char *s) {
    size_t o = 0;
    out[o++] = '"';
    for (; *s && o + 8 < cap; s++) {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\') { out[o++] = '\\'; out[o++] = (char)c; }
        else if (c < 0x20) o += (size_t)snprintf(out + o, cap - o, "\\u%04x", c);
        else out[o++] = (char)c;
    }
    out[o++] = '"';
    out[o] = 0;
}

static int send_all(int fd, const void *b, size_t n) {
    const uint8_t *p = b;
    while (n) {
        ssize_t k = send(fd, p, n, MSG_NOSIGNAL);
        if (k <= 0) { if (k < 0 && errno == EINTR) continue; return -1; }
        p += k, n -= (size_t)k;
    }
    return 0;
}

static int recv_all(int fd, void *b, size_t n) {
    uint8_t *p = b;
    while (n) {
        ssize_t k = recv(fd, p, n, 0);
        if (k <= 0) { if (k < 0 && errno == EINTR) continue; return -1; }
        p += k, n -= (size_t)k;
    }
    return 0;
}

/* one request on the current control connection: the answer's text in
 * *reply (malloc), 0; -EPIPE when the connection failed */
static int exchange(gx_t *g, const char *msg, char **reply) {
    uint32_t len = htonl((uint32_t)strlen(msg)), rlen;
    if (g->fd < 0 || send_all(g->fd, &len, 4) || send_all(g->fd, msg, strlen(msg)) || recv_all(g->fd, &rlen, 4))
        return -EPIPE;
    rlen = ntohl(rlen);
    if (rlen > (1u << 20)) return -EPIPE;
    char *r = malloc(rlen + 1);
    if (!r) return -ENOMEM;
    if (recv_all(g->fd, r, rlen)) { free(r); return -EPIPE; }
    r[rlen] = 0;
    *reply = r;
    g->requests++;
    return 0;
}

static int tcp_connect(const char *host, int port, char *local_ip, size_t cap) {
    char ps[16];
    snprintf(ps, sizeof(ps), "%d", port);
    struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM }, *res;
    if (getaddrinfo(host, ps, &hints, &res)) return -1;
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { freeaddrinfo(res); return -1; }
    struct timeval tv = { .tv_sec = GX_TIMEOUT_MS / 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    int rc = connect(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    struct sockaddr_in me;
    socklen_t ml = sizeof(me);
    if (rc || getsockname(fd, (struct sockaddr *)&me, &ml) || !inet_ntop(AF_INET, &me.sin_addr, local_ip, (socklen_t)cap)) {
        close(fd);
        return -1;
    }
    return fd;
}

/* connect through endpoint e: control connection, queue pair, region */
static int connect_endpoint(gx_t *g, int e) {
    char local_ip[INET_ADDRSTRLEN];
    disconnect(g);
    g->fd = tcp_connect(g->host[e], g->port[e], local_ip, sizeof(local_ip));
    if (g->fd < 0) return -EHOSTUNREACH;
    roce_raw_attr_t me;
    g->qp = roce_raw_qp_create(local_ip, &me);
    if (!g->qp) { disconnect(g); return -EIO; }
    uint64_t mac = 0;
    for (int i = 0; i < 6; i++) mac = mac << 8 | me.mac[i];
    char tenant[160], msg[512], *reply = NULL;
    jquote(tenant, sizeof(tenant), g->tenant);
    snprintf(msg, sizeof(msg), "{\"op\": \"connect\", \"tenant\": %s, \"qpn\": %u, \"psn\": %u, \"mac\": %llu, "
             "\"ip\": %u, \"sport\": %u}", tenant, me.qpn, me.psn, (unsigned long long)mac, me.ip, me.sport);
    int rc = exchange(g, msg, &reply);
    if (rc) { disconnect(g); return rc; }
    jfield_t f[J_FIELDS];
    int n = jparse(reply, f, J_FIELDS);
    uint64_t qpn, ip, rkey, va, length, pmtu = 1024;
    if (n < 0 || !jok(f, n)) { rc = n < 0 ? -EIO : jerrno(f, n); free(reply); disconnect(g); return rc; }
    if (jint(f, n, "qpn", &qpn) || jint(f, n, "ip", &ip) || jint(f, n, "rkey", &rkey) || jint(f, n, "va", &va) ||
        jint(f, n, "length", &length)) {
        free(reply); disconnect(g); return -EIO;
    }
    jint(f, n, "pmtu", &pmtu);   /* the target's path MTU; 1024 when the server does not say */
    free(reply);
    roce_raw_attr_t peer = { .qpn = (uint32_t)qpn, .psn = 0, .ip = (uint32_t)ip };
    if (roce_raw_qp_connect(g->qp, &peer, 1, (uint32_t)pmtu)) { disconnect(g); return -EIO; }
    g->rkey = (uint32_t)rkey;
    g->region_va = va;
    g->region_len = length;
    g->pmtu = (uint32_t)pmtu;
    g->cur = e;
    return 0;
}

/* a working path: the current one, else the endpoints in turn from `from` */
static int ensure(gx_t *g, int from) {
    if (g->cur >= 0) return 0;
    int rc = -EHOSTUNREACH;
    for (int k = 0; k < g->n; k++)
        if (!(rc = connect_endpoint(g, (from + k) % g->n))) return 0;
    return rc;
}

/* the current path failed: the next endpoint */
static int failover(gx_t *g) {
    int next = g->cur >= 0 ? (g->cur + 1) % g->n : 0;
    disconnect(g);
    g->failovers++;
    return ensure(g, next);
}

/* a control request, sent again on the next path if the connection fails
 * (*resent then: a commit or free that the first path did carry finds
 * nothing to do the second time); the parsed answer in f -- its fields
 * point into *reply, which the caller frees */
static int request_ex(gx_t *g, const char *msg, char **reply, jfield_t *f, int *n, int *resent) {
    int rc = ensure(g, 0);
    *reply = NULL;
    if (resent) *resent = 0;
    for (int tries = 0; !rc; tries++) {
        rc = exchange(g, msg, reply);
        if (rc != -EPIPE || tries >= g->n) break;
        if ((rc = failover(g))) break;
        if (resent) *resent = 1;
    }
    if (rc) return rc;
    *n = jparse(*reply, f, J_FIELDS);
    if (*n < 0) { free(*reply); *reply = NULL; return -EIO; }
    if (!jok(f, *n)) { rc = jerrno(f, *n); free(*reply); *reply = NULL; return rc; }
    return 0;
}

static int request(gx_t *g, const char *msg, char **reply, jfield_t *f, int *n) {
    return request_ex(g, msg, reply, f, n, NULL);
}

/* ------------------------------------------------------------ slots */
static int new_slot(gx_t *g, uint32_t *slot) {
    for (uint32_t k = 0; k < GX_SLOTS - 1; k++) {
        uint32_t s = 1 + (g->hint - 1 + k) % (GX_SLOTS - 1);
        if (!g->slot[s].used) { g->hint = s + 1 >= GX_SLOTS ? 1 : s + 1; *slot = s; return 0; }
    }
    return -ENOSPC;
}

static gx_slot_t *slot_of(gx_t *g, uint32_t slot) {
    return slot && slot < GX_SLOTS && g->slot[slot].used ? &g->slot[slot] : NULL;
}

static void key_of(gx_t *g, const char *name, char *key) {
    if (name && *name) { snprintf(key, GX_KEY, "%s", name); return; }
    char host[64] = "";
    gethostname(host, sizeof(host) - 1);
    snprintf(key, GX_KEY, "anon-%s-%d-%llu", host, (int)getpid(), (unsigned long long)++g->anon);
}

static int do_alloc(gx_t *g, const char *key, uint64_t size, uint64_t *va, uint64_t *version) {
    char k[GX_KEY * 2 + 8], t[160], msg[512], *reply = NULL;
    jquote(k, sizeof(k), key);
    jquote(t, sizeof(t), g->tenant);
    snprintf(msg, sizeof(msg), "{\"op\": \"alloc\", \"tenant\": %s, \"key\": %s, \"length\": %llu}", t, k,
             (unsigned long long)((size + 3) & ~3ull));
    jfield_t f[J_FIELDS];
    int n, rc = request(g, msg, &reply, f, &n);
    if (rc) return rc;
    rc = jint(f, n, "va", va) || jint(f, n, "version", version) ? -EIO : 0;
    free(reply);
    return rc;
}

static int do_commit(gx_t *g, const char *key, uint64_t version) {
    char k[GX_KEY * 2 + 8], t[160], msg[512], *reply = NULL;
    jquote(k, sizeof(k), key);
    jquote(t, sizeof(t), g->tenant);
    snprintf(msg, sizeof(msg), "{\"op\": \"commit\", \"tenant\": %s, \"key\": %s, \"version\": %llu}", t, k,
             (unsigned long long)version);
    jfield_t f[J_FIELDS];
    int n, resent, rc = request_ex(g, msg, &reply, f, &n, &resent);
    free(reply);
    return rc == -ENOENT && resent ? 0 : rc;
}

static int do_lookup(gx_t *g, const char *key, uint64_t *va, uint64_t *size, uint64_t *version, double *lease) {
    char k[GX_KEY * 2 + 8], t[160], msg[512], *reply = NULL;
    jquote(k, sizeof(k), key);
    jquote(t, sizeof(t), g->tenant);
    snprintf(msg, sizeof(msg), "{\"op\": \"lookup\", \"tenant\": %s, \"key\": %s}", t, k);
    jfield_t f[J_FIELDS];
    int n, rc = request(g, msg, &reply, f, &n);
    if (rc) return rc;
    rc = jint(f, n, "va", va) || jint(f, n, "length", size) || jint(f, n, "version", version) ? -EIO : 0;
    *lease = jnum(f, n, "lease", 0.0);
    free(reply);
    return rc;
}

int gx_alloc(gx_t *g, const char *name, uint64_t size, int exclusive, uint32_t *slot) {
    if (!size) return -ERANGE;
    char key[GX_KEY];
    key_of(g, name, key);
    pthread_mutex_lock(&g->lk);
    uint64_t va, version, have;
    double lease;
    int rc = name && *name ? do_lookup(g, key, &va, &have, &version, &lease) : -ENOENT;
    if (!rc) {
        if (exclusive) rc = -EEXIST;
        else if (!(rc = new_slot(g, slot)))
            g->slot[*slot] = (gx_slot_t){ .used = 1, .va = va, .size = have, .version = version,
                                          .lease_until = mono() + lease };
        if (!rc) snprintf(g->slot[*slot].key, GX_KEY, "%s", key);
    } else if (rc == -ENOENT && !(rc = new_slot(g, slot)) && !(rc = do_alloc(g, key, size, &va, &version)) &&
               !(rc = do_commit(g, key, version))) {
        g->slot[*slot] = (gx_slot_t){ .used = 1, .owner = 1, .va = va, .size = size, .version = version };
        snprintf(g->slot[*slot].key, GX_KEY, "%s", key);
    }
    pthread_mutex_unlock(&g->lk);
    return rc;
}

int gx_stage(gx_t *g, const char *name, uint64_t size, uint32_t *slot) {
    if (!size) return -ERANGE;
    if (!name || !*name) return -EINVAL;
    pthread_mutex_lock(&g->lk);
    uint64_t va, version;
    int rc = new_slot(g, slot);
    if (!rc && !(rc = do_alloc(g, name, size, &va, &version))) {
        g->slot[*slot] = (gx_slot_t){ .used = 1, .owner = 1, .pending = 1, .va = va, .size = size, .version = version };
        snprintf(g->slot[*slot].key, GX_KEY, "%s", name);
    }
    pthread_mutex_unlock(&g->lk);
    return rc;
}

int gx_commit(gx_t *g, uint32_t slot) {
    pthread_mutex_lock(&g->lk);
    gx_slot_t *s = slot_of(g, slot);
    int rc = !s ? -ENOENT : !s->pending ? -EINVAL : do_commit(g, s->key, s->version);
    if (!rc) s->pending = 0;
    pthread_mutex_unlock(&g->lk);
    return rc;
}

int gx_lookup(gx_t *g, const char *name, uint32_t *slot, uint64_t *size) {
    if (!name || !*name) return -EINVAL;
    pthread_mutex_lock(&g->lk);
    uint64_t va, have, version;
    double lease;
    int rc = do_lookup(g, name, &va, &have, &version, &lease);
    if (!rc) {
        /* the slot of this value already, if there is one */
        uint32_t s = 0;
        for (uint32_t k = 1; k < GX_SLOTS && !s; k++)
            if (g->slot[k].used && g->slot[k].version == version && !strcmp(g->slot[k].key, name)) s = k;
        if (s) {
            if (!g->slot[s].owner) g->slot[s].lease_until = mono() + lease;
        } else if (!(rc = new_slot(g, &s))) {
            g->slot[s] = (gx_slot_t){ .used = 1, .va = va, .size = have, .version = version, .lease_until = mono() + lease };
            snprintf(g->slot[s].key, GX_KEY, "%s", name);
        }
        if (!rc) { *slot = s; if (size) *size = g->slot[s].size; }
    }
    pthread_mutex_unlock(&g->lk);
    return rc;
}

int gx_free(gx_t *g, uint32_t slot) {
    pthread_mutex_lock(&g->lk);
    gx_slot_t *s = slot_of(g, slot);
    int rc = s ? 0 : -ENOENT;
    if (s && !s->pending) {
        char k[GX_KEY * 2 + 8], t[160], msg[512], *reply = NULL;
        jquote(k, sizeof(k), s->key);
        jquote(t, sizeof(t), g->tenant);
        snprintf(msg, sizeof(msg), "{\"op\": \"free\", \"tenant\": %s, \"key\": %s}", t, k);
        jfield_t f[J_FIELDS];
        int n, resent;
        rc = request_ex(g, msg, &reply, f, &n, &resent);
        free(reply);
        if (rc == -ENOENT && resent) rc = 0;
    }
    /* a staged value never committed: only forgotten here (the server
     * gives its space back when it recovers) */
    if (s && (!rc || rc == -ENOENT)) memset(s, 0, sizeof(*s));
    pthread_mutex_unlock(&g->lk);
    return rc;
}

int gx_size(gx_t *g, uint32_t slot, uint64_t *size) {
    pthread_mutex_lock(&g->lk);
    gx_slot_t *s = slot_of(g, slot);
    if (s && size) *size = s->size;
    pthread_mutex_unlock(&g->lk);
    return s ? 0 : -ENOENT;
}

/* a looked-up slot past its lease: still the key's value? (lock held) */
static int still_valid(gx_t *g, gx_slot_t *s) {
    if (s->owner || mono() < s->lease_until) return 0;
    uint64_t va, size, version;
    double lease;
    int rc = do_lookup(g, s->key, &va, &size, &version, &lease);
    if (!rc && version == s->version) { s->lease_until = mono() + lease; return 0; }
    if (rc == -ENOENT || !rc) { memset(s, 0, sizeof(*s)); return -ENOENT; }
    return rc;
}

/* RDMA on the current path, up to 16 operations; another path on failure */
static int transfer(gx_t *g, int write, const roce_read_t *ops, int n) {
    int rc = ensure(g, 0);
    for (int tries = 0; !rc && tries <= g->n; tries++) {
        roce_read_t o[16];
        for (int i = 0; i < n; i++) { o[i] = ops[i]; o[i].rkey = g->rkey; }
        if (!roce_raw_qp_rdma(g->qp, write, o, n, GX_TIMEOUT_MS)) return 0;
        rc = failover(g);
    }
    return rc ? rc : -EIO;
}

static int bounce(gx_t *g) {
    if (g->bounce) return 0;
    g->bounce = aligned_alloc(4096, GX_BOUNCE);
    g->bounce_mem = g->bounce ? roce_mem_register(g->bounce, GX_BOUNCE) : NULL;
    if (!g->bounce_mem) { free(g->bounce); g->bounce = NULL; return -ENOMEM; }
    return 0;
}

static int move(gx_t *g, int write, uint32_t slot, uint64_t off, struct rpga_mem *m, size_t moff, size_t len,
                int *direct) {
    if (off % 4 || len % 4) return -EINVAL;   /* the fabric moves 4-byte words */
    pthread_mutex_lock(&g->lk);
    gx_slot_t *s = slot_of(g, slot);
    int rc = s ? still_valid(g, s) : -ENOENT;
    if (!rc && (off > s->size || len > s->size - off)) rc = -ERANGE;
    uint64_t raddr = s && !rc ? s->va + off : 0;
    int dir = m->mem && !atomic_load(&m->broken);
    if (!rc && dir) {
        roce_read_t ops[16];
        int n = 0;
        for (size_t done = 0; done < len && !rc;) {
            uint32_t k = (uint32_t)(len - done < GX_ONE_OP ? len - done : GX_ONE_OP);
            ops[n++] = (roce_read_t){ raddr + done, 0, k, m->mem, m->base + moff + done };
            done += k;
            if (n == 16 || done == len) { rc = transfer(g, write, ops, n); n = 0; }
        }
    } else if (!rc && !(rc = bounce(g))) {
        for (size_t done = 0; done < len && !rc;) {
            size_t k = len - done < GX_BOUNCE ? len - done : GX_BOUNCE;
            roce_read_t op = { raddr + done, 0, (uint32_t)k, g->bounce_mem, g->bounce };
            if (write) {
                rc = m->rt->ops.from_dev(g->bounce, m->base + moff + done, k, m->rt->ops.ctx);
                if (!rc) rc = transfer(g, 1, &op, 1);
            } else if (!(rc = transfer(g, 0, &op, 1))) {
                rc = m->rt->ops.to_dev(m->base + moff + done, g->bounce, k, m->rt->ops.ctx);
            }
            done += k;
        }
    }
    pthread_mutex_unlock(&g->lk);
    if (direct) *direct = dir;
    return rc;
}

int gx_read(gx_t *g, uint32_t slot, uint64_t off, struct rpga_mem *m, size_t moff, size_t len, int *direct) {
    return move(g, 0, slot, off, m, moff, len, direct);
}

int gx_write(gx_t *g, uint32_t slot, uint64_t off, struct rpga_mem *m, size_t moff, size_t len, int *direct) {
    return move(g, 1, slot, off, m, moff, len, direct);
}

void gx_path(gx_t *g, int *endpoint, uint64_t *failovers, uint64_t *requests) {
    pthread_mutex_lock(&g->lk);
    if (endpoint) *endpoint = g->cur;
    if (failovers) *failovers = g->failovers;
    if (requests) *requests = g->requests;
    pthread_mutex_unlock(&g->lk);
}
