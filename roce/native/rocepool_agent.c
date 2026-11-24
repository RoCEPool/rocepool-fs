/* rocepool_agent.c -- one per client node: the node's small-file writes to
 * the MDTs, several processes' at a time, in one batch (MDT op 16).
 *
 * mdtest-hard's write phase is many ranks per node, each creating files one
 * after another: open, write 3901 bytes, close. With create_write (op 15)
 * that is one MDT request per file, and each rank waits for its own. The
 * agent takes those requests from the node's ranks over a UNIX socket and
 * sends whatever is waiting for an MDT as one request: what queued while
 * the previous batch was out is the next batch (no timer, so a lone rank
 * pays only the extra local hop). Per file nothing changes: each request is
 * handled, locked and logged at the MDT as if it came alone, and its rank
 * hears back only once the batch is durable (IO500 rule 6).
 *
 * The MDT then sees one request (and one reply, one completion, one
 * transport wakeup) per batch instead of per file; on EFA that per-request
 * cost was most of the MDT's CPU per file (SC26: transport 151 us, handler
 * 31 us). And only the agents keep connections to the MDTs.
 *
 * Socket: SOCK_SEQPACKET at ROCEPOOL_AGENT ("@name": Linux's abstract
 * namespace, one per network namespace -- per host, and per simulated node in
 * tools/netns_cluster.py). Request: u8 addr_len, MDT
 * address, u32 LE key_len, key, MDT request value (op 15 or 7). Reply: u8
 * status (0 OK: the entry follows; 1 the MDT's error: "CODE\0message"; 2 the
 * agent could not get an answer: the client sends the request itself --
 * create_write and write_inline at one offset are idempotent).
 *
 * Env: ROCEPOOL_AGENT (socket path), ROCEPOOL_AGENT_BATCH (max requests per
 * batch, default 64), ROCEPOOL_AGENT_INFLIGHT (batches in flight per MDT,
 * default 1), ROCEPOOL_AGENT_STATS (a file the counters are written to every
 * second and at exit).
 */
#define _GNU_SOURCE
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "rocepool_client.h"

#define MAX_MSG (64u << 10)  /* an inline write is at most 32 KiB */
#define MAX_MDTS 64
#define BATCH_BUF (4u << 20)  /* a batch's request: at most this many bytes */

typedef struct item {
    struct item *next;
    const char *key;
    uint32_t key_len;
    const uint8_t *val;
    uint32_t val_len;
    /* the answer */
    int done;
    uint8_t status;
    uint8_t *ans;
    uint32_t ans_len;
    pthread_cond_t cv;
} item_t;

typedef struct {
    rp_peer_t peer;
    pthread_mutex_t lk;
    pthread_cond_t cv;
    item_t *head, *tail;
    int queued, sending;
} mdt_q_t;

static mdt_q_t g_q[MAX_MDTS];
static int g_nq;
static pthread_mutex_t g_qlk = PTHREAD_MUTEX_INITIALIZER;
static int g_batch_max = 64, g_inflight = 1;
static atomic_ullong g_reqs, g_batches, g_failed, g_max_batch, g_clients;
static const char *g_sock, *g_stats;

static void wr32(uint8_t *p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (8 * i)); }
static uint32_t rd32(const uint8_t *p) {
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

static mdt_q_t *queue_for(const char *addr, size_t n) {
    pthread_mutex_lock(&g_qlk);
    for (int i = 0; i < g_nq; i++)
        if (strlen(g_q[i].peer.addr) == n && !memcmp(g_q[i].peer.addr, addr, n)) {
            pthread_mutex_unlock(&g_qlk);
            return &g_q[i];
        }
    mdt_q_t *q = NULL;
    if (g_nq < MAX_MDTS && n < sizeof(g_q[0].peer.addr)) {
        q = &g_q[g_nq];
        char a[128];
        memcpy(a, addr, n);
        a[n] = 0;
        rp_peer_init(&q->peer, a);
        pthread_mutex_init(&q->lk, NULL);
        pthread_cond_init(&q->cv, NULL);
        g_nq++;
    }
    pthread_mutex_unlock(&g_qlk);
    return q;
}

static void answer(item_t *it, uint8_t status, const void *b, size_t n) {
    it->status = status;
    it->ans = n ? malloc(n) : NULL;
    if (n && !it->ans) { it->status = 2; n = 0; }
    if (n) memcpy(it->ans, b, n);
    it->ans_len = (uint32_t)n;
}

/* Flat combining: a client's thread that finds fewer than g_inflight
 * batches out for its MDT takes what is queued (its own request and the
 * ones that came while the last batch was out) and sends it; the others
 * wait to be answered, or to lead the next batch. No thread in between: a
 * lone request costs the agent one wakeup-free pass. */
static void send_batch(mdt_q_t *q, item_t **its, int n, uint8_t *buf) {
    buf[0] = 16;
    wr32(buf + 1, (uint32_t)n);
    size_t p = 5;
    for (int i = 0; i < n; i++) {
        wr32(buf + p, its[i]->key_len);
        wr32(buf + p + 4, its[i]->val_len);
        memcpy(buf + p + 8, its[i]->key, its[i]->key_len);
        memcpy(buf + p + 8 + its[i]->key_len, its[i]->val, its[i]->val_len);
        p += 8 + its[i]->key_len + its[i]->val_len;
    }
    rp_resp_t r;
    int rc = rp_rpc(&q->peer, RP_MDT, "", buf, p, NULL, 0, &r);
    atomic_fetch_add(&g_batches, 1);
    unsigned long long mx = atomic_load(&g_max_batch);
    while ((unsigned long long)n > mx && !atomic_compare_exchange_weak(&g_max_batch, &mx, (unsigned long long)n)) {}
    const uint8_t *v = rc == 0 && r.op == RP_MDT_OK ? r.val : NULL;
    size_t vp = 0;
    for (int i = 0; i < n; i++) {
        item_t *it = its[i];
        if (v && vp + 5 <= r.vlen && vp + 5 + rd32(v + vp + 1) <= r.vlen) {
            uint32_t sl = rd32(v + vp + 1);
            answer(it, v[vp] == 0 ? 0 : 1, v + vp + 5, sl);
            vp += 5 + sl;
        } else {
            answer(it, 2, NULL, 0);  /* no answer for it: its client sends it itself */
            atomic_fetch_add(&g_failed, 1);
        }
    }
    if (rc == 0) rp_resp_free(&r);
}

/* called with q->lk held; returns with it held and *me answered */
static void wait_or_lead(mdt_q_t *q, item_t *me, uint8_t *buf, item_t **its) {
    while (!me->done) {
        if (!q->head || q->sending >= g_inflight) {
            pthread_cond_wait(&me->cv, &q->lk);
            continue;
        }
        int n = 0;
        size_t len = 5;
        while (q->head && n < g_batch_max && (!n || len + 8 + q->head->key_len + q->head->val_len <= BATCH_BUF)) {
            item_t *it = q->head;
            q->head = it->next;
            if (!q->head) q->tail = NULL;
            q->queued--;
            its[n++] = it;
            len += 8 + it->key_len + it->val_len;
        }
        q->sending++;
        pthread_mutex_unlock(&q->lk);
        send_batch(q, its, n, buf);
        pthread_mutex_lock(&q->lk);
        q->sending--;
        for (int i = 0; i < n; i++) {
            its[i]->done = 1;
            if (its[i] != me) pthread_cond_signal(&its[i]->cv);
        }
        if (q->head) pthread_cond_signal(&q->head->cv);  /* the next batch's leader */
    }
}

/* one client process's (thread's) connection */
static void *conn_main(void *arg) {
    int fd = (int)(intptr_t)arg;
    pthread_setname_np(pthread_self(), "rp-agent-rx");
    atomic_fetch_add(&g_clients, 1);
    uint8_t *msg = malloc(MAX_MSG), *buf = malloc(BATCH_BUF);  /* pages touched only when it leads */
    item_t **its = calloc((size_t)g_batch_max, sizeof(*its));
    if (!msg || !buf || !its) {
        free(msg); free(buf); free(its);
        close(fd);
        atomic_fetch_sub(&g_clients, 1);
        return NULL;
    }
    item_t it;
    pthread_cond_init(&it.cv, NULL);
    for (;;) {
        ssize_t n = recv(fd, msg, MAX_MSG, 0);
        if (n <= 0) break;
        uint8_t fail = 2;
        if (n < 1 || (size_t)n < 1u + msg[0] + 4u) { if (send(fd, &fail, 1, MSG_NOSIGNAL) < 0) break; continue; }
        size_t al = msg[0];
        uint32_t kl = rd32(msg + 1 + al);
        size_t kp = 1 + al + 4;
        if (kp + kl + 1 > (size_t)n) { if (send(fd, &fail, 1, MSG_NOSIGNAL) < 0) break; continue; }
        mdt_q_t *q = queue_for((const char *)msg + 1, al);
        if (!q) { if (send(fd, &fail, 1, MSG_NOSIGNAL) < 0) break; continue; }
        it.next = NULL;
        it.key = (const char *)msg + kp;
        it.key_len = kl;
        it.val = msg + kp + kl;
        it.val_len = (uint32_t)((size_t)n - kp - kl);
        it.done = 0;
        it.ans = NULL;
        atomic_fetch_add(&g_reqs, 1);
        pthread_mutex_lock(&q->lk);
        if (q->tail) q->tail->next = &it; else q->head = &it;
        q->tail = &it;
        q->queued++;
        wait_or_lead(q, &it, buf, its);
        pthread_mutex_unlock(&q->lk);
        struct iovec iov[2] = {{&it.status, 1}, {it.ans, it.ans_len}};
        struct msghdr mh = {.msg_iov = iov, .msg_iovlen = it.ans_len ? 2 : 1};
        ssize_t s = sendmsg(fd, &mh, MSG_NOSIGNAL);
        free(it.ans);
        if (s < 0) break;
    }
    atomic_fetch_sub(&g_clients, 1);
    close(fd);
    free(msg);
    free(buf);
    free(its);
    return NULL;
}

static void write_stats(void) {
    if (!g_stats) return;
    char tmp[4096];
    snprintf(tmp, sizeof(tmp), "%s.tmp", g_stats);
    FILE *f = fopen(tmp, "w");
    if (!f) return;
    unsigned long long r = atomic_load(&g_reqs), b = atomic_load(&g_batches);
    fprintf(f, "{\"requests\": %llu, \"batches\": %llu, \"avg_batch\": %.2f, \"max_batch\": %llu, "
               "\"failed\": %llu, \"clients\": %llu, \"mdts\": %d}\n",
            r, b, b ? (double)r / (double)b : 0.0, atomic_load(&g_max_batch), atomic_load(&g_failed),
            atomic_load(&g_clients), g_nq);
    fclose(f);
    rename(tmp, g_stats);
}

static void *stats_main(void *arg) {
    (void)arg;
    for (;;) {
        sleep(1);
        write_stats();
    }
    return NULL;
}

static void on_term(int sig) {
    (void)sig;
    write_stats();
    if (g_sock && g_sock[0] != '@') unlink(g_sock);
    _exit(0);
}

int main(void) {
    g_sock = getenv("ROCEPOOL_AGENT");
    if (!g_sock || !*g_sock) {
        fprintf(stderr, "rocepool-agent: set ROCEPOOL_AGENT to the socket path\n");
        return 2;
    }
    const char *e;
    if ((e = getenv("ROCEPOOL_AGENT_BATCH")) && atoi(e) > 0) g_batch_max = atoi(e) > 1024 ? 1024 : atoi(e);
    if ((e = getenv("ROCEPOOL_AGENT_INFLIGHT")) && atoi(e) > 0) g_inflight = atoi(e) > 16 ? 16 : atoi(e);
    g_stats = getenv("ROCEPOOL_AGENT_STATS");
    signal(SIGPIPE, SIG_IGN);
    signal(SIGTERM, on_term);
    signal(SIGINT, on_term);
    int ls = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    struct sockaddr_un sa = {.sun_family = AF_UNIX};
    if (ls < 0 || strlen(g_sock) >= sizeof(sa.sun_path)) {
        fprintf(stderr, "rocepool-agent: bad socket path %s\n", g_sock);
        return 2;
    }
    socklen_t sl = sizeof(sa);
    if (g_sock[0] == '@') {  /* abstract: no file */
        memcpy(sa.sun_path + 1, g_sock + 1, strlen(g_sock) - 1);
        sl = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + strlen(g_sock));
    } else {
        strcpy(sa.sun_path, g_sock);
        unlink(g_sock);
    }
    if (bind(ls, (struct sockaddr *)&sa, sl) || listen(ls, 1024)) {
        fprintf(stderr, "rocepool-agent: %s: %s\n", g_sock, strerror(errno));
        return 1;
    }
    pthread_t st;
    pthread_create(&st, NULL, stats_main, NULL);
    pthread_detach(st);
    fprintf(stderr, "rocepool-agent: listening on %s (batch %d, %d in flight per MDT)\n", g_sock, g_batch_max,
            g_inflight);
    for (;;) {
        int fd = accept4(ls, NULL, NULL, SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR) continue;
            usleep(1000);
            continue;
        }
        pthread_t t;
        pthread_attr_t at;
        pthread_attr_init(&at);
        pthread_attr_setstacksize(&at, 256 << 10);
        pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&t, &at, conn_main, (void *)(intptr_t)fd)) close(fd);
        pthread_attr_destroy(&at);
    }
}
