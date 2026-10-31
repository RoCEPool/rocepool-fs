/* A write-ahead log with group commit, for the MDT's namespace and the
 * OST's object index (rocepool_servers.c).
 *
 * IO500 rule 6: "All data must be written to persistent storage within the
 * measured time for the individual benchmark"; a cache must be flushed to
 * persistent storage before a close is acknowledged. The servers keep their
 * state in RAM; with a log, every change that a reply acknowledges is in
 * the log (and the OST's object bytes on its tier) and fdatasync'd first.
 *
 * Group commit: a request appends its record (a memcpy under a mutex) and
 * then waits for the log to be durable up to it. The first waiter writes
 * and syncs everything appended so far -- the tier files registered with
 * wal_add_sync() first, then the log -- while later requests queue up
 * behind it; one fdatasync covers all of them.
 *
 * Record on disk: u32 LE payload length, u32 LE CRC-32 of the payload, the
 * payload. Replay stops at the first short or corrupt record (a write torn
 * by a crash: never acknowledged) and cuts the file there. */
#ifndef ROCEPOOL_WAL_H
#define ROCEPOOL_WAL_H
#include <stddef.h>
#include <stdint.h>

typedef struct wal wal_t;

/* open (create) the log for appending; NULL + err on failure */
wal_t *wal_open(const char *path, char *err, size_t err_len);
/* also fdatasync this descriptor in every commit, before the log (data the
 * log's records point at) */
int wal_add_sync(wal_t *w, int fd);

/* append one record from up to three parts; returns its LSN (the log's
 * length after it), 0 on failure (out of memory / the log failed) */
uint64_t wal_append(wal_t *w, const void *a, size_t alen, const void *b, size_t blen, const void *c, size_t clen);
/* block until everything up to lsn is on persistent storage; 0 or -errno
 * (a failed write or sync fails every later wait too: the log is stuck) */
int wal_wait(wal_t *w, uint64_t lsn);

/* without blocking: 1 once everything up to lsn is durable, 0 not yet,
 * -errno when the log failed (it never will be) */
int wal_ready(wal_t *w, uint64_t lsn);
/* have the log's flusher thread commit what is appended (started on the
 * first kick): for a request whose reply waits in the transport until
 * wal_ready, instead of its handler thread blocking in wal_wait -- the
 * thread takes the next request meanwhile, and one commit covers the
 * records of every request it took */
void wal_kick(wal_t *w);

/* every record of the log at path, in order; cb returns nonzero to stop.
 * Returns the number of records, or -errno; a torn tail is cut off. */
long wal_replay(const char *path, int (*cb)(void *user, const uint8_t *rec, size_t len), void *user);

/* counters for STATS: records, commits (fdatasync rounds), bytes, sync time */
void wal_stats(wal_t *w, uint64_t *records, uint64_t *commits, uint64_t *bytes, uint64_t *sync_ns);

/* "{n}" in spec replaced by n (one log per server in a process) */
void wal_path(const char *spec, int n, char *out, size_t out_len);

#endif
