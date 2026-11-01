/* A write-ahead log with group commit, for the MDT's namespace and the
 * OST's object index (rocepool_servers.c).
 *
 * IO500 rule 6: "All data must be written to persistent storage within the
 * measured time for the individual benchmark"; a cache must be flushed to
 * persistent storage before a close is acknowledged. The servers keep their
 * state in RAM; with a log, every change that a reply acknowledges is in
 * the log (and the OST's object bytes on its tier) and fdatasync'd first.
 *
 * Group commit, optionally pipelined: a request appends its record (a
 * memcpy under a mutex) and waits for -- or has its transport wait for --
 * the log to be durable up to it. Flusher threads each take what is
 * appended (whole records, up to 256 KiB), sync the tier files registered
 * with wal_add_sync() and write it; with a pipeline (ROCEPOOL_LOG_PIPELINE)
 * several commits are in flight at once, and the log is durable up to the
 * end of the longest prefix of finished ones. On 6 x i3en one commit at a
 * time of ~150 us each bounded every MDT to ~73 k changes/s.
 *
 * A commit is either an append + fdatasync, or a direct, synchronous write
 * (starting on a 4 KiB block; a pad record fills the last one) into zeros
 * written beforehand (ROCEPOOL_LOG_PREFILL_MIB): no file system metadata
 * changes, so no journal commit behind it. wal_open runs each way for a
 * moment on the log's drive (ROCEPOOL_LOG_CALIBRATE_MS) and takes the
 * fastest (ROCEPOOL_LOG_DIRECT / ROCEPOOL_LOG_PIPELINE override).
 *
 * Record on disk: u32 LE payload length (high bit: a pad record), u32 LE
 * CRC-32 of the payload, the payload; zeros end the log. Replay stops at
 * the first zero, short or corrupt record and clears what follows up to a
 * long run of zeros: commits in flight at a crash may have reached the
 * drive out of order, and those after the first missing one (never
 * acknowledged) must not reappear behind new records. */
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

/* append one record from up to three parts; returns its LSN (its sequence
 * number in this log's life: 1, 2, ...), 0 on failure (out of memory, the
 * log failed, a record over 2 MiB) */
uint64_t wal_append(wal_t *w, const void *a, size_t alen, const void *b, size_t blen, const void *c, size_t clen);
/* block until everything up to lsn is on persistent storage; 0 or -errno
 * (a failed write or sync fails every later wait too: the log is stuck) */
int wal_wait(wal_t *w, uint64_t lsn);

/* the LSN of the last record appended (0: none yet in this life) */
uint64_t wal_last(wal_t *w);
/* records 1..wal_durable() are on the drive */
uint64_t wal_durable(wal_t *w);

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

/* the pipeline: direct writes into pre-written zeros (1) or appends +
 * fdatasync (0), flusher threads, most commits ever in flight, bytes of
 * zeros written ahead */
void wal_stats_more(wal_t *w, int *direct, int *pipeline, uint64_t *inflight_max, uint64_t *prefill_bytes);

/* what wal_open measured on the log's drive, records/s committed by
 * appends, by direct writes, by direct writes 4 at once (0: not run) */
void wal_calibration(wal_t *w, double *append, double *direct, double *direct4);

/* stops the log's threads and frees it (everything appended and waited
 * for is durable already) */
void wal_close(wal_t *w);

/* "{n}" in spec replaced by n (one log per server in a process) */
void wal_path(const char *spec, int n, char *out, size_t out_len);

#endif
