# Log checkpoints and asynchronous replication

Two additions for keeping a large store (tens of millions of files) on
rocepool. Both work off the request path: a client's request never waits
for a checkpoint or for a standby. Both are off unless configured; with
them off the servers behave as before. IO500 runs leave them off.

## Log checkpoints

**Problem.** With `ROCEPOOL_MDT_LOG` / `ROCEPOOL_OST_LOG` every change is
in a write-ahead log that is replayed on restart. The log only grew, so
both its size and the restart time grew with the store's whole history.

**What a checkpoint does.** A background thread in each server (MDT or
OST), once the current log segment reaches `ROCEPOOL_LOG_CHECKPOINT_MIB`:

1. **Starts a new segment.** It switches the log to a new file
   (`wal_rotate`): `<log>`, then `<log>.1`, `<log>.2`, and so on.
   - Every record appended before the switch stays in the old file.
   - Appends never wait for the switch.
   - A commit of a later record waits for at most one commit's time.
   - LSNs carry on across files: each file after the first starts with a
     header record holding its base LSN.
2. **Writes the whole store to `<log>.ckpt.tmp`** while the server keeps
   serving.
   - Entries are copied under their own locks, 2048 hash slots at a time,
     so no lock is held for long.
   - Writes are capped at `ROCEPOOL_LOG_CHECKPOINT_MBPS` (default 256 MiB/s).
3. **Waits until the log is durable up to its last record**, so every change
   the copy may contain is also in the log.
4. **Syncs the checkpoint, renames it to `<log>.ckpt` and deletes the
   segments before the new one.**

**Restart.** The server loads the checkpoint, then replays the segments
from the checkpoint's own segment onward.

**Why a copy made while serving is still correct.** The copy does not
capture one point in time.
- An OST record is an object's whole state, so the last record of a key
  wins.
- An MDT entry carries the LSN of its last change. A replayed record at or
  below that LSN is already in the copy, so it is skipped (`ckpt_skipped`
  in STATS).
- The MDT's asynchronous-commit bookkeeping survives a checkpoint. That
  covers the lives and the transno within the current life, which are kept
  in the checkpoint header, so clients' replays after a crash are still
  matched correctly.

**Crashes at any point.**

| Crash during | What happens on restart |
|---|---|
| The switch to a new segment | The old checkpoint and every segment are still there |
| Writing the checkpoint | The `.tmp` is deleted; the old checkpoint and the segments are used |
| After the rename, before the old segments are deleted | Segments older than the checkpoint are deleted |

**STATS** has a `checkpoint` section: `segment`, `segment_bytes`,
`checkpoints`, `last_s`, `last_entries`, `last_bytes`, `segments_deleted`,
`failures`.

| Setting | Default | Meaning |
|---|---|---|
| `ROCEPOOL_LOG_CHECKPOINT_MIB` | 0 (off) | Segment size that starts a checkpoint |
| `ROCEPOOL_LOG_CHECKPOINT_MBPS` | 256 | Cap on the checkpoint's writes |
| `ROCEPOOL_LOG_CHECKPOINT_POLL_MS` | 1000 | How often the size is checked |

## Asynchronous replication

Each MDT and OST can have a standby: another server process, usually on
another host, with its own log, tier and checkpoints.

| Setting | Meaning |
|---|---|
| `ROCEPOOL_MDT_REPLICAS`, `ROCEPOOL_OST_REPLICAS` | Comma lists; the n-th server of a kind in a process pairs with the n-th address |
| `ROCEPOOL_REPL_ROLE` | `primary` (default) or `standby`, for the first start |
| `ROCEPOOL_REPL_PROMOTE_S` | Default 2; 0 means never promote on its own |
| `ROCEPOOL_REPL_HEARTBEAT_MS` | Hello interval, default 200 |
| `ROCEPOOL_REPL_REPLY_CACHE` | Replies kept for request dedupe, default 65536 |
| `ROCEPOOL_REPL_MAX_QUEUE` | Keys queued at most (default 1 M) |
| `ROCEPOOL_MDTS` / `ROCEPOOL_OSTS` (client) | Each entry `primary|standby` |
| `ROCEPOOL_FAILOVER_S` (client) | How long a request keeps trying the two (default 60) |
| `ROCEPOOL_PAIR_CONNECT_MS` (client) | Connect timeout towards a pair's server, default 1000 |

Both sides are configured with each other's address, so either can be the
primary. Replication needs the server's log (`ROCEPOOL_*_LOG`).

**How changes travel.**

1. **A change marks its key dirty.** Each shard has a hash set; the change
   holds the shard's lock for a few hundred nanoseconds. Several changes to
   a key before it ships go as one, and an OST object's changed byte range
   is merged.
2. **Shipper threads send what is marked.** There is one shipper per shard.
   It takes keys in batches (up to 512 keys or 4 MiB) and reads each key's
   state as it is now:
   - an MDT entry with its inline bytes;
   - an OST object's changed range, 1 MiB at a time, read from the tier
     without holding the object's lock.
3. **The standby applies each batch** through its own durable path (its log
   and tier) before it replies.
4. **A failed send puts the keys back** in the queue.

**Resync.** A heartbeat thread says hello every 200 ms on a connection of
its own, so a long resync step never delays it. When the
standby's copy is not complete for this primary's run, it resyncs. That
happens with a new standby, after either side restarts, or after a queue
overflow while the standby was away. It goes in three steps:
1. Every key and its version is compared with the standby in batches.
2. Whatever differs is sent.
3. What the standby has and the primary does not is deleted there. Entries
   are marked unconfirmed at the start and swept at the end.

When only the standby was away, the queue kept everything and simply
drains: no resync is needed.

**Roles and failover.**
- **A standby refuses clients.** It answers `STANDBY` to everything but its
  primary. On `STANDBY`, or when its server cannot be reached, the client
  (`rocepool_client.c`) switches to the other address of the pair and
  retries.
  - A switch drops the client's pooled connections.
  - The client also stops using the MDT's lookup table and the one-sided
    read leases it had from the other server.
- **A standby takes over by itself** when it holds a complete copy and has
  not heard from its primary for `ROCEPOOL_REPL_PROMOTE_S`. It becomes
  primary with term + 1.
  - A standby still handling a request from its primary, such as a long
    resync step, is never "silent".
  - Measured locally: the first read answered 1.8 s after the primaries
    were killed.

**Request dedupe across a takeover (as NonStop process pairs do with
their sync IDs).**
- A client of a server pair tags every request with its process id and a
  sequence number. The tag stays the same on every retry.
- The primary keeps its replies to changes by tag, and ships each reply in
  the same batch as the change's state.
- A request sent again, to the same server or to the standby that took
  over, gets its first reply instead of being done twice. Without this, an
  exclusive create would answer `EXISTS` and a CAS would answer a conflict.
- Within an OST `BATCH`, the reply travels with the batch's last change,
  so a partial batch lost in a failover is answered as done.
- **A primary that hears from a primary of a higher term steps down** and is
  resynced by it. A returning old primary therefore becomes the standby.
- **Role, term and the copy's completeness are kept in `<log>.repl`**, so a
  restart does not bring a stale primary back.

**Versions across failovers.**
- An OST object's version never goes below `term << 40` within a term.
- An MDT entry ships its last change's LSN with the term on top.

A change one primary made and never shipped therefore cannot be mistaken
for another primary's change during a resync.

**What "asynchronous" means.** On a failover, the changes still queued
when the primary died are lost. That is the `queued` count in STATS,
usually milliseconds' worth.
- Within a key, changes arrive in order.
- Across keys, they may arrive in a different order.
- A network partition can let both sides act as primary until they hear
  from each other again. Then the higher term wins, and the other side's
  unshipped changes are dropped by the resync. Two servers cannot decide
  this without a third party. Set `ROCEPOOL_REPL_PROMOTE_S=0` and promote
  by hand (`RS_PROMOTE`) where that matters more than automatic takeover.

**STATS** has a `repl` section: `role`, `term`, `peer_up`, `in_sync`,
`queued`, `items`, `bytes`, `resyncs`, `mismatched`, `swept`, `applied`,
`promotions`, `demotions`, `overflows`.

## Tests

| File | What it covers |
|---|---|
| `tests/test_wal.py` | Segment switches under load in every commit mode: no record lost, repeated or reordered across files |
| `tests/test_checkpoint.py` | The whole durability suite with a checkpoint every 16 KiB of log, in synchronous and asynchronous MDT mode, plus clients' replays after an MDT crash; servers killed at random moments during a load with checkpoints running (inside checkpoints too), with every file a client saw closed read back exactly; the log kept bounded |
| `tests/test_replication.py` | Failover with the client switching on its own; a standby back after an absence; a resync after the primaries restarted while the standbys were away (differences shipped, deletions swept); an old primary returning as standby, then the pair failing over the other way; a standby refusing clients |

`tools/durability_overhead_bench.py` measures what each mode costs the
clients, and the restart time with and without a checkpoint.
