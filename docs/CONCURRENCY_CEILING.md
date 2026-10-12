# Concurrency ceiling: `tools/stress_concurrency.py`

Single-host stress test of the distributed design (2 MDT + 4 OST server
processes over the crossover-cable softverbs link, `tools/bench_ior.py
start_dist_servers`). N independent clients (each its own
`DistributedFileSystem`: own cache, batcher, connections), packed as worker
processes x threads split across both namespaces, released together, ladder
1 -> 200 and beyond. Attribute cache off, so every op reaches a server.

```bash
python3 tools/stress_concurrency.py                                  # md, md_shared, data; N = 1..200
python3 tools/stress_concurrency.py --ladder 200 --duration 60       # sustained 200 clients
python3 tools/stress_concurrency.py --core-dumps                     # a crashed server leaves a core
```

Sampled every 0.5 s: per server process threads / fds / RSS / CPU, per
namespace TIME_WAIT (also per destination) and ListenOverflows, host
memory and load, plus a per-second throughput timeline. Reports:
`stress_reports/`.

## What broke, in the order it broke (all fixed)

| # | Symptom | Evidence | Root cause | Fix |
|---|---|---|---|---|
| 1 | MDT processes die at **~50 clients**; 92-99 % errors after | returncode -11; core dump: `pthread_detach(th=128)` from `accept_loop` | use-after-free in `roce_transport.c`: the per-connection thread could serve its one request and `free(ct)` before the acceptor read `ct->thread` to detach it. Also present in the original design, never hit at low concurrency | create the thread detached via `pthread_attr`; on `pthread_create` failure drop that one connection instead of crashing; softverbs `thread_started` only set on success |
| 2 | at **200 clients** 32-39 % "unexpected CM event" | hot MDT pinned at 253-254 threads = ~127 connections x 2 threads | softverbs MR table was a fixed 256 entries (2 MRs per connection) and linearly scanned under a global lock on every post | O(1) MR table, slot encoded in the lkey (+ generation, stale keys still rejected), capacity 65 536; RC-semantics tests still pass |
| 3 | ListenOverflows at 200, p99 ~1.1 s | kernel `ListenOverflows` > 0 | listen backlog 64 | backlog 1024 (still capped by `somaxconn`) |
| 4 | sustained load: throughput collapses to **~38 %** in a ~60 s cycle, even at 50 clients | TIME_WAIT pinned at `tcp_max_tw_buckets` = 32 768 per namespace; timeline 1829 -> 690 -> 1620 -> 702 ops/s | connect-per-request over the emulator's TCP: every RPC parks one ephemeral port + one TIME_WAIT bucket for 60 s. RDMA RC QPs have no TIME_WAIT -- an emulator artefact, but it capped the whole design | softverbs: `SO_LINGER {1,0}` and `shutdown(SHUT_RD)` (not `SHUT_RDWR`, whose FIN let the peer close first and still put this side in TIME_WAIT). 90 s run: TIME_WAIT 0, flat ~1 900 ops/s |

Regression after all four: full unit suite (228) green, `run_crossover_rdma.py`
all 5 phases green, IOR/mdtest on both `pool` and `dist` all PASS with byte
verification. A/B of the fixed vs the original C library under
`bench_ior --backend dist` (2 runs each, alternated): within run-to-run noise.

## Round 2: 500 clients (fixes 5-6)

Same test (3 workloads x 60 s), N = 500:

| # | Symptom | Evidence | Root cause | Fix |
|---|---|---|---|---|
| 5 | data: 2 OSTs **OOM-killed** (rc -9, memcg OOM in dmesg), 71-76 % errors | `--mem-probe-at` (SIGUSR1 -> glibc `mallinfo2` + tracemalloc): per OST 22-48 MB stored, 30-56 MB live Python, but **535-2 440 MB freed-yet-retained** in malloc arenas | each connection malloc'd two `max_frame_bytes` (16 MiB) frame buffers; glibc's dynamic mmap threshold then moved them into per-thread arenas, where they stayed (touched pages resident) after the connection closed | `roce_transport.c`: frame buffers via `mmap`/`munmap`. Retained heap 535-2 440 MB -> 18-142 MB |
| 6 | still OOM at 500 after #5 | now `malloc_in_use` dominated, and 281-376 stripe objects stored per OST where ~125 were live | **no backpressure**: every connection accepted and its 1 MiB request received immediately, however far behind the handler was. Clients timed out and moved on while servers still executed their queued writes later (orphaned stripes), memory grew, more timeouts -- a feedback loop | admission gate in `roce_transport.c`: at most `ROCE_SERVER_MAX_CONNS` (default 64) open connections per server; excess clients wait at connection setup, before anything is sent. Refcounted gate so detached connection threads may outlive `roce_server_stop` |

500 clients x 60 s, before -> after fixes 5-6:

| | md | md_shared | data |
|---|---|---|---|
| before | 194 ops/s, 0.8 % err, p99 9.4 s | 188 ops/s, 0.7 % err, p99 8.6 s | 15 ops/s, **71 % err, 2 OSTs OOM-killed** |
| after | **1 137 ops/s, 0 err**, p99 0.95 s | **1 306 ops/s, 0 err**, p99 0.7 s | **278 ops/s, 2 timeouts in 16 703 ops**, no deaths |

After: OST RSS peak ~330 MB (was ~1 GB+), stored data 54-108 MB (only
live files), server threads capped at 131. Throughput at 500 is higher
than it was at 200 before the gate: fewer runnable threads means less of
the 2 cores lost to context switching. The 1->200 ladder re-run with the
gate: 0 errors everywhere, and at 200 md 1 017 -> 1 326 ops/s, md_shared
954 -> 1 455, p99 roughly halved. Unit suite, crossover rig, IOR/mdtest
(`pool` and `dist`) all green.

## Results after fixes 1-4 (first round, before the gate)

Ladder, 15 s per step (ops/s, error rate, worst p99):

| N | md | md_shared (one hot MDT) | data (1 MiB files) |
|---|---|---|---|
| 1 | 290, 0 %, 6 ms | 281, 0 %, 6 ms | 107, 0 %, 18 ms |
| 10 | 2 209, 0 %, 9 ms | 2 116, 0 %, 9 ms | 430, 0 %, 49 ms |
| 50 | 1 930, 0 %, 78 ms | 1 724, 0 %, 100 ms | 412, 0 %, 323 ms |
| 100 | 1 485, 0 %, 249 ms | 1 320, 0 %, 336 ms | 354, 0 %, 1.06 s |
| 200 | 1 017, 0 %, 785 ms | 954, 0 %, 665 ms | 279, **0.9 %** (5 s timeouts), 6.6 s |

200 clients sustained for 60 s: md 59 661 ops / 0 errors, md_shared 53 973 /
0, data 18 303 / 0; no server died, no client hung. Before the fixes the same
200-client steps ran at 97-99 % errors with MDTs dead.

## Where the ceiling is now, and why

- **Throughput peaks at 10-50 clients, then falls; latency grows.** At 200
  clients load average is 58-84 on **2 CPU cores**, while the servers
  themselves use only 30-50 % CPU each (a single MDT's GIL ceiling is 100 %).
  The client threads and servers are fighting for the same two cores: this
  is the load-generation host's limit, not a server-software one.
- **Beyond 200 it degrades, it doesn't break**: md at 400 is 0 % errors
  with p99 3.4 s; md at 800 and data at 400-800 hit the 5 s request timeout
  (22-40 %) with servers at 1-13 % CPU and load 126-162 -- they barely get
  scheduled. No crashes, no protocol errors.
- **Memory under large in-flight requests** (first-round finding, now
  fixed as #5-6 above). The first-round guess here was partly wrong: the
  two quick A/Bs (2 MiB frames, `MALLOC_ARENA_MAX=2`) didn't move RSS, and
  it was attributed to live Python copies. Measuring directly with
  mallinfo2 showed most of it was freed-but-retained arena memory, plus
  unbounded queuing -- hence the probe.
- **Remaining at 500 on this host**: latency, not failure -- data p99 8.4 s
  (mostly time spent waiting at the gate), load average 56-78 on 2 cores.
  A client that waits at the gate has no connect timeout (it queues until
  admitted); a wedged server would therefore show up as hung clients, which
  the harness reports.
- **What this host cannot tell us**: real-NIC behaviour (no TCP underneath,
  different CM costs), and whether one MDT process would saturate its GIL
  before the machine did -- here the machine always ran out first.

## Round 3: A+B+C

Native C servers, pooled persistent connections, SRQ + registered buffer
pool: the same ladder at 5.9-22x (metadata) and 2.7-6.5x (data), 500
clients x 60 s at 7 925 / 7 155 / 766 ops/s with 0 errors. Details, and
the eleven transport bugs found while making connection reuse safe:
`docs/ABC_UPGRADE.md`.

