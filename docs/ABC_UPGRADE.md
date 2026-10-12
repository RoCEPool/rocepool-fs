# A + B + C: native servers, persistent RDMA connections, real-hardware path

The three tiers from the hardware estimate, all implemented:

- **C** -- the data path off the Python GIL: OST and MDT servers in C
  (`roce/native/rocepool_servers.c`), wire-compatible with the Python
  reference servers; binary MDT protocol (`fs/mdt_wire.py`).
- **B** -- pooled persistent connections plus bounded registered memory:
  client connection pool with a safe retry rule, servers receiving through
  a shared receive queue (SRQ), frame buffers from one process-wide
  registered pool, completion channels instead of sleep-polling, a
  draining server stop.
- **A** -- the real-hardware path: builds and links against real rdma-core,
  hardware-only issues fixed, and the whole on-site bring-up automated
  (`tools/roce_onsite.py`, see `docs/ROCE_DEPLOYMENT_NOTES.md`).

## Results (same 2-core rig, same commands as before)

`tools/stress_concurrency.py`, 15 s per step, 10 client threads per
process, 2 MDTs + 4 OSTs over the crossover link. ops/s, all runs 0 % errors:

| clients | md: before | C only | **A+B+C** | data (1 MiB): before | C only | **A+B+C** |
|---|---|---|---|---|---|---|
| 1 | 255 | 1 896 | **5 720** (22x) | 89 | 427 | **585** (6.5x) |
| 10 | 1 777 | 2 560 | **6 331** (3.6x) | 439 | 658 | **1 169** (2.7x) |
| 50 | 1 789 | 2 723 | **12 242** (6.8x) | 401 | 660 | **1 118** (2.8x) |
| 100 | 1 434 | 2 611 | **9 758** (6.8x) | 340 | 626 | **1 088** (3.2x) |
| 200 | 1 326 | 2 481 | **7 858** (5.9x) | 292 | 602 | **991** (3.4x) |

md_shared (every client in one directory, one hot MDT) moves the same way:
1 454 -> **10 104** ops/s at 200 clients. "Before" is
`stress_reports/15_...`, "C only" is the native servers with pooling
switched off (`ROCE_CONN_POOL=0`, `17_...`), A+B+C is `16_...`.

Latency at 50 clients (md create): p50 35 ms -> 4.6 ms, p99 82 ms -> 14 ms.
Server CPU per metadata op: **0.47 ms -> 0.047 ms** (MDT CPU / ops).

500 clients x 60 s (`14_...` -> `18_...`):

| | md | md_shared | data |
|---|---|---|---|
| before | 1 137 ops/s, p99 0.95 s | 1 306, p99 0.7 s | 278, write p50 2.9 s / p99 8.4 s |
| A+B+C | **7 925**, p99 0.29 s | **7 155**, p99 0.36 s | **766**, write p50 0.84 s / p99 6.3 s |

All 0 errors, no server deaths. OST resident memory stays ~330 MB with
500 clients holding connections open (as before, with connect-per-request),
because receive buffers are per server (SRQ), not per connection.

IOR + mdtest (`tools/bench_ior.py --backend dist`, one client node, FUSE):

| | before | A+B+C |
|---|---|---|
| IOR bandwidth (geomean, 10 write+read results) | 0.175 GiB/s | 0.192 GiB/s |
| mdtest file creation | 218 /s | **2 652 /s** (12x) |
| mdtest file removal | 447 /s | **6 048 /s** (13.5x) |
| mdtest file read | 10 305 /s | 11 658 /s |
| mdtest tree creation / removal | 237 / 185 /s | **2 278 / 2 164 /s** |
| IO500-style score | 0.70 | **1.87** (2.7x) |

Reading these honestly:

- **IOR bandwidth barely moved.** One FUSE client in Python, the emulator's
  TCP copies and six servers share 2 cores; the single-client data path is
  client-bound, which none of A/B/C touched. The multi-client `data`
  workload above (2.7-3.4x) is where the server-side gains show.
- **mdtest "File stat" is not usable as a rate** here: 400 stats are served
  mostly from the kernel/attribute caches in under a millisecond, so the
  figure (58k before, 591k after) is timer noise. Without stat, the
  metadata geomean (create, read, removal) goes 1.0 -> 5.7 kIOPS.
- **Where the 500-client data tail comes from**: CPU queueing (500 clients
  / 766 ops/s is ~0.65 s mean). SRQ depth 32 vs 128 made no difference
  (p99 3.0 s vs 3.6 s, within noise), so it is not RNR back-pressure.
- The rig is still an emulator on 2 cores. Hardware numbers come from
  `tools/roce_onsite.py --bench full` on site.

## What changed, and the bugs found on the way

C (native servers): see `roce/native/rocepool_servers.c`; parity with the
Python servers is tested op-for-op (`tests/test_native_servers.py`).

B, in the order the problems showed up (9-11 came from an independent
review of the finished C code):

| # | problem | how it was found | fix |
|---|---|---|---|
| 1 | a request that reached the server but whose reply was lost (process exited right after its handler ran) | hpc crossover phase: 1 run in ~40 took 32 s instead of 2.7 s, and one rank exited non-zero -- but the harness still said PASS | `roce_server_stop()` drains replies in progress; the crossover harness now fails a phase when any rank exits non-zero |
| 2 | a stopped server kept serving pooled connections, calling a handler that had been freed (SIGSEGV) | new test: restart a server under a pooled connection | stop disconnects every live connection and waits for its thread; running servers' callbacks are kept alive until stop |
| 3 | retrying on `WR_FLUSH_ERR` could run a non-idempotent request twice | review of the verbs spec: a QP entering error flushes WRs already transmitted but not ACKed | retry only when the connection was provably dead **before** posting (CM event pending or its RECV flushed) or posting failed; after posting, every failure is surfaced |
| 4 | every idle pooled connection cost ~1000 wakeups/s (1 ms sleep-poll) | code review while adding pooling | completion channels + `poll()`; optional busy-poll `ROCE_POLL_SPIN_US` |
| 5 | 32 MiB pinned per connection on a real NIC (two 16 MiB registered frames) | hardware review | SRQ per server + process-wide registered buffer pool; `tests/test_conn_pool.py` checks 64 open connections add no receive buffers |
| 6 | with an SRQ, a disconnect produces no receive completion: server threads never noticed a gone peer or `stop()` | pool test: 2.4 s stop, restart case timing out | threads also watch the connection's CM channel |
| 7 | the one-slot completion stash lost a request when a pipelining peer had several receives complete on one SRQ connection | `test_softverbs_rc_semantics` pipelined case timed out | per-kind FIFO stash sized for SRQ depth; CQ sized for SRQ depth |
| 8 | several servers in one process starved each other's clients of pool buffers | native parity test: "registered buffer pool exhausted" | each SRQ has its own registered slab; the shared pool only backs transient buffers |
| 9 | a pipelining peer that disconnected mid-stream leaked the SRQ buffers of its stashed / unpolled receives; after ~SRQ-depth such events every SEND to that server was RNR-NAKed forever | independent review of the C changes; regression test stalls 9/12 rounds without the fix | on exit a connection returns every SRQ buffer it holds (errored receive, stash, CQ drained after disconnect) |
| 10 | a failed accept leaked the connection's CM event channel (an fd each): under overload the server would eventually accept nothing | independent review | channel destroyed on every accept failure path |
| 11 | large requests took their two pool buffers one at a time: at the pool cap every caller held one and waited for the other | independent review; regression test hangs without the fix | all-or-nothing acquisition; server replies use their own pool |

A (real hardware):

| issue (would only show on real rdma-core / a real NIC) | fix |
|---|---|
| destroying the listen id does not wake a thread blocked in `rdma_get_cm_event` on real rdma-core: `stop()` would hang | accept loop polls the channel fd with a timeout |
| `rdma_get_cm_event` has no timeout: a lost CM reply blocked `connect` forever | `poll()` on the channel fd with the connect timeout |
| SRQ depth / frame size vs device limits | `ibv_query_device`: clamp to `max_srq_wr`, check `max_mr_size`, clear error if no SRQ support |
| RoCE traffic in the wrong switch class | `ROCE_TOS` (from the PFC/DSCP preflight) and `ROCE_ACK_TIMEOUT` via `rdma_set_option` |
| `ibv_reg_mr` failing for RLIMIT_MEMLOCK was an opaque error | message names the limit and the fix; the preflight checks it before anything runs |
| `build.sh` required manually installed headers | finds / installs / builds rdma-core by itself |

Verified for A: the sources compile with `-Wall -Wextra -Werror` and link
against real rdma-core v59.2 (built from source here); every verbs/CM
symbol resolves against the real libraries; with no RDMA device the real
library fails cleanly. The emulator gained what the real API needs
(completion channels, pollable CM fd, SRQ, `ibv_query_device`,
`rdma_set_option`, PD checks on every lkey). Not verifiable here: a run on
real hardware -- that is step 1 on site.

## On site

```bash
sudo python3 tools/roce_onsite.py --ip <this RoCE IP> --peer root@<host2> --peer-ip <its RoCE IP> --bench full
```

preflight (with safe fixes) -> build -> transport scenarios on the real NIC
-> peer preflight + build -> functional phases across both servers ->
concurrency ladder to 500 clients. Each step is a PASS / WARN / FAIL
decision; it stops at the first FAIL. Details:
`docs/ROCE_DEPLOYMENT_NOTES.md`.

## Knobs (environment)

| variable | default | |
|---|---|---|
| `ROCE_CONN_POOL` | 1 | 0 = one connection per request (old behaviour) |
| `ROCE_POOL_MAX_IDLE` / `ROCE_POOL_IDLE_S` | 8 / 30 | idle connections kept per peer / closed after |
| `ROCE_SRQ_DEPTH` | 32 | receive buffers per server (max 256, clamped to the device) |
| `ROCE_BUFPOOL_MAX` | 512 | cap on pooled frame buffers per process |
| `ROCE_SERVER_MAX_INFLIGHT` / `_MAX_CONNS` | 64 / 4096 | handler concurrency / open connections per server |
| `ROCE_SERVER_DRAIN_MS` | 2000 | bound on each phase of a server stop |
| `ROCE_POLL_SPIN_US` | 0 | busy-poll before sleeping (preflight sets 20 on >= 16 cores) |
| `ROCE_TOS`, `ROCE_ACK_TIMEOUT` | unset | set by the preflight from the NIC's DCB setup |
| `ROCE_TRACE_SLOW_MS` | 0 | log requests slower than this, and every failed attempt |

## Tests added

`tests/test_conn_pool.py` (reuse, opt-out, concurrent callers, server
restart under a pooled connection, mismatched reply, stop drains an
in-flight reply, idle expiry, memory bounded by SRQ, SRQ exhaustion is
back-pressure), `tests/test_native_servers.py`,
`tests/test_softverbs_rc_semantics.py` (+ one-buffer SRQ pipelining and the
RNR negative control through the SRQ), `tests/test_roce_preflight.py`
(15 decisions on fake bnxt_re trees), `tests/test_roce_onsite.py` (the
orchestrator end to end). 265 tests pass; crossover rig 15/15 on the final
code (hpc phase 2.8 s -> 2.4 s), plus 40/40 and 20/20 on the builds before
the last review fixes. Stress spot-check after those fixes: md 11 493 /
9 911 ops/s and data 1 137 / 964 ops/s at 50 / 200 clients (unchanged).
