# IOR vs. YCSB: which one scores this system more favorably, and why

Both benchmarks are real, unmodified upstream tools (`tools/bench_ior.py`,
`tools/bench_ycsb.py`), run against the same two backends -- a single-node
in-memory `PooledFileSystem`/`PoolNode` (`--backend local`) and a genuine
3-node `PoolNode` cluster over `RoCETransport` across the crossover-cable
link (`--backend pool`, see `docs/CROSSOVER_RDMA_TEST.md`). Full numbers in
`docs/IOR_BENCHMARK.md` and `docs/YCSB_BENCHMARK.md`; this doc is the
direct answer to "which framing favors us."

## The answer: IOR/mdtest, clearly

**IOR** -- the IO500-style large-block/parallel-file benchmark -- scores
this system well. Local-backend bandwidth reaches into the hundreds of
MiB/s to ~2 GiB/s on reads, `mdtest` metadata rates reach into the hundreds
of thousands to over a million ops/sec, and even the distributed
`--backend pool` numbers (crossing real network hops through the softverbs
emulator) hold up: tens of MiB/s sustained, hundreds of IOPS, file-per-
process cases genuinely fanning out across both nodes.

**YCSB** -- the small-record KV-latency benchmark -- does not. Local
throughput (15-21k ops/sec) is respectable but unremarkable next to a
purpose-built KV store; under `--backend pool`, throughput drops to
roughly 300-620 ops/sec for point read/update/insert workloads, with every
single operation paying ~1.5-1.6ms of latency regardless of how small the
record is.

## Why: the same architecture, two very different amortization stories

Both benchmarks, under `--backend pool`, are paying the *exact same*
per-request cost: one full round trip through `RoCETransport`/softverbs
across the crossover cable, with no batching or pipelining at the
`PoolNode` protocol layer (confirmed directly -- IOR's `mdtest` metadata
ops and YCSB's KV reads land at a strikingly similar order of magnitude of
per-op latency, even though they exercise completely different code paths:
FUSE metadata calls vs. raw `PoolNode.get`/`put`. That's not a coincidence
-- it's the same fixed RTT surfacing twice).

- **IOR mostly hides that cost.** Its default and largest cases move
  megabytes per request (`-b 32m -t 4m`, IOR's `-b`/`-t` flags), so a ~1-2ms
  round trip is amortized over MBs of payload -- the arithmetic favors
  large transfers by construction. `mdtest` (many small metadata ops) is
  the one place IOR's own suite hits the same wall YCSB does, and it shows
  the same ~100x slowdown pattern between local and pool that YCSB shows.
- **YCSB never hides it.** Every operation is one small record (order
  100 bytes), so YCSB's ops/sec metric *is*, almost directly, "1 /
  round-trip-time" -- there's no large payload to amortize the fixed cost
  over. It measures exactly the thing this architecture hasn't been
  optimized for yet: request-response latency at small-record scale,
  with no batching, pipelining, or client-side caching in `PoolNode`'s
  current protocol.

In short: this system's real, demonstrated strength (also documented across
the earlier crossover-cable phases -- striping, MPI-IO, RemoteEngine) is
**moving bulk data efficiently across a distributed pool**, which is
exactly what IOR/IO500-style scoring rewards. Its current, real limitation
is **per-request overhead at small-record scale**, which is exactly what
YCSB scores. Neither number is an artifact of the benchmark harnesses
(both were run for real, repeatedly, with data integrity verification
where the tool supports it -- IOR's `-W -R -G 42`) -- they're measuring two
different real things, and this architecture is genuinely better at one of
them than the other.

## If YCSB-class numbers matter going forward

The gap is specific and addressable, not fundamental: `PoolNode`'s
protocol (`roce/protocol.py`) is currently strict request/response with no
pipelining, so a client thread's Nth operation cannot be sent before the
(N-1)th's response arrives -- the same shape that made `mpi/comm.py` need
an explicit wire-up probe (`docs/CROSSOVER_RDMA_TEST.md`, bug 5) and that
shows up here as a latency floor no amount of extra client threads can
raise throughput past very far (YCSB does use multiple client threads;
pool-backend throughput only partially scales with them because every
thread is still one-request-in-flight-at-a-time against the same kind of
RTT). Batching multiple small ops into one wire frame, or allowing several
in-flight requests per connection, would move the YCSB number without
touching anything IOR exercises -- but that's future work, not something
either benchmark run here implies is broken.

### Connection pooling: tried, reverted

The obvious first move -- reuse a persistent `RoCETransport` connection
per peer instead of `rdma_resolve_addr`/`route`/`connect` + QP setup on
*every* `send_request()` call -- was implemented and measured. It did
shave the connect+close overhead (~0.2ms, ~10-15% of a small request's
~1.5-2.3ms) as expected, matching `roce_client_send_request()`'s own
documented design (it re-arms its RECV buffer specifically so a
connection can be reused). But verifying it against the existing
crossover-cable test rig (`tools/run_crossover_rdma.py`), not just
re-running YCSB, surfaced a real regression: the `hpc` phase (4-rank MPI
+ striped Lustre-style I/O) started intermittently failing with
`RDMA_CM_EVENT_REJECTED` on reconnect, and, once instrumented at the
frame level, a deeper failure underneath that -- a held-open connection
could go silent forever: one side's low-level ACK for an already-posted
request just never arrived, with no error either side, until the
sender's own completion poll timed out. Reproduced independently of MPI
(a plain rank<->OST `RemoteEngine` connection hit it too), so it's a
concurrency/timing issue in the softverbs userspace RC emulator (or the
threading around it) that only a connection staying open -- idle, then
busy again, under concurrent multi-peer traffic -- can trigger; the old
connect-per-request pattern never exercised that shape of traffic in
this project's entire test history, hpc phase included.

Root-causing that race fully (deep in a from-scratch userspace RDMA
emulator, likely a scheduling/threading interaction rather than a
one-line bug) was going to take significantly more work with an
uncertain payoff, against a benchmark-only win that can silently wedge a
live MPI connection. Reverted `roce/roce_transport.py` back to
connect-per-request (`send_request()` now connects, sends, and closes
every call again, exactly as it did throughout this project's whole test
history including the hpc phase) rather than ship that trade. The YCSB
numbers in this doc and in `docs/YCSB_BENCHMARK.md` are the actual,
current, reverted-transport numbers -- re-run and reconfirmed after the
revert, `[PASS]` on every workload, `tools/run_crossover_rdma.py --phases
smoke,pool,mpi,hpc,stream` all green, and the 207-test pure-Python suite
green.
