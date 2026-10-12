# YCSB benchmark: `tools/bench_ycsb.py`

One command, no manual steps for `--backend local`; `--backend pool` needs
root (network namespaces):

```bash
python3 tools/bench_ycsb.py                    # --backend local (default): MemoryEngine
python3 tools/bench_ycsb.py --backend pool      # 3-node PoolNode over RoCETransport, crossover cable
python3 tools/bench_ycsb.py --quick             # smaller record/op counts, fewer workloads
```

It downloads the real, unmodified upstream YCSB 0.17.0 release
(https://github.com/brianfrankcooper/YCSB), compiles a custom `DB` binding
against it, and runs YCSB's own `load` then `run` phases for its standard
workloads a-f. Writes `ycsb_bench_report.json` and prints one PASS/FAIL
line per workload with throughput and per-operation latency.

## Why a bridge, not a Java reimplementation of the wire protocol

`PoolNode`'s real protocol (`roce/protocol.py`'s 24-byte `Frame` header,
key-hash routing, RoCE transport) is Python-native and already thoroughly
tested (`tests/test_pool_node.py`, the whole crossover-cable rig in
`docs/CROSSOVER_RDMA_TEST.md`). Reimplementing it in Java would benchmark a
second, parallel implementation instead of this project's actual code path
-- the same reasoning `tools/bench_ior.py` follows by mounting a real
kernel FUSE filesystem instead of writing a POSIX-alike test harness.

So `tools/ycsb_java/RocepoolClient.java` implements the real, unmodified
`site.ycsb.DB` interface and talks a small fixed-width binary protocol to
`tools/ycsb_bridge.py`, which forwards every `read`/`insert`/`update`/
`delete`/`scan` straight onto a real `PoolNode` (or a bare `MemoryEngine`
for the local backend) -- the bridge is protocol glue only, with its own
13 unit tests (`tests/test_ycsb_bridge.py`) covering the wire format
independent of Java or YCSB.

Maven Central (`repo.maven.apache.org`) is not reachable from this
sandbox (blocked by the egress proxy's allowlist, confirmed with a 403 --
see `ensure_ycsb()`'s docstring), so the harness downloads YCSB's prebuilt
GitHub *release* tarball (dependency jars vendored) rather than building
from a Maven source checkout; GitHub itself is reachable. `RocepoolClient`
compiles with plain `javac` against the release's `core-0.17.0.jar` --
YCSB's Java client, workload generators and statistics collection are
otherwise untouched.

## The two backends

- `--backend local`: bridge in front of a bare `MemoryEngine`, no network.
- `--backend pool`: the same 2-backer `PoolNode` cluster shape as
  `tools/bench_ior.py --backend pool` (crossover-cable veth,
  `RoCETransport`/softverbs), with the bridge itself running as the third
  `PoolNode` member inside `rocepool_nodeA`'s network namespace. The host's
  own namespace cannot reach `10.99.0.x` at all (deliberate, documented in
  `docs/CROSSOVER_RDMA_TEST.md`), so the bridge listens on its
  namespace-local loopback and the YCSB Java client is launched via
  `nsenter` into that same namespace -- the same pattern every other
  cluster-facing process in this project's test rig uses.

## Results (stable across repeated runs; local host, softverbs emulator)

| Workload | local throughput | pool throughput | local READ avg/p99 (us) | pool READ avg/p99 (us) |
|---|---|---|---|---|
| a (50/50 read/update) | ~15-17k ops/s | ~400-410 ops/s | 40-50 / 140-280 | 1590-1650 / 3400-3510 |
| b (95/5 read-mostly) | ~19-20k ops/s | ~585-610 ops/s | 38-40 / 130-140 | 1550-1590 / 3070-3320 |
| c (read-only) | ~15-21k ops/s | ~555-625 ops/s | 57-117 / 400-1150 | 1585-1600 / 3240-3910 |
| d (read-latest) | ~16-17k ops/s | ~610-620 ops/s | 48-49 / 160-220 | 1580-1600 / 3620-3770 |
| e (scan-heavy, scaled 1/20 for pool -- see below) | ~258-284 ops/s | ~11 ops/s | -- | -- |
| f (read-modify-write) | ~5.4-13k ops/s | ~310-320 ops/s | 42-92 / 150-1000 | 1550-1640 / 3180-4580 |

`--backend local` numbers are in-process (no real network) and dominated by
JVM/YCSB overhead, not this project's own code. `--backend pool` numbers
cross the crossover cable through the softverbs emulator for every single
operation -- one full request/response round trip per read/update/insert,
which is exactly `PoolNode`'s real current protocol shape (no batching, no
pipelining). Read latency lands around 1.5-1.6ms regardless of workload,
consistent with one RTT through the emulated RoCE path plus the bridge's
own TCP hop; this is the same class of number IOR's `mdtest` surfaced
(`docs/IOR_BENCHMARK.md`) from a completely different code path (FUSE
metadata ops vs KV get/put), which is a useful cross-check that it's a real
per-request-round-trip cost and not an artifact of either harness.

Workload e (scan) is run at recordcount/operationcount scaled to 1/20 under
`--backend pool` (`tools/bench_ycsb.py`'s `main()`): `Bridge.handle_scan`
(`tools/ycsb_bridge.py`) fans `list_prefix` out to every node on every
single scan call, and at full size (2000 records, 5000 ops) that
legitimately ran for many minutes over the network rather than the
harness hanging -- found by running it for real and hitting the 180s
per-phase subprocess timeout. At the scaled-down size it still exercises
the real fan-out path and finishes in well under a minute; the resulting
throughput is still on the same *kind* of measurement as the local run
(YCSB's own timing), just at a size that's actually practical to run
repeatedly. `run_ycsb_phase` now also catches a timeout gracefully
(reports the workload as failed with `"timeout": true`, `stdout_tail`
included) instead of letting an uncaught `TimeoutExpired` crash the whole
harness mid-matrix -- found the same way, on the first full (non-`--quick`)
`--backend pool` run.

## Issues found building this harness

Unlike `tools/bench_ior.py`, this didn't surface a correctness bug in the
storage layer itself (YCSB's KV access pattern doesn't touch the FUSE/POSIX
code IOR exercises) -- the "test for real" pass here instead surfaced two
real issues in the harness's own design, both fixed:

1. **Assumed the host namespace could reach the pool cluster's addresses.**
   The first `--backend pool` attempt had the bridge listen on
   `10.99.0.1` and tried to connect YCSB (run from the host) straight to
   it -- but the host's own network namespace was deliberately built to
   NOT reach `10.99.0.x` (`docs/CROSSOVER_RDMA_TEST.md` states this
   explicitly; the assumption in the first version of this file's
   `start_pool_cluster()` contradicted the project's own documented
   design and should not have been made without checking). Fixed by
   having the bridge listen on its namespace-local loopback and running
   the YCSB client via `nsenter` into that namespace instead.
2. **Scan-heavy workload at full size vs. a fixed subprocess timeout.**
   Described above -- fixed by scaling workload e down under `--backend
   pool` and by catching the timeout instead of crashing on it.
