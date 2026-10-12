# Framework notes (the original design write-up)

> Kept for the history of the design; the current overview is the top-level README.md.

An open, clean-room implementation of a distributed, RDMA(RoCE)-backed
shared memory pool exposed as a file system: a pluggable in-memory storage
engine, a multi-node pool coordinator, a POSIX-style file layer, and a
transport that runs identically over plain TCP (for development/CI) or
real RoCE (for deployment) -- plus a Lustre-style striping layer
(`fs/striping.py`) and a minimal MPI-IO-style collective I/O layer
(`mpi/`) built on top of it. See "Lustre- and MPI-style features" below.

## Lustre- and MPI-style features

Two new layers, both built on the existing interfaces rather than
requiring any change to them:

- **`fs/striping.py` (Lustre RAID0-style striping).** A file's bytes are
  split into fixed-size stripes and placed round-robin across a list of
  OSTs (`StorageEngine` instances -- could each be a remote `PoolNode`,
  though that composition isn't wired up in this pass), the same
  `stripe_count`/`stripe_size`/`stripe_offset` model as `lfs setstripe`,
  with `stripe_locations()` as the `lfs getstripe` analogue. Partial
  writes/reads (`write_range`/`read_range`) only touch the stripes a
  byte range actually overlaps, fanned out across OSTs concurrently via
  a thread pool -- that's what makes striping actually parallelize I/O.
  OSTs can be remote: `roce/remote_engine.py`'s `RemoteEngine` binds one
  OST to one server node over RoCE or TCP. Concurrent writes to disjoint
  byte ranges of one file all survive even when they share a stripe
  (per-stripe compare-and-swap in place of Lustre's DLM extent locks).
  What it deliberately doesn't include: no MDS/OSS process split (the
  layout record lives on OST0), no OST redundancy (RAID0 semantics --
  losing one OST loses every file striped across it), no DLM. See the
  module docstring for the full comparison.
- **`mpi/` (MPI-style communicator + collective file I/O).**
  `mpi/comm.py`'s `Communicator` gives point-to-point `send`/`recv` and
  `barrier`/`bcast`/`gather`/`allgather`/`reduce_sum`/`allreduce_sum`
  over the same `Transport` interface `pool_node.py` uses (real sockets
  via `LoopbackTransport`, same as everywhere else in this project).
  `mpi/mpi_file.py`'s `MPIFile` layers MPI-IO-shaped collective file
  operations (`MPIFile.open` / `write_at` / `write_at_all` /
  `read_at_all` / `set_view`) on top of `StripedStorage` -- the same
  pairing ROMIO's `ad_lustre` driver uses in real HPC systems, since
  collective I/O is most useful precisely when it lines up with a
  striped backing store. Scope, stated plainly: collectives are simple
  centralized (root-coordinated) algorithms, not the tree/butterfly
  ones real MPI uses at scale; `write_at_all`/`read_at_all` provide
  correct synchronization and genuine parallel per-rank I/O, but not
  ROMIO's specific two-phase memory-reshuffling optimization; reductions
  are `MPI_SUM` on 64-bit integers only. See both modules' docstrings.

A real concurrency bug was caught building this, not just avoided by
inspection: `StripedStorage`'s file-size metadata used blind
read-then-write, so concurrent ranks writing disjoint regions of the
same file (exactly the `write_at_all` collective-write case) would lose
each other's size updates -- `tests/test_mpi_file.py`'s collective write
test failed with a stale total size before this was fixed with a
compare-and-swap retry loop (`StripedStorage._bump_total_size`).

## Replacing TCP with RoCE/RDMA

`PoolNode` and `mpi.comm.Communicator` were already written against the
`roce.transport_base.Transport` interface (`send_request`/`serve`), not
against TCP directly, so `RoCETransport` (already present, previously
just unused/untested) is a structural drop-in for `LoopbackTransport` for
both of them with **no code change to either class** --
`examples/roce_pool_node_demo.py` demonstrates the swap for a 3-node pool
and a 3-rank MPI communicator.

The one place that genuinely used raw TCP directly -- `wlp/wlp_client.py`,
which talks to a RapidsSE (or `tests/fakes/wlp_test_server.py`) node over
its own WLP wire protocol, not over `Transport` -- did need a change,
because `socket.create_connection(...)` was hardcoded into its
constructor:

- `wlp/wlp_client.py`'s wire I/O (`_send`/`_recv`, via
  `wlp/framing.py`'s `send_frame`/`recv_frame`) only ever calls
  `self._sock.sendall(...)` and `self._sock.recv(n)` -- nothing TCP-
  specific. `WlpClient.from_stream(stream)` is a new alternate
  constructor that accepts anything with that shape; the original
  `WlpClient(host, port)` TCP constructor is unchanged. This is checked,
  not just asserted: `tests/test_wlp_client_stream_adapter.py` wraps a
  real socket in an object that exposes *only* `sendall`/`recv`/`close`
  (not a `socket.socket` subclass) and runs a full handshake + query
  through it, so if `wlp_client.py` or `framing.py` ever reached past
  that boundary the test would fail with `AttributeError` rather than
  silently working by inheritance.
- `roce/native/roce_transport.c`/`.h` gained two new functions,
  `roce_client_stream_send`/`roce_client_stream_recv`, built entirely
  from the existing `post_send`/`post_recv`/`poll_for_completion`
  helpers (no new `rdma_cma`/`ibverbs` calls) -- they let one already-
  established RDMA connection be driven as an independent send/recv pair
  instead of `roce_client_send_request`'s strict one-request-one-
  response shape, which is what `wlp_client.py`'s handshake and
  multi-message statement flows need.
- `roce/roce_transport.py` gained `RoCEStream`, a Python wrapper around
  those two functions presenting a `sendall`/`recv`/`close` surface --
  the exact shape `WlpClient.from_stream()` needs. RDMA SEND/RECV
  preserve message boundaries (unlike TCP's byte-stream `recv`), so
  `RoCEStream.recv(n)` buffers one whole inbound RDMA message and hands
  out `n`-byte slices of it, exactly like a TCP socket that happened to
  return a short read.

### Running it between two nodes: `tools/run_crossover_rdma.py`

One command (root, no manual steps) builds two network namespaces joined
by a single veth pair -- a back-to-back cable, no switch -- and runs the
RoCE transport between them as separate processes: the unmodified smoke
test, a 3-node `PoolNode` cluster split across the cable, a 4-rank MPI
communicator (2 ranks per node), and a pipelined `RoCEStream`. All pass,
repeatably.

The cable is real (a real kernel network link between isolated network
stacks); the RDMA *device* is not available here (no RDMA NIC, no
`rdma_rxe` module, no rdma-core packages -- rechecked). So
`roce_transport.c` is linked **unmodified** against
`roce/native/softverbs/`, a userspace RDMA device that enforces RC
queue-pair rules strictly (ACK-gated SEND completion, RNR NAK + retry per
`rnr_retry_count`, RECV length and lkey checks, flush on error), and
`tests/test_softverbs_rc_semantics.py` checks it also FAILS what hardware
fails. On a machine with Soft-RoCE available, the same command binds
`rdma_rxe` to the veth ends and runs the real library instead (that branch
is untested here).

Running it found seven real bugs, all fixed with regression tests -- three
in the RDMA code that had only ever been syntax-checked
(`RoCETransport.serve()` couldn't start at all; `rnr_retry_count` left at
0 so any pipelined SEND killed the QP; `poll_for_completion()` discarded
the other queue's completions), a `PoolNode` bug (remote CAS conflicts had
a different exception type than local ones), an MPI wire-up race between
separately launched ranks, a byte-by-byte Python copy that made large
replies ~7x slower, and a striping bug that lost data when MPI ranks wrote
disjoint parts of the same stripe concurrently. The `hpc` phase runs
Lustre basics (15 checks), MPI-IO (N-1 segmented, N-1 strided, N-N) and
MPI basics with 4 OSTs and 4 ranks split across the cable. Details, and exactly what this does
and does not prove: `docs/CROSSOVER_RDMA_TEST.md`.

### Performance benchmarks: `tools/bench_ior.py`, `tools/bench_ycsb.py`

Redesigned distributed POSIX path (dedicated sharded MDTs, client
attribute cache, striping across OSTs, parallel fan-out):
`tools/bench_ior.py --backend dist`, compared against the original in
`docs/DIST_REDESIGN.md` (IO500-style score 0.095 -> 0.72 on this rig).
Concurrency ceiling (1 -> 200+ clients, what broke first and the fixes):
`tools/stress_concurrency.py`, `docs/CONCURRENCY_CEILING.md`.
Native C servers + pooled persistent RDMA connections (SRQ, registered
buffer pool) + the real-hardware path: `docs/ABC_UPGRADE.md` (metadata
5.9-22x, data 2.7-6.5x across 1-200 clients; IO500-style score 0.70 -> 1.87).
Native C FUSE client replacing the Python one: `docs/NATIVE_CLIENT.md`
(IO500-style score 1.87 -> 3.50, 4.13 at 4x MPI ranks).
"Thick KV" -- OST ops for offset writes, versioned/ranged reads, truncate,
batches and ABA-safe version tokens; small files inline in the MDT
(Data-on-MDT); a native KV client API (`roce/native/rocepool_kv.h`,
`roce/kv.py`): `docs/THICK_KV.md` (IO500 hard pair: ior-hard write 21 ->
195 MiB/s, hard-pair score 0.85 -> 1.73; `tools/bench_ior.py --hard`,
`tools/bench_kv.py`).
N-node automation: `tools/roce_onsite.py --peers ...` deploys, starts
servers, mounts and runs the official IO500 on every host; `tools/io500_run.py
--nodes 10` rehearses it in one host and projects the result onto target
hardware with a per-phase bottleneck analysis: `docs/IO500_CLUSTER.md`.
Striped directories (one directory over every MDT, per-directory bucket
locks), zero-copy RDMA bulk (server-driven RDMA READ/WRITE, registered
client arena, no copy on a 1 MiB write) and FUSE over io_uring (probed
against /dev/fuse on the hosts, the faster one used):
`docs/SCALE_OUT_ROUND.md` (10-node projection 67 -> 98 on the classic
FUSE channel, 154 with io_uring's estimated per-mount rate; WEKA Afton 106).
Kernel bypass: the LD_PRELOAD interception client (`roce/native/
rocepool_intercept.c`, the FUSE client's core in the program's own process,
no mount; real IOR/mdtest/IO500 run under it, 1.9-2.6x FUSE's ior-hard
bandwidth here), the OST's NVMe capacity tier (`roce/native/ost_tier.c`:
write-through slabs, bounded RAM cache, NIC buffer -> drive direct writes;
kernel or SPDK backend) and P2P DMA from a drive's CMB, each gated by
`tools/nvme_preflight.py`; `tools/io500_run.py --client auto` probes FUSE
vs interception and `--ost-tier auto` uses the hosts' NVMe:
`docs/KERNEL_BYPASS_ROUND.md` (10-node projection 98 -> 375 in RAM, 687
with the tier; the metadata half is now beyond the model's measured
limits, the bandwidth half 22 GiB/s in RAM, the drives' with the tier).
One-sided reads and lookups (OST objects and MDT entries under a seqlock
in registered memory, read by the client's RDMA READ with the server's CPU
idle; stat phases 2.5-3x on the rig) and thread-per-core / NUMA placement
of the servers (`ROCEPOOL_NUMA_NODE`, pinned connection threads, one
MDT+OST per NUMA node on real hosts): `docs/ONE_SIDED_ROUND.md` (10-node
simulation 3.02 -> 3.77 measured, projection 375 -> 449).
AWS: a second transport backend on libfabric (`roce/native/ofi_transport.c`,
the EFA provider; the whole suite passes on it), unordered-safe one-sided
reads, Soft-RoCE (rxe) for the verbs backend unchanged, and
`tools/aws_cluster.py` -- one command creates the EC2 cluster, runs the
on-site suite + IO500, fetches the reports and releases everything:
`docs/AWS_ROUND.md`.
N-node IO500 (official app, all phases) with per-phase bottleneck analysis
and a projection onto target hardware -- simulated (`tools/io500_run.py
--nodes 10`) or on real hosts (`tools/roce_onsite.py --peers ...`):
`docs/IO500_CLUSTER.md` (10 x 32-core/100 GbE projected score ~67 vs
WEKA's 106; bottlenecks: RAM for 300 s writes, the hot MDT of a single
directory, the kernel FUSE channel).

Two real, unmodified upstream benchmarks, one command each, against a
single-node in-memory backend and against the genuine 3-node `PoolNode`
cluster over the crossover cable above:

- **IOR/mdtest** (https://github.com/hpc/ior, the IO500 suite's tool)
  against a real kernel FUSE mount of `PooledFileSystem`. Found and fixed
  three more real bugs this way (a lost-update bug in the FUSE flush path
  symmetric to the striping one above, a quadratic blowup in its first fix,
  and a wire-frame-size ceiling the quadratic bug had accidentally been
  hiding). Details and full results: `docs/IOR_BENCHMARK.md`.
- **YCSB** (https://github.com/brianfrankcooper/YCSB) against a small
  custom `site.ycsb.DB` binding that talks to a thin Python bridge in
  front of the real `PoolNode`. Details and full results:
  `docs/YCSB_BENCHMARK.md`.

Run both and they tell a consistent, honest story about where this
architecture's strengths and current limits are:
`docs/BENCHMARK_COMPARISON.md`.

## What's real and tested vs. what isn't

This project is explicit about this everywhere, not just here:

| Component | Status |
|---|---|
| `storage_engine/` (interface + in-memory reference engine) | **Tested.** 14 tests. |
| `storage_engine/rapidsse_engine.py` (RapidsSE-shaped SQL adapter) | **Tested two ways:** against a real sqlite3 backend directly (32 tests), and end-to-end over a real socket through the real WLP wire protocol below (18 tests) -- **not tested against RapidsSE's actual compiled server**, which this sandbox cannot build or run (no RDMA/cluster hardware, and the engine is ~2,500 C++ files with its own build toolchain). See the file's docstring. |
| `wlp/` (real WLP v5 client: Compact Protocol codec + RapidsSE's own wire framing) | **Tested.** 27 tests: protocol primitive round-trips (varint/zigzag/field-header/list-size edge cases), full message struct round-trips against the real `.thrift` IDL field IDs, real-socket framing, a full client<->server integration test (`tests/fakes/wlp_test_server.py`, backed by real sqlite3) covering the storage-engine contract plus binary/Unicode payloads and multi-connection state sharing, and `WlpClient.from_stream()`'s duck-typed adapter boundary (proves `wlp_client.py`/`framing.py` never call anything on the connection besides `sendall`/`recv`/`close`, which is what lets it run over `RoCEStream` unmodified). **Not verified against RapidsSE's actual C++ server** -- see `wlp/wlp_client.py`'s docstring for exactly what is and isn't confirmed. |
| `roce/protocol.py` (wire format) | **Tested**, via the transports that carry it. |
| `roce/loopback_transport.py` (real TCP transport) | **Tested.** 12 tests: concurrency, large payloads, real sockets. |
| `roce/pool_node.py` (multi-node pool logic) | **Tested.** 13 tests, real multi-node clusters over real sockets; also run as 3 separate processes across two network nodes over RoCETransport. |
| `roce/remote_engine.py` (one remote OST as a `StorageEngine`) | **Tested.** Full 26-test `StorageEngine` contract suite + remote-CAS test over a real socket; used as the OST handle in the two-node Lustre run. |
| `fs/posix_api.py` (POSIX file layer) | **Tested.** 20 tests, including through a real 3-node pool. |
| `fs/striping.py` (Lustre-style RAID0 striping) | **Tested.** 13 tests against real `MemoryEngine` OSTs: round-robin placement (including sub-pool `stripe_count < len(osts)`, which caught a real placement bug), partial-write stripe isolation, cross-stripe range I/O, delete, layout get/set. |
| `mpi/comm.py` (MPI-style communicator: point-to-point + collectives) | **Tested.** 9 tests (incl. wire-up of separately launched ranks) against real multi-rank clusters, each rank a real `Communicator` with its own real loopback-TCP server -- genuine socket round trips for send/recv/barrier/bcast/gather/allgather/reduce_sum/allreduce_sum. Also run as 4 separate processes across two network nodes over RoCETransport. |
| `mpi/mpi_file.py` (MPI-IO-style collective file I/O on striped storage) | **Tested.** 3 tests: real multi-rank collective writes/reads of a shared striped file, checked for byte-level correctness AND that data actually spread across multiple distinct OSTs. Caught and fixed a real lost-update race in concurrent size-metadata bookkeeping (see below). |
| `fs/fuse_lowlevel.py` (real kernel FUSE mount, hand-written against `/usr/include/linux/fuse.h`) | **Tested against the real Linux kernel** in this sandbox: mounted via a real `mount(2)` syscall, driven with real coreutils and Python file I/O (mkdir/cat/cp/dd+sha256/truncate/append/rm/rmdir/df/find, a 5MB binary round-trip, a 70KB write split across multiple kernel `WRITE` requests), clean unmount. One real bug found and fixed this way (a `statfs` struct-layout mismatch that crashed the mount). See `docs/REAL_KERNEL_FUSE_TEST.md` for the full transcript. Tested with `MemoryEngine` only; not yet re-run against `RapidsSEEngine`. |
| `roce/native/roce_transport.c` (real RDMA/RoCE transport) | **Compiled and run** between two network nodes over a back-to-back link (`tools/run_crossover_rdma.py`) against `softverbs`, a strict userspace RC emulator -- smoke, 3-node pool, 4-rank MPI and pipelined stream all pass; 5 emulator tests incl. 3 negative controls. Two real bugs found and fixed this way (RNR retry, dropped completions). **Never run on a real RDMA NIC or kernel verbs** -- none available here (verified directly). See `docs/CROSSOVER_RDMA_TEST.md`. |
| `roce/roce_transport.py` (Python binding: `RoCETransport` for `PoolNode`/`Communicator`, `RoCEStream` for `wlp_client.py`) | Same status as the C above: run end to end over softverbs across two nodes, not on hardware. Its server path could not start at all before that run (ctypes callback type; untyped `malloc`). |

**186 out of 186 tests pass** (79 for the original framework + 47 for the
real WLP protocol client and its stream adapter + 26 for the Lustre-style
striping and MPI-style layers + 27 for `RemoteEngine` + 2 for the
RoCE-unavailable failure path + 5 for the softverbs RC-semantics checks),
stable across repeated runs. Separately, `tools/run_crossover_rdma.py`
runs the two-node RDMA phases, including the Lustre/MPI-IO/MPI suite
(needs root). Run them yourself:

```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

## Architecture

See `docs/ARCHITECTURE.md` for the full picture. In short:

```
fs/posix_api.py           <- POSIX-style paths, directories, open/read/write
        |
roce/pool_node.py         <- consistent-hash ownership, request forwarding,
        |                     cross-node listing fan-out
roce/transport_base.py    <- abstract Transport interface
   |              |
loopback_transport.py   roce_transport.py   <- same interface, two implementations
   (real, tested)         (real API, untested -- see table above)
        |
storage_engine/base.py    <- pluggable engine interface
        |
   ------------------------------
   |                            |
memory_engine.py        rapidsse_engine.py  <- unchanged; DB-API2-shaped
(real, tested)                  |              RapidsSEClient Protocol
                                 |
                    ------------------------------
                    |                            |
          tests/fakes/sqlite_dbapi_client.py   wlp/wlp_client.py
          (unit-test double, no wire)           (real WLP v5 client: real
                                                  Compact Protocol codec +
                                                  RapidsSE's own 8-byte
                                                  frame header -- see
                                                  README's provenance
                                                  section)
```

## What this initial framework deliberately does NOT include

Stated plainly, not hidden (see `roce/pool_node.py`'s module docstring for
the canonical list):

- **No replication.** Each key lives on exactly one node. No redundancy.
- **No online rebalancing.** Node membership is a fixed list at construction
  time; changing it does not migrate already-placed keys.
- ~~No FUSE kernel mount~~ -- **now done**, at least for one backend:
  `fs/fuse_lowlevel.py` mounts `PooledFileSystem` at a real kernel
  mountpoint by talking directly to `/dev/fuse` in the kernel's own wire
  format (no `libfuse`/`fusepy`, neither installable here -- see that
  file's docstring), and was verified with real coreutils and Python file
  I/O against `MemoryEngine`; see `docs/REAL_KERNEL_FUSE_TEST.md` for the
  full transcript, including a real bug it caught. Not yet re-run against
  the `RapidsSEEngine`/WLP backend, and still no FUSE-level hard
  links/symlinks/xattrs/locking (see the module docstring for the exact
  op list).
- **No one-sided RDMA READ/WRITE data path.** The RoCE transport uses
  two-sided SEND/RECV, matching the loopback transport's request/response
  semantics. Using one-sided RDMA operations for the bulk data path (so a
  remote node's CPU isn't interrupted to serve a read) is a real,
  worthwhile next optimization, not included here.

## Getting started

```bash
python3 -m unittest discover -s tests -p "test_*.py" -v   # run everything that's testable here

python3 -c "
from storage_engine.memory_engine import MemoryEngine
from fs.posix_api import PooledFileSystem

fs = PooledFileSystem(MemoryEngine())
fs.write_file('hello.txt', b'world')
print(fs.read_file('hello.txt'))
"
```

For the multi-node pool, see `tests/test_pool_node.py`'s `_make_cluster()`
helper for a complete working example of bringing up several real nodes.

For real RoCE servers (e.g. Broadcom bnxt_re) it is one command --
`sudo python3 tools/roce_onsite.py --ip <RoCE IP> [--peer root@host2 --peer-ip <IP>]`
(preflight with automatic decisions, build, transport checks, two-node
functional run, benchmark ladder). See `docs/ROCE_DEPLOYMENT_NOTES.md`.
