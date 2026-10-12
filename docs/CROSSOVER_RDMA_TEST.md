# Two-node RoCE/RDMA test over a back-to-back link

One command, needs root, no manual steps:

```bash
python3 tools/run_crossover_rdma.py          # exit 0 = every phase passed
python3 tools/run_crossover_rdma.py --keep   # leave the two nodes up afterwards
```

It writes `crossover_rdma_report.json` and prints one PASS/FAIL line per phase.

## Topology: two nodes, one cable, no switch

`tools/netlink_veth.py` builds it with raw rtnetlink (no `ip`/iproute2 needed):

```
 network namespace rocepool_nodeA          network namespace rocepool_nodeB
 +-----------------------------+           +-----------------------------+
 |  lo                         |           |  lo                         |
 |  rpA  10.99.0.1/30  ========|== veth ===|======== rpB  10.99.0.2/30   |
 +-----------------------------+           +-----------------------------+
```

A veth pair is a point-to-point virtual Ethernet link: frames leaving `rpA`
arrive at `rpB` and nowhere else, with no bridge in between -- the same
thing as a crossover cable between two NICs. Each namespace has its own
network stack and sees only `lo` and its own end of the link; the host's
own namespace can't reach 10.99.0.x at all (checked when first building it).
Every node in the tests below is a separate OS process started inside one
of the two namespaces.

## The RDMA device: why softverbs, and what it models

A real RDMA path needs an RDMA device: a RoCE NIC, or the Soft-RoCE kernel
module (`rdma_rxe`) that turns an ordinary NIC into one. This environment
has neither (no `/dev/infiniband`, no `/lib/modules/*/.../infiniband`, no
`modprobe`, no rdma-core packages -- rechecked, not assumed). The cable can
be simulated; the device cannot be loaded.

So `roce/native/softverbs/softverbs.c` provides the device in userspace. It
implements exactly the `rdma_cma`/`ibverbs` calls `roce_transport.c` makes,
and `roce_transport.c` is linked against it **unmodified**. Each queue pair
carries its traffic over a TCP connection across the veth link, and the
emulator enforces Reliable Connected semantics strictly, so that typical
RDMA bugs fail here instead of silently working the way they would over a
plain socket:

| RC rule | What softverbs does |
|---|---|
| SEND completes only when the responder acknowledges it | completion is generated on the responder's ACK, not when bytes leave |
| No RECV posted when a SEND arrives (RNR) | responder NAKs; requester retries per its own `rnr_retry_count` (7 = forever, 0 = fail at once) then completes with `IBV_WC_RNR_RETRY_EXC_ERR` |
| SEND larger than the posted RECV buffer | responder: `IBV_WC_LOC_LEN_ERR`; requester: `IBV_WC_REM_INV_REQ_ERR` |
| Buffers must be registered | every sge's lkey and address range is checked against registered MRs |
| Queue depth | posting past `max_send_wr`/`max_recv_wr` returns `ENOMEM` |
| QP error / disconnect | all outstanding and new WRs complete with `IBV_WC_WR_FLUSH_ERR` |
| `rdma_connect` is asynchronous | a missing listener arrives as `RDMA_CM_EVENT_REJECTED`, not a return code |

`tests/test_softverbs_rc_semantics.py` checks the emulator in both
directions: the fixed transport passes, **and** three negative controls
fail the way hardware fails (a library deliberately built with the old
`rnr_retry_count=0` fails the pipelined test with "RNR retry counter
exceeded"; an oversized SEND gets "remote invalid request error"; a
connect to no listener gets `RDMA_CM_EVENT_REJECTED`).

On a machine that *can* do Soft-RoCE (the `rdma` tool, the `rdma_rxe`
module and rdma-core headers), the same command binds an `rdma_rxe` device
to each veth end, builds the real `libroce_transport.so` with
`roce/native/build.sh`, and runs the same phases on real kernel verbs over
the same cable (`--provider rxe` to require it). That branch is written but
untested here, since this environment has none of the three. The report's
`rdma_provider` field says which provider ran and, for softverbs, why the
real one wasn't available.

## Phases (all RoCETransport / RoCEStream, no TCP transport in the data path)

| Phase | Where | What is checked | Result |
|---|---|---|---|
| smoke | server nodeB, client nodeA | `roce/roce_transport_smoketest.py` unchanged: PUT, GET, 1 MB payload | PASS |
| pool | PoolNode on nodeA; two PoolNodes on nodeB | each of the two workload nodes writes 120 objects (0 B..256 KB, 12 MB each), 76 and 80 of them owned by the *other* side of the cable; byte-exact reads; stale-version CAS rejected by the remote owner; `list_prefix` fan-out = 121; delete | PASS |
| mpi | 4 ranks, 2 per node, alternating | barrier, bcast, gather, allgather, allreduce_sum (=10), ring send/recv 0→1→2→3→0 | PASS |
| hpc | 4 OST servers + 4 MPI ranks, both alternating nodeA/nodeB | Lustre, MPI-IO and MPI basics on one shared striped namespace -- 26 checks, listed below | PASS |
| stream | RoCEStream client nodeA, server nodeB | 4 PUT+GET pairs pipelined (0 B..300 KB), replies read back in 7-byte slices | PASS |

Stable across repeated runs. Timings in the report are **TCP-over-veth
timings of an emulator** and say nothing about RDMA performance.

## Bugs this found (all fixed, each with a regression test)

The first three were in code that had only ever been syntax/link-checked; the rest only showed up once keys, ranks and stripes genuinely lived on another machine:

1. **`RoCETransport.serve()` could not start at all.** ctypes rejects
   `POINTER(c_uint8)` as a callback return type ("invalid result type for
   callback function"). Also, `libc.malloc` was called without a declared
   `restype`, so ctypes would have truncated the 64-bit pointer to a C
   `int`. Fixed in `roce/roce_transport.py` (`c_void_p` return, `_libc()`
   with proper prototypes).
2. **RNR: `rnr_retry_count` was never set (defaults to 0).** Each
   connection has one RECV buffer, re-posted only after the previous
   message is consumed, so any pipelined second SEND fails with
   `IBV_WC_RNR_RETRY_EXC_ERR` and kills the QP. Fixed in `roce_transport.c`
   (`ROCE_RNR_RETRY_INFINITE`, 7, as rdma-core's `rping` uses).
3. **`poll_for_completion()` threw away completions.** Send and recv share
   one CQ; while waiting for a SEND completion it dequeued the reply's RECV
   completion and dropped it (`continue`), so the caller then waited
   forever. Can also hit plain request/response on hardware whenever the
   reply's completion is polled first. Fixed by stashing the other queue's
   completion (`roce_qp_resources.stashed`).
4. **`PoolNode`: remote CAS conflicts had a different exception type.**
   `put(expected_version=stale)` raised `VersionConflict` for a locally
   owned key but `RemoteError` for a remotely owned one; the striping
   layer's CAS retry loop catches `VersionConflict`, so it would have
   crashed instead of retrying against remote OSTs. Now
   `RemoteVersionConflict(VersionConflict, RemoteError)`. Not RDMA-specific
   -- it only surfaced once keys genuinely lived on another machine.
5. **MPI wire-up: first message to a not-yet-listening rank was lost.**
   Unit tests construct every rank before any runs; separate processes
   don't. Added an idempotent `MPI_PING` wire-up probe before the first
   send to each peer (retrying the real `MPI_SEND` could duplicate it).
6. **Large replies were copied byte-by-byte in Python** in the server
   callback (`(c_uint8 * n)(*response_bytes)`): GETs of 12 MB took 1.28 s
   vs 0.18 s for the PUTs. One `memmove` now; GETs 0.19 s.

7. **Striping lost data when concurrent writers shared a stripe.**
   `StripedStorage.write_range` read-modify-wrote each stripe object with
   a blind `put`, so the last writer silently erased the others' bytes in
   that stripe. The module had documented this as an accepted race, but
   POSIX/Lustre guarantee that concurrent writes to *disjoint* byte ranges
   all survive, and the standard N-1 MPI-IO patterns depend on it. Across
   the cable, M1 lost rank 1's entire 300 KiB segment and M3 lost 10 of 64
   blocks. Fixed with per-stripe compare-and-swap plus a consistent
   version/content snapshot. `tests/test_striping.py` now has 8 writers
   sharing every stripe; with the old blind write it loses 23 of 48 blocks,
   with the fix none.

## Phase `hpc`: Lustre, MPI-IO and MPI basics across the cable

```
 nodeA (10.99.0.1)                      nodeB (10.99.0.2)
   OST0 :19501   OST2 :19502              OST1 :19501   OST3 :19502
   rank0 :19601  rank2 :19602             rank1 :19601  rank3 :19602
```

Each OST is an ordinary single-node `PoolNode` server holding a
`MemoryEngine`. Every rank is a separate process that is both an MPI rank
(`Communicator` over RoCETransport) and a Lustre-style client: it mounts
the same striped namespace (`StripedStorage` over four
`roce.remote_engine.RemoteEngine` handles, one per OST), like a shared
Lustre mount on every compute node. With round-robin placement over
alternating OSTs, consecutive stripes of every file sit on different
sides of the cable. Every check runs on every rank that it applies to.

| Check | What it does | Result |
|---|---|---|
| L1 setstripe / getstripe | rank0 (nodeA) creates a 1 MiB file, `-c 4 -S 64K -i 0`; every client reads back the same layout and stripe map `[0,1,2,3]x4` | PASS |
| L1 cross-client coherence | all four clients (two on nodeB) read the file; sha256 matches | PASS |
| L2 placement on both nodes | each OST holds exactly 256 KiB of the file (OST0/2 on nodeA, OST1/3 on nodeB) | PASS |
| L3 `-c 2 -i 1` | rank1 (nodeB) creates a file striped over 2 OSTs from OST1: only OST1, OST2 used; OST0, OST3 hold nothing for it | PASS |
| L4 overwrite across a stripe boundary | 10 000 bytes straddling stripe 0 (OST0, nodeA) and stripe 1 (OST1, nodeB), written by rank2; range and whole-file reads on every client | PASS |
| L5 append | rank3 extends the file by 70 000 bytes; new size and content seen by all | PASS |
| L6 sparse file | write past EOF; size correct and the hole reads as zeros | PASS |
| L7 large file | 8 MiB, 1 MiB stripes, written from nodeB; sha256 on every client | PASS |
| L8 unlink | gone for every client; stripe objects freed on every OST | PASS |
| M1 N-1 segmented | `MPIFile.open` collective; each rank `set_view` to its own 300 KiB segment (not stripe-aligned, so neighbours share stripes); `write_at_all`, then `read_at_all` of the whole file on every rank | PASS (after fix 7) |
| M2 `get_size_all` | equals 4 x 300 KiB on every rank | PASS |
| M3 N-1 strided | 16 KiB blocks, 4 per 64 KiB stripe: all four ranks write inside every stripe concurrently | PASS (after fix 7) |
| M4 N-N | one file per rank; every rank reads every file | PASS |
| P1 ordering | 100 messages rank0 (nodeA) -> rank1 (nodeB), same tag, received in order | PASS |
| P2 tag matching | rank3 sends tag b then a; rank2 receives a first, then b | PASS |
| P3 bcast | 4 MiB from root 3 (nodeB) to all | PASS |
| P4 reductions / gathers | `reduce_sum` to root 1 with negatives and >32-bit values, `allreduce_sum`, `gather` of 0/1000/2000/3000-byte items, `allgather` | PASS |

## What this does not prove

It proves the *logic* of `roce_transport.c` and everything above it:
connection setup order, RECV-before-SEND discipline, completion handling,
buffer ownership across the C/Python boundary, teardown, and multi-node
behaviour of the pool and MPI layers across a real network link between
separate processes. It does **not** exercise a real RNIC, kernel verbs,
DMA/zero-copy, RoCEv2 encapsulation (UDP 4791), PFC/ECN, or RDMA latency and
throughput. The first step on real hardware is still
`docs/ROCE_DEPLOYMENT_NOTES.md`; after that, `tools/run_crossover_rdma.py`
runs the same phases against the real library automatically.
