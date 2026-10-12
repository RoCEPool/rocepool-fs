# One-sided reads and lookups; thread-per-core and NUMA placement

The two items from the Rapids-SE design comparison that pay most on real
hardware (`docs/KERNEL_BYPASS_ROUND.md` left them open): a read fast path
that keeps the server's CPU out of reads, and placement that lets 96-core
nodes be 96 cores.

## 1. One-sided reads of stripes (OST `LEASE` + client RDMA READ)

**Before.** Every read was an RPC: the OST thread woke, found the object,
built a reply in a registered buffer and RDMA WROTE it; its CPU paid
~2 us of handler plus ~4 us of transport per request and a memcpy per
byte. ior-hard-read's 47 008-byte pieces meant 22 RPCs per stripe.

**Now** (`roce/native/rocepool_servers.c`, `rocepool_client.c`,
`rocepool_fs.c`, `roce_transport.c`):

- The OST keeps every object's RAM copy in a **registered arena**
  (64 MiB chunks, size-class slabs) laid out as
  `[header: seq, keyhash, size][data][trailer: seq]` -- a **seqlock the
  NIC reads**. Writers set `seq` odd before touching an object and to a
  fresh even number after; freeing leaves it odd and the memory
  registered, so a stale reader finds an invalid header, never someone
  else's bytes.
- `LEASE` (op 44) returns the descriptor (`addr, rkey, seq, keyhash,
  size, version`); the client caches one per stripe key (direct-mapped,
  64 K entries) and reads with **one chain of three RDMA READs** --
  header, the byte range, trailer -- executed in order on the RC queue
  pair and completed as one. The bytes count only if
  `header.seq == trailer == lease.seq` (even), `keyhash` and `size`
  match; otherwise the lease is dropped, taken once more, and failing
  that the read goes by RPC. The transport gained
  `roce_client_rdma_read` (up to 3 ranges, in order) and
  `roce_server_mem_rkey`.
- With the NVMe tier an evicted object is made resident by the LEASE;
  eviction invalidates its seqlock (the test drives this).
- The Python reference OST has no LEASE: the client remembers that per
  server and reads by RPC. `ROCEPOOL_DIRECT_READ=0` turns the path off.

## 2. One-sided lookups (the MDT's slot table)

The MDT keeps a **registered open-addressing table** keyed by the hash of
the full path: 256-byte slots, each the entry's wire encoding under the
same seqlock (0 = empty, odd = being written or deleted, even = valid;
trailer repeats the seq), published on every create / set_size /
write_inline / freeze / spill_commit / unlink while the entry's bucket
lock is still held. A client hashes the path, RDMA READs 8 consecutive
slots (2 KiB) and takes a valid matching slot as the answer; an empty
slot in the run means ENOENT; a tombstone, a slot mid-write or a full
run sends it to the RPC. Fixed size (`ROCEPOOL_MDT_SLOTS`, default 1 M =
256 MiB per MDT); when a run is full the entry is simply not published
(`slots_unpublished` in STATS). Deleted entries keep their slot as a
tombstone keyed by the path, so mdtest's create/delete cycles reuse it.

`stat`, `exists`, `open`'s revalidation and every path walk use it;
reads of inline files still carry their bytes in one RPC as before.

## 3. Thread-per-core and NUMA placement

- `ROCEPOOL_NUMA_NODE=<n>`: a server pins itself to node n's cores
  (`sched_setaffinity`) and memory (`set_mempolicy(MPOL_PREFERRED)` -- was MPOL_BIND, which let one full node OOM-kill a server --, no
  libnuma): store, arena, slot table, buffer pools, every thread.
- Every server connection thread is pinned to one of the allowed cores,
  round-robin (`ROCE_PIN_THREADS=0` off): its CQ, SRQ buffers and the
  handler's working set stay on one core.
- `tools/io500_run.py` on real hosts: one MDT and one OST per NUMA node
  (`numa_nodes` from `procstat --facts`), each pinned to its node;
  MPI ranks bound to NUMA (`-bind-to numa` when hydra has hwloc);
  `--mdts-per-node/--osts-per-node/--bind-ranks` override.

## Measured here (2-core emulated rig)

The emulator serves an RDMA READ with the same TCP work as an RPC, so the
server-CPU saving cannot show; what does show is the round trips saved:

| hard pair, 4 ranks (`tools/bench_ior.py --hard --client intercept`) | RPC reads | one-sided |
|---|---|---|
| mdtest-hard stat | 24 159/s | **58 699/s** (2.4x) |
| ior-hard read | 767 MiB/s | 754 MiB/s (same) |

3-node simulation, the client probe (`stress_reports/40`): interception
with one-sided reads **3.01** vs RPC reads 2.55 (md 16.7 vs 11.4 kIOPS);
the full run measured 3.57 (last round 2.69).

10-node simulation (`stress_reports/41`, defaults; the probe chose the
interception client with one-sided reads: 2.79 vs 2.34 with RPC reads):

| | last round (`39`) | this round (`41`) |
|---|---|---|
| measured on the rig | 3.02 (md 17.4 kIOPS) | **3.77** (md 25.1 kIOPS) |
| mdtest-easy-stat / hard-stat measured | 25.0 / 23.5 kIOPS | **74.8 / 57.8 kIOPS** (3.0x / 2.5x) |
| projected score, 10 x 32c/100 GbE | 375 (241-462) | **449** (277-546) |
| + NVMe tier (model) | 687 | 761 |

The stat phases now show `rpc/op 0.00`: no request reaches an MDT thread.
Their projection is bounded by the ranks' own CPU (the client core's
16 us per lookup) -- the only cost left -- which is the honest reading of
"the server is out of the path".

## What the real hardware adds that the rig cannot show

- Every one-sided read costs the OST **zero CPU**; the 1 MiB memcpy per
  stripe read and the ~6 us of handler + transport per request were the
  OST's whole cost in the read phases. On 10 x 32 cores that CPU goes to
  writes and metadata instead.
- A one-sided stat is one NIC round trip (~2 us on a real fabric)
  instead of an RPC through a server thread (~6 us plus queueing); with
  85 ranks per node hammering 10 MDTs, the queueing was the part the
  model could not see.
- NUMA pinning: the rig has one node and two cores; on a 2-socket
  96-core host unpinned servers pay cross-socket memory traffic and lock
  cache-line bouncing on every request (typically 20-40% in this kind of
  server). The probe measures it: `--bind-ranks none` and
  `ROCE_PIN_THREADS=0` give the A/B.

## Tests

- `tests/test_direct_read.py` (6): reads go one-sided (counters on both
  ends); a long-lived reader's leases are invalidated by rewrites,
  partial writes, truncation, growth into a bigger slab, eviction to the
  tier, delete + recreate -- it never sees old bytes; the off switch;
  the Python OST read by RPC. Lookups: 50 stats with 0-2 MDT RPCs; an
  observer sees every change (resize, spill, truncate, delete, recreate);
  the Python MDT.
- `tests/test_numa_pin.py` (3): process pinned to the node's cores and
  memory; connection threads one per core, spread; off switch; a missing
  node ignored with a message.
- The full suite (379) passes; the FUSE and interception suites run on
  the one-sided path by default.
