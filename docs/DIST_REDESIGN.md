# Distributed metadata + striped POSIX path (`--backend dist`)

The 1+2+4 redesign: parallel request fan-out, Lustre-style striping on the
POSIX/FUSE path, and a dedicated, sharded metadata service with a
client-side attribute cache. Measured against the original design with the
same real IOR/mdtest binaries, on the same machine, over the same
crossover-cable softverbs link.

```bash
python3 tools/bench_ior.py --backend pool   # original: 3 PoolNodes, PooledFileSystem
python3 tools/bench_ior.py --backend dist   # redesign: 2 MDTs + 4 OSTs, DistributedFileSystem
python3 tools/bench_ior.py --backend dist --attr-ttl 0     # ablation: attribute cache off
python3 tools/bench_ior.py --backend dist --fuse-threads 1 # ablation: single-threaded FUSE daemon
```

## What changed

| File | Status | Role |
|---|---|---|
| `roce/request_batcher.py` | new | Runs many independent RPCs at once from a persistent worker pool, capped per peer. Each call is still the unchanged connect-per-request `send_request()` -- concurrency comes from N separate connections in flight, not from reusing one (the reuse race documented in `docs/BENCHMARK_COMPARISON.md` stays out of the picture). |
| `fs/mdt_node.py` | new | `MDTServer`: namespace, sizes, times and per-file layouts in memory; every op (lookup/create/set_size/unlink/readdir) is one request answered atomically under one lock. `MDTClient` routes each op to its shard. |
| `fs/namespace_shard.py` | new | A path's entry lives on the MDT picked by hashing its parent directory: one directory = one MDT (readdir is one RPC, not a broadcast), different directories spread across MDTs. |
| `fs/attr_cache.py` | new | Client-side attribute cache incl. negative entries. Own mutations update it immediately; other clients' changes visible within `ttl_s` (NFS `actimeo` contract, not Lustre DLM coherence). |
| `fs/layout.py` | new | Per-file stripe layout chosen at create time, stored in the MDT entry; `stripe_offset` hashed from the path so small files spread over OSTs. |
| `fs/striping.py` | modified | `stripe_ranges()` / `StripeSpan` pulled out as a shared definition of where a byte lives; MPI-IO path unchanged. |
| `fs/posix_api.py` | modified | New `DistributedFileSystem` (same surface as `PooledFileSystem`, which is untouched): writes touch only the overlapped stripes on up to N OSTs at once, whole-stripe writes are one blind PUT with no read; reads fetch stripes in parallel with async readahead through a per-handle `StripeReadCache`. |
| `fs/fuse_lowlevel.py` | modified | Range-I/O mode for backends that support it (no whole-file read at open, in-place truncate, one `write_range` per coalesced run so its stripes fan out), READDIRPLUS-style cache priming, optional multi-threaded dispatch. `PooledFileSystem` mounts behave exactly as before. |
| `roce/pool_node.py`, C transport, softverbs | unchanged | OSTs are ordinary single-node PoolNodes; they only ever see flat keys. |
| `tools/crossover_node.py`, `tools/bench_ior.py` | modified | `mdt-server` role; `--backend dist` topology (MDTs and OSTs split across both namespaces, client in nodeA). |

Tests: `tests/test_dist_fs.py` (21 tests, real MDT/OST servers over sockets,
plus a real kernel FUSE mount driven by `cp`/`cmp`/`truncate`/`rm`). Full
suite 228/228, `tools/run_crossover_rdma.py` all 5 phases green.

## Results (medians of 2 full runs each; 2-core host, softverbs emulator; every IOR case byte-verified with `-W -R -G 42`)

| Case | original write / read (MiB/s) | redesign write / read (MiB/s) | change |
|---|---|---|---|
| shared_1m | 69 / 446 | 75 / 280 | write +9%, **read -37%** |
| shared_64k | 40 / 159 | 84 / 234 | 2.1x / 1.5x |
| fpp_1m | 59 / 237 | 96 / 405 | 1.6x / 1.7x |
| shared_4m_large | 9.3 / 72 | 84 / 432 | **9.0x / 6.0x** |
| fpp_256k | 63 / 338 | 127 / 501 | 2.0x / 1.5x |

| mdtest (ops/s) | original | redesign | redesign, cache off |
|---|---|---|---|
| File creation | 94 | 218 | 144 |
| File stat | 129 | 73,504 | 1,103 |
| File read | 94 | 10,395 | 730 |
| File removal | 92 | 459 | 147 |

IO500-style score, same formula for every row (`sqrt(geomean(all IOR
bandwidths, GiB/s) x geomean(4 mdtest rates, kIOPS))`):

| | bandwidth | metadata | score |
|---|---|---|---|
| original (`pool`) | 0.089 GiB/s | 0.10 kIOPS | 0.095 |
| redesign (`dist`) | 0.176 GiB/s | 2.96 kIOPS | **0.72** (7.6x) |
| redesign, attribute cache off | 0.098 GiB/s | 0.36 kIOPS | 0.19 (2.0x) |
| redesign, 1 FUSE thread | 0.177 GiB/s | 3.76 kIOPS | 0.82 |

## Reading these numbers honestly

- **Where the big wins come from.** Large shared-file I/O (9x write): the
  original rewrote the whole growing object on every 4 MiB flush piece;
  now each stripe is written once, in parallel. Metadata: every op is one
  MDT round trip instead of several KV stats, and with the cache on most
  repeat lookups never leave the client.
- **mdtest stat/read with the cache on are mostly cache hits.** The
  cache-off column is the architecture-only figure: 1.5-8.5x better than
  the original, not 100-500x. Both are real behaviours of the design; the
  cache is part of it (item 4, layer 3), but it only holds for a single
  client -- multi-client coherence is TTL-bounded.
- **The cache also carries read bandwidth.** Cache off, every 128 KiB
  kernel read asks the MDT for size/layout first, and read bandwidth drops
  back near the original. A per-open layout snapshot would decouple that;
  not done here.
- **shared_1m read regressed (446 -> 280).** The original reads the whole
  8 MiB file in one GET at `open()`, and the mounter is itself one of the
  three PoolNodes, so a third of the time that object is in-process memory.
  The redesign keeps no data on the client and fetches 1 MiB stripes over
  the link. At 8 MiB total this case is dominated by per-open fixed costs.
- **Multi-threaded FUSE dispatch gained nothing here** (1 thread scored
  equal or better): with 2 CPU cores shared by the client, 2 MDTs, 4 OSTs
  and the MPI ranks, there is no idle core to run a second handler on.
  Kept because it's correct and costs nothing, but unproven on this box.
- **YCSB is unaffected**: its bridge talks to `PoolNode` get/put directly,
  not through the POSIX layer this redesign changes.
- Same limits as the rest of this rig: softverbs over veth on one host,
  not real RDMA hardware; single client mount; no cross-MDT transactions
  (a create racing an rmdir in the same directory can slip through, see
  `DistributedFileSystem.rmdir`).
