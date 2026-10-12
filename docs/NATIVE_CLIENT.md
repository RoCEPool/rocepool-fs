# Native C client (`roce/native/rocepool_fuse.c`)

The "C treatment" for the client side: a kernel FUSE daemon in C that
replaces the Python pair `fs/fuse_lowlevel.py` + `fs/posix_api.py`
(`DistributedFileSystem`) on the data and metadata path. Servers were
already native (tier C, `docs/ABC_UPGRADE.md`); after that the single
Python FUSE client was the bottleneck.

```bash
roce/native/rocepool-fuse --mount /mnt/rocepool --mdts h1:19901,h2:19902 --osts h1:19951,...
tools/bench_ior.py --backend dist --client native     # default; --client python for the old one
```

## What it is

*(Since the kernel-bypass round, `docs/KERNEL_BYPASS_ROUND.md`, the
client core -- attribute cache, layouts, Data-on-MDT, striped directories,
the registered write arena, handles and flush -- lives in
`roce/native/rocepool_fs.c/.h`; `rocepool_fuse.c` is the FUSE front end over
it and `rocepool_intercept.c` the LD_PRELOAD front end, with no mount.)*

- Raw `/dev/fuse` protocol from `<linux/fuse.h>` (no libfuse), N worker
  threads, no GIL; RPCs straight into `roce_transport.c` with a per-peer
  pooled-connection cache and the same retry rule as the Python client
  (re-send only when the request provably never left the host).
- **Wire- and layout-compatible with the Python client**: same frames,
  MDT/OST encodings, sha256 MDT placement and stripe offsets, stripe keys.
  Files written by either client read correctly through the other (tested
  both ways).
- Same semantics: TTL attribute cache incl. negative entries, blind PUT for
  whole stripes, per-handle stripe read cache with readahead,
  close-to-open consistency. Since the "thick KV" work
  (`docs/THICK_KV.md`): a partial stripe is one atomic `WRITE_AT` (was a
  CAS read-modify-write), random/strided reads use exact-range `READ_AT`,
  small files live inline in the MDT entry, truncate/unlink send one
  `BATCH` per OST. The OST RPC code is shared with the KV API in
  `rocepool_client.c`.
- Better than the Python daemon: 1 MiB kernel requests (FUSE_MAX_PAGES)
  instead of 128 KiB; a directory is listed once per opendir, not once per
  READDIR chunk; buffered writes capped at 64 MiB per handle; `fsync`
  flushes; the inode table is reference-counted (FORGET), so it does not grow
  with every path ever seen.
- Built with the emulator (`build_lib.ensure_fuse_client`) and against real
  rdma-core by `roce/native/build.sh` (`-Werror` clean against v59.2).
  `tools/roce_onsite.py` mounts it for the on-site IOR/mdtest step.

## Results (same 2-core rig, IOR/mdtest through the kernel mount)

| | Python client | **C client** |
|---|---|---|
| IO500-style score, standard matrix | 1.87 | **3.50** |
| IO500-style score, 4x MPI ranks | 1.47 | **4.13** (2.8x) |
| IOR bandwidth geomean, 4x ranks | 0.171 GiB/s | **0.448 GiB/s** (2.6x) |
| IOR writes, 4x ranks | 64-111 MiB/s | **229-570 MiB/s** (3.6-5.8x) |
| IOR reads, 4x ranks | 229-549 MiB/s | 302-770 MiB/s (1.3-1.6x) |
| mdtest create / read / remove, 4x ranks | 2 861 / 13 614 / 5 704 /s | **4 997 / 55 402 / 9 853 /s** |

(`stress_reports/21-23_*`.) The C client stops scaling at ~0.45 GiB/s here
because two cores now run 6 servers, the client, 8-16 MPI ranks and the
TCP-based RDMA emulator at once; the Python client was capped by its own GIL
well before that. mdtest "stat" is omitted: it is served from caches and the
figure is timer noise.

## Bugs found while building it (all fixed, each with a regression test)

| | bug | found by |
|---|---|---|
| 1 | a flush that failed part-way discarded the buffered writes it had not sent; `close()` then reported success | independent review |
| 2 | `fsync` returned success without writing anything | independent review |
| 3 | `ftruncate` left the handle's cached stripes, so a read after shrink+grow returned old bytes instead of zeros | independent review |
| 4 | a failed unlink (e.g. RPC timeout) cached the path as missing; `O_CREAT\|O_EXCL` then opened the existing file | independent review |
| 5 | inode table grew without bound (FORGET ignored) -- also true of the Python daemon | independent review |
| 6 | with concurrent writers, an older `set_size` reply landing after a newer one shrank the cached size: stat/read saw a truncated file (IOR 8 ranks, "read returned EOF prematurely"). **Also in the Python client** (`fs/attr_cache.py`), hidden by its slower timing; fixed in both | IOR at 4x ranks |
| 7 | softverbs accept path ignored a failed `rdma_create_id` (uninitialised pointer) | compiler warning in the new build |

Tests: `tests/test_native_fuse.py` (23 incl. the thick-KV ones: POSIX behaviour, multi-stripe
integrity, sparse/partial/truncate/append, 1 500-entry directory, concurrent
writers sharing stripes, stripes on every OST, interop both ways, the
regressions above).
