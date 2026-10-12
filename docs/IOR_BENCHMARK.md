# IOR/mdtest benchmark: `tools/bench_ior.py`

One command, needs root, no manual steps:

```bash
python3 tools/bench_ior.py                    # --backend local (default): single-node MemoryEngine
python3 tools/bench_ior.py --backend pool      # 3-node PoolNode over RoCETransport, crossover cable
python3 tools/bench_ior.py --quick             # smaller matrix, for a fast check
```

It builds MPICH and IOR/mdtest from real, unmodified upstream source
(https://github.com/pmodels/mpich, https://github.com/hpc/ior) if this
machine has neither, caching the build under `.bench_cache/` so a second
run is instant. It writes `ior_bench_report.json` and prints one PASS/FAIL
line per case, plus mdtest's operation rates.

IOR and mdtest are the same real upstream binaries the IO500 suite and
vendor comparisons (Lustre, GPFS, WEKA...) use -- nothing about them is
written for or tuned to this project. They talk POSIX to whatever is
mounted at the mountpoint; this project's own code is only the FUSE driver
underneath (`fs/fuse_lowlevel.py`, `fs/posix_api.py`).

## The two backends

- `--backend local`: `PooledFileSystem(MemoryEngine())`, single node, no
  network in the path. This is IOR/mdtest exercising the FUSE driver alone.
- `--backend pool`: a genuine 3-node `PoolNode` cluster over `RoCETransport`
  (softverbs, see `docs/CROSSOVER_RDMA_TEST.md`) across the crossover-cable
  veth link, with one node mounted as a real kernel FUSE filesystem so
  every POSIX call that lands on a key owned by the other two nodes
  genuinely crosses the cable. `-F` (file-per-process) IOR cases fan out
  across the cluster by key hash; the default single-shared-file pattern
  does not parallelize this way (there's no striping under a flat
  `PoolNode`, only under `fs/striping.py`, which isn't POSIX-mounted here).

## Matrix (`--quick` runs the first three plus mdtest)

| Case | Pattern | Block / xfer | Tasks |
|---|---|---|---|
| shared_1m | single shared file | 4 MiB / 1 MiB | 2 |
| shared_64k | single shared file | 2 MiB / 64 KiB | 2 |
| fpp_1m | file per process | 2 MiB / 1 MiB | 4 |
| shared_4m_large | single shared file | 32 MiB / 4 MiB | 2 |
| fpp_256k | file per process | 4 MiB / 256 KiB | 4 |

Every case runs with `-W -R -G 42` -- IOR verifies every byte it reads
back against what it wrote, not just the aggregate size.

## Results (stable across repeated runs; local host, softverbs emulator)

| Case | local write / read | pool write / read |
|---|---|---|
| shared_1m | 64 MiB/s / 1200 MiB/s | 60-68 MiB/s / 400-800 MiB/s |
| shared_64k | 80-230 MiB/s / 1250-1400 MiB/s | 33-43 MiB/s / 100-170 MiB/s |
| fpp_1m | 108-170 MiB/s / 1400-1650 MiB/s | 45-60 MiB/s / 150-400 MiB/s |
| shared_4m_large | 44-70 MiB/s / 530 MiB/s | 8.5 MiB/s / 72-75 MiB/s |
| fpp_256k | 130-170 MiB/s / 1870-1970 MiB/s | 56-62 MiB/s / 200-350 MiB/s |
| mdtest (create/stat/read/remove) | 8.8-10k / 1.3M / 25-26k / 29-31k ops/s | 91-95 / 121-135 / 88-97 / 88-95 ops/s |

`--backend local` numbers are TCP-loopback-fast (in-process, no real
network); `--backend pool` numbers are genuinely crossing the cable through
the softverbs emulator and, like the rest of this test rig, say nothing
about real RDMA hardware performance (see `docs/CROSSOVER_RDMA_TEST.md`'s
"What this does not prove"). The gap between them -- especially mdtest's
~100x drop -- is the real cost of one network round-trip per metadata
operation under `PoolNode`'s current per-request protocol, not a defect in
IOR or in this harness; it's exactly the kind of number this benchmark
exists to surface.

## Bugs this found (all fixed, each with a regression test)

Running a real, unmodified benchmark against the real kernel FUSE mount
surfaced defects that unit tests never would, because they only show up at
sizes and access patterns the project's own tests hadn't tried:

1. **Whole-file-buffer flush lost updates.** `_FileHandle` used to buffer an
   entire client-side snapshot per `open()` and flush with a full-object
   overwrite; two independently `open()`'d writers (IOR's 2 ranks, each
   filling a disjoint half of one shared file) would each flush their own
   zero-padded partial snapshot, and whichever flushed last erased the
   other's data. Reproduced live: `Stat() of aggregate file size` came back
   at half the expected value, then a later run hit a premature EOF on
   read. Fixed with `PooledFileSystem.write_range()` (CAS-protected partial
   write, `fs/posix_api.py`) and `_FileHandle.dirty_ranges` replacing the
   blind whole-buffer flush (`fs/fuse_lowlevel.py`). Same class of bug,
   same fix shape, as `fs/striping.py`'s `write_range` (found first there,
   via MPI-IO -- see `docs/CROSSOVER_RDMA_TEST.md`, bug 7).
2. **Quadratic flush after the fix above.** The kernel splits any write
   bigger than FUSE's `max_write` (128 KiB) into many small `FUSE_WRITE`
   requests, so a multi-megabyte IOR transfer arrives as hundreds of
   fragments. Replaying each one individually through `write_range` (a full
   object read-modify-write) made a large sequential write
   O(fragments x file_size): a 64 MiB single-shared-file case pegged a CPU
   core and never finished. Fixed by coalescing adjacent/overlapping
   fragments in `_flush_handle` before replaying them, cut to O(1) calls
   for the common contiguous-write case.
3. **Coalesced writes then exceeded RoCETransport's frame cap.** Once
   fragments were coalesced, a single `write_range` call could carry the
   whole of a large rank's block (up to 32 MiB) in one shot -- and
   `PoolNode`'s put is a whole-object wire call with no chunking of its own,
   so it collided with `RoCETransport`'s `max_frame_bytes` (16 MiB
   default): `request_len ... exceeds max_frame_bytes`. This is a real
   ceiling, not a bug to code around by shrinking the write further --
   `write_range` always resends the *entire current object*, so any
   single flat (non-striped) file bigger than the frame cap literally can't
   be stored over this path. Fixed two ways: capped each coalesced run at
   4 MiB (`_MAX_COALESCED_RUN`, keeps the O(1)-ish benefit while bounding
   any one call) and made `RoCETransport`'s frame cap configurable via
   `ROCE_MAX_FRAME_BYTES` (`roce/roce_transport.py`), which
   `tools/bench_ior.py`'s pool backend now sets to 128 MiB on both ends of
   every connection so this benchmark's largest case (64 MiB aggregate)
   fits.
4. **The mount daemon's stdout was a silently blockable pipe.** With no
   listener draining it, `subprocess.Popen(..., stdout=PIPE)` for the pool
   mount daemon can fill its OS pipe buffer and block the *daemon* mid
   FUSE-request the moment it prints enough (e.g. the traceback from bug 3,
   repeated) -- which looks exactly like IOR hanging, with no error
   anywhere. This is how bug 3 was hard to diagnose at first. Fixed by
   redirecting the daemon's stdout to a log file instead
   (`.bench_cache/pool_mounter.log`); `bench_ior.py` now prints its tail
   automatically whenever a case fails.
5. **IOR's own report table was parsed against the wrong section.** IOR
   prints two tables that both have lines starting with `write`/`read`: the
   per-test Results table (`bw(MiB/s)`, `IOPS`, ...) and a later "Summary of
   all tests" table (`Max(MiB)`, `Min(MiB)`, ...) -- entirely different
   columns under the same first word. Scanning past the summary let it
   silently overwrite the real IOPS with `Max(MiB)`/`Min(MiB)` (identical
   to bandwidth for one repetition, which is exactly why `write_iops` and
   `write_bw_mib_s` first came out equal for every case and nothing looked
   obviously wrong). Fixed by stopping the scan at "Summary of all tests".

All five were found by running the harness itself against a real kernel
FUSE mount and reading its output, not by inspection -- the same "test for
real, then fix" pattern as the rest of this project's testing.
