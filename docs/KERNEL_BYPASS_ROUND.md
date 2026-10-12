# Interception library, NVMe tier, SPDK and P2PDMA

The kernel-bypass items from the projection in `docs/SCALE_OUT_ROUND.md`
("four items done, the model gives 5-6x: the interception library
contributes the most, NVMe is its prerequisite, SPDK and P2PDMA together
under 20%"), implemented in C. What runs here was run here (the emulated
2-core rig, the 3- and 10-node simulations); what needs an NVMe bound to
SPDK or a drive with a P2P-capable CMB compiles against the real headers
and is gated by a preflight on site, and is marked **unverified on
hardware** below.

## 1. The LD_PRELOAD interception library (`roce/native/rocepool_intercept.c`)

`librocepool_intercept.so`, preloaded into an unmodified program, serves
every libc file call on paths under `ROCEPOOL_PREFIX` from the native
client core in the program's own process: no mount, no /dev/fuse, no
kernel on the I/O path. This is the libioil / WEKA-client shape.

- **One client core, three front ends.** `rocepool_fuse.c` was split into
  `rocepool_fs.c/.h` (attribute cache, layouts, Data-on-MDT, striped
  directories, the registered write arena, handles, flush, close-to-open
  revalidation -- everything that is not FUSE) and the FUSE front end;
  the interception library is a second front end over the same core, so
  it has exactly the FUSE client's semantics (the tests check it against
  the Python client on the same servers both ways).
- **Entry points**: `open/openat/creat` (+`64` and the `_FORTIFY_SOURCE`
  `__open_2` family IOR compiles to), `close`, `read/write/pread/pwrite`
  (+`64`, `readv/writev`, `__read_chk/__pread_chk`), `lseek`,
  `fsync/fdatasync`, `ftruncate/truncate`, `dup/dup2/dup3`,
  `fcntl/fcntl64` (F_DUPFD, F_GETFL/F_SETFL), `stat/lstat/fstat/fstatat`
  (+`64`, `__xstat` family), `unlink/unlinkat`, `mkdir/mkdirat`, `rmdir`,
  `access/faccessat`, `rename` (EXDEV: the protocol has no rename),
  `chmod/chown/utimes/utimensat` (accepted), `realpath/__realpath_chk`,
  `statfs/statvfs`, `opendir/fdopendir/readdir(64)/rewinddir/dirfd/closedir`,
  and `fopen/fopen64/fdopen` (libc's own stdio cannot be interposed on
  the inside, so a stream on one of our files is built with `fopencookie`
  over our descriptor). Our descriptors are real ones (`/dev/null`,
  reserved) so they slot into the program's fd space; `fork` is detected by
  pid and the child re-opens its connections.
- **Paths outside the prefix are untouched**: only the prefix string is
  compared before anything else happens; the client core (connections,
  I/O threads) comes up at the first path under it, so a shell or mpirun
  carrying the environment is not affected.
- **Found by running real programs under it** (each fixed, each with a
  test): libc symbols may be called by another library's constructor
  before ours ran (libselinux under `nsenter`): every entry point resolves
  libc lazily; `strtok` in our init clobbered io500's own `strtok` loop
  (`u_create_dir_recursive`) -- never `strtok` in a preloaded library;
  `rewinddir` must re-list (pfind); a `stat` after `close` must see
  another rank's extension (IOR's aggregate-size check), so close drops
  the attribute-cache entry.
- **Close-to-open** as the FUSE client: open of a striped file revalidates
  its size; the attribute cache (TTL `ROCEPOOL_ATTR_TTL`, 3 s) is
  per process, so a rank's stats of its neighbour's files are RPCs where a
  FUSE mount shared by the node's ranks would answer them from the kernel
  cache (that is the one place FUSE measured faster, below).

Build: `roce/native/build_lib.py:ensure_intercept_lib` (emulator) and
`roce/native/build.sh` (real NICs) compile the client core with
`-fvisibility=hidden`, so only the libc entry points leave the .so.

Tests: `tests/test_intercept.py` (13): a Python host program (its own
descriptors, dlopen, importlib's stat storms), a C probe for the entry
points Python does not reach (`tests/fakes/intercept_probe.c`, compiled
with `-D_FORTIFY_SOURCE=2`), interop with the Python client both ways,
`fork`, and real IOR (shared file, file-per-process, the ior-hard shape)
and mdtest-hard through `mpirun` with the library in every rank.

### Measured here (2 cores, emulator, `tools/bench_ior.py --hard --client intercept`)

Hard pair, 4 ranks, `stress_reports/35` (FUSE, classic channel) and `36`:

| | FUSE client | interception | |
|---|---|---|---|
| ior-hard write | 155 MiB/s | **290 MiB/s** | 1.9x |
| ior-hard read | 238 MiB/s | **622 MiB/s** | 2.6x |
| mdtest-hard create | 3 503/s | **5 335/s** | 1.5x |
| mdtest-hard stat | **68 333/s** | 20 422/s | FUSE: the kernel cache answers the neighbour's files |
| mdtest-hard read | 10 155/s | 8 271/s | |
| mdtest-hard removal | 8 524/s | 9 534/s | |

3-node simulation, the client probe (`stress_reports/37`): FUSE classic
1.83, FUSE over io_uring 1.70, **interception 2.37** (bw 0.51 vs 0.35
GiB/s, md 11.1 vs 9.6 kIOPS). The probe chose it and the run used it.

### Automation

- `tools/io500_run.py --client auto` (default) probes FUSE over /dev/fuse,
  FUSE over io_uring (when the kernel offers it) and the interception
  library with a short official run each, and uses the fastest; the ranks
  get the library through `mpiexec -genv LD_PRELOAD ...`, nothing is
  mounted, the report records all probe results (`client_probe`) and the
  choice (`client`). `--client fuse|intercept` forces one.
  `tools/roce_onsite.py` carries the choice into the valid run.
- `tools/bench_ior.py --backend dist --client intercept` for the local
  hard pair.
- The model (`tools/io500_model.py`): a run with the interception client
  has no FUSE-channel or per-node inode-lock limit, and the ranks are the
  client threads; the `intercept` scenario adds that to a FUSE run's
  projection (a no-op on an interception run).

## 2. The OST's capacity tier (`roce/native/ost_tier.c`), SPDK, P2PDMA

The reason the bandwidth half of the score trailed WEKA was capacity: a
valid 300 s write phase had to fit in the pool's RAM. The OST now keeps its
in-memory index but writes every object through to a tier and holds a
bounded RAM cache (`ROCEPOOL_OST_RAM_CACHE_MIB`, LRU) of copies.

- **Slabs**: size classes 4 KiB .. 16 MiB carved from 64 MiB zones, a free
  list per class; one object = one slab; a partial write rewrites the
  touched range from the object's RAM copy; a grown object moves to a
  bigger slab; delete frees the slab.
- **Direct path**: the transport now lands a request's bulk (a PUT's or
  WRITE_AT's data) on a page boundary in the server's registered buffer,
  so a whole-stripe write goes from the NIC's buffer to the device as it
  is -- `O_DIRECT` with the kernel backend, an NVMe command with SPDK --
  and no RAM copy is kept (`direct_writes` in STATS). Unaligned writes
  (ior-hard's 47 008-byte pieces) take the RAM path: read the object back
  if evicted, patch, write the touched LBAs. Reads of an evicted object go
  from the device straight into the reply buffer the NIC sends.
- **Backends** (`ROCEPOOL_OST_TIER`): `file:PATH[:GIB]` -- a file (grown
  to the size) or a block device through the kernel; **run here** (the
  parity scenario against the Python reference server with a RAM budget of
  0 and of 1 MiB, the whole FUSE suite on the tier, a 3-node IO500).
  `spdk:TRADDR[:NSID]` -- SPDK's userspace NVMe driver, a polled queue
  pair per server thread, `spdk_dma_malloc` RAM copies and the transport's
  buffer chunks registered with `spdk_mem_register` so NVMe DMAs straight
  from/to the NIC's buffers; built in with `SPDK_DIR=<built spdk>
  roce/native/build.sh`, compiled against SPDK v25.05's headers here,
  **unverified on hardware** (needs a drive bound to vfio-pci and
  hugepages).
- **P2PDMA** (`ROCEPOOL_P2PMEM=<sysfs p2pmem dir>`): the transport's
  server buffer chunks are allocated by mmap of the NVMe's
  `p2pmem/allocate` (Linux 6.2+, CONFIG_PCI_P2PDMA, a drive that publishes
  its CMB) and registered with the NIC; a write's bulk then lands in the
  drive's own memory and the direct `O_DIRECT` write from that buffer is a
  DMA inside the drive -- host memory never sees the bytes. If
  `ibv_reg_mr` refuses P2P pages (a NIC driver without
  FOLL_PCI_P2PDMA support), the transport says so once and uses host
  memory. **Unverified on hardware**; `tools/nvme_preflight.py` decides
  from sysfs whether to enable it (kernel >= 6.2, mlx5 NIC, a published
  CMB, NIC and drive under the same root port).

`tools/nvme_preflight.py` (per host, sysfs only, nothing touched): NVMe
controllers, CMB and p2pmem, RDMA NICs and their PCIe relationship to each
drive, NVMe-backed mounts and free space; recommends `ROCEPOOL_OST_TIER`
(a file on the NVMe-backed file system with the most room, sized for the
node's OSTs; a raw block device is never chosen by itself) and
`ROCEPOOL_P2PMEM`. `tools/io500_run.py --ost-tier auto` (default) runs it
on every real host and starts each OST with what its host offers;
`tools/roce_onsite.py` has an `nvme-preflight` step; the model projects a
tiered run with the drives' capacity and write bandwidth
(`nvme_gib`, `nvme_gib_s` in the profile; 'auto' profiles take the hosts'
real namespaces) instead of RAM.

Tests: `tests/test_ost_tier.py` (7), `tests/test_native_fuse.py`
`NativeFuseClientTieredTests` (28, the FUSE suite on the tier),
`tests/test_nvme_preflight.py` (6, fake sysfs trees: same/different root
port, old kernel, non-mlx5 driver, unpublished CMB, no room).

### Measured here

3-node simulation, interception client, OSTs on 2 GiB tier files with a
16 MiB RAM cache each (`stress_reports/38`): every phase completes, 80
direct writes per OST (every ior-easy stripe), 21 k reads served from the
tier, the RAM cache held at its budget. The score (2.86) is within noise
of the RAM-only run (2.37-2.69) because the emulator, not the disk, bounds
this rig; the projection's write limits move from "RAM for a 300 s write
phase" to the drives' write bandwidth.

## 3. The 10-node simulation, rerun (`stress_reports/39`)

`tools/io500_run.py --nodes 10`, defaults: the client probe chose the
interception library (FUSE classic 1.54, FUSE over io_uring 1.56,
interception 2.11), the OSTs in RAM (the simulated nodes share this host's
disk; the tier run is section 2's), projected onto 10 x (32 cores, 100 GbE,
512 GiB):

| | last round, classic FUSE (`31`) | last round, io_uring (`30`) | this round, interception (`39`) |
|---|---|---|---|
| measured on the rig | 1.68 | 1.68 | **3.02** (bw 0.53 GiB/s, md 17.4 kIOPS) |
| projected bandwidth | 12.8 GiB/s | 15.4 GiB/s | 22.1 GiB/s |
| projected metadata | 757 kIOPS | 1 534 kIOPS | 6 370 kIOPS (see below) |
| **projected score** | **98.2** (64-154) | 153.5 | **375** (241-462) |
| + NVMe tier (model) | 280-300 | | **687** (bw 74 GiB/s) |
| vs WEKA Afton 105.94 | 1.08x behind | | 3.5x ahead; 6.5x with the tier |

Bottlenecks of the interception run: ior writes -- RAM for a 300 s phase
(the tier removes it; then the drives' write bandwidth, 5 GiB/s per node
assumed); ior reads -- the NIC; every mdtest phase -- "per-rank latency"
(320 ranks, one RPC in flight each, 6 us round trip plus the measured CPU
per op: 19 us in the rank, 2 us in the MDT handler, 4 us of transport);
find -- CPU.

What the metadata number means: with the kernel and FUSE channel gone, no
*measured* cost caps mdtest below several million IOPS; the model's next
limits are the per-rank round trip and the cluster's CPU, both of which it
reproduces on the rig within 0.96-1.07x but has never seen under 320 ranks
on real NICs (queueing at a busy MDT, cache misses at scale). Read it as
"the metadata half is no longer where the score is decided, and the
on-site probe will say how far it really goes", not as 6 million IOPS.
The bandwidth half is the one to quote: 22 GiB/s in RAM, and with the tier
whatever the drives deliver (the model's 74 GiB/s assumes 5 GiB/s per node;
WEKA Afton did 33).

Two things the rerun found and fixed on the way: the 20 preloaded ranks
on this host's two cores stalled an MDT reply past the client's 5 s RPC
timeout, and IOR aborted on the failed close -- the timeout is now a
setting (`ROCEPOOL_RPC_TIMEOUT_MS`, the simulation raises it); and every
rank's 64-stripe read cache (64 MiB) pushed the 20 ranks over the
container's 6 GiB cgroup limit (OOM-killed an OST) -- the cache size is a
setting too (`ROCEPOOL_SCACHE_SLOTS`, readahead capped by it, the
simulation uses 8) and the simulation's memory budget now reads the
cgroup limit, not MemTotal.

## What is still estimated

- FUSE over io_uring's per-mount rate (x3) and the interception client's
  per-request cost on 32-core hosts: the probe measures both on site.
- NVMe write bandwidth per node (5 GiB/s, `nvme_gib_s`, in the
  sensitivity band 3-8) until a tiered run on real drives measures it.
- SPDK and P2P: compiled, gated, not run. The P2P path additionally needs
  the platform to allow P2P DMA between the NIC's and the drive's root
  port; the kernel refuses the mmap or the registration otherwise and the
  transport falls back, so a wrong guess costs nothing but the fallback
  message in the OST's log.
