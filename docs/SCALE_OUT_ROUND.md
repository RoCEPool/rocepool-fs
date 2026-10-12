# Striped directories, zero-copy RDMA bulk, FUSE over io_uring

The three changes the 10-node bottleneck analysis (`docs/IO500_CLUSTER.md`)
asked for, each with its regression tests, and the 10-node simulation
rerun with all three in place.

## 1. Striped directories and finer MDT locks

**Before.** A path's metadata entry lived on the MDT chosen by hashing its
*parent* directory. mdtest-hard (a million files in one directory) put every
create, stat, read and unlink on one MDT, behind that MDT's one lock: the
model's hot-MDT ceiling was 55-160 kIOPS per phase.

**Now** (`fs/namespace_shard.py`, `roce/native/rocepool_fuse.c`,
`roce/native/rocepool_servers.c`):

- The entry lives on the MDT chosen by hashing the *whole* path: one
  directory's entries spread over every MDT (Lustre's DNE phase-2 striped
  directory, with every MDT as a stripe). `readdir` and `child_count` go to
  all MDTs in parallel and merge; both clients place entries identically, so
  files written by either are found by the other.
- The C MDT's single mutex is gone: the directory table is under a
  read-write lock (write-locked only to add a directory), and each
  directory's entries are split by name hash into 64 independently locked
  buckets. `STATS` reports `lock_stripes`, and the model scales the lock
  ceiling by it.
- `ROCEPOOL_DIR_STRIPE=0` restores the by-parent rule (every client of a
  cluster must use the same rule).

10-node simulation: mdtest-hard's directory went from 99.9% of its requests
on one MDT to 10% on each; the hot-MDT lock is no longer any phase's
bottleneck.

## 2. Zero-copy RDMA bulk in the transport (`roce/native/roce_transport.c`)

**Before.** Every frame was a SEND into a receive buffer: a 1 MiB GET reply
was copied four times on the way to the kernel (store to reply, reply to
send buffer, receive buffer to the caller's buffer, caller's buffer to
/dev/fuse), a 1 MiB write five times.

**Now** the request SEND carries an envelope: the inline part of the frame,
up to 4 *source* descriptors (address, rkey, length in the client's
registered memory) and one *sink* descriptor (the client's reply buffer).
The Lustre/LNet bulk shape, server-driven:

- **Requests**: the server RDMA READs the sources straight into the buffer
  it hands its handler. The FUSE client keeps the kernel's WRITE request
  buffers in a registered arena (`rp_bulk_alloc`, 64 MiB chunks registered
  once per device) and sends a stripe as up to 4 of them in place -- a 1 MiB
  ior-easy stripe leaves the client with **no copy**; ior-hard's 22 pieces
  per stripe are gathered once into arena memory, still read in place.
- **Replies**: the server RDMA WRITEs a large reply, with immediate data,
  into the client's sink; the handler builds it directly in a registered
  buffer (`roce_reply_alloc`), so it is never copied on the server; the
  client hands that buffer to the caller as is (`roce_reply_t`) and a
  single-stripe READ_AT is answered to the kernel straight from it.
- RDMA READ needs `responder_resources` / `initiator_depth` on both sides;
  the CM parameters now request them (within the device's limits), and
  softverbs models the failure when they are missing, plus rkey / access /
  range checks with `IBV_WC_REM_ACCESS_ERR` -- the emulator gained RDMA
  READ and RDMA WRITE with immediate.
- Small frames go inline as before; stream sends are untouched; the receiver
  tells an envelope from a plain frame by its magic.

Copies per 1 MiB on the client: writes 5 -> 0 (io_uring channel: 1), reads
4 -> 1. The transport counts body bytes sent in place vs copied
(`body_bytes_zero_copy` in the client's stats dump), and
`tests/test_native_fuse.py` checks a kernel write goes out with none copied.

On this 2-core emulated rig the bandwidth numbers did not move (the
emulator copies everything through TCP anyway, and the run is CPU-bound);
the gain is on real NICs, where the copies were host CPU on the data path.

## 3. FUSE over io_uring (`roce/native/rocepool_fuse.c`)

Linux 6.14+ (`fuse.enable_uring=Y`): one queue per possible CPU, each an
io_uring (`IORING_SETUP_SQE128 | IORING_SETUP_CQE32`) with ring entries
registered through `FUSE_IO_URING_CMD_REGISTER`; the kernel routes each
request to the queue of the CPU it was issued on, and the reply goes back
with `COMMIT_AND_FETCH` -- no /dev/fuse channel, no read()/writev() per
request. Several threads serve one queue (`cfg.threads` spread over the
queues, at least 2 each) so a request that blocks on RPCs does not stall
its queue. FORGET still comes through /dev/fuse; the classic workers stay
for it.

The uapi (`fuse_uring_ent_in_out` with a 64-bit `commit_id`, `FUSE_OVER_IO_URING`
= bit 41, op headers in `op_in` only for opcodes that have one -- LOOKUP's
name is the payload) was checked against the v6.18 kernel source; the first
two attempts against a recollection of it produced EINVAL on every request.

**Measured here** (2 cores, emulator, `tools/bench_ior.py --hard`): the
ring path is 4-10% *slower* than /dev/fuse -- the extra copy of write data
out of the ring entry (the kernel reuses that buffer, so the arena cannot
keep it) and the per-request wakeups cost more than two syscalls save when
everything shares two cores. So it is **off by default** (`--uring 1` /
`ROCEPOOL_FUSE_URING=1`), and `tools/io500_run.py` **probes both channels
on the hosts at hand** -- a short official run each way -- and uses the
faster one for the real run (`--fuse-uring auto`, the default; both probe
results go in the report, and `roce_onsite.py` carries the choice into the
valid run). On 32-core nodes with real NICs the per-core queues are
expected to win; the probe decides, not the expectation.

## Found along the way (fixed; each with a test)

- **Cross-node close-to-open.** A client cached a striped file's size
  (attr TTL 3 s); another node extended and closed it; an open here within
  the TTL still used the old size and IOR's shared-file read phase failed
  with "read returned EOF prematurely". `open` now revalidates a striped
  file's attributes and, when the size changed, sends
  `FUSE_NOTIFY_INVAL_INODE` so the kernel's own cached attributes are
  dropped too (inline files need none of this: their reads carry the
  entry). The Python daemon revalidates the same way. Test:
  `test_open_revalidates_a_striped_file_another_client_extended`.
- **Arena memory for small writes.** Keeping the whole 1 MiB request buffer
  for a 47 KB write made ior-hard's 64 MiB write cache 1.5 GiB and
  OOM-killed a 10-node run; small writes are copied into a block of their
  own size class. The per-file write cap is 16 MiB now (`ROCEPOOL_DIRTY_CAP_MIB`).
- **Server buffer pool cap.** A server needs at most two pool buffers per
  in-flight request; its pool is capped there (512 x 1 MiB replies had
  been 512 MiB resident per OST).
- **Cluster runs record every server's log** and which servers died, and
  the process snapshots carry RSS per process.
- **Malformed envelope / oversized request**: refused before any bulk
  transfer; a request larger than the server's frame limit fails cleanly
  (test `test_request_larger_than_the_servers_frame_limit_fails_cleanly`).

## 10-node simulation, rerun (`stress_reports/30`, `31`)

`tools/io500_run.py --nodes 10`, projected onto 10 x (32 cores, 100 GbE,
512 GiB). The probe chose io_uring by a hair (1.684 vs 1.680); report 31 is
the same run forced to the classic channel.

| | before this round (`28`) | classic channel (`31`) | io_uring channel (`30`) |
|---|---|---|---|
| bandwidth | 13.3 GiB/s | 12.8 GiB/s | 15.4 GiB/s |
| metadata | 336 kIOPS | 757 kIOPS | 1 534 kIOPS |
| **score** | **66.9** (38-104) | **98.2** (64-154) | **153.5** (68-154) |
| vs WEKA Afton 105.94 | 1.6x behind | 1.08x behind | 0.69 (ahead) |

Bottlenecks now:

| phases | classic channel | io_uring channel |
|---|---|---|
| ior-easy-write, ior-hard-write | RAM for a 300 s write phase | same |
| every mdtest phase | kernel FUSE channel per mount (150 k req/s estimate) | client FUSE threads per node |
| ior reads | FUSE channel / client threads | client threads |
| find | CPU | CPU |

What is measured and what is estimated: every per-op cost is measured
(the model reproduces the run's own rates within 0.84-1.05x over the 12
phases); the FUSE channel ceilings (150 k req/s and 4 GiB/s per mount for
/dev/fuse, x3 / x2 for io_uring) and the lock convoy factor are estimates
that the bands span. The io_uring column rests on that x3; the classic
column is the one to quote until the on-site probe has measured the ring on
32-core hosts.

## WEKA reference

What could be found without access to io500.org's list pages (blocked for
automated fetching): the ISC25 10-node production list has WEKA at
rank 11, UVA "Afton", **105.94** (33.28 GiB/s, 337.29 kIOPS); the top of
that list is DAOS (2 885.57) and DDN EXAScaler (348.08). On the unlimited
production list WEKA is rank 4 with 1 826.86. Per-phase WEKA figures were
not reachable, so the comparison is on the three headline numbers:

| | WEKA Afton | rocepool, classic channel | rocepool, io_uring |
|---|---|---|---|
| bandwidth | 33.3 GiB/s | 12.8 GiB/s (2.6x behind) | 15.4 GiB/s (2.2x behind) |
| metadata | 337 kIOPS | 757 kIOPS (2.2x ahead) | 1 534 kIOPS (4.5x ahead) |
| score | 105.9 | 98.2 | 153.5 |

The bandwidth half is capacity: a valid 300 s write phase must fit in the
pool's RAM (the projection assumes 512 GiB per node and gives IOR 75% of
the usable part); WEKA's writes go to NVMe. With an NVMe tier behind the
memory pool the same run projects to 280-300. The metadata half is where
the striped directories, Data-on-MDT and the thick OST ops pay off; it is
also the half that depends most on the FUSE channel estimate.

## Tests

- `tests/test_dist_fs.py`: both placement rules.
- `tests/test_native_servers.py`: 8-thread create/set_size/lookup/unlink/
  readdir hammer on one directory of the native and Python MDTs, exclusive
  create races (also run under ThreadSanitizer: clean).
- `tests/test_native_fuse.py` (28): a directory striped over every MDT
  through both clients; write data going out with no copy (ior-easy and
  ior-hard shapes); requests answered through io_uring; cross-node
  close-to-open.
- `tests/test_softverbs_rc_semantics.py`: bulk goes by RDMA READ and RDMA
  WRITE with immediate (emulator trace); oversized raw SEND still rejected;
  oversized request refused cleanly.
- `tests/test_io500_cluster.py`: the 3-node simulation probes both FUSE
  channels and records the choice; mdtest-hard's directory spread over the
  MDTs; `lock_stripes` reported.
- Full suite: 316 tests.
