# IO500 on 6 x i3en: where the IOR phases' time goes, and two changes

From the servers' own counters in the three 6-node runs (branches
`aws-results/gh-40999521729`, `gh-41196712217`, `gh-41438027754`), per phase.

## What the counters say

A server has one endpoint per 4 cores (12 on i3en.12xlarge; EFA has few queue
pairs per host) and one thread per endpoint, which handled one request at a
time and waited for whatever that request waited for.

**ior-easy-write, 16.2-16.4 GiB/s in all six measurements** (client I/O
threads 4 or 16, durable or not). Per 1 MiB request on an OST: 15 ms queued
for a thread, 2.0 ms pulling the payload, 0.36 ms writing the NVMe, ~0.6 ms
the rest; threads 60% busy, the NVMe at one write at a time, the NIC at 40%,
the hosts 19% busy. The pull is slow on i3en because its EFA has no RDMA
read in hardware: libfabric has the client's CPU send the data, and the
server's thread only waited.

**ior-easy-read, 22-27 GiB/s.** Threads 92% busy: per request 1.2 ms reading
the tier, then 1.25 ms waiting for the megabyte's send.

**ior-hard-read.** 60 s runs were served from the RAM cache (75 GiB per OST),
300 s runs mostly from the tier, read in the endpoint's thread: 21.9 -> 14.3
and 21.6 -> 10.4 GiB/s. Asynchronous tier reads (in gh-41438027754) brought
300 s back to 14.5; but that run's all-RAM speed was 16.3, with twice the
CPU per read in the ranks (183 -> 349 us) and the hosts 51-59% busy against
40% -- at the same time the clients' I/O threads went from 4 to 16. Not yet
isolated.

**ior-hard-write, 9.4 (not durable) -> 6.46 -> 3.56 GiB/s**, the lowest of
the four and so the one that costs the bandwidth score most. Durable, every
47008-byte write waits for the tier's fdatasync, ~13 writes per commit; the
last step down came with commits of 1754 us against 708.

## 1. The commit on XFS: reproduced, #53 confirmed

`tools/hard_write_bench.py` writes ior-hard's shape against durable OSTs and
reads their commit counters. CI job `tier-commit` runs it on XFS and ext4,
on loop devices backed by RAM (a flush is free there, so a commit's time is
the file system's own work; on the runner's disk a 3.7 ms flush hid it):

| tier on | a fresh slab's gaps | commit | MiB/s |
|---|---|---|---|
| XFS | skipped (`ROCEPOOL_OST_SKIP_ZERO_GAPS=1`, as in gh-41438027754) | 1430-1492 us | 274-279 |
| XFS | written (the default since #53) | 904-957 us | 309-310 |
| ext4 | skipped | 1001 us | 312 |
| ext4 | written | 995 us | 301 |

On XFS, pieces written apart cost every commit half as much again; on ext4
nothing -- which is why it never showed locally. The default is the right
one. The size of the effect on i3en (192 ranks, real NVMe) is still to be
measured there.

## 2. The endpoint's thread no longer waits for a pull or a large reply

`ofi_transport.c`: the thread posts a request's payload reads and takes the
next request; the handler runs when the payload is in
(`ROCE_OFI_PULL_PIPELINE`, 4 pulls in flight per thread; 1 = wait). A large
reply is posted and its buffer released when the send completes
(`ROCE_OFI_BIG_REPLIES`, 4; 0 = wait). STATS: `pull_pipeline_max`,
`big_replies_async`.

`tools/ofi_lane_bench.py` (CI job `ofi-lanes`: libfabric's tcp provider, 2
endpoints, 32 clients, 1 MiB objects, a netem delay on the clients' link so
that a transfer takes wire time):

| link delay | writes, waiting -> in flight | queued for a thread |
|---|---|---|
| 2 ms | 915 -> 1325 MiB/s (+45%) | 29 -> 14 ms |
| 0.5 ms | 1197 -> 1423 MiB/s (+19%) | 22 -> 14 ms |

Reads did not change there (1.6-1.7 GiB/s either way): that setup's threads
are busy with the reads themselves, not waiting for sends. What this gives
on EFA -- where the wait is the client-driven pull (i3en) or nearly nothing
(families with RDMA read in hardware, e.g. hpc6id) -- is for the next run on
real hosts; the tier write (0.36 ms) and the tier read of a large GET are
still done in the thread.

Tests: `tests/test_ofi_concurrency.py` `OfiPipelineTests` (several pulls and
large replies in flight on one thread, every byte right; both turned off;
a capped buffer pool). The FUSE client, interception library and durability
suites also pass over the libfabric transport with the change.

## 3. mdtest-hard-write: the node agent (opt-in, `io500_run.py --agent on`)

SC26 (gh-41438027754) took mdtest-hard-write at ~0.72 ms per file per rank,
one file after another; per file the MDT spent 151 us in the transport and
31 us in the handler. With create_write (#104) a file is one MDT request.
The agent (`roce/native/rocepool_agent.c`) takes those requests from a
node's ranks over a UNIX socket and sends what waits for one MDT as one
batch (MDT op 16): whichever rank's thread finds no batch out for that MDT
sends its own request and those queued behind it (flat combining, no timer).
Per file nothing changes at the MDT -- each request is handled, locked and
logged alone, a replay sees the requests, never the batch -- and a rank
hears back only once the batch is durable (rule 6). A rank whose agent
cannot answer sends the request itself (create_write is idempotent).

Measured on the 3-node simulation (2 vCPUs shared by every node, 8 ranks
per node, 20 s stonewall, one run each unless noted):

| transport | agent off | agent on | requests per batch |
|---|---|---|---|
| softverbs (cheap per request) | 11.33 kIOPS, 174 us host CPU / file | 7.27 kIOPS, 254 us | 1.24-1.48 |
| libfabric tcp, no CQ wait object (EFA's wait loop) | 4.21 / 3.68 kIOPS, 499 / 558 us | 4.90 / 3.76 kIOPS, 422 / 542 us | 1.01-1.12 |

Where a request costs little, the extra local hop costs more than batching
saves. With EFA's wait loop the two are within the runs' noise. Two CPUs
cannot make requests queue behind one another the way 32 ranks per node on
real hosts might, so batches stay near 1 here; whether they grow there, and
whether ranks that no longer spin on EFA completions free enough CPU, is for
the next run on real hosts (an A/B pair, `--agent off` / `--agent on`). Off
by default until then.

## 4. ior-hard-write: stripe writes durable at fsync / close (`ROCEPOOL_OST_ASYNC`)

IO500 runs ior-hard with `-e` (fsync at close, inside the phase's time). A
rank's 47008-byte writes need to be durable only by its fsync; until now
every one waited for its OST's commit (tier fdatasync + log), ~13 writes
per commit at 1754 us on i3en: 3.56 GiB/s durable against 9.4 not durable.

Now (on by default in `io500_run.py` when the servers are durable;
`ROCEPOOL_OST_ASYNC=0` turns it off): a PUT / WRITE_AT flagged RQF_ASYNC is
replied to once its bytes are on the OST's tier and its log record
appended, with the OST's epoch and the record's log position (reply flag
RPF_TX). The client keeps, per OST, the highest such position; fsync, close
and exit send OP_OST_COMMIT to every OST with one, replied to once that
OST's log is durable through it. If the epoch changed -- the OST restarted
and may have lost what it acknowledged -- that fsync / close fails with EIO:
never a close that says durable when it is not (rule 6). sync() barriers
every OST.

`tools/hard_write_bench.py` (2 nodes x 4 writers, ext4 on this project's
2-vCPU VM, 20 s): 151 -> 216 MiB/s (+43%), 9.4 -> 26.1 writes per commit,
every piece read back right.

## 5. The tier on raw NVMe (`ROCEPOOL_NVME_LAYOUT=raw`, opt-in)

Section 1 measured the commit on a RAM-backed device -- where a flush costs
nothing -- at 0.9-1.5 ms on XFS and ~1 ms on ext4: the file system's own work
(journal, extent conversions) for every commit of a file tier. The OST tier
already took block devices (`ost_tier.c`), so the hosts now give it some:

- `tools/aws_cluster.py` (`ROCEPOOL_NVME_LAYOUT=raw`; `xfs`, the old layout, stays the default): every
  instance-store device is split -- a 64 GiB partition (`ROCEPOOL_NVME_LOG_GIB`)
  into a RAID 0 with XFS at `/nvmedata` for the servers' logs, the rest raw:
  one RAID 0 per NUMA node, `/dev/rocepool-tier-<j>` (hpc6id.32xlarge: 4
  devices, 2 NUMA nodes -> a RAID 0 of 2 per OST; i3en.12xlarge: 4 devices,
  1 node -> one RAID 0 of 4). Any step failing: the old layout (one XFS over all of them).
  `ROCEPOOL_NVME_LAYOUT=xfs` keeps the old one.
- `tools/nvme_preflight.py`: with as many raw tiers as OSTs per node and
  `/nvmedata` mounted, recommends `file:/dev/rocepool-tier-{l}` ({l}: the OST's
  number on its node) and the logs in `/nvmedata`; it measures the write
  speed on the raw device itself.
- A tier commit is then the log's write plus a device flush.

Tests: `tests/test_nvme_layout.py` (the setup script against fake devices:
i3en and hpc6id shapes, the fallback), `tests/test_raw_tier.py` (OSTs on loop
devices: ior-hard's shape survives SIGKILL, synced and async),
`tests/test_nvme_preflight.py`, `tests/test_io500_cluster.py`. CI
`tier-commit` adds raw RAM-backed devices beside XFS and ext4. Locally, on
loop devices backed by files, raw was slower (each flush is a file's fsync
underneath): only real NVMe tells. CI's RAM-backed devices (a flush free):
raw 437 MiB/s, commit 694 us; XFS files 449-454 MiB/s, 662-671 us; ext4 433
MiB/s, 784 us -- no gain where the device costs nothing, so XFS stays the
default until the hpc6id run compares them on real NVMe (the raw layout
keeps an XFS partition, so both can be measured on the same hosts).
