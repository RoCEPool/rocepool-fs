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
