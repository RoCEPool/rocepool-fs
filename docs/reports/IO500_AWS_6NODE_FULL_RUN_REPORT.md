# Full Test Run Report: 6 × i3en.12xlarge on AWS (run 40999521729)

| | |
|---|---|
| Report date | 2026-10-31 |
| System under test | RoCEPool (RDMA memory-pool distributed file system), main at commit 0f85b7b |
| Platform | Amazon EC2 us-east-2, 6 × i3en.12xlarge (each: 48 vCPU, 384 GiB RAM, 4 × 7.5 TB NVMe, 50 Gb/s EFA), Ubuntu, kernel 7.0.0-1014-aws, libfabric efa provider |
| Run | GitHub Actions run 40999521729, 2026-10-30 03:49–05:48 UTC; raw reports on branch `aws-results/gh-40999521729` |
| Overall verdict | **WARN.** Every check passed; the 300 s IO500 run scored 111.26, and IO500 marked it invalid for one phase (section 5) |
| Related | IO500 comparison with Vendor W: [IO500_AWS_6NODE_REPORT.md](IO500_AWS_6NODE_REPORT.md) · Japanese version of this report: [IO500_AWS_6ノード全項目テストレポート.md](IO500_AWS_6ノード全項目テストレポート.md) |

## 1. Summary

The whole run was automated, from launching 6 instances to deleting them, and took 1 h 59 min ($67).

- **Every functional and stress check passed** on the head node, and all 6 hosts passed their preflight and build.
- **IO500, 60 s run:** 124.79 (bandwidth 17.09 GiB/s, metadata 911.2 kIOPS).
- **IO500, 300 s run:** **111.26** (bandwidth 14.81 GiB/s, metadata 835.9 kIOPS), above Vendor W's 105.94 on 28 machines. IO500 marked it invalid because mdtest-easy-write ran out of files after 207 s; the other eleven phases met the rules. The cause is fixed (PR #35).
- **An earlier attempt the same day** (run 40984482522) stopped before IO500 because of a race when copying the tree to five peers at once. It was fixed (PR #33) and covered by a test (PR #34) before this run.

| Step | Status | Result |
|---|---|---|
| Preflight (head + 5 peers) | WARN | All checks pass; the WARN is that i3en's EFA has no hardware RDMA read (emulated, as expected) |
| Peers | PASS | SSH, root, addresses and unique hostnames on all 5 peers |
| Build (head + 5 peers) | PASS | Transport (libfabric/EFA), FUSE client, KV library, LD_PRELOAD client, cuFile library |
| Transport | PASS | 92.8 s of transport tests over EFA |
| Busy-poll calibration | PASS | 50 µs chosen |
| Functional | PASS | smoke, pool, MPI, HPC, stream |
| Concurrency stress | PASS | Up to 500 clients, 0% errors, no hung or dead servers |
| IOR + mdtest (FUSE) | PASS | 5 IOR patterns and mdtest |
| ior-hard + mdtest-hard | PASS | IO500's hard pair at functional scale |
| KV API | PASS | Native and Python clients |
| GPUDirect Storage | PASS | No GPU on this host type; covered by unit tests |
| KV-cache reuse | PASS | TTFT 1.36× faster than recomputing |
| Cluster preparation | WARN | Tree and MPI/IO500 builds copied to all 5 peers; peers' preflight WARN as above |
| NVMe preflight | PASS | Tier on every host, 1,971–2,488 MiB/s O_DIRECT write |
| IO500, 60 s | PASS | 124.79 |
| IO500, 300 s | WARN | 111.26, marked invalid (section 5) |

## 2. Timeline

| Time (UTC) | Event |
|---|---|
| 03:49:42 | Job starts; cost gate: at most $100.89 for 3 h |
| 03:50:17 | vCPU quota 300 ≥ 288 needed |
| 03:50:29 | 6 instances launched |
| 03:54:10 | All 6 hosts ready with EFA |
| 03:54:16 | On-site run starts on the head node: dependencies, build, checks |
| ~04:43 | IO500 60 s run (696 s) |
| ~04:55 | IO500 300 s run (3,112 s) |
| 05:47:58 | Reports collected |
| 05:48:33 | All resources released |

## 3. Checks on the head node

These checks run on one host with small data sizes. They prove function and stability, not cluster performance.

### 3.1 Transport and busy-poll calibration

The transport suite (connection pooling, stale-connection recovery, drain, receive back-pressure, bounded memory registration, MDT/OST parity, compare-and-swap atomicity) passed in 92.8 s.

| Busy-poll | 1 thread (req/s) | p50 latency | 8 threads (req/s) |
|---|---|---|---|
| 0 µs | 16,010 | 62 µs | 28,970 |
| 20 µs | 12,221 | 82 µs | 24,057 |
| **50 µs (chosen)** | **26,645** | **37 µs** | **38,518** |
| 100 µs | 26,864 | 37 µs | 39,502 |
| 200 µs | 26,648 | 37 µs | 39,106 |
| 400 µs | 26,713 | 37 µs | 39,206 |

### 3.2 Concurrency stress

Clients ramped from 1 to 500. Every level: 0% errors, 14 server threads, no TIME_WAIT sockets, no listen overflows, no hung or dead servers.

| Clients | Metadata (ops/s) | Shared-directory metadata (ops/s) | Data (ops/s) | Worst p99 (data) |
|---|---|---|---|---|
| 1 | 6,702 | 6,737 | 505 | 3.9 ms |
| 10 | 10,823 | 10,597 | 1,347 | 19.6 ms |
| 50 | 40,467 | 40,225 | 4,814 | 28.1 ms |
| 100 | 60,558 | 63,443 | 7,493 | 37.7 ms |
| 200 | 74,842 | 82,852 | 9,372 | 67.6 ms |
| 500 | 79,043 | 91,665 | 9,037 | 219.5 ms |

### 3.3 IOR and mdtest through the FUSE client

| Pattern | Write (MiB/s) | Read (MiB/s) |
|---|---|---|
| shared file, 1 MiB | 58.1 | 863.6 |
| shared file, 64 KiB | 308.5 | 1,013.2 |
| file per process, 1 MiB | 880.8 | 1,765.2 |
| shared file, 4 MiB, large | 783.4 | 1,505.3 |
| file per process, 256 KiB | 446.3 | 1,660.8 |
| ior-hard shape (47,008 B) | 152.9 | 265.0 |

mdtest: 5,804 creates/s, 1,030,542 stats/s, 87,509 reads/s, 13,620 removals/s; the mdtest-hard shape: 5,122 creates/s, 322,050 stats/s.

### 3.4 KV API and KV-cache reuse

| Client | Operation | Threads | Value | ops/s | p50 |
|---|---|---|---|---|---|
| native | get | 1 | 64 B | 16,278 | 54.5 µs |
| Python | get | 1 | 64 B | 12,525 | 68.2 µs |
| native | put | 1 | 64 B | 15,683 | 52.9 µs |
| native | get | 16 | 64 B | 22,251 | 72.7 µs |
| native | multi-get, 64 keys | 16 | 64 B | 242,048 keys/s | 3.5 ms |
| native | get | 1 | 4 KiB | 17,290 | 57.3 µs |
| native | multi-get, 64 keys | 16 | 4 KiB | 130,406 keys/s | 7.1 ms |

KV-cache reuse (llama3-70b, 6,144 of 8,192 tokens cached, 1,920 MiB of KV): loaded at 2.1 GB/s in 3.6 s (p50); time to first token 5.45 s against 7.41 s when recomputed, 1.36× faster (prefill estimated).

## 4. Cluster preparation and NVMe tier

The tree and the MPI/IO500 builds were copied to all 5 peers in parallel; each peer ran its own preflight and build. Clocks were within 0.146 s of the head.

| Host | NVMe tier | O_DIRECT write (MiB/s) |
|---|---|---|
| head (172.31.7.70) | 4-drive RAID 0, 21.9 TB tier file | 2,232 |
| 172.31.1.244 | same | 1,971 |
| 172.31.11.38 | same | 2,144 |
| 172.31.9.9 | same | 2,062 |
| 172.31.1.247 | same | 2,488 |
| 172.31.10.247 | same | 2,026 |

## 5. IO500

192 ranks (32 per node), LD_PRELOAD interception client (chosen by the client probe), 6 MDTs and 6 OSTs.

| Phase | 60 s run | 300 s run | 300 s run time (s) | Host CPU busy |
|---|---|---|---|---|
| ior-easy-write (GiB/s) | 16.33 | 16.36 | 367.2 | 18% |
| mdtest-easy-write (kIOPS) | 942.3 | 933.4 | **206.8** | 56% |
| ior-hard-write (GiB/s) | 9.56 | 9.36 | 306.4 | 37% |
| mdtest-hard-write (kIOPS) | 419.8 | 385.6 | 311.8 | 55% |
| find (kIOPS) | 3,187.2 | 2,321.4 | 134.4 | 68% |
| ior-easy-read (GiB/s) | 25.03 | 22.03 | 272.5 | 43% |
| mdtest-easy-stat (kIOPS) | 952.2 | 938.7 | 205.6 | 54% |
| ior-hard-read (GiB/s) | 21.85 | 14.25 | 201.4 | 36% |
| mdtest-hard-stat (kIOPS) | 942.5 | 934.1 | 129.4 | 54% |
| mdtest-easy-delete (kIOPS) | 959.0 | 949.4 | 203.4 | 53% |
| mdtest-hard-read (kIOPS) | 461.9 | 421.1 | 285.6 | 60% |
| mdtest-hard-delete (kIOPS) | 947.8 | 813.9 | 148.3 | 36% |
| **Bandwidth score (GiB/s)** | 17.09 | **14.81** | | |
| **Metadata score (kIOPS)** | 911.2 | **835.9** | | |
| **IO500 score** | 124.79 | **111.26** | 3,112 total | |

Host CPU busy is node 0's share of its 48 vCPUs during each phase of the 300 s run.

**Why IO500 marked the 300 s run invalid.** mdtest-easy-write created all the files it was given, 1 M per rank (192 M in total), in 207 s and stopped before the 300 s stonewall. The 1 M default dates from when a rank created about 600 files/s; it now creates about 4,900/s. The run's own report blamed ior-easy-write's data budget instead, which was also wrong: ior-easy-write had 59,006 GiB for 5,633 GiB needed. **Fix (PR #35):** mdtest's files per rank are now sized from the 60 s run's rate (for this run, about 2.2 M per rank), and the report names the phase that ran short.

**Data sizing for the 300 s run.** The OSTs' capacity budget was 118,013 GiB (bounded by the smallest tier, 21.9 TB). From the 60 s run's speeds: ior-easy 59,006 GiB, ior-hard 3,297 GiB. MDT lookup table: 67,108,864 slots per MDT; inline small-file budget 57,400 MiB per MDT.

## 6. The earlier attempt (run 40984482522)

Earlier the same day, the first 6-node attempt passed every head-node check, then failed while copying the tree to one of the five peers: `tar: .: file changed as we read it`. Each peer's same-machine check wrote and removed a token file in the tree's top directory while another peer's tar was reading it. With 1–3 peers the timing never collided; with 5 it did. The instances were released after 37 minutes ($20.6), before IO500 started.

- **Fix (PR #33):** the token goes to a directory the archive excludes, one file per check; tar's "file changed" exit is treated as success; a failed copy is retried once.
- **Test (PR #34):** five peers copied at once with a realistic 20 MB tree and staggered checks. It fails on the old code (3 of 3 tries) and passes on the fix. A standalone reproduction had no failure with 1 to 31 peers on the fix.

## 7. Cost

| Item | Amount |
|---|---|
| 6 × i3en.12xlarge with 400 GiB gp3 disks | $33.63 per hour |
| This run, 1 h 59 min of instance time | about $67 |
| The earlier attempt, 37 min | about $20.6 |

Costs are AWS on-demand prices computed from instance time, not the AWS bill.
