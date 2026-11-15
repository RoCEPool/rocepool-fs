# rocepond: DuckDB tasks over a cluster, data and shuffle on rocepool

The design of DeepSeek's smallpond (DuckDB + their 3FS file system + Ray), on
rocepool, in Python: data, the shuffle and every intermediate result are
Parquet files in rocepool's shared namespace; the work is DuckDB tasks, one
per partition, scheduled over the cluster.

```python
import rocepond
ex = rocepond.RayExecutor(address="auto")              # or rocepond.ProcessExecutor(...), rocepond.local_executor(8)
sp = rocepond.Session(ex, root="/mnt/rocepool/rocepond")

events = sp.read_parquet(["/mnt/rocepool/in/day=1/*.parquet", "/mnt/rocepool/in/day=2/*.parquet"])
users  = sp.read_csv("users", [["/mnt/rocepool/in/users.csv"]], columns, delim=",").broadcast()

by_user = events.repartition(1024, hash_by="user_id")                  # the shuffle, through rocepool
partial = sp.partition_sql("SELECT country, count(*) AS n FROM {0} JOIN {1} USING (user_id) GROUP BY country",
                           by_user, users)                              # 1024 tasks
rows = sp.sql("SELECT country, sum(n) FROM {0} GROUP BY country ORDER BY 2 DESC", partial)
```

## The API

A DataFrame is a list of partitions; a partition is data on the shared file
system. Operations run when called; each is one task per partition.

| | |
|---|---|
| `sp.read_parquet([...])` | one entry (path, glob, or list) per partition |
| `sp.read_csv(name, [[files], ...], columns, delim, trailing_delimiter)` | one list of files per partition |
| `sp.generate(name, sql, splits)` | `sql` in `splits` tasks (`{split}`, `{nsplits}`), a Parquet file each |
| `df.repartition(P, hash_by="cols")` / `(P, by="expr")` | the shuffle into P partitions; `max_fanout=1024`, see below |
| `df.broadcast()` | read whole by every task (the small side of a join) |
| `sp.partition_sql(sql, *dfs, name=, fmt=)` | `sql` per partition; `{0}`, `{1}` = partition i of each DataFrame (same P), or the whole of a broadcast one. Results: a DataFrame |
| `df.map_partitions(sql, name=, fmt="parquet" or "lines")` | `partition_sql` over one DataFrame |
| `sp.sql(sql, *dfs)` | one task over whole DataFrames; returns the rows |
| `df.write_parquet(name)`, `df.count()`, `df.take(n)` | |
| `df.as_table("orders")` | SQL may then say `orders` instead of `{0}` |
| `sp.drop(df)`, `sp.drop_tmp()` | remove what the session wrote |
| `sp.stats`, `ex.stats` | per stage: tasks, seconds, retries, tasks/s, worker utilization |

DataFrames repartitioned to the same P on their join key are co-partitioned:
`partition_sql` joins partition i with partition i. As in smallpond, the user
chooses the partitioning and writes the per-partition and the merging SQL;
nothing rewrites a query automatically.

**The shuffle** is DuckDB's partitioned write (`COPY ... PARTITION_BY`), as in
smallpond: a task per input partition writes its rows into P directories on
rocepool. A partitioned write holds buffers for every partition (measured:
~370 MiB per task writing 100 MiB into 4096 partitions), so above
`max_fanout` partitions it takes two passes -- into groups of `max_fanout`
neighbouring partitions, then a task per group. More I/O, bounded memory;
raise `max_fanout` on nodes with memory to spare.

## Scheduling

A task is a small JSON-able dict that names every file it reads and writes
(`rocepond/task.py`). Workers keep nothing between tasks, so any worker on
any node can run any task; the scheduler needs free workers, not data
locality. Two schedulers, one interface (`run_all(tasks) -> results`), the
same guarantees:

- thousands of tasks per call: queued, run as workers free up
- a failed task -- an error, a worker process that died, a task over
  `task_timeout` -- runs again on any worker, up to `max_retries` (3) times;
  a retried shuffle task first removes what its failed attempt wrote
- a task that fails every time fails the call at once, with every attempt's
  error; tasks not yet started are dropped
- every DuckDB gets a memory limit (a host's memory less a third, divided
  among the workers on it) instead of each assuming 80% of the host

**`rocepond.ProcessExecutor`** (no dependencies): `slots_per_node` worker
processes on every node, each a long-lived DuckDB, fed from one queue.
Started through tools/cluster.py (a namespace when simulated, ssh on real
hosts); a dead worker is restarted.

**`rocepond.RayExecutor`**: every task is a Ray task; Ray queues and places
them over the cluster's CPUs.

```python
rocepond.RayExecutor(address="auto",            # the running cluster; None: a local Ray
                     threads=4,                 # CPUs (and DuckDB threads) per task
                     memory=8 << 30,            # Ray's memory reservation per task (also DuckDB's limit)
                     resources={"nvme": 1},     # custom Ray resources per task
                     scheduling_strategy="SPREAD",   # or NodeAffinitySchedulingStrategy(...), a placement group
                     max_retries=3, task_timeout=3600)
```

The rocepond package is shipped to the Ray workers (`runtime_env`), so the
nodes need `pip install ray duckdb` and rocepool mounted, nothing else.
Retries are rocepond's (resubmission), not Ray's `max_retries`, so both
schedulers behave the same and every attempt's error is reported.

DuckDB is used through its Python package where installed, else through
the DuckDB CLI (`rocepond/duck.py`); the same tests run on both.

### Verified

`tests/test_rocepond.py`, the same cases against both schedulers (CI job
`rocepond` installs Ray and DuckDB and runs them, then the sort below on a
simulated 2-node rocepool cluster with each scheduler):

- 5,000 tasks in one call, results in order, spread over the workers
  (ProcessExecutor here: ~4,200 tasks/s on 4 workers)
- injected failures and worker processes killed mid-task: retried, counted,
  the executor whole afterwards
- an always-failing task: the call fails with all attempts' errors
- a task over its time limit: stopped, retried, reported
- a shuffle task that fails after writing its files: no duplicate rows
- shuffle + co-partitioned join + merge equal to the single-task answer;
  partitions without rows; one- and two-pass shuffles give equal partitions
- GraySort-style sort into 1,024 partitions: valid, globally ordered

## GraySort-style sort: thousands of tasks on rocepool

    sudo python3 tools/rocepond_sort.py [--gib 1] [--partitions 4096] [--executor process|ray] [--slots N]
    python3 -m rocepond.sort --root /mnt/rocepool/rocepond --gib 100 --partitions 4096 --executor ray   # on a Ray node

smallpond's own benchmark (`benchmarks/gray_sort_benchmark.py`): 100-byte
records with a 10-byte key, range-partitioned by key through the file system,
every partition sorted. In rocepond (`rocepond/sort.py`):

```python
records = sp.generate("sort_input", "SELECT left(a, 10) AS keys, ... AS records FROM range(...)", splits)
buckets = records.repartition(P, by=f"(('0x' || left(keys, 8))::BIGINT * {P}) >> 32")
result  = buckets.map_partitions("SELECT records FROM {0} ORDER BY keys", name="sort_output", fmt="lines")
```

Validation, also as tasks: every output file in key order, partition ranges
in order, record count and a checksum of all records equal to the input's.

2026-11-14, simulated 2-node cluster (one host, 2 cores, 8 GiB RAM, emulated
RDMA, OST data in RAM), ProcessExecutor with 2 slots per node, 1 GiB in
4,096 partitions (`docs/reports/rocepond_sched_sort_sim2.json`), valid:

| stage | tasks | seconds | retries |
|---|---|---|---|
| shuffle, pass 1 (16 input files -> 4 groups) | 16 | 6.6 | 0 |
| shuffle, pass 2 (groups -> 4,096 partitions) | 4 | 20.7 | 0 |
| sort | 4,096 | 23.6 (173 tasks/s) | 0 |
| validate output | 4,096 | 7.9 (521 tasks/s) | 0 |

What it says: 8,000+ tasks through rocepool, scheduled and validated, on two
cores. What it does not: throughput -- 1 GiB in 4,096 partitions is 256 KiB
per partition, so the time is per-file overhead (65,536 shuffle files) and
the second shuffle pass, not data: 9.7 MiB/s per core here, against 45
MiB/s per core for the same 1 GiB in 16 partitions (11.4 s). Partition
counts in the thousands belong to data in the hundreds of GiB.

## On real hosts: AWS, 2 x c5n.9xlarge with EFA

    python3 tools/aws_cluster.py run --mode efa --type c5n.9xlarge --nodes 2 \
        --command "python3 tools/rocepond_sort.py --engine cli --executor process,ray --gib 20 --partitions 1024 --slots 32"

tools/rocepond_sort.py brings the real cluster up itself (rocepool's setup as
run.sh does it, then duckdb, ray and the DuckDB CLI in `/opt/rocepond-venv`
on every host), runs each scheduler on a fresh rocepool, validates, and
reports. 2026-11-15 (`docs/reports/rocepond_sort_aws_c5n9x2.json`): 2 hosts x
36 vCPUs (18 physical cores, Xeon 8124M), 96 GiB, 50 Gbps EFA; rocepool's
OSTs keep data in RAM; 32 workers per host; DuckDB 1.5.6 CLI.

| 20 GiB, 1,024 partitions | shuffle | sort | shuffle + sort | per physical core | retries |
|---|---|---|---|---|---|
| ProcessExecutor | 122 s | 102 s | 224 s (5.4 GiB/min) | 2.54 MiB/s | 0 |
| RayExecutor (2-node Ray) | 122 s | 102 s | 224 s (5.3 GiB/min) | 2.54 MiB/s | 0 |
| smallpond + 3FS, published (110.5 TiB, 8,192 partitions) | | | 30 min 14 s | 6.65 MiB/s per compute core | |

Both valid: every record, in order, checksum equal. ~3,200 tasks per run.

What it says: the schedulers place and run thousands of tasks over real
hosts, and the work on rocepool over EFA is correct. Where the time goes,
from `--diagnose` (64 workers at once, the same data on local disk and on
rocepool): the shuffle's partitioned write -- every task writing 1,024 small
files -- runs at 3.2 MiB/s per task on rocepool against 20 on local disk;
plain Parquet writes reach 2 GB/s and reads 2.3 GB/s over both hosts. Small
files are rocepool's cost here, not DuckDB and not the network (50 Gbps is
used to a fraction).

**Small files: fewer round trips per file (after the AWS run).**
`tools/smallfile_bench.py` writes files the way the shuffle does (create,
header, body, footer, close; many workers, many directories) and reads the
FUSE client's per-opcode counters. A new file over the 32 KiB inline limit
used to start inline and, at its first flush, take the whole spill path:
freeze, version token, freeze again, PUT of the (empty) inline bytes,
commit, a fresh lookup, the stripe write, set_size. Now an empty inline
file's first flush writes its stripes and then one new MDT op,
`spill_set_size` (fs/mdt_wire.py, both MDTs, logged and replayed like the
others), makes it striped at its size; anything else (inline bytes
appeared, a spill under way, an older MDT answering UNSUPPORTED_OP) falls
back to the spill and writes again. A spill of an empty inline file also
skips its PUT.

| per 320 KiB file | MDT round trips | OST round trips | FLUSH (simulated, 2 cores) |
|---|---|---|---|
| before | 5.07 | 3.0 | 4.0 - 8.3 ms |
| after | 2.07 | 1.0 | 2.7 - 3.7 ms |

Files up to 32 KiB still stay inline (one MDT round trip each). On the
simulated cluster the times are bound by the emulated RDMA's CPU use; on
EFA each round trip saved is network latency off every file. Not yet
measured on real hosts.

**I/O errors on AWS: a FUSE client bug, found and fixed.** Writes on
rocepool failed now and then with EIO on AWS (about 150 per run with DuckDB
2.0, a few with 1.5.6; all recovered by the schedulers' retries), never on
the simulated cluster at first, and the FUSE client never answered EIO. The
cause: a WRITE's request buffer goes to the file handle (zero copy), and the
reply's byte count was read from that buffer after the hand-over. A flush
-- by this write past the dirty cap, by another thread writing the same fd,
or by the write-behind -- can free the buffer first, and the next request
lands in it: the reply then carried another request's count, a short write
when smaller, and when larger an EIO the kernel produces on its own. Many
writers per fd (DuckDB 2.0's asynchronous writer: up to 4 x the host's
cores of threads) made it frequent. `op_write` now reads what the reply
needs before the hand-over. `test_concurrent_writes_on_one_fd_are_answered_with_their_own_size`
(1 MiB dirty cap, 8 threads per fd) failed every time before and passes
since. The kernel log also shows EFA `DEREG_MR` failures (err -22) under
this load, without errors reaching the applications.

## The 11 TPC-H queries (first prototype)

    sudo python3 tools/rocepond_tpch.py --rapidsse <RapidsSE tree | auto> --duckdb <DuckDB CLI | auto>

The numbers in this and the next section were measured with the first
prototype (one worker per node, one partition per node; PRs #69, #70) and
are kept as measured. `rocepond/tpch.py` and the tool now run on the task
model above (lineitem and orders in `--partitions` partitions; answers
checked against the plan's SQL run as one task), but TPC-H is not where this
work is going.

rocepool up on a 2-node cluster, TPC-H data generated into it (the same data
as tools/rapidsse_tpch.py), lineitem and orders repartitioned by order key,
the other six tables broadcast, then the 11 queries of RapidsSE's regression
plan (`rocepond/tpch.py`: a per-partition part and a merge each; Q11, which
touches broadcast tables only, runs on one node). Every answer is compared
with a single DuckDB running the plan's original SQL over the same Parquet
files.

### Result (2026-11-14, simulated 2-node cluster, SF1)

One host, 2 cores, 8 GiB RAM; softverbs RDMA emulator; OST data in RAM;
DuckDB v1.5.6; 1 thread per worker (the 2 nodes share the 2 cores), 2 threads
for the single-node reference.

All 11 answers equal the single-node DuckDB's, in both modes.

| | rocepond, 2 nodes | single DuckDB | RapidsSE on rocepool (docs/RAPIDSSE_TPCH.md) |
|---|---|---|---|
| load: 1.03 GiB of .tbl -> queryable | 7.1 s (shuffle + broadcast through rocepool, 0.30 GiB Parquet) | | 38.7 s |
| 11 queries, reading Parquet from rocepool every time | 3.15 s | 3.70 s | |
| 11 queries, data held in memory (+2.1 s to load it) | 0.73 s | 0.66 s | 50.9 s |

| query, ms (median of 3) | from rocepool: 2 nodes | from rocepool: single | in memory: 2 nodes | in memory: single |
|---|---|---|---|---|
| Q1 | 269 | 338 | 113 | 110 |
| Q3 | 297 | 367 | 43 | 41 |
| Q4 | 231 | 290 | 43 | 42 |
| Q6 | 218 | 253 | 23 | 17 |
| Q7 | 315 | 395 | 45 | 44 |
| Q8 | 338 | 380 | 32 | 28 |
| Q9 | 479 | 514 | 156 | 154 |
| Q10 | 410 | 413 | 148 | 120 |
| Q11 | 103 | 93 | 12 | 11 |
| Q12 | 271 | 368 | 70 | 60 |
| Q14 | 223 | 289 | 44 | 35 |

What the numbers say, and do not say:
- The two "nodes" share one host's 2 cores, so distributed cannot beat
  single-node here; the point is that it is not slower -- the split, the
  Parquet round trip of partial results through rocepool and the merge cost
  about as much as they save. Scaling needs real hosts.
- With the data in memory, the same 11 queries take 0.73 s where RapidsSE
  took 50.9 s on the same host and data, and the load is ~5x faster.
- Reading from rocepool in every query costs ~200-300 ms per query here:
  that is the emulated RDMA and FUSE on 2 busy cores, i.e. this is the mode
  where rocepool's real bandwidth will show on hardware.

## A first comparison with smallpond (first prototype)

smallpond's own benchmark, and the one DeepSeek published a result for, is
GraySort (`benchmarks/gray_sort_benchmark.py`): 100-byte records with a
10-byte key; shuffle them by key prefix into P partitions through the file
system, then sort every partition. `rocepond/sort.py` is the same pipeline
in rocepond's Python API:

The shuffle is DuckDB's partitioned write (`COPY ... PARTITION_BY`), as in
smallpond's ShuffleNode with the DuckDB engine; every byte of shuffle and
sort is read from and written to rocepool. Validation: every output file in
key order, partition ranges in order, record count and a checksum of all
records equal to the input's.

Result (2026-11-14, simulated 2-node cluster: one host, 2 cores, 8 GiB RAM,
emulated RDMA; all validated):

| data | OST data on | shuffle | sort | shuffle + sort | per core |
|---|---|---|---|---|---|
| 1 GiB | RAM | 4.1 s | 4.8 s | 8.9 s = 6.7 GiB/min | 57 MiB/s |
| 1 GiB | disk tier | 4.3 s | 4.8 s | 9.1 s = 6.6 GiB/min | 56 MiB/s |
| 2 GiB | disk tier | 8.8 s | 10.8 s | 19.6 s = 6.1 GiB/min | 52 MiB/s |
| 4 GiB | disk tier | 18.6 s | 21.5 s | 40.1 s = 6.0 GiB/min | 51 MiB/s |

smallpond + 3FS, as published (3FS README): 110.5 TiB in 8,192 partitions in
30 min 14 s = 3.66 TiB/min, on 50 compute nodes of 192 physical cores each
plus 25 storage nodes: 6.65 MiB/s per compute core.

What this comparison is and is not:
- It is not the same software on the same machines: smallpond itself could
  not be run here (its dependencies -- Ray, pyarrow, polars, the duckdb
  Python package -- cannot be installed in this environment). It compares
  rocepond's measured throughput per core with smallpond's published one.
- Per core, rocepond + rocepool is ~8x the published figure (51-57 vs 6.65
  MiB/s), and here those 2 cores also run rocepool's servers and the
  emulated RDMA, while the published figure does not count the 25 storage
  nodes' cores.
- But the scales are five orders of magnitude apart, and small favours us:
  our partitions are 64-256 MiB (theirs ~13.5 GiB), nothing is replicated
  (3FS stores replicated data on SSDs), there are no stragglers, no
  scheduling of thousands of tasks, and the network is a memory copy. Our
  records are hexadecimal text from DuckDB, theirs gensort's.
- What it does show: the pipeline is correct on rocepool, its per-core cost
  is in a healthy range, and throughput holds as the data grows past the
  host's memory (4 GiB sorted = ~13 GiB written through rocepool).
- The comparison that would settle it: smallpond and rocepond on the same
  real hosts, each on rocepool and smallpond also on 3FS, at 100+ GiB.

## Found on the way (rocepool)

Two nodes creating the same directory tree at once (`mkdir -p` on every
node): the later one got EEXIST for the parent and then ENOENT creating
inside it, for up to the attribute cache's TTL -- its own cached "not there"
was not dropped when the MDT said the directory exists. Fixed in
`rocepool_fs.c` (FUSE client and intercept library): a cached miss is dropped
on EEXIST, and a missing parent is confirmed with the MDT before a create is
refused. Error paths only. (A directory another node created is otherwise
visible after at most the TTL, as before; `tests/test_native_fuse.py` has
both.)

A program that writes a file and stats it before closing saw a size short
by the tail still in the client's write buffer (the MDT learns the size at
the flush). The current DuckDB Python package checks every file it writes
this way and failed with "File size mismatch" on files over the 64 MiB
buffer. Fixed in `rocepool_fs.c` (`fs_stat`, used by the FUSE client's
lookup/getattr and the intercept library's stat): the reported size counts
the client's own buffered writes. `tests/test_native_fuse.py`:
`test_stat_counts_writes_still_buffered`.

## Limits

- The SQL is split by hand (per-partition part and merge), as in smallpond.
- Operations run eagerly, one stage at a time; there is no lazy plan, and
  no speculative re-execution of slow tasks (a time limit and retry only).
- Many small files cost: a P-way shuffle of M inputs writes up to M x P
  files, and rocepool pays per file. Choose P for partitions of hundreds of
  MiB, not for the task count.
- RayExecutor is verified on a local Ray and on a 2-node Ray cluster over
  the simulated rocepool cluster in CI; not yet on real hosts.
- rocepool has no rename: DuckDB writes with `USE_TMP_FILE false`, so a
  reader could see a file being written; stages do not overlap, and a retry
  rewrites its own files.
