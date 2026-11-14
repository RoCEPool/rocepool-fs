# rocepond: DuckDB on every node, data and shuffle on rocepool (prototype)

The design of DeepSeek's smallpond (DuckDB + their 3FS file system), on
rocepool: each node runs DuckDB; tables, the shuffle between nodes and the
partial results are Parquet files in rocepool's shared namespace. A small
Python API in smallpond's style:

```python
import rocepond
sp = rocepond.Session(executor, root="/mnt/rocepool/rocepond")
lineitem = sp.read_csv("lineitem", files_by_worker, columns).repartition(hash_by="l_orderkey")
orders   = sp.read_csv("orders",   files_by_worker, columns).repartition(hash_by="o_orderkey")
sp.read_csv("part", files_by_worker, columns).broadcast()

partial = sp.partial_sql("""select l_shipmode, count(*) as n from {0}, {1}
                            where o_orderkey = l_orderkey group by l_shipmode""", orders, lineitem)
rows = sp.sql("select l_shipmode, sum(n) from {0} group by l_shipmode order by 1", partial)
```

- `repartition(hash_by=K)`: every worker reads its share of the source and
  writes one Parquet file per partition into rocepool; worker p then sees
  partition p. Tables repartitioned on their join key are co-partitioned, so
  that join is local to a node. The shuffle goes through the file system.
- `broadcast()`: one copy on rocepool, read by every worker.
- `partial_sql(sql, ...)`: runs on every worker over its partition; each
  result is a Parquet file on rocepool.
- `sql(sql, ...)`: runs on one worker over broadcast tables and partial
  results; returns the rows.
- `set_cache("memory")`: workers load their partition into DuckDB's memory
  once; the default reads Parquet from rocepool in every query.

As in smallpond, the user chooses the partitioning and writes the
per-partition and the merging SQL; nothing rewrites a query automatically.

**Scheduling.** Tasks go to one long-lived worker process per node
(`rocepond/executor.py`, started over tools/cluster.py: a namespace when
simulated, ssh on real hosts). Ray is not used yet: the environment this was
built in cannot install packages. The executor is the one layer to replace
(a worker becomes a Ray actor pinned to its node); the session only calls
`run()` and `run_all()`. DuckDB is driven through its CLI for the same
reason (no `duckdb` Python package); `rocepond/duck.py` is that one layer.

## The 11 TPC-H queries

    sudo python3 tools/rocepond_tpch.py --rapidsse <RapidsSE tree | auto> --duckdb <DuckDB CLI | auto>

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

## A first comparison with smallpond: GraySort

    sudo python3 tools/rocepond_sort.py --duckdb <DuckDB CLI | auto> [--gib 1] [--partitions 16]

smallpond's own benchmark, and the one DeepSeek published a result for, is
GraySort (`benchmarks/gray_sort_benchmark.py`): 100-byte records with a
10-byte key; shuffle them by key prefix into P partitions through the file
system, then sort every partition. `rocepond/sort.py` is the same pipeline
in rocepond's Python API:

```python
records = sp.generate("sort_input", "SELECT left(a, 10) AS keys, ... AS records FROM range(...)")
buckets = records.repartition(16, by="('0x' || left(keys, 1))::INTEGER")       # shuffle through rocepool
result  = buckets.map_partitions("SELECT records FROM {0} ORDER BY keys", name="sort_output", fmt="lines")
```

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

## Limits

- A prototype: SQL split by hand per query, one partition per node, no
  retry of failed tasks, no spilling of large partial results beyond what
  DuckDB does itself.
- Simulated cluster only so far; real hosts go through the same
  tools/cluster.py layer.
- rocepool has no rename: DuckDB writes Parquet with `USE_TMP_FILE false`.
