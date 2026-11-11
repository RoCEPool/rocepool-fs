# RapidsSE on rocepool: RapidsSE's own TPC-H SF1 test

RapidsSE (the Rapids Storage Engine, Apache 2.0, upstream
https://bitbucket.org/rapidsdataproject/rapidsse/, full copy
https://github.com/RoCEPool/RapidsSE) runs with rocepool as its distributed
storage: every RapidsSE node's data disks are directories in one rocepool
namespace, read and written through rocepool's native client. RapidsSE's
own TPC-H SF1 regression test runs on it unchanged.

```bash
sudo python3 tools/rapidsse_tpch.py --rapidsse auto      # simulated 2-node cluster; auto = clone RapidsSE here
sudo python3 tools/rapidsse_tpch.py --rapidsse auto \
     --plan bamboo/perftest/configs/tests/tpch-sf100-2D2F.jmx --sf 100              # SF100, the same way
```

On AWS (2 x hpc6id.32xlarge, EFA), one workflow run does everything --
cluster setup, RapidsSE build and shipping, SF100 data, RapidsSE on rocepool,
DuckDB, RapidsSE on the instance NVMe -- and brings
`rapidsse_tpch_report.json` back:

    aws-test: mode=efa type=hpc6id.32xlarge nodes=2 max_hours=3 budget_usd=45
              command="python3 tools/rapidsse_tpch.py --rapidsse auto
                       --plan bamboo/perftest/configs/tests/tpch-sf100-2D2F.jmx --sf 100"

(With `HEAD_IP`/`PEER_IPS` set by the workflow, the runner installs what
RapidsSE's build and JDBC client need, runs `run.sh --bench none` for the
rocepool cluster, and ships the built RapidsSE tree and dbgen to the same
path on every host.)

One command does everything, unattended, and stops whatever it started (also
on failure). Result: `rapidsse_tpch_report.json`.

## What runs

| step | how |
|---|---|
| build | RapidsSE from its unmodified source (`make release`), offline: its third-party tarballs come from its Git LFS. Two build accommodations, no source change: ANTLR's runtime is built without its unit tests (their googletest is a download at configure time), and on gcc >= 12 the warnings RapidsSE's `-Werror` was not written for stay warnings. TPC-H `dbgen` from the same tree. |
| storage | one MDT and one OST per node (native C servers), the native FUSE client mounted at `/mnt/rocepool_rapidsse` on every node |
| the disks | the plan's `/mnt/disk1`, `/mnt/disk2` (data files) and the cluster's `disk_paths` (`seData`, RapidsSE's unload/reload directory) become `<rocepool>/rapidsse/NODE<k>/disk1, disk2`, bind-mounted at one path on node k. Each node reads its own share of the data, as from local disks, and every node's data is in the shared pool. |
| data | TPC-H with dbgen, chunked like RapidsSE's `bootstrap/generateData.sh`: nodes x paths x chunks-per-path, nation/region in each node's first path. Generated in parallel on every node (`tools/rapidsse_gen.py`; ~SF/10 chunks per path, since each dbgen start costs ~4 s), chunks on node-local scratch (`/nvmedata` on AWS), then one sequential write per file into the disks |
| RapidsSE | one process per node, with `bootstrap/boot.fn`'s command line; ready when every node reports both cluster sockets (`waitForClusterBoot`'s test) |
| the test | `bamboo/perftest/configs/tests/tpch-sf1-2D2F.jmx`, the SF1 test of RapidsSE's daily regression job, run step by step as JMeter runs it (`tools/rapidsse_jmx.py`: enabled elements only, loops, variables, assertions, stop on the first failure, tearDown group), over RapidsSE's own JDBC driver, or with `--client wlp` over rocepool's WLP client |
| DuckDB | after RapidsSE on rocepool, on node 0: the same `.tbl` files, read from rocepool's shared namespace (every node's share is there), loaded into an in-memory DuckDB (`tools/rapidsse_duckdb.py`, the DuckDB CLI; on hosts without one its official release build is fetched), the plan's own 11 query texts, as many runs as the plan. Each answer compared with RapidsSE's, exactly and by TPC-H's validation rules. |
| baseline | RapidsSE again with the disks on node-local storage (`--baseline local`; the instance NVMe on AWS), and every query's whole answer compared between the two storages (SHA-256 of all values) |

The plan, as RapidsSE ships it: create the 8 tables and 18 indexes, load
(`dload` of 4 files per node, `loadReplicated` for nation/region), compact
the indexes, check 6 row counts (lineitem 6001215, orders 1500000, partsupp
800000, part 200000, customer 150000, supplier 10000), then TPC-H Q1, Q3,
Q4, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q14, each 3 times. The other queries and
the pipeline queries are switched off in RapidsSE's plan and stay off.

## Result (2026-11-11, simulated 2-node cluster)

One host, 2 cores and 8 GiB RAM shared by everything; softverbs RDMA
emulator; RapidsSE v1.2.0-dev2-171 (90012081f5), 1 partition x 2 threads x
2752 MiB region per node; OST data on a file tier; DuckDB v1.5.6, 2 threads.

**Passed**: all 99 steps on both storages; the 6 row counts exact; all 39
query results identical between rocepool and the local disks. Also passed
end to end with `--client wlp` (1 loop).

| | RapidsSE, rocepool | RapidsSE, local disk | DuckDB (same data, from rocepool) |
|---|---|---|---|
| data preparation (1.03 GiB) | 108 s | 108 s | (the same files) |
| load | 38.7 s | 36.5 s | 7.2 s |
| 11 queries, sum of medians | 50.9 s | 52.3 s | 0.66 s |

| query | RapidsSE rocepool, s | RapidsSE local, s | DuckDB, s |
|---|---|---|---|
| Q1 | 3.79 | 3.68 | 0.140 |
| Q3 | 2.30 | 2.38 | 0.040 |
| Q4 | 1.59 | 1.69 | 0.043 |
| Q6 | 1.19 | 1.19 | 0.015 |
| Q7 | 9.01 | 9.37 | 0.046 |
| Q8 | 9.46 | 10.05 | 0.030 |
| Q9 | 9.86 | 9.87 | 0.138 |
| Q10 | 4.58 | 4.58 | 0.104 |
| Q11 | 3.87 | 4.13 | 0.008 |
| Q12 | 2.49 | 2.50 | 0.069 |
| Q14 | 2.78 | 2.87 | 0.028 |

What the numbers say: here the load is bound by RapidsSE parsing on 2 shared
cores, not by the storage, and the queries run in RapidsSE's memory, so both
storages time the same within noise. On this host DuckDB is ~5x faster to
load and ~77x faster on the queries (DuckDB uses both cores in one process;
RapidsSE's 2 nodes share them with each other and with rocepool's servers).
The comparison that shows rocepool's bandwidth, and RapidsSE at its intended
scale, needs real hosts (RapidsSE's regression: 2 x r6id.16xlarge, 32
partitions x 64 threads, 480 GB per node).

**Answers.** RapidsSE's answers equal DuckDB's exactly in 5 of 11 queries and
are within TPC-H's validation rules (spec 2.1.3.5: counts exact, sums within
100, averages and ratios within 1%) in 10 of 11. Q1 is not: its
`sum(l_extendedprice * (1 - l_discount))` is 53758257382.21 where TPC-H's
answer (and DuckDB's) is 53758257134.87, 247.3 off. That is exactly the sum
of every row's product rounded to 2 decimals (checked in DuckDB with
`sum(round(p * (1 - d), 2))`; `sum_charge` likewise, to the cent): RapidsSE
keeps DECIMAL(15,2) scale after each multiplication, where SQL keeps the
exact product. The error grows with the row count. RapidsSE's own plan checks
only row counts, so it never showed. Storage plays no part: rocepool and the
local disks give identical answers.

Memory: RapidsSE needs its region to hold the data and each query's
intermediates. At 2 GiB per node Q8 ran out of region memory; RapidsSE then
asserted while cleaning up and never answered the client. The runner gives
RapidsSE what the host can spare (`--mem-mib`, default from free RAM, capped
by /dev/shm) and fails a step that does not answer within `--step-timeout`
(default 1800 s) instead of hanging.

## Fixed on the way

- **RapidsSE mirror had no Git LFS objects**: a clone of
  github.com/RoCEPool/RapidsSE could not build (thrift/boost tarballs, the
  JDBC driver were pointers). The mirror job now copies them into the
  repository's LFS and as `rapidsse-lfs.tar` on its `backup` release (430
  objects, 255 MiB; a few objects of old branches are missing upstream too).
- **rocepool's WLP client had never talked to a real RapidsSE** and failed at
  the first message. Against RapidsSE's AdaptorWL: the header's size counts
  the header itself; the server sends an 8-byte `RapidsSE` welcome on
  connect; the handshake needs ClearTextTunnelRequest and SessionChangeRequest
  (states TUNNELED, READY); a failed statement still ends with a
  QueryExecuteResponse; DATE / TIME / TIMESTAMP / INTERVAL values (and i8) are
  decoded, DECIMAL comes back as `decimal.Decimal` (it was dropped), and
  fields the client does not declare are skipped (Thrift's rule) instead of
  aborting the connection.
- **Simulated cluster nodes had no default route**, so broadcasts to
  255.255.255.255 (RapidsSE's cluster heartbeat) went nowhere and each node
  saw only itself. Each namespace now has a default route via the bridge, as
  a real host does.

## Limits

- rocepool's metadata protocol has no rename. RapidsSE itself does not
  rename on its data disks; `generateData.sh` does (`<table>.tbl.<n>` ->
  `<table>.tbl`), so the runner lets dbgen write into a local FIFO of that
  name and streams it into `<table>.tbl` on rocepool (one write, same bytes).
- `boot.fn` checks readiness with a second `rapidsse --command status`
  process attached to the node's shared memory; on this host that process's
  exit took the server down (orderly exit, then SIGSEGV in its cleanup). The
  runner sends the same `status;` over the client port instead.
- RapidsSE's unload/reload directories are on rocepool, but the regression
  plan does not unload or reload, so that path is set up, not exercised.
- Run so far on the simulated cluster only. Real hosts go through the same
  `tools/cluster.py` layer and `run.sh --bench none` setup as the IO500 runs;
  that path is unit-tested (the command it builds), not yet run on hosts.
- The DuckDB comparison runs on one node (DuckDB is single-node); RapidsSE
  uses all nodes.
