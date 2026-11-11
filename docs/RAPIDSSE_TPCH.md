# RapidsSE on rocepool: RapidsSE's own TPC-H SF1 test

RapidsSE (the Rapids Storage Engine, Apache 2.0, upstream
https://bitbucket.org/rapidsdataproject/rapidsse/, full copy
https://github.com/RoCEPool/RapidsSE) runs with rocepool as its distributed
storage: every RapidsSE node's data disks are directories in one rocepool
namespace, read and written through rocepool's native client. RapidsSE's
own TPC-H SF1 regression test runs on it unchanged.

```bash
git clone -b master https://github.com/RoCEPool/RapidsSE /opt/RapidsSE   # git lfs installed
sudo python3 tools/rapidsse_tpch.py --rapidsse /opt/RapidsSE                       # simulated 2-node cluster
sudo ROCEPOOL_CLUSTER=cluster.json python3 tools/rapidsse_tpch.py --rapidsse /opt/RapidsSE   # real hosts
```

One command does everything, unattended, and stops whatever it started (also
on failure). Result: `rapidsse_tpch_report.json`.

## What runs

| step | how |
|---|---|
| build | RapidsSE from its unmodified source (`make release`), offline: its third-party tarballs come from its Git LFS. Two build accommodations, no source change: ANTLR's runtime is built without its unit tests (their googletest is a download at configure time), and on gcc >= 12 the warnings RapidsSE's `-Werror` was not written for stay warnings. TPC-H `dbgen` from the same tree. |
| storage | one MDT and one OST per node (native C servers), the native FUSE client mounted at `/mnt/rocepool_rapidsse` on every node |
| the disks | the plan's `/mnt/disk1`, `/mnt/disk2` (data files) and the cluster's `disk_paths` (`seData`, RapidsSE's unload/reload directory) become `<rocepool>/rapidsse/NODE<k>/disk1, disk2`, bind-mounted at one path on node k. Each node reads its own share of the data, as from local disks, and every node's data is in the shared pool. |
| data | TPC-H SF1 with dbgen, chunked like RapidsSE's `bootstrap/generateData.sh`: nodes x paths chunks (8), nation/region in each node's first path, written straight into rocepool |
| RapidsSE | one process per node, with `bootstrap/boot.fn`'s command line; ready when every node reports both cluster sockets (`waitForClusterBoot`'s test) |
| the test | `bamboo/perftest/configs/tests/tpch-sf1-2D2F.jmx`, the SF1 test of RapidsSE's daily regression job, run step by step as JMeter runs it (`tools/rapidsse_jmx.py`: enabled elements only, loops, variables, assertions, stop on the first failure, tearDown group), over RapidsSE's own JDBC driver, or with `--client wlp` over rocepool's WLP client |
| baseline | the same again with the disks on node-local storage (`--baseline local`), and every query's whole answer compared between the two (SHA-256 of all values) |

The plan, as RapidsSE ships it: create the 8 tables and 18 indexes, load
(`dload` of 4 files per node, `loadReplicated` for nation/region), compact
the indexes, check 6 row counts (lineitem 6001215, orders 1500000, partsupp
800000, part 200000, customer 150000, supplier 10000), then TPC-H Q1, Q3,
Q4, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q14, each 3 times. The other queries and
the pipeline queries are switched off in RapidsSE's plan and stay off.

## Result (2026-11-11, simulated 2-node cluster)

One host, 2 cores and 8 GiB RAM shared by both nodes; softverbs RDMA
emulator; RapidsSE v1.2.0-dev2-171 (90012081f5), 1 partition x 2 threads x
2880 MiB region per node; OST data on a file tier (all nodes share the host's
RAM with RapidsSE).

**Passed**: all 99 steps; the 6 row counts exact; all 39 query results
(11 queries x 3, and the 6 counts) identical to the local-disk run. Also
passed end to end with `--client wlp` (1 loop).

| | rocepool | local disk |
|---|---|---|
| data generated | 1.03 GiB, 8 chunks | same |
| load (all tables) | 32.5 s (32.3 MiB/s) | 29.6 s (35.4 MiB/s) |
| queries, sum of medians | 44.2 s | 43.1 s |

| query | rocepool, s | local, s |
|---|---|---|
| Q1 | 2.74 | 2.76 |
| Q3 | 2.03 | 2.11 |
| Q4 | 1.32 | 1.26 |
| Q6 | 0.99 | 0.96 |
| Q7 | 8.37 | 7.16 |
| Q8 | 8.63 | 8.41 |
| Q9 | 8.54 | 8.14 |
| Q10 | 3.69 | 3.87 |
| Q11 | 3.10 | 3.52 |
| Q12 | 1.96 | 1.95 |
| Q14 | 2.79 | 2.95 |

What the numbers say: here the load is bound by RapidsSE parsing on 2 shared
cores, not by the storage, and the queries run in RapidsSE's memory, so both
storages time the same within noise. The comparison that shows rocepool's
bandwidth needs real hosts (RapidsSE's regression cluster: 2 x r6id.16xlarge,
32 partitions x 64 threads, 480 GB per node).

Memory: RapidsSE needs its region to hold the data and each query's
intermediates. At 2 GiB per node Q8 ran out of region memory; RapidsSE then
asserted while cleaning up and never answered the client. The runner gives
RapidsSE what the host can spare (`--mem-mib`, default from free RAM) and
fails a step that does not answer within `--step-timeout` (default 1800 s)
instead of hanging.

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
  `tools/cluster.py` layer as the IO500 runs (`ROCEPOOL_CLUSTER`), with the
  RapidsSE tree at the same path on every host; not yet run there.
