"""rocepond on rocepool: DuckDB on every node, data and shuffle on rocepool,
the 11 TPC-H queries of RapidsSE's regression plan -- one command:

    sudo python3 tools/rocepond_tpch.py --rapidsse <RapidsSE tree | auto> --duckdb <DuckDB CLI | auto>

What it does (simulated 2-node cluster by default; ROCEPOOL_CLUSTER for real hosts):
  1. rocepool up: one MDT + one OST per node, the native FUSE client on every node
  2. TPC-H data with dbgen (from the RapidsSE tree, the same data as
     tools/rapidsse_tpch.py), every node generating its share, written into rocepool
  3. rocepond worker processes on every node (--slots each); lineitem and
     orders are repartitioned by order key into --partitions partitions -- the
     shuffle goes through rocepool as Parquet -- and the other six tables are
     broadcast
  4. the 11 queries (rocepond/tpch.py: a task per partition, then a merge),
     `--loops` times, every task reading its Parquet from rocepool
  5. the same queries, the plan's original SQL, as one task over the same
     Parquet files, as the reference: every distributed answer must equal it
  6. rocepond_tpch_report.json
"""
from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import rocepond  # noqa: E402
from rocepond import tpch  # noqa: E402
from tools.cluster import Cluster  # noqa: E402
from tools.rapidsse_tpch import (answers_match, clone_rapidsse, ensure_dbgen, ensure_duckdb,  # noqa: E402
                                 real_hosts_bootstrap, ship_to_peers)
from tools.rocepond_cluster import process_executor  # noqa: E402
from tools.rocepool_mount import RocepoolMount  # noqa: E402

MNT = "/mnt/rocepool_rocepond"


def log(msg: str) -> None:
    print(msg, flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rapidsse", required=True, help="RapidsSE tree (for its TPC-H dbgen), or auto: cloned here")
    ap.add_argument("--duckdb", default="auto", help="DuckDB CLI: a path, or auto (on PATH, else the release build)")
    ap.add_argument("--nodes", type=int, default=2, help="simulated cluster size (ignored with ROCEPOOL_CLUSTER)")
    ap.add_argument("--sf", default="1")
    ap.add_argument("--loops", type=int, default=3)
    ap.add_argument("--partitions", type=int, default=0, help="of lineitem and orders (default: 4 per slot)")
    ap.add_argument("--slots", type=int, default=0, help="worker processes per node (default: the node's cores)")
    ap.add_argument("--ost-tier", default="ram", help="ram, or a ROCEPOOL_OST_TIER spec ({node} = node index)")
    ap.add_argument("--workdir", default=os.path.join(ROOT, "rocepond_tpch_work"))
    ap.add_argument("--report", default=os.path.join(ROOT, "rocepond_tpch_report.json"))
    args = ap.parse_args()
    if os.geteuid() != 0:
        print("needs root (FUSE mounts, namespaces)", file=sys.stderr)
        return 2
    peers = real_hosts_bootstrap()
    if args.rapidsse == "auto":
        args.rapidsse = clone_rapidsse(os.path.join(ROOT, ".bench_cache", "rapidsse_src"))
    dbgen = ensure_dbgen(os.path.abspath(args.rapidsse))
    duck = ensure_duckdb(args.duckdb)
    if not duck:
        print("no DuckDB CLI (--duckdb)", file=sys.stderr)
        return 2
    if peers:
        ship_to_peers(peers, [os.path.dirname(dbgen)] + ([os.path.dirname(duck)] if duck.startswith(ROOT) else []))

    cluster = Cluster.current(args.nodes)
    n = len(cluster.nodes)
    cores = os.cpu_count() or 2
    share = 1 if cluster.real else n  # simulated nodes share this host's cores
    threads = max(1, cores // share)
    slots = args.slots or threads
    partitions = args.partitions or 4 * slots * n
    report = {"what": "rocepond (DuckDB per node, data and shuffle on rocepool): 11 TPC-H queries", "nodes": n,
              "sf": args.sf, "loops": args.loops, "slots_per_node": slots, "partitions": partitions}
    os.makedirs(args.workdir, exist_ok=True)
    cluster.up()
    env = cluster.env()
    if not cluster.real:
        env.update({"ROCEPOOL_RPC_TIMEOUT_MS": "60000"})
    env["PYTHONPATH"] = ROOT
    rp = RocepoolMount(cluster, env, args.workdir, MNT, args.ost_tier)
    ex = None
    try:
        rp.start()
        report["rocepool"] = rp.describe()
        log(f"== rocepool up: {n} MDT, {n} OST, mounted at {MNT} on all {n} nodes (OST data: "
            f"{report['rocepool']['ost_data']})")

        # 2. the data, every node its share, straight into rocepool
        sf = float(args.sf)
        per_node = max(1, min(threads, math.ceil(sf / 10)))
        total = n * per_node
        gen = os.path.join(ROOT, "tools", "rapidsse_gen.py")
        t0 = time.monotonic()
        procs = []
        for i in range(n):
            scratch = "/nvmedata/rocepond_gen" if cluster.real else f"/var/tmp/rocepond_gen/n{i}"
            procs.append(cluster.popen(i, [sys.executable, gen, "--dbgen", dbgen, "--sf", args.sf, "--total",
                                           str(total), "--first", str(i * per_node + 1), "--chunks-per-path",
                                           str(per_node), "--jobs", str(threads), "--scratch", scratch,
                                           "--paths", f"{MNT}/rocepond/raw/NODE{i + 1}"], env,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT))
        raw_bytes = 0
        for i, p in enumerate(procs):
            out = p.communicate()[0].decode(errors="replace")
            if p.returncode:
                raise RuntimeError(f"data generation on node {i} failed: {out[-2000:]}")
            raw_bytes += json.loads(out.strip().splitlines()[-1])["bytes"]
        report["data"] = {"raw_bytes": raw_bytes, "generate_s": round(time.monotonic() - t0, 1)}
        log(f"== TPC-H SF{args.sf}: {raw_bytes / 2**30:.2f} GiB of .tbl on rocepool "
            f"({report['data']['generate_s']} s, dbgen-bound)")

        # 3. workers, shuffle, broadcast
        ex = process_executor(cluster, env, slots, 1, duck)
        report["scheduler"] = ex.info
        sp = rocepond.Session(ex, f"{MNT}/rocepond")
        files = {t: [[f"{MNT}/rocepond/raw/NODE{k + 1}/{t}.tbl"] for k in range(n)] for t in tpch.SCHEMA}
        t0 = time.monotonic()
        tables = tpch.prepare(sp, files, partitions)
        prep = time.monotonic() - t0
        r = cluster.run(0, ["sh", "-c", f"du -sb {MNT}/rocepond/data | cut -f1"], env, net=False,
                        capture_output=True, text=True)
        pq_bytes = int((r.stdout or "0").strip() or 0)
        report["prepare"] = {"seconds": round(prep, 2), "parquet_bytes": pq_bytes, "steps": list(sp.stats)}
        log(f"== shuffle + broadcast through rocepool: {prep:.1f} s ({raw_bytes / 2**20 / prep:.0f} MiB/s of .tbl "
            f"in), {pq_bytes / 2**30:.2f} GiB of Parquet on rocepool, {partitions} partitions")

        # 4. the queries; the reference: the plan's SQL as one task (one DuckDB thread) over the same Parquet
        entry = report["queries"] = {}
        for q, fn in tpch.QUERIES.items():
            dist, single, verdict = [], [], None
            for _ in range(args.loops):
                t0 = time.monotonic()
                rows = fn(sp, tables)
                dist.append(time.monotonic() - t0)
                t0 = time.monotonic()
                ref = sp.sql(tpch.REFERENCE_SQL[q])
                single.append(time.monotonic() - t0)
                ok, why = answers_match(rows, ref)
                verdict = (ok, why) if verdict is None or not ok else verdict
            sp.drop_tmp()
            entry[q] = {"rocepond_ms": [round(x * 1000) for x in dist],
                        "single_task_ms": [round(x * 1000) for x in single],
                        "rocepond_median_ms": round(statistics.median(dist) * 1000),
                        "single_task_median_ms": round(statistics.median(single) * 1000),
                        "rows": len(rows), "same_answer": verdict[0], "check": verdict[1]}
            log(f"   {q:<4} rocepond {entry[q]['rocepond_median_ms']:>6} ms   one task "
                f"{entry[q]['single_task_median_ms']:>6} ms   rows {len(rows):>5}   "
                + ("same answer" if verdict[0] else f"DIFFERENT: {verdict[1]}"))
        report["rocepond_total_ms"] = sum(v["rocepond_median_ms"] for v in entry.values())
        report["single_task_total_ms"] = sum(v["single_task_median_ms"] for v in entry.values())
        report["answers_same"] = sum(v["same_answer"] for v in entry.values())
        log(f"== 11 queries (medians): rocepond, {partitions} partitions on {n} nodes x {slots} slots "
            f"{report['rocepond_total_ms'] / 1000:.2f} s; the plan's SQL as one task "
            f"{report['single_task_total_ms'] / 1000:.2f} s; answers the same: {report['answers_same']}/{len(entry)}")
        report["passed"] = report["answers_same"] == len(entry)
    except Exception as exc:  # noqa: BLE001
        report["error"] = f"{type(exc).__name__}: {exc}"
        report["passed"] = False
        log(f"== ERROR: {report['error'][:3000]}")
    finally:
        if ex is not None:
            ex.close()
        rp.stop()
        cluster.down()
        with open(args.report, "w") as f:
            json.dump(report, f, indent=1)
        log(f"== report: {args.report}")
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    sys.exit(main())
