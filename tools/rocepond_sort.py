"""A GraySort-style sort on rocepond + rocepool, in the shape of smallpond's
gray_sort_benchmark.py (rocepond/sort.py), with DeepSeek's published
smallpond + 3FS result beside it:

    sudo python3 tools/rocepond_sort.py [--gib 1] [--partitions 1024] [--executor process|ray]

rocepool up (one MDT + one OST per node, FUSE client on every node), the
scheduler's workers on every node, then: generate -> shuffle by key range
through rocepool (a task per input file) -> sort (a task per partition) ->
validate. The timed part is shuffle + sort, every byte of both read from and
written to rocepool.

--executor process: rocepond's own scheduler, --slots worker processes per node.
--executor ray:     a Ray cluster is started on the nodes (head on the first)
                    and the sort runs as Ray tasks; needs `pip install ray duckdb`.

The published figure (3FS README, GraySort with smallpond): 110.5 TiB in
8,192 partitions sorted in 30 min 14 s = 3.66 TiB/min, on 50 compute nodes
(192 physical cores, 2.2 TiB RAM, 1 x 200 Gbps each) and 25 storage nodes.
The two runs differ in scale by five orders of magnitude and in hardware, so
the report gives throughput per compute core next to the totals and says
what that does and does not compare.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import rocepond  # noqa: E402
from rocepond import sort  # noqa: E402
from tools.cluster import Cluster  # noqa: E402
from tools.rapidsse_tpch import ensure_duckdb  # noqa: E402
from rocepond.duck import have_module  # noqa: E402
from tools.rocepond_cluster import RayOnCluster, process_executor  # noqa: E402
from tools.rocepool_mount import RocepoolMount  # noqa: E402

MNT = "/mnt/rocepool_rocepond"
SMALLPOND_3FS = {  # https://github.com/deepseek-ai/3FS (README, "GraySort")
    "bytes": int(110.5 * 2**40), "seconds": 30 * 60 + 14, "partitions": 8192,
    "compute_nodes": 50, "cores_per_compute_node": 192, "storage_nodes": 25,
}


def log(msg: str) -> None:
    print(msg, flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--duckdb", default="auto", help="DuckDB CLI: a path, or auto (on PATH, else the release build)")
    ap.add_argument("--nodes", type=int, default=2, help="simulated cluster size (ignored with ROCEPOOL_CLUSTER)")
    ap.add_argument("--gib", type=float, default=1.0, help="data to sort, GiB (100-byte records)")
    ap.add_argument("--partitions", type=int, default=16, help="one sort task each (thousands are fine)")
    ap.add_argument("--splits", type=int, default=0, help="input files = shuffle tasks (default: one per slot)")
    ap.add_argument("--max-fanout", type=int, default=1024,
                    help="partitions one shuffle task writes; above it the shuffle takes two passes")
    ap.add_argument("--executor", default="process", choices=("process", "ray"))
    ap.add_argument("--slots", type=int, default=0, help="workers per node (default: the node's cores)")
    ap.add_argument("--threads", type=int, default=1, help="DuckDB threads per task")
    ap.add_argument("--ost-tier", default="ram", help="ram, or a ROCEPOOL_OST_TIER spec ({node} = node index)")
    ap.add_argument("--workdir", default=os.path.join(ROOT, "rocepond_sort_work"))
    ap.add_argument("--report", default=os.path.join(ROOT, "rocepond_sort_report.json"))
    args = ap.parse_args()
    if os.geteuid() != 0:
        print("needs root (FUSE mounts, namespaces)", file=sys.stderr)
        return 2
    duck = "" if have_module() and args.duckdb == "auto" else ensure_duckdb(args.duckdb)
    if not duck and not have_module():
        print("no DuckDB: pip install duckdb, or --duckdb <CLI>", file=sys.stderr)
        return 2
    cluster = Cluster.current(args.nodes)
    n = len(cluster.nodes)
    cores = os.cpu_count() or 2
    share = 1 if cluster.real else n
    slots = args.slots or max(1, cores // share // args.threads)
    total_cores = cores * (n if cluster.real else 1)
    report = {"what": "GraySort-style sort on rocepond + rocepool (rocepond/sort.py)", "nodes": n,
              "slots_per_node": slots, "threads_per_task": args.threads, "cores": total_cores,
              "cores_note": "all cores of the cluster" if cluster.real else
              "one host: its cores run every worker, rocepool's servers and clients, and the emulated RDMA",
              "executor": args.executor}
    os.makedirs(args.workdir, exist_ok=True)
    cluster.up()
    env = cluster.env()
    if not cluster.real:
        env.update({"ROCEPOOL_RPC_TIMEOUT_MS": "60000"})
    env["PYTHONPATH"] = ROOT
    rp = RocepoolMount(cluster, env, args.workdir, MNT, args.ost_tier)
    ex = ray_cluster = None
    try:
        rp.start()
        report["rocepool"] = rp.describe()
        log(f"== rocepool up: {n} MDT, {n} OST, mounted at {MNT} on all {n} nodes (OST data: "
            f"{report['rocepool']['ost_data']})")

        root = f"{MNT}/rocepond"
        if args.executor == "ray":
            ray_cluster = RayOnCluster(cluster, env, slots)
            ray_cluster.start()
            log(f"== Ray up: head on node 0 ({ray_cluster.address}), {n} nodes x {slots} CPUs")
            res = ray_cluster.run_driver(["-m", "rocepond.sort", "--root", root, "--gib", str(args.gib),
                                          "--partitions", str(args.partitions), "--splits", str(args.splits),
                                          "--threads", str(args.threads), "--duckdb", duck, "--no-ship-package",
                                          "--max-fanout", str(args.max_fanout)])
        else:
            ex = process_executor(cluster, env, slots, args.threads, duck)
            sp = rocepond.Session(ex, root)
            res = sort.run(sp, int(args.gib * 2**30), args.partitions, args.splits or None, args.max_fanout)
            res["executor"], res["stages"] = ex.info, ex.stats
        report["scheduler"] = res.pop("executor")
        report["stages"] = res.pop("stages")
        log(f"== scheduler: {json.dumps(report['scheduler'])}")
        if args.executor == "ray" and report["scheduler"]["nodes"] != n:
            raise RuntimeError(f"Ray sees {report['scheduler']['nodes']} nodes, the cluster has {n}")
        report["sort"] = res
        v = res["validation"]
        log(f"== {res['bytes'] / 2**30:.2f} GiB ({res['records']:,} records) in {res['partitions']} partitions: "
            f"generate {res['generate_s']} s (not timed), shuffle {res['shuffle_s']} s, sort {res['sort_s']} s")
        for st in report["stages"]:
            log(f"   {st['stage']:<58} {st['tasks']:>6} tasks {st['wall_s']:>8.2f} s  {st['tasks_per_s']:>7} tasks/s  "
                f"retries {st['retries']}")
        log(f"== validation: {'PASSED' if res['valid'] else 'FAILED'} -- {v['records_out']:,} records out, "
            f"{v['order_violations_in_files']} order violations, partition ranges in order: "
            f"{v['partition_ranges_in_order']}, checksum equal: {v['checksum_equal']}")
        ours = res["bytes"] / res["total_s"]
        ref = SMALLPOND_3FS
        theirs = ref["bytes"] / ref["seconds"]
        ref_cores = ref["compute_nodes"] * ref["cores_per_compute_node"]
        report["comparison"] = {
            "rocepond_rocepool": {"bytes": res["bytes"], "seconds": res["total_s"],
                                  "gib_per_min": round(ours * 60 / 2**30, 3), "cores": total_cores,
                                  "mib_per_s_per_core": round(ours / 2**20 / total_cores, 2)},
            "smallpond_3fs_published": {"bytes": ref["bytes"], "seconds": ref["seconds"],
                                        "gib_per_min": round(theirs * 60 / 2**30, 1), "compute_cores": ref_cores,
                                        "storage_nodes_not_counted": ref["storage_nodes"],
                                        "mib_per_s_per_compute_core": round(theirs / 2**20 / ref_cores, 2)},
        }
        c = report["comparison"]
        log(f"== shuffle + sort: {res['total_s']} s = {c['rocepond_rocepool']['gib_per_min']} GiB/min on "
            f"{total_cores} cores = {c['rocepond_rocepool']['mib_per_s_per_core']} MiB/s per core")
        log(f"== smallpond + 3FS (published): {c['smallpond_3fs_published']['gib_per_min']} GiB/min on {ref_cores} "
            f"compute cores (+{ref['storage_nodes']} storage nodes) = "
            f"{c['smallpond_3fs_published']['mib_per_s_per_compute_core']} MiB/s per compute core")
        report["passed"] = bool(res["valid"])
    except Exception as exc:  # noqa: BLE001
        report["error"] = f"{type(exc).__name__}: {exc}"
        report["passed"] = False
        log(f"== ERROR: {report['error'][:3000]}")
    finally:
        if ex is not None:
            ex.close()
        if ray_cluster is not None:
            ray_cluster.stop()
        rp.stop()
        cluster.down()
        with open(args.report, "w") as f:
            json.dump(report, f, indent=1)
        log(f"== report: {args.report}")
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    sys.exit(main())
