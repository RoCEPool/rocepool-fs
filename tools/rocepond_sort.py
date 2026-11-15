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
--executor process,ray: both, one after the other, on the same rocepool.

On hosts started by tools/aws_cluster.py (--command, HEAD_IP / PEER_IPS set)
it first brings the real cluster up itself: rocepool's setup as run.sh does
it (no benchmark), and duckdb + ray in a Python environment on every host.

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
from tools.rocepond_cluster import VENV, RayOnCluster, process_executor, real_hosts_bootstrap  # noqa: E402
from tools.rocepool_mount import RocepoolMount  # noqa: E402

MNT = "/mnt/rocepool_rocepond"
SMALLPOND_3FS = {  # https://github.com/deepseek-ai/3FS (README, "GraySort")
    "bytes": int(110.5 * 2**40), "seconds": 30 * 60 + 14, "partitions": 8192,
    "compute_nodes": 50, "cores_per_compute_node": 192, "storage_nodes": 25,
    "per_node": "192 physical cores, 2.2 TiB RAM, 1 x 200 Gbps InfiniBand NIC",
}


def physical_cores() -> int:
    """This host's physical cores (vCPUs / threads per core)."""
    try:
        sib = open("/sys/devices/system/cpu/cpu0/topology/thread_siblings_list").read().strip()
        per = sum(int(b) - int(a) + 1 if "-" in x else 1 for x in sib.split(",") for a, b in [(x.split("-") + [x])[:2]])
        return max(1, (os.cpu_count() or 1) // max(1, per))
    except (OSError, ValueError):
        return os.cpu_count() or 1


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
    ap.add_argument("--executor", default="process", help="process, ray, or process,ray (both, in turn)")
    ap.add_argument("--memory-limit", default="", help="per DuckDB, e.g. 2GiB (default: host memory / workers)")
    ap.add_argument("--diagnose", action="store_true",
                    help="first: every worker times the sort's I/O on local disk and on rocepool (see diagnose())")
    ap.add_argument("--slots", type=int, default=0, help="workers per node (default: the node's cores)")
    ap.add_argument("--threads", type=int, default=1, help="DuckDB threads per task")
    ap.add_argument("--ost-tier", default="ram", help="ram, or a ROCEPOOL_OST_TIER spec ({node} = node index)")
    ap.add_argument("--workdir", default=os.path.join(ROOT, "rocepond_sort_work"))
    ap.add_argument("--report", default=os.path.join(ROOT, "rocepond_sort_report.json"))
    args = ap.parse_args()
    executors = [e for e in args.executor.split(",") if e]
    if not executors or any(e not in ("process", "ray") for e in executors):
        ap.error("--executor: process, ray, or process,ray")
    if os.geteuid() != 0:
        print("needs root (FUSE mounts, namespaces)", file=sys.stderr)
        return 2
    if os.environ.get("HEAD_IP") and not os.environ.get("ROCEPOOL_CLUSTER"):  # real hosts (tools/aws_cluster.py)
        os.environ["ROCEPOOL_CLUSTER"] = real_hosts_bootstrap(log)
        log(f"== again with {VENV}/bin/python3 (duckdb, ray)")
        os.execv(f"{VENV}/bin/python3", [f"{VENV}/bin/python3", os.path.abspath(__file__)] + sys.argv[1:])
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
    total_phys = physical_cores() * (n if cluster.real else 1)
    report = {"what": "GraySort-style sort on rocepond + rocepool (rocepond/sort.py)", "nodes": n,
              "slots_per_node": slots, "threads_per_task": args.threads, "vcpus": total_cores,
              "physical_cores": total_phys, "memory_limit_per_duckdb": args.memory_limit or "auto",
              "cores_note": "all cores of the cluster" if cluster.real else
              "one host: its cores run every worker, rocepool's servers and clients, and the emulated RDMA",
              "host": host_facts(), "runs": []}
    os.makedirs(args.workdir, exist_ok=True)
    cluster.up()
    env = cluster.env()
    if not cluster.real:
        env.update({"ROCEPOOL_RPC_TIMEOUT_MS": "60000"})
    env["PYTHONPATH"] = ROOT
    rp = RocepoolMount(cluster, env, args.workdir, MNT, args.ost_tier)
    try:
        rp.start()
        report["rocepool"] = rp.describe()
        log(f"== rocepool up: {n} MDT, {n} OST, mounted at {MNT} on all {n} nodes (OST data: "
            f"{report['rocepool']['ost_data']})")
        if args.diagnose:
            report["diagnosis"] = diagnose(cluster, env, n, slots, duck, args)
        for kind in executors:
            # each run starts from an empty directory: the last one's data would hold the OSTs' memory
            cluster.run(0, ["rm", "-rf", f"{MNT}/rocepond"], env, net=False)
            report["runs"].append(one_run(kind, cluster, env, n, slots, duck, args, total_cores, total_phys))
        report["passed"] = all(r.get("passed") for r in report["runs"])
    except Exception as exc:  # noqa: BLE001
        report["error"] = f"{type(exc).__name__}: {exc}"
        report["passed"] = False
        log(f"== ERROR: {report['error'][:3000]}")
    finally:
        rp.stop()
        cluster.down()
        with open(args.report, "w") as f:
            json.dump(report, f, indent=1)
        log(f"== report: {args.report}")
    return 0 if report.get("passed") else 1


def host_facts() -> dict:
    out = {"vcpus_per_host": os.cpu_count(), "physical_cores_per_host": physical_cores()}
    try:
        with open("/proc/meminfo") as f:
            out["memory_gib_per_host"] = round(int(f.readline().split()[1]) / 2**20, 1)
        with open("/proc/cpuinfo") as f:
            out["cpu"] = next((ln.split(":", 1)[1].strip() for ln in f if ln.startswith("model name")), "")
    except (OSError, ValueError, IndexError):
        pass
    if os.environ.get("ROCEPOOL_INSTANCE_TYPE"):
        out["instance_type"] = os.environ["ROCEPOOL_INSTANCE_TYPE"]
    return out


DIAG_STEPS = ["generate (local disk)", "copy local disk -> rocepool", "read (local disk)", "read (rocepool)",
              "partitioned write (local disk)", "partitioned write (rocepool)"]


def diagnose(cluster, env, n: int, slots: int, duck: str, args) -> dict:
    """Where the sort's time goes: every worker (all at once, as in the
    sort) takes its share of the data through the same DuckDB operations
    twice -- on the host's local disk and on rocepool -- and times each:
    plain Parquet write and read, and the shuffle's partitioned write into
    --partitions directories. Same data, same DuckDB, same concurrency: the
    difference is the file system."""
    tasks_n = n * slots
    records = int(args.gib * 2**30 / sort.RECORD_BYTES / tasks_n)
    p = args.partitions
    log(f"== diagnosis: {tasks_n} tasks at once, {records * sort.RECORD_BYTES / 2**20:.0f} MiB each, local disk vs rocepool")
    tasks = []
    for k in range(tasks_n):
        loc, rp = f"/var/tmp/rocepond_diag/{k}", f"{MNT}/rocepond_diag"
        gen = (f"SELECT left(a, 10) AS keys, a || b || c || left(d, 3) AS records FROM (SELECT md5(i::VARCHAR || 'a') AS a, "
               f"md5(i::VARCHAR || 'b') AS b, md5(i::VARCHAR || 'c') AS c, md5(i::VARCHAR || 'd') AS d "
               f"FROM range({k * records}, {(k + 1) * records}) t(i))")
        bucket = f"*, ((('0x' || left(keys, 8))::BIGINT * {p}) >> 32)::INTEGER AS bucket"
        pw = (f"(FORMAT parquet, PARTITION_BY (bucket), OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'from-{k}-{{i}}', "
              "COMPRESSION uncompressed)")
        tasks.append({"name": f"diagnose {k}", "mkdirs": [loc, rp, f"{rp}/pw"], "sql": [
            f"COPY ({gen}) TO '{loc}/in.parquet' (FORMAT parquet, COMPRESSION uncompressed)",
            f"COPY (SELECT * FROM '{loc}/in.parquet') TO '{rp}/in-{k}.parquet' "
            "(FORMAT parquet, COMPRESSION uncompressed, USE_TMP_FILE false)",
            f"SELECT bit_xor(hash(records)) FROM '{loc}/in.parquet'",
            f"SELECT bit_xor(hash(records)) FROM '{rp}/in-{k}.parquet'",
            f"COPY (SELECT {bucket} FROM '{loc}/in.parquet') TO '{loc}/pw' {pw}",
            f"COPY (SELECT {bucket} FROM '{loc}/in.parquet') TO '{rp}/pw' {pw}"]})
    ex = process_executor(cluster, env, slots, args.threads, duck, memory_limit=args.memory_limit, max_retries=0)
    out = {"tasks": tasks_n, "mib_per_task": round(records * sort.RECORD_BYTES / 2**20, 1), "partitions": p}
    try:
        res = ex.run_all(tasks, "diagnose")
    finally:
        ex.close()
        for node in cluster.nodes:
            cluster.run(node.index, ["rm", "-rf", "/var/tmp/rocepond_diag"], env, net=False)
        cluster.run(0, ["rm", "-rf", f"{MNT}/rocepond_diag"], env, net=False)
    mib = records * sort.RECORD_BYTES / 2**20
    for j, name in enumerate(DIAG_STEPS):
        secs = sorted(r["step_s"][j] for r in res)
        out[name] = {"median_s": secs[len(secs) // 2], "max_s": secs[-1],
                     "mib_per_s_per_task": round(mib / secs[len(secs) // 2], 1),
                     "mib_per_s_all_tasks": round(mib * tasks_n / secs[len(secs) // 2], 0)}
        log(f"   {name:<34} median {secs[len(secs) // 2]:>7.2f} s  max {secs[-1]:>7.2f} s  "
            f"{out[name]['mib_per_s_per_task']:>7} MiB/s per task  {out[name]['mib_per_s_all_tasks']:>8.0f} MiB/s all")
    return out


def one_run(kind: str, cluster, env: dict, n: int, slots: int, duck: str, args, total_cores: int,
            total_phys: int) -> dict:
    run = {"executor": kind}
    ex = ray_cluster = None
    root = f"{MNT}/rocepond"
    log(f"== run: executor {kind}, {args.gib} GiB, {args.partitions} partitions, {n} nodes x {slots} slots")
    try:
        if kind == "ray":
            ray_cluster = RayOnCluster(cluster, env, slots)
            ray_cluster.start()
            log(f"== Ray up: head on node 0 ({ray_cluster.address}), {n} nodes x {slots} CPUs")
            res = ray_cluster.run_driver(["-m", "rocepond.sort", "--root", root, "--gib", str(args.gib),
                                          "--partitions", str(args.partitions), "--splits", str(args.splits),
                                          "--threads", str(args.threads), "--duckdb", duck, "--no-ship-package",
                                          "--max-fanout", str(args.max_fanout), "--memory-limit", args.memory_limit])
        else:
            ex = process_executor(cluster, env, slots, args.threads, duck, memory_limit=args.memory_limit)
            sp = rocepond.Session(ex, root)
            res = sort.run(sp, int(args.gib * 2**30), args.partitions, args.splits or None, args.max_fanout)
            res["executor"], res["stages"] = ex.info, ex.stats
        run["scheduler"] = res.pop("executor")
        run["stages"] = res.pop("stages")
        log(f"== scheduler: {json.dumps(run['scheduler'])}")
        if kind == "ray" and run["scheduler"]["nodes"] != n:
            raise RuntimeError(f"Ray sees {run['scheduler']['nodes']} nodes, the cluster has {n}")
        run["sort"] = res
        v = res["validation"]
        log(f"== {res['bytes'] / 2**30:.2f} GiB ({res['records']:,} records) in {res['partitions']} partitions: "
            f"generate {res['generate_s']} s (not timed), shuffle {res['shuffle_s']} s, sort {res['sort_s']} s")
        for st in run["stages"]:
            log(f"   {st['stage']:<58} {st['tasks']:>6} tasks {st['wall_s']:>8.2f} s  {st['tasks_per_s']:>7} tasks/s  "
                f"retries {st['retries']}")
        log(f"== validation ({kind}): {'PASSED' if res['valid'] else 'FAILED'} -- {v['records_out']:,} records out, "
            f"{v['order_violations_in_files']} order violations, partition ranges in order: "
            f"{v['partition_ranges_in_order']}, checksum equal: {v['checksum_equal']}")
        ours = res["bytes"] / res["total_s"]
        ref = SMALLPOND_3FS
        theirs = ref["bytes"] / ref["seconds"]
        ref_cores = ref["compute_nodes"] * ref["cores_per_compute_node"]
        run["comparison"] = {
            "rocepond_rocepool": {"bytes": res["bytes"], "seconds": res["total_s"],
                                  "gib_per_min": round(ours * 60 / 2**30, 3), "vcpus": total_cores,
                                  "physical_cores": total_phys,
                                  "mib_per_s_per_vcpu": round(ours / 2**20 / total_cores, 2),
                                  "mib_per_s_per_physical_core": round(ours / 2**20 / total_phys, 2),
                                  "storage": "on the same hosts (one MDT + one OST per host, data in RAM)"},
            "smallpond_3fs_published": {"bytes": ref["bytes"], "seconds": ref["seconds"],
                                        "gib_per_min": round(theirs * 60 / 2**30, 1), "compute_cores": ref_cores,
                                        "storage_nodes_not_counted": ref["storage_nodes"],
                                        "mib_per_s_per_compute_core": round(theirs / 2**20 / ref_cores, 2)},
        }
        c = run["comparison"]
        log(f"== shuffle + sort ({kind}): {res['total_s']} s = {c['rocepond_rocepool']['gib_per_min']} GiB/min = "
            f"{c['rocepond_rocepool']['mib_per_s_per_physical_core']} MiB/s per physical core "
            f"({c['rocepond_rocepool']['mib_per_s_per_vcpu']} per vCPU; {total_phys} cores, {total_cores} vCPUs)")
        log(f"== smallpond + 3FS (published): {c['smallpond_3fs_published']['gib_per_min']} GiB/min on {ref_cores} "
            f"compute cores (+{ref['storage_nodes']} storage nodes) = "
            f"{c['smallpond_3fs_published']['mib_per_s_per_compute_core']} MiB/s per compute core")
        run["passed"] = bool(res["valid"])
    except Exception as exc:  # noqa: BLE001
        run["error"] = f"{type(exc).__name__}: {exc}"
        run["passed"] = False
        log(f"== ERROR ({kind}): {run['error'][:3000]}")
    finally:
        if ex is not None:
            ex.close()
        if ray_cluster is not None:
            ray_cluster.stop()
    return run


if __name__ == "__main__":
    sys.exit(main())
