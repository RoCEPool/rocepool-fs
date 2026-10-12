"""Automated YCSB benchmark against this project's storage layer. One
command, no manual steps: downloads real upstream YCSB 0.17.0
(https://github.com/brianfrankcooper/YCSB, prebuilt release -- Maven
Central is not reachable from this sandbox, see the note in ensure_ycsb()),
compiles the custom `RocepoolClient` DB binding (tools/ycsb_java/, unmodified
`site.ycsb.DB` interface) against it, starts tools/ycsb_bridge.py in front of
this project's real storage layer, runs YCSB's own load+run phases for a
handful of its standard workloads, and writes a JSON report.

    python3 tools/bench_ycsb.py                 # --backend local: MemoryEngine, this process only
    python3 tools/bench_ycsb.py --backend pool   # distributed: PoolNode across the crossover cable
                                                  # (needs root; see tools/run_crossover_rdma.py)
    python3 tools/bench_ycsb.py --quick          # smaller record/op counts, fewer workloads

YCSB is the real upstream client (site.ycsb.Client) and the real upstream
workload definitions (workloads/workloada..f) -- nothing about them is
written for or tuned to this project. It talks to `RocepoolClient`, which
talks to `tools/ycsb_bridge.py`, which forwards every read/insert/update/
delete/scan straight to a real `PoolNode` (or `MemoryEngine` for the local
backend). See ycsb_bridge.py's module docstring for why a bridge instead of
a from-scratch Java reimplementation of roce/protocol.py.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
CACHE = os.path.join(ROOT, ".bench_cache")
YCSB_DIR = os.path.join(CACHE, "ycsb-0.17.0")
YCSB_TARBALL_URL = "https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz"
YCSB_CLASSES = os.path.join(CACHE, "ycsb_rocepool_classes")
BRIDGE_HOST = "127.0.0.1"
BRIDGE_PORT = 19900


def _run(cmd, **kw):
    print("+ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True, **kw)


def ensure_ycsb() -> str:
    """Returns YCSB_DIR, downloading and extracting the real upstream
    0.17.0 release tarball if not already cached. This is a *prebuilt*
    release (vendored dependency jars included) rather than a source
    checkout built with Maven: Maven Central (repo.maven.apache.org) is
    not on this sandbox's network allowlist (403 from the egress proxy),
    but GitHub releases are, and the release tarball needs nothing else
    at runtime."""
    if os.path.exists(os.path.join(YCSB_DIR, "lib", "core-0.17.0.jar")):
        return YCSB_DIR
    os.makedirs(CACHE, exist_ok=True)
    tarball = os.path.join(CACHE, "ycsb-0.17.0.tar.gz")
    if not os.path.exists(tarball):
        _run(["curl", "-sSL", "-o", tarball, YCSB_TARBALL_URL])
    _run(["tar", "xzf", tarball, "-C", CACHE])
    return YCSB_DIR


def ensure_binding(ycsb_dir: str) -> str:
    """Returns YCSB_CLASSES, compiling tools/ycsb_java/RocepoolClient.java
    against the real core-0.17.0.jar if not already built."""
    marker = os.path.join(YCSB_CLASSES, "site", "ycsb", "db", "RocepoolClient.class")
    if os.path.exists(marker):
        return YCSB_CLASSES
    os.makedirs(YCSB_CLASSES, exist_ok=True)
    core_jar = os.path.join(ycsb_dir, "lib", "core-0.17.0.jar")
    src = os.path.join(ROOT, "tools", "ycsb_java", "RocepoolClient.java")
    _run(["javac", "-cp", core_jar, "-d", YCSB_CLASSES, src])
    return YCSB_CLASSES


def _wait_for_port(host: str, port: int, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=0.3)
            s.close()
            return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"bridge did not come up on {host}:{port}")


def start_bridge_local() -> subprocess.Popen:
    p = subprocess.Popen(
        [sys.executable, "-u", os.path.join(ROOT, "tools", "ycsb_bridge.py"),
         "--backend", "local", "--host", BRIDGE_HOST, "--port", str(BRIDGE_PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=ROOT)
    _wait_for_port(BRIDGE_HOST, BRIDGE_PORT)
    return p


def stop_bridge_local(p: subprocess.Popen) -> None:
    p.terminate()
    try:
        p.wait(timeout=5)
    except subprocess.TimeoutExpired:
        p.kill()


def start_pool_cluster() -> tuple[subprocess.Popen, list[subprocess.Popen], str]:
    """Brings up the same 2-backer + 1-bridge-node cluster shape as
    tools/bench_ior.py's mount_pool(), minus the FUSE mount: here the
    bridge process itself (running inside nodeA's namespace) IS the third
    PoolNode member. Returns (bridge_proc, backer_procs, "host:port" to
    connect to *from inside netns_for_client*, netns_for_client) -- the
    caller must run its YCSB client inside that namespace (see
    run_ycsb_phase's `netns` parameter), not from the host.
    """
    from tools import netlink_veth
    netlink_veth.up()
    nodes = ["10.99.0.1:19801", "10.99.0.2:19801", "10.99.0.2:19802"]
    ns = ["rocepool_nodeA", "rocepool_nodeB", "rocepool_nodeB"]
    from roce.native.build_lib import ensure_softverbs_lib
    soft = ensure_softverbs_lib()  # rebuilds if any source is newer than the .so
    env = dict(os.environ, ROCE_TRANSPORT_LIB=soft, PYTHONPATH=ROOT,
              ROCE_MAX_FRAME_BYTES=str(64 * 1024 * 1024))
    helper = os.path.join(ROOT, "tools", "crossover_node.py")
    backers = [subprocess.Popen(
        ["nsenter", f"--net=/run/netns/{ns[i]}", sys.executable, "-u", helper, "pool-server", nodes[i], ",".join(nodes)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, cwd=ROOT)
        for i in (1, 2)]
    for p in backers:
        line = p.stdout.readline()
        if not line.startswith("READY"):
            raise RuntimeError(f"OST/pool backer did not come up: {line}{p.stdout.read()}")

    # The host's own namespace cannot reach 10.99.0.x at all -- deliberate,
    # documented isolation of this topology (docs/CROSSOVER_RDMA_TEST.md).
    # So the bridge listens on 127.0.0.1 *inside* nodeA's namespace (its own
    # private loopback, distinct from the host's), and the YCSB Java client
    # is launched the same way every other cluster-facing process in this
    # project's test rig is (tools/run_crossover_rdma.py's phases, the
    # backers above): via `nsenter` into that namespace, not from the host.
    bridge_addr = nodes[0]
    _, node_port_str = bridge_addr.split(":")
    bridge_port = int(node_port_str) + 100
    bridge = subprocess.Popen(
        ["nsenter", f"--net=/run/netns/{ns[0]}", sys.executable, "-u",
         os.path.join(ROOT, "tools", "ycsb_bridge.py"),
         "--backend", "pool", "--host", "127.0.0.1", "--port", str(bridge_port),
         "--pool-node", bridge_addr, "--pool-nodes", ",".join(nodes)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, cwd=ROOT)
    line = bridge.stdout.readline()
    if not line.startswith("READY"):
        raise RuntimeError(f"pool bridge did not come up: {line}{bridge.stdout.read()}")
    return bridge, backers, f"127.0.0.1:{bridge_port}", ns[0]


def stop_pool_cluster(bridge, backers) -> None:
    from tools import netlink_veth
    bridge.terminate()
    try:
        bridge.wait(timeout=10)
    except subprocess.TimeoutExpired:
        bridge.kill()
    for p in backers:
        try:
            p.stdin.close()
        except OSError:
            pass
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
    netlink_veth.down()


_METRIC_RE = re.compile(r"^\[(?P<op>[A-Z_]+)\], (?P<metric>[^,]+), (?P<value>.+)$")


def _parse_ycsb_output(out: str) -> dict:
    ops: dict[str, dict] = {}
    for line in out.splitlines():
        m = _METRIC_RE.match(line.strip())
        if not m:
            continue
        op, metric, value = m.group("op"), m.group("metric"), m.group("value")
        entry = ops.setdefault(op, {})
        if metric.startswith("Return="):
            entry.setdefault("returns", {})[metric[len("Return="):]] = int(value)
        else:
            try:
                entry[metric] = float(value)
            except ValueError:
                entry[metric] = value
    return ops


def run_ycsb_phase(classpath: str, workload_file: str, *, phase: str, props: dict,
                    timeout: int = 180, netns: str | None = None) -> dict:
    cmd = ["java", "-cp", classpath, "site.ycsb.Client", "-db", "site.ycsb.db.RocepoolClient",
           "-P", workload_file]
    for k, v in props.items():
        cmd += ["-p", f"{k}={v}"]
    cmd.append("-load" if phase == "load" else "-t")
    if netns:
        # --backend pool: the bridge only listens inside this namespace
        # (see start_pool_cluster's docstring), so the client must run
        # there too.
        cmd = ["nsenter", f"--net=/run/netns/{netns}"] + cmd
    t0 = time.monotonic()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=ROOT)
    except subprocess.TimeoutExpired as exc:
        # A real result, not a script bug: SCAN under --backend pool fans
        # out list_prefix to every node on every call (see
        # tools/ycsb_bridge.py's Bridge.handle_scan), so a scan-heavy
        # workload (workloade) at full record/op counts can legitimately
        # run past any reasonable timeout over the network -- report it as
        # a timeout rather than letting the whole harness crash on one
        # workload's uncaught exception.
        elapsed = time.monotonic() - t0
        return {"phase": phase, "elapsed_s": round(elapsed, 2), "returncode": None, "ok": False,
                "ops": {}, "timeout": True,
                "stdout_tail": (exc.stdout or "").strip().splitlines()[-20:] if exc.stdout else []}
    elapsed = time.monotonic() - t0
    ops = _parse_ycsb_output(proc.stdout)
    any_errors = any(op_stats.get("returns", {}).get("ERROR", 0) > 0 for op_stats in ops.values())
    result = {"phase": phase, "elapsed_s": round(elapsed, 2), "returncode": proc.returncode,
              "ok": proc.returncode == 0 and not any_errors, "ops": ops}
    if not result["ok"]:
        result["stdout_tail"] = proc.stdout.strip().splitlines()[-20:]
        result["stderr_tail"] = proc.stderr.strip().splitlines()[-20:]
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=("local", "pool"), default="local")
    ap.add_argument("--quick", action="store_true", help="fewer records/ops, fewer workloads")
    ap.add_argument("--report", default=os.path.join(ROOT, "ycsb_bench_report.json"))
    args = ap.parse_args()
    if args.backend == "pool" and os.geteuid() != 0:
        print("--backend pool needs root (network namespaces)", file=sys.stderr)
        return 2

    print("== ensuring YCSB (real upstream 0.17.0 release) ==", flush=True)
    ycsb_dir = ensure_ycsb()
    print(f"ycsb: {ycsb_dir}", flush=True)
    print("== compiling RocepoolClient against it ==", flush=True)
    classes_dir = ensure_binding(ycsb_dir)
    classpath = os.pathsep.join([classes_dir, os.path.join(ycsb_dir, "lib", "*")])

    bridge_proc = backers = netns = None
    if args.backend == "local":
        bridge_proc = start_bridge_local()
        bridge_addr = f"{BRIDGE_HOST}:{BRIDGE_PORT}"
    else:
        bridge_proc, backers, bridge_addr, netns = start_pool_cluster()
    host, port = bridge_addr.split(":")

    workloads = ["workloada", "workloadb", "workloadc", "workloadf"]
    if not args.quick:
        workloads += ["workloadd", "workloade"]
    recordcount = 200 if args.quick else 2000
    opcount = 500 if args.quick else 5000

    report = {"benchmark": "YCSB (real upstream 0.17.0, unmodified) via tools/ycsb_bridge.py",
              "backend": args.backend, "recordcount": recordcount, "operationcount": opcount,
              "workloads": {}}
    try:
        for wl in workloads:
            print(f"-- YCSB workload: {wl} --", flush=True)
            wl_path = os.path.join(ycsb_dir, "workloads", wl)
            wl_recordcount, wl_opcount = recordcount, opcount
            if args.backend == "pool" and wl == "workloade":
                # workloade is scan-heavy, and SCAN under --backend pool
                # fans list_prefix out to every node on every call (see
                # tools/ycsb_bridge.py's Bridge.handle_scan) -- at full
                # size this legitimately runs for many minutes over the
                # network rather than hanging, so it's scaled down here to
                # finish in a reasonable time while still exercising the
                # real fan-out path. The resulting throughput number is
                # still directly comparable to workloade's local-backend
                # run since both use YCSB's own timing at whatever size ran.
                wl_recordcount, wl_opcount = max(50, recordcount // 20), max(100, opcount // 20)
            props = {"recordcount": wl_recordcount, "operationcount": wl_opcount,
                      "rocepool.host": host, "rocepool.port": port}
            load = run_ycsb_phase(classpath, wl_path, phase="load", props=props, netns=netns)
            run = run_ycsb_phase(classpath, wl_path, phase="run", props=props, netns=netns)
            report["workloads"][wl] = {"load": load, "run": run,
                                        "recordcount": wl_recordcount, "operationcount": wl_opcount}
    finally:
        if args.backend == "local":
            stop_bridge_local(bridge_proc)
        else:
            stop_pool_cluster(bridge_proc, backers)

    report["ok"] = all(w["load"]["ok"] and w["run"]["ok"] for w in report["workloads"].values())
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)

    for wl, phases in report["workloads"].items():
        run = phases["run"]
        status = "PASS" if phases["load"]["ok"] and run["ok"] else "FAIL"
        overall = run["ops"].get("OVERALL", {})
        throughput = overall.get("Throughput(ops/sec)", "-")
        op_summaries = []
        for op, stats in run["ops"].items():
            if op in ("OVERALL", "CLEANUP") or op.startswith("TOTAL_"):
                continue
            avg = stats.get("AverageLatency(us)", "-")
            p99 = stats.get("99thPercentileLatency(us)", "-")
            op_summaries.append(f"{op}avg={avg}us/p99={p99}us")
        print(f"[{status}] {wl:12s} throughput={throughput} ops/sec   " + "  ".join(op_summaries))
    print(f"report: {args.report}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
