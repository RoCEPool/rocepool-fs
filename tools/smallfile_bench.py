"""Small-file creation and writing on rocepool, the way a shuffle writes:
every worker creates `--files` files of `--kib` KiB spread over `--dirs`
directories (bucket=<i>/from-<w>-0.bin), each written as a small header,
the body in 1 MiB writes, and a small footer, then closed. The same on the
host's local disk for reference. Reports files/s, MiB/s and, from the FUSE
client's per-opcode counters, what one file costs: requests by kind, their
average time, and MDT / OST round trips.

    sudo python3 tools/smallfile_bench.py [--nodes 2] [--workers 4] [--files 256] [--kib 320] [--dirs 64]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools.cluster import Cluster  # noqa: E402
from tools.rocepool_mount import RocepoolMount  # noqa: E402

MNT = "/mnt/rocepool_smallfile"
OPS = {1: "LOOKUP", 2: "FORGET", 3: "GETATTR", 4: "SETATTR", 9: "MKDIR", 14: "OPEN", 15: "READ", 16: "WRITE",
       18: "RELEASE", 20: "FSYNC", 25: "FLUSH", 26: "INIT", 27: "OPENDIR", 28: "READDIR", 29: "RELEASEDIR",
       34: "ACCESS", 35: "CREATE", 36: "INTERRUPT", 42: "BATCH_FORGET", 44: "READDIRPLUS"}

WORKER = r"""
import os, sys, time
root, w, files, kib, dirs = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
body = b"x" * (1 << 20)
t0 = time.monotonic()
for d in range(dirs):
    os.makedirs(f"{root}/bucket={d}", exist_ok=True)
for i in range(files):
    fd = os.open(f"{root}/bucket={i % dirs}/from-{w}-{i}.bin", os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    os.write(fd, b"PAR1" + b"h" * 22)
    left = kib * 1024 - 48
    while left > 0:
        n = min(left, len(body)); os.write(fd, body[:n]); left -= n
    os.write(fd, b"f" * 18 + b"PAR1")
    os.close(fd)
print(time.monotonic() - t0)
"""


def run_workers(cluster, env, root: str, args, label: str) -> dict:
    procs = []
    for n in cluster.nodes:
        for w in range(args.workers):
            procs.append(cluster.popen(n.index, [sys.executable, "-c", WORKER, root, f"n{n.index}w{w}",
                                                 str(args.files), str(args.kib), str(args.dirs)], env,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
    t0 = time.monotonic()
    secs = []
    for p in procs:
        out, err = p.communicate(timeout=3600)
        if p.returncode:
            raise RuntimeError(f"{label} worker failed: {err[-1000:]}")
        secs.append(float(out.strip()))
    wall = time.monotonic() - t0
    nfiles = len(procs) * args.files
    return {"where": label, "files": nfiles, "wall_s": round(wall, 2), "files_per_s": round(nfiles / wall),
            "mib_per_s": round(nfiles * args.kib / 1024 / wall, 1),
            "per_file_ms_per_worker": round(1000 * sum(secs) / nfiles, 2)}


def fuse_stats(cluster, env, workdir: str) -> list[dict]:
    """SIGUSR1 to every node's FUSE client; their per-opcode counters
    (RocepoolMount points each at <workdir>/fuse<node>.stats.json)."""
    out = []
    for n in cluster.nodes:
        path = os.path.join(workdir, f"fuse{n.index}.stats.json")
        old = json.load(open(path)).get("seq", 0) if os.path.exists(path) else 0
        cluster.run(n.index, ["pkill", "-USR1", "-f", f"rocepool-fuse --mount {MNT}"], env, net=False)
        deadline, got = time.monotonic() + 10, {}
        while time.monotonic() < deadline:
            try:
                got = json.load(open(path))
                if got.get("seq", 0) > old:
                    break
            except (OSError, ValueError):
                pass
            time.sleep(0.1)
        out.append(got)
    return out


def summarize(before: list, after: list, nfiles: int) -> dict:
    ops = {}
    for b, a in zip(before, after):
        for k, (c1, ns1) in a.get("ops", {}).items():
            c0, ns0 = b.get("ops", {}).get(k, [0, 0])
            name = OPS.get(int(k), k)
            c, ns = ops.get(name, (0, 0))
            ops[name] = (c + c1 - c0, ns + ns1 - ns0)
    per = {k: {"per_file": round(c / nfiles, 2), "avg_us": round(ns / c / 1000, 1) if c else 0,
               "ms_per_file": round(ns / 1e6 / nfiles, 3)} for k, (c, ns) in sorted(ops.items()) if c}
    mdt = sum(a.get("mdt_rpcs", 0) - b.get("mdt_rpcs", 0) for b, a in zip(before, after))
    ost = sum(a.get("ost_rpcs", 0) - b.get("ost_rpcs", 0) for b, a in zip(before, after))
    return {"fuse_ops": per, "mdt_rpcs_per_file": round(mdt / nfiles, 2), "ost_rpcs_per_file": round(ost / nfiles, 2)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--nodes", type=int, default=2)
    ap.add_argument("--workers", type=int, default=4, help="per node")
    ap.add_argument("--files", type=int, default=256, help="per worker")
    ap.add_argument("--kib", type=int, default=320)
    ap.add_argument("--dirs", type=int, default=64)
    ap.add_argument("--workdir", default=os.path.join(ROOT, "smallfile_work"))
    ap.add_argument("--report", default="")
    ap.add_argument("--fuse-args", default="", help="extra rocepool-fuse arguments")
    args = ap.parse_args()
    os.makedirs(args.workdir, exist_ok=True)
    for f in glob.glob(os.path.join(args.workdir, "fuse*.stats.json")):
        os.unlink(f)
    cluster = Cluster.current(args.nodes)
    cluster.up()
    env = cluster.env()
    if not cluster.real:
        env["ROCEPOOL_RPC_TIMEOUT_MS"] = "60000"
    import shlex
    rp = RocepoolMount(cluster, env, args.workdir, MNT, fuse_args=shlex.split(args.fuse_args))
    report = {"workers_per_node": args.workers, "files_per_worker": args.files, "kib": args.kib, "dirs": args.dirs}
    try:
        local = "/var/tmp/smallfile_local"
        for n in cluster.nodes:
            cluster.run(n.index, ["rm", "-rf", local], env, net=False)
        report["local"] = run_workers(cluster, env, local, args, "local disk")
        for n in cluster.nodes:
            cluster.run(n.index, ["rm", "-rf", local], env, net=False)
        rp.start()
        before = fuse_stats(cluster, env, args.workdir)
        report["rocepool"] = run_workers(cluster, env, f"{MNT}/sf", args, "rocepool")
        after = fuse_stats(cluster, env, args.workdir)
        report["rocepool"].update(summarize(before, after, report["rocepool"]["files"]))
    finally:
        rp.stop()
        cluster.down()
    for k in ("local", "rocepool"):
        r = report.get(k, {})
        print(f"{k:<9} {r.get('files')} files: {r.get('files_per_s')} files/s, {r.get('mib_per_s')} MiB/s, "
              f"{r.get('per_file_ms_per_worker')} ms per file per worker")
    r = report.get("rocepool", {})
    print(f"rocepool round trips per file: MDT {r.get('mdt_rpcs_per_file')}, OST {r.get('ost_rpcs_per_file')}")
    for k, v in r.get("fuse_ops", {}).items():
        print(f"   {k:<13} {v['per_file']:>6} per file  avg {v['avg_us']:>8} us  = {v['ms_per_file']:>7} ms per file")
    if args.report:
        json.dump(report, open(args.report, "w"), indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
