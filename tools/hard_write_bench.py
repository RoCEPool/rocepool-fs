"""ior-hard-write's shape against durable OSTs, to see what a commit costs on
a given file system: `--writers` processes per node write 47008-byte pieces
of one shared file, strided as IOR's ranks do (piece k of writer w at
(k * writers + w) * 47008), through the FUSE client, for `--seconds`. The
OSTs keep a log (ROCEPOOL_OST_LOG) and a file tier under --dir, so every
write is fdatasynced to the tier before its reply -- as in a valid IO500 run.

    sudo python3 tools/hard_write_bench.py --dir /mnt/xfs/bench [--sets "gaps-written:;gaps-skipped:ROCEPOOL_OST_SKIP_ZERO_GAPS=1"]

Every set (name:VAR=v,VAR=v -- the OSTs' environment; the clients' for
CLIENT_VARS, e.g. ROCEPOOL_OST_ASYNC=1: writes durable at the fsync, not one
by one) runs on fresh servers
and a fresh tier file. Reported per set, from the OSTs' own counters: MiB/s,
writes/s, commits/s, the time of a commit (the tier's fdatasync and the log
record), writes per commit, tier bytes written per byte of data, and the
tier file's extent count (filefrag) at the end.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools.cluster import Cluster  # noqa: E402
from tools.rocepool_mount import RocepoolMount  # noqa: E402

MNT = "/mnt/rocepool_hardwrite"
CLIENT_VARS = ("ROCEPOOL_OST_ASYNC", "ROCEPOOL_DIRTY_CAP_MIB")  # a set's variables for the client, not the OSTs
PIECE = 47008

WRITER = r"""
import os, sys, time
path, w, total, seconds, most = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), int(sys.argv[5])
piece = 47008
buf = bytes([w % 251 + 1]) * piece
fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o644)
end, k = time.monotonic() + seconds, 0
while time.monotonic() < end and k < most:   # `most`: this writer's share of what the tiers hold
    os.pwrite(fd, buf, (k * total + w) * piece)
    k += 1
os.fsync(fd)
os.close(fd)
print(k)
"""

STATS = r"""
import json, os, sys
sys.path.insert(0, sys.argv[1])
from roce.protocol import Frame, Op
from roce.roce_transport import RoCETransport
out = {}
for addr in sys.argv[2].split(","):
    out[addr] = json.loads(RoCETransport().send_request(addr, Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)
print(json.dumps(out))
"""


def ost_stats(cluster, env, osts: str) -> dict:
    r = cluster.run(0, [sys.executable, "-c", STATS, ROOT, osts], env, capture_output=True, text=True)
    return json.loads(r.stdout.strip().splitlines()[-1])


def one_set(cluster, base_env: dict, name: str, extra: dict, args) -> dict:
    d = os.path.join(args.dir, name)
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    env = dict(base_env, **{k: v for k, v in extra.items() if k in CLIENT_VARS})
    ost_env = dict({k: v for k, v in extra.items() if k not in CLIENT_VARS}, ROCEPOOL_OST_LOG=f"{d}/ostlog{{node}}", ROCEPOOL_OST_RAM_CACHE_MIB=str(args.ram_cache_mib))
    rp = RocepoolMount(cluster, env, os.path.join(args.workdir, name), MNT, ost_tier=f"file:{d}/tier{{node}}:{args.tier_gib}",
                       ost_env=ost_env)
    n = len(cluster.nodes)
    total = n * args.writers
    try:
        rp.start()
        # 40% of the tiers at most: the slabs are 1 MiB objects, gaps may be written too
        most = int(0.4 * args.tier_gib * n * 2**30 / PIECE / total)
        before = ost_stats(cluster, env, rp.osts)
        t0 = time.monotonic()
        procs = [cluster.popen(node.index, [sys.executable, "-c", WRITER, f"{MNT}/hard.bin", str(node.index * args.writers + w),
                                            str(total), str(args.seconds), str(most)], env, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True) for node in cluster.nodes for w in range(args.writers)]
        pieces = 0
        for p in procs:
            out, err = p.communicate(timeout=args.seconds + 600)
            if p.returncode:
                raise RuntimeError(f"writer failed: {err[-800:]}")
            pieces += int(out.strip())
        wall = time.monotonic() - t0
        after = ost_stats(cluster, env, rp.osts)
        # every piece is there, from its writer
        check = cluster.run(0, [sys.executable, "-c",
                                "import os,sys\nfd=os.open(sys.argv[1],os.O_RDONLY)\nt=int(sys.argv[2]);bad=0\n"
                                "for i in range(0,t*40,7):\n"
                                "    b=os.pread(fd,47008,i*47008)\n"
                                "    bad+= b!=bytes([i%t%251+1])*47008\nprint(bad)", f"{MNT}/hard.bin", str(total)],
                            env, capture_output=True, text=True)
        frag = {}
        for node in cluster.nodes:
            r = cluster.run(node.index, ["filefrag", f"{d}/tier{node.index}"], env, net=False, capture_output=True, text=True)
            frag[f"node{node.index}"] = (r.stdout.strip().rsplit(":", 1)[-1].strip() or r.stderr.strip())[:80]
    finally:
        rp.stop()
        shutil.rmtree(d, ignore_errors=True)  # the tiers: room for the next set

    def delta(*path):
        tot = 0
        for addr in after:
            a, b = after[addr], before[addr]
            for k in path:
                a, b = a.get(k, 0) if isinstance(a, dict) else 0, b.get(k, 0) if isinstance(b, dict) else 0
            tot += a - b
        return tot
    commits, recs = delta("log", "commits"), delta("log", "records")
    data = pieces * PIECE
    one = next(iter(after.values()))
    return {"set": name, "env": extra, "pieces": pieces, "seconds": round(wall, 1),
            "mib_per_s": round(data / 2**20 / wall, 1), "writes_per_s": round(pieces / wall),
            "commits_per_s": round(commits / wall), "async_writes": delta("async_writes"),
            "commit_barriers": delta("commit_barriers"), "commit_us": round(delta("log", "sync_ns") / max(1, commits) / 1e3),
            "writes_per_commit": round(recs / max(1, commits), 1),
            "handler_cpu_us_per_write": round(delta("handler_cpu_ns") / max(1, delta("reqs")) / 1e3),
            "tier_bytes_per_data_byte": round(delta("tier_bytes_written") / max(1, data), 2),
            "log_mode": {k: one.get("log", {}).get(k) for k in ("direct", "pipeline")},
            "tier_extents": frag, "pieces_checked_wrong": check.stdout.strip() or check.stderr[-300:]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", required=True, help="where the tiers and logs go: a directory on the file system to measure")
    ap.add_argument("--nodes", type=int, default=2)
    ap.add_argument("--writers", type=int, default=8, help="per node")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--tier-gib", type=int, default=8, help="per OST (preallocated)")
    ap.add_argument("--ram-cache-mib", type=int, default=256)
    ap.add_argument("--sets", default="gaps-written:;gaps-skipped:ROCEPOOL_OST_SKIP_ZERO_GAPS=1",
                    help="name:VAR=v,VAR=v;name:... -- environments of the OSTs")
    ap.add_argument("--workdir", default=os.path.join(ROOT, "hard_write_work"))
    ap.add_argument("--report", default="")
    args = ap.parse_args()
    cluster = Cluster.current(args.nodes)
    cluster.up()
    env = cluster.env()
    if not cluster.real:
        env["ROCEPOOL_RPC_TIMEOUT_MS"] = "60000"
    r = subprocess.run(["stat", "-f", "-c", "%T", args.dir if os.path.isdir(args.dir) else os.path.dirname(args.dir)],
                       capture_output=True, text=True)
    report = {"dir": args.dir, "fstype": r.stdout.strip(), "nodes": len(cluster.nodes), "writers_per_node": args.writers,
              "seconds": args.seconds, "sets": []}
    try:
        for spec in [x for x in args.sets.split(";") if x.strip()]:
            name, _, kv = spec.partition(":")
            extra = dict(x.split("=", 1) for x in kv.split(",") if "=" in x)
            res = one_set(cluster, env, name.strip(), extra, args)
            report["sets"].append(res)
            print(f"== {report['fstype']} {res['set']:<14} {res['mib_per_s']:>7} MiB/s  {res['writes_per_s']:>6} writes/s  "
                  f"{res['commits_per_s']:>5} commits/s  commit {res['commit_us']:>5} us  {res['writes_per_commit']:>5} writes/commit  "
                  f"tier bytes/data {res['tier_bytes_per_data_byte']}  extents {res['tier_extents']}  wrong {res['pieces_checked_wrong']}",
                  flush=True)
    finally:
        cluster.down()
        if args.report:
            json.dump(report, open(args.report, "w"), indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
