"""What log checkpoints and asynchronous replication cost the clients, and
what a checkpoint saves a restart -- on one host, servers in processes of
their own (tests/fakes/native_cluster.py), clients through the interception
library, the softverbs transport (TCP).

Modes, each on fresh logs and tiers:
  base       durable servers (logs on), as IO500 runs them
  ckpt       + a checkpoint whenever a log segment reaches --ckpt-mib
  repl-mark  + a standby configured but not running: the request path's
             share alone (keys marked and queued; nothing shipped)
  repl       + standbys running in their own process (shipping included;
             on a small host they take CPU from the primaries)
  ckpt+repl  both

Workloads, --workers processes at once: small files (3901 B, kept inline on
the MDT), 64 KiB files (on the OSTs) and 4 MiB files. Then, with a
standby, how long until everything is shipped (the replication lag at the
end of the load). Restart: --restart-files files, the servers killed and
started again, with and without a checkpoint.

    python3 tools/durability_overhead_bench.py [--workers 4] [--out bench.json]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
NM, NO = 2, 3

WORKER = r"""
import os, sys, time
P, w, kind, n = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
size = {"small": 3901, "mid": 65536, "big": 4 << 20}[kind]
body = os.urandom(size)
d = f"{P}/{kind}/{w}"
os.makedirs(d, exist_ok=True)
t0 = time.monotonic()
for i in range(n):
    with open(f"{d}/f{i:06d}", "wb") as f:
        f.write(body)
print(time.monotonic() - t0)
"""


def stats(lib: str, addr: str) -> dict:
    os.environ["ROCE_TRANSPORT_LIB"] = lib
    from roce.protocol import Frame, Op
    from roce.roce_transport import RoCETransport
    r = RoCETransport().send_request(addr, Frame(op=Op.STATS, request_id=1, key="", value=b""))
    return json.loads(r.value)


class Side:
    def __init__(self, lib, d, port, env):
        self.lib, self.d, self.port, self.env, self.p = lib, d, port, env, None
        os.makedirs(d, exist_ok=True)

    def start(self):
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT,
                   ROCEPOOL_OST_TIER=f"file:{self.d}/ost{{n}}.tier:1", ROCEPOOL_OST_RAM_CACHE_MIB="256",
                   ROCEPOOL_OST_LOG=f"{self.d}/ost{{n}}.log", ROCEPOOL_MDT_LOG=f"{self.d}/mdt{{n}}.log", **self.env)
        t0 = time.monotonic()
        self.p = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                                   str(self.port), str(NM), str(NO)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True, env=env)
        line = self.p.stdout.readline()
        if not line.startswith("READY"):
            raise RuntimeError(line + self.p.stderr.read()[-3000:])
        self.ready_s = time.monotonic() - t0
        _, self.mdts, self.osts = line.split()
        return self.ready_s

    def addrs(self):
        return self.mdts.split(",") + self.osts.split(",")

    def kill(self):
        self.p.kill()
        self.p.wait(timeout=60)
        self.err = self.p.stderr.read()

    def stop(self):
        if self.p and self.p.poll() is None:
            self.p.stdin.close()
            self.p.wait(timeout=60)


def peers(port):
    return {"ROCEPOOL_MDT_REPLICAS": ",".join(f"127.0.0.1:{port + i}" for i in range(NM)),
            "ROCEPOOL_OST_REPLICAS": ",".join(f"127.0.0.1:{port + 50 + i}" for i in range(NO))}


def client_env(lib, icp, prefix, a: Side):
    return dict(os.environ, ROCE_TRANSPORT_LIB=lib, PYTHONPATH=ROOT, LD_PRELOAD=icp, ROCEPOOL_PREFIX=prefix,
                ROCEPOOL_MDTS=a.mdts, ROCEPOOL_OSTS=a.osts, ROCEPOOL_INTERCEPT_LOG="1")


def run_load(env, prefix, kind, workers, n):
    ps = [subprocess.Popen([sys.executable, "-c", WORKER, prefix, str(w), kind, str(n)], env=env,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for w in range(workers)]
    t0 = time.monotonic()
    outs = [p.communicate(timeout=3600) for p in ps]
    wall = time.monotonic() - t0
    bad = [o[1][-500:] for p, o in zip(ps, outs) if p.returncode]
    if bad:
        raise RuntimeError(bad[0])
    return wall


def one_mode(lib, icp, mode, args, base_port):
    tmp = tempfile.mkdtemp(dir=args.dir)
    env_a, env_b = {}, None
    if "ckpt" in mode:
        env_a["ROCEPOOL_LOG_CHECKPOINT_MIB"] = str(args.ckpt_mib)
    if mode.startswith("repl") or "repl" in mode:
        env_a.update(peers(base_port + 100), ROCEPOOL_REPL_ROLE="primary", ROCEPOOL_REPL_PROMOTE_S="0")
        if mode != "repl-mark":
            env_b = dict(peers(base_port), ROCEPOOL_REPL_ROLE="standby", ROCEPOOL_REPL_PROMOTE_S="0")
            if "ckpt" in mode:
                env_b["ROCEPOOL_LOG_CHECKPOINT_MIB"] = str(args.ckpt_mib)
    b = Side(lib, os.path.join(tmp, "b"), base_port + 100, env_b) if env_b is not None else None
    a = Side(lib, os.path.join(tmp, "a"), base_port, env_a)
    res = {"mode": mode}
    try:
        if b:
            b.start()
        a.start()
        if b:  # the initial resync of an empty store: done before the load
            t0 = time.monotonic()
            while time.monotonic() - t0 < 60 and not all(stats(lib, x)["repl"]["in_sync"] for x in a.addrs()):
                time.sleep(0.1)
        prefix = os.path.join(tmp, "rp")
        env = client_env(lib, icp, prefix, a)
        for kind, n in (("small", args.small), ("mid", args.mid), ("big", args.big)):
            wall = run_load(env, prefix, kind, args.workers, n)
            files = args.workers * n
            size = {"small": 3901, "mid": 65536, "big": 4 << 20}[kind]
            res[kind] = {"files_s": round(files / wall, 1), "mib_s": round(files * size / wall / 2**20, 1),
                         "wall_s": round(wall, 2)}
        st = [stats(lib, x) for x in a.addrs()]
        res["checkpoints"] = sum(s.get("checkpoint", {}).get("checkpoints", 0) for s in st)
        res["ckpt_last_s_max"] = max(s.get("checkpoint", {}).get("last_s", 0) for s in st)
        if "repl" in mode:
            res["queued_at_end"] = sum(s["repl"]["queued"] for s in st)
            if b:
                t0 = time.monotonic()
                while time.monotonic() - t0 < 600:
                    st = [stats(lib, x) for x in a.addrs()]
                    if all(s["repl"]["queued"] == 0 for s in st):
                        break
                    time.sleep(0.05)
                res["drain_s"] = round(time.monotonic() - t0, 2)
                res["shipped_items"] = sum(s["repl"]["items"] for s in st)
                res["shipped_mib"] = round(sum(s["repl"]["bytes"] for s in st) / 2**20, 1)
    finally:
        a.stop()
        if b:
            b.stop()
        shutil.rmtree(tmp, ignore_errors=True)
    return res


def restart(lib, icp, args, base_port, ckpt):
    tmp = tempfile.mkdtemp(dir=args.dir)
    env_a = {"ROCEPOOL_LOG_CHECKPOINT_MIB": str(args.ckpt_mib)} if ckpt else {}
    a = Side(lib, os.path.join(tmp, "a"), base_port, env_a)
    try:
        a.start()
        prefix = os.path.join(tmp, "rp")
        env = client_env(lib, icp, prefix, a)
        run_load(env, prefix, "small", args.workers, args.restart_files // args.workers)
        if ckpt:
            time.sleep(3)  # the last checkpoint
        st = [stats(lib, x) for x in a.addrs()]
        logged = sum(s["log"]["bytes"] for s in st)
        a.kill()
        ready = a.start()
        st2 = [stats(lib, x) for x in a.addrs()]
        out = subprocess.run([sys.executable, "-c", f"import os; print(sum(len(os.listdir(os.path.join({prefix!r}, 'small', d))) for d in os.listdir({prefix!r} + '/small')))"],
                             env=env, capture_output=True, text=True, timeout=600)
        return {"checkpoint": ckpt, "files": int(out.stdout.strip() or -1), "restart_s": round(ready, 2),
                "log_mib_written": round(logged / 2**20, 1),
                "checkpoints_before": sum(s.get("checkpoint", {}).get("checkpoints", 0) for s in st),
                "segment_after": max(s.get("checkpoint", {}).get("segment", 0) for s in st2)}
    finally:
        a.stop()
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--small", type=int, default=1500, help="small files per worker")
    ap.add_argument("--mid", type=int, default=400)
    ap.add_argument("--big", type=int, default=24)
    ap.add_argument("--ckpt-mib", type=int, default=8)
    ap.add_argument("--restart-files", type=int, default=40000)
    ap.add_argument("--modes", default="base,ckpt,repl-mark,repl,ckpt+repl")
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--dir", default=None)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
    lib, icp = ensure_softverbs_lib(), ensure_intercept_lib()
    rows = []
    port = 27100
    for r in range(a.repeat):
        for m in a.modes.split(","):
            res = one_mode(lib, icp, m, a, port)
            res["round"] = r
            print(json.dumps(res), flush=True)
            rows.append(res)
            port += 300
    rs = []
    if a.restart_files:
        for ck in (False, True):
            res = restart(lib, icp, a, port, ck)
            print(json.dumps(res), flush=True)
            rs.append(res)
            port += 300
    doc = {"host_cpus": os.cpu_count(), "workers": a.workers, "modes": rows, "restart": rs}
    if a.out:
        with open(a.out, "w") as f:
            json.dump(doc, f, indent=2)


if __name__ == "__main__":
    main()
