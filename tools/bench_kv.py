"""Native KV API throughput vs the Python client, same servers.

    python3 tools/bench_kv.py                       # crossover rig (root)
    ROCEPOOL_TOPOLOGY=onsite_topology.json python3 tools/bench_kv.py   # real hosts

4 native OSTs split over the two nodes (tools/topology.py); clients on
node A. Cases (value sizes 64 B and 4 KiB):
  native get / put     rocepool-kv-bench (C, roce/native/rocepool_kv.c), 1 and 16 threads
  native mget x64      one BATCH frame per OST per call, 16 threads
  python get / put     roce/remote_engine.py RemoteEngine (the pre-KV-API client), 1 and 16 threads
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PY_WORKER = r"""
import os, sys, threading, time, json, hashlib
sys.path.insert(0, %(root)r)
from roce.remote_engine import RemoteEngine
from roce.roce_transport import RoCETransport
osts, threads, secs, vb, mode = %(osts)r, %(threads)d, %(secs)f, %(vb)d, %(mode)r
val = b"v" * vb
def owner(k): return osts[int.from_bytes(hashlib.sha256(k.encode()).digest()[:8], "big") %% len(osts)]
engs = {a: RemoteEngine(a, RoCETransport()) for a in osts}
for t in range(threads):
    for k in range(200):
        key = f"pybench/k{t}-{k}"; engs[owner(key)].put(key, val)
counts, lats = [0] * threads, [[] for _ in range(threads)]
until = time.monotonic() + secs
def run(t):
    local = {a: RemoteEngine(a, RoCETransport()) for a in osts}
    i = 0
    while time.monotonic() < until:
        key = f"pybench/k{t}-{i %% 200}"; t0 = time.monotonic()
        if mode == "put": local[owner(key)].put(key, val)
        else: local[owner(key)].get(key)
        lats[t].append((time.monotonic() - t0) * 1e6); counts[t] += 1; i += 1
ts = [threading.Thread(target=run, args=(t,)) for t in range(threads)]
[x.start() for x in ts]; [x.join() for x in ts]
al = sorted(x for l in lats for x in l)
print("RESULT " + json.dumps({"ops_per_s": sum(counts) / secs, "p50_us": al[len(al)//2], "p99_us": al[int(len(al)*.99)]}))
os._exit(0)
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=5.0)
    ap.add_argument("--report", default=os.path.join(ROOT, "kv_bench_report.json"))
    args = ap.parse_args()
    from roce.native.build_lib import ensure_kv_lib
    from tools.bench_ior import start_dist_servers
    from tools.topology import Topology
    topo = Topology.current()
    bench = ensure_kv_lib()[1] if not topo.real else os.path.join(os.path.dirname(topo.lib), "rocepool-kv-bench")
    servers, _, osts, env = start_dist_servers(n_mdts=1, n_osts=4, topo=topo)
    ost_list = [a for _, a in osts]
    results = []
    try:
        for vb in (64, 4096):
            for threads in (1, 16):
                for mode in ("get", "put"):
                    r = subprocess.run(topo.launch["A"].cmd([bench, ",".join(ost_list), str(threads),
                                                             str(args.seconds), str(vb), mode]),
                                       capture_output=True, text=True, env=env, timeout=120)
                    line = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None)
                    row = {"client": "native", "mode": mode, "threads": threads, "value_bytes": vb,
                           **(json.loads(line[7:]) if line else {"error": (r.stdout + r.stderr)[-500:]})}
                    results.append(row)
                    print(json.dumps(row), flush=True)
                    p = topo.popen("A", ["-c", PY_WORKER % {"root": ROOT, "osts": ost_list, "threads": threads,
                                                           "secs": args.seconds, "vb": vb, "mode": mode}],
                                   env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                    out = p.communicate(timeout=300)[0]
                    line = next((l for l in out.splitlines() if l.startswith("RESULT ")), None)
                    row = {"client": "python", "mode": mode, "threads": threads, "value_bytes": vb,
                           **(json.loads(line[7:]) if line else {"error": out[-500:]})}
                    results.append(row)
                    print(json.dumps(row), flush=True)
            r = subprocess.run(topo.launch["A"].cmd([bench, ",".join(ost_list), "16", str(args.seconds), str(vb),
                                                     "mget", "64"]), capture_output=True, text=True, env=env,
                               timeout=120)
            line = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None)
            row = {"client": "native", "mode": "mget64", "threads": 16, "value_bytes": vb,
                   **(json.loads(line[7:]) if line else {"error": (r.stdout + r.stderr)[-500:]})}
            results.append(row)
            print(json.dumps(row), flush=True)
    finally:
        for s in servers:
            try:
                s.stdin.close()
            except OSError:
                pass
        for s in servers:
            try:
                s.wait(timeout=10)
            except subprocess.TimeoutExpired:
                s.kill()
        topo.down()
    with open(args.report, "w") as f:
        json.dump({"topology": topo.describe, "seconds": args.seconds, "results": results,
                   "when": time.strftime("%Y-%m-%d %H:%M:%S")}, f, indent=2)
    print(f"report: {args.report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
