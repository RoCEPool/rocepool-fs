"""A KV-cache block read by key vs by address (roce/native/rocepool_addr_bench.c).

    python3 tools/bench_addr.py                       # crossover rig (root)
    ROCEPOOL_TOPOLOGY=onsite_topology.json python3 tools/bench_addr.py   # real hosts

Two nodes (tools/topology.py); one OST on node B, the client on node A, so
every read crosses the link. Modes: rpc (by key, the OST sends the value),
lease (by key, LEASE RPC then a one-sided read), cached (by key, cached
lease: one RDMA READ chain with the seqlock check), addr (by address: the
data alone, one RDMA READ). Block sizes 4 KiB, 64 KiB (a block of 16
tokens of one layer of Llama-3-8B) and 2 MiB (the same block for all 32
layers); 1 and 8 client threads.

On the crossover rig the RDMA device is softverbs, which carries RC traffic
over TCP: the numbers are TCP numbers, good to compare the number of round
trips and copies of the modes, not the speed of a real NIC.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

SIZES = (4096, 65536, 2 * 1048576)
THREADS = (1, 8)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=3.0)
    ap.add_argument("--blocks", type=int, default=32)
    ap.add_argument("--sizes", default=",".join(str(s) for s in SIZES))
    ap.add_argument("--threads", default=",".join(str(t) for t in THREADS))
    ap.add_argument("--report", default=os.path.join(ROOT, "addr_bench_report.json"))
    args = ap.parse_args()
    import subprocess
    from roce.native.build_lib import ensure_addr_bench
    from tools.bench_ior import start_dist_servers
    from tools.topology import Topology
    topo = Topology.current()
    bench = ensure_addr_bench() if not topo.real else os.path.join(os.path.dirname(topo.lib), "rocepool-addr-bench")
    # 2 OSTs: the first on A, the second on B (start_dist_servers alternates); the client reads B's.
    servers, _, osts, env = start_dist_servers(n_mdts=1, n_osts=2, topo=topo)
    ost = next(a for node, a in osts if node == "B")
    results = []
    try:
        for size in (int(s) for s in args.sizes.split(",")):
            for threads in (int(t) for t in args.threads.split(",")):
                r = subprocess.run(topo.launch["A"].cmd([bench, ost, str(size), str(args.blocks), str(threads),
                                                         str(args.seconds)]),
                                   capture_output=True, text=True, env=env, timeout=600)
                rows = [json.loads(l[7:]) for l in r.stdout.splitlines() if l.startswith("RESULT ")]
                if not rows:
                    rows = [{"block_bytes": size, "threads": threads, "error": (r.stdout + r.stderr)[-800:]}]
                for row in rows:
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
        json.dump({"topology": topo.describe, "seconds": args.seconds, "blocks": args.blocks, "results": results,
                   "when": time.strftime("%Y-%m-%d %H:%M:%S")}, f, indent=2)
    print(f"report: {args.report}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
