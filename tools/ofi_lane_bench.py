"""Does a server endpoint's thread wait while a transfer is on the wire?

One native OST on node 0 (libfabric transport, its tcp provider, --eps
endpoints = threads), `--procs` x `--clients` client threads on node 1 writing then reading
1 MiB objects, with the thread waiting for every payload pull and large
reply (ROCE_OFI_PULL_PIPELINE=1, ROCE_OFI_BIG_REPLIES=0: as before) and
with them in flight (the default). --delay adds a netem delay to node 1's
link, so that a transfer spends its time on the wire, not in the CPU -- as
on EFA, where the client's side sends a pulled megabyte in ~2 ms.

    sudo python3 tools/ofi_lane_bench.py [--delay 1ms] [--procs 4] [--clients 8] [--objects 20] [--eps 2] [--rounds 2]

Per run, from the OST's transport counters: MiB/s, the time a request
waited for a thread, the threads' busy share.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools.cluster import Cluster  # noqa: E402

CLIENT = r'''
# `procs` processes (the GIL must not be what is measured), each with
# `nthreads` threads doing one request at a time; argv[5] = write | read
import json, multiprocessing, sys, threading, time
sys.path.insert(0, sys.argv[1])
ost, procs, nthreads, per, what = sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]), sys.argv[6]
blob = bytes(range(256)) * 4096

def stats():
    from roce.roce_transport import RoCETransport
    from roce.protocol import Frame, Op
    return json.loads(RoCETransport().send_request(ost, Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)["transport"]

def proc(pi, start, q):
    from roce.roce_transport import RoCETransport
    from roce.remote_engine import RemoteEngine
    es = [RemoteEngine(ost, RoCETransport()) for _ in range(nthreads)]
    for e in es:
        e.put(f"warm{pi}", b"x")   # connected before the clock starts
    bad = [0]
    def run(e, t):
        for i in range(per):
            k = f"k{pi}.{t}.{i}"
            if what == "write":
                e.put(k, blob)
            else:
                v = e.get(k)
                if len(v) != len(blob) or (i % 8 == 0 and v != blob):
                    bad[0] += 1
    start.wait()
    ts = [threading.Thread(target=run, args=(es[t], t)) for t in range(nthreads)]
    [x.start() for x in ts]; [x.join() for x in ts]
    q.put(bad[0])

if __name__ == "__main__":
    start, q = multiprocessing.Event(), multiprocessing.Queue()
    ps = [multiprocessing.Process(target=proc, args=(pi, start, q)) for pi in range(procs)]
    [x.start() for x in ps]
    time.sleep(1.5)
    a = stats(); t0 = time.time()
    start.set()
    bad = sum(q.get() for _ in ps)
    dt = time.time() - t0; b = stats()
    [x.join() for x in ps]
    n = max(1, b["reqs"] - a["reqs"] - 1)
    print("RESULT " + json.dumps({"mib_s": round(procs * nthreads * per / dt), "wrong": bad,
        "queue_us": round((b["queue_ns"] - a["queue_ns"]) / n / 1e3),
        "busy": round((b["busy_ns"] - a["busy_ns"]) / max(1, b["thread_ns"] - a["thread_ns"]), 2),
        "pull_pipeline_max": b["pull_pipeline_max"], "big_replies_async": b["big_replies_async"]}))
'''

MODES = (("waiting", {"ROCE_OFI_PULL_PIPELINE": "1", "ROCE_OFI_BIG_REPLIES": "0"}), ("in flight", {}))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--delay", default="", help="netem delay on the clients' link, e.g. 1ms (needs tc and sch_netem)")
    ap.add_argument("--procs", type=int, default=4, help="client processes")
    ap.add_argument("--clients", type=int, default=8, help="threads per client process, one request at a time each")
    ap.add_argument("--objects", type=int, default=20, help="1 MiB objects per client")
    ap.add_argument("--eps", type=int, default=2, help="server endpoints (one thread each)")
    ap.add_argument("--rounds", type=int, default=2)
    ap.add_argument("--report", default="")
    a = ap.parse_args()
    was = os.environ.get("ROCEPOOL_BACKEND")
    os.environ["ROCEPOOL_BACKEND"] = "ofi"
    try:
        from roce.native.build_lib import ensure_softverbs_lib
        lib = ensure_softverbs_lib()
    finally:
        if was is None:
            os.environ.pop("ROCEPOOL_BACKEND")
        else:
            os.environ["ROCEPOOL_BACKEND"] = was
    c = Cluster.current(2)
    c.up()
    base = dict(c.env(), ROCE_TRANSPORT_LIB=lib, PYTHONPATH=ROOT, ROCE_OFI_PROVIDER="tcp", ROCE_OFI_SERVER_EPS=str(a.eps))
    report = {"delay": a.delay or "none", "client_procs": a.procs, "threads_per_proc": a.clients, "eps": a.eps, "runs": []}
    try:
        if a.delay:
            r = c.run(1, ["sh", "-c", "dev=$(ip -o link | grep -v ' lo:' | head -1 | cut -d: -f2 | cut -d@ -f1 | tr -d ' '); "
                                      f"tc qdisc add dev $dev root netem delay {a.delay} && tc qdisc show dev $dev"],
                      base, capture_output=True, text=True)
            if r.returncode:
                report["delay"] = f"none (netem unavailable: {r.stderr.strip()[-200:]})"
            print(f"== link delay: {report['delay']}", flush=True)
        for rnd in range(a.rounds):
            for name, extra in MODES:
                env = dict(base, **extra)
                ost = f"{c.nodes[0].ip}:{23600 + rnd}"
                srv = c.popen(0, [sys.executable, "-u", os.path.join(ROOT, "tools", "crossover_node.py"), "ost-native", ost],
                              env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
                try:
                    while "READY" not in srv.stdout.readline():
                        if srv.poll() is not None:
                            raise RuntimeError("the OST did not start")
                    d = {"mode": name, "wrong": 0}
                    for what in ("write", "read"):
                        r = c.run(1, [sys.executable, "-c", CLIENT, ROOT, ost, str(a.procs), str(a.clients), str(a.objects), what],
                                  dict(env, ROCE_OFI_CLIENT_EPS=str(a.clients)), capture_output=True, text=True, timeout=1800)
                        line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULT ")), None)
                        if not line:
                            raise RuntimeError(f"client failed: {r.stderr[-1500:]}")
                        d[what] = json.loads(line[7:])
                        d["wrong"] += d[what]["wrong"]
                    d["pull_pipeline_max"] = d["read"]["pull_pipeline_max"]
                    d["big_replies_async"] = d["read"]["big_replies_async"]
                finally:
                    srv.stdin.close()
                    srv.wait(timeout=60)
                report["runs"].append(d)
                print(f"== {name:<9} write {d['write']['mib_s']:>6} MiB/s (queued {d['write']['queue_us']:>6} us, threads "
                      f"{d['write']['busy']:.2f} busy)   read {d['read']['mib_s']:>6} MiB/s (queued {d['read']['queue_us']:>6} us, "
                      f"{d['read']['busy']:.2f} busy)   pulls in flight max {d['pull_pipeline_max']}, large replies not waited "
                      f"for {d['big_replies_async']}, wrong {d['wrong']}", flush=True)
    finally:
        c.down()
        if a.report:
            json.dump(report, open(a.report, "w"), indent=1)
    return 1 if any(d["wrong"] for d in report["runs"]) else 0


if __name__ == "__main__":
    sys.exit(main())
