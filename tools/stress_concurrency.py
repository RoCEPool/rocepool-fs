"""Concurrency-ceiling stress test for the distributed (MDT + striped OST)
design: how many truly concurrent clients does the SOFTWARE survive on one
machine, and what gives out first?

    python3 tools/stress_concurrency.py                       # md, md_shared, data; 1..200 clients
    python3 tools/stress_concurrency.py --ladder 1,50,200,400 --workload md
    python3 tools/stress_concurrency.py --report stress.json

Topology per ladder step (rebuilt from scratch each step so kernel socket
state from the previous step can't leak into the next): the crossover-cable
veth link, 2 MDT + 4 OST server processes split across both namespaces
(tools/bench_ior.py start_dist_servers), and N independent clients.

"Client" here means one independent DistributedFileSystem instance -- its
own attribute cache, its own request batcher, its own connections -- i.e.
what one client node would run. N clients are packed as
ceil(N / --threads-per-proc) worker processes x up to --threads-per-proc
threads each, alternating between the two network namespaces, all released
by one GO at the same instant and run for --duration seconds. Clients talk
to the servers directly through the DistributedFileSystem API rather than
200 FUSE mounts: the question is what the server side and the transport do
under N-way concurrency, not how one kernel mount serialises its callers.

Workloads (one POSIX-level op = one timed call):
  md         create / stat / unlink small files in the client's OWN directory
             -> directories hash across both MDTs
  md_shared  same, but every client in ONE shared directory -> one hot MDT
  data       write 1 MiB file / read it back / unlink -> OSTs + MDTs

Attribute cache defaults to OFF (--attr-ttl 0) so every op really reaches
a server; with it on, most stats would never leave the client and the
test would measure the cache, not the servers.

What is sampled while each step runs, every 0.5 s:
  per server process: threads, RSS, open fds, CPU%
  per namespace: TCP sockets in TIME_WAIT / ESTABLISHED, and the kernel's
  ListenOverflows / ListenDrops counters (accept queue full -> SYN dropped)
  host: MemAvailable, load average
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import statistics
import subprocess
import sys
import threading
import time
import traceback
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

NAMESPACES = ["rocepool_nodeA", "rocepool_nodeB"]
_CLK_TCK = os.sysconf("SC_CLK_TCK")


# ---------------------------------------------------------------------------
# worker process: T client threads, each its own DistributedFileSystem
# ---------------------------------------------------------------------------
def _client_thread(cfg: dict, client_id: int, ready: threading.Barrier, go: threading.Event,
                   out: dict) -> None:
    from fs.posix_api import DistributedFileSystem, FileExistsInPool
    from roce.remote_engine import RemoteEngine
    from roce.request_batcher import RequestBatcher
    from roce.roce_transport import RoCETransport

    lat: dict[str, list[float]] = {}
    errors: Counter = Counter()
    per_sec: Counter = Counter()
    ok = 0
    try:
        tr = RoCETransport()
        dfs = DistributedFileSystem(
            mdt_transport=tr, mdts=cfg["mdts"], osts=[RemoteEngine(a, tr) for a in cfg["osts"]],
            attr_ttl_s=cfg["attr_ttl"],
            batcher=RequestBatcher(max_workers=cfg["batcher_workers"], max_per_peer=cfg["batcher_per_peer"]))
        wl = cfg["workload"]
        if wl == "md_shared":
            base = "shared"
            for _ in range(50):
                try:
                    dfs.mkdir(base)
                    break
                except FileExistsInPool:
                    break
                except Exception:  # noqa: BLE001 -- racing 200 mkdirs of one dir at startup; retry
                    time.sleep(0.05)
            prefix = f"{base}/c{client_id}_"
        else:
            base = f"c{client_id}"
            dfs.mkdir(base)
            prefix = f"{base}/"
        payload = random.Random(client_id).randbytes(cfg["file_bytes"]) if wl == "data" else b""
    except Exception as exc:  # noqa: BLE001
        out[client_id] = {"setup_error": f"{type(exc).__name__}: {exc}"[:300]}
        try:
            ready.wait(timeout=120)
        except threading.BrokenBarrierError:
            pass
        return

    try:
        ready.wait(timeout=120)
    except threading.BrokenBarrierError:
        pass
    go.wait()
    deadline = cfg["deadline"]

    def timed(name: str, fn) -> bool:
        nonlocal ok
        t0 = time.monotonic()
        try:
            fn()
            ok += 1
            lat.setdefault(name, []).append(time.monotonic() - t0)
            per_sec[int(time.time())] += 1
            return True
        except Exception as exc:  # noqa: BLE001 -- classify and keep going
            msg = str(exc)
            for needle in ("REJECTED", "timed out", "RNR", "ECONNREFUSED", "Connection refused",
                           "flush", "RETRY_EXC", "unexpected CM event"):
                if needle in msg:
                    msg = needle
                    break
            errors[f"{name}:{type(exc).__name__}:{msg[:80]}"] += 1
            return False

    i = 0
    while time.time() < deadline:
        path = f"{prefix}f{i}"
        if wl in ("md", "md_shared"):
            if timed("create", lambda: dfs.create_file(path)):
                timed("stat", lambda: dfs.stat_path(path))
                timed("unlink", lambda: dfs.remove_file(path))
        else:
            if timed("write", lambda: dfs.write_file(path, payload)):
                timed("read", lambda: dfs.read_file(path))
                timed("unlink", lambda: dfs.remove_file(path))
        i += 1
    dfs.batcher.close()
    out[client_id] = {"ok": ok, "errors": dict(errors), "lat": lat, "per_sec": dict(per_sec)}


def worker_main(cfg_json: str) -> None:
    cfg = json.loads(cfg_json)
    ids = cfg["client_ids"]
    ready = threading.Barrier(len(ids) + 1)
    go = threading.Event()
    out: dict = {}
    threads = [threading.Thread(target=_client_thread, args=(cfg, cid, ready, go, out), daemon=True)
               for cid in ids]
    for t in threads:
        t.start()
    try:
        ready.wait(timeout=180)
    except threading.BrokenBarrierError:
        pass
    setup_errors = {cid: v["setup_error"] for cid, v in out.items() if "setup_error" in v}
    print("READY " + json.dumps({"setup_errors": setup_errors}), flush=True)
    line = sys.stdin.readline().split()
    cfg["deadline"] = float(line[1])
    go.set()
    for t in threads:
        t.join(timeout=max(1.0, cfg["deadline"] - time.time()) + 60)
    hung = [cid for cid, t in zip(ids, threads) if t.is_alive()]
    agg_lat: dict[str, list[float]] = {}
    errs: Counter = Counter()
    per_sec: Counter = Counter()
    ok = 0
    for cid in ids:
        r = out.get(cid, {})
        ok += r.get("ok", 0)
        per_sec.update(r.get("per_sec", {}))
        errs.update(r.get("errors", {}))
        for k, v in r.get("lat", {}).items():
            agg_lat.setdefault(k, []).extend(v)
    # latencies are subsampled so 200 clients' worth fits on one stdout line
    for k, v in agg_lat.items():
        if len(v) > 4000:
            agg_lat[k] = random.Random(0).sample(v, 4000)
    print("RESULT " + json.dumps({"ok": ok, "errors": dict(errs), "lat": agg_lat, "hung": hung,
                                  "per_sec": {str(k): v for k, v in per_sec.items()},
                                  "setup_errors": setup_errors, "clients": len(ids)}), flush=True)
    os._exit(0)  # don't wait on daemon threads stuck in a C call


# ---------------------------------------------------------------------------
# resource sampling
# ---------------------------------------------------------------------------
def _proc_sample(pid: int) -> dict | None:
    try:
        status = open(f"/proc/{pid}/status").read()
        threads = int(next(l for l in status.splitlines() if l.startswith("Threads:")).split()[1])
        rss_kb = int(next(l for l in status.splitlines() if l.startswith("VmRSS:")).split()[1])
        fds = len(os.listdir(f"/proc/{pid}/fd"))
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        cpu_ticks = int(f[11]) + int(f[12])
        return {"threads": threads, "rss_mb": rss_kb / 1024, "fds": fds, "cpu_ticks": cpu_ticks}
    except (OSError, StopIteration, ValueError):
        return None


def _ns_sample(ns: str) -> dict:
    if not os.path.exists(f"/run/netns/{ns}"):
        return {}  # real-hardware topology: no crossover namespaces (and no TCP to count)
    tcp = subprocess.run(["nsenter", f"--net=/run/netns/{ns}", "cat", "/proc/net/tcp", "/proc/net/netstat"],
                         capture_output=True, text=True).stdout.splitlines()
    tw = est = 0
    tw_by_dst: Counter = Counter()
    overflows = drops = 0
    netstat_hdr = None
    for line in tcp:
        parts = line.split()
        if len(parts) > 3 and parts[0].endswith(":") and parts[0][:-1].isdigit():
            st = parts[3]
            tw += st == "06"
            est += st == "01"
            if st == "06":
                tw_by_dst[parts[2]] += 1  # rem_address hex ip:port
        elif line.startswith("TcpExt:"):
            if netstat_hdr is None:
                netstat_hdr = parts
            else:
                row = dict(zip(netstat_hdr, parts))
                overflows = int(row.get("ListenOverflows", 0))
                drops = int(row.get("ListenDrops", 0))
    top = {}
    for hexaddr, n in tw_by_dst.most_common(4):
        ip_hex, port_hex = hexaddr.split(":")
        ip = ".".join(str(b) for b in bytes.fromhex(ip_hex)[::-1])
        top[f"{ip}:{int(port_hex, 16)}"] = n
    return {"time_wait": tw, "established": est, "listen_overflows": overflows, "listen_drops": drops,
            "time_wait_by_dst": top}


class Sampler(threading.Thread):
    def __init__(self, servers: list[tuple[str, int]], interval: float = 0.5):
        super().__init__(daemon=True)
        self.servers, self.interval = servers, interval
        self.samples: list[dict] = []
        self._stop_ev = threading.Event()

    def run(self) -> None:
        while not self._stop_ev.is_set():
            t = time.time()
            s = {"t": t, "procs": {name: _proc_sample(pid) for name, pid in self.servers},
                 "ns": {ns: _ns_sample(ns) for ns in NAMESPACES}}
            mem = open("/proc/meminfo").read()
            s["mem_available_mb"] = int(next(l for l in mem.splitlines() if l.startswith("MemAvailable:")).split()[1]) / 1024
            s["loadavg1"] = float(open("/proc/loadavg").read().split()[0])
            self.samples.append(s)
            self._stop_ev.wait(self.interval)

    def stop(self) -> None:
        self._stop_ev.set()
        self.join(timeout=5)


def _summarise_samples(samples: list[dict], t0: float, t1: float) -> dict:
    window = [s for s in samples if t0 <= s["t"] <= t1] or samples
    out = {"servers": {}, "ns": {}}
    names = window[0]["procs"].keys() if window else []
    for name in names:
        pts = [s["procs"][name] for s in window if s["procs"].get(name)]
        if not pts:
            out["servers"][name] = {"dead": True}
            continue
        cpu = (pts[-1]["cpu_ticks"] - pts[0]["cpu_ticks"]) / _CLK_TCK / max(1e-9, t1 - t0) * 100 if len(pts) > 1 else 0
        out["servers"][name] = {"peak_threads": max(p["threads"] for p in pts),
                                "peak_rss_mb": round(max(p["rss_mb"] for p in pts), 1),
                                "peak_fds": max(p["fds"] for p in pts), "cpu_pct": round(cpu, 1)}
    for ns in NAMESPACES:
        pts = [s["ns"][ns] for s in window]
        if not pts or not pts[0]:
            continue  # no crossover namespaces (real-hardware topology)
        peak_by_dst: dict = {}
        for p in pts:
            for d, n in p.get("time_wait_by_dst", {}).items():
                peak_by_dst[d] = max(peak_by_dst.get(d, 0), n)
        out["ns"][ns] = {"peak_time_wait": max(p["time_wait"] for p in pts),
                         "peak_time_wait_by_dst": peak_by_dst,
                         "peak_established": max(p["established"] for p in pts),
                         "listen_overflows": pts[-1]["listen_overflows"] - samples[0]["ns"][ns]["listen_overflows"],
                         "listen_drops": pts[-1]["listen_drops"] - samples[0]["ns"][ns]["listen_drops"]}
    out["min_mem_available_mb"] = round(min(s["mem_available_mb"] for s in window), 0)
    out["peak_loadavg1"] = max(s["loadavg1"] for s in window)
    return out


def _pct(v: list[float], q: float) -> float:
    if not v:
        return float("nan")
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))]


# ---------------------------------------------------------------------------
# orchestrator
# ---------------------------------------------------------------------------
def run_step(*, n_clients: int, workload: str, args) -> dict:
    from tools.bench_ior import start_dist_servers
    from tools.topology import Topology
    topo = Topology.current()
    wrapper = (["gdb", "-q", "-batch", "-ex", "set pagination off", "-ex", "handle SIGPIPE nostop noprint",
                "-ex", "run", "-ex", "bt 30", "-ex", "info threads", "--args"] if args.gdb_servers
               else ["sh", "-c", 'ulimit -c unlimited; exec "$@"', "sh"] if args.core_dumps else None)
    servers, mdts, osts, env = start_dist_servers(n_mdts=args.mdts, n_osts=args.osts,
                                                  extra_env=args.server_env, wrapper=wrapper,
                                                  native=not args.python_servers, topo=topo)
    names = [f"mdt{i}@{a}" for i, (_, a) in enumerate(mdts)] + [f"ost{i}@{a}" for i, (_, a) in enumerate(osts)]
    # Drain every server's stdout/stderr into a log file for the whole step:
    # a crash's traceback/abort message is the evidence, and an undrained
    # PIPE would eventually block a chatty server mid-request instead.
    log_dir = os.path.join(args.log_dir, f"{workload}_N{n_clients}")
    os.makedirs(log_dir, exist_ok=True)
    log_paths = {}

    def _drain(proc, path):
        with open(path, "w") as f:
            for line in proc.stdout:
                f.write(line)
                f.flush()
    for name, p in zip(names, servers):
        log_paths[name] = os.path.join(log_dir, name.replace(":", "_") + ".log")
        threading.Thread(target=_drain, args=(p, log_paths[name]), daemon=True).start()
    # process stats come from this host's /proc: servers on an ssh peer are skipped
    sampler = Sampler([(n, p.pid) for n, p, (node, _) in zip(names, servers, mdts + osts) if not topo.is_remote(node)])
    sampler.start()
    workers = []
    result: dict = {"clients": n_clients, "workload": workload}
    try:
        n_procs = math.ceil(n_clients / args.threads_per_proc)
        ids = list(range(n_clients))
        for w in range(n_procs):
            cfg = {"mdts": [a for _, a in mdts], "osts": [a for _, a in osts], "workload": workload,
                   "attr_ttl": args.attr_ttl, "file_bytes": args.file_bytes,
                   "batcher_workers": args.batcher_workers, "batcher_per_peer": args.batcher_per_peer,
                   "client_ids": ids[w::n_procs]}
            node = "A" if topo.is_remote("B") else ["A", "B"][w % 2]  # clients stay on this host with an ssh peer
            workers.append(topo.popen(node, [os.path.abspath(__file__), "_worker", json.dumps(cfg)], env,
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      text=True))
        setup_errors = {}
        for p in workers:
            line = ""
            while not line.startswith("READY"):
                line = p.stdout.readline()
                if not line:
                    raise RuntimeError(f"worker died during setup: {p.stdout.read()[-2000:]}")
            setup_errors.update(json.loads(line[len("READY "):])["setup_errors"])
        t_go = time.time() + 0.5
        deadline = t_go + args.duration
        if args.mem_probe_at:
            import signal as _signal

            def _probe_all():
                for name, p in zip(names, servers):
                    if name.startswith("ost") and p.poll() is None:
                        try:
                            os.kill(p.pid, _signal.SIGUSR1)
                        except OSError:
                            pass
            for at in args.mem_probe_at:
                threading.Timer(max(0.0, t_go + at - time.time()), _probe_all).start()
        for p in workers:
            p.stdin.write(f"GO {deadline}\n")
            p.stdin.flush()
        results = []
        for p in workers:
            text = ""
            try:
                text, _ = p.communicate(timeout=args.duration + 120)
            except subprocess.TimeoutExpired:
                p.kill()
                text = "WORKER_TIMEOUT"
            line = next((l for l in text.splitlines() if l.startswith("RESULT ")), None)
            results.append(json.loads(line[len("RESULT "):]) if line else {"worker_failed": text[-1500:]})
        t_end = time.time()
    finally:
        sampler.stop()
        for p in workers:
            if p.poll() is None:
                p.kill()
        dead_servers = [n for n, p in zip(names, servers) if p.poll() is not None]
        dead_detail = {n: {"returncode": p.returncode,
                           "log_tail": open(log_paths[n]).read()[-1500:] if os.path.exists(log_paths[n]) else ""}
                       for n, p in zip(names, servers) if p.poll() is not None}
        for p in servers:
            try:
                p.stdin.close()
            except OSError:
                pass
        for p in servers:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        topo.down(quiet=True)

    lat: dict[str, list[float]] = {}
    errors: Counter = Counter()
    ok, hung, failed = 0, [], 0
    per_sec: Counter = Counter()
    for r in results:
        if "worker_failed" in r:
            failed += 1
            errors["worker_failed"] += 1
            continue
        ok += r["ok"]
        per_sec.update({int(k): v for k, v in r.get("per_sec", {}).items()})
        errors.update(r["errors"])
        hung += r["hung"]
        for k, v in r["lat"].items():
            lat.setdefault(k, []).extend(v)
    total_err = sum(errors.values())
    result.update({
        "ops_ok": ok, "ops_err": total_err, "ops_per_s": round(ok / args.duration, 1),
        "error_rate": round(total_err / max(1, ok + total_err), 4),
        "errors_top": dict(errors.most_common(8)), "hung_clients": len(hung), "failed_workers": failed,
        "setup_errors": len(setup_errors), "setup_error_sample": list(setup_errors.values())[:3],
        "dead_servers": dead_servers, "dead_detail": dead_detail,
        "mem_probes": {n: [json.loads(l[len("MEMPROBE "):]) for l in open(log_paths[n]).read().splitlines()
                           if l.startswith("MEMPROBE ")] for n in names if os.path.exists(log_paths[n])},
        "latency_ms": {k: {"p50": round(_pct(v, .5) * 1000, 1), "p99": round(_pct(v, .99) * 1000, 1),
                           "max": round(max(v) * 1000, 1)} for k, v in lat.items()},
        "resources": _summarise_samples(sampler.samples, t_go, min(t_end, deadline)),
        "ops_per_second_timeline": [per_sec.get(t, 0) for t in range(int(t_go), int(deadline))],
    })
    return result


def _print_step(r: dict) -> None:
    res = r["resources"]
    srv = res["servers"]
    peak_thr = max((v.get("peak_threads", 0) for v in srv.values()), default=0)
    peak_fd = max((v.get("peak_fds", 0) for v in srv.values()), default=0)
    tw = sum(v["peak_time_wait"] for v in res["ns"].values())
    ovf = sum(v["listen_overflows"] for v in res["ns"].values())
    worst_p99 = max((v["p99"] for v in r["latency_ms"].values()), default=float("nan"))
    print(f"  N={r['clients']:4d} {r['workload']:9s} ok/s={r['ops_per_s']:8.1f} err={r['error_rate']*100:5.1f}% "
          f"p99max={worst_p99:8.1f}ms srv_threads={peak_thr:4d} srv_fds={peak_fd:5d} TIME_WAIT={tw:6d} "
          f"listen_ovf={ovf:5d} hung={r['hung_clients']} dead={r['dead_servers']}", flush=True)
    if r["errors_top"]:
        print(f"         errors: {r['errors_top']}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", default="md,md_shared,data")
    ap.add_argument("--ladder", default="1,10,50,100,200")
    ap.add_argument("--threads-per-proc", type=int, default=10)
    ap.add_argument("--duration", type=float, default=15.0)
    ap.add_argument("--attr-ttl", type=float, default=0.0)
    ap.add_argument("--file-bytes", type=int, default=1 << 20)
    ap.add_argument("--mdts", type=int, default=2)
    ap.add_argument("--osts", type=int, default=4)
    ap.add_argument("--batcher-workers", type=int, default=4)
    ap.add_argument("--batcher-per-peer", type=int, default=2)
    ap.add_argument("--server-env", default="", help="KEY=VAL,... extra env for server processes")
    ap.add_argument("--gdb-servers", action="store_true", help="run servers under gdb; a crash logs its backtrace")
    ap.add_argument("--core-dumps", action="store_true", help="servers may dump core (into the repo root) on a crash")
    ap.add_argument("--mem-probe-at", default="", help="seconds after GO to SIGUSR1 the OSTs (comma list)")
    ap.add_argument("--python-servers", action="store_true", help="Python MDT/OST servers instead of native C")
    ap.add_argument("--log-dir", default="/tmp/stress_logs")
    ap.add_argument("--report", default=os.path.join(ROOT, "stress_concurrency_report.json"))
    args = ap.parse_args()
    args.server_env = dict(kv.split("=", 1) for kv in args.server_env.split(",") if kv)
    args.mem_probe_at = [float(x) for x in args.mem_probe_at.split(",") if x]
    if os.geteuid() != 0:
        print("needs root (network namespaces)", file=sys.stderr)
        return 2
    report = {"config": {k: v for k, v in vars(args).items()}, "steps": []}
    for wl in args.workload.split(","):
        print(f"== workload {wl} ==", flush=True)
        for n in [int(x) for x in args.ladder.split(",")]:
            try:
                r = run_step(n_clients=n, workload=wl, args=args)
            except Exception as exc:  # noqa: BLE001 -- record and continue up the ladder
                r = {"clients": n, "workload": wl, "harness_error": f"{type(exc).__name__}: {exc}",
                     "trace": traceback.format_exc()[-2000:]}
                print(f"  N={n:4d} {wl:9s} HARNESS ERROR {r['harness_error']}", flush=True)
            else:
                _print_step(r)
            report["steps"].append(r)
            with open(args.report, "w") as f:
                json.dump(report, f, indent=1)
    print(f"report: {args.report}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "_worker":
        worker_main(sys.argv[2])
    else:
        raise SystemExit(main())
