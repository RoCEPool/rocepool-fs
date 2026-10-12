"""One command: two-node RoCE/RDMA test over a back-to-back ("crossover
cable") link, inside a single Linux host. Needs root. No manual steps.

    python3 tools/run_crossover_rdma.py            # everything, exit 0 = all pass
    python3 tools/run_crossover_rdma.py --keep     # leave the topology up afterwards

What it does, in order:
  1. Builds the topology (tools/netlink_veth.py): network namespaces
     rocepool_nodeA (10.99.0.1) and rocepool_nodeB (10.99.0.2) joined by
     one veth pair -- point to point, no bridge, no switch.
  2. Picks the RDMA provider (--provider auto): if this machine can do
     Soft-RoCE (rdma tool + rdma_rxe + rdma-core headers), binds an
     rdma_rxe device to each veth end and builds the REAL
     libroce_transport.so -- real kernel verbs over the cable (this branch
     is untested in the environment the tool was written in, which has
     none of those). Otherwise builds roce_transport.c UNMODIFIED against
     softverbs (roce/native/softverbs/), a strict userspace RC emulator.
     The report records which ran and why -- a softverbs pass is a
     logic-level pass, not a hardware pass.
  3. Phase "smoke": roce/roce_transport_smoketest.py server on nodeB,
     client on nodeA (PUT, GET, 1 MB payload).
  4. Phase "pool": 3 PoolNodes (1 on nodeA, 2 on nodeB) over RoCETransport;
     workloads on both nodes put/get 120 objects each (0 B..256 KB),
     verify byte-exact reads, cross-node CAS rejection, list_prefix
     fan-out, delete.
  5. Phase "mpi": 4 MPI ranks (2 per node) over RoCETransport: barrier,
     bcast, gather, allgather, allreduce_sum, ring send/recv.
  6. Phase "hpc": 4 OST servers + 4 MPI ranks, alternating nodeA/nodeB,
     one shared striped namespace (StripedStorage over RemoteEngine):
     Lustre basics (setstripe/getstripe, placement, coherence, boundary
     overwrite, append, sparse, 8 MiB, unlink), MPI-IO (N-1 segmented,
     N-1 strided, N-N, get_size_all) and MPI basics (ordering, tag
     matching, 4 MiB bcast, reductions, gathers).
  7. Phase "stream": RoCEStream client on nodeA pipelining requests to a
     server on nodeB, reading replies back in 7-byte slices.
  8. Tears the topology down (unless --keep), writes a JSON report.

The same phases run on REAL RoCE hardware (tools/roce_onsite.py drives this;
no manual steps):

    # two servers: this host + a peer reachable by ssh (key auth)
    python3 tools/run_crossover_rdma.py --real --local-ip 192.168.50.1 \
        --peer root@nodeb --peer-ip 192.168.50.2 --lib roce/native/libroce_transport.so
    # one server: both "nodes" on this host, traffic through the NIC loopback
    python3 tools/run_crossover_rdma.py --real --local-ip 192.168.50.1 --lib ...

In --real mode node A is this host and node B is the peer (or this host
again, with port offsets); the peer gets a copy of this tree under
--remote-dir, builds its own library there (roce/native/build.sh), and
its processes run under ssh.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tools import netlink_veth  # noqa: E402

NATIVE = os.path.join(ROOT, "roce", "native")
SOFT_LIB = os.path.join(NATIVE, "softverbs", "libroce_transport_softverbs.so")
REAL_LIB = os.path.join(NATIVE, "libroce_transport.so")
NODE = os.path.join(ROOT, "tools", "crossover_node.py")
A, B = "rocepool_nodeA", "rocepool_nodeB"
IP = {A: "10.99.0.1", B: "10.99.0.2"}
PORT_OFFSET = {A: 0, B: 0}  # B shifted when both nodes share one IP (--real, no peer)


def ep(node: str, port: int) -> str:
    return f"{IP[node]}:{port + PORT_OFFSET[node]}"


from tools.topology import SSH_OPTS, LocalLauncher, NetnsLauncher, SshLauncher  # noqa: E402

LAUNCH: dict = {A: NetnsLauncher(A), B: NetnsLauncher(B)}


def _build_softverbs() -> None:
    from roce.native.build_lib import build_softverbs
    build_softverbs(SOFT_LIB)


def _try_real_rxe_on_veth() -> str | None:
    """On a machine that CAN do RDMA: a veth link only carries RDMA if a
    Soft-RoCE (rdma_rxe) device is bound to each end, so bind one inside
    each namespace and build the real library. Returns None on success,
    else why it couldn't. UNTESTED here: this environment has no rdma
    tool, no rdma_rxe module and no rdma-core headers."""
    if not shutil.which("rdma"):
        return "no `rdma` tool (iproute2 rdma)"
    if subprocess.run(["sh", "-c", "echo '#include <infiniband/verbs.h>' | cc -E -x c - >/dev/null 2>&1"]).returncode:
        return "no rdma-core headers"
    subprocess.run(["modprobe", "rdma_rxe"], capture_output=True)
    for ns, (ifname, _) in netlink_veth.NODES.items():
        r = subprocess.run(["nsenter", f"--net=/run/netns/{ns}", "rdma", "link", "add",
                            f"rxe_{ifname}", "type", "rxe", "netdev", ifname], capture_output=True, text=True)
        if r.returncode:
            return f"could not bind rdma_rxe to {ifname}: {r.stderr.strip()}"
    subprocess.run(["bash", os.path.join(NATIVE, "build.sh")], check=True)
    return None


def choose_provider(requested: str) -> tuple[str, str]:
    if requested in ("auto", "rxe"):
        why_not = _try_real_rxe_on_veth()
        if why_not is None:
            return "real kernel verbs via Soft-RoCE (rdma_rxe) on the veth ends", REAL_LIB
        if requested == "rxe":
            raise SystemExit(f"--provider rxe unavailable: {why_not}")
    else:
        why_not = "softverbs requested"
    _build_softverbs()
    return f"softverbs (userspace RC emulator; real verbs unavailable: {why_not})", SOFT_LIB


class Proc:
    """A node process inside a namespace, with line-oriented stdout."""

    def __init__(self, ns: str, args: list[str], env: dict, name: str):
        self.name = name
        argv, env = LAUNCH[ns].argv(args, env)
        self.p = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, cwd=ROOT,
        )
        self.lines: queue.Queue[str] = queue.Queue()
        self.log: list[str] = []
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        for line in self.p.stdout:
            self.log.append(line.rstrip("\n"))
            self.lines.put(line.rstrip("\n"))
        self.lines.put(None)

    def wait_for(self, prefix: str, timeout: float) -> str:
        deadline = time.monotonic() + timeout
        while True:
            try:
                line = self.lines.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty:
                raise TimeoutError(f"{self.name}: no '{prefix}' within {timeout}s; log:\n" + "\n".join(self.log[-30:]))
            if line is None:
                raise RuntimeError(f"{self.name} exited before '{prefix}'; log:\n" + "\n".join(self.log[-30:]))
            if line.startswith(prefix):
                return line

    def result(self, timeout: float = 120) -> dict:
        return json.loads(self.wait_for("RESULT ", timeout)[len("RESULT "):])

    def say(self, text: str) -> None:
        self.p.stdin.write(text)
        self.p.stdin.flush()

    def stop(self, timeout: float = 15) -> int:
        try:
            self.p.stdin.close()
        except OSError:
            pass
        try:
            return self.p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.p.kill()
            return -9


def phase_smoke(env) -> dict:
    srv = Proc(B, [os.path.join(ROOT, "roce", "roce_transport_smoketest.py"), "server", ep(B, 19000)], env, "smoke-server")
    try:
        srv.wait_for("[server] listening", 20)
        argv, cenv = LAUNCH[A].argv([os.path.join(ROOT, "roce", "roce_transport_smoketest.py"), "client", ep(B, 19000)], env)
        cli = subprocess.run(argv, capture_output=True, text=True, env=cenv, timeout=120, cwd=ROOT)
        ok = cli.returncode == 0 and "ALL CHECKS PASSED" in cli.stdout
        return {"ok": ok, "client_output": cli.stdout.strip().splitlines()[-3:] + cli.stderr.strip().splitlines()[-3:]}
    finally:
        srv.p.send_signal(2)
        srv.stop()


def phase_pool(env) -> dict:
    nodes = [ep(A, 19201), ep(B, 19201), ep(B, 19202)]
    all_ = ",".join(nodes)
    procs = [
        Proc(A, [NODE, "pool-workload", nodes[0], all_], env, "pool-A1"),
        Proc(B, [NODE, "pool-workload", nodes[1], all_], env, "pool-B1"),
        Proc(B, [NODE, "pool-server", nodes[2], all_], env, "pool-B2"),
    ]
    try:
        for p in procs:
            p.wait_for("READY", 20)
        for p in procs[:2]:
            p.say("go\n")
        results = [p.result() for p in procs[:2]]
    finally:
        for p in procs:
            p.stop()
    ok = all(
        r["get_mismatches"] == [] and r["cas_ok"] and r["delete_ok"]
        and r["list_prefix_count"] == r["objects"] + 1 and r["remote_owned"] > 0
        for r in results
    )
    return {"ok": ok, "nodes": nodes, "workloads": results}


def phase_mpi(env) -> dict:
    ranks = [ep(A, 19301), ep(B, 19301), ep(A, 19302), ep(B, 19302)]
    all_ = ",".join(ranks)
    ns_of = [A, B, A, B]
    procs = [Proc(ns_of[r], [NODE, "mpi", str(r), all_], env, f"mpi-rank{r}") for r in range(len(ranks))]
    try:
        results = [p.result(timeout=120) for p in procs]
    finally:
        for p in procs:
            p.stop()
    n = len(ranks)
    expect_ag = [f"r{r}@{ranks[r]}" for r in range(n)]
    ok = all(
        r["bcast"] == "hello-from-root"
        and r["allgather"] == expect_ag
        and r["allreduce_sum"] == n * (n + 1) // 2
        and r["ring_recv"] == f"token-from-{(r['rank'] - 1) % n}"
        and (r["gather"] == [f"r{i}" for i in range(n)] if r["rank"] == 0 else r["gather"] is None)
        for r in results
    )
    return {"ok": ok, "ranks": ranks, "results": results}


def phase_hpc(env) -> dict:
    """Lustre-style striping + MPI-IO + MPI basics: 4 OST servers and 4
    MPI ranks, both alternating nodeA/nodeB, so stripes and collectives
    cross the cable."""
    osts = [ep(A, 19501), ep(B, 19501), ep(A, 19502), ep(B, 19502)]
    ranks = [ep(A, 19601), ep(B, 19601), ep(A, 19602), ep(B, 19602)]
    alt = [A, B, A, B]
    servers = [Proc(alt[i], [NODE, "pool-server", a, a], env, f"ost{i}") for i, a in enumerate(osts)]
    procs: list[Proc] = []
    try:
        for s in servers:
            s.wait_for("READY", 20)
        procs = [Proc(alt[r], [NODE, "hpc", str(r), ",".join(ranks), ",".join(osts)], env, f"hpc-rank{r}")
                 for r in range(len(ranks))]
        results = [p.result(timeout=300) for p in procs]
        # The ranks still run a closing barrier after emitting RESULT; a rank
        # that dies there (or hangs a peer until its recv timeout) is a
        # failure too -- it used to pass silently as a 30 s slow run.
        exits = []
        for p in procs:
            try:
                exits.append(p.p.wait(timeout=60))
            except subprocess.TimeoutExpired:
                exits.append("hung")
    finally:
        for p in procs + servers:
            p.stop()
    names = [c["name"] for c in results[0]["checks"]]
    table = {}
    for res in results:
        for c in res["checks"]:
            e = table.setdefault(c["name"], {"ok": True, "ranks": [], "failed_detail": {}})
            e["ranks"].append(res["rank"])
            if not c["ok"]:
                e["ok"] = False
                e["failed_detail"][res["rank"]] = c["detail"]
    for res in results:  # checks that only some ranks run (P1 on rank 1, ...)
        names += [c["name"] for c in res["checks"] if c["name"] not in names]
    clean_exit = all(x == 0 for x in exits)
    out = {"ok": clean_exit and all(e["ok"] for e in table.values()), "osts": osts, "ranks": ranks,
           "rank_exit_codes": exits, "checks": {name: table[name] for name in names}}
    if not clean_exit:
        out["rank_logs_tail"] = {p.name: p.log[-15:] for p, x in zip(procs, exits) if x != 0}
    return out


def phase_stream(env) -> dict:
    addr = ep(B, 19401)
    srv = Proc(B, [NODE, "pool-server", addr, addr], env, "stream-server")
    try:
        srv.wait_for("READY", 20)
        cli = Proc(A, [NODE, "stream", addr], env, "stream-client")
        try:
            r = cli.result(timeout=120)
        except (RuntimeError, TimeoutError) as exc:
            return {"ok": False, "error": str(exc).splitlines()[:12]}
        finally:
            cli.stop()
    finally:
        srv.stop()
    return {"ok": r["all_ok"], "result": r}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="leave the two-node topology up afterwards")
    ap.add_argument("--phases", default="smoke,pool,mpi,hpc,stream")
    ap.add_argument("--provider", choices=("auto", "softverbs", "rxe"), default="auto",
                    help="auto: Soft-RoCE on the veth ends if this machine supports it, else softverbs")
    ap.add_argument("--report", default=os.path.join(ROOT, "crossover_rdma_report.json"))
    ap.add_argument("--real", action="store_true", help="run over real RDMA hardware (see module docstring)")
    ap.add_argument("--local-ip", help="--real: this host's RoCE interface IP")
    ap.add_argument("--peer", help="--real: user@host of the second server (ssh key auth)")
    ap.add_argument("--peer-ip", help="--real: the peer's RoCE interface IP")
    ap.add_argument("--lib", default=REAL_LIB, help="--real: the real libroce_transport.so on this host")
    ap.add_argument("--remote-dir", default="/tmp/rocepool_onsite", help="--real: where the peer's copy goes")
    ap.add_argument("--remote-build", default="bash roce/native/build.sh",
                    help="--real: command (run in --remote-dir on the peer) that builds its library")
    ap.add_argument("--no-ship", action="store_true", help="--real: the peer already has the tree (and library)")
    ap.add_argument("--remote-lib", default="roce/native/libroce_transport.so",
                    help="--real: the peer's library, relative to --remote-dir")
    args = ap.parse_args()
    if args.real:
        return main_real(args)
    if os.geteuid() != 0 or not shutil.which("nsenter"):
        print("needs root and nsenter (util-linux)", file=sys.stderr)
        return 2

    report: dict = {"topology": f"{A}({IP[A]}) <==veth, no switch==> {B}({IP[B]})"}
    netlink_veth.up()
    try:
        provider, lib = choose_provider(args.provider)
        report["rdma_provider"] = provider
        env = dict(os.environ, ROCE_TRANSPORT_LIB=lib, PYTHONPATH=ROOT)
        run_phases(args.phases.split(","), env, report)
    finally:
        if not args.keep:
            netlink_veth.down()
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)
    print(f"provider: {report.get('rdma_provider')}\nreport:   {args.report}")
    return 0 if all(v.get("ok") for k, v in report.items() if isinstance(v, dict)) else 1


PHASES = {"smoke": phase_smoke, "pool": phase_pool, "mpi": phase_mpi, "hpc": phase_hpc, "stream": phase_stream}


def run_phases(names: list[str], env: dict, report: dict) -> None:
    for name in names:
        t0 = time.monotonic()
        try:
            res = PHASES[name](env)
        except Exception as exc:  # noqa: BLE001
            res = {"ok": False, "error": f"{type(exc).__name__}: {exc}".splitlines()[:15]}
        res["seconds"] = round(time.monotonic() - t0, 2)
        report[name] = res
        print(f"[{'PASS' if res['ok'] else 'FAIL'}] {name:6s} ({res['seconds']}s)", flush=True)


def ship_tree(peer: str, remote_dir: str) -> None:
    """Copy this tree to the peer (tar over ssh: no rsync needed there)."""
    import shlex
    # sources only (a few MB): no caches, builds or results -- .bench_cache
    # alone holds GBs once MPI and IO500 are built, and the peer builds its own
    tar = subprocess.Popen(["tar", "czf", "-", "--exclude=.git", "--exclude=__pycache__", "--exclude=*.so",
                            "--exclude=*.o", "--exclude=*.a", "--exclude=*.buildinfo", "--exclude=./.bench_cache",
                            "--exclude=./aws_runs", "--exclude=./mnt", "--exclude=rocepool-results-*",
                            "--exclude=./roce/native/rocepool-fuse", "--exclude=./roce/native/rocepool-kv-bench",
                            "--exclude=./roce/native/softverbs/rocepool-fuse",
                            "--exclude=./roce/native/softverbs/rocepool-kv-bench", "--exclude=./roce/native/ofi",
                            "--exclude=stress_reports", "--exclude=./cluster.conf", "--exclude=*.json", "-C", ROOT, "."], stdout=subprocess.PIPE)
    try:
        r = subprocess.run(["ssh", *SSH_OPTS, peer,
                            f"rm -rf {shlex.quote(remote_dir)} && mkdir -p {shlex.quote(remote_dir)} && "
                            f"tar xzf - -C {shlex.quote(remote_dir)}"], stdin=tar.stdout, capture_output=True, text=True,
                           timeout=900)
    except subprocess.TimeoutExpired:
        tar.kill()
        raise RuntimeError(f"copying the tree to {peer} took over 15 min (network?)")
    tar.wait()
    if r.returncode or tar.returncode:
        raise RuntimeError(f"copying the tree to {peer} failed: {r.stderr.strip()[-500:]}")


def main_real(args) -> int:
    """Same phases over real RoCE: node A = this host, node B = --peer (or
    this host again through the NIC's loopback)."""
    import shlex
    import uuid
    if not args.local_ip:
        print("--real needs --local-ip (this host's RoCE IP)", file=sys.stderr)
        return 2
    if not os.path.exists(args.lib):
        print(f"--real: library {args.lib} not found (build it: bash roce/native/build.sh)", file=sys.stderr)
        return 2
    IP[A] = args.local_ip
    LAUNCH[A] = LocalLauncher()
    report: dict = {"mode": "real"}
    if args.peer:
        if not args.peer_ip:
            print("--peer needs --peer-ip", file=sys.stderr)
            return 2
        IP[B] = args.peer_ip
        if not args.no_ship:
            ship_tree(args.peer, args.remote_dir)
        b = subprocess.CompletedProcess([], 0) if args.no_ship else subprocess.run(["ssh", *SSH_OPTS, args.peer,
                            f"cd {shlex.quote(args.remote_dir)} && {args.remote_build}"],
                           capture_output=True, text=True)
        if b.returncode:
            print(f"building on {args.peer} failed:\n{b.stdout[-1500:]}{b.stderr[-1500:]}", file=sys.stderr)
            return 1
        LAUNCH[B] = SshLauncher(args.peer, args.remote_dir,
                                os.path.join(args.remote_dir, args.remote_lib),
                                uuid.uuid4().hex[:8])
        report["topology"] = f"{args.local_ip} (this host) <==RoCE==> {args.peer_ip} ({args.peer})"
    else:
        IP[B] = args.local_ip
        PORT_OFFSET[B] = 50
        LAUNCH[B] = LocalLauncher()
        report["topology"] = f"{args.local_ip} -> itself (NIC loopback; single host)"
    report["rdma_provider"] = (f"softverbs emulator ({args.lib}): exercises the --real plumbing only"
                               if "softverbs" in args.lib else f"real kernel verbs ({args.lib})")
    env = dict(os.environ, ROCE_TRANSPORT_LIB=os.path.abspath(args.lib), PYTHONPATH=ROOT)
    try:
        run_phases(args.phases.split(","), env, report)
    finally:
        for launcher in LAUNCH.values():
            launcher.cleanup()
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)
    print(f"provider: {report.get('rdma_provider')}\nreport:   {args.report}")
    return 0 if all(v.get("ok") for k, v in report.items() if isinstance(v, dict)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
