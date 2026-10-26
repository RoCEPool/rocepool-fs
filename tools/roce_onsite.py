"""One command for bringing rocepool up on real RoCE servers -- every step
automated, every result a PASS / WARN / FAIL decision:

    sudo python3 tools/roce_onsite.py --ip 192.168.50.1                    # one server
    sudo python3 tools/roce_onsite.py --ip 192.168.50.1 \\
         --peer root@nodeb --peer-ip 192.168.50.2                          # two servers
    ... --bench quick | full | none                                        # default quick

Steps (stops at the first FAIL unless --force):
  1. preflight   tools/roce_preflight.py on this host, --fix when root
                 (RoCE v2 mode, memlock); writes roce_env.sh (ROCE_TOS from
                 the PFC/DSCP setup, busy-poll, ACK timeout, ...)
  2. build       roce/native/build.sh -> the real libroce_transport.so
                 (installs rdma-core dev packages or builds rdma-core itself)
  3. transport   the connection-pool / SRQ / buffer-pool scenario and the
                 native-server parity scenario, on the real library, over
                 the RoCE IP (NIC loopback)
  4. peer        (--peer) ship this tree over ssh, run the preflight there
                 (--fix), build there
  5. functional  tools/run_crossover_rdma.py --real: smoke, pool, MPI, HPC
                 (striping + MPI-IO), stream -- across the two servers, or
                 through the NIC loopback on one
  6. bench       tools/stress_concurrency.py (ladder quick 1..100 / full
                 1..500, md + md_shared + data), native C servers, pooled
                 connections, both servers
  7. cluster     (--peers, N hosts) ship the tree to the SAME path on every
                 peer, preflight + build on each, then the official IO500
                 (easy + hard + find, tools/io500_run.py) with servers, a
                 client mount and MPI ranks on every host; the report names
                 each phase's bottleneck from measured per-op costs
                 (quick: 30 s stonewall; full: the untouched 300 s procedure)

    sudo python3 tools/roce_onsite.py --ip 192.168.50.1 \\
         --peers root@n2=192.168.50.2,root@n3=192.168.50.3,...            # N servers
Writes onsite_report.json (everything) and onsite_topology.json (so any
later benchmark can run on the same hosts: ROCEPOOL_TOPOLOGY=onsite_topology.json).
Needs on the peer only: sshd with key auth for --peer, python3, gcc.
"""
from __future__ import annotations

import argparse
import json
import re
import os
import shlex
import socket
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tools.roce_preflight import FAIL, PASS, WARN, Preflight, write_env  # noqa: E402

REAL_LIB = os.path.join(ROOT, "roce", "native", "libroce_transport.so")


def say(status: str, step: str, detail: str) -> None:
    print(f"[{status}] {step:10s} {detail}", flush=True)


def eval_pool(res: dict) -> list[str]:
    """Same criteria as tests/test_conn_pool.py."""
    bad = []
    if "error" in res:
        return [res["error"].strip().splitlines()[-1]]
    if res["pooled"] != {"ok": True, "connects": 1}:
        bad.append(f"sequential requests did not reuse one connection: {res['pooled']}")
    if res["unpooled_connects"] != 50:
        bad.append("pool opt-out did not connect per request")
    if res["concurrent"]["bad"]:
        bad.append(f"concurrent callers got foreign replies: {res['concurrent']['bad'][:5]}")
    if not res["restart"]["ok"]:
        bad.append(f"stale pooled connection after server restart not recovered: {res['restart']}")
    if not res["mismatch"]["raised"]:
        bad.append("a mismatched reply was accepted")
    if res["drain"]["reply"] != "drained":
        bad.append(f"server stop lost an in-flight reply: {res['drain']}")
    if res["idle_expiry_connects"] != 2:
        bad.append("idle connections did not expire")
    if res["bounded"]["buffers_after"] - res["bounded"]["buffers_before"] > 40:
        bad.append(f"receive memory grew with connections: {res['bounded']}")
    if res["srq_backpressure"]["errors"] or res["srq_backpressure"]["ok"] != 120:
        bad.append(f"SRQ exhaustion caused failures: {res['srq_backpressure']}")
    return bad


def eval_native(res: dict) -> list[str]:
    """Same criteria as tests/test_native_servers.py."""
    if "error" in res:
        return [res["error"].strip().splitlines()[-1]]
    bad = []
    if res["ost_native"] != res["ost_python"]:
        bad.append("native OST differs from the Python reference")
    if res["mdt_native"] != res["mdt_python"]:
        bad.append("native MDT differs from the Python reference")
    if not all(res["dfs_native"]) or res["dfs_python"] != res["dfs_native"]:
        bad.append(f"distributed FS end-to-end checks failed: {res['dfs_native']}")
    if res["cas_native"] != 320:
        bad.append(f"CAS lost updates: {res['cas_native']}/320")
    return bad


def run_scenario(script: str, port: int, ip: str, env: dict, timeout: int = 600) -> dict:
    out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", script), ROOT, str(port), ip],
                         env=env, capture_output=True, text=True, timeout=timeout, cwd=ROOT)
    line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
    if not line:
        return {"error": f"no result (exit {out.returncode}): {out.stdout[-800:]}{out.stderr[-1500:]}"}
    return json.loads(line[len("RESULT "):])


SPIN_CANDIDATES = (0, 20, 50, 100, 200, 400)
BULK_SPIN_CANDIDATES = (100, 500, 2000)
SPIN_SERVER = r"""
import sys
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
h = RoCETransport().serve_native("ost", sys.argv[2])
print("READY", h.local_address(), flush=True)
sys.stdin.read()
import os; os._exit(0)
"""


def calibrate_bulk_spin(ip: str, env: dict, kv_bench: str, port: int = 21450, secs: float = 2.0,
                        candidates=BULK_SPIN_CANDIDATES, runner=None) -> dict:
    """How long a wait on a bulk transfer busy-polls (ROCE_POLL_BULK_SPIN_US):
    1 MiB puts and gets, 4 threads, per candidate. Without RDMA read in
    hardware the data moves only while the waiting side polls, so the small
    spin (measured on 64-byte gets) is the wrong one for it: between two AWS
    runs it went 100 -> 50 us and ior-easy-write 5.63 -> 4.86 GiB/s. Best
    geometric mean of put and get MB/s; within 3% of it the shortest wins."""
    def run_one(bs):
        e = dict(env, ROCE_POLL_BULK_SPIN_US=str(bs))
        srv = subprocess.Popen([sys.executable, "-c", SPIN_SERVER, ROOT, f"{ip}:{port}"], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=e)
        try:
            line = srv.stdout.readline()
            if not line.startswith("READY"):
                return {"error": (line + srv.stderr.read())[-300:]}
            addr = line.split()[1]
            res = {}
            for mode in ("put", "get"):
                r = subprocess.run([kv_bench, addr, "4", str(secs), str(1 << 20), mode], capture_output=True, text=True,
                                   env=e, timeout=120)
                m = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None)
                if not m:
                    return {"error": (r.stdout + r.stderr)[-300:]}
                res[mode] = json.loads(m[len("RESULT "):])
            return res
        finally:
            srv.stdin.close()
            try:
                srv.wait(timeout=10)
            except subprocess.TimeoutExpired:
                srv.kill()
    runner = runner or run_one
    table = {bs: runner(bs) for bs in candidates}
    ok = {s: r for s, r in table.items() if "error" not in r}
    if not ok:
        return {"table": table, "chosen": None}
    score = {s: (r["put"]["ops_per_s"] * r["get"]["ops_per_s"]) ** 0.5 for s, r in ok.items()}
    best = max(score.values())
    chosen = min(s for s, v in score.items() if v >= 0.97 * best)
    return {"table": table, "score": score, "chosen": chosen}


def calibrate_spin(ip: str, env: dict, kv_bench: str, port: int = 21400, secs: float = 2.0,
                   candidates=SPIN_CANDIDATES, runner=None) -> dict:
    """How long a thread should busy-poll its completion queue before
    sleeping (ROCE_POLL_SPIN_US), measured instead of guessed: a native OST
    and rocepool-kv-bench (1 and 8 threads, 64 B gets) per candidate. Too
    short and every request pays the sleep/wake-up (and a request gap longer
    than the spin keeps it that way); too long burns cores. Picks the best
    geometric mean of 1-thread and 8-thread ops/s; within 3% of it the
    shortest spin wins."""
    def run_one(spin):
        e = dict(env, ROCE_POLL_SPIN_US=str(spin))
        srv = subprocess.Popen([sys.executable, "-c", SPIN_SERVER, ROOT, f"{ip}:{port}"], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=e)
        try:
            line = srv.stdout.readline()
            if not line.startswith("READY"):
                return {"error": (line + srv.stderr.read())[-300:]}
            addr = line.split()[1]
            res = {}
            for th in (1, 8):
                r = subprocess.run([kv_bench, addr, str(th), str(secs), "64", "get"], capture_output=True, text=True,
                                   env=e, timeout=120)
                m = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None)
                if not m:
                    return {"error": (r.stdout + r.stderr)[-300:]}
                res[th] = json.loads(m[len("RESULT "):])
            return res
        finally:
            srv.stdin.close()
            try:
                srv.wait(timeout=10)
            except subprocess.TimeoutExpired:
                srv.kill()
    runner = runner or run_one
    table = {}
    for spin in candidates:
        table[spin] = runner(spin)
    ok = {s: r for s, r in table.items() if "error" not in r}
    if not ok:
        return {"table": table, "chosen": None}
    score = {s: (r[1]["ops_per_s"] * r[8]["ops_per_s"]) ** 0.5 for s, r in ok.items()}
    best = max(score.values())
    chosen = min(s for s, v in score.items() if v >= 0.97 * best)
    return {"table": table, "score": score, "chosen": chosen}


def ssh(peer: str, cmd: str, timeout: int = 1800) -> subprocess.CompletedProcess:
    from tools.topology import SSH_OPTS
    return subprocess.run(["ssh", *SSH_OPTS, peer, cmd], capture_output=True, text=True, timeout=timeout)


def begin(step_name: str, what: str) -> None:
    """a line before a long step, so a run that takes minutes never looks stuck"""
    print(f"[ ... ] {step_name:10s} {what}", flush=True)


PEER_PROBE = ("echo ROCEPOOL_PROBE; id -u; hostname; "
              "(ip -4 -o addr show 2>/dev/null || hostname -I) | tr ' ' '\\n' | grep -E '^[0-9]+\\.' | cut -d/ -f1 | tr '\\n' ' '")


def peer_precheck(peers: list[tuple[str, str]], local_ip: str, runner=None, pinger=None) -> tuple[str, str, dict]:
    """Every peer, before anything is built or shipped, in seconds:
    passwordless SSH as this user (BatchMode), root there (servers, FUSE
    mounts, the preflight fixes all need it), the given RoCE IP really is
    one of the peer's addresses and answers from here, and hostnames that
    differ from each other and from this host's (MPI). -> (status, detail, data)"""
    runner = runner or (lambda peer: ssh(peer, PEER_PROBE, timeout=40))
    pinger = pinger or (lambda ip: subprocess.run(["ping", "-c", "1", "-W", "2", ip], capture_output=True).returncode == 0)
    problems, info = [], {"local": {"hostname": socket.gethostname(), "ip": local_ip}}
    import concurrent.futures as cf

    def probe(peer):
        try:
            return runner(peer)
        except subprocess.TimeoutExpired:
            return None
    with cf.ThreadPoolExecutor(min(32, max(1, len(peers)))) as ex:  # 100 hosts: seconds, not minutes
        answers = list(ex.map(probe, [p for p, _ in peers]))
    for (peer, pip), r in zip(peers, answers):
        if r is None:
            problems.append(f"{peer}: SSH did not answer within 40 s")
            continue
        out = r.stdout.split("ROCEPOOL_PROBE", 1)[-1].split("\n") if "ROCEPOOL_PROBE" in r.stdout else None
        if r.returncode or not out:
            err = (r.stderr or r.stdout).strip()[-300:]
            hint = ""
            if "Permission denied" in err or "publickey" in err:
                hint = (" -- this host's root has no key on the peer: run.sh sets it up when started from a terminal "
                        f"(or: sudo ssh-copy-id {peer})")
            elif "resolve" in err.lower():
                hint = " -- the peer's host name does not resolve here: use its IP (root@<ip>=<RoCE ip>)"
            problems.append(f"{peer}: SSH failed: {err}{hint}")
            continue
        uid, host, addrs = (out[1].strip() if len(out) > 1 else "?"), (out[2].strip() if len(out) > 2 else "?"), \
            (out[3].split() if len(out) > 3 else [])
        info[peer] = {"uid": uid, "hostname": host, "addresses": addrs, "roce_ip": pip}
        if uid != "0":
            user = peer.split("@")[0] if "@" in peer else "?"
            problems.append(f"{peer}: logged in as {user} (uid {uid}), not root -- the servers, FUSE mounts and "
                            f"host fixes need root there: use root@{peer.split('@')[-1]}")
        if addrs and pip not in addrs:
            # a typo'd RoCE IP: when exactly one of the peer's addresses is on
            # this host's RoCE subnet (/24), that is the one -- use it, say so
            same = [a for a in addrs if a.rsplit(".", 1)[0] == local_ip.rsplit(".", 1)[0] and a != local_ip]
            if len(same) == 1 and pinger(same[0]):
                info[peer]["roce_ip"] = same[0]
                info[peer]["roce_ip_given"] = pip
                pip = same[0]
            else:
                problems.append(f"{peer}: {pip} is not one of its addresses {addrs} -- wrong RoCE IP in --peers?")
                continue
        if not pinger(pip):
            problems.append(f"{peer}: {pip} does not answer ping from this host")
    names = {k: v["hostname"] for k, v in info.items() if v.get("hostname")}
    dup = sorted({h for h in names.values() if list(names.values()).count(h) > 1})
    if dup:
        problems.append(f"hosts share a hostname {dup}: {names} -- MPI would take them for one machine; give each its "
                        f"own (sudo hostnamectl set-hostname <new name>)")
    if problems:
        return FAIL, "\n".join(problems), info
    fixed = [f"{k}: --peers said {v['roce_ip_given']}, which is not its address; using {v['roce_ip']} "
             f"(its address on this host's RoCE subnet)" for k, v in info.items() if v.get("roce_ip_given")]
    detail = "; ".join(f"{k}: {v.get('hostname')} {v.get('roce_ip', v.get('ip'))}" for k, v in info.items())
    return (WARN, "\n".join(fixed) + "\n" + detail, info) if fixed else (PASS, detail, info)


def same_machine(peer: str) -> bool:
    """Is `peer` really this machine (or one sharing this tree's
    filesystem)? A fresh random token written here is read back there.
    (Not /etc/machine-id: cloned VMs share it, and their tree would then
    never be shipped.)"""
    import uuid
    token = uuid.uuid4().hex
    path = os.path.join(ROOT, ".rocepool_host_token")
    with open(path, "w") as f:
        f.write(token)
    try:
        return ssh(peer, f"cat {shlex.quote(path)} 2>/dev/null", timeout=60).stdout.strip() == token
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def _tar_to_peer(peer: str, tar_args: list[str], timeout: int) -> None:
    from tools.topology import SSH_OPTS
    tar = subprocess.Popen(["tar", "czf", "-", "-C", ROOT, *tar_args], stdout=subprocess.PIPE)
    try:
        r = subprocess.run(["ssh", *SSH_OPTS, peer, f"mkdir -p {shlex.quote(ROOT)} && tar xzf - -C {shlex.quote(ROOT)}"],
                           stdin=tar.stdout, capture_output=True, text=True, timeout=timeout)
    finally:
        tar.stdout.close()  # ssh gone -> tar gets EPIPE instead of blocking on a full pipe
        try:
            tar.wait(timeout=60)
        except subprocess.TimeoutExpired:
            tar.kill()
            tar.wait()
    if r.returncode or tar.returncode:
        raise RuntimeError(f"copying to {peer}:{ROOT} failed (ssh {r.returncode}, tar {tar.returncode}): "
                           f"{r.stderr.strip()[-500:]}")


def ship_same_path(peer: str, extra: list[str] | None = None, timeout: int = 1800) -> str:
    """Put this tree at the same absolute path on `peer` (MPI starts the same
    binary path on every host), plus the `extra` paths (relative to the
    tree, e.g. the MPI / IO500 builds under .bench_cache). Never deletes
    anything there, and skips a "peer" that is really this machine."""
    if same_machine(peer):
        return "same machine (or shared filesystem) as this one: nothing to copy"
    _tar_to_peer(peer, ["--exclude=.git", "--exclude=__pycache__", "--exclude=.bench_cache",
                        "--exclude=stress_reports", "--exclude=.rocepool_host_token", "--exclude=*.o",
                        "--exclude=*.buildinfo", "--exclude=aws_runs", "--exclude=rocepool-results-*", "."], timeout)
    if extra:
        # a separate archive: in the first one, --exclude=.bench_cache would
        # also drop these explicitly named members (GNU tar applies excludes
        # to named members too) -- the peers then had no MPI / IO500 at all
        _tar_to_peer(peer, list(extra), timeout)
    return f"copied to {ROOT}" + (f" (+ {', '.join(extra)})" if extra else "")


def parse_peers(spec: str) -> list[tuple[str, str]]:
    out = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"--peers entries are user@host=roce_ip, got {item!r}")
        who, ip = item.split("=", 1)
        out.append((who, ip))
    return out


def write_probe(cfile: str, env: dict, cluster: dict, step) -> None:
    """IOR's two write phases (ior-easy-write, ior-hard-write) across every
    host, 30 s each, through the interception client, once per bulk-transfer
    spin: the setting is judged under the real load (a calibration on one
    host chose 2 ms, which made IOR's writes slower), and the OSTs'
    transport counters say where each request's time went."""
    # the variable varied (ROCEPOOL_WRITE_PROBE_VAR, default the bulk spin)
    # is set on every host (ROCEPOOL_FORCE_ENV): a value only node 0 got made
    # an earlier comparison one-sided
    var = os.environ.get("ROCEPOOL_WRITE_PROBE_VAR", "ROCE_POLL_BULK_SPIN_US")
    spins = [s.strip() for s in os.environ.get("ROCEPOOL_WRITE_PROBE_SPINS", "0,200,1000").split(",") if s.strip()]
    tag = "spin" if var == "ROCE_POLL_BULK_SPIN_US" else var.lower().replace("roce_", "").replace("_us", "")
    rows = []
    for sp in spins:
        name, fname = f"writes-{tag}{sp}", f"onsite_writes_{tag}{sp}.json"
        out = os.path.join(ROOT, fname)
        begin(name, f"ior-easy-write + ior-hard-write on {len(cluster['nodes'])} hosts, {var}={sp} on every host")
        cmd = [sys.executable, os.path.join(ROOT, "tools", "io500_run.py"), "--report", out, "--project", "auto",
               "--client", "intercept", "--direct-read", "off", "--stonewall", "30",
               "--phases", "ior-easy-write,ior-hard-write"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600,
                           env=dict(env, ROCEPOOL_CLUSTER=cfile, ROCEPOOL_FORCE_ENV=var, **{var: sp}))
        try:
            with open(out) as f:
                rep = json.load(f)
            ph = {k: round(v["value"], 3) for k, v in (rep.get("phases") or {}).items()}
        except (OSError, ValueError):
            ph = {}
        ok = r.returncode == 0 and "ior-easy-write" in ph and "ior-hard-write" in ph
        rows.append(f"{var}={sp}: ior-easy-write {ph.get('ior-easy-write')} GiB/s, "
                    f"ior-hard-write {ph.get('ior-hard-write')} GiB/s")
        step(name, PASS if ok else FAIL, rows[-1] if ok else (r.stdout + r.stderr)[-1500:], report_file=fname)
    step("writes", PASS, "\n".join(rows))


def cluster_step(args, ip: str, lib: str, fuse_client: str, intercept_lib: str, env: dict, pf_env: dict,
                 step) -> None:
    """7. N-host IO500. Every peer gets the tree (same path), its own
    preflight and build, then io500_run.py runs across all of them."""
    import concurrent.futures as cf
    peers = parse_peers(args.peers or "")  # none: a one-host IO500 on this machine
    from tools.bench_ior import ensure_mpi
    from tools.io500_run import IO500_SRC, ensure_io500
    begin("cluster-prep", "MPI + IO500 (built from deps/ the first time: ~10 min), then this tree and those "
          "builds to every peer, their preflight and build")
    try:
        mpi_bin = ensure_mpi()
        ensure_io500(mpi_bin)
    except Exception as exc:  # noqa: BLE001
        step("cluster", FAIL, f"building MPI / IO500 on this host failed: {exc}")
        return
    rel = lambda p: os.path.relpath(p, ROOT)  # noqa: E731
    built = [rel(os.path.join(ROOT, ".bench_cache", "mpich")), rel(os.path.join(IO500_SRC, "io500")),
             rel(os.path.join(IO500_SRC, "bin"))]

    abi_probe = "python3 -c 'import platform; print(platform.machine(), *platform.libc_ver())'"
    import platform
    local_abi = " ".join([platform.machine(), *platform.libc_ver()])

    def prepare(peer_ip):
        peer, pip = peer_ip
        # MPI / IO500 binaries built here run there only on the same C library
        # (one host Ubuntu 24.04, the other 22.04: glibc 2.39 vs 2.35); else
        # the peer builds its own from deps/, at the same path
        r = ssh(peer, abi_probe, timeout=60)
        same_abi = r.returncode == 0 and r.stdout.strip() == local_abi
        try:
            shipped = ship_same_path(peer, built if same_abi else None)
        except RuntimeError as exc:
            return peer, FAIL, str(exc), None
        d = shlex.quote(ROOT)
        if not same_abi:
            bld = ssh(peer, f"cd {d} && python3 -c \"import sys; sys.path.insert(0, '.'); "
                            f"from tools.bench_ior import ensure_mpi; from tools.io500_run import ensure_io500; "
                            f"ensure_io500(ensure_mpi())\" > .bench_cache_build.log 2>&1; echo rc=$?", timeout=5400)
            if "rc=0" not in bld.stdout:
                tail = ssh(peer, f"tail -n 30 {d}/.bench_cache_build.log", timeout=60).stdout
                return peer, FAIL, f"building MPI / IO500 there ({r.stdout.strip()} vs here {local_abi}) failed: " \
                                   f"{tail[-800:]}", None
            shipped += f"; MPI + IO500 built there ({r.stdout.strip()}; here {local_abi})"
        fix = "--fix" if peer.startswith("root@") or "@" not in peer else ""
        fix += " --soft-roce" if args.soft_roce and fix else ""
        r = ssh(peer, f"cd {d} && python3 tools/roce_preflight.py --ip {shlex.quote(pip)} {fix} "
                      f"--env-out roce_env.sh --json -")
        try:
            ppf = json.loads(r.stdout[r.stdout.index("{"):])
        except ValueError:
            return peer, FAIL, f"preflight produced no report: {(r.stdout + r.stderr)[-600:]}", None
        if ppf["verdict"] == FAIL and not args.force:
            return peer, FAIL, f"preflight failing: {[c['check'] for c in ppf['checks'] if c['status'] == FAIL]}", ppf
        if "softverbs" not in os.path.basename(lib):
            b = ssh(peer, f"cd {d} && {args.peer_build}")
            if b.returncode:
                return peer, FAIL, f"build failed: {(b.stdout + b.stderr)[-600:]}", ppf
        chk = ssh(peer, f"cd {d} && .bench_cache/io500_src/io500 --help >/dev/null 2>&1; echo rc=$?", timeout=60)
        if "rc=0" not in chk.stdout and "rc=1" not in chk.stdout:  # io500 --help exits 0 or 1
            return peer, FAIL, f"the IO500 binary does not run there (different OS/libs?): {chk.stdout[-300:]}", ppf
        return peer, ppf["verdict"], f"{shipped}; preflight {ppf['verdict']} ({ppf['device']} {ppf['ip']})", ppf

    with cf.ThreadPoolExecutor(min(32, len(peers) or 1)) as ex:
        res = list(ex.map(prepare, peers))
    bad = [r for r in res if r[1] == FAIL]
    ok = step("cluster-prep", FAIL if bad else (WARN if any(r[1] == WARN for r in res) else PASS),
              "\n".join(f"{p}: {d}" for p, _, d, _ in res), peers={p: {"status": st, "detail": d} for p, st, d, _ in res})
    if not ok:
        return
    # what every host offers the OST's NVMe tier and the P2P path
    # (tools/nvme_preflight.py); io500_run.py --ost-tier auto applies it
    nv = {}
    def nvme_pf(peer):
        d = shlex.quote(ROOT)
        r = ssh(peer, f"cd {d} && python3 tools/nvme_preflight.py --json --measure", timeout=400) if peer != "local" else \
            subprocess.run([sys.executable, os.path.join(ROOT, "tools", "nvme_preflight.py"), "--json", "--measure"],
                           capture_output=True, text=True, timeout=400)
        try:
            return peer, json.loads(r.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            return peer, {"error": (r.stdout + r.stderr)[-300:]}
    with cf.ThreadPoolExecutor(min(32, len(peers) + 1)) as ex:
        nv = dict(ex.map(nvme_pf, ["local"] + [p for p, _ in peers]))
    tiers = {h: v.get("recommendation", {}).get("tier") for h, v in nv.items()}
    p2ps = {h: v.get("recommendation", {}).get("p2pmem") for h, v in nv.items()}
    speed = {h: (v.get("recommendation", {}).get("tier_write") or {}).get("mib_s") for h, v in nv.items()}
    detail = "\n".join(f"{h}: tier {tiers[h] or 'none (RAM only)'}"
                       + (f" (O_DIRECT write {speed[h]:.0f} MiB/s)" if speed[h] else "")
                       + f"; P2P memory {p2ps[h] or 'none'}"
                       + ("; " + "; ".join(v["recommendation"]["notes"]) if v.get("recommendation", {}).get("notes") else "")
                       + (f"; error {v['error']}" if v.get("error") else "") for h, v in nv.items())
    known = [x for x in speed.values() if x]
    slow = [h for h, x in speed.items() if x and len(known) > 1 and x < max(known) / 3]
    if slow:
        detail += (f"\nslow tier on {', '.join(slow)}: data is striped evenly over every OST, so the cluster writes "
                   f"at about {len(known)} x the slowest host's speed; its OSTs queue up")
    step("nvme-preflight", PASS if all(tiers.values()) and not slow else WARN,
         detail + ("" if all(tiers.values()) else "\nhosts without an NVMe-backed file system keep the pool in RAM: "
                                                 "a valid 300 s write phase must then fit in RAM"),
         hosts={h: v.get("recommendation") for h, v in nv.items()})
    cluster = {"nodes": [{"ip": ip}] + [{"ip": pip, "peer": p} for p, pip in peers], "dir": ROOT,
               "lib": os.path.abspath(lib), "fuse_client": fuse_client, "intercept_lib": intercept_lib, "env": pf_env}
    cfile = os.path.join(ROOT, "onsite_cluster.json")
    with open(cfile, "w") as f:
        json.dump(cluster, f, indent=2)
    if args.bench == "none":
        step("io500", WARN, f"not run (--bench none); cluster ready: ROCEPOOL_CLUSTER={cfile} tools/io500_run.py")
        return
    if args.bench == "writes":
        write_probe(cfile, env, cluster, step)
        return
    runs = [("io500", "onsite_io500.json", ["--stonewall", "30" if args.bench == "quick" else "60"])]
    if args.bench == "full":  # + the untouched official procedure, for the score itself
        runs.append(("io500-valid", "onsite_io500_valid.json", ["--valid"]))
    # the first run probes the clients (FUSE over /dev/fuse, FUSE over
    # io_uring, the interception library); the valid run reuses its choice
    channel, client, direct = "auto", "auto", "auto"
    for name, fname, extra in runs:
        out = os.path.join(ROOT, fname)
        cmd = [sys.executable, os.path.join(ROOT, "tools", "io500_run.py"), "--report", out, "--project", "auto",
               "--fuse-uring", channel, "--client", client, "--direct-read", direct, *extra]
        if name == "io500-valid" and os.path.exists(os.path.join(ROOT, "onsite_io500.json")):
            # the quick run's write speeds size the data: each write phase lasts 300 s
            cmd += ["--rates-from", os.path.join(ROOT, "onsite_io500.json")]
        begin(name, "official IO500 on " + ", ".join(n["ip"] for n in cluster["nodes"])
              + (" (300 s per phase: about an hour)" if "--valid" in extra else " (about 15-30 min)"))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                             env=dict(env, ROCEPOOL_CLUSTER=cfile))
        try:
            text, timed_out = p.communicate(timeout=6 * 3600)[0], False
        except subprocess.TimeoutExpired:
            # SIGTERM, not SIGKILL: io500_run then unmounts and stops servers,
            # ranks and clients on every host before it exits
            p.terminate()
            try:
                text = p.communicate(timeout=600)[0]
            except subprocess.TimeoutExpired:
                p.kill()
                text = p.communicate()[0]
            timed_out = True
        lines = [l for l in text.splitlines() if l.startswith("  ") or l.startswith("IO500") or l.startswith("==")]
        detail = "\n".join(lines[-40:]) if lines else text[-2000:]
        if p.returncode:  # the error itself (a traceback's last line is not indented)
            err = [l for l in text.splitlines() if re.match(r"^[A-Za-z_.]*(Error|Exception)\b", l)]
            if err:
                detail += "\n" + err[-1][:2000]
        # the run worked, but is the result usable? IO500's own INVALID
        # reasons beyond those a short run has by design, and client probes
        # that all failed (the faster client then goes unmeasured), are WARN
        problems = []
        try:
            with open(out) as f:
                rep = json.load(f)
            v = rep.get("validity") or {}
            problems += [f"IO500 invalid: {r}" for r in v.get("unexpected", [])]
            problems += [f"why: {d}" for d in v.get("diagnosis", [])]
            if (rep.get("client_probe") or {}).get("all_failed"):
                problems.append("every client probe failed: the client was not chosen by measurement")
        except (OSError, ValueError):
            pass
        if problems:
            detail = "\n".join(problems) + "\n" + detail
        step(name, FAIL if timed_out or p.returncode else (WARN if problems else PASS),
             ("timed out after 6 h; stopped and cleaned up\n" if timed_out else "") + detail, report_file=fname)
        try:  # the client (and FUSE channel) the probe chose, for the runs that follow
            with open(out) as f:
                rep = json.load(f)
            chosen = rep.get("fuse_channel")
            if "direct_read" in rep:
                direct = "on" if rep["direct_read"] else "off"
            if rep.get("client") == "intercept":
                client = "intercept"
            elif chosen in ("classic", "io_uring"):
                client, channel = "fuse", "on" if chosen == "io_uring" else "off"
        except (OSError, ValueError):
            pass


def gds_step(args, env: dict, topo_file: str, step) -> None:
    """GPUDirect Storage: the GPUs of every host (driver, dma-buf / peermem,
    PCIe path to the NIC), and on a host with GPUs the streaming benchmark
    into GPU memory (tools/bench_gds.py, rocepool's libcufile.so.0)."""
    begin("gds", "GPUs and GPUDirect Storage streaming into GPU memory")
    out = os.path.join(ROOT, "onsite_gds.json")
    r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "bench_gds.py"), "--report", out,
                        "--size-mib", "1024" if args.bench == "quick" else "4096"],
                       capture_output=True, text=True, env=dict(env, ROCEPOOL_TOPOLOGY=topo_file), timeout=7200)
    try:
        with open(out) as f:
            rep = json.load(f)
    except (OSError, ValueError):
        step("gds", WARN, f"GPU check produced no report: {(r.stdout + r.stderr)[-1200:]}")
        return
    peers_gpus = {}
    for peer, _ in (parse_peers(args.peers) if args.peers else ([(args.peer, "")] if args.peer else [])):
        try:  # the tree is at this path on cluster hosts, at --remote-dir on a two-host peer
            p = ssh(peer, f"cd {shlex.quote(ROOT)} 2>/dev/null || cd {shlex.quote(args.remote_dir)}; "
                          "python3 tools/bench_gds.py --inventory", timeout=300)
            inv = json.loads(p.stdout)
            peers_gpus[peer] = {"gpus": len(inv["gpus"]), "dmabuf": inv["dmabuf"], "peermem": inv["peermem"],
                                "notes": inv["notes"]}
        except (ValueError, KeyError, OSError, subprocess.TimeoutExpired) as exc:
            peers_gpus[peer] = {"error": str(exc)[-300:]}
    detail = rep["summary"]
    with_gpus = [f"{h}: {v['gpus']} GPU(s)" for h, v in peers_gpus.items() if v.get("gpus")]
    if with_gpus:
        detail += "; peers with GPUs: " + ", ".join(with_gpus)
    step("gds", rep["status"], detail, report_file="onsite_gds.json", peers=peers_gpus)


def kvcache_step(args, env: dict, topo_file: str, step) -> None:
    """KV-cache reuse (prefix caching, prefill/decode disaggregation): store a
    model's KV blocks, look up a prompt's cached prefix, load it -- straight
    into GPU memory on a GPU host, into host memory otherwise -- and the TTFT
    it gives against recomputing the prefix (tools/bench_kvcache.py)."""
    begin("kvcache", "KV-cache reuse: prefix lookup + load into GPU memory, TTFT")
    out = os.path.join(ROOT, "onsite_kvcache.json")
    cmd = [sys.executable, os.path.join(ROOT, "tools", "bench_kvcache.py"), "--report", out]
    if args.bench != "quick":
        cmd += ["--model", "llama3-70b", "--prompt", "8192", "--prefix", "6144", "--requests", "64", "--prefixes", "2"]
    r = subprocess.run(cmd, capture_output=True, text=True, env=dict(env, ROCEPOOL_TOPOLOGY=topo_file), timeout=7200)
    try:
        with open(out) as f:
            rep = json.load(f)
    except (OSError, ValueError):
        step("kvcache", WARN, f"KV-cache benchmark produced no report: {(r.stdout + r.stderr)[-1200:]}")
        return
    step("kvcache", rep["status"], rep["summary"], report_file="onsite_kvcache.json")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--ip", help="this host's RoCE IP (default: auto-detected RoCE v2 GID)")
    ap.add_argument("--peer", help="user@host of the second server")
    ap.add_argument("--peer-ip", help="the peer's RoCE IP")
    ap.add_argument("--peers", help="N-host run: user@host=roce_ip,user@host=roce_ip,... (every server but this "
                                    "one); the two-host steps use the first of them")
    ap.add_argument("--remote-dir", default="/tmp/rocepool_onsite")
    ap.add_argument("--bench", choices=("quick", "full", "none", "writes"), default="quick",
                    help="writes: only IOR's two write phases across the hosts (30 s each), once per bulk-transfer "
                         "spin in ROCEPOOL_WRITE_PROBE_SPINS (default 0,200,1000) -- a targeted measurement")
    ap.add_argument("--lib", help="use this already-built library instead of building (skips step 2)")
    ap.add_argument("--force", action="store_true", help="continue past FAIL results")
    ap.add_argument("--soft-roce", action="store_true",
                    help="hosts without an RDMA device get Soft-RoCE (rxe0) created on them (AWS without EFA)")
    ap.add_argument("--report", default=os.path.join(ROOT, "onsite_report.json"))
    ap.add_argument("--sysfs-root", default="/", help=argparse.SUPPRESS)  # tests
    ap.add_argument("--configfs-root", default="/sys/kernel/config", help=argparse.SUPPRESS)
    ap.add_argument("--peer-build", default="bash roce/native/build.sh", help=argparse.SUPPRESS)
    args = ap.parse_args()
    # the servers, clients and test scenarios hold a socket per connection
    # and libfabric/verbs descriptors: a 1024-file soft limit (ssh's
    # default) ran out on site; raise it to the hard limit for us and our
    # children
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft != resource.RLIM_INFINITY and (hard == resource.RLIM_INFINITY or soft < hard):
            resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, 1 << 20) if hard != resource.RLIM_INFINITY else 1 << 20, hard))
    except (ValueError, OSError):
        pass
    if args.peers and not args.peer:
        try:
            args.peer, args.peer_ip = parse_peers(args.peers)[0]
        except (ValueError, IndexError) as exc:
            ap.error(str(exc))
    if args.peer and not args.peer_ip:
        ap.error("--peer needs --peer-ip")
    report: dict = {"started": time.strftime("%Y-%m-%d %H:%M:%S"), "steps": {}}
    root = os.geteuid() == 0

    def finish(code: int) -> int:
        st = [s.get("status") for s in report["steps"].values()]
        report["verdict"] = FAIL if code or FAIL in st else (WARN if WARN in st else PASS)
        code = 1 if report["verdict"] == FAIL else 0
        with open(args.report, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\nverdict: {report['verdict']}   report: {args.report}")
        return code

    def step(name: str, status: str, detail: str, **data) -> bool:
        report["steps"][name] = {"status": status, "detail": detail, **data}
        say(status, name, detail)
        return status != FAIL or args.force

    # 1. preflight -----------------------------------------------------
    pf = Preflight(ip=args.ip, sysfs_root=args.sysfs_root, configfs_root=args.configfs_root, fix=root,
                   soft_roce=args.soft_roce).run_all()
    for c in pf["checks"]:
        say(c["status"], "  " + c["check"], c["detail"])
    env_file = os.path.join(ROOT, "roce_env.sh")
    write_env(env_file, pf["env"])
    if not step("preflight", pf["verdict"], f"{pf['device']} port {pf['port']} ({pf['netdev']}) ip {pf['ip']}; "
                f"env: {pf['env'] or '{}'}; fixes: {pf['fixes_applied'] or 'none'}", preflight=pf):
        return finish(1)
    ip = pf["ip"] or args.ip or "127.0.0.1"
    env = dict(os.environ, PYTHONPATH=ROOT, **pf["env"])

    # 1b. the peers: reachable, root, the right RoCE IPs, distinct names --
    # checked before anything is built or shipped
    all_peers = parse_peers(args.peers) if args.peers else ([(args.peer, args.peer_ip)] if args.peer else [])
    if all_peers:
        begin("peers", f"checking {', '.join(p for p, _ in all_peers)} (SSH, root, RoCE IP, hostname)")
        st, detail, info = peer_precheck(all_peers, ip)
        if not step("peers", st, detail, peers=info):
            return finish(1)
        all_peers = [(p, info.get(p, {}).get("roce_ip", pip)) for p, pip in all_peers]
        if args.peers:
            args.peers = ",".join(f"{p}={pip}" for p, pip in all_peers)
        args.peer, args.peer_ip = all_peers[0]

    # 2. build ------------------------------------------------------------
    lib = args.lib
    if not lib:
        # the backend follows the device: verbs (RoCE, Soft-RoCE) or ofi (EFA)
        b = subprocess.run(["bash", os.path.join(ROOT, "roce", "native", "build.sh")], capture_output=True, text=True,
                           env=dict(os.environ, ROCE_BACKEND=pf.get("backend", "auto")))
        lib = REAL_LIB
        if b.returncode or not os.path.exists(lib):
            step("build", FAIL, (b.stdout + b.stderr)[-1500:])
            return finish(1)
        step("build", PASS, " / ".join(l for l in b.stdout.splitlines() if l.startswith("[build]")))
    env["ROCE_TRANSPORT_LIB"] = os.path.abspath(lib)

    # 3. transport scenarios on the real library ----------------------------
    t0 = time.monotonic()
    bad = eval_pool(run_scenario("pool_scenario.py", 21000, ip, env)) + \
        eval_native(run_scenario("native_scenario.py", 21100, ip, env))
    if not step("transport", FAIL if bad else PASS,
                "; ".join(bad) if bad else f"pooling, stale-connection recovery, drain, SRQ back-pressure, bounded "
                f"registration, native OST/MDT parity + CAS atomicity over {ip} ({time.monotonic() - t0:.1f}s)"):
        return finish(1)

    # 3b. completion busy-poll, measured on this NIC and these CPUs --------
    kv_bench = os.path.join(os.path.dirname(os.path.abspath(lib)), "rocepool-kv-bench")
    if "softverbs" not in os.path.basename(lib) and os.path.exists(kv_bench):
        begin("spin", "measuring the completion busy-poll on this NIC (about 30 s)")
        cal = calibrate_spin(ip, env, kv_bench)
        if cal["chosen"] is None:
            step("spin", WARN, f"calibration failed, keeping ROCE_POLL_SPIN_US={pf['env'].get('ROCE_POLL_SPIN_US', 0)}: "
                 f"{list(cal['table'].values())[0]}", calibration=cal)
        else:
            spin = str(cal["chosen"])
            pf["env"]["ROCE_POLL_SPIN_US"] = env["ROCE_POLL_SPIN_US"] = spin
            write_env(env_file, pf["env"])
            rows = "; ".join(f"{s} us: 1 thr {r[1]['ops_per_s']:.0f}/s p50 {r[1]['p50_us']:.0f} us, "
                             f"8 thr {r[8]['ops_per_s']:.0f}/s" for s, r in cal["table"].items() if "error" not in r)
            step("spin", PASS, f"ROCE_POLL_SPIN_US={spin} (measured: {rows})", calibration=cal)
        # ROCEPOOL_BULK_SPIN_CALIBRATE=1 only: a few threads of 1 MiB puts on
        # one host chose 2 ms, which under IOR's 32 ranks a host made writes
        # slower (CPU 80% busy, run 40309495341)
        if os.environ.get("ROCEPOOL_BULK_SPIN_CALIBRATE") == "1" and \
                (os.environ.get("ROCE_BACKEND", "") == "ofi" or "ofi" in pf["env"].get("ROCE_BACKEND", "")):
            begin("bulk-spin", "measuring the busy-poll for bulk transfers (1 MiB puts/gets, about 30 s)")
            bcal = calibrate_bulk_spin(ip, env, kv_bench)
            if bcal["chosen"] is None:
                step("bulk-spin", WARN, f"calibration failed, keeping the default: {list(bcal['table'].values())[0]}",
                     calibration=bcal)
            else:
                pf["env"]["ROCE_POLL_BULK_SPIN_US"] = env["ROCE_POLL_BULK_SPIN_US"] = str(bcal["chosen"])
                write_env(env_file, pf["env"])
                rows = "; ".join(f"{s} us: put {r['put']['ops_per_s']:.0f}/s get {r['get']['ops_per_s']:.0f}/s"
                                 for s, r in bcal["table"].items() if "error" not in r)
                step("bulk-spin", PASS, f"ROCE_POLL_BULK_SPIN_US={bcal['chosen']} (1 MiB, 4 threads: {rows})",
                     calibration=bcal)

    # 4. peer -------------------------------------------------------------
    if args.peer:
        from tools.run_crossover_rdma import ship_tree
        begin("peer", f"copying this tree to {args.peer}:{args.remote_dir}, then its preflight and build")
        try:
            ship_tree(args.peer, args.remote_dir)
        except RuntimeError as exc:
            step("peer", FAIL, str(exc))
            return finish(1)
        d = shlex.quote(args.remote_dir)
        fix = "--fix" if args.peer.startswith("root@") or "@" not in args.peer else ""
        soft = "--soft-roce" if args.soft_roce and fix else ""
        r = ssh(args.peer, f"cd {d} && python3 tools/roce_preflight.py --ip {shlex.quote(args.peer_ip)} {fix} {soft} "
                           f"--env-out roce_env.sh --json -")
        try:
            ppf = json.loads(r.stdout[r.stdout.index("{"):])
        except ValueError:
            step("peer", FAIL, f"peer preflight produced no report: {(r.stdout + r.stderr)[-1200:]}")
            return finish(1)
        if not step("peer", ppf["verdict"], f"{args.peer}: {ppf['device']} {ppf['ip']}; env {ppf['env'] or '{}'}; "
                    f"failing: {[c['check'] for c in ppf['checks'] if c['status'] == FAIL] or 'none'}",
                    preflight=ppf):
            return finish(1)
        b = ssh(args.peer, f"cd {d} && {args.peer_build}")
        if not step("peer-build", FAIL if b.returncode else PASS, (b.stdout + b.stderr)[-600:].strip()):
            return finish(1)

    # the native C FUSE client: built next to the real library by build.sh (or,
    # with --lib pointing at the emulator build, the emulator-linked client)
    if "softverbs" in os.path.basename(lib):
        from roce.native.build_lib import ensure_fuse_client, ensure_intercept_lib
        fuse_client, intercept_lib = ensure_fuse_client(), ensure_intercept_lib()
    else:
        fuse_client = os.path.join(os.path.dirname(os.path.abspath(lib)), "rocepool-fuse")
        intercept_lib = os.path.join(os.path.dirname(os.path.abspath(lib)), "librocepool_intercept.so")

    # 5. functional phases -----------------------------------------------------
    topo = {"A": {"ip": ip},
            "B": ({"ip": args.peer_ip, "peer": args.peer, "dir": args.remote_dir,
                   "lib": "roce/native/libroce_transport.so"} if args.peer else {"ip": ip}),
            "lib": os.path.abspath(lib), "env": pf["env"], "fuse_client": fuse_client}
    topo_file = os.path.join(ROOT, "onsite_topology.json")
    with open(topo_file, "w") as f:
        json.dump(topo, f, indent=2)
    cmd = [sys.executable, os.path.join(ROOT, "tools", "run_crossover_rdma.py"), "--real", "--local-ip", ip,
           "--lib", os.path.abspath(lib), "--report", os.path.join(ROOT, "onsite_crossover.json")]
    if args.peer:
        cmd += ["--peer", args.peer, "--peer-ip", args.peer_ip, "--remote-dir", args.remote_dir, "--no-ship"]
    begin("functional", "smoke / pool / mpi / hpc / stream phases over RoCE (a few minutes)")
    r = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=1800)
    phases = [l for l in r.stdout.splitlines() if l.startswith("[")]
    if not step("functional", PASS if r.returncode == 0 else FAIL, " ".join(p.split("(")[0].strip() for p in phases)
                or (r.stdout + r.stderr)[-1200:], report_file="onsite_crossover.json"):
        return finish(1)

    # 6. bench -----------------------------------------------------------------
    if args.bench not in ("none", "writes"):
        ladder = "1,10,50,100" if args.bench == "quick" else "1,10,50,100,200,500"
        out = os.path.join(ROOT, "onsite_stress.json")
        begin("bench", f"concurrency ladder {ladder} clients (md, md_shared, data)")
        r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "stress_concurrency.py"),
                            "--workload", "md,md_shared,data", "--ladder", ladder,
                            "--duration", "10" if args.bench == "quick" else "20", "--report", out],
                           capture_output=True, text=True, env=dict(env, ROCEPOOL_TOPOLOGY=topo_file), timeout=7200)
        rows = [l.strip() for l in r.stdout.splitlines() if l.strip().startswith("N=")]
        errs = [l for l in rows if " err=  0.0%" not in l or "dead=[]" not in l]
        step("bench", FAIL if r.returncode or not rows else (WARN if errs else PASS),
             "\n".join(rows) if rows else (r.stdout + r.stderr)[-1500:], report_file="onsite_stress.json")
        # IOR + mdtest (real upstream binaries, provisioned by bench_ior.py)
        # through the native C FUSE client, servers on both hosts
        if os.path.exists("/dev/fuse") and root:
            out = os.path.join(ROOT, "onsite_ior.json")
            begin("ior", "IOR + mdtest through the native FUSE client (builds MPI/IOR from deps/ the first time)")
            cmd = [sys.executable, os.path.join(ROOT, "tools", "bench_ior.py"), "--backend", "dist",
                   "--client", "native", "--report", out] + (["--quick"] if args.bench == "quick" else [])
            r = subprocess.run(cmd, capture_output=True, text=True, env=dict(env, ROCEPOOL_TOPOLOGY=topo_file),
                               timeout=14400)
            lines = [l for l in r.stdout.splitlines() if l.startswith("[")]
            bad = [l for l in lines if l.startswith("[FAIL")]
            step("ior", FAIL if r.returncode or bad or not lines else PASS,
                 "\n".join(lines) if lines else (r.stdout + r.stderr)[-1500:], report_file="onsite_ior.json")
            # IO500's "hard" pair: ior-hard (47008-byte strided shared file)
            # + mdtest-hard (3901-byte files, one directory) -- the
            # workloads the thick-KV ops target (docs/THICK_KV.md)
            out = os.path.join(ROOT, "onsite_ior_hard.json")
            begin("ior_hard", "IO500's hard pair: ior-hard + mdtest-hard")
            cmd = [sys.executable, os.path.join(ROOT, "tools", "bench_ior.py"), "--backend", "dist",
                   "--client", "native", "--hard", "--report", out] + \
                (["--hard-segments", "50", "--mdtest-n", "100"] if args.bench == "quick" else [])
            r = subprocess.run(cmd, capture_output=True, text=True, env=dict(env, ROCEPOOL_TOPOLOGY=topo_file),
                               timeout=14400)
            lines = [l for l in r.stdout.splitlines() if l.startswith("[")]
            bad = [l for l in lines if l.startswith("[FAIL")]
            step("ior_hard", FAIL if r.returncode or bad or not lines else PASS,
                 "\n".join(lines) if lines else (r.stdout + r.stderr)[-1500:], report_file="onsite_ior_hard.json")
        else:
            step("ior", WARN, "skipped: needs root and /dev/fuse for the native FUSE client mount")
        # native KV API vs the Python client, same OSTs
        out = os.path.join(ROOT, "onsite_kv.json")
        begin("kv", "native KV API vs the Python client")
        r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "bench_kv.py"), "--report", out,
                            "--seconds", "3" if args.bench == "quick" else "10"],
                           capture_output=True, text=True, env=dict(env, ROCEPOOL_TOPOLOGY=topo_file), timeout=3600)
        rows = [l for l in r.stdout.splitlines() if l.startswith("{")]
        bad = [l for l in rows if '"error"' in l]
        step("kv", FAIL if r.returncode or bad or not rows else PASS,
             "\n".join(rows) if rows else (r.stdout + r.stderr)[-1500:], report_file="onsite_kv.json")
        gds_step(args, env, topo_file, step)
        kvcache_step(args, env, topo_file, step)
    # 7. N-host IO500 ---------------------------------------------------------
    # IO500 on every host given -- this one alone when there are no --peers
    # (a one-node score; it still shows the per-phase limits)
    if args.peers or args.bench != "none":
        if not (os.path.exists("/dev/fuse") and root):
            step("io500", WARN, "skipped: needs root and /dev/fuse for the client mounts")
        else:
            cluster_step(args, ip, lib, fuse_client, intercept_lib, env, pf["env"], step)
    return finish(0)


if __name__ == "__main__":
    sys.exit(main())
