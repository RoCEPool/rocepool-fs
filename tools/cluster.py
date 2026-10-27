"""N nodes for multi-node runs (tools/io500_run.py): where each node's
servers, FUSE client and MPI ranks execute.

Simulated (default): tools/netns_cluster.py -- N network + mount namespaces
on one bridge, the softverbs RDMA emulator. Real: a cluster file
(ROCEPOOL_CLUSTER=<json>, written by tools/roce_onsite.py --peers ...):

    {"nodes": [{"ip": "192.168.50.1"},
               {"ip": "192.168.50.2", "peer": "root@node2"}, ...],
     "dir": "/abs/path/of/this/tree",          # same path on every host
     "lib": "/abs/path/roce/native/libroce_transport.so",
     "fuse_client": "/abs/path/roce/native/rocepool-fuse",
     "intercept_lib": "/abs/path/roce/native/librocepool_intercept.so",   # optional
     "env": {"ROCE_TOS": "104"}}

Node 0 is always this host. The tree lives at the same absolute path on
every host, because MPI starts the same binary path everywhere.
"""
from __future__ import annotations

import json
import os
import shlex
import stat
import subprocess
import time
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Runs on every node: python3 -c NAMES_SCRIPT '{"name": "ip", ...}' check
#                  or python3 -c NAMES_SCRIPT '{"name": "ip", ...}' fix '["name", ...]'
# MPICH (ch3:sock, as built by tools/bench_ior.ensure_mpi) finds a rank by
# its host's NAME: every host must resolve every host's hostname to the same
# reachable address. Found rehearsing the N-host run: a name that does not
# resolve made MPI_Init hang silently; one that resolves to loopback
# (Debian's "127.0.1.1 <hostname>") made the other ranks' connections fail.
# `check` prints what this host resolves each name to; Cluster.ensure_names
# compares all hosts and has only the broken names fixed: a marked block at
# the top of /etc/hosts (first match wins, so it beats 127.0.1.1), rebuilt
# from the current cluster on every run, written atomically.
NAMES_SCRIPT = r"""
import json, os, socket, subprocess, sys, tempfile
want = json.loads(sys.argv[1])
BEGIN, END = "# BEGIN rocepool cluster (tools/cluster.py)", "# END rocepool cluster"
def resolve():
    out = {}
    for name in want:
        try:
            out[name] = socket.gethostbyname(name)
        except OSError:
            out[name] = None
    return out
res = {}
if sys.argv[2] == "fix":
    fix = set(json.loads(sys.argv[3]))
    try:
        path = os.path.realpath("/etc/hosts")
        with open(path, encoding="utf-8", errors="surrogateescape") as f:
            text = f.read()
        if not os.path.exists(path + ".rocepool.bak"):
            with open(path + ".rocepool.bak", "w", encoding="utf-8", errors="surrogateescape") as f:
                f.write(text)
        lines = text.splitlines()
        if BEGIN in lines and END in lines[lines.index(BEGIN):]:
            i = lines.index(BEGIN)
            j = lines.index(END, i)
            fix |= {l.split()[1] for l in lines[i + 1:j] if len(l.split()) >= 2 and l.split()[1] in want}
            del lines[i:j + 1]
        block = [BEGIN] + [want[n] + " " + n for n in sorted(fix)] + [END]
        new = "\n".join(block + lines) + "\n"
        d = os.path.dirname(path)
        fd, tmp = tempfile.mkstemp(dir=d, prefix=".hosts.rocepool.")
        try:
            with os.fdopen(fd, "w", encoding="utf-8", errors="surrogateescape") as f:
                f.write(new)
                f.flush()
                os.fsync(f.fileno())
            os.chmod(tmp, os.stat(path).st_mode & 0o7777)
            try:
                os.replace(tmp, path)
            except OSError:  # a bind-mounted /etc/hosts (containers) cannot be replaced: rewrite in place
                with open(path, "w", encoding="utf-8", errors="surrogateescape") as f:
                    f.write(new)
                    f.flush()
                    os.fsync(f.fileno())
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        try:  # SELinux: give the new file /etc/hosts' proper label
            subprocess.run(["restorecon", path], capture_output=True, timeout=30)
        except (OSError, subprocess.SubprocessError):
            pass
        res["fixed"] = sorted(fix)
    except (OSError, UnicodeError, ValueError) as exc:
        res["error"] = type(exc).__name__ + ": " + str(exc)
res["resolved"] = resolve()
print(json.dumps(res))
"""


class Node:
    def __init__(self, index: int, ip: str, peer: str | None = None, sim: bool = False):
        self.index, self.ip, self.peer, self.sim = index, ip, peer, sim

    @property
    def local(self) -> bool:
        return self.peer is None

    def wrap(self, argv: list[str], *, net: bool = True, env: dict | None = None) -> list[str]:
        """argv -> a command that runs it on this node (namespaces / ssh)."""
        if self.sim:
            from tools.netns_cluster import MNTNS_DIR, NETNS_DIR, node_name
            ns = node_name(self.index)
            pre = ["nsenter", f"--mount={os.path.join(MNTNS_DIR, ns)}"]
            if net:
                pre.insert(1, f"--net={os.path.join(NETNS_DIR, ns)}")
            return pre + argv
        if self.local and (env or os.environ).get("ROCEPOOL_HEAD_VIA_SSH") != "1":
            return list(argv)
        # ROCEPOOL_HEAD_VIA_SSH=1: node 0's processes start through ssh too,
        # exactly as the peers' (its own roce_env.sh, the same passthrough):
        # a diagnosis -- node 0's OST pulled payloads 2x slower than a peer's
        target = self.peer or f"root@{self.ip}"
        # only what must be the same on every host; each host's own RoCE
        # tuning (ROCE_TOS, busy-poll, ... from its preflight) comes from its
        # roce_env.sh, which node 0's values must not override
        # ROCEPOOL_FORCE_ENV=NAME,...: these too, on every host (a targeted
        # measurement that must set e.g. ROCE_POLL_SPIN_US the same everywhere
        # -- otherwise only node 0's processes get it)
        force = {n.strip() for n in (env or {}).get("ROCEPOOL_FORCE_ENV", "").split(",") if n.strip()}
        passthru = " ".join(f"{k}={shlex.quote(v)}" for k, v in (env or {}).items()
                            if k.startswith("ROCEPOOL") or k in ("ROCE_TRANSPORT_LIB", "ROCE_MAX_FRAME_BYTES")
                            or k in force)
        cmd = (f"cd {shlex.quote(ROOT)} && {{ [ -f roce_env.sh ] && . ./roce_env.sh; true; }} && "
               f"exec env {passthru} PYTHONPATH={shlex.quote(ROOT)} {' '.join(shlex.quote(a) for a in argv)}")
        from tools.topology import SSH_OPTS
        return ["ssh", *SSH_OPTS, target, cmd]


class Cluster:
    def __init__(self, nodes: list[Node], *, real: bool, lib: str = "", fuse_client: str = "",
                 intercept_lib: str = "", env: dict | None = None, describe: str = ""):
        self.nodes, self.real, self.lib, self.fuse_client = nodes, real, lib, fuse_client
        self.intercept_lib = intercept_lib  # LD_PRELOAD client library, same path on every host
        self.extra_env = env or {}
        self.describe = describe
        self._procs: list[subprocess.Popen] = []

    @classmethod
    def sim(cls, n: int) -> "Cluster":
        from tools.netns_cluster import node_ip
        return cls([Node(i, node_ip(i), sim=True) for i in range(n)], real=False,
                   describe=f"simulated: {n} network+mount namespaces on one bridge, softverbs RDMA emulator, "
                            f"{os.cpu_count()} CPU cores shared by all nodes")

    @classmethod
    def from_file(cls, path: str) -> "Cluster":
        with open(path) as f:
            cfg = json.load(f)
        nodes = [Node(i, n["ip"], None if i == 0 else n.get("peer")) for i, n in enumerate(cfg["nodes"])]
        if any(i and not n.peer for i, n in enumerate(nodes)):
            raise ValueError("every node after the first needs a 'peer' (ssh target)")
        if cfg.get("dir", ROOT) != ROOT:
            raise ValueError(f"the tree must be at the same path on every host ({ROOT}), cluster file says {cfg['dir']}")
        return cls(nodes, real=True, lib=cfg["lib"], env=cfg.get("env"),
                   fuse_client=cfg.get("fuse_client", os.path.join(ROOT, "roce", "native", "rocepool-fuse")),
                   intercept_lib=cfg.get("intercept_lib",
                                         os.path.join(ROOT, "roce", "native", "librocepool_intercept.so")),
                   describe=f"real RoCE: {len(nodes)} hosts " + ", ".join(n.ip for n in nodes))

    @classmethod
    def current(cls, n: int = 10) -> "Cluster":
        path = os.environ.get("ROCEPOOL_CLUSTER")
        return cls.from_file(path) if path else cls.sim(n)

    # -- lifecycle ---------------------------------------------------------
    def up(self) -> None:
        if self.real:
            # anything an earlier run of this tree left on the hosts (servers
            # still exiting, ranks, mounts) goes first, and is waited for
            self.down()
            return
        if not self.real:
            from tools import netns_cluster
            netns_cluster.up(len(self.nodes))
            from roce.native.build_lib import ensure_fuse_client, ensure_intercept_lib, ensure_softverbs_lib
            self.lib = ensure_softverbs_lib()
            self.fuse_client = ensure_fuse_client()
            self.intercept_lib = ensure_intercept_lib()

    def down(self) -> None:
        for p in self._procs:
            if p.poll() is None:
                p.kill()
        # wait for them to be gone: a server killed but still exiting (unpinning
        # tens of GiB of registered memory takes seconds) keeps its RDMA port
        # and its tier file's blocks -- the next run's servers then failed with
        # "rdma_bind_addr: Address already in use" and saw a fuller disk
        for p in self._procs:
            try:
                p.wait(timeout=120)
            except subprocess.TimeoutExpired:
                pass
        if self.real:
            # everything this run may have left on any host, incl. MPI ranks
            # orphaned by a failed start (their ssh parents are already gone).
            # [x]yz: a pattern that cannot match this shell's own command line,
            # or pkill would kill the shell before the later commands ran.
            # only processes started from THIS tree: other MPI jobs' hydra
            # proxies, other FUSE mounts etc. on these hosts are not ours
            def pat(path: str) -> str:
                import re
                return shlex.quote(f"[{path[0]}]{re.escape(path[1:])}")
            fuse = self.fuse_client or os.path.join(ROOT, "roce", "native", "rocepool-fuse")
            kill = " ; ".join([
                f"pkill -f {pat(os.path.join(ROOT, 'tools', 'crossover_node.py'))}' (mdt|ost)-native'",
                f"pkill -f {pat(fuse)}' --mount'",
                f"pkill -9 -f {pat(os.path.join(ROOT, '.bench_cache', 'io500_src', 'io500'))}",
                f"pkill -9 -f {pat(os.path.join(ROOT, '.bench_cache', 'mpich', 'bin', 'hydra_pmi_proxy'))}",
                "true"])
            servers = pat(os.path.join(ROOT, 'tools', 'crossover_node.py')) + "' (mdt|ost)-native'"
            # then until none of our servers is left on the host (up to 120 s), then -9
            wait = (f"i=0; while pgrep -f {servers} >/dev/null && [ $i -lt 240 ]; do sleep 0.5; i=$((i+1)); done; "
                    f"pkill -9 -f {servers}; sleep 1; true")

            def reap(n):
                try:
                    subprocess.run(n.wrap(["sh", "-c", kill + " ; " + wait]), capture_output=True, timeout=200)
                except subprocess.TimeoutExpired:
                    pass
            import concurrent.futures as cf
            with cf.ThreadPoolExecutor(min(32, len(self.nodes))) as ex:
                list(ex.map(reap, self.nodes))
        else:
            from tools import netns_cluster
            netns_cluster.down(quiet=True)

    def env(self, extra: dict | None = None) -> dict:
        return dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, **self.extra_env, **(extra or {}))

    def popen(self, i: int, argv: list[str], env: dict, **kw) -> subprocess.Popen:
        p = subprocess.Popen(self.nodes[i].wrap(argv, env=env), env=env, cwd=ROOT, **kw)
        self._procs.append(p)
        return p

    def run(self, i: int, argv: list[str], env: dict | None = None, *, net: bool = True, **kw):
        env = env or self.env()
        return subprocess.run(self.nodes[i].wrap(argv, net=net, env=env), env=env, cwd=ROOT, **kw)

    def _names(self, i: int, want: str, env: dict | None, *extra: str) -> dict:
        r = self.run(i, ["python3", "-c", NAMES_SCRIPT, want, *extra], env, capture_output=True, text=True,
                     timeout=120)
        try:
            return json.loads(r.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            return {"error": f"exit {r.returncode}: {(r.stdout + r.stderr)[-300:]}", "resolved": {}}

    @staticmethod
    def _broken(names: list[str], resolved: dict[int, dict]) -> dict[str, dict]:
        """name -> {node: address} for every name that some host cannot
        resolve, resolves to loopback, or resolves differently from another
        host (e.g. only the host itself resolves its name, via
        nss-myhostname to a local docker0 address)."""
        out = {}
        for name in names:
            seen = {i: r.get(name) for i, r in resolved.items()}
            vals = set(seen.values())
            if None in vals or len(vals) != 1 or any(v.startswith("127.") for v in vals if v):
                out[name] = seen
        return out

    def ensure_names(self, names: list[str], env: dict | None = None) -> dict:
        """Every host must resolve every host's hostname to one common,
        non-loopback address (NAMES_SCRIPT). Broken names are fixed on every
        host where we are root (a marked /etc/hosts block mapping them to
        their cluster IPs); a name every host already resolves consistently
        is left alone. Returns the record for the report; raises if any name
        is still broken afterwards."""
        want = json.dumps({name: n.ip for name, n in zip(names, self.nodes)})
        first = {n.index: self._names(n.index, want, env, "check") for n in self.nodes}
        broken = self._broken(names, {i: r.get("resolved", {}) for i, r in first.items()})
        record = {"before": {i: r.get("resolved") for i, r in first.items()}, "broken": broken}
        if not broken:
            return record
        fixed = {n.index: self._names(n.index, want, env, "fix", json.dumps(sorted(broken))) for n in self.nodes}
        record["fix"] = {i: {k: v for k, v in r.items() if k != "resolved"} for i, r in fixed.items()}
        still = self._broken(names, {i: r.get("resolved", {}) for i, r in fixed.items()})
        for _ in range(3):  # a resolver cache (nscd, sssd) may still answer from before the fix
            if not still:
                break
            for n in self.nodes:
                self.run(n.index, ["sh", "-c", "nscd -i hosts 2>/dev/null; sss_cache -E 2>/dev/null; true"], env,
                         capture_output=True, timeout=60)
            time.sleep(2)
            fixed = {n.index: self._names(n.index, want, env, "check") | {k: v for k, v in fixed[n.index].items()
                                                                          if k != "resolved"} for n in self.nodes}
            still = self._broken(names, {i: r.get("resolved", {}) for i, r in fixed.items()})
        record["after"] = {i: r.get("resolved") for i, r in fixed.items()}
        if still:
            errors = {i: r["error"] for i, r in fixed.items() if r.get("error")}
            lines = "\n".join(f"{n.ip} {name}" for name, n in zip(names, self.nodes))
            raise RuntimeError(f"hostnames do not resolve to one reachable address on every host: {still} "
                               f"(MPI finds ranks by host name); fixing failed: {errors or 'no error reported'}. "
                               f"Put these lines first in /etc/hosts on every host:\n{lines}")
        return record

    # -- MPI ---------------------------------------------------------------
    def mpi_files(self, workdir: str, ranks_per_node: int) -> tuple[str, str, list[str]]:
        """(hostfile, launcher script, extra mpiexec args). The launcher maps a
        hostfile name to 'enter node i's mount namespace' (simulated: ranks
        see that node's FUSE mount) or 'ssh to host i' (real). Real hosts are
        named by their cluster IP: hydra hands each rank its hostfile name as
        the address to advertise (MPIR_CVAR_CH3_INTERFACE_HOSTNAME), and a
        host's own name often resolves to 127.0.1.1."""
        os.makedirs(workdir, exist_ok=True)
        # hydra starts every rank in mpiexec's working directory, on every
        # host: it must exist there too. (The client probe's directories
        # existed only here, so on real clusters every probe died at launch
        # -- "process manager error waiting for completion" -- and the run
        # never got the interception client.)
        for n in self.nodes:
            if self.real and not n.local:
                self.run(n.index, ["mkdir", "-p", workdir], net=False, capture_output=True, timeout=60)
        label = (lambda n: n.ip) if self.real else (lambda n: f"rpnode{n.index}")
        hostfile = os.path.join(workdir, "hosts")
        with open(hostfile, "w") as f:
            for n in self.nodes:
                f.write(f"{label(n)}:{ranks_per_node}\n")
        cases = []
        for n in self.nodes:
            if n.sim:
                from tools.netns_cluster import MNTNS_DIR, node_name
                run = f'exec nsenter --mount={MNTNS_DIR}/{node_name(n.index)} sh -c "$*"'
            elif n.local:
                run = 'exec sh -c "$*"'
            else:
                from tools.topology import SSH_OPTS_SH
                run = f'exec ssh {SSH_OPTS_SH} {shlex.quote(n.peer)} "$*"'
            cases.append(f"  {label(n)}) {run};;")
        script = os.path.join(workdir, "launch.sh")
        with open(script, "w") as f:
            f.write("#!/bin/sh\n# MPICH hydra -launcher-exec: [ssh options] HOST COMMAND...\n"
                    'while [ "${1#-}" != "$1" ]; do\n'
                    '  case "$1" in -o|-p|-i|-l|-F|-J|-b|-c|-E|-m|-S|-W|-w) shift 2;; *) shift;; esac\n'
                    'done\nhost=$1; shift\ncase "$host" in\n'
                    + "\n".join(cases) + '\n  *) echo "unknown host $host" >&2; exit 1;;\nesac\n')
        os.chmod(script, os.stat(script).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        extra = ["-launcher", "ssh", "-launcher-exec", script]
        if self.real:
            extra += ["-localhost", self.nodes[0].ip]  # peers' proxies call back here
        return hostfile, script, extra


if __name__ == "__main__":
    sys.path.insert(0, ROOT)
    c = Cluster.current(int(sys.argv[1]) if len(sys.argv) > 1 else 10)
    print(c.describe)
