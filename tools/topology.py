"""Where the benchmark / test processes of a two-node run execute.

Two nodes, "A" and "B". By default they are the two network namespaces of
the crossover-cable rig (tools/netlink_veth.py) with the softverbs library;
with ROCEPOOL_TOPOLOGY=<json file> they are real hosts on a real RoCE
fabric (written by tools/roce_onsite.py):

    {"A": {"ip": "192.168.50.1"},
     "B": {"ip": "192.168.50.2", "peer": "root@nodeb", "dir": "/tmp/rocepool_onsite",
           "lib": "roce/native/libroce_transport.so"},
     "lib": "/abs/path/roce/native/libroce_transport.so",
     "env": {"ROCE_TOS": "104"}}

Node A always runs on this host (in real mode: in the root namespace); B
runs over ssh, or on this host too (same IP -> B's ports are shifted) when
"B" has no "peer". tools/bench_ior.py (dist), tools/stress_concurrency.py
and tools/run_crossover_rdma.py all launch through this.
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Every ssh this project runs: never prompt (BatchMode), accept a host key
# seen for the first time instead of failing on it (a fresh cluster is not in
# known_hosts yet; a CHANGED key still fails), and give up on an unreachable
# host instead of hanging.
SSH_OPTS = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=15",
            "-o", "ServerAliveInterval=60", "-o", "ServerAliveCountMax=5"]
SSH_OPTS_SH = " ".join(SSH_OPTS)


class NetnsLauncher:
    remote = False

    def __init__(self, ns: str):
        self.ns = ns

    def argv(self, args: list[str], env: dict, wrapper: list[str] | None = None) -> tuple[list[str], dict]:
        return ["nsenter", f"--net=/run/netns/{self.ns}", *(wrapper or []), sys.executable, "-u", *args], env

    def cmd(self, argv: list[str]) -> list[str]:
        return ["nsenter", f"--net=/run/netns/{self.ns}", *argv]

    def cleanup(self) -> None:
        pass


class LocalLauncher:
    remote = False

    def argv(self, args: list[str], env: dict, wrapper: list[str] | None = None) -> tuple[list[str], dict]:
        return [*(wrapper or []), sys.executable, "-u", *args], env

    def cmd(self, argv: list[str]) -> list[str]:
        return list(argv)

    def cleanup(self) -> None:
        pass


class SshLauncher:
    """A node on another host. Paths under this tree are rewritten to the
    peer's copy; ROCE_* settings are passed through (the peer's own
    roce_env.sh, from its preflight, is sourced first and then overridden by
    nothing but what this side explicitly sets); every process records its
    pid so cleanup() can stop stragglers if ssh dies first."""
    remote = True

    def __init__(self, peer: str, remote_dir: str, remote_lib: str, tag: str | None = None):
        self.peer, self.dir, self.lib = peer, remote_dir, remote_lib
        self.tag = tag or uuid.uuid4().hex[:8]
        self.n = 0

    def ssh(self, cmd: str, **kw):
        return subprocess.run(["ssh", *SSH_OPTS, self.peer, cmd], **kw)

    def argv(self, args: list[str], env: dict, wrapper: list[str] | None = None) -> tuple[list[str], dict]:
        self.n += 1
        rargs = [a.replace(ROOT, self.dir) for a in args]
        passthru = {k: v for k, v in env.items()
                    if k.startswith("ROCE_") and k not in ("ROCE_TRANSPORT_LIB",)}
        envs = " ".join(f"{k}={shlex.quote(v)}" for k, v in passthru.items())
        wrap = " ".join(shlex.quote(w) for w in (wrapper or []))
        cmd = (f"cd {shlex.quote(self.dir)} && {{ [ -f roce_env.sh ] && . ./roce_env.sh; true; }} && "
               f"echo $$ > /tmp/rocepool_{self.tag}_{self.n}.pid && "
               f"exec env {envs} ROCE_TRANSPORT_LIB={shlex.quote(self.lib)} PYTHONPATH={shlex.quote(self.dir)} "
               f"{wrap} python3 -u {' '.join(shlex.quote(a) for a in rargs)}")
        return ["ssh", *SSH_OPTS, self.peer, cmd], env

    def cmd(self, argv: list[str]) -> list[str]:
        return ["ssh", *SSH_OPTS, self.peer, " ".join(shlex.quote(a) for a in argv)]

    def cleanup(self) -> None:
        self.ssh(f"for f in /tmp/rocepool_{self.tag}_*.pid; do [ -f \"$f\" ] && kill $(cat \"$f\") 2>/dev/null; "
                 f"rm -f \"$f\"; done", capture_output=True, timeout=30)


class Topology:
    def __init__(self, *, real: bool, ip: dict, launch: dict, lib: str, env: dict | None = None,
                 port_offset: dict | None = None, describe: str = "", fuse_client: str = ""):
        self.real = real
        self.fuse_client = fuse_client  # native C FUSE client binary on node A
        self.intercept_lib = os.path.join(os.path.dirname(fuse_client) if fuse_client else os.path.join(ROOT, "roce", "native"),
                                          "librocepool_intercept.so")
        self.ip = ip
        self.launch = launch
        self.lib = lib
        self.extra_env = env or {}
        self.port_offset = port_offset or {"A": 0, "B": 0}
        self.describe = describe

    @classmethod
    def netns(cls) -> "Topology":
        return cls(real=False, ip={"A": "10.99.0.1", "B": "10.99.0.2"},
                   launch={"A": NetnsLauncher("rocepool_nodeA"), "B": NetnsLauncher("rocepool_nodeB")},
                   lib="", describe="crossover rig: 2 network namespaces over one veth, softverbs")

    @classmethod
    def from_file(cls, path: str) -> "Topology":
        with open(path) as f:
            cfg = json.load(f)
        a, b = cfg["A"], cfg["B"]
        if b.get("peer"):
            lb = SshLauncher(b["peer"], b["dir"], os.path.join(b["dir"], b.get("lib", "roce/native/libroce_transport.so")))
            off = {"A": 0, "B": 0}
            desc = f"real RoCE: {a['ip']} (this host) <-> {b['ip']} ({b['peer']})"
        else:
            lb = LocalLauncher()
            off = {"A": 0, "B": 50 if b["ip"] == a["ip"] else 0}
            desc = f"real RoCE: single host {a['ip']} (NIC loopback)"
        return cls(real=True, ip={"A": a["ip"], "B": b["ip"]}, launch={"A": LocalLauncher(), "B": lb},
                   lib=cfg["lib"], env=cfg.get("env"), port_offset=off, describe=desc,
                   fuse_client=cfg.get("fuse_client", os.path.join(ROOT, "roce", "native", "rocepool-fuse")))

    @classmethod
    def current(cls) -> "Topology":
        path = os.environ.get("ROCEPOOL_TOPOLOGY")
        return cls.from_file(path) if path else cls.netns()

    def ep(self, node: str, port: int) -> str:
        return f"{self.ip[node]}:{port + self.port_offset[node]}"

    def up(self) -> None:
        if not self.real:
            from tools import netlink_veth
            netlink_veth.up()
            from roce.native.build_lib import ensure_fuse_client, ensure_softverbs_lib
            self.lib = ensure_softverbs_lib()  # rebuilds if any source is newer than the .so
            self.fuse_client = ensure_fuse_client()

    def down(self, quiet: bool = False) -> None:
        for launcher in self.launch.values():
            launcher.cleanup()
        if not self.real:
            from tools import netlink_veth
            netlink_veth.down(quiet=quiet)

    def env(self, extra: dict | None = None) -> dict:
        return dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, **self.extra_env, **(extra or {}))

    def popen(self, node: str, args: list[str], env: dict, *, wrapper: list[str] | None = None, **kw):
        argv, env = self.launch[node].argv(args, env, wrapper)
        return subprocess.Popen(argv, env=env, cwd=ROOT, **kw)

    def popen_cmd(self, node: str, argv: list[str], env: dict, **kw):
        """A non-Python program (e.g. the native FUSE client) on `node`."""
        return subprocess.Popen(self.launch[node].cmd(argv), env=env, cwd=ROOT, **kw)

    def is_remote(self, node: str) -> bool:
        return self.launch[node].remote
