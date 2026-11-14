"""rocepool brought up on a cluster for a tool that needs it as a file system:
one MDT and one OST per node (the native C servers) and the native FUSE
client mounted at the same path on every node. Works on the simulated cluster
and on real hosts (tools/cluster.py).

    rp = RocepoolMount(cluster, env, workdir, "/mnt/rocepool_x"); rp.start() ... rp.stop()

(The same steps as in tools/rapidsse_tpch.py, which predates this helper.)
"""
from __future__ import annotations

import os
import shlex
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class RocepoolMount:
    def __init__(self, cluster, env: dict, workdir: str, mount: str, ost_tier: str = "ram",
                 mdt_port: int = 19901, ost_port: int = 19951):
        self.cluster, self.env, self.workdir, self.mount, self.ost_tier = cluster, env, workdir, mount, ost_tier
        self.mdt_port, self.ost_port = mdt_port, ost_port
        self.procs, self.fuse, self.tier_files = [], [], []
        self.mdts = self.osts = ""

    def start(self) -> None:
        c, env = self.cluster, self.env
        os.makedirs(self.workdir, exist_ok=True)
        helper = os.path.join(ROOT, "tools", "crossover_node.py")
        servers = []
        for node in c.nodes:
            servers.append({"kind": "mdt", "node": node.index, "addr": f"{node.ip}:{self.mdt_port}"})
            servers.append({"kind": "ost", "node": node.index, "addr": f"{node.ip}:{self.ost_port}"})
        for s in servers:
            s["env"] = dict(env)
            if s["kind"] == "ost" and self.ost_tier != "ram":
                spec = self.ost_tier.replace("{node}", str(s["node"]))
                s["env"].update(ROCEPOOL_OST_TIER=spec, ROCEPOOL_OST_RAM_CACHE_MIB="128")
                path = spec[5:].rsplit(":", 1)[0] if spec.startswith("file:") else ""
                if path and not path.startswith("/dev/"):
                    c.run(s["node"], ["sh", "-c", f"mkdir -p {shlex.quote(os.path.dirname(path))} && "
                                                  f"rm -f {shlex.quote(path)}"], env, net=False)
                    self.tier_files.append((s["node"], path))
            s["log"] = os.path.join(self.workdir, f"{s['kind']}{s['node']}.log")
            self.procs.append(c.popen(s["node"], [sys.executable, "-u", helper, f"{s['kind']}-native", s["addr"]],
                                      s["env"], stdin=subprocess.PIPE, stdout=open(s["log"], "w"),
                                      stderr=subprocess.STDOUT))
        deadline = time.monotonic() + 120
        for s, p in zip(servers, self.procs):
            while "READY" not in open(s["log"]).read():
                if p.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError(f"{s['kind']} on node {s['node']} did not start: "
                                       f"{open(s['log']).read()[-2000:]}")
                time.sleep(0.2)
        self.mdts = ",".join(s["addr"] for s in servers if s["kind"] == "mdt")
        self.osts = ",".join(s["addr"] for s in servers if s["kind"] == "ost")
        for node in c.nodes:
            c.run(node.index, ["mkdir", "-p", self.mount], env, net=False, check=True)
            self.fuse.append(c.popen(node.index, [c.fuse_client, "--mount", self.mount, "--mdts", self.mdts,
                                                  "--osts", self.osts, "--attr-ttl", "3"], env,
                                     stdin=subprocess.PIPE,
                                     stdout=open(os.path.join(self.workdir, f"client{node.index}.log"), "w"),
                                     stderr=subprocess.STDOUT))
        deadline = time.monotonic() + 60
        for node in c.nodes:
            while c.run(node.index, ["mountpoint", "-q", self.mount], env, net=False).returncode:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"rocepool client did not mount on node {node.index}")
                time.sleep(0.3)

    def describe(self) -> dict:
        return {"mdts": self.mdts, "osts": self.osts, "mount": self.mount, "client": "native FUSE (rocepool-fuse)",
                "ost_data": "RAM" if self.ost_tier == "ram" else f"tier {self.ost_tier} (128 MiB RAM cache per OST)"}

    def stop(self) -> None:
        c, env = self.cluster, self.env
        for node in c.nodes:
            c.run(node.index, ["umount", "-l", self.mount], env, net=False, capture_output=True)
        for p in self.fuse + self.procs:
            try:
                if p.stdin:
                    p.stdin.close()
                p.wait(timeout=15)
            except Exception:  # noqa: BLE001
                p.kill()
        for node, path in self.tier_files:
            c.run(node, ["rm", "-f", path], env, net=False)
