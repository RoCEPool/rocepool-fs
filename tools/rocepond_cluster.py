"""rocepond's schedulers on a rocepool cluster (tools/cluster.py: simulated
namespaces or real hosts): the process executor's workers, or a Ray cluster,
started on the nodes -- so that the tasks run where rocepool is mounted."""
from __future__ import annotations

import json
import os
import secrets
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import rocepond  # noqa: E402


def process_executor(cluster, env: dict, slots: int, threads: int, duckdb: str, **kw) -> "rocepond.ProcessExecutor":
    """`slots` worker processes on every node."""
    def launch(node, argv):
        return cluster.popen(node, argv, env, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    # simulated nodes are one host: its memory is shared by all of their workers
    kw.setdefault("workers_per_host", slots if cluster.real else slots * len(cluster.nodes))
    return rocepond.ProcessExecutor(launch, [nd.index for nd in cluster.nodes], duckdb, threads,
                                    slots_per_node=slots, **kw)


class RayOnCluster:
    """A Ray cluster on the nodes: the head on node 0, a worker node on each
    other; `cpus` per node. The driver runs on node 0 (a Ray driver needs a
    Ray node where it runs)."""
    PORT = 6399

    def __init__(self, cluster, env: dict, cpus: int):
        self.cluster, self.cpus = cluster, cpus
        # one authentication token for the head, the worker nodes and the driver
        # (current Ray refuses a node whose token differs from the head's)
        self.env = dict(env, RAY_DISABLE_USAGE_STATS="1", RAY_AUTH_MODE="token", RAY_AUTH_TOKEN=secrets.token_hex(32))
        force = [v for v in env.get("ROCEPOOL_FORCE_ENV", "").split(",") if v]
        self.env["ROCEPOOL_FORCE_ENV"] = ",".join(force + ["RAY_DISABLE_USAGE_STATS", "RAY_AUTH_MODE", "RAY_AUTH_TOKEN"])
        self.address = f"{cluster.nodes[0].ip}:{self.PORT}"
        self.started = False

    def _ray(self, i: int, args: list[str], check: bool = True):
        r = self.cluster.run(i, [sys.executable, "-m", "ray.scripts.scripts"] + args, self.env,
                             capture_output=True, text=True)
        if check and r.returncode:
            raise RuntimeError(f"node {i}: ray {' '.join(args[:2])} failed: {(r.stdout + r.stderr)[-2000:]}")
        return r

    def start(self) -> None:
        self.started = True
        for i, node in enumerate(self.cluster.nodes):
            if not self.cluster.real:
                # simulated nodes are one host and would share /tmp/ray (the raylets'
                # sockets): each node's mount namespace gets its own
                own = f"/var/tmp/rocepond_ray/n{i}"
                shutil.rmtree(own, ignore_errors=True)
                os.makedirs(own)
                os.makedirs("/tmp/ray", exist_ok=True)
                r = self.cluster.run(i, ["mount", "--bind", own, "/tmp/ray"], self.env, net=False,
                                     capture_output=True, text=True)
                if r.returncode:
                    raise RuntimeError(f"node {i}: private /tmp/ray: {r.stderr[-500:]}")
            common = ["--node-ip-address", node.ip, "--num-cpus", str(self.cpus), "--disable-usage-stats",
                      "--object-store-memory", str(256 << 20)]
            if i == 0:
                self._ray(i, ["start", "--head", "--port", str(self.PORT), "--include-dashboard=false"] + common)
            else:
                self._ray(i, ["start", "--address", self.address] + common)

    def run_driver(self, argv: list[str], timeout: float | None = None) -> dict:
        """`python3 <argv> --address <head> --json <file>` on node 0; the JSON it wrote."""
        with tempfile.NamedTemporaryFile(suffix=".json", dir="/var/tmp") as f:
            r = self.cluster.run(0, [sys.executable] + argv + ["--address", self.address, "--json", f.name],
                                 self.env, capture_output=True, text=True, timeout=timeout)
            text = open(f.name).read()
        if not text.strip():
            raise RuntimeError(f"the Ray driver failed ({r.returncode}): {(r.stdout + r.stderr)[-3000:]}")
        return json.loads(text)

    def stop(self) -> None:
        if self.started:
            for i in reversed(range(len(self.cluster.nodes))):
                self._ray(i, ["stop", "--force"], check=False)
                if not self.cluster.real:
                    self.cluster.run(i, ["umount", "-l", "/tmp/ray"], self.env, net=False, capture_output=True)
