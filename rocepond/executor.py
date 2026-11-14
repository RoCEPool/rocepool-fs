"""Where rocepond's tasks run: one worker (rocepond/worker.py, a long-lived
DuckDB) per node, tasks sent to them in parallel.

`launch(node, argv)` starts a process on a node and returns a Popen-like
object with text pipes -- locally subprocess.Popen, on a rocepool cluster
tools/cluster.py's Cluster.popen (a network/mount namespace when simulated,
ssh on real hosts).

This is the one layer to replace to schedule with Ray: a worker becomes a
Ray actor pinned to its node, run_all() becomes one .remote() call per actor
and ray.get() on the results. The session (rocepond/__init__.py) only uses
run() and run_all().
"""
from __future__ import annotations

import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.py")


class TaskError(RuntimeError):
    pass


class _Worker:
    def __init__(self, name: str, node: int, proc):
        self.name, self.node, self.proc = name, node, proc
        self.lock = threading.Lock()
        self.next_id = 0
        line = proc.stdout.readline()
        if not line:
            raise TaskError(f"worker {name} did not start")
        self.info = json.loads(line)

    def run(self, task: dict) -> dict:
        with self.lock:
            self.next_id += 1
            self.proc.stdin.write(json.dumps(dict(task, id=self.next_id)) + "\n")
            self.proc.stdin.flush()
            line = self.proc.stdout.readline()
        if not line:
            raise TaskError(f"worker {self.name} exited")
        res = json.loads(line)
        if not res["ok"]:
            raise TaskError(f"worker {self.name}: {res['error']}\n-- task: {json.dumps(task)[:1500]}")
        return res


class ProcessExecutor:
    """One worker process per node (plus any extra named workers)."""

    def __init__(self, launch, nodes: list[int], duckdb: str, threads: int, memory_limit: str = ""):
        self.launch, self.duckdb, self.memory_limit = launch, duckdb, memory_limit
        self.workers: dict[str, _Worker] = {}
        self.partition_workers: list[str] = []
        for k, node in enumerate(nodes):
            self.add_worker(f"w{k}", node, threads)
            self.partition_workers.append(f"w{k}")
        self.pool = ThreadPoolExecutor(max_workers=max(4, 2 * len(nodes)))

    def add_worker(self, name: str, node: int, threads: int) -> None:
        argv = [sys.executable, WORKER, "--duckdb", self.duckdb, "--threads", str(threads)]
        if self.memory_limit:
            argv += ["--memory-limit", self.memory_limit]
        self.workers[name] = _Worker(name, node, self.launch(node, argv))

    def run(self, worker: str, task: dict) -> dict:
        return self.workers[worker].run(task)

    def run_all(self, tasks: dict[str, dict]) -> dict[str, dict]:
        """Tasks on several workers at once; returns when all are done."""
        futs = {w: self.pool.submit(self.workers[w].run, t) for w, t in tasks.items()}
        return {w: f.result() for w, f in futs.items()}

    def close(self) -> None:
        for w in self.workers.values():
            try:
                w.proc.stdin.close()
                w.proc.wait(timeout=30)
            except Exception:  # noqa: BLE001
                w.proc.kill()
        self.pool.shutdown(wait=False)
