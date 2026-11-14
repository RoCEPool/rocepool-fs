"""Where rocepond's tasks run. One interface, two schedulers:

    ex.run_all(tasks, stage="sort")  -> results, in the tasks' order
    ex.run(task)                     -> one result
    ex.parallelism                   tasks that run at the same time
    ex.stats                         one summary per run_all (tasks, seconds, retries, ...)
    ex.close()

A task (rocepond/task.py) names every file it reads and writes on the shared
file system, so any worker on any node can run any task: the scheduler needs
no data locality, only free workers.

Both schedulers give the same guarantees:
  - thousands of tasks per call: they queue and run as workers free up
  - a failed task (an error, a worker that died, a task over its time limit)
    is run again, on any worker, up to `max_retries` times; then run_all
    raises TaskError with the task and every attempt's error
  - a task that fails every time fails the call quickly: tasks not yet
    started are dropped

ProcessExecutor (this file): `slots_per_node` worker processes on every node
(rocepond/worker.py, a long-lived DuckDB each), started through
`launch(node, argv)` -- subprocess.Popen locally, tools/cluster.py's
Cluster.popen on a rocepool cluster (a namespace when simulated, ssh on real
hosts). No dependencies.

RayExecutor (rocepond/ray_executor.py): every task is a Ray task; Ray places
and queues them over the cluster's CPUs.
"""
from __future__ import annotations

import json
import os
import queue
import select
import sys
import threading
import time

WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.py")


class TaskError(RuntimeError):
    pass


def describe(task: dict) -> str:
    return task.get("name") or (task.get("sql") or ["(no sql)"])[-1][:200]


def summarize(stage: str, n: int, t0: float, results: list, retries: int, parallelism: int) -> dict:
    wall = time.monotonic() - t0
    busy = sum(r["wall_s"] for r in results if r)
    return {"stage": stage, "tasks": n, "wall_s": round(wall, 3), "retries": retries,
            "tasks_per_s": round(n / wall, 1) if wall > 0 else None,
            "task_s_total": round(busy, 3), "duck_s_total": round(sum(r["duck_s"] for r in results if r), 3),
            # the part of the workers' time not spent inside tasks: scheduling, pipes, idle tails
            "utilization": round(busy / (wall * parallelism), 3) if wall > 0 and parallelism else None}


class Executor:
    parallelism = 1

    def __init__(self):
        self.stats: list[dict] = []

    def run_all(self, tasks: list[dict], stage: str = "") -> list[dict]:
        raise NotImplementedError

    def run(self, task: dict, stage: str = "") -> dict:
        return self.run_all([task], stage)[0]

    def close(self) -> None:
        pass


class _Slot:
    """One worker process; os-level pipe I/O so a read can time out."""

    def __init__(self, name: str, node, launch, argv: list[str], start_timeout: float):
        self.name, self.node, self.launch, self.argv, self.start_timeout = name, node, launch, argv, start_timeout
        self.proc = None
        self.buf = b""
        self.next_id = 0
        self.restarts = -1
        self.info: dict = {}
        self.start()

    def start(self) -> None:
        self.kill()
        self.proc = self.launch(self.node, self.argv)
        self.buf = b""
        self.restarts += 1
        line = self._readline(self.start_timeout)
        if line is None:
            self.kill()
            raise TaskError(f"worker {self.name} did not start within {self.start_timeout:.0f} s")
        self.info = json.loads(line)

    def kill(self) -> None:
        if self.proc is not None:
            try:
                self.proc.kill()
                self.proc.wait(timeout=10)
            except Exception:  # noqa: BLE001
                pass
            for f in (self.proc.stdin, self.proc.stdout):
                try:
                    f.close()
                except Exception:  # noqa: BLE001
                    pass
            self.proc = None

    def _readline(self, timeout: float | None) -> bytes | None:
        """One line from the worker; None if it exited or `timeout` passed."""
        fd = self.proc.stdout.fileno()
        deadline = None if timeout is None else time.monotonic() + timeout
        while b"\n" not in self.buf:
            wait = None if deadline is None else max(0.0, deadline - time.monotonic())
            if not select.select([fd], [], [], wait)[0]:
                return None
            chunk = os.read(fd, 1 << 20)
            if not chunk:
                return None
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return line

    def run(self, task: dict, timeout: float | None) -> dict:
        """The task's result; raises OSError if the worker died or the task
        ran over `timeout` (the caller restarts the worker)."""
        self.next_id += 1
        data = (json.dumps(dict(task, id=self.next_id)) + "\n").encode()
        fd = self.proc.stdin.fileno()
        while data:
            data = data[os.write(fd, data):]
        line = self._readline(timeout)
        if line is None:
            alive = self.proc.poll() is None
            raise OSError(f"no answer in {timeout:.0f} s (time limit)" if alive else
                          f"worker process exited ({self.proc.returncode})")
        return json.loads(line)

    def close(self) -> None:
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=20)
        except Exception:  # noqa: BLE001
            pass
        self.kill()


class ProcessExecutor(Executor):
    def __init__(self, launch, nodes: list, duckdb: str = "", threads: int = 1, slots_per_node: int = 1,
                 engine: str = "auto", memory_limit: str = "", workers_per_host: int | None = None,
                 max_retries: int = 3, task_timeout: float | None = None, start_timeout: float = 120):
        """memory_limit: per DuckDB, e.g. "4GiB"; "" = a host's memory (less a
        third) divided by workers_per_host (default: slots_per_node; more
        when several simulated nodes are one host); "none" = DuckDB's default."""
        super().__init__()
        self.max_retries, self.task_timeout = max_retries, task_timeout
        argv = [sys.executable, WORKER, "--engine", engine, "--threads", str(threads),
                "--share", str(workers_per_host or slots_per_node)]
        if duckdb:
            argv += ["--duckdb", duckdb]
        if memory_limit:
            argv += ["--memory-limit", memory_limit]
        specs = [(f"n{k}s{s}", node) for k, node in enumerate(nodes) for s in range(slots_per_node)]
        self.slots: list[_Slot] = [None] * len(specs)
        errors = []

        def start(i, name, node):
            try:
                self.slots[i] = _Slot(name, node, launch, argv, start_timeout)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{name}: {exc}")
        threads_ = [threading.Thread(target=start, args=(i, n, nd)) for i, (n, nd) in enumerate(specs)]
        for t in threads_:
            t.start()
        for t in threads_:
            t.join()
        if errors:
            self.close()
            raise TaskError("workers did not start: " + "; ".join(errors))
        self.parallelism = len(self.slots)
        self.info = {"scheduler": "rocepond.ProcessExecutor", "nodes": len(nodes), "slots_per_node": slots_per_node,
                     "threads_per_slot": threads, "duckdb": self.slots[0].info.get("duckdb", "")}

    def run_all(self, tasks: list[dict], stage: str = "") -> list[dict]:
        n = len(tasks)
        results: list = [None] * n
        if n == 0:
            return results
        todo: queue.Queue = queue.Queue()
        for i, t in enumerate(tasks):
            todo.put((i, t, 0, []))
        state = {"left": n, "retries": 0, "failure": None}
        lock = threading.Lock()
        t0 = time.monotonic()

        def stop_all() -> None:  # with the lock held: wake every worker thread
            for _ in self.slots:
                todo.put(None)

        def work(slot: _Slot) -> None:
            while True:
                item = todo.get()
                if item is None:
                    return
                with lock:
                    if state["failure"]:
                        continue
                i, task, attempts, errs = item
                queued = time.monotonic() - t0
                try:
                    res = slot.run(dict(task, attempt=attempts), self.task_timeout)
                except OSError as exc:  # the worker died or the task ran over its time limit
                    res = {"ok": False, "error": f"{type(exc).__name__}: {exc}", "rows": [], "duck_s": 0.0,
                           "wall_s": 0.0}
                    try:
                        slot.start()
                    except Exception as exc2:  # noqa: BLE001
                        with lock:
                            if not state["failure"]:
                                state["failure"] = TaskError(f"worker {slot.name} could not be restarted: {exc2}")
                                stop_all()
                        return
                if res["ok"]:
                    res.update(attempts=attempts + 1, slot=slot.name, queued_s=round(queued, 3))
                    with lock:
                        results[i] = res
                        state["left"] -= 1
                        if state["left"] == 0:
                            stop_all()
                    continue
                errs = errs + [f"attempt {attempts + 1} on {slot.name}: {res['error']}"]
                with lock:
                    if attempts < self.max_retries:
                        state["retries"] += 1
                        todo.put((i, task, attempts + 1, errs))
                    elif not state["failure"]:
                        state["failure"] = TaskError(f"task failed {attempts + 1} times: {describe(task)}\n  "
                                                     + "\n  ".join(errs))
                        stop_all()

        threads_ = [threading.Thread(target=work, args=(s,), daemon=True) for s in self.slots]
        for t in threads_:
            t.start()
        for t in threads_:
            t.join()
        self.stats.append(summarize(stage, n, t0, results, state["retries"], self.parallelism))
        if state["failure"]:
            raise state["failure"]
        return results

    def close(self) -> None:
        for s in self.slots:
            if s is not None:
                s.close()
