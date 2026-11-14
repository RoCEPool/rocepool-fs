"""rocepond's tasks scheduled by Ray: every task (rocepond/task.py) is one Ray
task, placed and queued by Ray over the cluster's CPUs; each Ray worker
process keeps one DuckDB (the `duckdb` Python package) between tasks.

    ex = RayExecutor()                              # a local Ray, all cores of this host
    ex = RayExecutor(address="auto")                # the running Ray cluster
    ex = RayExecutor(address="auto", threads=4,     # 4 CPUs and 4 DuckDB threads per task
                     memory=8 << 30,                # Ray's memory reservation per task
                     resources={"nvme": 1},         # custom Ray resources per task
                     scheduling_strategy="SPREAD",  # or a NodeAffinitySchedulingStrategy, ...
                     max_retries=3, task_timeout=3600)

Same interface and guarantees as ProcessExecutor (rocepond/executor.py).
Retries are done here, by resubmitting, not by Ray's own max_retries: a task
that reports an error, a worker that died and a task over its time limit are
all handled the same way, and every attempt's error is kept for the report.

The rocepond package is shipped to the Ray workers (runtime_env py_modules),
so the cluster's nodes need Ray and the `duckdb` package, nothing else.
"""
from __future__ import annotations

import os
import time

from .executor import Executor, TaskError, describe, summarize


def _run_task(task: dict, conf: dict) -> dict:
    """Body of the Ray task (runs in a Ray worker process)."""
    from rocepond.task import engine_for, execute
    return execute(task, engine_for(conf))


class RayExecutor(Executor):
    def __init__(self, address: str | None = None, threads: int = 1, duckdb: str = "", engine: str = "auto",
                 memory_limit: str = "", memory: int | None = None, resources: dict | None = None,
                 scheduling_strategy=None, max_retries: int = 3, task_timeout: float | None = None,
                 ship_package: bool = True, **ray_init):
        super().__init__()
        import ray
        self.ray = ray
        self.max_retries, self.task_timeout = max_retries, task_timeout
        # DuckDB's limit: as given; else Ray's reservation for the task; else the
        # worker's host's memory shared by its cores / threads (rocepond/task.py)
        if not memory_limit and memory:
            memory_limit = f"{memory >> 20}MiB"
        self.conf = {"engine": engine, "duckdb": duckdb, "threads": threads, "memory_limit": memory_limit}
        self._owns_ray = not ray.is_initialized()
        if self._owns_ray:
            kw = dict(ray_init)
            if address:
                kw["address"] = address
            if ship_package:
                env = dict(kw.get("runtime_env") or {})
                env.setdefault("py_modules", []).append(os.path.dirname(os.path.abspath(__file__)))
                kw["runtime_env"] = env
            kw.setdefault("include_dashboard", False)
            ray.init(**kw)
        opts = {"num_cpus": threads, "max_retries": 0}
        if memory:
            opts["memory"] = memory
        if resources:
            opts["resources"] = resources
        if scheduling_strategy is not None:
            opts["scheduling_strategy"] = scheduling_strategy
        self._remote = ray.remote(_run_task).options(**opts)
        cpus = ray.cluster_resources().get("CPU", 1)
        self.parallelism = max(1, int(cpus // max(1, threads)))
        self.info = {"scheduler": "rocepond.RayExecutor", "ray": ray.__version__,
                     "nodes": len([n for n in ray.nodes() if n.get("Alive")]), "cpus": cpus,
                     "threads_per_task": threads}

    def run_all(self, tasks: list[dict], stage: str = "") -> list[dict]:
        ray = self.ray
        n = len(tasks)
        results: list = [None] * n
        if n == 0:
            return results
        t0 = time.monotonic()
        pending = {}  # ObjectRef -> (index, attempts so far, errors, submitted at)

        def submit(i, attempts, errs):
            pending[self._remote.remote(dict(tasks[i], attempt=attempts), self.conf)] = (i, attempts, errs, time.monotonic())

        for i in range(n):
            submit(i, 0, [])
        retries, failure = 0, None
        while pending and failure is None:
            done, _ = ray.wait(list(pending), num_returns=min(len(pending), 256), timeout=1.0)
            finished = []
            for ref in done:
                i, attempts, errs, _ = pending.pop(ref)
                try:
                    res = ray.get(ref)
                except Exception as exc:  # noqa: BLE001  (the worker died, the task was cancelled, ...)
                    res = {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:500]}", "rows": [],
                           "duck_s": 0.0, "wall_s": 0.0}
                finished.append((i, attempts, errs, res))
            if self.task_timeout:  # a task over its time limit: cancel it, count it as a failed attempt
                now = time.monotonic()
                for ref, (i, attempts, errs, at) in list(pending.items()):
                    if now - at > self.task_timeout:
                        pending.pop(ref)
                        ray.cancel(ref, force=True)
                        finished.append((i, attempts, errs, {
                            "ok": False, "error": f"no answer in {self.task_timeout:.0f} s (time limit)",
                            "rows": [], "duck_s": 0.0, "wall_s": 0.0}))
            for i, attempts, errs, res in finished:
                if res["ok"]:
                    res.update(attempts=attempts + 1, slot=f"{res.get('host', '')}:{res.get('pid', '')}")
                    results[i] = res
                    continue
                errs = errs + [f"attempt {attempts + 1}: {res['error']}"]
                if attempts < self.max_retries:
                    retries += 1
                    submit(i, attempts + 1, errs)
                elif failure is None:
                    failure = TaskError(f"task failed {attempts + 1} times: {describe(tasks[i])}\n  "
                                        + "\n  ".join(errs))
        if failure is not None:
            for ref in pending:  # drop what has not finished
                try:
                    ray.cancel(ref, force=True)
                except Exception:  # noqa: BLE001
                    pass
        self.stats.append(summarize(stage, n, t0, results, retries, self.parallelism))
        if failure is not None:
            raise failure
        return results

    def close(self) -> None:
        if self._owns_ray:
            self.ray.shutdown()
