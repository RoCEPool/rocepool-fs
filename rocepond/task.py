"""A rocepond task and its execution -- the same code in a process worker
(rocepond/worker.py) and in a Ray worker (rocepond/ray_executor.py).

A task is a JSON-able dict; it carries everything it needs (the files it
reads and writes are paths on the shared file system), so any worker on any
node can run any task:

    {"sql": ["CREATE OR REPLACE TEMP VIEW ...", "COPY (...) TO '...'"],   statements, in order
     "fetch": true,                 return the last statement's rows
     "mkdirs": [...], "rmtree": [...],                                   before the SQL
     "cleanup_globs": [...],        files of an earlier, failed attempt of this task: removed
                                    before a retry ("attempt" > 0, set by the scheduler)
     "check_sorted": [files], "key_bytes": 10,                           see check_sorted()
     "name": "sort p17"}            for reports and error messages

The result: {"ok", "rows", "checks", "duck_s", "wall_s", "error", "host", "pid"}.

Test hooks (used by the scheduler's tests to make failures happen):
    "_fail_once": marker   fail unless the marker file exists (and create it)
    "_die_once": marker    kill this worker process unless the marker exists
    "_sleep": seconds
"""
from __future__ import annotations

import glob
import os
import shutil
import socket
import time

from .duck import DuckError, open_engine

_ENGINES: dict = {}  # one DuckDB per process and configuration, kept between tasks


def auto_memory_limit(share: int) -> str:
    """This host's memory, less a third for the file system and everything
    else, divided among the `share` DuckDBs running on it. Without a limit
    every DuckDB assumes 80% of the host's memory is its own."""
    try:
        with open("/proc/meminfo") as f:
            kib = int(next(line for line in f if line.startswith("MemTotal:")).split()[1])
    except (OSError, StopIteration, ValueError):
        return ""
    return f"{max(256, kib * 2 // 3 // max(1, share) // 1024)}MiB"


def engine_for(conf: dict):
    """conf: engine, duckdb (CLI path), threads, memory_limit ("" = auto,
    for `share` DuckDBs on this host; "none" = DuckDB's default)."""
    limit = conf.get("memory_limit") or ""
    if not limit:
        limit = auto_memory_limit(int(conf.get("share") or max(1, (os.cpu_count() or 1) // int(conf.get("threads", 1)))))
    elif limit == "none":
        limit = ""
    key = (conf.get("engine", "auto"), conf.get("duckdb", ""), int(conf.get("threads", 1)), limit)
    if key not in _ENGINES:
        _ENGINES[key] = open_engine(key[2], key[1], key[3] or None, key[0])
    return _ENGINES[key]


def check_sorted(path: str, key_bytes: int) -> list:
    """A file of newline-terminated records: [records, bytes, first key, last
    key, order violations] -- the keys (the first key_bytes of each record)
    must not decrease."""
    n = bad = size = 0
    first = prev = b""
    with open(path, "rb", buffering=8 << 20) as f:
        for line in f:
            key = line[:key_bytes]
            if n == 0:
                first = key
            elif key < prev:
                bad += 1
            prev = key
            n += 1
            size += len(line)
    return [n, size, first.decode("latin-1"), prev.decode("latin-1"), bad]


def _once(marker: str) -> bool:
    """True the first time (the marker did not exist; it does now)."""
    try:
        fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
        return True
    except FileExistsError:
        return False


def execute(task: dict, engine) -> dict:
    """Run one task on `engine`; never raises for a failure of the task
    itself (the result says so), so a worker survives a bad task."""
    out = {"ok": True, "duck_s": 0.0, "rows": [], "error": "", "host": socket.gethostname(), "pid": os.getpid()}
    t0 = time.monotonic()
    try:
        if task.get("_die_once") and _once(task["_die_once"]):
            os._exit(17)
        if task.get("_fail_once") and _once(task["_fail_once"]):
            raise OSError("injected failure (first attempt)")
        if task.get("_sleep"):
            time.sleep(float(task["_sleep"]))
        if task.get("attempt"):
            for pattern in task.get("cleanup_globs", []):
                for path in glob.glob(pattern):
                    os.unlink(path)
        for d in task.get("rmtree", []):
            shutil.rmtree(d, ignore_errors=True)
        for d in task.get("mkdirs", []):
            os.makedirs(d, exist_ok=True)
        for sql in task.get("sql", []):
            secs, rows = engine.run(sql)
            out["duck_s"] += secs
            out["rows"] = rows
        if not task.get("fetch"):
            out["rows"] = []
        if task.get("check_sorted"):
            out["checks"] = [check_sorted(path, task.get("key_bytes", 10)) for path in task["check_sorted"]]
    except (DuckError, OSError) as exc:
        out.update(ok=False, error=f"{type(exc).__name__}: {exc}", rows=[])
    out["wall_s"] = round(time.monotonic() - t0, 4)
    out["duck_s"] = round(out["duck_s"], 4)
    return out
