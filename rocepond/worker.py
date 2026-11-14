"""A rocepond process worker: one long-lived DuckDB, taking tasks
(rocepond/task.py) from its standard input, one JSON object per line, and
answering one JSON object per line. Started on a node by ProcessExecutor
(rocepond/executor.py), several per node."""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rocepond.task import engine_for, execute  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", default="")
    ap.add_argument("--engine", default="auto", choices=("auto", "module", "cli"))
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--memory-limit", default="", help="per DuckDB; default: this host's memory shared by --share")
    ap.add_argument("--share", type=int, default=1, help="DuckDB workers on this host")
    a = ap.parse_args()
    engine = engine_for({"engine": a.engine, "duckdb": a.duckdb, "threads": a.threads,
                         "memory_limit": a.memory_limit, "share": a.share})
    print(json.dumps({"ready": True, "duckdb": engine.version, "threads": a.threads, "pid": os.getpid()}),
          flush=True)
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        task = json.loads(line)
        res = execute(task, engine)
        res["id"] = task.get("id")
        print(json.dumps(res), flush=True)
    engine.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
