"""A rocepond worker: one long-lived DuckDB on one node, taking tasks from
its standard input, one JSON object per line, answering one per line:

    in : {"id": 7, "mkdirs": ["/mnt/x/tmp/q7"], "sql": ["CREATE ...", "COPY ..."], "fetch": false}
         (also "rmtree": [dirs], done first)
    out: {"id": 7, "ok": true, "duck_s": 0.41, "wall_s": 0.43, "rows": [], "error": ""}

`sql` runs in order; `rows` is the last statement's result when "fetch" is
set. Started by the executor on the node (rocepond/executor.py); with Ray
this is the body of an actor pinned to the node.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from rocepond.duck import Duck, DuckError  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True)
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--memory-limit", default="")
    a = ap.parse_args()
    duck = Duck(a.duckdb, a.threads, a.memory_limit or None)
    print(json.dumps({"ready": True, "duckdb": duck.version, "threads": a.threads}), flush=True)
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        task = json.loads(line)
        out = {"id": task.get("id"), "ok": True, "duck_s": 0.0, "rows": [], "error": ""}
        t0 = time.monotonic()
        try:
            for d in task.get("rmtree", []):
                shutil.rmtree(d, ignore_errors=True)
            for d in task.get("mkdirs", []):
                os.makedirs(d, exist_ok=True)
            for sql in task.get("sql", []):
                secs, rows = duck.run(sql)
                out["duck_s"] += secs
                out["rows"] = rows
            if not task.get("fetch"):
                out["rows"] = []
        except (DuckError, OSError) as exc:
            out.update(ok=False, error=f"{type(exc).__name__}: {exc}", rows=[])
        out["wall_s"] = round(time.monotonic() - t0, 4)
        out["duck_s"] = round(out["duck_s"], 4)
        print(json.dumps(out), flush=True)
    duck.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
