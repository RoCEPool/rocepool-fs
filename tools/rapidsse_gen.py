"""One node's share of the TPC-H data for tools/rapidsse_tpch.py, generated in
parallel -- the scheme of RapidsSE's bootstrap/generateData.sh: the dataset
is cut into nodes x paths x chunks-per-path dbgen chunks; a node generates
its own chunks and each of its paths gets <table>.tbl = its chunks
concatenated in order; nation and region whole in the node's first path.

    python3 tools/rapidsse_gen.py --dbgen D --sf 100 --total 512 --first 1 \\
        --chunks-per-path 64 --jobs 64 --scratch /nvmedata/x --paths P1 P2 P3 P4

dbgen writes <table>.tbl.<chunk> into its working directory; the chunks go
to local scratch (in parallel, `--jobs` at a time) and are then streamed, in
chunk order, into <table>.tbl in the path -- one sequential write per file,
no rename on the target (rocepool has none). Scratch holds one path's
chunks at a time. Prints one JSON line: seconds generating, seconds writing,
bytes written.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import shutil
import subprocess
import sys
import time

BIG = [("lineitem", "L"), ("orders", "O"), ("partsupp", "S"), ("part", "P"), ("customer", "c"), ("supplier", "s")]
SMALL = [("nation", "n"), ("region", "r")]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dbgen", required=True)
    ap.add_argument("--sf", required=True)
    ap.add_argument("--total", type=int, required=True, help="chunks of the whole dataset (all nodes, all paths)")
    ap.add_argument("--first", type=int, required=True, help="this node's first chunk number (1-based)")
    ap.add_argument("--chunks-per-path", type=int, default=1)
    ap.add_argument("--jobs", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--scratch", required=True, help="local directory for the chunks before they are written")
    ap.add_argument("--paths", nargs="+", required=True)
    a = ap.parse_args()
    a.dbgen = os.path.abspath(a.dbgen)  # dbgen runs in the chunk directories
    dists = os.path.join(os.path.dirname(a.dbgen), "dists.dss")
    gen_s = write_s = 0.0
    written = 0
    os.makedirs(a.scratch, exist_ok=True)
    for path in a.paths:  # before anything is generated: the small tables go straight into the first one
        os.makedirs(path, exist_ok=True)

    def dbgen(table_code: str, chunk: int | None, cwd: str) -> None:
        argv = [a.dbgen, "-b", dists, "-f", "-q", "-s", str(a.sf), "-T", table_code]
        if chunk is not None:
            argv[1:1] = ["-C", str(a.total), "-S", str(chunk)]
        r = subprocess.run(argv, cwd=cwd, capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(f"dbgen {' '.join(argv[1:])} failed ({r.returncode}): {r.stdout}{r.stderr}")

    t_start = time.monotonic()
    with cf.ThreadPoolExecutor(max_workers=max(1, a.jobs)) as pool:
        # every path's chunks at once (biggest first, so the pool stays full);
        # each path is written as soon as its own chunks are done
        per_path = []
        for j, _ in enumerate(a.paths):
            chunks = [a.first + j * a.chunks_per_path + k for k in range(a.chunks_per_path)]
            per_path.append((chunks, []))
        for name, code in BIG:
            for chunks, futs in per_path:
                for c in chunks:
                    d = os.path.join(a.scratch, f"{name}.{c}")
                    os.makedirs(d, exist_ok=True)
                    futs.append(pool.submit(dbgen, code, c, d))
        small = [pool.submit(dbgen, code, None, a.paths[0]) for _, code in SMALL]
        for path, (chunks, futs) in zip(a.paths, per_path):
            for f in futs:
                f.result()
            t1 = time.monotonic()
            os.makedirs(path, exist_ok=True)
            for name, _ in BIG:
                with open(os.path.join(path, f"{name}.tbl"), "wb", buffering=0) as out:
                    for c in chunks:
                        src = os.path.join(a.scratch, f"{name}.{c}", f"{name}.tbl.{c}")
                        if not os.path.exists(src):  # dbgen names an only chunk without the suffix
                            src = os.path.join(a.scratch, f"{name}.{c}", f"{name}.tbl")
                        with open(src, "rb") as f:
                            shutil.copyfileobj(f, out, 16 << 20)
                        written += os.path.getsize(src)
                        shutil.rmtree(os.path.join(a.scratch, f"{name}.{c}"))
            write_s += time.monotonic() - t1
        for f in small:
            f.result()
        for name, _ in SMALL:
            written += os.path.getsize(os.path.join(a.paths[0], f"{name}.tbl"))
    gen_s = time.monotonic() - t_start - write_s
    shutil.rmtree(a.scratch, ignore_errors=True)
    print(json.dumps({"generate_s": round(gen_s, 1), "write_s": round(write_s, 1), "bytes": written}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
