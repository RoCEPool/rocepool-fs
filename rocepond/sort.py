"""A GraySort-style distributed sort on rocepond, in the shape of smallpond's
benchmarks/gray_sort_benchmark.py (the benchmark behind DeepSeek's published
110.5 TiB result): 100-byte records with a 10-byte key;

    1. generate   `splits` tasks, each writing its share of the records (Parquet)
    2. shuffle    one task per split: range-partition by key into P partitions,
                  through the shared file system (DuckDB's partitioned write,
                  as smallpond's ShuffleNode with the DuckDB engine)
    3. sort       one task per partition: sorted by key (DuckDB), written as
                  raw 100-byte records; partition i holds only keys below
                  partition i+1's, so the files in order are the sorted data
    4. validate   one task per partition and per split: every file in key
                  order, partition ranges in order, record count and a
                  checksum of all records equal to the input's

With P in the thousands this is thousands of tasks per stage (smallpond's
run: 8192 partitions).

Differences from smallpond's benchmark, stated: the records come from DuckDB
(pseudo-random hexadecimal text: a 10-character key and 89 more characters,
plus the newline = 100 bytes), not from gensort, and validation is this
module's, not valsort. The timed phases, 2 and 3, are the same work.
"""
from __future__ import annotations

import time

RECORD_BYTES = 100
KEY_BYTES = 10


def generate(sp, total_bytes: int, splits: int):
    """N = total_bytes / 100 records; split k makes records
    [k*N/splits, (k+1)*N/splits). A record: 99 hexadecimal characters (md5
    of the record number with four salts), its first 10 the key."""
    n = total_bytes // RECORD_BYTES
    return sp.generate("sort_input", f"""
        SELECT left(a, {KEY_BYTES}) AS keys, a || b || c || left(d, 3) AS records
        FROM (SELECT md5(i::VARCHAR || 'a') AS a, md5(i::VARCHAR || 'b') AS b, md5(i::VARCHAR || 'c') AS c,
                     md5(i::VARCHAR || 'd') AS d
              FROM range(({n}::BIGINT * {{split}}) // {{nsplits}},
                         ({n}::BIGINT * ({{split}} + 1)) // {{nsplits}}) t(i))""", splits,
                       compression="uncompressed"), n


def run(sp, total_bytes: int, partitions: int = 16, splits: int | None = None, max_fanout: int = 1024) -> dict:
    """The whole benchmark; returns the timings and the validation."""
    if not 1 <= partitions <= 1 << 20:
        raise ValueError("partitions: 1 .. 1048576")
    splits = splits or max(1, sp.ex.parallelism)
    out = {"records": 0, "bytes": 0, "partitions": partitions, "splits": splits}
    t0 = time.monotonic()
    records, n = generate(sp, total_bytes, splits)
    out["generate_s"] = round(time.monotonic() - t0, 2)
    out["records"], out["bytes"] = n, n * RECORD_BYTES

    # the key's first 8 hexadecimal digits, as a fraction of 2^32, times P: equal key ranges
    t0 = time.monotonic()
    buckets = records.repartition(partitions, by=f"(('0x' || left(keys, 8))::BIGINT * {partitions}) >> 32",
                                  name="sort_shuffled", compression="uncompressed", max_fanout=max_fanout)
    out["shuffle_s"] = round(time.monotonic() - t0, 2)

    t0 = time.monotonic()
    result = buckets.map_partitions("SELECT records FROM {0} ORDER BY keys", name="sort_output", fmt="lines")
    out["sort_s"] = round(time.monotonic() - t0, 2)
    out["total_s"] = round(out["shuffle_s"] + out["sort_s"], 2)
    out["gib_per_min"] = round(out["bytes"] / 2**30 / (out["total_s"] / 60), 3)
    out["tasks"] = {s["what"]: s["tasks"] for s in sp.stats}

    # 4. validate
    t0 = time.monotonic()
    outs = sp._run("sort: validate output", [{
        "sql": [f"SELECT count(*), coalesce(bit_xor(hash(line)), 0) FROM {p.sql}"], "fetch": True,
        "check_sorted": [result.partition_file(i)], "key_bytes": KEY_BYTES} for i, p in enumerate(result.parts)])
    ins = sp._run("sort: validate input", [{
        "sql": [f"SELECT count(*), coalesce(bit_xor(hash(records)), 0) FROM {p.sql}"], "fetch": True}
        for p in records.parts])

    def fold(res):
        cnt = x = 0
        for r in res:
            cnt += int(r["rows"][0][0])
            x ^= int(r["rows"][0][1])
        return cnt, x
    (in_n, in_x), (out_n, out_x) = fold(ins), fold(outs)
    files = [r["checks"][0] for r in outs]
    nonempty = [f for f in files if f[0]]
    out["validation"] = {
        "records_in": in_n, "records_out": out_n, "output_bytes": sum(f[1] for f in files),
        "order_violations_in_files": sum(f[4] for f in files),
        "partition_ranges_in_order": all(a[3] <= b[2] for a, b in zip(nonempty, nonempty[1:])),
        "checksum_equal": in_x == out_x, "empty_partitions": len(files) - len(nonempty),
        "largest_partition_records": max(f[0] for f in files), "seconds": round(time.monotonic() - t0, 2)}
    v = out["validation"]
    out["valid"] = (in_n == n == out_n and v["output_bytes"] == n * RECORD_BYTES
                    and v["order_violations_in_files"] == 0 and v["partition_ranges_in_order"]
                    and v["checksum_equal"])
    return out


def main(argv=None) -> int:
    """python3 -m rocepond.sort --root /mnt/rocepool/rocepond --gib 100 --partitions 4096 --executor ray

    Run where the tasks' scheduler is reachable: on a node of the Ray
    cluster (--executor ray --address auto), or on one host (--executor local)."""
    import argparse
    import json

    import rocepond
    ap = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", required=True, help="a directory on the shared file system")
    ap.add_argument("--gib", type=float, default=1.0)
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--splits", type=int, default=0, help="input files / shuffle tasks (default: one per worker)")
    ap.add_argument("--max-fanout", type=int, default=1024,
                    help="partitions one shuffle task writes; above it the shuffle takes two passes")
    ap.add_argument("--executor", default="ray", choices=("ray", "local"))
    ap.add_argument("--address", default="auto", help="Ray: the cluster (auto: the one running on this node)")
    ap.add_argument("--slots", type=int, default=0, help="local: worker processes (default: this host's cores)")
    ap.add_argument("--threads", type=int, default=1, help="DuckDB threads (and CPUs) per task")
    ap.add_argument("--duckdb", default="", help="DuckDB CLI, where the duckdb Python package is not installed")
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--no-ship-package", action="store_true",
                    help="Ray: rocepond is importable on every node already (PYTHONPATH)")
    ap.add_argument("--json", default="", help="write the result here")
    a = ap.parse_args(argv)
    if a.executor == "ray":
        ex = rocepond.RayExecutor(address=a.address, threads=a.threads, duckdb=a.duckdb,
                                  max_retries=a.max_retries, ship_package=not a.no_ship_package)
    else:
        import os
        ex = rocepond.local_executor(a.slots or os.cpu_count() or 1, duckdb=a.duckdb, threads=a.threads,
                                     max_retries=a.max_retries)
    try:
        res = run(rocepond.Session(ex, a.root), int(a.gib * 2**30), a.partitions, a.splits or None, a.max_fanout)
        res["executor"] = ex.info
        res["stages"] = ex.stats
    finally:
        ex.close()
    text = json.dumps(res, indent=1)
    if a.json:
        with open(a.json, "w") as f:
            f.write(text)
    print(text)
    return 0 if res["valid"] else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
