"""A GraySort-style distributed sort on rocepond, in the shape of smallpond's
benchmarks/gray_sort_benchmark.py (the benchmark behind DeepSeek's published
110.5 TiB result): 100-byte records with a 10-byte key;

    1. generate   every worker writes its share of the records (Parquet)
    2. shuffle    range-partition by key prefix into P partitions, through
                  the shared file system (DuckDB's partitioned write, as
                  smallpond's ShuffleNode with the DuckDB engine)
    3. sort       every partition sorted by key (DuckDB), written as raw
                  100-byte records; partition i holds only keys below
                  partition i+1's, so the files in order are the sorted data
    4. validate   every file in key order, partition ranges in order, record
                  count and a checksum of all records equal to the input's

Differences from smallpond's benchmark, stated: the records come from DuckDB
(pseudo-random hexadecimal text: a 10-character key and 89 more characters,
plus the newline = 100 bytes), not from gensort, and validation is this
module's, not valsort. The timed phases, 2 and 3, are the same work.
"""
from __future__ import annotations

import time

RECORD_BYTES = 100
KEY_BYTES = 10


def generate(sp, total_bytes: int):
    """N = total_bytes / 100 records; worker k makes records
    [k*N/n, (k+1)*N/n). A record: 99 hexadecimal characters (md5 of the
    record number with four salts), its first 10 the key."""
    n = total_bytes // RECORD_BYTES
    return sp.generate("sort_input", f"""
        SELECT left(a, {KEY_BYTES}) AS keys, a || b || c || left(d, 3) AS records
        FROM (SELECT md5(i::VARCHAR || 'a') AS a, md5(i::VARCHAR || 'b') AS b, md5(i::VARCHAR || 'c') AS c,
                     md5(i::VARCHAR || 'd') AS d
              FROM range(({n}::BIGINT * {{worker}}) // {{nworkers}},
                         ({n}::BIGINT * ({{worker}} + 1)) // {{nworkers}}) t(i))"""), n


def run(sp, total_bytes: int, partitions: int = 16) -> dict:
    """The whole benchmark; returns the timings and the validation."""
    digits = {16: 1, 256: 2, 4096: 3}.get(partitions)
    if not digits:
        raise ValueError("partitions: 16, 256 or 4096 (a whole number of leading hexadecimal key digits)")
    out = {"records": 0, "bytes": 0, "partitions": partitions}
    t0 = time.monotonic()
    records, n = generate(sp, total_bytes)
    out["generate_s"] = round(time.monotonic() - t0, 2)
    out["records"], out["bytes"] = n, n * RECORD_BYTES

    t0 = time.monotonic()
    buckets = records.repartition(partitions, by=f"('0x' || left(keys, {digits}))::INTEGER", name="sort_shuffled")
    out["shuffle_s"] = round(time.monotonic() - t0, 2)

    t0 = time.monotonic()
    result = buckets.map_partitions("SELECT records FROM {0} ORDER BY keys", name="sort_output", fmt="lines")
    out["sort_s"] = round(time.monotonic() - t0, 2)
    out["total_s"] = round(out["shuffle_s"] + out["sort_s"], 2)
    out["gib_per_min"] = round(out["bytes"] / 2**30 / (out["total_s"] / 60), 3)

    # 4. validate
    t0 = time.monotonic()
    checks = sp.ex.run_all({w: {"check_sorted": [sp.partition_files(result, i) for i in range(partitions)
                                                 if i % sp.n == k], "key_bytes": KEY_BYTES}
                            for k, w in enumerate(sp.workers)})
    per = {}
    for k, w in enumerate(sp.workers):
        for i, row in zip([i for i in range(partitions) if i % sp.n == k], checks[w]["rows"]):
            per[i] = row
    files = [per[i] for i in range(partitions)]
    in_sum = sp.ex.run_all({w: {"sql": [
        f"SELECT count(*), bit_xor(hash(records)) FROM read_parquet('{sp._data('sort_input')}/from-{k}.parquet')"],
        "fetch": True} for k, w in enumerate(sp.workers)})
    out_sum = sp.ex.run_all({w: {"sql": [
        "SELECT count(*), bit_xor(hash(r)) FROM read_csv([" + ", ".join(
            f"'{sp.partition_files(result, i)}'" for i in range(partitions) if i % sp.n == k)
        + "], header=false, quote='', escape='', delim='\\t', columns={'r': 'VARCHAR'})"], "fetch": True}
        for k, w in enumerate(sp.workers)})

    def fold(res):
        cnt = x = 0
        for r in res.values():
            cnt += int(r["rows"][0][0])
            x ^= int(r["rows"][0][1])
        return cnt, x
    (in_n, in_x), (out_n, out_x) = fold(in_sum), fold(out_sum)
    nonempty = [f for f in files if f[0]]
    out["validation"] = {
        "records_in": in_n, "records_out": out_n, "output_bytes": sum(f[1] for f in files),
        "order_violations_in_files": sum(f[4] for f in files),
        "partition_ranges_in_order": all(a[3] <= b[2] for a, b in zip(nonempty, nonempty[1:])),
        "checksum_equal": in_x == out_x,
        "largest_partition_records": max(f[0] for f in files), "seconds": round(time.monotonic() - t0, 2)}
    v = out["validation"]
    out["valid"] = (in_n == n == out_n and v["output_bytes"] == n * RECORD_BYTES
                    and v["order_violations_in_files"] == 0 and v["partition_ranges_in_order"]
                    and v["checksum_equal"])
    return out
