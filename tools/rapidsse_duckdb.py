"""DuckDB on the same TPC-H data and the same queries as RapidsSE's plan, for
tools/rapidsse_tpch.py's comparison. Runs on one node, with the DuckDB CLI:

    python3 tools/rapidsse_duckdb.py --duckdb /path/duckdb --threads 64 \\
        --tables-from <dir with NODE*/disk*/data*/<table>.tbl> --queries q.json --out duckdb.json

- schema: the TPC-H tables with the specification's column types
- load: every <table>.tbl of every node (nation/region: one copy, they are
  replicated per node) with read_csv, into an in-memory database
- queries: the plan's own SQL text, each as many times as the plan runs it;
  the time is DuckDB's own (`.timer on`, wall clock); every answer kept for
  the comparison with RapidsSE's

Only one adjustment to the SQL: `interval 'n' day (3)` (the interval
precision of TPC-H Q1) becomes `interval 'n' day`, which DuckDB's parser
needs; it means the same.
"""
from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import os
import re
import subprocess
import sys
import time

SCHEMA = {
    "nation": [("n_nationkey", "INTEGER"), ("n_name", "VARCHAR"), ("n_regionkey", "INTEGER"),
               ("n_comment", "VARCHAR")],
    "region": [("r_regionkey", "INTEGER"), ("r_name", "VARCHAR"), ("r_comment", "VARCHAR")],
    "part": [("p_partkey", "INTEGER"), ("p_name", "VARCHAR"), ("p_mfgr", "VARCHAR"), ("p_brand", "VARCHAR"),
             ("p_type", "VARCHAR"), ("p_size", "INTEGER"), ("p_container", "VARCHAR"),
             ("p_retailprice", "DECIMAL(15,2)"), ("p_comment", "VARCHAR")],
    "supplier": [("s_suppkey", "INTEGER"), ("s_name", "VARCHAR"), ("s_address", "VARCHAR"),
                 ("s_nationkey", "INTEGER"), ("s_phone", "VARCHAR"), ("s_acctbal", "DECIMAL(15,2)"),
                 ("s_comment", "VARCHAR")],
    "partsupp": [("ps_partkey", "INTEGER"), ("ps_suppkey", "INTEGER"), ("ps_availqty", "INTEGER"),
                 ("ps_supplycost", "DECIMAL(15,2)"), ("ps_comment", "VARCHAR")],
    "customer": [("c_custkey", "INTEGER"), ("c_name", "VARCHAR"), ("c_address", "VARCHAR"),
                 ("c_nationkey", "INTEGER"), ("c_phone", "VARCHAR"), ("c_acctbal", "DECIMAL(15,2)"),
                 ("c_mktsegment", "VARCHAR"), ("c_comment", "VARCHAR")],
    "orders": [("o_orderkey", "BIGINT"), ("o_custkey", "INTEGER"), ("o_orderstatus", "VARCHAR"),
               ("o_totalprice", "DECIMAL(15,2)"), ("o_orderdate", "DATE"), ("o_orderpriority", "VARCHAR"),
               ("o_clerk", "VARCHAR"), ("o_shippriority", "INTEGER"), ("o_comment", "VARCHAR")],
    "lineitem": [("l_orderkey", "BIGINT"), ("l_partkey", "INTEGER"), ("l_suppkey", "INTEGER"),
                 ("l_linenumber", "INTEGER"), ("l_quantity", "DECIMAL(15,2)"),
                 ("l_extendedprice", "DECIMAL(15,2)"), ("l_discount", "DECIMAL(15,2)"),
                 ("l_tax", "DECIMAL(15,2)"), ("l_returnflag", "VARCHAR"), ("l_linestatus", "VARCHAR"),
                 ("l_shipdate", "DATE"), ("l_commitdate", "DATE"), ("l_receiptdate", "DATE"),
                 ("l_shipinstruct", "VARCHAR"), ("l_shipmode", "VARCHAR"), ("l_comment", "VARCHAR")],
}
REPLICATED = ("nation", "region")
SENTINEL = "@@rocepool-step-end@@"


def adapt(sql: str) -> str:
    return re.sub(r"(interval\s+'[^']*'\s+day)\s*\(\s*\d+\s*\)", r"\1", sql, flags=re.I)


class Cli:
    """One DuckDB CLI process (in-memory database); statements one at a
    time, each followed by a sentinel select so its output can be cut out."""

    def __init__(self, exe: str, threads: int):
        self.p = subprocess.Popen([exe, "-csv", "-noheader"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.run(".timer on")
        self.run(f"SET threads = {threads};")
        self.run("SET preserve_insertion_order = false;")

    def run(self, sql: str) -> tuple[float | None, list[list[str]], str]:
        """(seconds as DuckDB timed it, result rows, error text)"""
        self.p.stdin.write(sql.strip() + ("" if sql.strip().startswith(".") else "\n") + "\n")
        self.p.stdin.write(f"SELECT '{SENTINEL}';\n")
        self.p.stdin.flush()
        lines = []
        while True:
            line = self.p.stdout.readline()
            if not line:
                raise RuntimeError("duckdb exited: " + "".join(lines)[-2000:])
            if line.strip().strip('"') == SENTINEL:
                self.p.stdout.readline()  # the sentinel's own Run Time line
                break
            lines.append(line)
        secs, rows, err = None, [], []
        for line in lines:
            m = re.match(r"Run Time \(s\): real ([0-9.]+)", line)
            if m:
                secs = float(m.group(1)) if secs is None else secs
            elif re.match(r"(\w+ )?Error:", line) or err:
                err.append(line.rstrip())
            else:
                rows.extend(csv.reader(io.StringIO(line)))
        return secs, rows, "\n".join(err)

    def close(self) -> None:
        try:
            self.p.stdin.write(".quit\n")
            self.p.stdin.close()
            self.p.wait(timeout=60)
        except Exception:  # noqa: BLE001
            self.p.kill()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True, help="the DuckDB CLI")
    ap.add_argument("--threads", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--tables-from", required=True, help="directory holding NODE*/disk*/data*/<table>.tbl")
    ap.add_argument("--queries", required=True, help="JSON: [{name, sql, loops}]")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    out = {"duckdb": "", "threads": a.threads, "load_s": {}, "queries": {}, "errors": []}
    ver = subprocess.run([a.duckdb, "-version"], capture_output=True, text=True)
    out["duckdb"] = (ver.stdout or ver.stderr).strip()
    cli = Cli(a.duckdb, a.threads)
    try:
        t_all = time.monotonic()
        for table, cols in SCHEMA.items():
            cli.run(f"CREATE TABLE {table} ({', '.join(f'{c} {t} NOT NULL' for c, t in cols)});")
            files = sorted(glob.glob(os.path.join(a.tables_from, "NODE*", "disk*", "data*", f"{table}.tbl")))
            if table in REPLICATED:
                files = files[:1]
            if not files:
                raise RuntimeError(f"no {table}.tbl under {a.tables_from}")
            colspec = ", ".join(f"'{c}': '{t}'" for c, t in cols) + ", 'zz_trailing': 'VARCHAR'"
            flist = ", ".join(f"'{f}'" for f in files)
            secs, _, err = cli.run(f"INSERT INTO {table} SELECT * EXCLUDE (zz_trailing) FROM read_csv([{flist}], "
                                   f"delim='|', header=false, quote='', escape='', columns={{{colspec}}});")
            if err:
                raise RuntimeError(f"loading {table}: {err}")
            out["load_s"][table] = secs
        out["load_total_s"] = round(time.monotonic() - t_all, 2)
        for q in json.load(open(a.queries)):
            runs, rows, err = [], [], ""
            for _ in range(q.get("loops", 1)):
                secs, rows, err = cli.run(adapt(q["sql"]))
                if err:
                    break
                runs.append(secs)
            out["queries"][q["name"]] = {"runs_s": runs, "rows": rows, "error": err}
            if err:
                out["errors"].append(f"{q['name']}: {err}")
    finally:
        cli.close()
    with open(a.out, "w") as f:
        json.dump(out, f)
    return 0


if __name__ == "__main__":
    sys.exit(main())
