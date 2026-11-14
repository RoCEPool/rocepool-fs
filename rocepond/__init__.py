"""rocepond: DuckDB on every node, the data and everything exchanged between
nodes on a shared file system (rocepool) -- the design of DeepSeek's
smallpond (DuckDB + 3FS), as a small prototype.

    sp = rocepond.Session(executor, root="/mnt/rocepool/rocepond")
    lineitem = sp.read_csv("lineitem", files_by_worker, columns).repartition(hash_by="l_orderkey")
    orders   = sp.read_csv("orders",   files_by_worker, columns).repartition(hash_by="o_orderkey")
    part     = sp.read_csv("part", files_by_worker, columns).broadcast()

    partial = sp.partial_sql("select l_shipmode, count(*) as n from {0}, {1} "
                             "where o_orderkey = l_orderkey group by l_shipmode", orders, lineitem)
    rows = sp.sql("select l_shipmode, sum(n) from {0} group by l_shipmode order by 1", partial)

How it works:
  - a table is a set of Parquet files under `root` on the shared file system
  - repartition(hash_by=K): every worker reads its own share of the source
    and writes one Parquet file per partition into the shared file system
    (the shuffle goes through the file system, not through sockets); there
    is one partition per worker, and worker p then sees partition p of every
    repartitioned table under the table's name. Tables repartitioned on
    their join key are co-partitioned: a join on that key is local.
  - broadcast(): one copy, every worker reads all of it.
  - partial_sql(sql, *tables): `sql` runs on every worker over its
    partition; each result is a Parquet file in the shared file system.
  - sql(sql, *tables): `sql` runs on one worker (over broadcast tables and
    partial results) and its rows come back.
  - cache="memory": workers load their partition into DuckDB's memory once
    (queries then do not read the file system); the default "none" reads
    Parquet from the file system in every query.

As in smallpond, the user decides the partitioning and writes the per-partition
and the merging SQL; nothing rewrites a query automatically. One difference:
workers are long-lived (a DuckDB per node), not one process per task.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

from .executor import ProcessExecutor, TaskError  # noqa: F401


def _lit(path: str) -> str:
    return "'" + path.replace("'", "''") + "'"


@dataclass
class DataFrame:
    session: "Session"
    name: str
    kind: str  # source | partitioned | broadcast | partial
    columns: list[tuple[str, str]] = field(default_factory=list)
    files_by_worker: list[list[str]] = field(default_factory=list)  # source only
    trailing_delimiter: bool = True
    delim: str = "|"

    # ---- a source's rows, as a DuckDB table expression
    def _csv(self, files: list[str]) -> str:
        cols = ", ".join(f"'{c}': '{t}'" for c, t in self.columns)
        extra = ", 'zz_trailing': 'VARCHAR'" if self.trailing_delimiter else ""
        flist = ", ".join(_lit(f) for f in files)
        sel = "* EXCLUDE (zz_trailing)" if self.trailing_delimiter else "*"
        return (f"(SELECT {sel} FROM read_csv([{flist}], delim={_lit(self.delim)}, header=false, quote='', "
                f"escape='', columns={{{cols}{extra}}}))")

    def repartition(self, hash_by: str) -> "DataFrame":
        return self.session._repartition(self, hash_by)

    def broadcast(self) -> "DataFrame":
        return self.session._broadcast(self)


class Session:
    def __init__(self, executor: ProcessExecutor, root: str, cache: str = "none"):
        if cache not in ("none", "memory"):
            raise ValueError("cache: none or memory")
        self.ex, self.root, self.cache = executor, root.rstrip("/"), cache
        self.workers = list(executor.partition_workers)
        self.n = len(self.workers)
        self.tables: dict[str, DataFrame] = {}
        self.stats: list[dict] = []
        self._tmp = 0
        self._patterns: dict = {}
        self._registered: dict[str, str] = {}  # table name -> VIEW | TABLE, as the workers have it

    # ---- sources
    def read_csv(self, name: str, files_by_worker: list[list[str]], columns: list[tuple[str, str]],
                 delim: str = "|", trailing_delimiter: bool = True) -> DataFrame:
        """`files_by_worker[k]`: the files worker k reads (its share of the table)."""
        if len(files_by_worker) != self.n:
            raise ValueError(f"{name}: files for {len(files_by_worker)} workers, there are {self.n}")
        return DataFrame(self, name, "source", columns, files_by_worker, trailing_delimiter, delim)

    def _data(self, name: str) -> str:
        return f"{self.root}/data/{name}"

    def _stat(self, what: str, t0: float, results: dict) -> None:
        self.stats.append({"what": what, "wall_s": round(time.monotonic() - t0, 3),
                           "duck_s": {w: r["duck_s"] for w, r in results.items()}})

    def _register(self, name: str, pattern_for=None) -> None:
        """Make `name` visible on every worker (a view over its Parquet, or a
        table in memory with cache="memory")."""
        if pattern_for is None:
            pattern_for = self._patterns[name]
        self._patterns[name] = pattern_for
        how = "TABLE" if self.cache == "memory" else "VIEW"
        # DuckDB refuses DROP VIEW on a table and the reverse: drop what is there
        drop = [f"DROP {self._registered[name]} {name}"] if name in self._registered else []
        self._registered[name] = how
        tasks = {w: {"sql": drop + [f"CREATE {how} {name} AS SELECT * FROM read_parquet({_lit(pattern_for(p))})"]}
                 for p, w in enumerate(self.workers)}
        t0 = time.monotonic()
        self._stat(f"register {name} ({how.lower()})", t0, self.ex.run_all(tasks))

    def set_cache(self, cache: str) -> float:
        """Switch between reading Parquet from the file system in every query
        ("none") and each worker holding its partition in memory ("memory");
        returns the seconds it took."""
        if cache not in ("none", "memory"):
            raise ValueError("cache: none or memory")
        self.cache = cache
        t0 = time.monotonic()
        for name in self.tables:
            self._register(name)
        return time.monotonic() - t0

    def _repartition(self, src: DataFrame, hash_by: str) -> DataFrame:
        """The shuffle: worker k writes rows with hash_by % n == p to
        data/<name>/p=<p>/from-<k>.parquet, for every p."""
        if src.kind != "source":
            raise ValueError("repartition: a source table (read_csv)")
        base = self._data(src.name)
        tasks = {}
        for k, w in enumerate(self.workers):
            sql = [f"CREATE OR REPLACE TEMP TABLE _shuffle AS SELECT * FROM {src._csv(src.files_by_worker[k])}"]
            for p in range(self.n):
                sql.append(f"COPY (SELECT * FROM _shuffle WHERE {hash_by} % {self.n} = {p}) TO "
                           f"{_lit(f'{base}/p={p}/from-{k}.parquet')} (FORMAT parquet, USE_TMP_FILE false)")
            sql.append("DROP TABLE _shuffle")
            tasks[w] = {"mkdirs": [f"{base}/p={p}" for p in range(self.n)], "sql": sql}
        t0 = time.monotonic()
        self._stat(f"repartition {src.name} by {hash_by}", t0, self.ex.run_all(tasks))
        df = DataFrame(self, src.name, "partitioned", src.columns)
        self.tables[src.name] = df
        self._register(src.name, lambda p: f"{base}/p={p}/*.parquet")
        return df

    def _broadcast(self, src: DataFrame) -> DataFrame:
        """One copy in the shared file system (each worker converts its
        share), read whole by every worker."""
        if src.kind != "source":
            raise ValueError("broadcast: a source table (read_csv)")
        base = self._data(src.name)
        tasks = {w: {"mkdirs": [base], "sql": [
            f"COPY (SELECT * FROM {src._csv(src.files_by_worker[k])}) TO {_lit(f'{base}/from-{k}.parquet')} "
            f"(FORMAT parquet, USE_TMP_FILE false)"]}
            for k, w in enumerate(self.workers) if src.files_by_worker[k]}
        t0 = time.monotonic()
        self._stat(f"broadcast {src.name}", t0, self.ex.run_all(tasks))
        df = DataFrame(self, src.name, "broadcast", src.columns)
        self.tables[src.name] = df
        self._register(src.name, lambda p: f"{base}/*.parquet")
        return df

    # ---- queries
    def _ref(self, df: DataFrame, single: bool) -> str:
        if df.kind == "partial":
            return f"read_parquet({_lit(f'{self.root}/tmp/{df.name}/*.parquet')})"
        if df.kind == "partitioned" and single:
            raise ValueError(f"sql(): {df.name} is partitioned -- one worker holds one partition; use partial_sql()")
        if df.kind == "source":
            raise ValueError(f"{df.name}: repartition() or broadcast() it first")
        return df.name

    def partial_sql(self, sql: str, *dfs: DataFrame) -> DataFrame:
        """`sql` on every worker, over its partition of the partitioned
        tables ({0}, {1}, ... are the tables given; broadcast tables can also
        be named directly). The results: one Parquet file per worker."""
        self._tmp += 1
        name = f"t{self._tmp}"
        out = f"{self.root}/tmp/{name}"
        q = sql.strip().rstrip(";").format(*[self._ref(d, single=False) for d in dfs])
        tasks = {w: {"mkdirs": [out], "sql": [
            f"COPY ({q}) TO {_lit(f'{out}/part-{p}.parquet')} (FORMAT parquet, USE_TMP_FILE false)"]}
            for p, w in enumerate(self.workers)}
        t0 = time.monotonic()
        self._stat(f"partial_sql -> {name}", t0, self.ex.run_all(tasks))
        return DataFrame(self, name, "partial")

    def sql(self, sql: str, *dfs: DataFrame, worker: str | None = None) -> list[list[str]]:
        """`sql` on one worker (broadcast tables, partial results); its rows."""
        q = sql.strip().rstrip(";").format(*[self._ref(d, single=True) for d in dfs])
        w = worker or self.workers[0]
        t0 = time.monotonic()
        res = self.ex.run(w, {"sql": [q], "fetch": True})
        self._stat("sql", t0, {w: res})
        return res["rows"]

    def full_views(self, in_memory: bool = False, replacing: str | None = None) -> list[str]:
        """Every table whole (all partitions), as views over the Parquet files
        or as tables in memory -- for a single-node DuckDB on the same data.
        `replacing`: "VIEW" or "TABLE" if that DuckDB already has them so."""
        out = []
        for name, df in self.tables.items():
            pat = f"{self._data(name)}/*/*.parquet" if df.kind == "partitioned" else f"{self._data(name)}/*.parquet"
            if replacing:
                out.append(f"DROP {replacing} {name}")
            out.append(f"CREATE {'TABLE' if in_memory else 'VIEW'} {name} AS SELECT * FROM read_parquet({_lit(pat)})")
        return out

    def drop_tmp(self) -> None:
        """Remove the partial results written so far (by a worker: the shared
        file system need not be mounted where the driver runs)."""
        self.ex.run(self.workers[0], {"rmtree": [f"{self.root}/tmp"]})
