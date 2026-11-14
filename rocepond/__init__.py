"""rocepond: DuckDB on every node, the data and everything exchanged between
nodes on a shared file system (rocepool), tasks scheduled over the cluster --
the design of DeepSeek's smallpond (DuckDB + 3FS + Ray), in Python.

    import rocepond
    ex = rocepond.RayExecutor(address="auto")            # or rocepond.ProcessExecutor(...)
    sp = rocepond.Session(ex, root="/mnt/rocepool/rocepond")

    events = sp.read_parquet(["/mnt/rocepool/in/day=1/*.parquet", "/mnt/rocepool/in/day=2/*.parquet"])
    users  = sp.read_csv("users", [["/mnt/rocepool/in/users.csv"]], columns, delim=",").broadcast()

    by_user = events.repartition(1024, hash_by="user_id")           # the shuffle: 1024 partitions
    partial = sp.partition_sql("SELECT country, count(*) AS n FROM {0} JOIN {1} USING (user_id) "
                               "GROUP BY country", by_user, users)   # 1024 tasks
    rows = sp.sql("SELECT country, sum(n) FROM {0} GROUP BY country ORDER BY 2 DESC", partial)

The model:
  - a DataFrame is a list of partitions; a partition is data on the shared
    file system (Parquet files, or the files a read_csv names)
  - every operation is one task per partition, and a task names every file
    it reads and writes, so any worker on any node can run any task. The
    executor (rocepond/executor.py: worker processes; rocepond/ray_executor.py:
    Ray) queues the tasks, runs them as workers free up, and runs a failed
    task again. Thousands of partitions are thousands of tasks.
  - repartition(P, hash_by= / by=): the shuffle goes through the file system
    (DuckDB's partitioned write, as smallpond's): a task per input partition
    writes its rows into P directories; output partition i is directory i.
    DataFrames repartitioned to the same P on their join key are
    co-partitioned: partition_sql over them joins partition i with partition i.
  - broadcast(): the DataFrame is read whole by every task.
  - partition_sql(sql, *dfs): `sql` over every partition ({0}, {1}, ... are
    the DataFrames given, by position; df.as_table("orders") lets SQL say
    `orders` instead); each result is a Parquet file, the results a
    new DataFrame.
  - sql(sql, *dfs): `sql` in one task over whole DataFrames; its rows come
    back to the driver.

As in smallpond, the user decides the partitioning and writes the
per-partition and the merging SQL; nothing rewrites a query automatically.
Operations run when they are called (not lazily); sp.stats lists them.
"""
from __future__ import annotations

import re
import subprocess
import time
from dataclasses import dataclass, field

from .executor import Executor, ProcessExecutor, TaskError  # noqa: F401

PARQUET = "FORMAT parquet, USE_TMP_FILE false"  # rocepool has no rename: write in place


def __getattr__(name):  # RayExecutor without importing ray until it is asked for
    if name == "RayExecutor":
        from .ray_executor import RayExecutor
        return RayExecutor
    raise AttributeError(name)


def local_executor(slots: int = 2, **kw) -> ProcessExecutor:
    """Worker processes on this host (no cluster): for trying things out."""
    return ProcessExecutor(lambda node, argv: subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE),
                           [0], slots_per_node=slots, **kw)


def _lit(path: str) -> str:
    return "'" + path.replace("'", "''") + "'"


def _parquet(patterns: list[str]) -> str:
    return f"read_parquet([{', '.join(_lit(p) for p in patterns)}], hive_partitioning=false)"


@dataclass
class Partition:
    """Parquet files (paths or globs), or any DuckDB table expression."""
    files: list[str] = field(default_factory=list)
    expr: str = ""

    @property
    def sql(self) -> str:
        return _parquet(self.files) if self.files else self.expr


@dataclass
class DataFrame:
    session: "Session"
    name: str
    parts: list[Partition]
    is_broadcast: bool = False
    fmt: str = "parquet"  # "lines" for map_partitions(fmt="lines") results
    whole_files: list[str] = field(default_factory=list)  # a shorter way to name all partitions' files

    @property
    def npartitions(self) -> int:
        return len(self.parts)

    @property
    def whole(self) -> str:
        """All partitions as one table expression."""
        if self.whole_files:
            return _parquet(self.whole_files)
        if self.fmt == "parquet" and all(p.files for p in self.parts):
            return _parquet([f for p in self.parts for f in p.files])
        return "(" + " UNION ALL ".join(f"SELECT * FROM {p.sql}" for p in self.parts) + ")"

    def partition_file(self, i: int) -> str:
        """The file of partition i (results of map_partitions / partition_sql)."""
        if len(self.parts[i].files) != 1:
            raise ValueError(f"{self.name}: partition {i} is not one file")
        return self.parts[i].files[0]

    def repartition(self, npartitions: int, *, hash_by: str | None = None, by: str | None = None,
                    name: str | None = None, compression: str = "zstd", max_fanout: int = 1024) -> "DataFrame":
        """Into `npartitions` partitions: hash_by="column, ..." (rows with
        equal values land in the same partition), or by="expr", the
        partition's number 0..npartitions-1 (e.g. a key range before a sort).
        More than `max_fanout` partitions are written in two passes (see
        Session._repartition)."""
        return self.session._repartition(self, npartitions, hash_by, by, name, compression, max_fanout)

    def map_partitions(self, sql: str, name: str | None = None, fmt: str = "parquet") -> "DataFrame":
        """`sql` over every partition ({0}); result i is one file. fmt:
        "parquet", or "lines": the first column, one value per line."""
        return self.session.partition_sql(sql, self, name=name, fmt=fmt)

    def write_parquet(self, name: str) -> "DataFrame":
        """Every partition as one Parquet file under <root>/data/<name>/."""
        return self.session.partition_sql("SELECT * FROM {0}", self, name=name)

    def broadcast(self) -> "DataFrame":
        """Read whole by every task that uses it (the small side of a join).
        Data that is not Parquet yet is converted once."""
        df = self if self.fmt == "parquet" and all(p.files for p in self.parts) else self.write_parquet(self.name)
        return DataFrame(self.session, df.name, df.parts, True, df.fmt, df.whole_files)

    def as_table(self, table: str) -> "DataFrame":
        """Let SQL given to this session name this DataFrame as `table`
        (its partition i in partition_sql, all of it if broadcast or in sql())."""
        self.session._names[table] = self
        return self

    def count(self) -> int:
        res = self.session._run(f"count {self.name}", [
            {"sql": [f"SELECT count(*) FROM {p.sql}"], "fetch": True} for p in self.parts])
        return sum(int(r["rows"][0][0]) for r in res)

    def take(self, n: int) -> list[list[str]]:
        return self.session.sql(f"SELECT * FROM {{0}} LIMIT {int(n)}", self)


class Session:
    def __init__(self, executor: Executor, root: str):
        self.ex, self.root = executor, root.rstrip("/")
        self.stats: list[dict] = []
        self._auto = 0
        self._names: dict[str, DataFrame] = {}  # as_table(): DataFrames that SQL can name directly

    # ---- running a stage
    def _run(self, stage: str, tasks: list[dict]) -> list[dict]:
        for i, t in enumerate(tasks):
            t.setdefault("name", f"{stage} [{i}/{len(tasks)}]")
        t0 = time.monotonic()
        try:
            return self.ex.run_all(tasks, stage)
        finally:
            self.stats.append({"what": stage, "tasks": len(tasks), "wall_s": round(time.monotonic() - t0, 3)})

    def _data(self, name: str) -> str:
        return f"{self.root}/data/{name}"

    def _name(self, name: str | None, prefix: str) -> tuple[str, str]:
        """(name, directory): named results under data/, the others under tmp/."""
        if name:
            return name, self._data(name)
        self._auto += 1
        return f"{prefix}{self._auto}", f"{self.root}/tmp/{prefix}{self._auto}"

    # ---- sources
    def read_parquet(self, partitions: list, name: str = "") -> DataFrame:
        """`partitions`: one entry per partition, a path or glob or a list of them."""
        parts = [Partition(files=[p] if isinstance(p, str) else list(p)) for p in partitions]
        return DataFrame(self, name or "parquet", parts)

    def read_csv(self, name: str, partitions: list[list[str]], columns: list[tuple[str, str]],
                 delim: str = "|", trailing_delimiter: bool = False) -> DataFrame:
        """`partitions[i]`: the files of partition i (entries without files
        are dropped). trailing_delimiter: lines end with the delimiter (dbgen's .tbl)."""
        cols = ", ".join(f"'{c}': '{t}'" for c, t in columns)
        extra = ", 'zz_trailing': 'VARCHAR'" if trailing_delimiter else ""
        sel = "* EXCLUDE (zz_trailing)" if trailing_delimiter else "*"
        parts = [Partition(expr=(
            f"(SELECT {sel} FROM read_csv([{', '.join(_lit(f) for f in files)}], delim={_lit(delim)}, "
            f"header=false, quote='', escape='', columns={{{cols}{extra}}}))")) for files in partitions if files]
        return DataFrame(self, name, parts)

    def generate(self, name: str, sql: str, splits: int, compression: str = "zstd") -> DataFrame:
        """`sql` in `splits` tasks ({split} = the task's number, {nsplits}),
        each writing its rows as one Parquet file."""
        base = self._data(name)
        files = [f"{base}/part-{k:05d}.parquet" for k in range(splits)]
        self._run(f"generate {name}", [{"mkdirs": [base], "sql": [
            f"COPY ({sql.strip().rstrip(';').format(split=k, nsplits=splits)}) TO {_lit(files[k])} "
            f"({PARQUET}, COMPRESSION {compression})"]} for k in range(splits)])
        return DataFrame(self, name, [Partition(files=[f]) for f in files], whole_files=[f"{base}/part-*.parquet"])

    # ---- the shuffle
    def _repartition(self, df: DataFrame, n: int, hash_by, by, name, compression, max_fanout) -> DataFrame:
        """One task per input partition k: its rows, written hive-partitioned
        by the partition number (DuckDB's COPY ... PARTITION_BY, as
        smallpond's shuffle) into <dir>/bucket=<i>/from-<k>-*.parquet.

        A partitioned write holds buffers for every partition it writes
        (hundreds of MiB per task at thousands of partitions), so above
        `max_fanout` partitions the shuffle takes two passes: first into
        groups of max_fanout neighbouring partitions, then a task per group
        into its partitions. More I/O, bounded memory."""
        if n < 1 or bool(hash_by) == bool(by):
            raise ValueError("repartition(npartitions, hash_by='columns') or repartition(npartitions, by='expr')")
        expr = f"hash({hash_by}) % {n}" if hash_by else f"({by})"
        name_, base = self._name(name, "shuffle")
        copy = (f"(FORMAT parquet, PARTITION_BY ({{col}}), OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'from-{{k}}-{{{{i}}}}', "
                f"COMPRESSION {compression})")
        if n <= max_fanout:
            self._run(f"repartition {df.name} -> {name_} ({n} partitions)", [{
                "mkdirs": [base], "cleanup_globs": [f"{base}/bucket=*/from-{k}-*.parquet"],
                "sql": [f"COPY (SELECT *, ({expr})::INTEGER AS bucket FROM {p.sql}) TO {_lit(base)} "
                        + copy.format(col="bucket", k=k)]} for k, p in enumerate(df.parts)])
        else:
            tmp = f"{base}/_pass1"
            self._run(f"repartition {df.name} -> {name_} ({n} partitions), pass 1", [{
                "mkdirs": [tmp], "cleanup_globs": [f"{tmp}/rp_group=*/from-{k}-*.parquet"],
                "sql": [f"COPY (SELECT *, rp_bucket // {max_fanout} AS rp_group FROM (SELECT *, ({expr})::INTEGER AS "
                        f"rp_bucket FROM {p.sql})) TO {_lit(tmp)} " + copy.format(col="rp_group", k=k)]}
                for k, p in enumerate(df.parts)])
            groups = sorted(int(r[0]) for r in self._run(f"repartition {name_}: list pass 1", [{"sql": [
                "SELECT DISTINCT regexp_extract(file, 'rp_group=(-?[0-9]+)', 1) FROM "
                f"glob({_lit(tmp + '/rp_group=*/*.parquet')})"], "fetch": True}])[0]["rows"])
            self._run(f"repartition {df.name} -> {name_} ({n} partitions), pass 2", [{
                "cleanup_globs": [f"{base}/bucket=*/from-g{g}-*.parquet"],
                "sql": [f"COPY (SELECT * EXCLUDE (rp_bucket), rp_bucket AS bucket FROM "
                        f"read_parquet({_lit(f'{tmp}/rp_group={g}/*.parquet')}, hive_partitioning=false)) "
                        f"TO {_lit(base)} " + copy.format(col="bucket", k=f"g{g}")]} for g in groups])
            self._run(f"repartition {name_}: drop pass 1", [{"rmtree": [tmp]}])
        # which partitions got rows (the others have no directory); an empty
        # Parquet file with the columns stands for those
        empty = f"{base}/_empty.parquet"
        res = self._run(f"repartition {name_}: list", [{"sql": [
            f"COPY (SELECT * FROM {df.parts[0].sql} LIMIT 0) TO {_lit(empty)} ({PARQUET})",
            f"SELECT DISTINCT regexp_extract(file, 'bucket=(-?[0-9]+)', 1) FROM glob({_lit(base + '/bucket=*/*.parquet')})"],
            "fetch": True}])[0]
        have = {int(r[0]) for r in res["rows"]}
        if any(i < 0 or i >= n for i in have):
            raise ValueError(f"repartition: partition numbers outside 0..{n - 1}: {sorted(have)[:5]} ...")
        parts = [Partition(files=[f"{base}/bucket={i}/*.parquet" if i in have else empty]) for i in range(n)]
        return DataFrame(self, name_, parts, whole_files=[f"{base}/bucket=*/*.parquet"] if have else [empty])

    # ---- SQL over partitions
    def _views(self, sql: str, part_index: int | None) -> list[str]:
        """TEMP VIEWs for the named DataFrames that `sql` mentions (made in
        the task itself: workers keep no state between tasks)."""
        out = []
        for name, df in self._names.items():
            if re.search(rf"\b{re.escape(name)}\b", sql):
                whole = df.is_broadcast or part_index is None
                out.append(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM "
                           f"{df.whole if whole else df.parts[part_index].sql}")
        return out

    def partition_sql(self, sql: str, *dfs: DataFrame, name: str | None = None, fmt: str = "parquet") -> DataFrame:
        """`sql` once per partition: {0}, {1}, ... are partition i of the
        DataFrames given (all of the same number of partitions), or the whole
        of a broadcast one. Result i is one file; the results are a DataFrame."""
        if fmt not in ("parquet", "lines"):
            raise ValueError("fmt: parquet or lines")
        counts = {d.npartitions for d in dfs if not d.is_broadcast}
        counts |= {d.npartitions for nm, d in self._names.items()
                   if not d.is_broadcast and all(d is not x for x in dfs) and re.search(rf"\b{re.escape(nm)}\b", sql)}
        if len(counts) > 1:
            raise ValueError(f"partition_sql: DataFrames of different partition counts {sorted(counts)}: "
                             "repartition them to the same number, or broadcast() the small ones")
        n = counts.pop() if counts else 1
        name_, base = self._name(name, "t")
        ext = "parquet" if fmt == "parquet" else "dat"
        opts = PARQUET if fmt == "parquet" else \
            "FORMAT csv, HEADER false, QUOTE '', ESCAPE '', DELIMITER '\t', USE_TMP_FILE false"
        files = [f"{base}/part-{i:05d}.{ext}" for i in range(n)]
        q = sql.strip().rstrip(";")
        self._run(f"partition_sql -> {name_}", [{"mkdirs": [base], "sql": self._views(q, i) + [
            "COPY (" + q.format(*[d.whole if d.is_broadcast else d.parts[i].sql for d in dfs])
            + f") TO {_lit(files[i])} ({opts})"]} for i in range(n)])
        if fmt == "lines":
            parts = [Partition(files=[f], expr=f"read_csv({_lit(f)}, header=false, quote='', escape='', "
                                               "delim='\\t', columns={'line': 'VARCHAR'})") for f in files]
            return DataFrame(self, name_, [LinesPartition(p.files, p.expr) for p in parts], fmt="lines")
        return DataFrame(self, name_, [Partition(files=[f]) for f in files], whole_files=[f"{base}/part-*.parquet"])

    partial_sql = partition_sql

    def sql(self, sql: str, *dfs: DataFrame) -> list[list[str]]:
        """`sql` in one task over whole DataFrames ({0}, {1}, ...); its rows."""
        q = sql.strip().rstrip(";")
        res = self._run("sql", [{"sql": self._views(q, None) + [q.format(*[d.whole for d in dfs])], "fetch": True}])
        return res[0]["rows"]

    def drop(self, df: DataFrame) -> None:
        """Remove a DataFrame this session wrote (not one it only reads)."""
        for base in (self._data(df.name), f"{self.root}/tmp/{df.name}"):
            if any(f.startswith(base + "/") for p in df.parts for f in p.files):
                self._run(f"drop {df.name}", [{"rmtree": [base]}])
        for table in [t for t, d in self._names.items() if d is df]:
            del self._names[table]

    def drop_tmp(self) -> None:
        """Remove the unnamed results written so far (by a worker: the shared
        file system need not be mounted where the driver runs)."""
        self._run("drop tmp", [{"rmtree": [f"{self.root}/tmp"]}])


@dataclass
class LinesPartition(Partition):
    """A file of lines (map_partitions(fmt="lines")): read back as one VARCHAR column `line`."""

    @property
    def sql(self) -> str:
        return self.expr
