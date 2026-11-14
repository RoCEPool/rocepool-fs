"""One DuckDB, driven statement by statement, behind one interface:

    engine.run(sql) -> (seconds, rows)     rows: lists of strings ("" for NULL)

Two engines: DuckModule, the `duckdb` Python package (in process; what Ray
workers and any host with the package use), and Duck, the DuckDB CLI (a
single binary, for hosts where no package can be installed). open_engine()
picks the package when it can be imported, else the CLI."""
from __future__ import annotations

import csv
import io
import re
import subprocess
import time

SENTINEL = "@@rocepond-step-end@@"


class DuckError(RuntimeError):
    pass


class Duck:
    """An in-memory DuckDB. run(sql) -> (seconds as DuckDB timed it, rows)."""

    def __init__(self, exe: str, threads: int, memory_limit: str | None = None):
        self.p = subprocess.Popen([exe, "-csv", "-noheader"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True, bufsize=1)
        self._raw(".timer on")
        self.run(f"SET threads = {int(threads)};")
        if memory_limit:
            self.run(f"SET memory_limit = '{memory_limit}';")
        self.version = self.run("SELECT version();")[1][0][0]

    def _raw(self, text: str) -> list[str]:
        self.p.stdin.write(text.strip() + "\n")
        self.p.stdin.write(f"SELECT '{SENTINEL}';\n")
        self.p.stdin.flush()
        lines = []
        while True:
            line = self.p.stdout.readline()
            if not line:
                raise DuckError("duckdb exited: " + "".join(lines)[-2000:])
            if line.strip().strip('"') == SENTINEL:
                self.p.stdout.readline()  # the sentinel's own Run Time line
                return lines
            lines.append(line)

    def run(self, sql: str) -> tuple[float, list[list[str]]]:
        sql = sql.strip()
        if not sql.endswith(";"):
            sql += ";"
        secs, out, err = None, [], []
        for line in self._raw(sql):
            m = re.match(r"Run Time \(s\): real ([0-9.]+)", line)
            if m:
                secs = float(m.group(1)) if secs is None else secs
            elif err or re.match(r"((\w+ )+)?Error:", line):
                err.append(line.rstrip())
            else:
                out.append(line)
        if err:
            raise DuckError("\n".join(err)[:4000])
        return secs or 0.0, list(csv.reader(io.StringIO("".join(out))))

    def close(self) -> None:
        try:
            self.p.stdin.write(".quit\n")
            self.p.stdin.close()
            self.p.wait(timeout=30)
        except Exception:  # noqa: BLE001
            self.p.kill()


class DuckModule:
    """The `duckdb` Python package, in memory, in this process."""

    def __init__(self, threads: int, memory_limit: str | None = None):
        import duckdb
        self._duckdb = duckdb
        self.con = duckdb.connect(database=":memory:")
        self.con.execute(f"SET threads = {int(threads)}")
        if memory_limit:
            self.con.execute(f"SET memory_limit = '{memory_limit}'")
        self.version = "v" + duckdb.__version__ + " (python package)"

    def run(self, sql: str) -> tuple[float, list[list[str]]]:
        t0 = time.monotonic()
        try:
            cur = self.con.execute(sql)
            rows = cur.fetchall() if cur.description else []
        except self._duckdb.Error as exc:
            raise DuckError(f"{type(exc).__name__}: {exc}"[:4000]) from None
        secs = time.monotonic() - t0
        # COPY ... TO returns its row count: not a result of the statement's query
        if sql.lstrip()[:5].upper() == "COPY ":
            rows = []
        return secs, [["" if v is None else _text(v) for v in row] for row in rows]

    def close(self) -> None:
        self.con.close()


def _text(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("latin-1")
    return str(v)


def have_module() -> bool:
    try:
        import duckdb  # noqa: F401
        return True
    except ImportError:
        return False


def open_engine(threads: int, exe: str = "", memory_limit: str | None = None, engine: str = "auto"):
    """engine: auto (the Python package if importable, else the CLI at `exe`), module, cli."""
    if engine == "module" or (engine == "auto" and have_module()):
        return DuckModule(threads, memory_limit)
    if not exe:
        raise DuckError("no DuckDB: the `duckdb` Python package is not installed and no CLI path was given")
    return Duck(exe, threads, memory_limit)
