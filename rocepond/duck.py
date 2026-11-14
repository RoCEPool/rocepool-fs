"""One DuckDB process, driven statement by statement (the DuckDB CLI: it is a
single binary with no Python package to install; the `duckdb` Python module
would fit the same interface)."""
from __future__ import annotations

import csv
import io
import re
import subprocess

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
