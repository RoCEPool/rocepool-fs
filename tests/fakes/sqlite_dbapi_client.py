"""A real DB-API2 client backed by Python's stdlib sqlite3, satisfying
storage_engine.rapidsse_engine.RapidsSEClient.

This exists so tests/test_rapidsse_engine.py exercises RapidsSEEngine's
actual generated SQL against a real, standard SQL engine -- not a mock
that just records "was .execute() called." sqlite3 accepts the specific
statements RapidsSEEngine issues (CREATE TABLE IF NOT EXISTS with BLOB/
BIGINT/DOUBLE PRECISION/BOOLEAN/VARCHAR column types -- SQLite's type
affinity system accepts arbitrary declared type names; LIKE ... ESCAPE;
parameterized queries), so this is a genuine, if not RapidsSE-specific,
correctness check on the SQL itself.

Explicitly NOT a claim that RapidsSE's real SQL dialect matches SQLite's
-- see rapidsse_engine.py's "DIALECT NOTES" for what to re-verify against
RapidsSE's actual SQL reference before production use.
"""
from __future__ import annotations

import sqlite3


class SQLiteDBAPIClient:
    """Adapts a sqlite3 connection to the RapidsSEClient Protocol.

    Uses "?" placeholders (SQLite's native style) rather than "%s" -- pass
    placeholder="?" to RapidsSEEngine when using this client.
    """

    def __init__(self, path: str = ":memory:"):
        self._conn = sqlite3.connect(path)
        self._cursor = self._conn.cursor()

    def execute(self, sql: str, params: tuple = ()) -> None:
        self._cursor.execute(sql, params)

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
