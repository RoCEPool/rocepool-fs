"""RapidsSE-backed StorageEngine adapter.

Status: this adapter is written against a small, INJECTED client
interface (`RapidsSEClient` below), not RapidsSE's own Python bindings
directly -- consistent with the interface-first approach requested: swap
the client without touching this file, and everything above this layer
(pool_node.py, fs/) needs zero changes either way. Two clients satisfy
`RapidsSEClient` today: `tests/fakes/sqlite_dbapi_client.py` (a unit-test
double, no wire protocol involved) and `wlp/wlp_client.py` -- a real
implementation of RapidsSE's own client wire protocol (WLP v5), built
directly from the `.thrift` IDL and C++ framing code in RapidsSE's own
source (kept unmodified in third_party/rapidsse/, see its UPSTREAM.md).
See `wlp/wlp_client.py`'s docstring for what that
implementation confirms versus assumes.

Why a DB-API2-shaped interface specifically: it's the near-universal
convention for Python SQL clients (sqlite3, psycopg2, pyodbc), and
`wlp_client.py` implements exactly this surface (execute/fetchone/
fetchall/commit) over the real protocol, so this was the right interface
to pick even before the real client existed.

TESTED how: tests/test_rapidsse_engine.py runs the storage-engine
contract suite against this class over the sqlite3 fake (genuine SQL
execution, not a mock). tests/test_rapidsse_wlp_engine.py reruns the same
contract suite plus wire-specific cases (binary payloads, Unicode,
multi-connection state sharing) with this class driven by the real
WlpClient talking over a real TCP socket to a real WLP-protocol test
server. What neither proves: that RapidsSE's actual compiled C++ server
accepts this exact SQL syntax or wire byte sequence -- this sandbox
cannot build or run that engine (see the project README's provenance
section for why). Confirm against a real RapidsSE node before production
use -- see this module's DIALECT NOTES below.
"""
from __future__ import annotations

import time
from typing import Protocol

from storage_engine.base import (
    ObjectExists, ObjectMeta, ObjectNotFound, PutResult, StorageEngine, VersionConflict,
)

DEFAULT_TABLE = "sefs_pool_objects"


class RapidsSEClient(Protocol):
    """The only thing this file needs from a real RapidsSE connection.
    Deliberately just the PEP 249 (DB-API2) subset this adapter uses --
    not the full DB-API2 surface, and not RapidsSE-specific in any way,
    so any SQL client satisfying this can be dropped in.
    """
    def execute(self, sql: str, params: tuple = ()) -> None: ...
    def fetchone(self) -> tuple | None: ...
    def fetchall(self) -> list[tuple]: ...
    def commit(self) -> None: ...


# ---------------------------------------------------------------------
# DIALECT NOTES -- read before pointing this at a real RapidsSE cluster.
#
# The SQL below uses the most portable, lowest-common-denominator forms
# on purpose (no vendor-specific UPSERT/MERGE syntax), because RapidsSE's
# actual SQL dialect was not available to check against. Specifically:
#
#   - `CREATE TABLE IF NOT EXISTS`: widely but not universally supported;
#     some analytical engines require a separate existence check instead.
#   - Parameter placeholders default to `%s` (psycopg2/MySQLdb style),
#     configurable via `RapidsSEEngine(..., placeholder=...)`. sqlite3
#     uses `?` (see tests/fakes/sqlite_dbapi_client.py, which sets
#     placeholder="?"); some ODBC drivers expect yet another marker.
#     Pass whatever RapidsSE's real driver actually expects.
#   - Compare-and-swap (`put` with `expected_version`) is implemented as
#     SELECT-then-INSERT/UPDATE inside what is assumed to be one
#     transaction per RapidsSEClient instance (i.e. autocommit is off
#     until `.commit()`), relying on the connection's isolation level to
#     make the read-then-write atomic. Without RapidsSE's real
#     concurrency-control documentation in hand, this is a reasonable
#     default, NOT a verified guarantee -- a true compare-and-swap under
#     concurrent writers may need an engine-specific atomic UPSERT
#     instead. Flagging this explicitly rather than presenting untested
#     concurrency behavior as solid.
# ---------------------------------------------------------------------


class RapidsSEEngine(StorageEngine):
    def __init__(self, client: RapidsSEClient, *, table: str = DEFAULT_TABLE,
                 ensure_schema: bool = True, placeholder: str = "%s"):
        self._client = client
        self._table = table
        self._placeholder = placeholder
        if ensure_schema:
            self._ensure_schema()

    def _ensure_schema(self) -> None:
        self._client.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            f"object_key VARCHAR(4096) PRIMARY KEY, "
            f"object_value BLOB, "
            f"object_version BIGINT NOT NULL, "
            f"created_at DOUBLE PRECISION NOT NULL, "
            f"updated_at DOUBLE PRECISION NOT NULL, "
            f"is_dir BOOLEAN NOT NULL DEFAULT FALSE)"
        )
        self._client.commit()

    def _q(self, sql: str) -> str:
        return sql.replace("?", self._placeholder)

    def _select_one(self, key: str):
        self._client.execute(
            self._q(f"SELECT object_value, object_version, created_at, updated_at, is_dir "
                     f"FROM {self._table} WHERE object_key = ?"),
            (key,),
        )
        return self._client.fetchone()

    def put(self, key: str, data: bytes, *, expected_version: int | None = None) -> PutResult:
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes")
        existing = self._select_one(key)
        current_version = existing[1] if existing else 0
        if expected_version is not None and expected_version != current_version:
            raise VersionConflict(key, expected_version, current_version)

        now = time.time()
        new_version = current_version + 1
        if existing is None:
            self._client.execute(
                self._q(f"INSERT INTO {self._table} "
                        f"(object_key, object_value, object_version, created_at, updated_at, is_dir) "
                        f"VALUES (?, ?, ?, ?, ?, ?)"),
                (key, bytes(data), new_version, now, now, False),
            )
        else:
            created_at = existing[2]
            self._client.execute(
                self._q(f"UPDATE {self._table} SET object_value = ?, object_version = ?, updated_at = ? "
                        f"WHERE object_key = ? AND object_version = ?"),
                (bytes(data), new_version, now, key, current_version),
            )
        self._client.commit()
        return PutResult(key=key, version=new_version, size=len(data))

    def get(self, key: str) -> bytes:
        row = self._select_one(key)
        if row is None:
            raise ObjectNotFound(key)
        return bytes(row[0])

    def stat(self, key: str) -> ObjectMeta:
        row = self._select_one(key)
        if row is None:
            raise ObjectNotFound(key)
        value, version, created_at, updated_at, is_dir = row
        return ObjectMeta(key=key, size=len(bytes(value)) if value is not None else 0,
                           created_at=created_at, updated_at=updated_at,
                           version=version, is_dir=bool(is_dir))

    def delete(self, key: str) -> None:
        if self._select_one(key) is None:
            raise ObjectNotFound(key)
        self._client.execute(self._q(f"DELETE FROM {self._table} WHERE object_key = ?"), (key,))
        self._client.commit()

    def list_prefix(self, prefix: str) -> list[ObjectMeta]:
        # LIKE + escaping the SQL wildcard characters that may appear in a
        # literal key prefix, so a key containing "%" or "_" cannot alter
        # which rows match.
        escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        self._client.execute(
            self._q(f"SELECT object_key, object_value, object_version, created_at, updated_at, is_dir "
                     f"FROM {self._table} WHERE object_key LIKE ? ESCAPE '\\' ORDER BY object_key"),
            (escaped + "%",),
        )
        rows = self._client.fetchall()
        return [
            ObjectMeta(key=k, size=len(bytes(v)) if v is not None else 0,
                       created_at=c, updated_at=u, version=ver, is_dir=bool(d))
            for k, v, ver, c, u, d in rows
        ]

    def exists(self, key: str) -> bool:
        return self._select_one(key) is not None

    def total_bytes(self) -> int:
        self._client.execute(self._q(f"SELECT COALESCE(SUM(LENGTH(object_value)), 0) FROM {self._table}"))
        row = self._client.fetchone()
        return int(row[0]) if row else 0
