"""Tests for RapidsSEEngine, against a REAL sqlite3-backed DB-API2 client
(tests/fakes/sqlite_dbapi_client.py) -- so the actual SQL this adapter
generates is genuinely executed and checked, not mocked.

Two test classes:
  - RapidsSEEngineContractTests: reruns the exact same
    StorageEngineContractTests suite from test_storage_engine_contract.py
    against RapidsSEEngine, proving it honors the same interface contract
    MemoryEngine does -- this is the whole point of StorageEngine being an
    interface (see storage_engine/base.py).
  - RapidsSESpecificTests: SQL-generation specifics worth checking
    directly (schema creation, LIKE-wildcard escaping in keys, multi-row
    total_bytes aggregation).
"""
import unittest

from storage_engine.base import ObjectNotFound, VersionConflict
from storage_engine.rapidsse_engine import RapidsSEEngine
from tests.fakes.sqlite_dbapi_client import SQLiteDBAPIClient
from tests.test_storage_engine_contract import StorageEngineContractTests


def _make_engine() -> RapidsSEEngine:
    client = SQLiteDBAPIClient(":memory:")
    return RapidsSEEngine(client, placeholder="?")


class RapidsSEEngineContractTests(StorageEngineContractTests):
    """Inherits every test method from StorageEngineContractTests and reruns
    them with self.engine bound to a RapidsSEEngine instead of MemoryEngine
    -- exactly the reuse StorageEngine's docstring says this suite is for."""

    def setUp(self):
        self.engine = _make_engine()


class RapidsSESpecificTests(unittest.TestCase):
    def setUp(self):
        self.client = SQLiteDBAPIClient(":memory:")
        self.engine = RapidsSEEngine(self.client, placeholder="?")

    def test_schema_creation_is_idempotent(self):
        # Constructing a second engine against the same live connection
        # must not fail on "table already exists".
        RapidsSEEngine(self.client, placeholder="?", ensure_schema=True)

    def test_key_containing_percent_is_not_treated_as_a_sql_wildcard(self):
        self.engine.put("50%-done.txt", b"x")
        self.engine.put("50XXdone.txt", b"y")  # must NOT match "50%-done.txt"'s LIKE pattern
        results = self.engine.list_prefix("50%")
        self.assertEqual([m.key for m in results], ["50%-done.txt"],
                          "a literal '%' in a key must not act as a SQL wildcard in list_prefix")

    def test_key_containing_underscore_is_not_treated_as_a_sql_wildcard(self):
        self.engine.put("a_b.txt", b"x")
        self.engine.put("aXb.txt", b"y")  # must NOT match "a_b.txt"'s LIKE pattern if unescaped
        results = self.engine.list_prefix("a_")
        self.assertEqual([m.key for m in results], ["a_b.txt"])

    def test_total_bytes_reflects_real_sql_sum_across_many_rows(self):
        for i in range(10):
            self.engine.put(f"k{i}", bytes(i))
        self.assertEqual(self.engine.total_bytes(), sum(range(10)))

    def test_data_survives_a_real_round_trip_through_sqlite_blob_storage(self):
        payload = bytes(range(256)) * 100
        self.engine.put("binary", payload)
        self.assertEqual(self.engine.get("binary"), payload)

    def test_two_engine_instances_share_state_through_the_same_connection(self):
        # Simulates two callers (e.g. two PoolNode-local operations) using
        # RapidsSEEngine against what would be the same live RapidsSE
        # connection/session.
        other = RapidsSEEngine(self.client, placeholder="?", ensure_schema=False)
        self.engine.put("shared", b"v1")
        self.assertEqual(other.get("shared"), b"v1")


if __name__ == "__main__":
    unittest.main()
