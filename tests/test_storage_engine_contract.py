"""Contract tests for StorageEngine implementations.

Written against the abstract interface, not MemoryEngine's internals, on
purpose: point ENGINE_UNDER_TEST at any other StorageEngine implementation
(a future RapidsSE-backed adapter included) and these same tests should
pass unmodified if that implementation honors the contract.
"""
import unittest

from storage_engine.base import ObjectNotFound, VersionConflict
from storage_engine.memory_engine import MemoryEngine, StorageEngineFull

ENGINE_UNDER_TEST = MemoryEngine


class StorageEngineContractTests(unittest.TestCase):
    def setUp(self):
        self.engine = ENGINE_UNDER_TEST()

    def test_put_then_get_roundtrips_exact_bytes(self):
        self.engine.put("a/b.bin", b"hello world")
        self.assertEqual(self.engine.get("a/b.bin"), b"hello world")

    def test_get_missing_raises_object_not_found(self):
        with self.assertRaises(ObjectNotFound):
            self.engine.get("does/not/exist")

    def test_put_overwrite_increments_version(self):
        r1 = self.engine.put("k", b"v1")
        r2 = self.engine.put("k", b"v2")
        self.assertEqual(r1.version, 1)
        self.assertEqual(r2.version, 2)
        self.assertEqual(self.engine.get("k"), b"v2")

    def test_delete_then_get_raises(self):
        self.engine.put("k", b"v")
        self.engine.delete("k")
        with self.assertRaises(ObjectNotFound):
            self.engine.get("k")

    def test_delete_missing_raises(self):
        with self.assertRaises(ObjectNotFound):
            self.engine.delete("nope")

    def test_list_prefix_returns_sorted_matching_keys_only(self):
        self.engine.put("dir/a", b"1")
        self.engine.put("dir/b", b"2")
        self.engine.put("other/c", b"3")
        results = self.engine.list_prefix("dir/")
        self.assertEqual([m.key for m in results], ["dir/a", "dir/b"])

    def test_exists(self):
        self.assertFalse(self.engine.exists("k"))
        self.engine.put("k", b"v")
        self.assertTrue(self.engine.exists("k"))

    def test_total_bytes_tracks_puts_and_deletes(self):
        self.assertEqual(self.engine.total_bytes(), 0)
        self.engine.put("a", b"12345")
        self.assertEqual(self.engine.total_bytes(), 5)
        self.engine.put("b", b"123")
        self.assertEqual(self.engine.total_bytes(), 8)
        self.engine.delete("a")
        self.assertEqual(self.engine.total_bytes(), 3)

    def test_total_bytes_accounts_for_overwrite_size_delta(self):
        self.engine.put("a", b"12345")       # +5
        self.engine.put("a", b"1")           # overwrite -> -4 net
        self.assertEqual(self.engine.total_bytes(), 1)

    # -- compare-and-swap: this is the primitive the RoCE replication
    # protocol leans on to avoid silent last-writer-wins data loss --
    def test_expected_version_zero_requires_object_absent(self):
        self.engine.put("k", b"v", expected_version=0)  # ok: didn't exist
        with self.assertRaises(VersionConflict):
            self.engine.put("k", b"v2", expected_version=0)  # now exists at v1

    def test_expected_version_match_succeeds(self):
        r1 = self.engine.put("k", b"v1")
        self.engine.put("k", b"v2", expected_version=r1.version)
        self.assertEqual(self.engine.get("k"), b"v2")

    def test_expected_version_mismatch_raises_and_does_not_apply_write(self):
        self.engine.put("k", b"v1")
        with self.assertRaises(VersionConflict):
            self.engine.put("k", b"stale-write", expected_version=99)
        self.assertEqual(self.engine.get("k"), b"v1", "rejected CAS must not modify stored data")

    def test_stat_fields(self):
        self.engine.put("k", b"12345")
        meta = self.engine.stat("k")
        self.assertEqual(meta.key, "k")
        self.assertEqual(meta.size, 5)
        self.assertEqual(meta.version, 1)
        self.assertGreaterEqual(meta.updated_at, meta.created_at)


class MemoryEngineCapacityTests(unittest.TestCase):
    """Reference-engine-specific: capacity enforcement isn't part of the
    abstract contract (other engines may size differently), so this stays
    out of StorageEngineContractTests."""

    def test_put_beyond_max_bytes_raises_and_does_not_partially_apply(self):
        engine = MemoryEngine(max_bytes=10)
        engine.put("a", b"12345")
        with self.assertRaises(StorageEngineFull):
            engine.put("b", b"123456")  # 5 + 6 = 11 > 10
        self.assertFalse(engine.exists("b"))
        self.assertEqual(engine.total_bytes(), 5)


if __name__ == "__main__":
    unittest.main()
