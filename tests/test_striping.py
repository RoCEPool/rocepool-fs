"""Tests for fs/striping.py (Lustre-style RAID0 striping), against real
MemoryEngine instances standing in for OSTs -- so "does data actually
land on the OST the layout says it should" is checked by inspecting each
OST engine's own contents, not just trusting round-trip correctness
(which could pass even if striping silently didn't happen at all, e.g.
if every stripe accidentally landed on the same OST).
"""
import os
import threading
import time
import unittest

from fs.striping import StripeLayout, StripedStorage
from storage_engine.base import ObjectNotFound
from storage_engine.memory_engine import MemoryEngine


def _make_storage(num_osts: int = 4, stripe_size: int = 16) -> StripedStorage:
    osts = [MemoryEngine() for _ in range(num_osts)]
    return StripedStorage(osts, default_layout=StripeLayout(stripe_count=num_osts, stripe_size=stripe_size))


class SmallFileTests(unittest.TestCase):
    def test_put_get_roundtrip_smaller_than_one_stripe(self):
        storage = _make_storage()
        storage.put("f.txt", b"hello")
        self.assertEqual(storage.get("f.txt"), b"hello")
        self.assertEqual(storage.total_size("f.txt"), 5)

    def test_exists_and_delete(self):
        storage = _make_storage()
        self.assertFalse(storage.exists("f.txt"))
        storage.put("f.txt", b"x")
        self.assertTrue(storage.exists("f.txt"))
        storage.delete("f.txt")
        self.assertFalse(storage.exists("f.txt"))
        with self.assertRaises(ObjectNotFound):
            storage.get("f.txt")

    def test_delete_removes_stripe_objects_from_every_ost_touched(self):
        storage = _make_storage(num_osts=4, stripe_size=4)
        storage.put("f.bin", os.urandom(40))  # 10 stripes, spread round-robin over 4 OSTs
        self.assertGreater(sum(ost.total_bytes() for ost in storage.osts[1:]), 0)
        storage.delete("f.bin")
        for ost in storage.osts:
            self.assertEqual(ost.total_bytes(), 0)


class StripingActuallyHappensTests(unittest.TestCase):
    def test_multi_stripe_file_lands_on_multiple_distinct_osts(self):
        storage = _make_storage(num_osts=4, stripe_size=16)
        data = bytes(range(256)) * 4  # 1024 bytes -> 64 stripes @ 16B, over 4 OSTs
        storage.put("big.bin", data)
        locations = storage.stripe_locations("big.bin")
        self.assertEqual(len(locations), 64)
        self.assertEqual(set(locations), {0, 1, 2, 3})  # every OST actually used

        # Round-trip correctness, independent of the distribution check above.
        self.assertEqual(storage.get("big.bin"), data)

        # Directly confirm bytes are physically present on OSTs other than
        # OST[0] (which also carries the LOV record, so OST[0] having data
        # doesn't by itself prove anything).
        for i in (1, 2, 3):
            self.assertGreater(storage.osts[i].total_bytes(), 0)

    def test_round_robin_placement_matches_stripe_offset(self):
        storage = _make_storage(num_osts=4, stripe_size=8)
        layout = StripeLayout(stripe_count=4, stripe_size=8, stripe_offset=2)
        storage.put("f.bin", b"A" * 32, layout=layout)  # 4 stripes
        # stripe i -> OST (2 + i) % 4
        self.assertEqual(storage.stripe_locations("f.bin"), [2, 3, 0, 1])

    def test_stripe_count_can_be_fewer_than_available_osts(self):
        storage = _make_storage(num_osts=4, stripe_size=8)
        layout = StripeLayout(stripe_count=2, stripe_size=8)
        storage.put("f.bin", b"B" * 32, layout=layout)  # 4 stripes over 2 OSTs
        self.assertEqual(set(storage.stripe_locations("f.bin")), {0, 1})
        self.assertEqual(storage.get("f.bin"), b"B" * 32)


class RangeIOTests(unittest.TestCase):
    def test_partial_write_only_touches_overlapping_stripes(self):
        storage = _make_storage(num_osts=4, stripe_size=16)
        storage.put("f.bin", b"\x00" * 64)  # 4 stripes, all zero
        versions_before = [
            storage.osts[storage.get_layout("f.bin").ost_for_stripe(i, 4)].stat(f"f.bin.stripe.{i:08d}").version
            for i in range(4)
        ]
        storage.write_range("f.bin", 20, b"XXXX")  # entirely inside stripe 1 ([16,32))
        versions_after = [
            storage.osts[storage.get_layout("f.bin").ost_for_stripe(i, 4)].stat(f"f.bin.stripe.{i:08d}").version
            for i in range(4)
        ]
        self.assertEqual(versions_before[0], versions_after[0])
        self.assertNotEqual(versions_before[1], versions_after[1])  # only stripe 1 touched
        self.assertEqual(versions_before[2], versions_after[2])
        self.assertEqual(versions_before[3], versions_after[3])

        result = storage.get("f.bin")
        self.assertEqual(result[20:24], b"XXXX")
        self.assertEqual(result[:20], b"\x00" * 20)
        self.assertEqual(result[24:], b"\x00" * 40)

    def test_write_range_spanning_stripe_boundary(self):
        storage = _make_storage(num_osts=4, stripe_size=16)
        storage.put("f.bin", b"\x00" * 32)
        storage.write_range("f.bin", 10, b"Y" * 12)  # spans stripe 0 [10,16) and stripe 1 [16,22)
        result = storage.get("f.bin")
        self.assertEqual(result, b"\x00" * 10 + b"Y" * 12 + b"\x00" * 10)

    def test_write_range_extends_file_and_updates_total_size(self):
        storage = _make_storage(num_osts=4, stripe_size=16)
        storage.put("f.bin", b"AAAA")
        self.assertEqual(storage.total_size("f.bin"), 4)
        storage.write_range("f.bin", 100, b"Z")
        self.assertEqual(storage.total_size("f.bin"), 101)
        result = storage.get("f.bin")
        self.assertEqual(len(result), 101)
        self.assertEqual(result[100:101], b"Z")
        self.assertEqual(result[4:100], b"\x00" * 96)

    def test_read_range_partial_and_cross_stripe(self):
        storage = _make_storage(num_osts=4, stripe_size=8)
        data = bytes(range(40))
        storage.put("f.bin", data)
        self.assertEqual(storage.read_range("f.bin", 5, 10), data[5:15])
        self.assertEqual(storage.read_range("f.bin", 0, 40), data)
        self.assertEqual(storage.read_range("f.bin", 35, 100), data[35:40])  # clamped past EOF


class LayoutTests(unittest.TestCase):
    def test_get_layout_reflects_what_was_used_at_put_time(self):
        storage = _make_storage(num_osts=4, stripe_size=16)
        layout = StripeLayout(stripe_count=3, stripe_size=32, stripe_offset=1)
        storage.put("f.bin", b"x" * 5, layout=layout)
        got = storage.get_layout("f.bin")
        self.assertEqual((got.stripe_count, got.stripe_size, got.stripe_offset), (3, 32, 1))

    def test_set_default_layout_rejects_stripe_count_above_ost_count(self):
        storage = _make_storage(num_osts=4)
        with self.assertRaises(ValueError):
            storage.set_default_layout(StripeLayout(stripe_count=5))

    def test_zero_ost_construction_rejected(self):
        with self.assertRaises(ValueError):
            StripedStorage([])


class _SlowReadEngine(MemoryEngine):
    """Adds latency after every read, like a network round trip, which
    widens the gap between observing a stripe's version and writing it
    back -- so a lost update is near-certain if write_range doesn't
    protect it."""

    def get(self, key):
        data = super().get(key)
        time.sleep(0.002)
        return data

    def stat(self, key):
        meta = super().stat(key)
        time.sleep(0.002)
        return meta


class ConcurrentDisjointWritesTests(unittest.TestCase):
    def test_disjoint_writes_sharing_a_stripe_all_survive(self):
        # 8 writers, 2KiB blocks, 16KiB stripes: every stripe is written by
        # all 8 writers at once (N-1 strided MPI-IO). Found across two
        # network nodes first -- see fs/striping.py write_range docstring.
        storage = StripedStorage([_SlowReadEngine() for _ in range(2)],
                                 default_layout=StripeLayout(stripe_count=2, stripe_size=16 * 1024))
        storage.put("f", b"")
        writers, blocks, bs = 8, 6, 2048

        def _writer(w):
            for k in range(blocks):
                storage.write_range("f", (k * writers + w) * bs, bytes([w + 1]) * bs)

        threads = [threading.Thread(target=_writer, args=(w,)) for w in range(writers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        got = storage.get("f")
        lost = [(k, w) for k in range(blocks) for w in range(writers)
                if got[(k * writers + w) * bs:(k * writers + w + 1) * bs] != bytes([w + 1]) * bs]
        self.assertEqual(lost, [])
        self.assertEqual(len(got), writers * blocks * bs)


if __name__ == "__main__":
    unittest.main()
