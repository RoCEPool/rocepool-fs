""""Thick KV": Data-on-MDT small files and the one-round-trip OST ops, at the
file-system level (fs/posix_api.py DistributedFileSystem over the Python
reference servers). Server op-by-op parity with the native C servers is in
tests/test_native_servers.py; the C client runs the same scenarios through
the kernel in tests/test_native_fuse.py."""
import os
import random
import threading
import unittest

from fs.posix_api import DistributedFileSystem
from roce.loopback_transport import LoopbackTransport
from roce.remote_engine import RemoteEngine
from tests.test_dist_fs import _cluster

SS = 4096  # stripe size in these tests; inline limit = min(32 KiB, one stripe) = 4096


def _second_client(dfs, ttl=0.0):
    return DistributedFileSystem(mdt_transport=LoopbackTransport(), mdts=dfs.shard.mdts,
                                 osts=[RemoteEngine(o.address, LoopbackTransport()) for o in dfs.osts],
                                 stripe_size=SS, attr_ttl_s=ttl)


def _ost_keys(dfs, path):
    return sorted(m.key for o in dfs.osts for m in o.list_prefix(path + ".stripe."))


class DataOnMDTTests(unittest.TestCase):
    def setUp(self):
        self.dfs, self.mdts, self.ost_nodes, self.teardown = _cluster(stripe_size=SS, ttl=0.0)
        self.others = []

    def tearDown(self):
        for c in self.others:
            c.batcher.close()
        self.teardown()

    def client(self, ttl=0.0):
        c = _second_client(self.dfs, ttl)
        self.others.append(c)
        return c

    def test_small_file_lives_in_the_mdt_only(self):
        self.dfs.write_file("a.txt", b"hello inline")
        self.assertEqual(self.dfs.read_file("a.txt"), b"hello inline")
        self.assertEqual(_ost_keys(self.dfs, "a.txt"), [])
        self.assertTrue(self.dfs.getattr("a.txt").inline)
        self.dfs.write_range("a.txt", 20, b"XY")  # gap reads as zeros
        self.assertEqual(self.dfs.read_file("a.txt"), b"hello inline" + b"\x00" * 8 + b"XY")
        self.dfs.remove_file("a.txt")
        self.assertIsNone(self.dfs.getattr("a.txt"))

    def test_growing_past_the_limit_spills_to_stripes(self):
        data = random.Random(1).randbytes(3 * SS + 100)
        self.dfs.write_range("b.bin", 0, data[:1000])
        self.assertTrue(self.dfs.getattr("b.bin").inline)
        self.dfs.write_range("b.bin", 1000, data[1000:])
        self.assertFalse(self.dfs.getattr("b.bin").inline)
        self.assertEqual(self.dfs.read_file("b.bin"), data)
        self.assertEqual(len(_ost_keys(self.dfs, "b.bin")), 4)
        self.assertEqual(self.client().read_file("b.bin"), data)

    def test_truncate_inline_and_truncate_past_limit(self):
        self.dfs.write_file("t", b"0123456789")
        self.dfs.truncate("t", 4)
        self.assertEqual(self.dfs.read_file("t"), b"0123")
        self.dfs.truncate("t", 6)
        self.assertEqual(self.dfs.read_file("t"), b"0123\x00\x00")
        self.dfs.truncate("t", SS + 10)  # grows past the inline limit -> spill
        self.assertFalse(self.dfs.getattr("t").inline)
        self.assertEqual(self.dfs.read_file("t"), b"0123" + b"\x00" * (SS + 6))

    def test_a_frozen_spill_left_by_another_client_is_finished_by_the_next_writer(self):
        self.dfs.write_file("f", b"frozen-bytes")
        self.dfs.mdt.freeze("f")  # a spiller that "crashed" right after step 1
        self.client().write_range("f", 12, b"+more")
        self.assertEqual(self.dfs.read_file("f"), b"frozen-bytes+more")
        self.assertFalse(self.dfs.getattr("f").inline)

    def test_stale_striped_attr_on_a_recreated_inline_file_rewrites_inline(self):
        # A client whose cached attributes (within attr_ttl) still say
        # "striped" for a path another client recreated as an inline file:
        # a size-extending write is caught by the MDT (set_size -> INLINE) and
        # redone where the file lives. (A non-extending write in that window
        # is lost, like a write through a stale NFS handle -- the same
        # attr-TTL staleness contract as before, see docs/THICK_KV.md.)
        c = self.client(ttl=30.0)
        self.dfs.write_file("r", b"x" * (SS + 1))  # striped
        self.assertFalse(c.getattr("r").inline)
        self.dfs.remove_file("r")
        self.dfs.write_file("r", b"small")  # recreated, inline now
        c.write_range("r", SS + 100, b"-appended")  # c still believes "striped", size SS+1
        want = b"small" + b"\x00" * (SS + 95) + b"-appended"
        self.assertEqual(self.dfs.read_file("r"), want)

    def test_stale_striped_attr_on_a_file_frozen_mid_spill_finishes_it_and_rewrites(self):
        # as above, but the recreated inline file is frozen (a spill in
        # flight): set_size answers FROZEN -- finish the spill, then redo
        c = self.client(ttl=30.0)
        self.dfs.write_file("rf", b"x" * (SS + 1))
        self.assertFalse(c.getattr("rf").inline)
        self.dfs.remove_file("rf")
        self.dfs.write_file("rf", b"small")
        self.dfs.mdt.freeze("rf")
        c.write_range("rf", SS + 100, b"-appended")
        self.assertEqual(self.dfs.read_file("rf"), b"small" + b"\x00" * (SS + 95) + b"-appended")
        self.assertFalse(self.dfs.getattr("rf").inline)

    def test_a_slow_spill_helper_cannot_write_into_a_recreated_file(self):
        # ABA: helper H reads stripe 0's state ("absent"), confirms the file
        # is still frozen, then stalls. Meanwhile the spill is finished by
        # someone else, the file is removed and recreated (striped, stripe 0
        # a hole). H's CAS must not land: with a plain "expected 0 = absent"
        # it would, putting the old file's bytes into the new one.
        self.dfs.write_file("aba", b"old-file-bytes")
        h = self.client()
        real_freeze, calls = h.mdt.freeze, []
        stalled, release = threading.Event(), threading.Event()

        def freeze(path):
            r = real_freeze(path)
            calls.append(path)
            if len(calls) == 2:
                stalled.set()
                release.wait(20)
            return r
        h.mdt.freeze = freeze
        t = threading.Thread(target=h._spill, args=("aba",))
        t.start()
        self.assertTrue(stalled.wait(20))
        self.client().write_range("aba", 14, b"x" * (2 * SS))  # finishes the spill itself
        self.dfs.remove_file("aba")
        striped = DistributedFileSystem(mdt_transport=LoopbackTransport(), mdts=self.dfs.shard.mdts,
                                        osts=[RemoteEngine(o.address, LoopbackTransport()) for o in self.dfs.osts],
                                        stripe_size=SS, attr_ttl_s=0.0, inline=False)
        self.others.append(striped)
        striped.write_range("aba", SS, b"new")
        release.set()
        t.join(20)
        self.assertFalse(t.is_alive())
        self.assertEqual(self.dfs.read_file("aba"), b"\x00" * SS + b"new")

    def test_concurrent_writers_across_the_spill_lose_nothing(self):
        for rnd in range(8):
            path = f"race{rnd}"
            self.dfs.write_file(path, b"")
            writers = [self.client() for _ in range(4)]
            blk = 700  # 4 x 700 small writes stay inline; the big one forces the spill
            big = bytes([200]) * (2 * SS)

            def small(i, c):
                c.write_range(path, i * blk, bytes([i + 1]) * blk)

            def large(c):
                c.write_range(path, 4 * blk, big)
            ts = [threading.Thread(target=small, args=(i, writers[i])) for i in range(3)]
            ts.append(threading.Thread(target=large, args=(writers[3],)))
            [t.start() for t in ts]
            [t.join() for t in ts]
            want = b"".join(bytes([i + 1]) * blk for i in range(3)) + b"\x00" * blk + big
            self.assertEqual(self.dfs.read_file(path), want, f"round {rnd}")
            for c in writers:
                c.batcher.close()
            self.others = [c for c in self.others if c not in writers]

    def test_inline_disabled_client_still_reads_inline_files(self):
        self.dfs.write_file("m", b"made inline")
        c = self.client()
        c.inline = False
        self.assertEqual(c.read_file("m"), b"made inline")
        c.write_file("n", b"made striped")
        self.assertFalse(self.dfs.getattr("n").inline)
        self.assertEqual(self.dfs.read_file("n"), b"made striped")


class ThickOSTOpsAtFSLevelTests(unittest.TestCase):
    def setUp(self):
        self.dfs, _, self.ost_nodes, self.teardown = _cluster(stripe_size=SS, ttl=0.0)
        self.dfs.inline = False

    def tearDown(self):
        self.teardown()

    def test_partial_stripe_writes_are_single_write_at_calls(self):
        calls = []
        for o in self.dfs.osts:
            orig = o.write_at
            o.write_at = (lambda orig: lambda *a, **k: calls.append(a[0]) or orig(*a, **k))(orig)
        self.dfs.write_file("p", b"\x00" * (2 * SS))
        self.dfs.write_range("p", 10, b"abc")          # inside stripe 0
        self.dfs.write_range("p", SS - 2, b"WXYZ")     # straddles stripes 0/1
        self.assertEqual(sorted(calls), ["p.stripe.00000000", "p.stripe.00000000", "p.stripe.00000001"])
        got = self.dfs.read_file("p")
        self.assertEqual(got[10:13], b"abc")
        self.assertEqual(got[SS - 2:SS + 2], b"WXYZ")

    def test_concurrent_unaligned_writers_share_stripes_without_loss(self):
        self.dfs.write_file("s", b"")
        blk = 1000
        ts = [threading.Thread(target=self.dfs.write_range, args=("s", i * blk, bytes([65 + i]) * blk))
              for i in range(20)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(self.dfs.read_file("s"), b"".join(bytes([65 + i]) * blk for i in range(20)))

    def test_truncate_and_remove_use_batches(self):
        self.dfs.write_file("q", os.urandom(10 * SS + 5))
        self.dfs.truncate("q", 2 * SS + 7)
        self.assertEqual(len(self.dfs.read_file("q")), 2 * SS + 7)
        self.assertEqual(len(_ost_keys(self.dfs, "q")), 3)
        self.dfs.remove_file("q")
        self.assertEqual(_ost_keys(self.dfs, "q"), [])


if __name__ == "__main__":
    unittest.main()
