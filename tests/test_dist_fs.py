"""Tests for the distributed-metadata + striped POSIX path:
fs/mdt_node.py, fs/namespace_shard.py, fs/attr_cache.py, fs/layout.py,
fs/striping.stripe_ranges, roce/request_batcher.py and
fs/posix_api.DistributedFileSystem -- plus a real kernel FUSE mount of it
(skipped only where /dev/fuse or root is unavailable).

The MDTs and OSTs here are real servers on real sockets (LoopbackTransport),
not in-process fakes, so routing and wire encoding are exercised; OST
contents are inspected directly to check data really is striped.
"""
import os
import random
import subprocess
import tempfile
import threading
import time
import unittest

from fs.attr_cache import MISSING, Attr, AttrCache
from fs.layout import choose_layout
from fs.mdt_node import MDTError, MDTServer
from fs.namespace_shard import NamespaceShard
from fs.posix_api import (
    DirectoryNotEmpty, DistributedFileSystem, FileExistsInPool, FileNotFoundInPool, IsADirectory,
    StripeReadCache,
)
from fs.striping import StripeLayout, stripe_ranges
from roce.loopback_transport import LoopbackTransport
from roce.pool_node import PoolNode
from roce.remote_engine import RemoteEngine
from roce.request_batcher import RequestBatcher
from storage_engine.memory_engine import MemoryEngine

_PORTS = iter(range(19100, 19900))


def _cluster(n_mdt=2, n_ost=4, stripe_size=16, ttl=3.0):
    servers, mdts, ost_addrs, ost_nodes = [], [], [], []
    for _ in range(n_mdt):
        addr = f"127.0.0.1:{next(_PORTS)}"
        s = MDTServer(LoopbackTransport())
        s.start(addr)
        servers.append(s)
        mdts.append(addr)
    for _ in range(n_ost):
        addr = f"127.0.0.1:{next(_PORTS)}"
        node = PoolNode(local_address=addr, nodes=[addr], engine=MemoryEngine(), transport=LoopbackTransport())
        node.start()
        ost_nodes.append(node)
        ost_addrs.append(addr)
    osts = [RemoteEngine(a, LoopbackTransport()) for a in ost_addrs]
    dfs = DistributedFileSystem(mdt_transport=LoopbackTransport(), mdts=mdts, osts=osts,
                                stripe_size=stripe_size, attr_ttl_s=ttl)

    def teardown():
        dfs.batcher.close()
        for s in servers:
            s.stop()
        for nd in ost_nodes:
            nd.stop()
    return dfs, servers, ost_nodes, teardown


class StripeRangesTests(unittest.TestCase):
    def test_spans_cover_the_range_exactly_and_map_round_robin(self):
        layout = StripeLayout(stripe_count=4, stripe_size=10, stripe_offset=1)
        spans = stripe_ranges(layout, 5, 30, num_osts=4)  # [5,35): stripes 0..3
        self.assertEqual([s.stripe_index for s in spans], [0, 1, 2, 3])
        self.assertEqual([s.ost_index for s in spans], [1, 2, 3, 0])
        self.assertEqual(sum(s.length for s in spans), 30)
        self.assertEqual((spans[0].in_stripe_lo, spans[0].in_stripe_hi, spans[0].buf_lo), (5, 10, 0))
        self.assertEqual((spans[-1].in_stripe_lo, spans[-1].in_stripe_hi, spans[-1].buf_lo), (0, 5, 25))

    def test_empty_range(self):
        self.assertEqual(stripe_ranges(StripeLayout(2, 10, 0), 7, 0, 2), [])


class LayoutTests(unittest.TestCase):
    def test_different_files_start_on_different_osts(self):
        offsets = {choose_layout(f"d/f{i}", 4).stripe_offset for i in range(64)}
        self.assertEqual(offsets, {0, 1, 2, 3})


class NamespaceShardTests(unittest.TestCase):
    def test_by_parent_rule_siblings_share_an_mdt_and_directories_spread(self):
        shard = NamespaceShard(["a", "b", "c"], striped=False)
        self.assertEqual(len({shard.mdt_for(f"dir/f{i}") for i in range(50)}), 1)
        self.assertEqual(shard.mdt_for("dir/x"), shard.mdt_for_children("dir"))
        self.assertGreater(len({shard.mdt_for_children(f"d{i}") for i in range(50)}), 1)

    def test_striped_rule_spreads_one_directory_over_every_mdt(self):
        shard = NamespaceShard(["a", "b", "c"], striped=True)
        self.assertEqual({shard.mdt_for(f"dir/f{i}") for i in range(50)}, {"a", "b", "c"})
        self.assertEqual(shard.mdts_for_children("dir"), ["a", "b", "c"])
        self.assertEqual(shard.mdt_for("/dir//f1/"), shard.mdt_for("dir/f1"))  # normalised first
        with self.assertRaises(ValueError):
            shard.mdt_for_children("dir")


class AttrCacheTests(unittest.TestCase):
    def test_write_size_updates_never_shrink_but_truncate_does(self):
        from fs.attr_cache import Attr, AttrCache
        c = AttrCache(ttl_s=10)
        c.put("f", Attr("f", False, 100, 0.0, 0.0, None))
        c.update_size("f", 300)
        c.update_size("f", 200)  # an older set_size(max) reply arriving late
        self.assertEqual(c.get("f").size, 300)
        c.update_size("f", 50, exact=True)  # truncate
        self.assertEqual(c.get("f").size, 50)

    def test_hit_negative_hit_expiry_and_disable(self):
        c = AttrCache(ttl_s=0.05)
        a = Attr("p", False, 3, 0, 0, None)
        self.assertIs(c.get("p"), MISSING)
        c.put("p", a)
        c.put("q", None)
        self.assertEqual(c.get("p"), a)
        self.assertIsNone(c.get("q"))
        time.sleep(0.07)
        self.assertIs(c.get("p"), MISSING)
        off = AttrCache(ttl_s=0)
        off.put("p", a)
        self.assertIs(off.get("p"), MISSING)


class RequestBatcherTests(unittest.TestCase):
    def test_results_in_order_and_actually_concurrent(self):
        b = RequestBatcher(max_workers=8, max_per_peer=8)
        t0 = time.monotonic()
        out = b.map(lambda i: (time.sleep(0.1), i * i)[1], range(8))
        self.assertEqual(out, [i * i for i in range(8)])
        self.assertLess(time.monotonic() - t0, 0.5)  # serial would be 0.8s
        b.close()

    def test_per_peer_cap_is_enforced(self):
        b = RequestBatcher(max_workers=16, max_per_peer=2)
        live, peak, lock = [0], [0], threading.Lock()

        def task(_):
            with lock:
                live[0] += 1
                peak[0] = max(peak[0], live[0])
            time.sleep(0.05)
            with lock:
                live[0] -= 1
        b.map(task, range(10), peer_of=lambda _: "same-peer")
        self.assertLessEqual(peak[0], 2)
        b.close()

    def test_first_exception_propagates_after_all_finish(self):
        b = RequestBatcher()
        done = []

        def task(i):
            if i == 1:
                raise ValueError("boom")
            time.sleep(0.05)
            done.append(i)
        with self.assertRaises(ValueError):
            b.map(task, range(4))
        self.assertEqual(sorted(done), [0, 2, 3])
        b.close()


class MDTServerStateMachineTests(unittest.TestCase):
    def test_create_lookup_size_unlink(self):
        s = MDTServer(LoopbackTransport())
        s.apply({"op": "create", "path": "d", "is_dir": True})
        s.apply({"op": "create", "path": "d/f", "is_dir": False, "layout": {"x": 1}})
        with self.assertRaises(MDTError) as cm:
            s.apply({"op": "create", "path": "d/f", "is_dir": False})
        self.assertEqual(cm.exception.code, "EXISTS")
        self.assertEqual(s.apply({"op": "set_size", "path": "d/f", "size": 10})["size"], 10)
        self.assertEqual(s.apply({"op": "set_size", "path": "d/f", "size": 4})["size"], 10)  # max
        self.assertEqual(s.apply({"op": "set_size", "path": "d/f", "size": 4, "mode": "exact"})["size"], 4)
        self.assertEqual([e["name"] for e in s.apply({"op": "readdir", "path": "d"})], ["f"])
        s.apply({"op": "unlink", "path": "d/f", "is_dir": False})
        with self.assertRaises(MDTError):
            s.apply({"op": "lookup", "path": "d/f"})


class DistributedFileSystemTests(unittest.TestCase):
    def setUp(self):
        self.dfs, self.mdt_servers, self.ost_nodes, self._td = _cluster()

    def tearDown(self):
        self._td()

    def test_roundtrip_across_many_stripes_lands_on_every_ost(self):
        data = bytes(random.Random(1).randrange(256) for _ in range(1000))
        self.dfs.write_file("f.bin", data)
        self.assertEqual(self.dfs.read_file("f.bin"), data)
        self.assertEqual(self.dfs.stat_path("f.bin").size, 1000)
        per_ost = [len(n.engine.list_prefix("f.bin.stripe.")) for n in self.ost_nodes]
        self.assertTrue(all(c > 0 for c in per_ost), per_ost)
        self.assertEqual(sum(per_ost), (1000 + 15) // 16)

    def test_partial_writes_and_unaligned_reads(self):
        self.dfs.write_range("f", 5, b"hello world")  # creates, leaves a 5-byte hole
        self.dfs.write_range("f", 30, b"XY")
        want = b"\x00" * 5 + b"hello world" + b"\x00" * 14 + b"XY"
        self.assertEqual(self.dfs.read_file("f"), want)
        for off in range(0, 32, 3):
            for ln in (1, 7, 17, 40):
                self.assertEqual(self.dfs.read_range("f", off, ln), want[off:off + ln])

    def test_concurrent_disjoint_writers_to_one_file_lose_nothing(self):
        self.dfs.create_file("shared")
        blocks = {i: bytes([65 + i % 26]) * 7 for i in range(40)}  # 7-byte blocks straddle 16-byte stripes
        threads = [threading.Thread(target=self.dfs.write_range, args=("shared", i * 7, b)) for i, b in blocks.items()]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(self.dfs.read_file("shared"), b"".join(blocks[i] for i in range(40)))

    def test_truncate_shrink_then_grow_reads_zeros_not_stale_bytes(self):
        self.dfs.write_file("t", b"A" * 100)
        self.dfs.truncate("t", 20)
        self.assertEqual(self.dfs.read_file("t"), b"A" * 20)
        self.dfs.truncate("t", 60)
        self.assertEqual(self.dfs.read_file("t"), b"A" * 20 + b"\x00" * 40)

    def test_remove_deletes_stripes_and_entry(self):
        self.dfs.write_file("gone", b"z" * 100)
        self.dfs.remove_file("gone")
        self.assertFalse(self.dfs.exists("gone"))
        self.assertEqual(sum(len(n.engine.list_prefix("gone.")) for n in self.ost_nodes), 0)
        with self.assertRaises(FileNotFoundInPool):
            self.dfs.read_file("gone")

    def test_directories(self):
        self.dfs.mkdir("a/b/c", parents=True)
        self.dfs.write_file("a/b/c/f", b"x")
        self.assertEqual(self.dfs.listdir("a/b"), ["c"])
        self.assertEqual(self.dfs.listdir("a/b/c"), ["f"])
        with self.assertRaises(FileExistsInPool):
            self.dfs.mkdir("a/b")
        with self.assertRaises(DirectoryNotEmpty):
            self.dfs.rmdir("a/b/c")
        with self.assertRaises(FileNotFoundInPool):
            self.dfs.write_file("nope/f", b"x")
        with self.assertRaises(IsADirectory):
            self.dfs.read_file("a")
        self.dfs.remove_file("a/b/c/f")
        self.dfs.rmdir("a/b/c")
        self.assertEqual(self.dfs.listdir("a/b"), [])

    def test_directory_entries_are_spread_over_both_mdts(self):
        for i in range(20):
            self.dfs.mkdir(f"d{i}")
            self.dfs.write_file(f"d{i}/f", b"1")
        per_mdt = [s.ops_served for s in self.mdt_servers]
        self.assertTrue(all(c > 0 for c in per_mdt), per_mdt)

    def test_attr_cache_saves_round_trips_and_ttl_zero_does_not(self):
        self.dfs.write_file("c", b"123")
        before = self.dfs.mdt.rpcs
        for _ in range(10):
            self.assertEqual(self.dfs.stat_path("c").size, 3)
        self.assertEqual(self.dfs.mdt.rpcs, before)
        self.dfs.cache.ttl_s = 0
        before = self.dfs.mdt.rpcs
        for _ in range(10):
            self.dfs.stat_path("c")
        self.assertEqual(self.dfs.mdt.rpcs - before, 10)

    def test_readdir_primes_cache_for_following_lookups(self):
        for i in range(5):
            self.dfs.write_file(f"p/f{i}", b"x", create_parents=True)
        self.dfs.cache = AttrCache(ttl_s=3)  # cold client
        self.dfs.listdir("p")
        before = self.dfs.mdt.rpcs
        for i in range(5):
            self.assertTrue(self.dfs.exists(f"p/f{i}"))
        self.assertEqual(self.dfs.mdt.rpcs, before)

    def test_read_cache_with_readahead_matches_uncached_reads(self):
        data = bytes(random.Random(2).randrange(256) for _ in range(500))
        self.dfs.write_file("r", data)
        rc = StripeReadCache()
        got = b"".join(self.dfs.read_range("r", off, 13, stripe_cache=rc) for off in range(0, 500, 13))
        self.assertEqual(got, data)

    def test_second_client_sees_first_clients_writes(self):
        other = DistributedFileSystem(mdt_transport=LoopbackTransport(), mdts=self.dfs.shard.mdts,
                                      osts=self.dfs.osts, stripe_size=16, attr_ttl_s=0)
        self.dfs.write_file("shared.txt", b"from-client-1" * 5)
        self.assertEqual(other.read_file("shared.txt"), b"from-client-1" * 5)
        other.write_range("shared.txt", 0, b"CLIENT-2")
        self.dfs.cache.invalidate("shared.txt")
        self.assertEqual(self.dfs.read_range("shared.txt", 0, 8), b"CLIENT-2")
        other.batcher.close()


@unittest.skipUnless(os.path.exists("/dev/fuse") and os.geteuid() == 0, "needs /dev/fuse and root")
class RealKernelMountTests(unittest.TestCase):
    """Mount DistributedFileSystem through the real kernel FUSE driver
    (multi-threaded daemon, range I/O mode) and drive it with coreutils."""

    def test_coreutils_roundtrip_through_kernel(self):
        from fs.fuse_lowlevel import FuseKernelMount, FuseLowLevelDaemon
        dfs, _, _, td = _cluster(stripe_size=64 * 1024)
        mnt = tempfile.mkdtemp(prefix="dfs_mnt_")
        mount = FuseKernelMount(mnt)
        daemon = FuseLowLevelDaemon(mount.fd, dfs)
        t = threading.Thread(target=daemon.run, kwargs={"threads": 4}, daemon=True)
        t.start()
        try:
            src = os.path.join(tempfile.mkdtemp(), "src.bin")
            with open(src, "wb") as f:
                f.write(os.urandom(3 * 1024 * 1024 + 12345))
            os.makedirs(os.path.join(mnt, "sub/dir"))
            subprocess.run(["cp", src, os.path.join(mnt, "sub/dir/copy.bin")], check=True)
            subprocess.run(["cmp", src, os.path.join(mnt, "sub/dir/copy.bin")], check=True)
            with open(os.path.join(mnt, "sub/dir/copy.bin"), "r+b") as f:  # in-place overwrite + readback on same fd
                f.seek(100_000)
                f.write(b"PATCH")
                f.seek(99_998)
                self.assertEqual(f.read(9)[2:7], b"PATCH")
            os.truncate(os.path.join(mnt, "sub/dir/copy.bin"), 1000)
            self.assertEqual(os.path.getsize(os.path.join(mnt, "sub/dir/copy.bin")), 1000)
            self.assertEqual(sorted(os.listdir(os.path.join(mnt, "sub/dir"))), ["copy.bin"])
            os.remove(os.path.join(mnt, "sub/dir/copy.bin"))
            os.rmdir(os.path.join(mnt, "sub/dir"))
            self.assertEqual(os.listdir(os.path.join(mnt, "sub")), [])
        finally:
            subprocess.run(["umount", "-l", mnt])
            mount.close()
            t.join(timeout=5)
            td()


if __name__ == "__main__":
    unittest.main()
