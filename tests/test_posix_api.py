import unittest

from fs.posix_api import (
    DirectoryNotEmpty, FileExistsInPool, FileNotFoundInPool, IsADirectory, PooledFileSystem,
)
from storage_engine.memory_engine import MemoryEngine


class PooledFileSystemTests(unittest.TestCase):
    def setUp(self):
        self.fs = PooledFileSystem(MemoryEngine())

    def test_write_then_read_roundtrips(self):
        self.fs.write_file("hello.txt", b"world")
        self.assertEqual(self.fs.read_file("hello.txt"), b"world")

    def test_read_missing_raises(self):
        with self.assertRaises(FileNotFoundInPool):
            self.fs.read_file("nope.txt")

    def test_write_without_existing_parent_dir_raises(self):
        with self.assertRaises(FileNotFoundInPool):
            self.fs.write_file("a/b/c.txt", b"x")

    def test_write_with_create_parents_makes_the_whole_path(self):
        self.fs.write_file("a/b/c.txt", b"x", create_parents=True)
        self.assertEqual(self.fs.read_file("a/b/c.txt"), b"x")
        self.assertIn("b", self.fs.listdir("a"))
        self.assertIn("c.txt", self.fs.listdir("a/b"))

    def test_mkdir_then_listdir(self):
        self.fs.mkdir("dir")
        self.fs.write_file("dir/one.txt", b"1")
        self.fs.write_file("dir/two.txt", b"2")
        self.fs.mkdir("dir/sub")
        self.assertEqual(self.fs.listdir("dir"), ["one.txt", "sub", "two.txt"])

    def test_listdir_is_one_level_only_not_recursive(self):
        self.fs.mkdir("a", parents=True)
        self.fs.mkdir("a/b", parents=True)
        self.fs.write_file("a/b/deep.txt", b"x")
        self.assertEqual(self.fs.listdir("a"), ["b"])  # deep.txt must NOT show up here

    def test_mkdir_twice_raises_file_exists(self):
        self.fs.mkdir("d")
        with self.assertRaises(FileExistsInPool):
            self.fs.mkdir("d")

    def test_mkdir_without_parents_flag_on_missing_parent_raises(self):
        with self.assertRaises(FileNotFoundInPool):
            self.fs.mkdir("a/b")

    def test_mkdir_parents_true_creates_intermediate_dirs(self):
        self.fs.mkdir("a/b/c", parents=True)
        self.assertTrue(self.fs.exists("a"))
        self.assertTrue(self.fs.exists("a/b"))
        self.assertTrue(self.fs.exists("a/b/c"))

    def test_rmdir_non_empty_raises(self):
        self.fs.mkdir("d")
        self.fs.write_file("d/f.txt", b"x")
        with self.assertRaises(DirectoryNotEmpty):
            self.fs.rmdir("d")

    def test_rmdir_empty_succeeds(self):
        self.fs.mkdir("d")
        self.fs.rmdir("d")
        self.assertFalse(self.fs.exists("d"))

    def test_write_to_a_directory_path_raises_is_a_directory(self):
        self.fs.mkdir("d")
        with self.assertRaises(IsADirectory):
            self.fs.write_file("d", b"x")

    def test_read_a_directory_path_raises_is_a_directory(self):
        self.fs.mkdir("d")
        with self.assertRaises(IsADirectory):
            self.fs.read_file("d")

    def test_stat_reports_is_dir_correctly(self):
        self.fs.mkdir("d")
        self.fs.write_file("f.txt", b"12345")
        self.assertTrue(self.fs.stat_path("d").is_dir)
        self.assertFalse(self.fs.stat_path("f.txt").is_dir)
        self.assertEqual(self.fs.stat_path("f.txt").size, 5)

    def test_path_normalization_strips_slashes_and_dot_segments(self):
        self.fs.write_file("/a/b.txt", b"x", create_parents=True)
        self.assertEqual(self.fs.read_file("a/b.txt"), b"x")
        self.assertEqual(self.fs.read_file("./a/./b.txt"), b"x")

    def test_overwrite_existing_file_updates_content_and_version(self):
        r1 = self.fs.write_file("f.txt", b"v1")
        r2 = self.fs.write_file("f.txt", b"v2")
        self.assertEqual(r1.version, 1)
        self.assertEqual(r2.version, 2)
        self.assertEqual(self.fs.read_file("f.txt"), b"v2")

    def test_remove_file(self):
        self.fs.write_file("f.txt", b"x")
        self.fs.remove_file("f.txt")
        self.assertFalse(self.fs.exists("f.txt"))


class PooledFileSystemOverPoolNodeTests(unittest.TestCase):
    """The same file-semantics tests, but through an actual multi-node
    PoolNode over real sockets -- proves the fs layer works transparently
    across the network boundary, not just against a bare local engine."""

    def setUp(self):
        from roce.loopback_transport import LoopbackTransport
        from roce.pool_node import PoolNode
        addrs = ["127.0.0.1:18960", "127.0.0.1:18961", "127.0.0.1:18962"]
        self.nodes = []
        for addr in addrs:
            node = PoolNode(local_address=addr, nodes=addrs, engine=MemoryEngine(), transport=LoopbackTransport())
            node.start()
            self.nodes.append(node)
        self.addCleanup(lambda: [n.stop() for n in self.nodes])
        # Deliberately use a DIFFERENT node as the fs entry point than the
        # one that will end up owning any given key, to force forwarding.
        self.fs = PooledFileSystem(self.nodes[0])

    def test_write_via_one_node_readable_via_another_nodes_fs(self):
        self.fs.write_file("shared/greeting.txt", b"hello pool", create_parents=True)
        other_fs = PooledFileSystem(self.nodes[2])
        self.assertEqual(other_fs.read_file("shared/greeting.txt"), b"hello pool")

    def test_directory_listing_works_across_the_pool(self):
        self.fs.mkdir("d")
        for i in range(5):
            self.fs.write_file(f"d/file{i}.txt", f"content-{i}".encode())
        self.assertEqual(self.fs.listdir("d"), [f"file{i}.txt" for i in range(5)])

    def test_list_prefix_merges_results_actually_scattered_across_all_nodes(self):
        """Not incidental: explicitly finds keys that hash to each of the
        3 nodes and confirms list_prefix (used by listdir) fans out to and
        merges from all of them, not just the entry node or the majority
        owner."""
        from roce.pool_node import _owner_index
        owners_needed = {addr: None for addr in [n.local_address for n in self.nodes]}
        i = 0
        while any(v is None for v in owners_needed.values()) and i < 10000:
            key = f"scatter/k{i}"
            owner = self.nodes[0].nodes[_owner_index(key, len(self.nodes[0].nodes))]
            if owners_needed.get(owner) is None:
                owners_needed[owner] = key
            i += 1
        self.assertTrue(all(owners_needed.values()), "could not find keys covering all 3 nodes")

        for key in owners_needed.values():
            self.fs.write_file(key, b"x", create_parents=True)

        results = self.nodes[1].list_prefix("scatter/")
        # list_prefix is the raw engine-level API, so it legitimately also
        # returns the "scatter" directory's own marker object -- exclude
        # it here since this test is about scatter/merge, not fs markers
        # (posix_api.listdir is what filters markers out, and that's
        # already covered by test_directory_listing_works_across_the_pool).
        result_keys = {m.key for m in results if m.key != "scatter/.dir"}
        self.assertEqual(result_keys, set(owners_needed.values()))


class ConcurrentDisjointWriteRangeTests(unittest.TestCase):
    """The bug found by running real IOR (2 ranks, single shared file,
    disjoint halves) against a real kernel FUSE mount: independent writers
    to disjoint byte ranges of the same file must all survive. write_file
    (whole-object put) cannot give this -- write_range is what fixes it in
    fs/fuse_lowlevel.py's flush path."""

    def setUp(self):
        self.fs = PooledFileSystem(MemoryEngine())

    def test_two_disjoint_ranges_both_survive(self):
        self.fs.write_file("shared.bin", b"")
        self.fs.write_range("shared.bin", 0, b"A" * 4096)
        self.fs.write_range("shared.bin", 4096, b"B" * 4096)
        got = self.fs.read_file("shared.bin")
        self.assertEqual(got, b"A" * 4096 + b"B" * 4096)

    def test_many_writers_sharing_overlapping_regions_all_land(self):
        import threading
        self.fs.write_file("shared.bin", b"\x00" * (16 * 2048))
        n = 16

        def _writer(i):
            self.fs.write_range("shared.bin", i * 1024, bytes([i + 1]) * 1024)

        threads = [threading.Thread(target=_writer, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        got = self.fs.read_file("shared.bin")
        lost = [i for i in range(n) if got[i * 1024:(i + 1) * 1024] != bytes([i + 1]) * 1024]
        self.assertEqual(lost, [])

    def test_write_range_extends_the_file(self):
        self.fs.write_file("shared.bin", b"head")
        self.fs.write_range("shared.bin", 10, b"tail")
        got = self.fs.read_file("shared.bin")
        self.assertEqual(got, b"head" + b"\x00" * 6 + b"tail")


if __name__ == "__main__":
    unittest.main()
