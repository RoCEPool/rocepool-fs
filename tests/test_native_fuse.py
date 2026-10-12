"""The native C FUSE client (roce/native/rocepool_fuse.c) mounted for real
through the kernel, against native MDT/OST servers over the softverbs
transport: POSIX behaviour through ordinary syscalls, striping across every
OST, CAS-safe concurrent writers, large directories, and interoperability
with the Python client (fs/posix_api.py DistributedFileSystem) in both
directions on the same servers."""
import errno
import hashlib
import json
import os
import signal
import random
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@unittest.skipUnless(os.path.exists("/dev/fuse") and os.geteuid() == 0 and shutil.which("gcc"),
                     "needs /dev/fuse, root and gcc")
class NativeFuseClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_fuse_client, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.bin = os.environ.get("ROCEPOOL_FUSE_BIN") or ensure_fuse_client()
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=cls.lib, PYTHONPATH=ROOT)
        cls.srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                    ROOT, "23200", "2", "4"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   text=True, env=cls.env)
        _, cls.mdts, cls.osts = cls.srv.stdout.readline().split()
        cls.tmp = tempfile.mkdtemp()
        cls.mnt = os.path.join(cls.tmp, "mnt")
        # FUSE over io_uring when this kernel offers it (fuse.enable_uring),
        # so the ring path is what the suite exercises here
        try:
            cls.uring = open("/sys/module/fuse/parameters/enable_uring").read().strip() == "Y"
        except OSError:
            cls.uring = False
        cls.fuse = subprocess.Popen([cls.bin, "--mount", cls.mnt, "--mdts", cls.mdts, "--osts", cls.osts,
                                     "--attr-ttl", "3", "--uring", "1" if cls.uring else "0"],
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=cls.env)
        line = cls.fuse.stdout.readline()
        assert line.startswith("READY"), line + cls.fuse.stdout.read()

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["umount", "-l", cls.mnt], capture_output=True)
        cls.fuse.terminate()
        try:
            cls.fuse.wait(timeout=10)
        except subprocess.TimeoutExpired:
            cls.fuse.kill()
        cls.srv.stdin.close()
        cls.srv.wait(timeout=10)
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def p(self, *parts):
        return os.path.join(self.mnt, *parts)

    def dfs(self, *args):
        out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "dfs_cli.py"), ROOT,
                              self.mdts, self.osts, *args], capture_output=True, text=True, env=self.env, timeout=120)
        line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
        assert line, out.stdout + out.stderr
        return json.loads(line[len("RESULT "):])

    # -- basics -----------------------------------------------------------
    def test_small_file_roundtrip_and_stat(self):
        os.makedirs(self.p("basic", "d"), exist_ok=True)
        with open(self.p("basic", "d", "f"), "wb") as f:
            f.write(b"hello world")
        with open(self.p("basic", "d", "f"), "rb") as f:
            self.assertEqual(f.read(), b"hello world")
        st = os.stat(self.p("basic", "d", "f"))
        self.assertEqual(st.st_size, 11)
        self.assertTrue(os.path.isdir(self.p("basic", "d")))

    def test_multi_stripe_file_integrity(self):
        data = random.Random(1).randbytes(9 * 1024 * 1024 + 12345)
        with open(self.p("big.bin"), "wb") as f:
            f.write(data)
        with open(self.p("big.bin"), "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).digest(), hashlib.sha256(data).digest())
        # unaligned reads across stripe boundaries
        with open(self.p("big.bin"), "rb") as f:
            for off in (0, 1 << 20 - 7, 3 * (1 << 20) - 1, len(data) - 100):
                f.seek(off)
                self.assertEqual(f.read(5000), data[off:off + 5000])

    def test_partial_overwrite_and_sparse_holes(self):
        path = self.p("sparse.bin")
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
        os.pwrite(fd, b"A" * 100, 0)
        os.pwrite(fd, b"B" * 10, 3 * 1024 * 1024 + 5)  # into stripe 3, leaving holes
        os.close(fd)
        with open(path, "rb") as f:
            got = f.read()
        self.assertEqual(len(got), 3 * 1024 * 1024 + 15)
        self.assertEqual(got[:100], b"A" * 100)
        self.assertEqual(got[100:3 * 1024 * 1024 + 5], b"\x00" * (3 * 1024 * 1024 + 5 - 100))
        self.assertEqual(got[-10:], b"B" * 10)
        fd = os.open(path, os.O_RDWR)
        os.pwrite(fd, b"xyz", 50)  # read-modify-write of a partial stripe
        os.close(fd)
        with open(path, "rb") as f:
            self.assertEqual(f.read(103), b"A" * 50 + b"xyz" + b"A" * 47 + b"\x00" * 3)

    def test_truncate_shrink_then_grow_reads_zeros(self):
        path = self.p("trunc.bin")
        with open(path, "wb") as f:
            f.write(b"\xff" * (2 * 1024 * 1024 + 10))
        os.truncate(path, 1000)
        os.truncate(path, 1024 * 1024 + 5)
        with open(path, "rb") as f:
            got = f.read()
        self.assertEqual(got, b"\xff" * 1000 + b"\x00" * (1024 * 1024 + 5 - 1000))
        with open(path, "wb") as f:  # O_TRUNC then write
            f.write(b"new")
        with open(path, "rb") as f:
            self.assertEqual(f.read(), b"new")

    def test_append(self):
        path = self.p("append.txt")
        for i in range(5):
            with open(path, "ab") as f:
                f.write(f"line{i}\n".encode())
        with open(path) as f:
            self.assertEqual(f.read(), "".join(f"line{i}\n" for i in range(5)))

    def test_same_fd_read_after_write(self):
        path = self.p("raw.bin")
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
        os.write(fd, b"0123456789")
        self.assertEqual(os.pread(fd, 10, 0), b"0123456789")
        os.close(fd)

    # -- directories --------------------------------------------------------
    def test_directory_errors(self):
        os.makedirs(self.p("dirs", "full"), exist_ok=True)
        open(self.p("dirs", "full", "x"), "wb").close()
        with self.assertRaises(OSError) as cm:
            os.rmdir(self.p("dirs", "full"))
        self.assertEqual(cm.exception.errno, errno.ENOTEMPTY)
        with self.assertRaises(FileExistsError):
            os.mkdir(self.p("dirs", "full"))
        with self.assertRaises(FileNotFoundError):
            open(self.p("dirs", "nope", "f"), "wb")
        with self.assertRaises(IsADirectoryError):
            open(self.p("dirs", "full"), "rb").read()
        os.unlink(self.p("dirs", "full", "x"))
        self.assertFalse(os.path.exists(self.p("dirs", "full", "x")))
        os.rmdir(self.p("dirs", "full"))
        self.assertFalse(os.path.exists(self.p("dirs", "full")))

    def test_large_directory_listing_is_complete(self):
        # > one READDIR reply's worth: served from the handle's snapshot
        os.makedirs(self.p("many"), exist_ok=True)
        names = {f"file_{i:05d}_with_a_longish_name" for i in range(1500)}
        for n in names:
            open(self.p("many", n), "wb").close()
        os.mkdir(self.p("many", "sub"))
        listed = os.listdir(self.p("many"))
        self.assertEqual(len(listed), len(names) + 1)
        self.assertEqual(set(listed), names | {"sub"})
        kinds = {e.name: e.is_dir() for e in os.scandir(self.p("many"))}
        self.assertTrue(kinds["sub"])
        self.assertFalse(kinds["file_00000_with_a_longish_name"])

    def stats(self):
        """the daemon's counters (SIGUSR1 dump, as tools/procstat.py reads them)"""
        path = f"/tmp/rocepool-fuse-{self.fuse.pid}.stats.json"
        before = 0
        try:
            before = json.load(open(path))["seq"]
        except (OSError, ValueError, KeyError):
            pass
        os.kill(self.fuse.pid, signal.SIGUSR1)
        for _ in range(1000):
            try:
                st = json.load(open(path))
                if st["seq"] > before:
                    return st
            except (OSError, ValueError, KeyError):
                pass
            time.sleep(0.01)
        self.fail("no stats dump")

    def test_write_data_goes_to_the_osts_without_a_copy(self):
        # the kernel's WRITE request buffers are registered arena memory
        # and the OST RDMA READs them in place: every body byte of a large
        # sequential write is "zero copy", none is copied into a bounce
        # buffer (ior-easy's shape: 1 MiB pieces into 1 MiB stripes)
        st0 = self.stats()
        data = random.Random(21).randbytes(8 * 1024 * 1024)
        with open(self.p("zc.bin"), "wb") as f:
            f.write(data)
        st1 = self.stats()
        zc = st1["body_bytes_zero_copy"] - st0["body_bytes_zero_copy"]
        cp = st1["body_bytes_copied"] - st0["body_bytes_copied"]
        self.assertGreaterEqual(zc, len(data))
        self.assertEqual(cp, 0, (zc, cp))
        self.assertGreater(st1["arena_bytes"], 0)
        with open(self.p("zc.bin"), "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).hexdigest(), hashlib.sha256(data).hexdigest())
        # ior-hard's shape: 47008-byte pieces, many per stripe -> gathered
        # once into arena memory, still read in place by the OST
        st0 = self.stats()
        fd = os.open(self.p("zc2.bin"), os.O_WRONLY | os.O_CREAT)
        piece = b"q" * 47008
        for i in range(60):
            os.pwrite(fd, piece, i * 47008)
        os.close(fd)
        st1 = self.stats()
        self.assertEqual(st1["body_bytes_copied"] - st0["body_bytes_copied"], 0)
        self.assertGreaterEqual(st1["body_bytes_zero_copy"] - st0["body_bytes_zero_copy"], 60 * 47008)

    def test_requests_come_through_io_uring_when_the_kernel_offers_it(self):
        if not self.uring:
            self.skipTest("kernel without FUSE over io_uring (fuse.enable_uring)")
        st0 = self.stats()
        for i in range(50):
            with open(self.p(f"ur{i}"), "wb") as f:
                f.write(b"x")
            os.stat(self.p(f"ur{i}"))
            os.unlink(self.p(f"ur{i}"))
        st1 = self.stats()
        # every one of those requests (create, write, flush, release, stat,
        # unlink...) was answered through a ring (the per-opcode counters
        # count both channels; FORGET, op 2, stays on /dev/fuse by design)
        ring = st1["uring_reqs"] - st0["uring_reqs"]
        self.assertGreaterEqual(ring, 250, (st0, st1))
        counted = sum(c for op, (c, _) in st1["ops"].items() if op != "2") - \
            sum(c for op, (c, _) in st0["ops"].items() if op != "2")
        self.assertEqual(counted, ring)

    def test_one_directory_is_striped_over_every_mdt(self):
        # fs/namespace_shard.py striped rule (the default): mdtest-hard's
        # one directory spreads over all MDTs; both clients place entries the
        # same way, and readdir merges every MDT's part in name order
        os.makedirs(self.p("striped"), exist_ok=True)
        for i in range(40):
            open(self.p("striped", f"n{i:03d}"), "wb").close()
        per_mdt = self.dfs("mdtcounts", "striped")
        self.assertEqual(sum(per_mdt), 40)
        self.assertTrue(all(c > 0 for c in per_mdt), per_mdt)
        self.assertEqual(os.listdir(self.p("striped")), sorted(f"n{i:03d}" for i in range(40)))
        self.dfs("write", "striped/frompy", "10")
        self.assertIn("frompy", os.listdir(self.p("striped")))
        os.unlink(self.p("striped", "frompy"))
        self.assertEqual(self.dfs("attr", "striped/frompy"), None)
        with self.assertRaises(OSError):
            os.rmdir(self.p("striped"))  # not empty: the count spans every MDT
        for i in range(40):
            os.unlink(self.p("striped", f"n{i:03d}"))
        os.rmdir(self.p("striped"))

    # -- concurrency ----------------------------------------------------------
    def test_concurrent_disjoint_writers_share_stripes_without_loss(self):
        path = self.p("shared.bin")
        open(path, "wb").close()
        block = 100_000  # not stripe aligned: many writers per stripe
        blocks = {i: bytes([65 + i % 26]) * block for i in range(24)}

        def w(i):
            fd = os.open(path, os.O_WRONLY)
            os.pwrite(fd, blocks[i], i * block)
            os.close(fd)
        ts = [threading.Thread(target=w, args=(i,)) for i in range(24)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        time.sleep(0.1)
        with open(path, "rb") as f:
            self.assertEqual(f.read(), b"".join(blocks[i] for i in range(24)))

    # -- layout & interop with the Python client --------------------------------
    def test_stripes_land_on_every_ost(self):
        with open(self.p("spread.bin"), "wb") as f:
            f.write(b"s" * (8 * 1024 * 1024))
        counts = self.dfs("ostcounts")
        self.assertTrue(all(c > 0 for c in counts), counts)

    def test_python_client_reads_what_native_client_wrote(self):
        data = random.Random(7).randbytes(3 * 1024 * 1024 + 777)
        os.makedirs(self.p("interop"), exist_ok=True)
        with open(self.p("interop", "from_c.bin"), "wb") as f:
            f.write(data)
        self.assertEqual(self.dfs("sha", "interop/from_c.bin"), hashlib.sha256(data).hexdigest())

    def test_native_client_reads_what_python_client_wrote(self):
        want = self.dfs("write", "interop2/from_py.bin", str(5 * 1024 * 1024 + 3))
        with open(self.p("interop2", "from_py.bin"), "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).hexdigest(), want)

    # -- review regressions ---------------------------------------------------
    def test_ftruncate_drops_this_fds_cached_stripes(self):
        path = self.p("ftrunc.bin")
        with open(path, "wb") as f:
            f.write(b"A" * 4096)
        fd = os.open(path, os.O_RDWR)
        self.assertEqual(os.pread(fd, 4096, 0), b"A" * 4096)  # caches stripe 0
        os.ftruncate(fd, 0)
        os.ftruncate(fd, 4096)
        self.assertEqual(os.pread(fd, 4096, 0), b"\x00" * 4096)
        os.close(fd)

    def test_fsync_makes_buffered_writes_visible_to_other_clients(self):
        # The writer is its own process holding the fd open, so the checks
        # (subprocesses of THIS process) never hold a copy of it: every
        # close of an fd copy sends FUSE_FLUSH, which would push the data
        # out without fsync.
        path = self.p("fsync.bin")
        writer = subprocess.Popen([sys.executable, "-c", (
            "import os, random, sys\n"
            f"fd = os.open({path!r}, os.O_CREAT | os.O_WRONLY, 0o644)\n"
            "os.write(fd, random.Random(3).randbytes(200_000))\n"
            "print('WROTE', flush=True); sys.stdin.readline()\n"
            "os.fsync(fd)\n"
            "print('SYNCED', flush=True); sys.stdin.readline()\n"
            "os.close(fd)\n")], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(writer.stdout.readline().strip(), "WROTE")
            empty = hashlib.sha256(b"").hexdigest()
            self.assertEqual(self.dfs("sha", "fsync.bin"), empty)  # still buffered in the daemon
            writer.stdin.write("go\n")
            writer.stdin.flush()
            self.assertEqual(writer.stdout.readline().strip(), "SYNCED")
            self.assertEqual(self.dfs("sha", "fsync.bin"),
                             hashlib.sha256(random.Random(3).randbytes(200_000)).hexdigest())
        finally:
            writer.stdin.write("go\n")
            writer.stdin.flush()
            writer.wait(timeout=30)

    def test_open_revalidates_a_striped_file_another_client_extended(self):
        # close-to-open across clients (IOR's shared file across nodes): this
        # mount cached the file's size (TTL 3 s); the Python client -- another
        # node -- extends and closes it; an open here must see the new size
        # and read the tail, not hit "EOF" at the stale size
        path = self.p("shared.bin")
        with open(path, "wb") as f:
            f.write(b"a" * 100_000)  # striped (> the inline limit)
        self.assertEqual(os.stat(path).st_size, 100_000)  # cached now
        self.dfs("extend", "shared.bin", str(3 * 1024 * 1024))
        time.sleep(1.2)  # past the kernel's own attribute cache (1 s), within the daemon's TTL (3 s)
        fd = os.open(path, os.O_RDONLY)
        try:
            self.assertEqual(os.fstat(fd).st_size, 3 * 1024 * 1024)
            self.assertEqual(len(os.pread(fd, 47008, 3 * 1024 * 1024 - 47008)), 47008)
        finally:
            os.close(fd)

    def test_files_still_work_after_the_kernel_forgets_inodes(self):
        os.makedirs(self.p("forget"), exist_ok=True)
        for i in range(200):
            with open(self.p("forget", f"f{i}"), "wb") as f:
                f.write(str(i).encode())
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("2")  # evicts dentries/inodes -> FUSE_FORGET / BATCH_FORGET
        for i in range(0, 200, 7):
            with open(self.p("forget", f"f{i}"), "rb") as f:
                self.assertEqual(f.read(), str(i).encode())
        self.assertEqual(len(os.listdir(self.p("forget"))), 200)

    def test_concurrent_block_writers_then_stat_sees_full_size(self):
        # IOR shared-file pattern: 8 writers, disjoint 4 MiB blocks, then
        # stat/read right away (out-of-order set_size replies must not shrink
        # the cached size)
        for rnd in range(3):
            path = self.p(f"ior_like_{rnd}.bin")
            open(path, "wb").close()
            blk = 4 * 1024 * 1024

            def w(i):
                fd = os.open(path, os.O_WRONLY)
                os.pwrite(fd, bytes([i + 1]) * blk, i * blk)
                os.close(fd)
            ts = [threading.Thread(target=w, args=(i,)) for i in range(8)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            self.assertEqual(os.stat(path).st_size, 8 * blk)
            with open(path, "rb") as f:
                self.assertEqual(len(f.read()), 8 * blk)

    # -- thick KV through the kernel -----------------------------------------
    def test_small_files_are_inline_in_the_mdt(self):
        os.makedirs(self.p("tiny"), exist_ok=True)
        for i in range(50):
            with open(self.p("tiny", f"f{i}"), "wb") as f:
                f.write(b"x" * 3901)  # mdtest-hard's file size
        for i in range(0, 50, 9):
            with open(self.p("tiny", f"f{i}"), "rb") as f:
                self.assertEqual(f.read(), b"x" * 3901)
        self.assertEqual(self.dfs("attr", "tiny/f0"), {"size": 3901, "inline": True})
        self.assertEqual(self.dfs("stripes", "tiny/f0"), 0)
        os.unlink(self.p("tiny", "f0"))
        self.assertFalse(os.path.exists(self.p("tiny", "f0")))

    def test_growing_file_spills_and_both_clients_agree(self):
        data = random.Random(11).randbytes(3 * 1024 * 1024 + 5)
        path = self.p("grow.bin")
        with open(path, "wb") as f:
            f.write(data[:1000])  # inline
        self.assertTrue(self.dfs("attr", "grow.bin")["inline"])
        with open(path, "ab") as f:
            f.write(data[1000:])  # past the limit: spill + stripes
        self.assertEqual(self.dfs("attr", "grow.bin"), {"size": len(data), "inline": False})
        with open(path, "rb") as f:
            self.assertEqual(f.read(), data)
        self.assertEqual(self.dfs("sha", "grow.bin"), hashlib.sha256(data).hexdigest())

    def test_python_written_inline_file_reads_through_native_client(self):
        want = self.dfs("write", "pysmall.txt", "3000")
        self.assertTrue(self.dfs("attr", "pysmall.txt")["inline"])
        with open(self.p("pysmall.txt"), "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).hexdigest(), want)

    def test_strided_reads_use_exact_ranges_and_are_correct(self):
        data = random.Random(5).randbytes(6 * 1024 * 1024)
        path = self.p("strided.bin")
        with open(path, "wb") as f:
            f.write(data)
        fd = os.open(path, os.O_RDONLY)
        try:
            for i in reversed(range(0, 120, 7)):  # backwards, 47008-byte pieces (ior-hard)
                off = i * 47008
                self.assertEqual(os.pread(fd, 47008, off), data[off:off + 47008])
        finally:
            os.close(fd)

    def test_concurrent_writers_across_the_spill_through_the_kernel(self):
        for rnd in range(4):
            path = self.p(f"spillrace{rnd}.bin")
            open(path, "wb").close()
            blk = 5000
            big = b"\xee" * (2 * 1024 * 1024)

            def small(i):
                fd = os.open(path, os.O_WRONLY)
                os.pwrite(fd, bytes([i + 1]) * blk, i * blk)
                os.close(fd)

            def large():
                fd = os.open(path, os.O_WRONLY)
                os.pwrite(fd, big, 4 * blk)
                os.close(fd)
            ts = [threading.Thread(target=small, args=(i,)) for i in range(4)] + [threading.Thread(target=large)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            with open(path, "rb") as f:
                got = f.read()
            self.assertEqual(got, b"".join(bytes([i + 1]) * blk for i in range(4)) + big, f"round {rnd}")

    def test_stale_striped_attrs_meet_a_file_frozen_mid_spill(self):
        # this mount caches "striped" (attr TTL 3 s); meanwhile another client
        # recreates the path inline and leaves it frozen. The extending write
        # gets FROZEN from set_size: finish the spill, then redo the write.
        path = self.p("stalefz.bin")
        with open(path, "wb") as f:
            f.write(b"x" * 40000)  # > the 32 KiB inline limit: striped
        fd = os.open(path, os.O_WRONLY)
        try:
            os.fstat(fd)
            self.dfs("refreeze", "stalefz.bin", "small")
            os.pwrite(fd, b"-appended", 50000)
        finally:
            os.close(fd)
        want = b"small" + b"\x00" * (50000 - 5) + b"-appended"
        self.assertEqual(self.dfs("sha", "stalefz.bin"), hashlib.sha256(want).hexdigest())
        self.assertEqual(self.dfs("attr", "stalefz.bin"), {"size": len(want), "inline": False})

    def test_new_file_with_a_run_bigger_than_one_frame(self):
        # a new file starts inline; its first flush here is one 40 MiB run
        # (> the 16 MiB frame): it must spill, not ship the run to the MDT
        path = self.p("bigrun.bin")
        data = random.Random(9).randbytes(40 * 1024 * 1024)
        with open(path, "wb") as f:
            for i in range(0, len(data), 1 << 20):
                f.write(data[i:i + (1 << 20)])
        with open(path, "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).digest(), hashlib.sha256(data).digest())
        self.assertEqual(self.dfs("attr", "bigrun.bin"), {"size": len(data), "inline": False})


class NativeFuseClientTieredTests(NativeFuseClientTests):
    """The same suite with the OSTs on the capacity tier (roce/native/
    ost_tier.c: a file here, an NVMe on site) and a 4 MiB RAM cache each:
    whole-stripe writes go from the NIC's buffer to the device directly,
    partial ones through the RAM copy, reads of evicted stripes from the
    device straight into the reply."""

    @classmethod
    def setUpClass(cls):
        cls.tier_tmp = tempfile.mkdtemp()
        os.environ["ROCEPOOL_OST_TIER"] = f"file:{cls.tier_tmp}/tier{{n}}:1"
        os.environ["ROCEPOOL_OST_RAM_CACHE_MIB"] = "4"
        try:
            super().setUpClass()
        finally:
            del os.environ["ROCEPOOL_OST_TIER"], os.environ["ROCEPOOL_OST_RAM_CACHE_MIB"]

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        shutil.rmtree(cls.tier_tmp, ignore_errors=True)

    def test_stripes_were_written_direct_and_evicted(self):
        from roce.protocol import Frame, Op
        from roce.roce_transport import RoCETransport
        os.environ["ROCE_TRANSPORT_LIB"] = self.lib
        t = RoCETransport()
        with open(self.p("tiered.bin"), "wb") as f:
            f.write(b"\x77" * (6 << 20))
        stats = [json.loads(t.send_request(a, Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)
                 for a in self.osts.split(",")]
        self.assertTrue(all(s["tier"] == "file" for s in stats), stats)
        self.assertGreaterEqual(sum(s["direct_writes"] for s in stats), 6, stats)
        self.assertTrue(all(s["ram_cache_bytes"] <= s["ram_cache_budget"] + (1 << 20) + 4096 for s in stats), stats)
        with open(self.p("tiered.bin"), "rb") as f:
            self.assertEqual(f.read(), b"\x77" * (6 << 20))


if __name__ == "__main__":
    unittest.main()
