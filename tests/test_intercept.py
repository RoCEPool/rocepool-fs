"""The LD_PRELOAD interception library (roce/native/rocepool_intercept.c):
an unmodified program's open/read/write/stat/readdir on paths under
ROCEPOOL_PREFIX are served by the native client core in the program's own
process -- no mount, no kernel, no /dev/fuse. Checked from a Python host
(a demanding one: its own descriptors, dlopen, importlib's stat storms), a
C probe for the libc entry points Python does not reach, the real IOR and
mdtest through mpirun when they are built, and interoperability with the
Python client on the same servers."""
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class _Cluster(unittest.TestCase):
    """Native MDT/OST servers over softverbs, and the library's environment."""
    port = 23300
    server_env: dict = {}

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=cls.lib, PYTHONPATH=ROOT)
        cls.srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                    ROOT, str(cls.port), "2", "4"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   text=True, env=dict(cls.env, **cls.server_env))
        _, cls.mdts, cls.osts = cls.srv.stdout.readline().split()
        cls.tmp = tempfile.mkdtemp()
        # the prefix need not exist on the host: it is a name, not a mount
        cls.prefix = os.path.join(cls.tmp, "rp")
        cls.penv = dict(cls.env, LD_PRELOAD=cls.icp, ROCEPOOL_PREFIX=cls.prefix, ROCEPOOL_MDTS=cls.mdts,
                        ROCEPOOL_OSTS=cls.osts, ROCEPOOL_ATTR_TTL="3")

    @classmethod
    def tearDownClass(cls):
        cls.srv.stdin.close()
        cls.srv.wait(timeout=10)
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def py(self, code: str, timeout=120) -> str:
        """Runs `code` in a Python process under the library; returns stdout."""
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=self.penv,
                             timeout=timeout, cwd=self.tmp)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        return out.stdout

    def dfs(self, *args):
        out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "dfs_cli.py"), ROOT,
                              self.mdts, self.osts, *args], capture_output=True, text=True, env=self.env, timeout=120)
        line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
        assert line, out.stdout + out.stderr
        return json.loads(line[len("RESULT "):])



@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class InlineBudgetTests(_Cluster):
    """An MDT holds inline file bytes up to its budget
    (ROCEPOOL_MDT_INLINE_BUDGET_MIB); beyond it new small files are striped
    over the OSTs, and every byte still reads back. The first EFA IO500
    failed in mdtest-hard because its millions of 3901-byte files grew the
    MDTs without bound."""
    port = 23420
    server_env = {"ROCEPOOL_MDT_INLINE_BUDGET_MIB": "1"}  # per MDT; 2 MDTs

    def test_small_files_beyond_the_budget_go_to_the_osts(self):
        out = self.py(f"""
import hashlib, os
P = {self.prefix!r}
os.mkdir(P + "/hard")
for i in range(1500):  # 1500 x 3901 B = 5.6 MiB > 2 x 1 MiB
    with open(f"{{P}}/hard/f{{i}}", "wb") as f: f.write(hashlib.sha256(str(i).encode()).digest() * 121 + b"x" * 29)
bad = [i for i in range(1500)
       if open(f"{{P}}/hard/f{{i}}", "rb").read() != hashlib.sha256(str(i).encode()).digest() * 121 + b"x" * 29]
print(len(bad))
""", timeout=600)
        self.assertEqual(out.strip(), "0")
        inline = sum(self.dfs("attr", f"hard/f{i}")["inline"] for i in (0, 1, 2, 1497, 1498, 1499))
        self.assertGreater(inline, 0)      # the first ones fit
        self.assertLess(inline, 6)         # the last ones did not
        self.assertEqual(self.dfs("attr", "hard/f1499")["size"], 3901)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class BigDirectoryTests(_Cluster):
    port = 23390
    server_env = {"ROCE_MAX_FRAME_BYTES": str(256 << 10)}

    def test_directory_larger_than_one_reply_is_listed_whole(self):
        """A directory whose listing exceeds a frame is read page by page.
        The site's IO500 find saw none of mdtest-hard's 5.4 M files (one
        directory, ~75 MB of entries per listing against a 16 MiB frame):
        here 256 KiB frames and 9000 entries do the same in miniature."""
        code = f"""
import os
P = {self.prefix!r}
os.mkdir(P + "/huge")
for i in range(9000):
    os.close(os.open(P + "/huge/file.mdtest.%d.%d" % (i % 7, i), os.O_CREAT | os.O_WRONLY, 0o644))
print(len(os.listdir(P)))
"""
        list_code = f"""
import os
names = os.listdir({self.prefix!r} + "/huge")
print(len(names), len(set(names)))
"""
        env = dict(self.penv, ROCE_MAX_FRAME_BYTES=str(64 << 10))  # ~600 entries per page
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300, cwd=self.tmp,
                             env=env)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        before = sum(s["readdir_copied"] for s in self.dfs("mdtstats"))
        for _ in range(2):  # two listings: each copies the directory once per MDT
            out = subprocess.run([sys.executable, "-c", list_code], capture_output=True, text=True, timeout=300,
                                 cwd=self.tmp, env=env)
            self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
            self.assertEqual(out.stdout.split(), ["9000", "9000"])
        copied = sum(s["readdir_copied"] for s in self.dfs("mdtstats")) - before
        # paged by copying everything after the cursor, ~8 pages per MDT
        # cost ~4x this (IO500 find on 13 M entries per MDT: 1130 s)
        self.assertLessEqual(copied, 2 * 9000 + 100)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class StridedFlushTests(_Cluster):
    """IOR hard's pattern from one rank: 47008-byte pieces a stride apart,
    so no two buffered writes touch. A flush sent them one at a time, each
    a WRITE_AT and an MDT set_size round trip (0.68 GiB/s on site); now all
    of a flush's stripe writes are in flight together and the MDT hears one
    size update."""
    port = 23440

    def test_strided_pieces_flush_together_and_read_back(self):
        set_size = lambda: sum(s.get("by_op", {}).get("3", 0) for s in self.dfs("mdtstats"))
        before = set_size()
        out = self.py(f"""
import hashlib, os
P = {self.prefix!r}
piece, nranks, nseg = 47008, 8, 200
fd = os.open(P + "/strided", os.O_CREAT | os.O_WRONLY, 0o644)
for k in range(nseg):  # this rank (3 of 8) writes its piece of every segment
    os.pwrite(fd, hashlib.sha256(b"%d" % k).digest() * 1469, (k * nranks + 3) * piece)
os.close(fd)
with open(P + "/strided", "rb") as f:
    data = f.read()
ok = len(data) == (nseg - 1) * nranks * piece + 4 * piece
for k in range(nseg):
    at = (k * nranks + 3) * piece
    ok = ok and data[at:at + piece] == hashlib.sha256(b"%d" % k).digest() * 1469
    ok = ok and data[at - piece:at] == bytes(piece)  # the holes read as zeros
print(ok)
""", timeout=300)
        self.assertEqual(out.strip(), "True")
        # 200 pieces in one flush (16 MiB buffer): a handful of size
        # updates (create, the flush), not one per piece
        self.assertLessEqual(set_size() - before, 4)

    def test_unlink_is_one_mdt_round_trip(self):
        """A delete used to look the file up first (two MDT requests per
        file): mdtest's delete phases ran at half the MDTs' request rate."""
        self.py(f"""
import os
P = {self.prefix!r}
os.mkdir(P + "/del")
for i in range(300):
    os.close(os.open(P + "/del/f%d" % i, os.O_CREAT | os.O_WRONLY, 0o644))
with open(P + "/del/big", "wb") as f: f.write(os.urandom(3 << 20))  # striped: its objects go too
os.mkdir(P + "/del/sub")
""")
        ops = lambda: [sum(s.get("by_op", {}).get(k, 0) for s in self.dfs("mdtstats")) for k in ("1", "4")]
        before = ops()
        # lookups by RPC, as on EFA without RDMA read in hardware (here a
        # lookup would be a one-sided read the MDT never counts)
        penv, self.penv = self.penv, dict(self.penv, ROCEPOOL_DIRECT_LOOKUP="0")
        self.addCleanup(setattr, self, "penv", penv)
        out = self.py(f"""
import errno, os
P = {self.prefix!r}
for i in range(300):
    os.unlink(P + "/del/f%d" % i)
os.unlink(P + "/del/big")
def err(fn):
    try: fn(); return 0
    except OSError as e: return e.errno
print(err(lambda: os.unlink(P + "/del/sub")) == errno.EISDIR, err(lambda: os.unlink(P + "/del/nope")) == errno.ENOENT,
      sorted(os.listdir(P + "/del")))
""")
        self.assertEqual(out.strip(), "True True ['sub']")
        lookups, unlinks = [a - b for a, b in zip(ops(), before)]
        self.assertGreaterEqual(unlinks, 301)
        self.assertLess(lookups, 30, (lookups, unlinks))  # was one per file
        self.assertEqual(self.dfs("stripes", "del/big"), 0)  # its stripe objects went too

    def test_open_and_read_of_a_small_file_is_one_mdt_round_trip(self):
        """mdtest-hard-read opens a 3901-byte (inline) file and reads it: a
        lookup, then an inline read. The open now fetches the content with
        the attributes, and the read is served from it."""
        self.py(f"""
import os
P = {self.prefix!r}
os.mkdir(P + "/rd")
for i in range(200):
    with open(P + "/rd/f%d" % i, "wb") as f: f.write(bytes([i % 251]) * 3901)
with open(P + "/rd/big", "wb") as f: f.write(bytes(range(256)) * 12288)  # 3 MiB, striped
""")
        ops = lambda: sum(s.get("by_op", {}).get(k, 0) for s in self.dfs("mdtstats") for k in ("1", "8"))
        before = ops()
        penv, self.penv = self.penv, dict(self.penv, ROCEPOOL_DIRECT_LOOKUP="0")  # lookups by RPC, as on EFA
        self.addCleanup(setattr, self, "penv", penv)
        out = self.py(f"""
import errno, os
P = {self.prefix!r}
ok = all(open(P + "/rd/f%d" % i, "rb").read() == bytes([i % 251]) * 3901 for i in range(200))
fd = os.open(P + "/rd/f7", os.O_RDONLY)  # pread inside and past the end, fstat
ok = ok and os.pread(fd, 10, 3895) == bytes([7]) * 6 and os.pread(fd, 10, 5000) == b"" and os.fstat(fd).st_size == 3901
os.close(fd)
ok = ok and open(P + "/rd/big", "rb").read() == bytes(range(256)) * 12288
try: os.open(P + "/rd/nope", os.O_RDONLY); ok = False
except OSError as e: ok = ok and e.errno == errno.ENOENT
d = os.open(P + "/rd", os.O_RDONLY); os.close(d)  # a directory opens as before
r = os.open(P + "/rd/f9", os.O_RDONLY)  # the reader's content goes with a truncate here
os.truncate(P + "/rd/f9", 5)
ok = ok and os.read(r, 100) == bytes([9]) * 5
print(ok)
""")
        self.assertEqual(out.strip(), "True")
        self.assertLess(ops() - before, 200 * 1.2)  # was 2 per small file (lookup + inline read)

    def test_create_is_one_mdt_round_trip(self):
        """open(O_CREAT) looked the new file up (ENOENT) before creating it:
        mdtest-easy-write took two MDT requests per file, mdtest-hard-write
        three (with the inline write), and ran at 1/2 and 1/3 of the MDTs'
        request rate. Now the create goes first, and the semantics stay."""
        self.py(f"import os; os.mkdir({self.prefix!r} + '/cr')")
        ops = lambda: {k: sum(s.get("by_op", {}).get(k, 0) for s in self.dfs("mdtstats")) for k in ("1", "2", "7")}
        before = ops()
        penv, self.penv = self.penv, dict(self.penv, ROCEPOOL_DIRECT_LOOKUP="0")  # lookups by RPC, as on EFA
        self.addCleanup(setattr, self, "penv", penv)
        out = self.py(f"""
import errno, os
P = {self.prefix!r} + "/cr"
for i in range(200):  # mdtest-easy-write: create, close
    os.close(os.open(P + "/e%d" % i, os.O_CREAT | os.O_RDWR, 0o644))
for i in range(200):  # mdtest-hard-write: create, 3901 bytes, close
    fd = os.open(P + "/h%d" % i, os.O_CREAT | os.O_RDWR, 0o644); os.write(fd, b"h" * 3901); os.close(fd)
def err(fn):
    try: fn(); return 0
    except OSError as e: return e.errno
ok = err(lambda: os.open(P + "/h1", os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)) == errno.EEXIST
fd = os.open(P + "/h2", os.O_CREAT | os.O_RDWR, 0o644); os.close(fd)  # an existing file keeps its bytes
ok = ok and open(P + "/h2", "rb").read() == b"h" * 3901
fd = os.open(P + "/h3", os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o644); os.close(fd)
ok = ok and os.stat(P + "/h3").st_size == 0
ok = ok and err(lambda: os.open(P, os.O_CREAT | os.O_RDWR, 0o644)) == errno.EISDIR
ok = ok and err(lambda: os.open(P + "/nodir/x", os.O_CREAT | os.O_RDWR, 0o644)) == errno.ENOENT
ok = ok and os.open(P + "/new", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644) > 0
ok = ok and len(os.listdir(P)) == 401
print(ok)
""")
        self.assertEqual(out.strip(), "True")
        after = ops()
        d = {k: after[k] - before[k] for k in after}
        # 400 files: 400 creates and 200 inline writes; a few lookups (the
        # directory, the checks above) -- was 400 lookups more
        self.assertLess(d["1"] + d["2"] + d["7"], 400 + 200 + 60, d)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class SlowOSTTests(_Cluster):
    """An OST that takes 6.5 s per write (a slow drive with a queue in front
    of it, as on site) is slow, not dead: the writes must succeed. With the
    old 5 s RPC timeout each one became EIO and IOR aborted."""
    port = 23380
    server_env = {"ROCEPOOL_TEST_OST_WRITE_DELAY_MS": "6500"}

    def test_slow_writes_complete(self):
        out = self.py(f"""
import os, time
P = {self.prefix!r}
os.makedirs(P + "/slow", exist_ok=True)
data = os.urandom(3 << 20)
t0 = time.monotonic()
with open(P + "/slow/f", "wb") as f: f.write(data)
dt = time.monotonic() - t0
print(open(P + "/slow/f", "rb").read() == data, dt > 6)
""", timeout=300)
        self.assertEqual(out.split(), ["True", "True"])


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class InterceptLibraryTests(_Cluster):
    def test_roundtrip_stat_listdir_unlink(self):
        out = self.py(f"""
import os
P = {self.prefix!r}
os.makedirs(P + "/t1/d", exist_ok=True)
with open(P + "/t1/d/f", "wb") as f: f.write(b"hello world")
print(open(P + "/t1/d/f", "rb").read() == b"hello world")
print(os.stat(P + "/t1/d/f").st_size, sorted(os.listdir(P + "/t1")), sorted(os.listdir(P + "/t1/d")))
print(os.path.isdir(P + "/t1/d"), os.path.isfile(P + "/t1/d/f"), os.path.exists(P + "/t1/nope"))
os.unlink(P + "/t1/d/f"); print(os.path.exists(P + "/t1/d/f"))
os.rmdir(P + "/t1/d"); print(os.listdir(P + "/t1"))
""")
        self.assertEqual(out.splitlines(), ["True", "11 ['d'] ['f']", "True True False", "False", "[]"])
        self.assertFalse(os.path.exists(self.prefix), "nothing touched the host filesystem")

    def test_multi_stripe_file_integrity_and_seeks(self):
        out = self.py(f"""
import os, random, hashlib
P = {self.prefix!r}
data = random.Random(7).randbytes(9 * 1024 * 1024 + 12345)
with open(P + "/big.bin", "wb") as f: f.write(data)
got = open(P + "/big.bin", "rb").read()
print(hashlib.sha256(got).hexdigest() == hashlib.sha256(data).hexdigest(), len(got))
with open(P + "/big.bin", "rb") as f:
    for off in (0, (1 << 20) - 7, 3 * (1 << 20) - 1, len(data) - 100):
        f.seek(off); assert f.read(5000) == data[off:off + 5000], off
    f.seek(0, 2); print(f.tell())
print(hashlib.sha256(data).hexdigest())
""")
        lines = out.splitlines()
        self.assertEqual(lines[0], "True 9449529")
        self.assertEqual(lines[1], "9449529")
        # the Python client on the same servers reads the same bytes
        self.assertEqual(self.dfs("sha", "big.bin"), lines[2])
        self.assertEqual(self.dfs("stripes", "big.bin"), 10)

    def test_pwrite_sparse_holes_and_rmw(self):
        out = self.py(f"""
import os
P = {self.prefix!r}
fd = os.open(P + "/sparse.bin", os.O_CREAT | os.O_RDWR, 0o644)
os.pwrite(fd, b"A" * 100, 0)
os.pwrite(fd, b"B" * 10, 3 * 1024 * 1024 + 5)
os.close(fd)
got = open(P + "/sparse.bin", "rb").read()
print(len(got), got[:100] == b"A" * 100, got[100:3 * 1024 * 1024 + 5] == bytes(3 * 1024 * 1024 + 5 - 100), got[-10:] == b"B" * 10)
fd = os.open(P + "/sparse.bin", os.O_RDWR); os.pwrite(fd, b"xyz", 50); os.close(fd)
print(open(P + "/sparse.bin", "rb").read(60)[48:54])
""")
        self.assertEqual(out.splitlines(), [f"{3 * 1024 * 1024 + 15} True True True", "b'AAxyzA'"])

    def test_excl_append_dup_truncate_and_fd_position(self):
        out = self.py(f"""
import os
P = {self.prefix!r}
fd = os.open(P + "/x", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644); os.write(fd, b"abc"); os.close(fd)
try:
    os.open(P + "/x", os.O_CREAT | os.O_EXCL | os.O_WRONLY); print("no EEXIST")
except FileExistsError: print("EEXIST")
fd = os.open(P + "/x", os.O_WRONLY | os.O_APPEND); os.write(fd, b"def")
fd2 = os.dup(fd); os.write(fd2, b"g"); os.close(fd); os.write(fd2, b"h"); os.close(fd2)
print(open(P + "/x", "rb").read())
fd = os.open(P + "/x", os.O_RDWR)
print(os.read(fd, 2), os.read(fd, 2), os.lseek(fd, 1, os.SEEK_SET), os.read(fd, 3))
os.ftruncate(fd, 4); print(os.fstat(fd).st_size); os.close(fd)
print(open(P + "/x", "rb").read())
os.truncate(P + "/x", 6); print(open(P + "/x", "rb").read())
with open(P + "/x", "wb") as f: pass   # O_TRUNC
print(os.stat(P + "/x").st_size)
try:
    open(P + "/missing/x", "wb"); print("no ENOENT")
except FileNotFoundError: print("ENOENT")
try:
    os.rmdir(P); print("rmdir root")
except OSError as e: print(type(e).__name__)
""")
        self.assertEqual(out.splitlines(), ["EEXIST", "b'abcdefgh'", "b'ab' b'cd' 1 b'bcd'", "4", "b'abcd'",
                                            "b'abcd\\x00\\x00'", "0", "ENOENT", "OSError"])

    def test_c_probe_covers_the_libc_entry_points(self):
        exe = os.path.join(self.tmp, "intercept_probe")
        subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-D_FORTIFY_SOURCE=2",
                        os.path.join(ROOT, "tests", "fakes", "intercept_probe.c"), "-o", exe], check=True)
        out = subprocess.run([exe, self.prefix], capture_output=True, text=True, env=self.penv, timeout=120)
        self.assertEqual(out.stdout.strip(), "OK", out.stdout + out.stderr)
        self.assertEqual(out.returncode, 0)

    def test_scandir_and_large_directory_listing(self):
        out = self.py(f"""
import os
P = {self.prefix!r}
os.mkdir(P + "/many")
for i in range(1500):
    with open(P + "/many/f%04d" % i, "wb") as f: f.write(b"x")
names = sorted(os.listdir(P + "/many"))
print(len(names), names[0], names[-1])
with os.scandir(P + "/many") as it:
    ents = list(it)
print(len(ents), all(e.is_file() for e in ents[:50]), sum(e.stat().st_size for e in ents))
""")
        self.assertEqual(out.splitlines(), ["1500 f0000 f1499", "1500 True 1500"])
        # ... striped over every MDT, like the FUSE client's directories
        counts = self.dfs("mdtcounts", "many")
        self.assertEqual(len(counts), 2)
        self.assertTrue(all(c > 500 for c in counts), counts)

    def test_interop_with_the_python_client_both_ways(self):
        sha = self.dfs("write", "from_py.bin", str(2 * 1024 * 1024 + 99))
        out = self.py(f"""
import hashlib
P = {self.prefix!r}
print(hashlib.sha256(open(P + "/from_py.bin", "rb").read()).hexdigest())
data = bytes(range(256)) * 4097
with open(P + "/from_icp.bin", "wb") as f: f.write(data)
print(hashlib.sha256(data).hexdigest())
""")
        got, sha2 = out.split()
        self.assertEqual(got, sha)
        self.assertEqual(self.dfs("sha", "from_icp.bin"), sha2)

    def test_small_files_are_inline_and_rename_is_exdev(self):
        out = self.py(f"""
import os
P = {self.prefix!r}
with open(P + "/tiny", "wb") as f: f.write(b"3901 bytes" * 390)
try:
    os.rename(P + "/tiny", "/tmp/elsewhere"); print("renamed")
except OSError as e: print(e.errno == 18)
""")
        self.assertEqual(out.strip(), "True")
        self.assertEqual(self.dfs("attr", "tiny"), {"size": 3900, "inline": True})

    def test_fork_child_keeps_working(self):
        out = self.py(f"""
import os
P = {self.prefix!r}
with open(P + "/forked", "wb") as f: f.write(b"parent")
pid = os.fork()
if pid == 0:
    with open(P + "/forked", "ab") as f: f.write(b"+child")
    os._exit(0)
os.waitpid(pid, 0)
print(open(P + "/forked", "rb").read())
""")
        self.assertEqual(out.strip(), "b'parent+child'")

    def test_paths_outside_the_prefix_are_untouched(self):
        out = self.py(f"""
import os, tempfile
d = tempfile.mkdtemp()
with open(d + "/real", "w") as f: f.write("kernel")
print(open(d + "/real").read(), os.listdir(d), os.path.isdir("/"))
os.unlink(d + "/real"); os.rmdir(d); print(os.path.exists(d))
""")
        self.assertEqual(out.splitlines(), ["kernel ['real'] True", "False"])


_PFIND = os.path.join(ROOT, ".bench_cache", "io500_src", "build", "pfind", "pfind")
_MPIRUN = os.path.join(ROOT, ".bench_cache", "mpich", "bin", "mpirun")


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class DirectoryPositionTests(_Cluster):
    """telldir/seekdir on our directory streams. A directory larger than
    every MDT's first page is "paged": read as its MDTs' listings one after
    another, fetched from any position, with positions spread over
    [0, LONG_MAX) -- IO500 find's pfind -H 1 halves a reader's position
    range to give the other half to an idle rank, which seeks there. Before
    this, one rank read mdtest-hard's whole directory (9.4 M entries on 4
    nodes) and find stayed at ~1 M entries/s from 2 nodes to 4."""
    port = 23450

    def positions(self, d, n_files, env):
        code = f"""
import ctypes, os
P = {self.prefix!r}
os.makedirs(P + "/{d}", exist_ok=True)
for i in range({n_files}):
    os.close(os.open(P + "/{d}/file.mdtest.%d.%d" % (i % 5, i), os.O_CREAT | os.O_WRONLY, 0o644))
libc = ctypes.CDLL(None)
libc.opendir.restype = ctypes.c_void_p
libc.opendir.argtypes = [ctypes.c_char_p]
libc.readdir.restype = ctypes.c_void_p
libc.readdir.argtypes = [ctypes.c_void_p]
libc.telldir.restype = ctypes.c_long
libc.telldir.argtypes = [ctypes.c_void_p]
libc.seekdir.argtypes = [ctypes.c_void_p, ctypes.c_long]
libc.closedir.argtypes = [ctypes.c_void_p]
NAME_OFF = 19  # struct dirent: d_ino 8, d_off 8, d_reclen 2, d_type 1
def name(ent):
    return ctypes.string_at(ent + NAME_OFF).decode()
LONG_MAX = 2**63 - 1
d = libc.opendir((P + "/{d}").encode())
seq = []
while True:
    pos = libc.telldir(d)
    ent = libc.readdir(d)
    if not ent:
        break
    seq.append((pos, name(ent)))
end = libc.telldir(d)
poss = [p for p, _ in seq]
print(len(seq), len(set(n for _, n in seq)), poss == sorted(poss), len(set(poss)) == len(poss), end == LONG_MAX)
# every recorded position leads back to its entry, also from another stream
e = libc.opendir((P + "/{d}").encode())
bad = 0
for k in list(range(0, len(seq), 97)) + [len(seq) - 1]:
    libc.seekdir(e, seq[k][0])
    if libc.telldir(e) != seq[k][0] or name(libc.readdir(e)) != seq[k][1]:
        bad += 1
print(bad)
# pfind's split: positions between entries go to the next one; past the
# end, telldir answers LONG_MAX (>= any position sought)
ranges, cuts = [], sorted({{0, LONG_MAX // 3, LONG_MAX // 2, LONG_MAX - 5, LONG_MAX}})
seen = []
for a, b in zip(cuts, cuts[1:]):
    f = libc.opendir((P + "/{d}").encode())
    if a:
        libc.seekdir(f, a)
    while True:
        pos = libc.telldir(f)
        if pos < a:
            seen.append("BACKWARDS")
            break
        if pos >= b:
            break
        ent = libc.readdir(f)
        if not ent:
            break
        seen.append(name(ent))
    libc.closedir(f)
print(len(seen), len(set(seen)), sorted(seen) == sorted(n for _, n in seq))
libc.closedir(d); libc.closedir(e)
"""
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300, cwd=self.tmp,
                             env=dict(self.penv, **env))
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        return out.stdout.split("\n")

    def test_paged_directory_positions_split_and_seek(self):
        lines = self.positions("paged", 3000, {"ROCEPOOL_READDIR_FIRST": "4096", "ROCEPOOL_READDIR_PAGE": "8192"})
        self.assertEqual(lines[:3], ["3000 3000 True True True", "0", "3000 3000 True"])

    def test_small_directory_positions_split_and_seek(self):
        lines = self.positions("small", 40, {})
        self.assertEqual(lines[:3], ["40 40 True True True", "0", "40 40 True"])

    def test_a_change_is_seen_by_the_next_listing(self):
        """Positional readers share one snapshot per MDT, but only while the
        directory is unchanged."""
        env = dict(self.penv, ROCEPOOL_READDIR_FIRST="4096", ROCEPOOL_READDIR_PAGE="8192")
        code = f"""
import os
P = {self.prefix!r}
os.makedirs(P + "/chg", exist_ok=True)
for i in range(1500):
    os.close(os.open(P + "/chg/f%05d" % i, os.O_CREAT | os.O_WRONLY, 0o644))
print(len(os.listdir(P + "/chg")))
os.unlink(P + "/chg/f00007")
os.close(os.open(P + "/chg/new", os.O_CREAT | os.O_WRONLY, 0o644))
names = os.listdir(P + "/chg")
print(len(names), "f00007" in names, "new" in names)
with open(P + "/chg/f00009", "wb") as f:
    f.write(b"x" * 123)
print([e.stat().st_size for e in os.scandir(P + "/chg") if e.name == "f00009"])
"""
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300, cwd=self.tmp,
                             env=env)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertEqual(out.stdout.split("\n")[:3], ["1500", "1500 False True", "[123]"])


@unittest.skipUnless(shutil.which("gcc") and os.path.exists(_PFIND) and os.path.exists(_MPIRUN),
                     "needs gcc, and pfind + mpirun built in .bench_cache")
class PfindSplitTests(_Cluster):
    """The unmodified pfind of the IO500 app, through mpirun with the
    library preloaded, with and without -H 1 (IO500's
    pfind-parallelize-single-dir-access-using-hashing): the same files are
    found either way, and with it the big directory is read by every rank."""
    port = 23470

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.ts = os.path.join(cls.tmp, "timestampfile")
        with open(cls.ts, "w"):
            pass
        os.utime(cls.ts, (1, 1))   # every file is newer
        code = f"""
import os
P = {cls.prefix!r}
os.makedirs(P + "/find/hard", exist_ok=True)
for r in range(3):
    os.makedirs(P + "/find/easy/r%d" % r, exist_ok=True)
    for i in range(300):
        os.close(os.open(P + "/find/easy/r%d/file.mdtest.%d.%d" % (r, r, i), os.O_CREAT | os.O_WRONLY, 0o644))
data = b"h" * 3901
for i in range(6000):
    with open(P + "/find/hard/file.mdtest.%d.%d" % (i % 4, i), "wb") as f:
        f.write(data)
"""
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=600, cwd=cls.tmp,
                             env=cls.penv)
        assert out.returncode == 0, out.stdout + out.stderr
        cls.expect = sum(1 for i in range(6000) if "01" in "file.mdtest.%d.%d" % (i % 4, i))

    def pfind(self, *extra):
        env = dict(self.penv, ROCEPOOL_READDIR_FIRST="4096", ROCEPOOL_READDIR_PAGE="16384")
        out = subprocess.run([_MPIRUN, "-n", "4", _PFIND, self.prefix + "/find", "-newer", self.ts, "-size", "3901c",
                              "-name", "*01*", "-C", "-P", "-q", "10000", "-M", "100", *extra],
                             capture_output=True, text=True, env=env, timeout=600)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        return out.stdout

    def matched(self, out):
        import re
        m = re.search(r"MATCHED (\d+)/(\d+)", out)
        self.assertTrue(m, out)
        return int(m.group(1)), int(m.group(2))

    def test_same_files_found_with_and_without_the_split(self):
        total = 6000 + 3 * 300
        plain = self.pfind()
        split = self.pfind("-H", "1")
        self.assertEqual(self.matched(plain), (self.expect, total), plain)
        self.assertEqual(self.matched(split), (self.expect, total), split)


@unittest.skipUnless(shutil.which("gcc") and os.path.exists(os.path.join(ROOT, ".bench_cache", "ior_src", "src", "ior"))
                     and os.path.exists(os.path.join(ROOT, ".bench_cache", "mpich", "bin", "mpirun")),
                     "needs the IOR/MPICH built by tools/bench_ior.py")
class InterceptIORTests(_Cluster):
    """Real upstream IOR and mdtest, unmodified, through mpirun, with the
    library preloaded into every rank and no filesystem mounted."""
    port = 23320

    def mpi(self, prog, *args, timeout=300):
        mpirun = os.path.join(ROOT, ".bench_cache", "mpich", "bin", "mpirun")
        bin_ = os.path.join(ROOT, ".bench_cache", "ior_src", "src", prog)
        out = subprocess.run([mpirun, "-n", "2", bin_, *args], capture_output=True, text=True, env=self.penv,
                             timeout=timeout)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertNotIn("ERROR", out.stdout)
        self.assertNotIn("WARNING", out.stdout)
        return out.stdout

    def test_ior_shared_and_file_per_process_verify(self):
        out = self.mpi("ior", "-a", "POSIX", "-b", "4m", "-t", "1m", "-w", "-r", "-W", "-R", "-G", "42",
                       "-o", self.prefix + "/ior_shared")
        self.assertIn("\nread ", out)
        self.py(f"import os; os.makedirs({self.prefix!r} + '/fpp', exist_ok=True)")
        out = self.mpi("ior", "-a", "POSIX", "-F", "-b", "2m", "-t", "256k", "-w", "-r", "-W", "-R",
                       "-o", self.prefix + "/fpp/f")
        self.assertIn("\nread ", out)

    def test_ior_hard_shape(self):
        out = self.mpi("ior", "-a", "POSIX", "-b", "47008", "-t", "47008", "-s", "50", "-w", "-r", "-W", "-R",
                       "-o", self.prefix + "/ior_hard")
        self.assertIn("\nread ", out)

    def test_mdtest_hard_shape(self):
        self.py(f"import os; os.makedirs({self.prefix!r} + '/md', exist_ok=True)")
        out = self.mpi("mdtest", "-d", self.prefix + "/md", "-n", "100", "-F", "-w", "3901", "-e", "3901", "-N", "1")
        for op in ("File creation", "File stat", "File read", "File removal"):
            self.assertIn(op, out)
        self.assertEqual(self.dfs("mdtcounts", "md"), [0, 0])  # mdtest removed its tree


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class StrideReadaheadTests(_Cluster):
    """stride readahead (rocepool_fs.c ra_read): a reader stepping through a
    file in equal pieces at a constant stride (IOR hard's read) gets the next
    pieces read ahead -- and always the file's bytes: across stripe edges, at
    the end of the file, when the pattern changes, after its own writes"""
    port = 23490

    def run_logged(self, code, env=None):
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                             env=dict(self.penv, ROCEPOOL_INTERCEPT_LOG="1", **(env or {})), timeout=300, cwd=self.tmp)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        st = [l for l in out.stderr.splitlines() if " stats " in l][-1]
        return out.stdout.strip(), {k: int(v) for k, v in (w.split("=") for w in st.split()[2:] if "=" in w)}

    def test_strided_reads_are_the_files_bytes(self):
        code = f"""
import os, random
P = {self.prefix!r}
r = random.Random(5)
data = r.randbytes((3 << 20) + 777777)  # 1 MiB stripes; pieces cross their edges
with open(P + "/s", "wb") as f: f.write(data)
fd = os.open(P + "/s", os.O_RDWR)
def check(off, n):
    got = os.pread(fd, n, off)
    want = bytes(data[off:off + n])
    assert got == want, (off, n, len(got))
piece, nr = 47008, 8
for me in (3, 0, 7):  # IOR hard: one rank's pieces, to the end of the file
    off = me * piece
    while off < len(data): check(off, piece); off += nr * piece
for k in range(0, 60, 3): check(k * 50000, 12345)  # another stride and length
for k in range(80, 20, -2): check(k * 40000, 9000)  # backwards: no readahead, exact bytes
data = bytearray(data)
for k in range(38):  # its own writes between strided reads are seen (inside the file)
    off = (k * 2 + 1) * piece
    check(off, piece)
    if k % 7 == 3:
        nxt = ((k + 1) * 2 + 1) * piece
        b = r.randbytes(piece)
        os.pwrite(fd, b, nxt)
        data[nxt:nxt + piece] = b
os.close(fd)
print("ok")
"""
        out, st = self.run_logged(code)
        self.assertEqual(out, "ok")
        self.assertGreater(st["stride_ra_hits"], 50, st)
        self.assertLessEqual(st["stride_ra_hits"], st["stride_ra_issued"], st)

    def test_truncate_by_another_fd_drops_what_was_read_ahead(self):
        code = f"""
import os
P = {self.prefix!r}
piece, nr = 47008, 4
with open(P + "/t", "wb") as f: f.write(b"a" * (64 * nr * piece))
fd = os.open(P + "/t", os.O_RDONLY)
for k in range(8): assert os.pread(fd, piece, k * nr * piece) == b"a" * piece
os.truncate(P + "/t", 0)  # this client: invalidate_open_caches
with open(P + "/t", "r+b") as f: f.write(b"b" * (64 * nr * piece))
for k in range(8, 16): assert os.pread(fd, piece, k * nr * piece) == b"b" * piece, k
os.close(fd)
print("ok")
"""
        self.assertEqual(self.run_logged(code)[0], "ok")

    def test_off_reads_one_piece_at_a_time(self):
        code = f"""
import os
P = {self.prefix!r}
with open(P + "/o", "wb") as f: f.write(os.urandom(4 << 20))
fd = os.open(P + "/o", os.O_RDONLY)
for k in range(40): os.pread(fd, 47008, k * 4 * 47008)
os.close(fd)
print("ok")
"""
        _, st = self.run_logged(code, {"ROCEPOOL_STRIDE_RA": "0"})
        self.assertEqual(st["stride_ra_issued"], 0, st)
