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
names = os.listdir(P + "/huge")
print(len(names), len(set(names)))
"""
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300, cwd=self.tmp,
                             env=dict(self.penv, ROCE_MAX_FRAME_BYTES=str(256 << 10)))
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertEqual(out.stdout.split(), ["9000", "9000"])


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
