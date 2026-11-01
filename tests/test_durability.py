"""Durability (roce/native/wal.c, IO500 rule 6) and close-to-open
consistency across clients (IO500 rule 2).

With ROCEPOOL_MDT_LOG / ROCEPOOL_OST_LOG every change a server acknowledges
is in its log -- and an OST's object bytes on its tier -- and fdatasync'd
before the reply. Checked by killing the servers (SIGKILL: nothing flushed
on the way out) after the clients' closes returned, starting them again on
the same logs and tiers, and reading everything back."""
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _stats(lib: str, addr: str) -> dict:
    os.environ["ROCE_TRANSPORT_LIB"] = lib
    from roce.protocol import Frame, Op
    from roce.roce_transport import RoCETransport
    r = RoCETransport().send_request(addr, Frame(op=Op.STATS, request_id=1, key="", value=b""))
    return json.loads(r.value)


class _Servers:
    """MDTs + OSTs in one process (tests/fakes/native_cluster.py), on logs
    and tiers under `d`; start() again after kill() = a restart on the same
    state."""

    def __init__(self, lib, d, port, nm=2, no=3, durable=True, env=None):
        self.lib, self.d, self.port, self.nm, self.no, self.durable = lib, d, port, nm, no, durable
        self.env = dict(env or {})  # the servers' extra environment, from the next start()
        self.p = None

    def start(self):
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT,
                   ROCEPOOL_OST_TIER=f"file:{self.d}/ost{{n}}.tier:1", ROCEPOOL_OST_RAM_CACHE_MIB="16", **self.env)
        if self.durable:
            env.update(ROCEPOOL_OST_LOG=f"{self.d}/ost{{n}}.log", ROCEPOOL_MDT_LOG=f"{self.d}/mdt{{n}}.log")
        self.p = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                                   str(self.port), str(self.nm), str(self.no)], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        line = self.p.stdout.readline()
        if not line.startswith("READY"):
            raise RuntimeError("servers did not start: " + line + self.p.stderr.read()[-2000:])
        _, self.mdts, self.osts = line.split()

    def kill(self):
        self.p.kill()   # SIGKILL: no flush, no clean shutdown
        self.p.wait(timeout=30)
        self.err = self.p.stderr.read()
        time.sleep(0.3)

    def stop(self):
        if self.p and self.p.poll() is None:
            self.p.stdin.close()
            self.p.wait(timeout=30)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class DurableRestartTests(unittest.TestCase):
    port = 24100
    SERVER_ENV: dict = {}
    CLIENT_ENV: dict = {}

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.srv = _Servers(self.lib, self.tmp, self.port, env=self.SERVER_ENV)
        self.srv.start()
        self.addCleanup(self.srv.stop)
        self.prefix = os.path.join(self.tmp, "rp")

    def py(self, code: str, env: dict | None = None) -> str:
        e = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                 ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                 ROCEPOOL_ATTR_TTL="3", **self.CLIENT_ENV, **(env or {}))
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=e, timeout=300, cwd=self.tmp)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        return out.stdout

    def test_everything_acknowledged_survives_a_crash(self):
        # small inline files, striped files, a shared file written in
        # unaligned pieces by two processes, a truncate, deletes, a rmdir
        write = f"""
import hashlib, os, random
P = {self.prefix!r}
os.makedirs(P + "/d/sub")
os.makedirs(P + "/gone")
r = random.Random(7)
sums = {{}}
for i in range(40):
    b = r.randbytes(3901)
    with open(P + "/d/small%02d" % i, "wb") as f: f.write(b)
    sums["d/small%02d" % i] = hashlib.sha256(b).hexdigest()
for i, n in enumerate((1, 3 << 20, (5 << 20) + 12345)):
    b = r.randbytes(n)
    with open(P + "/d/sub/big%d" % i, "wb") as f: f.write(b)
    sums["d/sub/big%d" % i] = hashlib.sha256(b).hexdigest()
b = r.randbytes(7 << 20)
with open(P + "/d/trunc", "wb") as f: f.write(b)
os.truncate(P + "/d/trunc", 1000003)
sums["d/trunc"] = hashlib.sha256(b[:1000003]).hexdigest()
for i in range(0, 40, 4): os.unlink(P + "/d/small%02d" % i); del sums["d/small%02d" % i]
os.rmdir(P + "/gone")
print(repr(sums))
"""
        sums = eval(self.py(write).strip())
        # ior-hard's shape: 47008-byte pieces of one shared file, two writers
        piece = 47008
        hard = f"""
import os, sys
P = {self.prefix!r}
me = int(sys.argv[1])
fd = os.open(P + "/d/shared", os.O_WRONLY | os.O_CREAT, 0o644)
for k in range(me, 200, 2):
    os.pwrite(fd, bytes([k % 251]) * {piece}, k * {piece})
os.close(fd)
"""
        e = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                 ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts, **self.CLIENT_ENV)
        ps = [subprocess.Popen([sys.executable, "-c", hard, str(m)], env=e, cwd=self.tmp) for m in (0, 1)]
        self.assertEqual([p.wait(timeout=300) for p in ps], [0, 0])
        sums["d/shared"] = hashlib.sha256(b"".join(bytes([k % 251]) * piece for k in range(200))).hexdigest()

        st = [_stats(self.lib, a) for a in self.srv.mdts.split(",") + self.srv.osts.split(",")]
        self.assertTrue(all(s["durable"] for s in st), st)
        self.assertTrue(all(s["log"]["commits"] > 0 and s["log"]["append_failures"] == 0 for s in st), st)
        # the OFI transport (EFA) holds a durable write's reply until its
        # commit while the thread goes on; the RC transport's handler waits
        from roce.native.build_lib import backend
        held = [s["log"]["replies_held"] for s in st]
        if backend() == "ofi":
            self.assertTrue(all(h > 0 for h in held), held)
        else:
            self.assertEqual(held, [0] * len(st))

        self.srv.kill()
        self.srv.start()

        read = f"""
import hashlib, os
P = {self.prefix!r}
got = {{}}
for top, dirs, files in os.walk(P):
    for n in files:
        p = os.path.join(top, n)
        got[os.path.relpath(p, P)] = hashlib.sha256(open(p, "rb").read()).hexdigest()
print(repr(got))
print(sorted(os.listdir(P)), sorted(os.listdir(P + "/d/sub")))
print(os.stat(P + "/d/trunc").st_size, os.stat(P + "/d/shared").st_size)
"""
        lines = self.py(read).strip().splitlines()
        self.assertEqual(eval(lines[0]), sums)
        self.assertEqual(lines[1], "['d'] ['big0', 'big1', 'big2']")
        self.assertEqual(lines[2], f"1000003 {200 * piece}")
        # and the restarted servers keep working (and logging)
        self.py(f"open({self.prefix!r} + '/d/after', 'wb').write(b'x' * 99)")
        self.srv.kill()
        self.srv.start()
        self.assertEqual(self.py(f"print(len(open({self.prefix!r} + '/d/after', 'rb').read()))").strip(), "99")

    def test_a_torn_log_tail_is_dropped(self):
        """A crash in the middle of writing a record: the record was never
        acknowledged; replay stops before it and the log continues."""
        self.py(f"import os; os.makedirs({self.prefix!r} + '/t'); open({self.prefix!r} + '/t/a', 'wb').write(b'a' * 10)")
        self.srv.kill()
        with open(os.path.join(self.tmp, "mdt0.log"), "ab") as f:
            f.write(b"\x40\x00\x00\x00junk")   # a header promising 64 bytes, then 4
        with open(os.path.join(self.tmp, "mdt1.log"), "ab") as f:
            f.write(b"\x40\x00\x00\x00junk")
        self.srv.start()
        self.assertEqual(self.py(f"print(open({self.prefix!r} + '/t/a', 'rb').read())").strip(), "b'aaaaaaaaaa'")
        self.py(f"open({self.prefix!r} + '/t/b', 'wb').write(b'b')")
        self.srv.kill()
        self.srv.start()
        self.assertEqual(self.py(f"import os; print(sorted(os.listdir({self.prefix!r} + '/t')))").strip(), "['a', 'b']")


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class TierParityDurableTests(unittest.TestCase):
    """Every OST op against the Python reference (tests/fakes/native_scenario.py)
    with the log on: durability changes when a reply goes out, not what it says."""

    def test_every_op_matches_the_reference_with_the_log(self):
        from roce.native.build_lib import ensure_softverbs_lib
        lib = ensure_softverbs_lib()
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        env = dict(os.environ, ROCE_TRANSPORT_LIB=lib, ROCEPOOL_OST_TIER=f"file:{d}/tier{{n}}:1",
                   ROCEPOOL_OST_RAM_CACHE_MIB="1", ROCEPOOL_OST_LOG=f"{d}/ost{{n}}.log")
        out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "native_scenario.py"), ROOT, "24300"],
                             env=env, capture_output=True, text=True, timeout=300)
        line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
        self.assertTrue(line, out.stdout[-2000:] + out.stderr[-2000:])
        res = json.loads(line[len("RESULT "):])
        self.assertNotIn("error", res, res.get("error"))
        self.assertEqual(res["ost_native"], res["ost_python"])
        self.assertEqual(res["cas_native"], 8 * 40)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class CloseToOpenTests(unittest.TestCase):
    """IO500 rule 2: data one client wrote and closed is read by another.
    Process A looks first (caching "no such file" and an old size for the
    TTL), B writes and closes, then A opens and reads -- within the TTL."""
    port = 24200

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()
        cls.tmp = tempfile.mkdtemp()
        cls.srv = _Servers(cls.lib, cls.tmp, cls.port, durable=False)
        cls.srv.start()
        cls.prefix = os.path.join(cls.tmp, "rp")

    @classmethod
    def tearDownClass(cls):
        cls.srv.stop()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def env(self):
        return dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                    ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                    ROCEPOOL_ATTR_TTL="30")   # a long TTL: only revalidation can make A see B's writes

    def test_reader_sees_another_clients_closed_writes(self):
        P = self.prefix
        subprocess.run([sys.executable, "-c", f"import os; os.makedirs({P!r} + '/c'); open({P!r} + '/c/grow', 'wb').write(b'g' * 10)"],
                       env=self.env(), check=True, timeout=120)
        reader = f"""
import os, sys
P = {P!r}
for n in ("new_small", "new_big", "grow"):   # cache what is there now
    try: os.stat(P + "/c/" + n)
    except FileNotFoundError: pass
print("LOOKED", flush=True)
sys.stdin.readline()                           # B writes meanwhile
out = []
for n in ("new_small", "new_big", "grow"):
    rw = os.open(P + "/c/" + n, os.O_RDWR)       # the generic open path first: nothing refreshed the cache yet
    out.append(os.fstat(rw).st_size)
    os.close(rw)
    fd = os.open(P + "/c/" + n, os.O_RDONLY)
    data = b""
    while True:
        b = os.read(fd, 1 << 20)
        if not b: break
        data += b
    os.close(fd)
    out.append(len(data))
print(out, flush=True)
"""
        a = subprocess.Popen([sys.executable, "-c", reader], env=self.env(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             text=True)
        self.assertEqual(a.stdout.readline().strip(), "LOOKED")
        writer = f"""
P = {P!r}
open(P + "/c/new_small", "wb").write(b"s" * 3901)
open(P + "/c/new_big", "wb").write(b"b" * (3 << 20))
with open(P + "/c/grow", "ab") as f: f.write(b"g" * (2 << 20))
"""
        subprocess.run([sys.executable, "-c", writer], env=self.env(), check=True, timeout=120)
        a.stdin.write("go\n")
        a.stdin.flush()
        got = a.stdout.readline().strip()
        a.wait(timeout=60)
        self.assertEqual(got, str([3901, 3901, 3 << 20, 3 << 20, (2 << 20) + 10, (2 << 20) + 10]))


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(shutil.which("gcc") and os.geteuid() == 0, "needs gcc and root (a small tmpfs)")
class FullTierTests(unittest.TestCase):
    """A full tier (or log) is ENOSPC for the program, like a full disk --
    it used to be EIO, and a test that ran out of disk failed as 'tier
    write failed' with no reason given"""
    port = 24400

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def check(self, durable: bool):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        subprocess.run(["mount", "-t", "tmpfs", "-o", "size=8m", "tmpfs", d], check=True)
        self.addCleanup(subprocess.run, ["umount", "-l", d])
        srv = _Servers(self.lib, d, self.port + (10 if durable else 0), nm=1, no=1, durable=durable)
        srv.start()
        self.addCleanup(srv.stop)
        code = f"""
import os
fd = os.open({d!r} + "/rp/big", os.O_WRONLY | os.O_CREAT, 0o644)
try:
    os.write(fd, b"x" * (32 << 20))
    os.close(fd)
    print("no error")
except OSError as x:
    print(x.errno)
"""
        e = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                 ROCEPOOL_PREFIX=d + "/rp", ROCEPOOL_MDTS=srv.mdts, ROCEPOOL_OSTS=srv.osts)
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=e, timeout=120)
        import errno
        self.assertEqual(out.stdout.strip(), str(errno.ENOSPC), out.stdout + out.stderr)

    def test_full_tier_is_enospc(self):
        self.check(durable=False)

    def test_full_tier_is_enospc_when_durable(self):
        self.check(durable=True)



ASYNC = {"ROCEPOOL_MDT_ASYNC": "1", "ROCEPOOL_MDT_RECOVERY_S": "1"}


class DurableRestartAsyncTests(DurableRestartTests):
    """the same crashes with Lustre-style commit of creates and unlinks:
    everything a process was replied to is durable once it exited"""
    port = 24500
    SERVER_ENV = ASYNC
    CLIENT_ENV = {"ROCEPOOL_MDT_ASYNC": "1"}

    def test_everything_acknowledged_survives_a_crash(self):
        super().test_everything_acknowledged_survives_a_crash()
        st = [_stats(self.lib, a) for a in self.srv.mdts.split(",")]
        self.assertTrue(all("mdt_async" in x for x in st), st)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class AsyncReplayTests(unittest.TestCase):
    """An MDT that crashes with creates and unlinks it replied to but had
    not committed: the clients still alive replay them, the MDT applies
    them in their original order (transno) at the end of its recovery
    window. A slow log (ROCEPOOL_TEST_LOG_DELAY_MS) keeps them uncommitted."""
    port = 24600

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.srv = _Servers(self.lib, self.tmp, self.port, nm=2, no=2, env=dict(ASYNC, ROCEPOOL_MDT_RECOVERY_S="2"))
        self.srv.start()
        self.addCleanup(self.srv.stop)
        self.prefix = os.path.join(self.tmp, "rp")

    def env(self):
        return dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                    ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                    ROCEPOOL_MDT_ASYNC="1", ROCEPOOL_ATTR_TTL="0", ROCEPOOL_INTERCEPT_LOG="1")

    def client(self, code):
        """a client process that runs `code` in steps: each "step()" waits
        for a line on stdin; returns the Popen"""
        pre = "import os, sys\nP = %r\ndef step(tag):\n    print(tag, flush=True)\n    sys.stdin.readline()\n" % self.prefix
        return subprocess.Popen([sys.executable, "-c", pre + code], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, env=self.env(), cwd=self.tmp)

    def crash_and_restart(self):
        self.srv.kill()
        self.srv.env.pop("ROCEPOOL_TEST_LOG_DELAY_MS", None)
        self.srv.start()

    def listing(self, d):
        code = f"import os; print(sorted(os.listdir({os.path.join(self.prefix, d)!r})))"
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=self.env(), timeout=120)
        self.assertEqual(out.returncode, 0, out.stderr)
        return eval(out.stdout.strip())

    def test_uncommitted_creates_and_unlinks_are_replayed(self):
        # the directory and the first files committed, then a slow log
        c = self.client("""
os.makedirs(P + "/d")
for i in range(10): open(P + "/d/old%02d" % i, "w").close()
os.fsync(os.open(P + "/d/old00", os.O_RDONLY))  # a barrier: all of it durable
step("ready")
for i in range(30): open(P + "/d/f%02d" % i, "w").close()
for i in range(0, 10, 2): os.unlink(P + "/d/old%02d" % i)
step("changed")
for i in range(30, 35): open(P + "/d/f%02d" % i, "w").close()  # meets the recovery, replays, goes on
print(sorted(os.listdir(P + "/d")))
""")
        self.assertEqual(c.stdout.readline().strip(), "ready")
        # from here on nothing commits for 5 s: everything that follows is
        # replied to and uncommitted when the MDTs die
        self.srv.kill()
        self.srv.env["ROCEPOOL_TEST_LOG_DELAY_MS"] = "5000"
        self.srv.start()
        c.stdin.write("\n"); c.stdin.flush()
        self.assertEqual(c.stdout.readline().strip(), "changed")
        st = [_stats(self.lib, a) for a in self.srv.mdts.split(",")]
        self.assertGreater(sum(x["mdt_async"]["early_replies"] for x in st), 0, st)
        self.crash_and_restart()
        c.stdin.write("\n"); c.stdin.flush()
        out, err = c.communicate(timeout=180)
        want = sorted([f"f{i:02d}" for i in range(35)] + [f"old{i:02d}" for i in range(1, 10, 2)])
        self.assertEqual(eval(out.strip().splitlines()[-1]), want, err[-2000:])
        self.assertRegex(err, r"mdt_replayed=[1-9]")
        self.assertIn("mdt_lost=0", err)
        st = [_stats(self.lib, a) for a in self.srv.mdts.split(",")]
        self.assertGreater(sum(x["mdt_async"]["replays_applied"] for x in st), 0, st)
        # the client exited (its barrier): all of it durable on its own now
        self.crash_and_restart()
        self.assertEqual(self.listing("d"), want)

    def test_two_clients_changes_replay_in_their_order(self):
        a = self.client("""
os.makedirs(P + "/d")
os.fsync(os.open(P + "/d", os.O_RDONLY))
step("dir")
open(P + "/d/x", "w").close()
open(P + "/d/z", "w").close()
step("a")
os.stat(P + "/d")       # meets the recovery: replays
print(sorted(os.listdir(P + "/d")))
""")
        self.assertEqual(a.stdout.readline().strip(), "dir")
        self.srv.kill()
        self.srv.env["ROCEPOOL_TEST_LOG_DELAY_MS"] = "5000"
        self.srv.start()
        a.stdin.write("\n"); a.stdin.flush()
        self.assertEqual(a.stdout.readline().strip(), "a")
        b = self.client("""
os.unlink(P + "/d/x")   # after A's create of x
open(P + "/d/y", "w").close()
os.unlink(P + "/d/z")
open(P + "/d/z", "w").close()   # z again, after A's
step("b")
os.stat(P + "/d")
print(sorted(os.listdir(P + "/d")))
""")
        self.assertEqual(b.stdout.readline().strip(), "b")
        self.crash_and_restart()
        for p in (a, b):
            p.stdin.write("\n"); p.stdin.flush()
        outs = [p.communicate(timeout=180) for p in (a, b)]
        for out, err in outs:
            self.assertEqual(eval(out.strip().splitlines()[-1]), ["y", "z"], err[-2000:])
            self.assertIn("mdt_lost=0", err)
        self.crash_and_restart()
        self.assertEqual(self.listing("d"), ["y", "z"])


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class FreedSlabTests(unittest.TestCase):
    """A slab an uncommitted change frees must not be reused before that
    change is durable: a crash replays the old object onto it. A was
    acknowledged; A's overwrite (A moves, its slab freed) and B (would take
    that slab) are both in flight, the log slowed so neither commits, when
    the OST dies: A must read as acknowledged."""
    port = 24700

    def test_an_acknowledged_object_survives_its_uncommitted_overwrite(self):
        import threading
        from roce.native.build_lib import ensure_softverbs_lib
        lib = ensure_softverbs_lib()
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        srv = _Servers(lib, tmp, self.port, nm=1, no=1)
        srv.start()
        self.addCleanup(srv.stop)
        os.environ["ROCE_TRANSPORT_LIB"] = lib
        from roce.remote_engine import RemoteEngine
        from roce.roce_transport import RoCETransport
        e = RemoteEngine(srv.osts, RoCETransport())
        a1 = b"\x41" * 4000
        e.put("A", a1)  # durable: its reply waited for the log
        srv.kill()
        srv.env["ROCEPOOL_TEST_LOG_DELAY_MS"] = "4000"
        srv.start()
        e = RemoteEngine(srv.osts, RoCETransport())
        self.assertEqual(e.get("A"), a1)

        def bg(key, data):
            try:
                RemoteEngine(srv.osts, RoCETransport()).put(key, data)
            except Exception:  # noqa: BLE001 -- the OST dies under it
                pass
        t1 = threading.Thread(target=bg, args=("A", b"\x61" * 20000), daemon=True)
        t1.start()
        time.sleep(0.5)
        t2 = threading.Thread(target=bg, args=("B", b"\x42" * 4000), daemon=True)
        t2.start()
        time.sleep(0.5)
        srv.kill()
        srv.env.pop("ROCEPOOL_TEST_LOG_DELAY_MS")
        srv.start()
        e = RemoteEngine(srv.osts, RoCETransport())
        self.assertEqual(e.get("A"), a1)


class DurableRestartWriteBehindTests(DurableRestartTests):
    """the same crashes with a 1 MiB write buffer: every file over it is
    sent in batches by the write-behind thread while the program goes on,
    and close still returns only once all of them are durable"""
    port = 24900
    CLIENT_ENV = {"ROCEPOOL_DIRTY_CAP_MIB": "1", "ROCEPOOL_WRITE_BEHIND": "1"}
    test_a_torn_log_tail_is_dropped = None  # nothing write-behind changes there


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class WriteBehindTests(unittest.TestCase):
    """write-behind (handle_write -> wb_start): batches reach the OSTs in
    write order, a read or stat through the fd sees every earlier write,
    and an error of a background batch is the program's at close"""
    port = 25000

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def run_client(self, d, srv, code, env):
        e = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                 ROCEPOOL_PREFIX=d + "/rp", ROCEPOOL_MDTS=srv.mdts, ROCEPOOL_OSTS=srv.osts,
                 ROCEPOOL_DIRTY_CAP_MIB="1", **env)
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=e, timeout=300)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        return out.stdout.strip()

    def test_overlapping_writes_across_batches_keep_their_order(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        srv = _Servers(self.lib, d, self.port, nm=1, no=2, env={"ROCEPOOL_TEST_LOG_DELAY_MS": "1"})
        srv.start()
        self.addCleanup(srv.stop)
        # random overlapping writes over 6 MiB, many 1 MiB batches; reads,
        # fstat and lseek through the fd in between check against a model
        code = r'''
import hashlib, os, random, sys
P = os.environ["ROCEPOOL_PREFIX"]
name = P + "/f" + os.environ["WB"]
r = random.Random(11)
size = 6 << 20
model = bytearray(size)
fd = os.open(name, os.O_RDWR | os.O_CREAT, 0o644)
os.pwrite(fd, b"\0" * size, 0)
for i in range(400):
    off, n = r.randrange(size - 300000), r.randrange(1, 300000)
    b = bytes([i % 251 + 1]) * n
    os.pwrite(fd, b, off)
    model[off:off + n] = b
    if i % 97 == 50:
        o = r.randrange(size - 4096)
        assert os.pread(fd, 4096, o) == bytes(model[o:o + 4096]), ("same-fd read", i)
        assert os.fstat(fd).st_size == size
os.fsync(fd)
os.close(fd)
print(hashlib.sha256(model).hexdigest())
'''
        read = r'''
import hashlib, os
print(hashlib.sha256(open(os.environ["ROCEPOOL_PREFIX"] + "/f" + os.environ["WB"], "rb").read()).hexdigest())
'''
        want = {}
        for wb in ("1", "0"):
            want[wb] = self.run_client(d, srv, code, {"WB": wb, "ROCEPOOL_WRITE_BEHIND": wb})
            self.assertEqual(self.run_client(d, srv, read, {"WB": wb}), want[wb], f"write-behind={wb}")
        self.assertEqual(want["1"], want["0"])  # the same random writes
        srv.kill()  # and every batch was durable when close returned
        srv.start()
        for wb in ("1", "0"):
            self.assertEqual(self.run_client(d, srv, read, {"WB": wb}), want[wb], f"after a crash, write-behind={wb}")

    def test_a_failed_background_batch_is_enospc(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        subprocess.run(["mount", "-t", "tmpfs", "-o", "size=8m", "tmpfs", d], check=True)
        self.addCleanup(subprocess.run, ["umount", "-l", d])
        srv = _Servers(self.lib, d, self.port + 10, nm=1, no=1)
        srv.start()
        self.addCleanup(srv.stop)
        # 1 MiB writes: the tier fills under a batch in flight; the error
        # comes back on a later write or at close, never lost
        code = r'''
import errno, os
assert os.environ["ROCEPOOL_WRITE_BEHIND"] == "1"
fd = os.open(os.environ["ROCEPOOL_PREFIX"] + "/big", os.O_WRONLY | os.O_CREAT, 0o644)
try:
    for i in range(32):
        os.write(fd, b"x" * (1 << 20))
    os.close(fd)
    print("no error")
except OSError as x:
    print(x.errno)
'''
        import errno
        self.assertEqual(self.run_client(d, srv, code, {"ROCEPOOL_WRITE_BEHIND": "1"}), str(errno.ENOSPC))


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class AsyncTierReadTests(unittest.TestCase):
    """async tier reads (rocepool_servers.c tier_rd_submit): a READ_AT of an
    object only on the tier is read by the OST's reader threads while the
    transport holds its reply (OFI; the RC transport cannot hold one and
    reads in the handler as before) -- and is always that object's bytes,
    also when it is deleted and its slab reused while the read is out"""
    port = 25100

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import backend, ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()
        cls.ofi = backend() == "ofi"

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, ignore_errors=True)

    def client(self, srv, code, **env):
        e = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                 ROCEPOOL_PREFIX=self.d + "/rp", ROCEPOOL_MDTS=srv.mdts, ROCEPOOL_OSTS=srv.osts,
                 ROCEPOOL_DIRECT_READ="0", ROCE_OFI_CLIENT_EPS="16", **env)  # every read a READ_AT (as on AWS)
        return subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=e)

    def done(self, p):
        out, err = p.communicate(timeout=300)
        self.assertEqual(p.returncode, 0, out + err)
        return out.strip()

    def ost_stats(self, srv):
        st = [_stats(self.lib, a) for a in srv.osts.split(",")]
        return {k: sum(x.get(k, 0) for x in st) for k in ("async_tier_reads", "async_tier_rereads", "tier_reads", "tier_read_threads")} | \
            {"tier_direct_reads": st[0].get("tier_direct_reads", 0)}  # one count per process

    def test_tier_reads_are_the_bytes_through_the_page_cache(self):
        self.test_tier_reads_are_the_bytes(env={"ROCEPOOL_OST_TIER_DIRECT_READ": "0"}, port=self.port + 20)

    def test_tier_reads_are_the_bytes(self, env=None, port=None):
        srv = _Servers(self.lib, self.d, port or self.port, nm=1, no=2, env=env)
        srv.start()
        self.addCleanup(srv.stop)
        # 48 MiB through a 16 MiB RAM cache per OST: most of it only on the tier
        w = r'''
import hashlib, os, random
P = os.environ["ROCEPOOL_PREFIX"]
r = random.Random(3)
for i in range(6):
    with open(P + "/f%d" % i, "wb") as f: f.write(r.randbytes((8 << 20) - 1000 * i))
print("ok")
'''
        self.assertEqual(self.done(self.client(srv, w)), "ok")
        before = self.ost_stats(srv)
        rd = r'''
import os, random
P = os.environ["ROCEPOOL_PREFIX"]
r = random.Random(3)
for i in range(6):
    data = r.randbytes((8 << 20) - 1000 * i)
    fd = os.open(P + "/f%d" % i, os.O_RDONLY)
    for k in range(0, len(data) // 47008, 3):  # strided pieces
        assert os.pread(fd, 47008, k * 47008) == data[k * 47008:(k + 1) * 47008], (i, k)
    for off in (len(data) - 5000, 1 << 20, (1 << 20) - 100):  # the end, stripe edges
        assert os.pread(fd, 20000, off) == data[off:off + 20000], (i, off)
    os.close(fd)
    assert open(P + "/f%d" % i, "rb").read() == data, i
print("ok")
'''
        self.assertEqual(self.done(self.client(srv, rd)), "ok")
        st = self.ost_stats(srv)
        self.assertEqual(st["tier_read_threads"], 2 * 32, st)
        # past the page cache (O_DIRECT), as the writes: unaligned pieces included
        if env is None:
            self.assertGreater(st["tier_direct_reads"] - before["tier_direct_reads"], 100, st)
        else:
            self.assertEqual(st["tier_direct_reads"], 0, st)
        self.assertGreater(st["tier_reads"] - before["tier_reads"], 100, st)
        if self.ofi:
            self.assertGreater(st["async_tier_reads"] - before["async_tier_reads"], 100, st)
        else:
            self.assertEqual(st["async_tier_reads"], 0, st)

    def test_a_deleted_objects_reused_slab_is_never_read_as_its_bytes(self):
        # each tier read waits 300 ms first: the file is deleted and its slabs
        # (freed once durable) taken by another file while reads are out
        srv = _Servers(self.lib, self.d, self.port + 10, nm=1, no=2, env={"ROCEPOOL_TEST_OST_READ_DELAY_US": "300000"})
        srv.start()
        self.addCleanup(srv.stop)
        w = r'''
import os
P = os.environ["ROCEPOOL_PREFIX"]
with open(P + "/a", "wb") as f: f.write(b"A" * (8 << 20))
with open(P + "/filler", "wb") as f: f.write(b"F" * (40 << 20))  # a's stripes out of RAM
os.unlink(P + "/filler")
print("ok")
'''
        self.assertEqual(self.done(self.client(srv, w)), "ok")
        rd = r'''
import os, time
P = os.environ["ROCEPOOL_PREFIX"]
fd = os.open(P + "/a", os.O_RDONLY)
bad = 0
seen = set()
try:
    for rep in range(40):  # strided pieces (READ_AT): until the file is gone
        for k in range(rep % 2, (8 << 20) // 47008, 2):
            b = os.pread(fd, 47008, k * 47008)
            seen |= set(b)
            bad += b.count(b"B")
except OSError:
    pass
print(bad, sorted(seen))
'''
        reader = self.client(srv, rd)
        time.sleep(3.0)  # its stride readahead under way: ~16 reads out at any time
        churn = r'''
import os
P = os.environ["ROCEPOOL_PREFIX"]
os.unlink(P + "/a")
for i in range(4):
    with open(P + "/b%d" % i, "wb") as f: f.write(b"B" * (8 << 20))
print("ok")
'''
        self.assertEqual(self.done(self.client(srv, churn)), "ok")
        bad, seen = self.done(reader).split(" ", 1)
        self.assertEqual(bad, "0", "a deleted file's reads returned another file's bytes: " + seen)
        self.assertTrue(set(eval(seen)) <= {0, ord("A")}, seen)
        # (whether a read was out across the delete is timing: async_tier_rereads
        # says; 6-8 of ~80 here on OFI -- not asserted, it would be flaky)
