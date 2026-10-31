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

    def __init__(self, lib, d, port, nm=2, no=3, durable=True):
        self.lib, self.d, self.port, self.nm, self.no, self.durable = lib, d, port, nm, no, durable
        self.p = None

    def start(self):
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT,
                   ROCEPOOL_OST_TIER=f"file:{self.d}/ost{{n}}.tier:1", ROCEPOOL_OST_RAM_CACHE_MIB="16")
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

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.srv = _Servers(self.lib, self.tmp, self.port)
        self.srv.start()
        self.addCleanup(self.srv.stop)
        self.prefix = os.path.join(self.tmp, "rp")

    def py(self, code: str, env: dict | None = None) -> str:
        e = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                 ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                 ROCEPOOL_ATTR_TTL="3", **(env or {}))
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
                 ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts)
        ps = [subprocess.Popen([sys.executable, "-c", hard, str(m)], env=e, cwd=self.tmp) for m in (0, 1)]
        self.assertEqual([p.wait(timeout=300) for p in ps], [0, 0])
        sums["d/shared"] = hashlib.sha256(b"".join(bytes([k % 251]) * piece for k in range(200))).hexdigest()

        st = [_stats(self.lib, a) for a in self.srv.mdts.split(",") + self.srv.osts.split(",")]
        self.assertTrue(all(s["durable"] for s in st), st)
        self.assertTrue(all(s["log"]["commits"] > 0 and s["log"]["append_failures"] == 0 for s in st), st)

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
