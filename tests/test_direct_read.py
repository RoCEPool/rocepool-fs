"""One-sided reads: the client takes a LEASE on a stripe object and RDMA
READs it straight out of the OST's registered RAM copy, validating the
object's seqlock. Checked: reads go one-sided (counters on both ends); a
stale lease -- the object rewritten, grown, truncated, deleted or evicted
to the NVMe tier by another client -- is detected and never returns old
bytes; the Python reference server (no LEASE op) is read by RPC with no
error; ROCEPOOL_DIRECT_READ=0 turns the path off."""
import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _stats(transport, addr: str) -> dict:
    from roce.protocol import Frame, Op
    return json.loads(transport.send_request(addr, Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)


class _Cluster(unittest.TestCase):
    # ROCEPOOL_DIRECT_LOOKUP=1: one-sided lookups on whatever the transport
    # (auto turns them off on unordered transports -- EFA)
    extra_env = {"ROCEPOOL_DIRECT_LOOKUP": "1"}
    port = 23700

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()
        cls.tmp = tempfile.mkdtemp()
        # a tier with a 3 MiB RAM cache per OST (two 1 MiB stripes and their
        # slab headers fit; more get evicted)
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=cls.lib, PYTHONPATH=ROOT,
                       ROCEPOOL_OST_TIER=f"file:{cls.tmp}/tier{{n}}:1", ROCEPOOL_OST_RAM_CACHE_MIB="3",
                       **cls.extra_env)
        cls.srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                    ROOT, str(cls.port), "1", "2"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, env=cls.env)
        line = cls.srv.stdout.readline()
        assert line.startswith("READY"), line + cls.srv.stderr.read()
        _, cls.mdts, cls.osts = line.split()
        cls.prefix = os.path.join(cls.tmp, "rp")
        cls.penv = dict(cls.env, LD_PRELOAD=cls.icp, ROCEPOOL_PREFIX=cls.prefix, ROCEPOOL_MDTS=cls.mdts,
                        ROCEPOOL_OSTS=cls.osts, ROCEPOOL_ATTR_TTL="0", ROCEPOOL_INTERCEPT_LOG="1")
        os.environ["ROCE_TRANSPORT_LIB"] = cls.lib
        from fs.posix_api import DistributedFileSystem
        from roce.remote_engine import RemoteEngine
        from roce.roce_transport import RoCETransport
        cls.transport = RoCETransport()
        cls.dfs = DistributedFileSystem(mdt_transport=cls.transport, mdts=cls.mdts.split(","),
                                        osts=[RemoteEngine(a, cls.transport) for a in cls.osts.split(",")], attr_ttl_s=0)

    @classmethod
    def tearDownClass(cls):
        cls.srv.stdin.close()
        cls.srv.wait(timeout=10)
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def py(self, code: str, env=None) -> tuple[str, dict]:
        """runs code under the library; returns (stdout, the library's exit counters)"""
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env or self.penv,
                             timeout=120)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        m = re.search(r"stats (.*)$", out.stderr, re.M)
        counters = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", m.group(1))} if m else {}
        return out.stdout, counters

    def leases(self):
        return sum(_stats(self.transport, a)["leases"] for a in self.osts.split(","))


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class DirectReadTests(_Cluster):

    def test_reads_go_one_sided(self):
        before = self.leases()
        out, c = self.py(f"""
import os, random
P = {self.prefix!r}
data = random.Random(1).randbytes(3 * 1048576 + 1000)
with open(P + "/a", "wb") as f: f.write(data)
with open(P + "/a", "rb") as f: print(f.read() == data)
with open(P + "/a", "rb") as f:          # strided pieces of the stripes
    for off in range(0, len(data) - 47008, 300000):
        f.seek(off); assert f.read(47008) == data[off:off + 47008], off
print("ok")
""")
        self.assertEqual(out.split(), ["True", "ok"])
        self.assertGreaterEqual(c["direct_reads"], 4 + 10, c)   # whole stripes + pieces
        self.assertGreaterEqual(self.leases() - before, 4)       # one lease per stripe
        self.assertEqual(c["stale_leases"], 0, c)

    def test_stale_leases_never_return_old_bytes(self):
        # one long-lived reader process holds leases; the Python client
        # changes the objects underneath between its reads
        reader = subprocess.Popen([sys.executable, "-u", "-c", f"""
import sys, hashlib
P = {self.prefix!r}
for line in sys.stdin:
    cmd = line.strip()
    if cmd == "quit": break
    with open(P + "/b", "rb") as f: d = f.read()
    print(len(d), hashlib.sha256(d).hexdigest(), flush=True)
"""], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=self.penv)

        def read():
            reader.stdin.write("go\n")
            reader.stdin.flush()
            n, h = reader.stdout.readline().split()
            return int(n), h

        def sha(b):
            return hashlib.sha256(b).hexdigest()

        d1 = random.Random(2).randbytes(2 * 1048576 + 17)
        self.dfs.write_file("b", d1, create_parents=True)
        self.assertEqual(read(), (len(d1), sha(d1)))       # leases taken here
        d2 = random.Random(3).randbytes(2 * 1048576 + 17)
        self.dfs.write_file("b", d2)                       # rewritten in place: same sizes, new seqs
        self.assertEqual(read(), (len(d2), sha(d2)))
        self.dfs.write_range("b", 1048576 + 5, b"PATCH" * 100)  # a partial write into a leased stripe
        d3 = bytearray(d2); d3[1048576 + 5:1048576 + 505] = b"PATCH" * 100
        self.assertEqual(read(), (len(d3), sha(bytes(d3))))
        self.dfs.truncate("b", 1048576 + 100)              # the second stripe shrinks
        self.assertEqual(read(), (1048576 + 100, sha(bytes(d3[:1048576 + 100]))))
        d4 = random.Random(4).randbytes(5 * 1048576)        # grows: bigger slabs, new objects
        self.dfs.write_file("b", d4)
        self.assertEqual(read(), (len(d4), sha(d4)))
        # push the objects out of the OSTs' 2 MiB RAM caches: the leased
        # copies are evicted (seq left odd), the reader must re-lease
        for i in range(8):
            self.dfs.write_file(f"filler{i}", bytes([i]) * (3 * 1048576), create_parents=True)
        self.assertEqual(read(), (len(d4), sha(d4)))
        self.dfs.remove_file("b")
        self.dfs.write_file("b", b"small", create_parents=True)   # recreated, inline
        self.assertEqual(read(), (5, sha(b"small")))
        reader.stdin.write("quit\n")
        reader.stdin.flush()
        reader.wait(timeout=30)
        err = reader.stderr.read()
        m = re.search(r"stats (.*)$", err, re.M)
        c = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", m.group(1))}
        self.assertGreater(c["stale_leases"], 0, c)        # the changes were caught, not missed
        self.assertGreater(c["direct_reads"], 5, c)

    def test_off_switch_and_python_server_fallback(self):
        env = dict(self.penv, ROCEPOOL_DIRECT_READ="0")
        out, c = self.py(f"""
import os
P = {self.prefix!r}
with open(P + "/c", "wb") as f: f.write(bytes(range(256)) * 8192)
with open(P + "/c", "rb") as f: print(f.read() == bytes(range(256)) * 8192)
""", env)
        self.assertEqual(out.strip(), "True")
        self.assertEqual(c["direct_reads"], 0, c)
        self.assertEqual(c["leases"], 0, c)

    def test_python_reference_ost_has_no_lease_and_is_read_by_rpc(self):
        # a Python PoolNode OST: LEASE is UNSUPPORTED_OP there; the client
        # remembers that per server and reads by RPC without a second try
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tools", "crossover_node.py"),
                                "pool-server", f"127.0.0.1:{self.port + 90}", f"127.0.0.1:{self.port + 90}"],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                               env=self.env)
        self.assertTrue(srv.stdout.readline().startswith("READY"))
        try:
            env = dict(self.penv, ROCEPOOL_OSTS=f"127.0.0.1:{self.port + 90}")
            out, c = self.py(f"""
import os
P = {self.prefix!r}
d = bytes(range(256)) * 8192
with open(P + "/py", "wb") as f: f.write(d)
with open(P + "/py", "rb") as f: print(f.read() == d)
with open(P + "/py", "rb") as f: f.seek(100); print(f.read(1000) == d[100:1100])
""", env)
            self.assertEqual(out.split(), ["True", "True"])
            self.assertEqual(c["direct_reads"], 0, c)
        finally:
            srv.stdin.close()
            srv.wait(timeout=10)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class DirectLookupTests(_Cluster):
    """The MDT's slot table: lookups (stat, exists, open) are one-sided
    RDMA reads of the entry's slot; every change the MDT makes is seen."""

    def test_lookups_go_one_sided_and_track_every_change(self):
        self.dfs.mkdir("lk", parents=True)
        for i in range(50):
            self.dfs.write_file(f"lk/f{i}", b"x" * (100 + i))
        mdt_before = sum(_stats(self.transport, a)["by_op"].get("1", 0) for a in self.mdts.split(","))
        out, c = self.py(f"""
import os
P = {self.prefix!r}
print(sum(os.stat(P + "/lk/f%d" % i).st_size for i in range(50)), os.path.exists(P + "/lk/missing"),
      os.path.isdir(P + "/lk"))
""")
        self.assertEqual(out.split(), [str(sum(100 + i for i in range(50))), "False", "True"])
        self.assertGreaterEqual(c["direct_lookups"], 50, c)
        mdt_after = sum(_stats(self.transport, a)["by_op"].get("1", 0) for a in self.mdts.split(","))
        self.assertLessEqual(mdt_after - mdt_before, 2, "stats should not have reached the MDT as RPCs")
        # a long-lived observer; the Python client changes entries under it
        obs = subprocess.Popen([sys.executable, "-u", "-c", f"""
import os, sys
P = {self.prefix!r}
for line in sys.stdin:
    name = line.strip()
    if name == "quit": break
    try: print(os.stat(P + "/" + name).st_size, flush=True)
    except FileNotFoundError: print("ENOENT", flush=True)
"""], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                               env=dict(self.penv, ROCEPOOL_ATTR_TTL="0"))

        def look(name):
            obs.stdin.write(name + "\n")
            obs.stdin.flush()
            return obs.stdout.readline().strip()

        self.assertEqual(look("lk/f1"), "101")
        self.dfs.write_file("lk/f1", b"y" * 5000)           # resized inline entry: slot republished
        self.assertEqual(look("lk/f1"), "5000")
        self.dfs.write_file("lk/f1", b"z" * (3 << 20))      # spilled to the OSTs: layout, size
        self.assertEqual(look("lk/f1"), str(3 << 20))
        self.dfs.truncate("lk/f1", 777)
        self.assertEqual(look("lk/f1"), "777")
        self.dfs.remove_file("lk/f1")                       # tombstone: not found
        self.assertEqual(look("lk/f1"), "ENOENT")
        self.dfs.write_file("lk/f1", b"back")               # recreated in the same slot
        self.assertEqual(look("lk/f1"), "4")
        self.assertEqual(look("lk/never"), "ENOENT")
        obs.stdin.write("quit\n")
        obs.stdin.flush()
        obs.wait(timeout=30)
        m = re.search(r"stats (.*)$", obs.stderr.read(), re.M)
        c = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", m.group(1))}
        self.assertGreaterEqual(c["direct_lookups"], 6, c)

    def test_python_mdt_has_no_table(self):
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tools", "crossover_node.py"),
                                "mdt-server", f"127.0.0.1:{self.port + 91}"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, env=self.env)
        self.assertTrue(srv.stdout.readline().startswith("READY"))
        try:
            env = dict(self.penv, ROCEPOOL_MDTS=f"127.0.0.1:{self.port + 91}")
            out, c = self.py(f"""
import os
P = {self.prefix!r}
os.mkdir(P + "/pm"); open(P + "/pm/a", "wb").write(b"abc")
print(os.stat(P + "/pm/a").st_size, os.path.exists(P + "/pm/b"))
""", env)
            self.assertEqual(out.split(), ["3", "False"])
            self.assertEqual(c["direct_lookups"], 0, c)
        finally:
            srv.stdin.close()
            srv.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()


# The same tests with the RC transport declaring itself unordered: the
# validation EFA's SRD needs (data before header; slot re-read bracketed by
# seqs) runs here on every backend.
class UnorderedDirectReadTests(DirectReadTests):
    extra_env = {"ROCEPOOL_DIRECT_LOOKUP": "1", "ROCE_READS_UNORDERED": "1"}
    port = 23720


class UnorderedDirectLookupTests(DirectLookupTests):
    extra_env = {"ROCEPOOL_DIRECT_LOOKUP": "1", "ROCE_READS_UNORDERED": "1"}
    port = 23740

    def test_auto_off_when_unordered(self):
        env = dict(self.penv)
        env.pop("ROCEPOOL_DIRECT_LOOKUP")
        self.dfs.write_file("auto", b"a")
        out, c = self.py(f"""
import os
print(os.stat({self.prefix!r} + "/auto").st_size)
""", env)
        self.assertEqual(out.split(), ["1"])
        self.assertEqual(c["direct_lookups"], 0, c)
