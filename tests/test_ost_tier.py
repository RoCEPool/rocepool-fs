"""The OST's capacity tier (roce/native/ost_tier.c): objects written through
to a file / block device (here: a file), a bounded RAM cache in front of
it, page-aligned writes handed to the device straight from the NIC's
buffer, evicted objects read from the device straight into the reply.
Checked with a RAM budget of 0 (everything evicted at once -- every read
comes from the device) and of 1 MiB, against the Python reference server
op by op (tests/fakes/native_scenario.py) and through the file system."""
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
sys.path.insert(0, ROOT)


def _stats(transport, addr: str) -> dict:
    from roce.protocol import Frame, Op
    r = transport.send_request(addr, Frame(op=Op.STATS, request_id=1, key="", value=b""))
    return json.loads(r.value)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class TierParityTests(unittest.TestCase):
    """The parity scenario (native OST vs Python PoolNode, every op) with the
    native OST on the tier and nothing allowed to stay in RAM."""

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.tmp = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _run(self, port: int, ram_mib: str) -> dict:
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, ROCEPOOL_OST_TIER=f"file:{self.tmp}/tier{port}_{{n}}:1",
                   ROCEPOOL_OST_RAM_CACHE_MIB=ram_mib)
        out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "native_scenario.py"), ROOT, str(port)],
                             env=env, capture_output=True, text=True, timeout=300)
        line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
        self.assertTrue(line, out.stdout[-2000:] + out.stderr[-2000:])
        res = json.loads(line[len("RESULT "):])
        self.assertNotIn("error", res, res.get("error"))
        return res

    def _check(self, res):
        self.assertEqual(res["ost_native"], res["ost_python"])
        self.assertEqual(res["dfs_native"], [True] * len(res["dfs_native"]))
        self.assertEqual(res["cas_native"], 8 * 40)

    def test_every_op_matches_the_reference_with_no_ram_cache(self):
        res = self._run(19720, "0")
        self._check(res)

    def test_every_op_matches_the_reference_with_a_small_ram_cache(self):
        self._check(self._run(19730, "1"))


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class TierDataPathTests(unittest.TestCase):
    """Direct writes, reads from the device, eviction and integrity through
    the Python client against a native cluster on the tier."""

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.tmp = tempfile.mkdtemp()
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=cls.lib, PYTHONPATH=ROOT,
                       ROCEPOOL_OST_TIER=f"file:{cls.tmp}/tier{{n}}:1", ROCEPOOL_OST_RAM_CACHE_MIB="2")
        cls.srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                    ROOT, "23500", "1", "2"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, env=cls.env)
        line = cls.srv.stdout.readline()
        assert line.startswith("READY"), line + cls.srv.stderr.read()
        _, cls.mdts, cls.osts = line.split()
        os.environ["ROCE_TRANSPORT_LIB"] = cls.lib
        from fs.posix_api import DistributedFileSystem
        from roce.remote_engine import RemoteEngine
        from roce.roce_transport import RoCETransport
        cls.transport = RoCETransport()
        cls.engines = [RemoteEngine(a, cls.transport) for a in cls.osts.split(",")]
        cls.dfs = DistributedFileSystem(mdt_transport=cls.transport, mdts=cls.mdts.split(","), osts=cls.engines,
                                        attr_ttl_s=0)

    @classmethod
    def tearDownClass(cls):
        cls.srv.stdin.close()
        cls.srv.wait(timeout=10)
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def stats(self):
        return [_stats(self.transport, a) for a in self.osts.split(",")]

    def test_tier_is_reported_and_bounded(self):
        for s in self.stats():
            self.assertEqual(s["tier"], "file")
            self.assertGreaterEqual(s["tier_capacity"], 1 << 30)
            self.assertEqual(s["ram_cache_budget"], 2 << 20)

    def test_big_file_round_trip_through_the_tier(self):
        data = random.Random(3).randbytes(24 * 1024 * 1024 + 4321)  # 12x the RAM budget per OST
        self.dfs.write_file("big", data, create_parents=True)
        for s in self.stats():
            self.assertLessEqual(s["ram_cache_bytes"], s["ram_cache_budget"] + (1 << 20) + 4096, s)
            self.assertGreater(s["evictions"], 0, s)
            self.assertGreater(s["tier_used"], 0, s)
        got = self.dfs.read_file("big")
        self.assertEqual(hashlib.sha256(got).hexdigest(), hashlib.sha256(data).hexdigest())
        self.assertTrue(any(s["tier_reads"] > 0 for s in self.stats()))

    def test_page_aligned_writes_go_direct_from_the_request_buffer(self):
        # The C client sends a stripe as RDMA bulk from its registered arena;
        # the server lands it on a page boundary and hands it to the device
        # as it is. (The Python client's frames go inline: the RAM path.)
        from roce.native.build_lib import ensure_intercept_lib
        prefix = os.path.join(self.tmp, "rp")
        penv = dict(self.env, LD_PRELOAD=ensure_intercept_lib(), ROCEPOOL_PREFIX=prefix, ROCEPOOL_MDTS=self.mdts,
                    ROCEPOOL_OSTS=self.osts)
        before = sum(s["direct_writes"] for s in self.stats())
        out = subprocess.run([sys.executable, "-c", f"""
import os
P = {prefix!r}
with open(P + "/direct.bin", "wb") as f:
    f.write(bytes([0x5a]) * (3 << 20))        # three whole stripes
with open(P + "/direct.bin", "rb") as f:
    print(f.read() == bytes([0x5a]) * (3 << 20))
"""], env=penv, capture_output=True, text=True, timeout=120)
        self.assertEqual(out.stdout.strip(), "True", out.stdout + out.stderr)
        after = sum(s["direct_writes"] for s in self.stats())
        self.assertGreaterEqual(after, before + 3, (before, after))
        self.assertEqual(hashlib.sha256(self.dfs.read_file("direct.bin")).hexdigest(),
                         hashlib.sha256(bytes([0x5a]) * (3 << 20)).hexdigest())
        # an unaligned write into the same object: the RAM path, still correct
        self.engines[0].write_at("direct/at", 0, b"\x3c" * 65536)
        self.engines[0].write_at("direct/at", 100, b"xyz")
        got = self.engines[0].get("direct/at")
        self.assertEqual(got[98:104], b"\x3c\x3cxyz\x3c")
        self.assertEqual(len(got), 65536)

    def test_partial_writes_holes_truncate_and_delete(self):
        e = self.engines[1]
        e.write_at("p/obj", 5000, b"B" * 10)  # a new object with a hole
        got = e.get("p/obj")
        self.assertEqual(got, b"\x00" * 5000 + b"B" * 10)
        e.write_at("p/obj", 0, b"A" * 4096)   # aligned prefix into an existing (evictable) object
        e.write_at("p/obj", 300000, b"C" * 7)  # grows into a bigger size class: a new slab
        got = e.get("p/obj")
        self.assertEqual(len(got), 300007)
        self.assertEqual(got[:4096], b"A" * 4096)
        self.assertEqual(got[5000:5010], b"B" * 10)
        self.assertEqual(got[4096:5000], b"\x00" * 904)
        self.assertEqual(got[-7:], b"C" * 7)
        e.truncate_object("p/obj", 4100)
        self.assertEqual(e.get("p/obj"), b"A" * 4096 + b"\x00" * 4)
        e.delete("p/obj")
        with self.assertRaises(Exception):
            e.get("p/obj")
        # the slab is reused: used space does not grow with churn
        used0 = _stats(self.transport, self.osts.split(",")[1])["tier_used"]
        for i in range(20):
            e.put("p/churn", bytes([i]) * 100000)
            e.delete("p/churn")
        used1 = _stats(self.transport, self.osts.split(",")[1])["tier_used"]
        self.assertEqual(used0, used1)

    def test_one_sided_reads_stay_within_the_ram_budget(self):
        # The C client reads stripes one-sided: it asks for a lease, the OST
        # brings the object into RAM (from the tier) and publishes it. That
        # path never evicted: on site, ior-easy-read grew an OST's 29 GiB
        # cache to 117 GiB and the OOM killer took it.
        from roce.native.build_lib import ensure_intercept_lib
        prefix = os.path.join(self.tmp, "rp")
        penv = dict(self.env, LD_PRELOAD=ensure_intercept_lib(), ROCEPOOL_PREFIX=prefix, ROCEPOOL_MDTS=self.mdts,
                    ROCEPOOL_OSTS=self.osts, ROCEPOOL_DIRECT_READ="1")
        out = subprocess.run([sys.executable, "-c", f"""
import hashlib, os
P = {prefix!r}
data = bytes(range(256)) * (96 << 12)          # 24 MiB: 12x the budget per OST
with open(P + "/lease.bin", "wb") as f:
    f.write(data)
for _ in range(2):
    with open(P + "/lease.bin", "rb") as f:
        got = f.read()
print(got == data)
"""], capture_output=True, text=True, env=penv, timeout=300)
        self.assertEqual(out.stdout.split(), ["True"], out.stdout + out.stderr)
        st = self.stats()
        self.assertGreater(sum(s["leases"] for s in st), 0, st)
        for s in st:
            self.assertLessEqual(s["ram_cache_bytes"], s["ram_cache_budget"] + (2 << 20), s)

    def test_strided_pieces_are_written_to_the_tier_about_once(self):
        # IOR hard on site: 47008-byte pieces of a shared file, ~22 per 1 MiB
        # stripe object, arriving out of order. Each piece used to rewrite
        # the object from its offset to the end (and every slab growth the
        # whole object): ~10x the bytes in tier writes, 0.68 GiB/s on NVMe
        # that took 5.6 GiB/s of large writes. Pieces are interleaved over
        # 24 objects (12x the RAM budget) so objects are evicted mid-fill.
        e, addr = self.engines[0], self.osts.split(",")[0]
        piece, nobj = 47008, 24
        want = {f"h/{o}": bytearray() for o in range(nobj)}
        jobs = []
        for o in range(nobj):
            data = random.Random(o).randbytes(22 * piece)
            want[f"h/{o}"] = data
            order = list(range(22))
            random.Random(100 + o).shuffle(order)
            jobs.append([(f"h/{o}", k * piece, data[k * piece:(k + 1) * piece]) for k in order])
        before = _stats(self.transport, addr)["tier_bytes_written"]
        for r in range(22):
            for o in range(nobj):
                e.write_at(*jobs[o][r])
        for k, v in want.items():
            self.assertEqual(e.get(k), v, k)
        written = _stats(self.transport, addr)["tier_bytes_written"] - before
        # old code: 10.5x here; now ~1.8x in this worst case (every object
        # written back between pieces, zero gaps and all); on site, with a
        # RAM cache far larger than the objects in flight, ~1x
        self.assertLessEqual(written, 2.2 * nobj * 22 * piece, written)

    def test_many_small_objects_survive_eviction(self):
        e = self.engines[0]
        blobs = {f"s/{i}": random.Random(i).randbytes(3000 + i) for i in range(1500)}  # ~6 MiB, 3x the budget
        for k, v in blobs.items():
            e.put(k, v)
        for k, v in random.Random(9).sample(sorted(blobs.items()), 200):
            self.assertEqual(e.get(k), v, k)
        self.assertEqual(len(e.list_prefix("s/")), 1500)


if __name__ == "__main__":
    unittest.main()
