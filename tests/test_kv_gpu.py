"""The KV API's GPU calls (rocepool_kv.h: rpkv_*_dev, rpkv_mcontains; Python
roce/kv.py DeviceBuffer): KV-cache blocks moved straight between the OSTs'
RAM and GPU memory -- a get is a one-sided RDMA READ into the GPU, a put an
RDMA READ by the OST out of it.

No GPU here: ROCEPOOL_DEV_EMU=1, the library's emulated device, whose
memory faults on any CPU access (so a byte the CPU copied would crash the
test, not pass it) and which the RDMA emulator reaches through its dma-buf
registration. Every scenario runs in a fresh process against native OSTs.
"""
import json
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PRELUDE = r"""
import errno, json, os, random, sys
sys.path.insert(0, {root!r})
from roce.kv import KVClient, EmulatedDevice
kv = KVClient({osts!r}.split(","), namespace="kvgpu/")
dev = EmulatedDevice()
R = {{}}
def pattern(key, n):
    return random.Random(key).randbytes(n)
"""


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class KVGpuTests(unittest.TestCase):
    port = 23620
    direct = os.environ.get("ROCEPOOL_BACKEND", "softverbs") == "softverbs"  # libfabric tcp: no FI_HMEM

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                       PYTHONPATH=ROOT, ROCEPOOL_DEV_EMU="1")
        cls.srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                    ROOT, str(cls.port), "0", "4"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   text=True, env=cls.env)
        cls.osts = cls.srv.stdout.readline().split()[-1]

    @classmethod
    def tearDownClass(cls):
        cls.srv.stdin.close()
        cls.srv.wait(timeout=20)

    def run_py(self, body: str, **env) -> dict:
        code = PRELUDE.format(root=ROOT, osts=self.osts) + body + "\nprint('RESULT ' + json.dumps(R))\n"
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300,
                             env=dict(self.env, **env))
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        line = next(l for l in out.stdout.splitlines() if l.startswith("RESULT "))
        return json.loads(line[7:])

    def moved(self, r, n, direct=None):
        d = self.direct if direct is None else direct
        self.assertEqual((r["stats"]["direct_bytes"], r["stats"]["bounced_bytes"]), (n, 0) if d else (0, n))

    def test_put_get_roundtrip_sizes(self):
        r = self.run_py("""
sizes = [1, 4097, 64 << 10, 2 << 20, 15 << 20]
total = sum(sizes)
p = dev.alloc(total)
buf = kv.register_device(p, total)
off, offs = 0, []
for i, n in enumerate(sizes):
    dev.write(p + off, pattern(f"k{i}", n)); offs.append(off); off += n
for i, n in enumerate(sizes):
    buf.put(f"k{i}", offs[i], n)
q = dev.alloc(total)
out = kv.register_device(q, total)
ok = []
for i, n in enumerate(sizes):
    got = out.get(f"k{i}", offs[i], n)
    ok.append(got == n and dev.read(q + offs[i], n) == pattern(f"k{i}", n))
# readable by the host-memory API too
ok.append(kv.get("k1") == pattern("k1", 4097))
R.update(ok=ok, direct=buf.direct, stats=kv.dev_stats(), total=total)
""")
        self.assertTrue(all(r["ok"]), r["ok"])
        self.assertEqual(r["direct"], self.direct)
        self.moved(r, 2 * r["total"])  # every put and every get by RDMA into / out of the device

    def test_missing_small_cap_and_host_written_values(self):
        r = self.run_py("""
p = dev.alloc(1 << 20)
buf = kv.register_device(p, 1 << 20)
try:
    buf.get("nope", 0, 4096); R["missing"] = "found?"
except KeyError:
    R["missing"] = "KeyError"
kv.put("host-written", pattern("h", 300000))
try:
    buf.get("host-written", 0, 1000); R["small"] = "fit?"
except OSError as e:
    R["small"] = e.errno == errno.EFBIG
n = buf.get("host-written", 4096, 300000)
R["host"] = n == 300000 and dev.read(p + 4096, n) == pattern("h", 300000)
try:
    buf.get("host-written", (1 << 20) - 10, 300000); R["range"] = "accepted?"
except OSError as e:
    R["range"] = e.errno == errno.EINVAL
""")
        self.assertEqual(r["missing"], "KeyError")
        self.assertIs(r["small"], True)
        self.assertIs(r["host"], True)
        self.assertIs(r["range"], True)

    def test_cached_lease_sees_overwrite_and_delete(self):
        r = self.run_py("""
n = 1 << 20
p = dev.alloc(2 * n)
buf = kv.register_device(p, 2 * n)
dev.write(p, pattern("v1", n))
buf.put("blk", 0, n)
buf.get("blk", n, n)                       # lease taken and cached
s1 = kv.dev_stats()
buf.get("blk", n, n)                       # served by the cached lease: no RPC
s2 = kv.dev_stats()
other = KVClient({osts!r}.split(","), namespace="kvgpu/")
other.put("blk", pattern("v2", n))         # another client overwrites it
buf.get("blk", n, n)
R["after_overwrite"] = dev.read(p + n, n) == pattern("v2", n)
other.delete("blk")
try:
    buf.get("blk", n, n); R["after_delete"] = "found"
except KeyError:
    R["after_delete"] = "KeyError"
R["hit_used"] = s2["lease_hits"] - s1["lease_hits"]
R["rpc_free"] = s2["leases"] == s1["leases"]
""".replace("{osts!r}", repr(self.osts)))
        self.assertIs(r["after_overwrite"], True)
        self.assertEqual(r["after_delete"], "KeyError")
        if self.direct:
            self.assertEqual(r["hit_used"], 1)
            self.assertIs(r["rpc_free"], True)

    def test_batched_prefix_lookup_and_load(self):
        r = self.run_py("""
blk, nb = 64 << 10, 400
p = dev.alloc(blk * nb)
buf = kv.register_device(p, blk * nb)
keys = [f"pfx/b{i}" for i in range(nb)]
for i, k in enumerate(keys):
    dev.write(p + i * blk, pattern(k, blk))
stored = 300                                  # blocks 0..299 cached, the rest not
buf.mput(keys[:stored], [i * blk for i in range(stored)], [blk] * stored)
rpcs0 = kv.rpcs
R["prefix"] = kv.cached_prefix(keys)
R["lookup_rpcs"] = kv.rpcs - rpcs0            # one BATCH per OST
q = dev.alloc(blk * nb)
out = kv.register_device(q, blk * nb)
s0 = kv.dev_stats()
rpcs1 = kv.rpcs
lens = out.mget(keys, [i * blk for i in range(nb)], [blk] * nb)
s1 = kv.dev_stats()
R["lens_ok"] = lens[:stored] == [blk] * stored and lens[stored:] == [None] * (nb - stored)
R["data_ok"] = all(dev.read(q + i * blk, blk) == pattern(keys[i], blk) for i in range(stored))
R["new_leases_on_load"] = s1["leases"] - s0["leases"]
R["load_rpcs"] = kv.rpcs - rpcs1
R["stats"] = {"direct_bytes": s1["direct_bytes"] - s0["direct_bytes"], "bounced_bytes": s1["bounced_bytes"] - s0["bounced_bytes"]}
R["stored_bytes"] = stored * blk
""")
        self.assertEqual(r["prefix"], 300)
        self.assertLessEqual(r["lookup_rpcs"], 4)
        self.assertIs(r["lens_ok"], True)
        self.assertIs(r["data_ok"], True)
        self.moved(r, r["stored_bytes"])
        if self.direct:
            # the 300 stored blocks come by the leases the lookup cached: no lease RPC;
            # only the 100 absent keys are asked about again, one BATCH per OST
            self.assertEqual(r["new_leases_on_load"], 0)
            self.assertLessEqual(r["load_rpcs"], 4)

    def test_no_nic_registration_bounces_correctly(self):
        r = self.run_py("""
n = 3 << 20
p = dev.alloc(2 * n)
buf = kv.register_device(p, 2 * n)
dev.write(p, pattern("x", n))
buf.put("nb", 0, n)
buf.get("nb", n, n)
R["ok"] = dev.read(p + n, n) == pattern("x", n)
R["direct"] = buf.direct
R["stats"] = kv.dev_stats()
""", ROCEPOOL_DEV_EMU_NO_DMABUF="1")
        self.assertIs(r["ok"], True)
        self.assertIs(r["direct"], False)
        self.moved(r, 6 << 20, direct=False)

    def test_not_device_memory_is_refused(self):
        r = self.run_py("""
import ctypes
host = ctypes.create_string_buffer(4096)
try:
    kv.register_device(ctypes.addressof(host), 4096); R["refused"] = False
except ValueError:
    R["refused"] = True
""")
        self.assertIs(r["refused"], True)


@unittest.skipUnless(os.geteuid() == 0 and shutil.which("gcc"), "needs root (the crossover rig) and gcc")
class KVCacheBenchToolTests(unittest.TestCase):
    """tools/bench_kvcache.py end to end on the crossover rig (emulated GPU):
    store, prefix lookup, load, data check, TTFT arithmetic."""

    def test_emulated_ttft_benchmark(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            rep = os.path.join(td, "kvc.json")
            r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "bench_kvcache.py"), "--emulate",
                                "--prompt", "256", "--prefix", "192", "--requests", "4", "--concurrency", "2",
                                "--prefixes", "2", "--report", rep],
                               capture_output=True, text=True, timeout=1200, env=dict(os.environ, ROCEPOOL_TOPOLOGY=""))
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            with open(rep) as f:
                d = json.load(f)
        rows = {(x["case"], x["granularity"]): x for x in d["results"]}
        self.assertEqual(set(rows), {(c, g) for c in ("gpu-direct", "gpu-bounce", "host") for g in ("block", "layer")})
        for x in rows.values():
            self.assertEqual((x["errors"], x["misses"], x["store_errors"]), (0, 0, 0), x)
            self.assertGreater(x["load_ms_p50"], 0)
            self.assertEqual(x["cached_bytes_per_request"], 192 * d["kv_bytes_per_token"])
        if KVGpuTests.direct:
            self.assertEqual(rows[("gpu-direct", "block")]["bounced_bytes"], 0)
            self.assertGreater(rows[("gpu-direct", "block")]["direct_bytes"], 0)
        self.assertEqual(rows[("gpu-bounce", "block")]["direct_bytes"], 0)
        x = rows[("gpu-direct", "block")]
        self.assertAlmostEqual(x["ttft_cached_ms"], round(x["load_ms_p50"] + 2 * 8.0e9 * 64 / (312e12 * 0.5) * 1e3, 2),
                               delta=0.02)
        self.assertIn(d["status"], ("PASS", "WARN"))


if __name__ == "__main__":
    unittest.main()
