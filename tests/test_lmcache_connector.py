"""The KV-cache connector for inference engines (roce/native/rocepool_lmc.c,
roce/lmcache_connector.py): LMCache's native-connector contract -- event fd,
batched get / set / exists / delete, drained completions -- over native
OSTs. Every scenario runs in a fresh process.

Checked: chunks of any size round-trip (split into parts across the OSTs);
transfers from/to a registered region are zero-copy, others are copied;
only committed chunks exist; a missing or differently-sized chunk fails per
key; delete removes the commit and every part; many concurrent operations
complete exactly once; close finishes what was queued.
"""
import json
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PRELUDE = r"""
import ctypes, json, mmap, os, random, select, sys, threading, time
sys.path.insert(0, {root!r})
from roce.lmcache_connector import RoCEPoolConnector
from roce.kv import KVClient
R = {{}}
def pattern(key, n):
    return random.Random(key).randbytes(n)
def region(n):
    m = mmap.mmap(-1, n)
    return m, ctypes.addressof((ctypes.c_char * n).from_buffer(m))
def wait(c, fids, timeout=120):
    want, got = set(fids), {{}}
    end = time.time() + timeout
    while want - set(got) and time.time() < end:
        if select.select([c.event_fd()], [], [], 0.5)[0]:
            for fid, ok, err, res in c.drain_completions():
                assert fid not in got, ("completed twice", fid)
                got[fid] = (ok, err, res)
    assert not want - set(got), ("not completed", want - set(got))
    return got
def one(c, fid):
    return wait(c, [fid])[fid]
"""


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class LMCacheConnectorTests(unittest.TestCase):
    port = 23740

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                       PYTHONPATH=ROOT)
        cls.srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                    ROOT, str(cls.port), "0", "4"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   text=True, env=cls.env)
        cls.osts = cls.srv.stdout.readline().split()[-1]

    @classmethod
    def tearDownClass(cls):
        cls.srv.stdin.close()
        cls.srv.wait(timeout=20)

    def run_py(self, body: str, **env) -> dict:
        code = PRELUDE.format(root=ROOT) + f"OSTS = {self.osts!r}\n" + body + \
            "\nprint('RESULT ' + json.dumps(R))\n"
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300,
                             env=dict(self.env, **env))
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        line = next(l for l in out.stdout.splitlines() if l.startswith("RESULT "))
        return json.loads(line[7:])

    def test_roundtrip_registered_is_zero_copy(self):
        r = self.run_py("""
c = RoCEPoolConnector(osts=OSTS, namespace="t1/", part_mb=0.25)   # 256 KiB parts: chunks span many OSTs
sizes = [1, 4097, 256 << 10, (256 << 10) + 1, 3 << 20, (9 << 20) + 123]
total = sum(sizes)
src, sa = region(total); dst, da = region(total)
R["direct_src"] = c.register_region(sa, total)
R["direct_dst"] = c.register_region(da, total)
views_src, views_dst, off = [], [], 0
for i, n in enumerate(sizes):
    src[off:off + n] = pattern(f"k{i}", n)
    views_src.append(memoryview(src)[off:off + n]); views_dst.append(memoryview(dst)[off:off + n]); off += n
keys = [f"chunk/{i}" for i in range(len(sizes))]
R["set"] = one(c, c.submit_batch_set(keys, views_src))
R["exists"] = one(c, c.submit_batch_exists(keys + ["chunk/none"]))
R["get"] = one(c, c.submit_batch_get(keys, views_dst))
R["equal"] = bytes(dst[:total]) == bytes(src[:total])
R["stats"] = c.stats()
c.close()
""")
        self.assertTrue(r["direct_src"] and r["direct_dst"])
        self.assertEqual(r["set"][:2], [True, ""])
        self.assertEqual(r["exists"][2], [True] * 6 + [False])
        self.assertEqual(r["get"][:2], [True, ""])
        self.assertTrue(r["equal"])
        total = 1 + 4097 + (256 << 10) + (256 << 10) + 1 + (3 << 20) + (9 << 20) + 123
        s = r["stats"]
        self.assertEqual((s["bytes_stored"], s["bytes_loaded"]), (total, total))
        self.assertEqual(s["bytes_copied"], 0)            # nothing through the CPU
        self.assertEqual(s["bytes_zero_copy"], 2 * total)  # out by the OSTs' READs, in by ours
        self.assertEqual(s["regions"], 2)

    def test_unregistered_buffers_are_copied(self):
        r = self.run_py("""
c = RoCEPoolConnector(osts=OSTS, namespace="t2/", part_mb=1)
data = [pattern(f"u{i}", n) for i, n in enumerate([10, 1 << 20, (5 << 20) + 7])]
keys = [f"u/{i}" for i in range(3)]
R["set"] = one(c, c.submit_batch_set(keys, [bytearray(d) for d in data]))
R["set_ro"] = one(c, c.submit_batch_set(["u/ro"], [b"read-only bytes"]))   # read-only source: sent from a copy
outs = [bytearray(len(d)) for d in data]
R["get"] = one(c, c.submit_batch_get(keys, outs))
R["equal"] = [bytes(o) == d for o, d in zip(outs, data)]
try:
    c.submit_batch_get(["u/ro"], [b"x" * 15]); R["ro_target"] = "accepted"
except ValueError:
    R["ro_target"] = "rejected"
s = c.stats(); R["copied"] = s["bytes_copied"]; R["zc"] = s["bytes_zero_copy"]
R["total"] = 2 * sum(map(len, data)) + 15
c.close()
""")
        self.assertTrue(r["set"][0] and r["set_ro"][0] and r["get"][0])
        self.assertEqual(r["equal"], [True, True, True])
        self.assertEqual(r["ro_target"], "rejected")
        self.assertEqual((r["copied"], r["zc"]), (r["total"], 0))

    def test_only_committed_chunks_exist_and_delete_removes_parts(self):
        r = self.run_py("""
c = RoCEPoolConnector(osts=OSTS, namespace="t3/", part_mb=0.25)
kv = KVClient(OSTS.split(","), namespace="t3/")
kv.put("half#0", b"x" * 1000)                      # a part with no commit: a store cut short
R["half"] = one(c, c.submit_batch_exists(["half"]))[2]
n = (1 << 20) + 5                                  # 5 parts
src = bytearray(pattern("d", n))
R["set"] = one(c, c.submit_batch_set(["d"], [src]))[0]
R["parts_before"] = sum(1 for i in range(5) if kv.get(f"d#{i}") is not None)
R["delete"] = one(c, c.submit_batch_delete(["d", "never-stored"]))
left = 0
for i in range(5):
    try:
        kv.get(f"d#{i}"); left += 1
    except KeyError:
        pass
R["parts_after"] = left
R["exists_after"] = one(c, c.submit_batch_exists(["d"]))[2]
c.close(); kv.close()
""")
        self.assertEqual(r["half"], [False])
        self.assertTrue(r["set"])
        self.assertEqual(r["parts_before"], 5)
        self.assertEqual(r["delete"][2], [True, False])
        self.assertEqual((r["parts_after"], r["exists_after"]), (0, [False]))

    def test_missing_and_resized_chunks_fail_per_key(self):
        r = self.run_py("""
c = RoCEPoolConnector(osts=OSTS, namespace="t4/", part_mb=0.25)
one(c, c.submit_batch_set(["a", "b"], [bytearray(pattern("a", 1000)), bytearray(pattern("b", 300 << 10))]))
bufs = [bytearray(1000), bytearray(2000), bytearray(10), bytearray(300 << 10)]
R["get"] = one(c, c.submit_batch_get(["a", "a", "missing", "b"], bufs))
R["a_ok"] = bytes(bufs[0]) == pattern("a", 1000)
R["b_ok"] = bytes(bufs[3]) == pattern("b", 300 << 10)
c.close()
""")
        ok, err, res = r["get"]
        self.assertFalse(ok)
        self.assertEqual(res, [True, False, False, True])  # a resized buffer is not a hit
        self.assertTrue(r["a_ok"] and r["b_ok"])

    def test_many_concurrent_operations_complete_once(self):
        r = self.run_py("""
c = RoCEPoolConnector(osts=OSTS, namespace="t5/", workers=6, part_mb=0.5)
N, SZ = 40, (700 << 10)
src, sa = region(N * SZ); dst, da = region(N * SZ)
c.register_region(sa, N * SZ); c.register_region(da, N * SZ)
for i in range(N):
    src[i * SZ:(i + 1) * SZ] = pattern(f"m{i}", SZ)
fids, lk = [], threading.Lock()
def submitter(t):
    for i in range(t, N, 4):
        f = c.submit_batch_set([f"m/{i}"], [memoryview(src)[i * SZ:(i + 1) * SZ]])
        with lk: fids.append(f)
ths = [threading.Thread(target=submitter, args=(t,)) for t in range(4)]
[t.start() for t in ths]; [t.join() for t in ths]
done = wait(c, fids)
R["sets_ok"] = all(v[0] for v in done.values())
g = c.submit_batch_get([f"m/{i}" for i in range(N)], [memoryview(dst)[i * SZ:(i + 1) * SZ] for i in range(N)])
R["get_ok"] = one(c, g)[0]
R["equal"] = bytes(dst[:N * SZ]) == bytes(src[:N * SZ])
R["ops"] = c.stats()["ops"]
c.close()
""")
        self.assertTrue(r["sets_ok"] and r["get_ok"] and r["equal"])
        self.assertEqual(r["ops"], 41)

    def test_close_finishes_queued_operations(self):
        r = self.run_py("""
c = RoCEPoolConnector(osts=OSTS, namespace="t6/", workers=1)
for i in range(20):
    c.submit_batch_set([f"q/{i}"], [bytearray(pattern(f"q{i}", 64 << 10))])
c.close()                                         # returns after all 20 stores
c2 = RoCEPoolConnector(osts=OSTS, namespace="t6/")
R["exists"] = one(c2, c2.submit_batch_exists([f"q/{i}" for i in range(20)]))[2]
try:
    c.submit_batch_exists(["q/0"]); R["after_close"] = "accepted"
except RuntimeError:
    R["after_close"] = "rejected"
c2.close()
""")
        self.assertEqual(r["exists"], [True] * 20)
        self.assertEqual(r["after_close"], "rejected")

    def test_osts_from_environment_and_errors(self):
        r = self.run_py("""
c = RoCEPoolConnector()                           # ROCEPOOL_OSTS
R["ok"] = one(c, c.submit_batch_set(["e"], [bytearray(b"v")]))[0]
c.close()
os.environ.pop("ROCEPOOL_OSTS")
try:
    RoCEPoolConnector(); R["no_osts"] = "accepted"
except ValueError:
    R["no_osts"] = "rejected"
""", ROCEPOOL_OSTS=self.osts)
        self.assertTrue(r["ok"])
        self.assertEqual(r["no_osts"], "rejected")


if __name__ == "__main__":
    unittest.main()
