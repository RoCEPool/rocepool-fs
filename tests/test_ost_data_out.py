"""The OST's data_out counter: object bytes it sent back in replies. A read
the client makes one-sided (RDMA READ of a leased object) never passes
through the OST's request path, so data_out stays 0 however much is read;
the same reads into unregistered memory (the RPC path) count every byte.
tools/kvcache_e2e.py relies on this to show that loads bypass the server."""
import json
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SCRIPT = r"""
import ctypes, json, mmap, os, select, sys
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
from roce.lmcache_connector import RoCEPoolConnector
t = RoCETransport()
osts = [t.serve_native("ost", f"127.0.0.1:{int(sys.argv[2]) + i}") for i in range(2)]
c = RoCEPoolConnector(osts=",".join(o.local_address() for o in osts), namespace="d/", part_mb=1)
n = 6 << 20
src, dst = mmap.mmap(-1, n), mmap.mmap(-1, n)
src[:] = os.urandom(n)
if sys.argv[3] == "registered":        # as LMCache's L1 arena: zero copy, one-sided reads
    for m in (src, dst):
        c.register_region(ctypes.addressof((ctypes.c_char * n).from_buffer(m)), n)
def run(fid):
    while True:
        select.select([c.event_fd()], [], [], 1)
        for f, ok, err, res in c.drain_completions():
            if f == fid:
                return ok
assert run(c.submit_batch_set(["chunk"], [memoryview(src)]))
before = sum(o.stats()["data_out"] for o in osts)
for _ in range(3):
    assert run(c.submit_batch_get(["chunk"], [memoryview(dst)]))
    assert dst[:] == src[:]
after = sum(o.stats()["data_out"] for o in osts)
st = c.stats()
print("RESULT " + json.dumps({"read": 3 * n, "data_out": after - before, "zero_copy": st["bytes_zero_copy"],
                              "copied": st["bytes_copied"]}), flush=True)
c.close()
os._exit(0)
"""


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OstDataOut(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                       PYTHONPATH=ROOT)

    def measure(self, port, mode, **env):
        r = subprocess.run([sys.executable, "-c", SCRIPT, ROOT, str(port), mode], capture_output=True, text=True,
                           timeout=120, env=dict(self.env, **env))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        return json.loads(next(l for l in r.stdout.splitlines() if l.startswith("RESULT "))[7:])

    def test_one_sided_reads_never_pass_through_the_ost(self):
        # the connector as LMCache drives it: a registered region, so loads are
        # RDMA READs of leased parts
        r = self.measure(23860, "registered")
        self.assertEqual(r["data_out"], 0, "18 MiB read, none of it sent by an OST")
        self.assertEqual(r["copied"], 0)

    def test_reads_through_the_ost_count_every_byte(self):
        # the same loads into unregistered memory go through the OSTs' replies
        r = self.measure(23870, "plain")
        self.assertEqual(r["data_out"], r["read"])
        self.assertEqual(r["zero_copy"], 0)


if __name__ == "__main__":
    unittest.main()
