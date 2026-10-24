"""The OST's RAM cache stays within its budget when its objects change size:
a cache full of 1 MiB copies (IOR) then filled with 3901-byte objects
(mdtest-hard's files beyond the MDTs' inline budget). Before: freed slabs
never moved between size classes and a small object counted 4 KiB while
its slab held 8 -- the arena grew to 3x the budget here, and on EFA to 57
GiB against 19, until the kernel killed the OST."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CLIENT = r'''
import json, sys
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
from roce.remote_engine import RemoteEngine
from roce.protocol import Frame, Op
ost = sys.argv[2]
t = RoCETransport(); e = RemoteEngine(ost, t)
def st(): return json.loads(t.send_request(ost, Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)
for i in range(60): e.put(f"big{i}", bytes([i]) * (1 << 20))
a = st()
for i in range(20000): e.put(f"small{i}", bytes([i % 251]) * 3901)
b = st()
# the other way round: reads of the 1 MiB objects (now evicted) evict
# hundreds of small copies each, all within the budget
for i in range(60): assert e.get(f"big{i}") == bytes([i]) * (1 << 20)
c = st()
ok = c["ram_cache_bytes"] <= c["ram_cache_budget"] and all(e.get(f"small{i}") == bytes([i % 251]) * 3901 for i in range(0, 20000, 997)) and e.get("big7") == bytes([7]) * (1 << 20)
print("RESULT " + json.dumps({"big": a["arena_bytes"], "small": b["arena_bytes"], "cache": b["ram_cache_bytes"], "ok": ok}))
'''


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OstArenaTests(unittest.TestCase):
    def test_cache_turning_from_large_to_small_objects_stays_in_budget(self):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_softverbs_lib
        with tempfile.TemporaryDirectory() as td:
            env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), PYTHONPATH=ROOT,
                       ROCEPOOL_OST_TIER=f"file:{td}/t.tier:2", ROCEPOOL_OST_RAM_CACHE_MIB="60")
            srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                                    "23500", "1", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
            try:
                _, _, osts = srv.stdout.readline().split()
                r = subprocess.run([sys.executable, "-c", CLIENT, ROOT, osts], env=env, capture_output=True, text=True,
                                   timeout=900)
            finally:
                srv.stdin.close()
                srv.wait(timeout=20)
        line = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None)
        self.assertIsNotNone(line, r.stdout + r.stderr)
        d = json.loads(line[7:])
        self.assertTrue(d["ok"])
        self.assertLessEqual(d["cache"], 60 << 20)
        self.assertEqual(d["small"], d["big"])  # no new chunk: the freed 1 MiB slabs were reused


if __name__ == "__main__":
    unittest.main()
