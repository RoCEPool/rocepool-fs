"""The first one-sided read of freshly stored data costs what any later read
costs: the OST registers each 64 MiB arena chunk with its device when the
write that needs the chunk makes it, not at the first lease of an object in
it.

Registered at the first lease, the reader paid for pinning whole chunks, with
the OST's store lock held. On g6.8xlarge (EFA, 2026-11-21) the host that
loaded freshly stored KV cache first loaded it at half the rate of the host
that loaded it next (T3: 0.38 vs 0.19 ms per MiB at 16K tokens)."""
import json
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SCRIPT = r"""
import ctypes, json, mmap, os, select, sys, time
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
from roce.lmcache_connector import RoCEPoolConnector
t = RoCETransport()
osts = [t.serve_native("ost", f"127.0.0.1:{int(sys.argv[2]) + i}") for i in range(2)]
addrs = ",".join(o.local_address() for o in osts)
chunk, n = 14 << 20, 12                      # 168 MiB: several new 64 MiB arena chunks on each OST
src, dst = mmap.mmap(-1, chunk * n), mmap.mmap(-1, chunk * n)
src[:] = os.urandom(len(src))
def connector():
    c = RoCEPoolConnector(osts=addrs, namespace="f/", part_mb=4)
    for m in (src, dst):
        c.register_region(ctypes.addressof((ctypes.c_char * len(m)).from_buffer(m)), len(m))
    return c
def run(c, fid):
    while True:
        select.select([c.event_fd()], [], [], 1)
        for f, ok, err, res in c.drain_completions():
            if f == fid:
                return ok
keys = [f"chunk{i}" for i in range(n)]
views = lambda m: [memoryview(m)[i * chunk:(i + 1) * chunk] for i in range(n)]
writer = connector()
assert run(writer, writer.submit_batch_set(keys, views(src)))
st0 = [o.stats() for o in osts]
times = []
for reader in (connector(), connector()):   # the first read of this data, then a second (another client)
    t0 = time.perf_counter()
    assert run(reader, reader.submit_batch_get(keys, views(dst)))
    times.append(time.perf_counter() - t0)
    assert dst[:] == src[:]
st1 = [o.stats() for o in osts]
print("RESULT " + json.dumps({
    "lease_registrations": sum(b.get("lease_registrations", -1) - a.get("lease_registrations", 0) for a, b in zip(st0, st1)),
    "data_out": sum(b["data_out"] - a["data_out"] for a, b in zip(st0, st1)),
    "first_s": times[0], "second_s": times[1], "mib": chunk * n >> 20}), flush=True)
os._exit(0)
"""


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OstFirstRead(unittest.TestCase):
    def test_chunks_are_registered_by_the_write_not_the_first_read(self):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                   PYTHONPATH=ROOT)
        r = subprocess.run([sys.executable, "-c", SCRIPT, ROOT, "23760"], capture_output=True, text=True,
                           timeout=300, env=env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr[-3000:])
        d = json.loads(next(l for l in r.stdout.splitlines() if l.startswith("RESULT "))[7:])
        print(f"\n    {d['mib']} MiB, first read {d['first_s'] * 1e3:.0f} ms, second {d['second_s'] * 1e3:.0f} ms",
              file=sys.stderr)
        self.assertEqual(d["data_out"], 0, "both reads were one-sided")
        self.assertEqual(d["lease_registrations"], 0, "no arena chunk was registered by a read")


if __name__ == "__main__":
    unittest.main()
