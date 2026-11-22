"""When the OST registers its 64 MiB arena chunks with the device.

- Clients read one-sided (LMCache's adapter): after the OST's first lease,
  each chunk is registered by the write that makes it, so the first read
  of freshly stored data costs what any later read costs.
- No client reads one-sided (IO500 on i3en ran with direct reads off): the
  OST registers nothing, as before 2026-11-22.

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
    if sys.argv[3] == "one-sided":       # registered memory: one-sided reads (else: reads by RPC)
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
mode = sys.argv[3]
writer = connector()
if mode == "one-sided":
    # a first one-sided read on every OST (as LMCache's first L2 load)
    small = mmap.mmap(-1, 16 << 20)
    assert run(writer, writer.submit_batch_set(["warm"], [memoryview(src)[:16 << 20]]))
    assert run(writer, writer.submit_batch_get(["warm"], [memoryview(dst)[:16 << 20]]))
st0 = [o.stats() for o in osts]
assert run(writer, writer.submit_batch_set(keys, views(src)))
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
    "registered_total": sum(b.get("arena_registered", -1) for b in st1),
    "first_s": times[0], "second_s": times[1], "mib": chunk * n >> 20}), flush=True)
os._exit(0)
"""


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OstFirstRead(unittest.TestCase):
    def run_mode(self, mode, port):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                   PYTHONPATH=ROOT)
        r = subprocess.run([sys.executable, "-c", SCRIPT, ROOT, str(port), mode], capture_output=True, text=True,
                           timeout=300, env=env)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr[-3000:])
        d = json.loads(next(l for l in r.stdout.splitlines() if l.startswith("RESULT "))[7:])
        print(f"\n    {mode}: {d['mib']} MiB, first read {d['first_s'] * 1e3:.0f} ms, "
              f"second {d['second_s'] * 1e3:.0f} ms, chunks registered {d['registered_total']}", file=sys.stderr)
        return d

    def test_after_the_first_lease_chunks_are_registered_by_the_write(self):
        d = self.run_mode("one-sided", 23760)
        self.assertEqual(d["data_out"], 0, "both reads were one-sided")
        self.assertEqual(d["lease_registrations"], 0, "no chunk of the new data was registered by a read")
        self.assertGreaterEqual(d["registered_total"], 4)

    def test_without_one_sided_reads_nothing_is_registered(self):
        d = self.run_mode("rpc", 23770)
        self.assertGreaterEqual(d["data_out"], 2 * d["mib"] << 20, "both reads went through the OSTs")
        self.assertEqual(d["registered_total"], 0, "no chunk was registered with the device")


if __name__ == "__main__":
    unittest.main()
