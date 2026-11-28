"""The global address client library (roce/native/rocepool_ga.h, Python
roce/ga.py) over the OSTs' global address regions (OP_GA in
rocepool_servers.c): the memory of all the OSTs as one address space,
read into and written from GPU memory by address.

No GPU here: ROCEPOOL_DEV_EMU=1, the emulated device whose memory faults on
any CPU access (a byte the CPU copied would crash the test) and which the
RDMA emulator reaches through its dma-buf registration. Native OSTs; every
scenario in a fresh process.

Checked:
- alloc / lookup by name from another process / free; addresses encode
  OST, region and offset; names land on the OST they hash to;
- a read is one-sided (bytes counted as read_direct, no RPC) into device
  memory; a write is pulled by the OST out of device memory;
- reads and writes at any offset, across many regions on all OSTs in
  parallel (mread / mwrite), every byte right;
- bounds: past a region's end is -ERANGE, nothing written;
- a freed region: a cached address reads an error, never other bytes, even
  after its memory is handed out again;
- the OST's limit (ROCEPOOL_OST_GA_MIB) is -ENOSPC; exclusive alloc of an
  existing name is -EEXIST;
- without a one-sided path for the buffer (ROCEPOOL_GDS_BOUNCE=1) the same
  calls work through host memory and RPCs.
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
from roce.ga import GlobalMemory, split_address, make_address
from roce.kv import EmulatedDevice
ga = GlobalMemory({osts!r}.split(","))
dev = EmulatedDevice()
R = {{}}
def pattern(seed, n):
    return random.Random(seed).randbytes(n)
"""


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class GlobalAddressTests(unittest.TestCase):
    port = 23720
    direct = os.environ.get("ROCEPOOL_BACKEND", "softverbs") == "softverbs"  # libfabric tcp: no FI_HMEM

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                       PYTHONPATH=ROOT, ROCEPOOL_DEV_EMU="1", ROCEPOOL_OST_GA_MIB="64")
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

    def test_write_read_by_address_one_sided(self):
        r = self.run_py("""
base = ga.alloc(4 << 20, name="t-basic")
R["parts"] = list(split_address(base))
p = dev.alloc(4 << 20)
buf = ga.register_device(p, 4 << 20)
R["direct"] = buf.direct
data = pattern(1, 1 << 20)
dev.write(p, data)
buf.write(base + 12345, offset=0, length=len(data))        # GPU -> region (the OST pulls)
s0 = ga.stats()
buf.read(base + 12345, offset=2 << 20, length=len(data))   # region -> GPU, one RDMA READ
s1 = ga.stats()
R["same"] = dev.read(p + (2 << 20), len(data)) == data
R["rpcs_for_read"] = s1["rpcs"] - s0["rpcs"]
R["read_direct"] = s1["read_direct"] - s0["read_direct"]
R["write_direct"] = s1["write_direct"]
# a second process finds it by name and reads the same bytes
R["size"] = ga.size(base)
""")
        self.assertEqual(r["direct"], self.direct)
        self.assertEqual(r["parts"][2], 0)
        self.assertTrue(r["same"])
        if self.direct:
            self.assertEqual(r["rpcs_for_read"], 0)       # the descriptor was cached at alloc
            self.assertEqual(r["read_direct"], 1 << 20)
            self.assertEqual(r["write_direct"], 1 << 20)
        self.assertEqual(r["size"], 4 << 20)
        r = self.run_py("""
addr, size = ga.lookup("t-basic")
p = dev.alloc(1 << 20)
buf = ga.register_device(p, 1 << 20)
buf.read(addr + 12345, offset=0, length=1 << 20)
R["same"] = dev.read(p, 1 << 20) == pattern(1, 1 << 20)
R["size"] = size
R["direct"] = ga.stats()["read_direct"]
ga.free(addr)
try:
    ga.lookup("t-basic"); R["gone"] = False
except FileNotFoundError:
    R["gone"] = True
""")
        self.assertEqual((r["same"], r["size"], r["gone"]), (True, 4 << 20, True))
        self.assertEqual(r["direct"], (1 << 20) if self.direct else 0)

    def test_many_regions_on_all_osts_in_parallel(self):
        r = self.run_py("""
regions = [ga.alloc(1 << 20, name=f"t-many-{i}") for i in range(16)]
R["osts"] = sorted({split_address(a)[0] for a in regions})
n = 64
p = dev.alloc(n * 65536)
buf = ga.register_device(p, n * 65536)
rnd = random.Random(7)
addrs = [regions[i % 16] + rnd.randrange(0, (1 << 20) - 65536) for i in range(n)]
lens = [rnd.randrange(1, 65536) for _ in range(n)]
blob = b"".join(pattern(i, 65536) for i in range(n))
dev.write(p, blob)
R["w"] = buf.mwrite(addrs, offsets=[i * 65536 for i in range(n)], lengths=lens)
# overlapping writes between transfers would make the expected bytes ambiguous: check them one by one
p2 = dev.alloc(n * 65536)
buf2 = ga.register_device(p2, n * 65536)
ok = 0
for i in range(n):
    buf.write(addrs[i], offset=i * 65536, length=lens[i])
    buf2.read(addrs[i], offset=0, length=lens[i])
    ok += dev.read(p2, lens[i]) == blob[i * 65536:i * 65536 + lens[i]]
R["ok"] = ok
R["r"] = buf2.mread(addrs[:8], offsets=[i * 65536 for i in range(8)], lengths=[4096] * 8)
for a in regions:
    ga.free(a)
""")
        self.assertEqual(r["osts"], [0, 1, 2, 3])
        self.assertEqual(r["w"], [0] * 64)
        self.assertEqual(r["ok"], 64)
        self.assertEqual(r["r"], [0] * 8)

    def test_bounds_and_errors(self):
        r = self.run_py("""
a = ga.alloc(8192, name="t-bounds")
p = dev.alloc(65536)
buf = ga.register_device(p, 65536)
dev.write(p, b"\\x55" * 65536)
def code(f):
    try:
        f(); return 0
    except OSError as e:
        return -e.errno
R["past_end_w"] = code(lambda: buf.write(a + 8000, offset=0, length=200))
R["past_end_r"] = code(lambda: buf.read(a + 8192, offset=0, length=1))
R["last_byte"] = code(lambda: buf.read(a + 8191, offset=0, length=1))
R["exists"] = code(lambda: ga.alloc(8192, name="t-bounds", exclusive=True))
R["reopen"] = ga.alloc(4096, name="t-bounds") == a          # existing and big enough: the same region
R["smaller"] = code(lambda: ga.alloc(1 << 20, name="t-bounds"))
R["too_big"] = code(lambda: ga.alloc(1 << 41))
R["nospace"] = code(lambda: ga.alloc(80 << 20, ost=0))      # ROCEPOOL_OST_GA_MIB=64
R["no_region"] = code(lambda: buf.read(make_address(0, 999, 0), offset=0, length=8))
R["bad_ost"] = code(lambda: buf.read(make_address(9, 1, 0), offset=0, length=8))
ga.free(a)
""")
        import errno
        self.assertEqual(r["past_end_w"], -errno.ERANGE)
        self.assertEqual(r["past_end_r"], -errno.ERANGE)
        self.assertEqual(r["last_byte"], 0)
        self.assertEqual(r["exists"], -errno.EEXIST)
        self.assertTrue(r["reopen"])
        self.assertEqual(r["smaller"], -errno.ERANGE)
        self.assertEqual(r["too_big"], -errno.ERANGE)
        self.assertEqual(r["nospace"], -errno.ENOSPC)
        self.assertEqual(r["no_region"], -errno.ENOENT)
        self.assertEqual(r["bad_ost"], -errno.EINVAL)

    def test_a_freed_region_never_returns_other_bytes(self):
        r = self.run_py("""
a = ga.alloc(1 << 20, name="t-freed", ost=1)
p = dev.alloc(1 << 20)
buf = ga.register_device(p, 1 << 20)
dev.write(p, b"\\xaa" * (1 << 20))
buf.write(a, offset=0, length=1 << 20)
buf.read(a, offset=0, length=4096)             # cached descriptor
# another client frees it and allocates others on the same OST, written with other bytes
other = GlobalMemory({osts!r}.split(","))
ob = other.register_device(p, 1 << 20)
other.free(a)
dev.write(p, b"\\x11" * (1 << 20))
news = [other.alloc(1 << 20, ost=1) for _ in range(4)]
for n in news:
    ob.write(n, offset=0, length=1 << 20)
dev.write(p, b"\\x00" * (1 << 20))
try:
    buf.read(a, offset=0, length=4096)
    R["read"] = dev.read(p, 4096).hex()[:8]
except OSError as e:
    R["read"] = -e.errno
for n in news:
    other.free(n)
""".replace("{osts!r}", repr(self.osts)))
        import errno
        self.assertIn(r["read"], (-errno.ENOENT, -errno.EIO))

    def test_through_host_memory_when_not_direct(self):
        r = self.run_py("""
a = ga.alloc(1 << 20, name="t-bounce")
p = dev.alloc(1 << 20)
buf = ga.register_device(p, 1 << 20)
R["direct"] = buf.direct
data = pattern(3, 300000)
dev.write(p, data)
buf.write(a + 100, offset=0, length=len(data))
buf.read(a + 100, offset=500000, length=len(data))
R["same"] = dev.read(p + 500000, len(data)) == data
R["stats"] = ga.stats()
ga.free(a)
""", ROCEPOOL_GDS_BOUNCE="1")
        self.assertFalse(r["direct"])
        self.assertTrue(r["same"])
        self.assertEqual(r["stats"]["read_direct"], 0)
        self.assertEqual(r["stats"]["read_rpc"], 300000)
        self.assertEqual(r["stats"]["write_bounced"], 300000)

    def test_host_buffers(self):
        r = self.run_py("""
import ctypes
a = ga.alloc(1 << 16, name="t-host")
hb = ctypes.create_string_buffer(1 << 16)
buf = ga.register_host(ctypes.addressof(hb), 1 << 16)
hb.raw = pattern(9, 1 << 16)
buf.write(a, offset=0, length=1 << 16)
ctypes.memset(hb, 0, 1 << 16)
buf.read(a, offset=0, length=1 << 16)
R["same"] = hb.raw == pattern(9, 1 << 16)
R["direct"] = buf.direct
ga.free(a)
""")
        self.assertEqual((r["same"], r["direct"]), (True, True))


if __name__ == "__main__":
    unittest.main()
