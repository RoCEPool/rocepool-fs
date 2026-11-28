"""The exiom backend of the global address client library
(roce/native/rocepool_ga_exiom.c): an entry of the list that is an exiom
memory server pair, "exiom://tenant@host:port|host:port".

The memory server is tests/fakes/exiom_server.py: the control protocol of
exiom/connect.py and a responder-only queue pair per connection, connected
by hand (roce_raw_qp_*, no CM -- the FPGA has none) on softverbs. Two paths
(127.0.0.2 and 127.0.0.3, as the primary and the backup memory server), two
tenants. No GPU: ROCEPOOL_DEV_EMU=1, the emulated device whose memory
faults on any CPU access. Every scenario in a fresh process.

Checked:
- values by key read and written by address, one RDMA READ / WRITE into and
  out of device memory (bytes counted as direct, a handful of control
  requests in all); another process finds the value by its key;
- stage / commit: a new value is not visible before its commit; a reader
  holding the old value's address reads the old bytes during its lease, and
  -ENOENT after it (never the new value's bytes); a new lookup gives the new
  value;
- errors: exclusive alloc of a key that exists, offsets and lengths not
  multiples of 4, past the end, a key of another tenant, an unknown tenant,
  no space left;
- a path cut (the primary): the next transfer and the next request go
  through the other path, the bytes are right;
- without a one-sided path for the buffer (ROCEPOOL_GDS_BOUNCE=1) the same
  calls bounce through host memory;
- many transfers over two memory servers in parallel (mread / mwrite).
"""
import json
import os
import random
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PRELUDE = r"""
import ctypes, errno, json, os, random, socket, struct, sys, time
sys.path.insert(0, {root!r})
from roce.ga import GlobalMemory, split_address
from roce.kv import EmulatedDevice
PATHS = {paths!r}
def exiom(tenant):
    return "exiom://%s@%s" % (tenant, PATHS)
dev = EmulatedDevice()
R = {{}}
def pattern(seed, n):
    return random.Random(seed).randbytes(n)
def control(via, **m):
    host, port = PATHS.split("|")[via].rsplit(":", 1)
    s = socket.create_connection((host, int(port)))
    d = json.dumps(m).encode()
    s.sendall(struct.pack(">I", len(d)) + d)
    n = struct.unpack(">I", s.recv(4))[0]
    r = json.loads(s.recv(n))
    s.close()
    return r
def errno_of(fn):
    try:
        fn()
    except OSError as e:
        return e.errno
    return 0
"""


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
@unittest.skipUnless(os.environ.get("ROCEPOOL_BACKEND", "softverbs") == "softverbs", "exiom: verbs backend only")
class ExiomGlobalAddressTests(unittest.TestCase):
    def setUp(self):
        # a fresh memory server per scenario: a cut path stays cut
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        port = random.Random(os.getpid() ^ id(self)).randrange(20000, 30000)
        self.env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                        PYTHONPATH=ROOT, ROCEPOOL_DEV_EMU="1", SOFTVERBS_RAW_PORT=str(port),
                        EXIOM_EMU_LEASE="1.0")
        self.srv = subprocess.Popen(
            [sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "exiom_server.py"), ROOT,
             "team-a:1:16777216,team-b:2:1048576", f"127.0.0.2:{port + 1}", f"127.0.0.3:{port + 1}"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=self.env)
        self.paths = self.srv.stdout.readline().split()[-1]

    def tearDown(self):
        self.srv.stdin.close()
        self.srv.wait(timeout=20)
        self.srv.stdout.close()

    def run_py(self, body: str, **env) -> dict:
        code = PRELUDE.format(root=ROOT, paths=self.paths) + body + "\nprint('RESULT ' + json.dumps(R))\n"
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=300,
                             env=dict(self.env, **env))
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        line = next(l for l in out.stdout.splitlines() if l.startswith("RESULT "))
        return json.loads(line[7:])

    def test_by_key_then_by_address_into_device(self):
        r = self.run_py("""
ga = GlobalMemory([exiom("team-a")])
a = ga.alloc(1 << 20, name="kv/blk-1", ost=0)
R["parts"] = list(split_address(a))
p = dev.alloc(4 << 20)
buf = ga.register_device(p, 4 << 20)
R["direct"] = buf.direct
data = pattern(1, 1 << 20)
dev.write(p, data)
buf.write(a, offset=0, length=len(data))                       # device -> memory server, one RDMA WRITE
s0 = ga.stats()
buf.read(a + 4096, offset=2 << 20, length=len(data) - 4096)    # memory server -> device, one RDMA READ
s1 = ga.stats()
R["same"] = dev.read(p + (2 << 20), len(data) - 4096) == data[4096:]
R["read_direct"] = s1["read_direct"] - s0["read_direct"]
R["requests"] = s1["rpcs"]
R["write_direct"] = s1["write_direct"]
# another process (handle): by its key
other = GlobalMemory([exiom("team-a")])
b, size = other.lookup("kv/blk-1", ost=0)
q = dev.alloc(1 << 20)
obuf = other.register_device(q, 1 << 20)
obuf.read(b, offset=0, length=size)
R["other"] = dev.read(q, size) == data
R["size"] = size
R["path"] = list(ga.path(0))
""")
        self.assertEqual(r["parts"][0], 0)
        self.assertTrue(r["direct"])
        self.assertTrue(r["same"])
        self.assertTrue(r["other"])
        self.assertEqual(r["size"], 1 << 20)
        self.assertEqual(r["read_direct"], (1 << 20) - 4096)
        self.assertEqual(r["write_direct"], 1 << 20)
        self.assertLessEqual(r["requests"], 4)   # connect, lookup, alloc, commit: no data through the server
        self.assertEqual(r["path"], [0, 0])

    def test_stage_commit_and_leases(self):
        r = self.run_py("""
w = GlobalMemory([exiom("team-a")])
rd = GlobalMemory([exiom("team-a")])
p = dev.alloc(1 << 20)
wb = w.register_device(p, 1 << 20)
rb = rd.register_device(p + (512 << 10), 512 << 10)
v1, v2 = pattern(1, 65536), pattern(2, 65536)
a1 = w.stage("model/layer0", 65536, ost=0)
R["before_commit"] = errno_of(lambda: rd.lookup("model/layer0", ost=0))
dev.write(p, v1)
wb.write(a1, offset=0, length=65536)
w.commit(a1)
old, _ = rd.lookup("model/layer0", ost=0)
rb.read(old, offset=0, length=65536)
R["v1"] = dev.read(p + (512 << 10), 65536) == v1
a2 = w.stage("model/layer0", 65536, ost=0)                      # a new value: another place
R["new_place"] = a2 != a1
dev.write(p, v2)
wb.write(a2, offset=0, length=65536)
w.commit(a2)
rb.read(old, offset=0, length=65536)                            # within the lease: the old value
R["old_in_lease"] = dev.read(p + (512 << 10), 65536) == v1
time.sleep(1.3)
R["old_after_lease"] = errno_of(lambda: rb.read(old, offset=0, length=65536))
new, _ = rd.lookup("model/layer0", ost=0)
rb.read(new, offset=0, length=65536)
R["v2"] = dev.read(p + (512 << 10), 65536) == v2
""")
        self.assertEqual(r["before_commit"], 2)          # ENOENT
        self.assertTrue(r["v1"])
        self.assertTrue(r["new_place"])
        self.assertTrue(r["old_in_lease"])
        self.assertEqual(r["old_after_lease"], 2)       # ENOENT, not the new bytes
        self.assertTrue(r["v2"])

    def test_errors(self):
        r = self.run_py("""
ga = GlobalMemory([exiom("team-a")])
a = ga.alloc(4096, name="e/one", ost=0)
p = dev.alloc(8192)
buf = ga.register_device(p, 8192)
R["exists"] = errno_of(lambda: ga.alloc(4096, name="e/one", ost=0, exclusive=True))
R["open_existing"] = ga.alloc(4096, name="e/one", ost=0) is not None
R["unaligned_offset"] = errno_of(lambda: buf.read(a + 2, offset=0, length=8))
R["unaligned_length"] = errno_of(lambda: buf.write(a, offset=0, length=6))
R["past_end"] = errno_of(lambda: buf.read(a + 4092, offset=0, length=8))
b = GlobalMemory([exiom("team-b")])
R["other_tenant"] = errno_of(lambda: b.lookup("e/one", ost=0))
z = GlobalMemory([exiom("nobody")])
R["unknown_tenant"] = errno_of(lambda: z.alloc(64, name="x", ost=0))
R["no_space"] = errno_of(lambda: b.alloc(2 << 20, name="big", ost=0))
R["stage_on_nothing"] = errno_of(lambda: ga.commit(a))
ga.free(a)
R["freed"] = errno_of(lambda: GlobalMemory([exiom("team-a")]).lookup("e/one", ost=0))
""")
        self.assertEqual(r["exists"], 17)              # EEXIST
        self.assertTrue(r["open_existing"])
        self.assertEqual(r["unaligned_offset"], 22)    # EINVAL
        self.assertEqual(r["unaligned_length"], 22)
        self.assertEqual(r["past_end"], 34)            # ERANGE
        self.assertEqual(r["other_tenant"], 2)         # ENOENT: tenants do not see each other's keys
        self.assertEqual(r["unknown_tenant"], 13)      # EACCES
        self.assertEqual(r["no_space"], 28)            # ENOSPC
        self.assertEqual(r["stage_on_nothing"], 22)    # committed already
        self.assertEqual(r["freed"], 2)

    def test_path_cut(self):
        r = self.run_py("""
ga = GlobalMemory([exiom("team-a")])
data = pattern(7, 262144)
a = ga.alloc(len(data), name="f/blk", ost=0)
p = dev.alloc(1 << 20)
buf = ga.register_device(p, 1 << 20)
dev.write(p, data)
buf.write(a, offset=0, length=len(data))
R["before"] = list(ga.path(0))
R["cut"] = control(1, op="emu_cut", endpoint=0)["ok"]          # the primary goes away
buf.read(a, offset=len(data), length=len(data))                 # through the backup
R["read"] = dev.read(p + len(data), len(data)) == data
R["after"] = list(ga.path(0))
b = ga.alloc(4096, name="f/new", ost=0)                         # requests too
buf.write(b, offset=0, length=4096)
R["new"] = True
""")
        self.assertEqual(r["before"], [0, 0])
        self.assertTrue(r["cut"])
        self.assertTrue(r["read"])
        self.assertEqual(r["after"], [1, 1])
        self.assertTrue(r["new"])

    def test_bounced(self):
        r = self.run_py("""
ga = GlobalMemory([exiom("team-a")])
data = pattern(3, 6 << 20)                                     # more than the 4 MiB bounce buffer
a = ga.alloc(len(data), name="b/blk", ost=0)
p = dev.alloc(16 << 20)
buf = ga.register_device(p, 16 << 20)
R["direct"] = buf.direct
dev.write(p, data)
buf.write(a, offset=0, length=len(data))
buf.read(a, offset=8 << 20, length=len(data))
R["same"] = dev.read(p + (8 << 20), len(data)) == data
R["stats"] = ga.stats()
""", ROCEPOOL_GDS_BOUNCE="1")
        self.assertFalse(r["direct"])
        self.assertTrue(r["same"])
        self.assertEqual(r["stats"]["read_rpc"], 6 << 20)       # bounced through host memory
        self.assertEqual(r["stats"]["write_bounced"], 6 << 20)
        self.assertEqual(r["stats"]["read_direct"], 0)

    def test_many_over_two_servers(self):
        r = self.run_py("""
ga = GlobalMemory([exiom("team-a"), exiom("team-b")])          # two memory servers (here: two tenants)
rng = random.Random(5)
regions = [ga.alloc(65536, name="m/%d" % i, ost=i % 2) for i in range(8)]
n = 32
addrs, lens, data = [], [], []
for i in range(n):
    reg = regions[rng.randrange(8)]
    off = 4 * rng.randrange(0, 4096)
    ln = 4 * rng.randrange(1, (65536 - off) // 4 + 1)
    addrs.append(reg + off); lens.append(ln); data.append(pattern(100 + i, ln))
# distinct, non-overlapping targets: keep only the first of overlapping ones
keep, used = [], []
for i in range(n):
    s = addrs[i]; e = s + lens[i]
    if all(e <= a or s >= b for a, b in used):
        keep.append(i); used.append((s, e))
p = dev.alloc(4 << 20)
buf = ga.register_device(p, 4 << 20)
offs, o = [], 0
for i in keep:
    dev.write(p + o, data[i]); offs.append(o); o += lens[i]
st = buf.mwrite([addrs[i] for i in keep], offsets=offs, lengths=[lens[i] for i in keep])
R["write_status"] = st
st = buf.mread([addrs[i] for i in keep], offsets=[x + (2 << 20) for x in offs], lengths=[lens[i] for i in keep])
R["read_status"] = st
R["same"] = all(dev.read(p + (2 << 20) + offs[k], lens[i]) == data[i] for k, i in enumerate(keep))
R["count"] = len(keep)
R["servers"] = sorted({split_address(addrs[i])[0] for i in keep})
""")
        self.assertTrue(all(s == 0 for s in r["write_status"]), r["write_status"])
        self.assertTrue(all(s == 0 for s in r["read_status"]), r["read_status"])
        self.assertTrue(r["same"])
        self.assertGreater(r["count"], 4)
        self.assertEqual(r["servers"], [0, 1])


if __name__ == "__main__":
    unittest.main()
