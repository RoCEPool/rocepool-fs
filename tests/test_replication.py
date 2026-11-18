"""Asynchronous replication (rocepool_servers.c's repl_*): every MDT and
OST ships its changes to a standby of its own; the client, given each
server as "primary|standby", goes to the standby once it has taken over.

Two server processes on one host: the primaries and their standbys, each
with its own logs and tiers. The client reads back, after a failover,
everything it wrote and saw replicated."""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

import test_durability as td  # noqa: E402

NM, NO = 2, 3


class _Side(td._Servers):
    """one side of the pairs: its own directory, ports, role and peers"""

    def __init__(self, lib, d, port, peer_port, role, extra=None):
        peers = {"ROCEPOOL_MDT_REPLICAS": ",".join(f"127.0.0.1:{peer_port + i}" for i in range(NM)),
                 "ROCEPOOL_OST_REPLICAS": ",".join(f"127.0.0.1:{peer_port + 50 + i}" for i in range(NO)),
                 "ROCEPOOL_REPL_ROLE": role, "ROCEPOOL_REPL_PROMOTE_S": "1.5", **(extra or {})}
        os.makedirs(d, exist_ok=True)
        super().__init__(lib, d, port, nm=NM, no=NO, env=peers)

    def addrs(self):
        return self.mdts.split(",") + self.osts.split(",")


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class ReplicationTests(unittest.TestCase):
    port = 26200

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.pa, self.pb = self.port, self.port + 100
        self.a = _Side(self.lib, os.path.join(self.tmp, "a"), self.pa, self.pb, "primary")
        self.b = _Side(self.lib, os.path.join(self.tmp, "b"), self.pb, self.pa, "standby")
        self.b.start()
        self.a.start()
        self.addCleanup(self.a.stop)
        self.addCleanup(self.b.stop)
        self.prefix = os.path.join(self.tmp, "rp")

    def env(self):
        mdts = ",".join(f"127.0.0.1:{self.pa + i}|127.0.0.1:{self.pb + i}" for i in range(NM))
        osts = ",".join(f"127.0.0.1:{self.pa + 50 + i}|127.0.0.1:{self.pb + 50 + i}" for i in range(NO))
        return dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                    ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=mdts, ROCEPOOL_OSTS=osts, ROCEPOOL_ATTR_TTL="0",
                    ROCEPOOL_FAILOVER_S="30")

    def py(self, code):
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=self.env(), timeout=600,
                             cwd=self.tmp)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr[-3000:])
        return out

    def stats(self, side):
        return [td._stats(self.lib, a) for a in side.addrs()]

    def wait_synced(self, side, timeout=60):
        """every server of `side` a primary whose standby holds all it has"""
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            st = self.stats(side)
            r = [s["repl"] for s in st]
            if all(x["role"] == "primary" and x["in_sync"] and x["queued"] == 0 and not x["resyncing"] for x in r):
                time.sleep(0.3)  # a batch in flight when it was taken off the queue
                return st
            time.sleep(0.1)
        self.fail("not in sync: " + json.dumps([s["repl"] for s in self.stats(side)]))

    WRITE = r'''
import hashlib, os, random, sys
P, seed = sys.argv[1], int(sys.argv[2])
r = random.Random(seed)
top = P + "/s%d" % seed
os.makedirs(top + "/sub/deeper")
os.makedirs(top + "/gone")
sums = {}
for i in range(60):
    n = r.choice((0, 17, 3901, 40000, (1 << 20) + 3, 3 << 20))
    b = r.randbytes(n)
    name = "%s/f%03d" % ("sub" if i % 3 else "sub/deeper", i)
    with open(top + "/" + name, "wb") as f: f.write(b)
    if i % 5 == 1 and n > 100:  # a piece overwritten in place
        with open(top + "/" + name, "r+b") as f:
            f.seek(n // 3); f.write(b"Q" * 50)
        b = b[:n // 3] + b"Q" * 50 + b[n // 3 + 50:]
    if i % 7 == 2:
        os.truncate(top + "/" + name, n // 2); b = b[:n // 2]
    sums["s%d/%s" % (seed, name)] = hashlib.sha256(b).hexdigest()
for name in list(sums)[::6]:
    os.unlink(P + "/" + name); del sums[name]
os.rmdir(top + "/gone")
print(repr(sums))
'''

    READ = r'''
import hashlib, os, sys
P = sys.argv[1]
got = {}
for top, dirs, files in os.walk(P):
    for n in files:
        p = os.path.join(top, n)
        got[os.path.relpath(p, P)] = hashlib.sha256(open(p, "rb").read()).hexdigest()
dirs = sorted(os.path.relpath(os.path.join(t, d), P) for t, ds, _ in os.walk(P) for d in ds)
print(repr(got)); print(repr(dirs))
'''

    def write(self, seed):
        out = subprocess.run([sys.executable, "-c", self.WRITE, self.prefix, str(seed)], capture_output=True, text=True,
                             env=self.env(), timeout=600, cwd=self.tmp)
        self.assertEqual(out.returncode, 0, out.stderr[-3000:])
        return eval(out.stdout.strip())

    def read(self):
        out = subprocess.run([sys.executable, "-c", self.READ, self.prefix], capture_output=True, text=True,
                             env=self.env(), timeout=600, cwd=self.tmp)
        self.assertEqual(out.returncode, 0, out.stderr[-3000:])
        lines = out.stdout.strip().splitlines()
        return eval(lines[0]), eval(lines[1]), out.stderr

    def wait_role(self, side, role, timeout=30):
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            try:
                if all(s["repl"]["role"] == role for s in self.stats(side)):
                    return
            except Exception:
                pass
            time.sleep(0.1)
        self.fail(f"not all {role}")

    def test_a_failover_keeps_everything_that_was_replicated(self):
        sums = self.write(1)
        st = self.wait_synced(self.a)
        self.assertGreater(sum(s["repl"]["items"] for s in st), 50)
        # the standby answered nobody but its primary
        sb = self.stats(self.b)
        self.assertTrue(all(s["repl"]["role"] == "standby" and s["repl"]["synced"] for s in sb), sb)
        self.assertGreater(sum(s["repl"]["applied"] for s in sb), 50)
        self.a.kill()  # the primaries die (SIGKILL)
        t0 = time.monotonic()
        got, dirs, err = self.read()  # the client fails over as the standbys take over
        took = time.monotonic() - t0
        self.assertEqual(got, sums)
        self.assertEqual(dirs, ["s1", "s1/sub", "s1/sub/deeper"])
        self.assertIn("unreachable", err)
        self.assertLess(took, 30)
        st = self.stats(self.b)
        self.assertTrue(all(s["repl"]["role"] == "primary" and s["repl"]["term"] >= 2 for s in st), st)
        # and the new primaries take changes
        self.py(f"open({self.prefix!r} + '/s1/after', 'wb').write(b'z' * 70000)")
        got, _, _ = self.read()
        self.assertEqual(got["s1/after"], hashlib.sha256(b"z" * 70000).hexdigest())

    def test_a_standby_that_was_away_gets_what_was_queued(self):
        sums = self.write(2)
        self.wait_synced(self.a)
        self.b.kill()  # the standbys go away; changes go on, queued
        more = self.write(3)
        drop = sorted(sums)[:5]
        self.py("import os\n" + "".join(f"os.unlink({self.prefix!r} + '/' + {n!r})\n" for n in drop))
        for n in drop:
            del sums[n]
        sums.update(more)
        st = self.stats(self.a)
        self.assertGreater(sum(s["repl"]["queued"] for s in st), 0)
        self.b.start()  # back: the queue drains into it, deletes included
        st = self.wait_synced(self.a)
        self.assertGreater(sum(s["repl"]["errors"] for s in st), 0)  # sends that failed while it was away
        self.a.kill()
        got, _, _ = self.read()
        self.assertEqual(got, sums)

    def test_a_standby_is_resynced_when_the_queue_is_gone(self):
        """the primaries restart while their standbys are away: what they
        queued is lost, so the standbys are compared key by key, sent what
        differs, and have what was deleted meanwhile removed"""
        sums = self.write(6)
        self.wait_synced(self.a)
        self.b.kill()
        more = self.write(7)
        drop = sorted(sums)[:5]
        self.py("import os\n" + "".join(f"os.unlink({self.prefix!r} + '/' + {n!r})\n" for n in drop))
        for n in drop:
            del sums[n]
        sums.update(more)
        self.a.kill()
        self.a.start()
        self.b.start()
        st = self.wait_synced(self.a)
        self.assertTrue(all(s["repl"]["resyncs"] >= 1 for s in st), [s["repl"] for s in st])
        self.assertGreater(sum(s["repl"]["mismatched"] for s in st), 0)
        sb = self.stats(self.b)
        self.assertGreater(sum(s["repl"]["swept"] for s in sb), 0, [s["repl"] for s in sb])
        self.a.kill()
        got, _, _ = self.read()
        self.assertEqual(got, sums)

    def test_the_old_primary_comes_back_as_a_standby(self):
        sums = self.write(4)
        self.wait_synced(self.a)
        self.a.kill()
        self.wait_role(self.b, "primary")
        sums.update(self.write(5))  # on the new primaries
        self.a.start()  # its saved role says primary; the pair says otherwise
        self.wait_role(self.a, "standby")
        st = self.wait_synced(self.b)
        self.assertTrue(all(s["repl"]["term"] >= 2 for s in st), st)
        sa = self.stats(self.a)
        self.assertTrue(all(s["repl"]["demotions"] >= 1 or s["repl"]["role"] == "standby" for s in sa), sa)
        # now the other way round: B dies, A (synced) takes over again
        self.b.kill()
        got, _, _ = self.read()
        self.assertEqual(got, sums)
        self.wait_role(self.a, "primary")

    def test_a_standby_refuses_clients(self):
        from roce.protocol import Frame, Op
        from roce.roce_transport import RoCETransport
        os.environ["ROCE_TRANSPORT_LIB"] = self.lib
        r = RoCETransport().send_request(self.b.osts.split(",")[0], Frame(op=Op.GET, request_id=1, key="x", value=b""))
        self.assertEqual(r.op, Op.ERROR)
        self.assertTrue(r.value.startswith(b"STANDBY"), r.value)


if __name__ == "__main__":
    unittest.main()
