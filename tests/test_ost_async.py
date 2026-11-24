"""Stripe writes durable at fsync / close, not one by one
(ROCEPOOL_OST_ASYNC=1): a durable OST replies to a PUT / WRITE_AT once its
bytes are on the tier and its log record appended; the client keeps the
OST's epoch and the record's log position, and fsync, close and exit send a
commit barrier (OP_OST_COMMIT) to every OST it has such writes on.

IO500's ior-hard-write runs IOR with -e (fsync at close): its 47008-byte
writes need to be durable at the fsync. Durable one by one, every write
waited for its OST's commit (tier fdatasync + log), ~13 writes per commit at
1754 us on 6 x i3en: 3.56 GiB/s against 9.4 not durable (SC26,
gh-41438027754).

Checked here, through the interception library over native servers on logs
and tiers:
- ior-hard's shape (one shared file, pieces of 47008 bytes) goes out
  acknowledged early, with barriers at fsync/close, and every byte survives
  the OSTs being SIGKILLed right after the closes (IO500 rule 6);
- a close without fsync waits for the barrier too (a SIGKILL keeps the
  page cache, so a skipped fdatasync shows only in the barrier counts);
- an OST restarted between a write and the close: the close fails with EIO
  -- never a close that says durable when it is not;
- off (the default) sends what it sent before: no early replies.
"""
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
from tests.test_durability import _Servers, _stats  # noqa: E402

PIECE = 47008

WRITER = """
import os, sys
P, r, ranks, n, sync = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5] == "1"
fd = os.open(P + "/d/shared", os.O_CREAT | os.O_WRONLY, 0o644)
for i in range(n):                                  # IOR -s n, segmented: rank r's piece of segment i
    os.pwrite(fd, bytes([(r * 7 + i) %% 251]) * %d, (i * ranks + r) * %d)
if sync:
    os.fsync(fd)                                    # IOR -e
os.close(fd)
""" % (PIECE, PIECE)

CHECK = """
import os, sys
P, ranks, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
b = open(P + "/d/shared", "rb").read()
bad = [(r, i) for r in range(ranks) for i in range(n)
       if b[(i * ranks + r) * %d:(i * ranks + r + 1) * %d] != bytes([(r * 7 + i) %% 251]) * %d]
print(len(b), len(bad))
""" % (PIECE, PIECE, PIECE)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OstAsyncTests(unittest.TestCase):
    port = 26400

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.srv = _Servers(self.lib, self.tmp, self.port, nm=1, no=3)
        self.srv.start()
        self.addCleanup(self.srv.stop)
        self.prefix = os.path.join(self.tmp, "rp")
        type(self).port += 20

    def env(self, on=True):
        return dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                    ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                    ROCEPOOL_ATTR_TTL="0", ROCEPOOL_INTERCEPT_LOG="1", ROCEPOOL_DIRTY_CAP_MIB="1",
                    ROCEPOOL_STRIPE_SIZE=str(1 << 20), ROCEPOOL_OST_ASYNC="1" if on else "0")

    def py(self, code, *args, on=True, check=True):
        r = subprocess.run([sys.executable, "-c", code, self.prefix, *map(str, args)], capture_output=True,
                           text=True, env=self.env(on), cwd=self.tmp, timeout=300)
        if check:
            self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-3000:])
        return r

    def ost_stats(self):
        out = {"async_writes": 0, "commit_barriers": 0}
        for a in self.srv.osts.split(","):
            st = _stats(self.lib, a)
            for k in out:
                out[k] += st.get(k, 0)
        return out

    def ranks(self, ranks, n, sync=True, on=True):
        ps = [subprocess.Popen([sys.executable, "-c", WRITER, self.prefix, str(r), str(ranks), str(n),
                                "1" if sync else "0"], env=self.env(on), cwd=self.tmp, stderr=subprocess.PIPE,
                               text=True) for r in range(ranks)]
        errs = [p.communicate(timeout=300)[1] for p in ps]
        self.assertEqual([p.returncode for p in ps], [0] * ranks, "\n".join(e[-1500:] for e in errs))
        return errs

    def check(self, ranks, n):
        out = self.py(CHECK, ranks, n, on=False)
        self.assertEqual(out.stdout.split(), [str(ranks * n * PIECE), "0"], out.stderr[-2000:])

    def test_ior_hard_shape_durable_at_fsync(self):
        self.py('import os, sys; os.makedirs(sys.argv[1] + "/d")')
        before = self.ost_stats()
        errs = self.ranks(4, 60)
        st = self.ost_stats()
        self.assertGreater(st["async_writes"] - before["async_writes"], 0, st)
        self.assertGreater(st["commit_barriers"] - before["commit_barriers"], 0, st)
        self.assertTrue(all("ost_lost=0" in e for e in errs), errs[0][-600:])
        self.srv.kill()          # SIGKILL right after the closes: nothing more is synced
        self.srv.start()
        self.check(4, 60)

    def test_close_without_fsync_is_durable_too(self):
        self.py('import os, sys; os.makedirs(sys.argv[1] + "/d")')
        errs = self.ranks(3, 40, sync=False)
        # every close sent its barrier (SIGKILL keeps the page cache, so the
        # restart below cannot tell a skipped fdatasync; the barrier can)
        for e in errs:
            n = int(e.split("ost_barriers=")[1].split()[0])
            self.assertGreater(n, 0, e[-600:])
        self.srv.kill()
        self.srv.start()
        self.check(3, 40)

    def test_off_sends_what_it_sent_before(self):
        self.py('import os, sys; os.makedirs(sys.argv[1] + "/d")')
        before = self.ost_stats()
        self.ranks(2, 30, on=False)
        st = self.ost_stats()
        self.assertEqual(st["async_writes"], before["async_writes"])
        self.check(2, 30)

    def test_an_ost_restart_before_the_close_fails_the_close(self):
        self.py('import os, sys; os.makedirs(sys.argv[1] + "/d")')
        go = os.path.join(self.tmp, "go")
        code = """
import os, sys, time
P, go = sys.argv[1], sys.argv[2]
fd = os.open(P + "/d/f", os.O_CREAT | os.O_WRONLY)
for i in range(60):
    os.pwrite(fd, b"x" * %d, i * %d)          # past the dirty cap: sent before the close
print("written", flush=True)
while not os.path.exists(go):
    time.sleep(0.05)
try:
    os.close(fd)
    print("closed")
except OSError as e:
    print("close", e.errno)
""" % (PIECE, PIECE)
        p = subprocess.Popen([sys.executable, "-c", code, self.prefix, go], env=self.env(), cwd=self.tmp,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(p.stdout.readline().strip(), "written")
        self.srv.kill()
        self.srv.start()
        open(go, "w").close()
        out, err = p.communicate(timeout=120)
        self.assertEqual(out.strip(), "close 5", err[-2000:])   # EIO
        self.assertIn("ost_lost=", err)
        self.assertNotIn("ost_lost=0", err)


if __name__ == "__main__":
    unittest.main()
