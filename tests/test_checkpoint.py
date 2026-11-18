"""Log checkpoints (ROCEPOOL_LOG_CHECKPOINT_MIB, rocepool_servers.c's
logmgr): the servers switch their log to a new segment, write their whole
store to <log>.ckpt while serving, and delete the segments it covers. A
restart loads the checkpoint and replays only the segments after it.

The durability suite runs again with a checkpoint every few KiB of log (so
nearly every crash lands next to or inside one), plus crashes in the middle
of a load with checkpoints running, and the bound on what stays on disk."""
import glob
import hashlib
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

import test_durability as td  # noqa: E402  (as a module: its own classes are not collected here again)

ASYNC, _Servers, _stats = td.ASYNC, td._Servers, td._stats

# a checkpoint once a segment holds 16 KiB, looked at every 20 ms
CKPT = {"ROCEPOOL_LOG_CHECKPOINT_BYTES": "16384", "ROCEPOOL_LOG_CHECKPOINT_POLL_MS": "20"}


def _segments(d: str) -> list[str]:
    return sorted(p for p in glob.glob(os.path.join(d, "*.log*")) if not p.endswith((".ckpt", ".tmp", ".calibrate")))


class _CkptMixin:
    def assert_checkpointed(self):
        st = [_stats(self.lib, a) for a in self.srv.mdts.split(",") + self.srv.osts.split(",")]
        self.assertTrue(all(s["checkpoint"]["enabled"] for s in st), st)
        self.assertTrue(all(s["checkpoint"]["failures"] == 0 for s in st), [s["checkpoint"] for s in st])
        self.assertGreater(sum(s["checkpoint"]["checkpoints"] for s in st), 0, [s["checkpoint"] for s in st])
        return st


class CheckpointRestartTests(_CkptMixin, td.DurableRestartTests):
    """every crash of the durability suite, with checkpoints all along"""
    port = 25100
    SERVER_ENV = CKPT

    def test_everything_acknowledged_survives_a_crash(self):
        # the base test crashes twice; checkpoints were taken before both
        orig_kill = self.srv.kill
        kills = []

        def kill():
            if not kills:  # the first life wrote enough for checkpoints; the second only a little
                self.assert_checkpointed()
            kills.append(1)
            orig_kill()
        self.srv.kill = kill
        super().test_everything_acknowledged_survives_a_crash()
        self.assertTrue(glob.glob(os.path.join(self.tmp, "*.ckpt")))

    def test_a_torn_log_tail_is_dropped(self):
        """the torn record lands at the end of each server's current segment"""
        self.py(f"import os; os.makedirs({self.prefix!r} + '/t'); open({self.prefix!r} + '/t/a', 'wb').write(b'a' * 10)")
        self.srv.kill()
        for m in ("mdt0", "mdt1"):
            segs = sorted(glob.glob(os.path.join(self.tmp, m + ".log*")))
            segs = [s for s in segs if not s.endswith((".ckpt", ".tmp"))]
            cur = max(segs, key=lambda s: int(s.rsplit(".", 1)[1]) if s.rsplit(".", 1)[1].isdigit() else 0)
            with open(cur, "ab") as f:
                f.write(b"\x40\x00\x00\x00junk")
        self.srv.start()
        self.assertEqual(self.py(f"print(open({self.prefix!r} + '/t/a', 'rb').read())").strip(), "b'aaaaaaaaaa'")
        self.py(f"open({self.prefix!r} + '/t/b', 'wb').write(b'b')")
        self.srv.kill()
        self.srv.start()
        self.assertEqual(self.py(f"import os; print(sorted(os.listdir({self.prefix!r} + '/t')))").strip(), "['a', 'b']")


class CheckpointRestartAsyncTests(_CkptMixin, td.DurableRestartAsyncTests):
    """the same with Lustre-style asynchronous creates and unlinks"""
    port = 25200
    SERVER_ENV = dict(ASYNC, **CKPT)
    test_a_torn_log_tail_is_dropped = CheckpointRestartTests.test_a_torn_log_tail_is_dropped


class CheckpointAsyncReplayTests(td.AsyncReplayTests):
    """clients' replay of uncommitted creates and unlinks after an MDT
    crash, with checkpoints taken in the life the changes were made in: the
    MDT still knows which of them its log has (transnos count per life)"""
    port = 25300

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.srv = _Servers(self.lib, self.tmp, self.port, nm=2, no=2,
                            env=dict(ASYNC, ROCEPOOL_MDT_RECOVERY_S="2", **CKPT))
        self.srv.start()
        self.addCleanup(self.srv.stop)
        self.prefix = os.path.join(self.tmp, "rp")


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class CheckpointUnderLoadTests(unittest.TestCase):
    """A writer records every file whose close returned; the servers are
    killed at random moments while it runs (inside checkpoints too) and
    restarted; every recorded file must read back exactly."""
    port = 25400

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.srv = _Servers(self.lib, self.tmp, self.port, nm=2, no=3, env=dict(CKPT))
        self.srv.start()
        self.addCleanup(self.srv.stop)
        self.prefix = os.path.join(self.tmp, "rp")
        self.done = os.path.join(self.tmp, "done.txt")

    def env(self):
        return dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                    ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                    ROCEPOOL_ATTR_TTL="0")

    WRITER = r'''
import hashlib, os, random, sys
P, done, seed = sys.argv[1], sys.argv[2], int(sys.argv[3])
r = random.Random(seed)
os.makedirs(P + "/w%d" % seed, exist_ok=True)
log = open(done, "a")
i = 0
while True:
    name = "w%d/f%05d" % (seed, i)
    n = r.choice((0, 17, 3901, 40000, 300000))
    b = r.randbytes(n)
    try:
        with open(P + "/" + name, "wb") as f:
            f.write(b)
        if i % 7 == 3:  # some overwritten, some removed
            with open(P + "/" + name, "r+b") as f:
                f.seek(n // 2); f.write(b"Z" * 10)
            b = b[:n // 2] + b"Z" * 10 + b[n // 2 + 10:]
        if i % 11 == 5:
            os.unlink(P + "/" + name)
            log.write("gone %s\n" % name)
        else:
            log.write("ok %s %s\n" % (name, hashlib.sha256(b).hexdigest()))
        log.flush(); os.fsync(log.fileno())
    except OSError:
        sys.exit(0)  # the servers went away mid-call: that one is not recorded
    i += 1
'''

    def verify(self):
        want = {}
        for ln in open(self.done):
            p = ln.split()
            if p[0] == "ok":
                want[p[1]] = p[2]
            else:
                want[p[1]] = None
        code = f"""
import hashlib, os, sys
P = {self.prefix!r}
bad = []
for ln in open({self.done!r}):
    p = ln.split()
    path = P + "/" + p[1]
    if p[0] == "ok":
        h = hashlib.sha256(open(path, "rb").read()).hexdigest() if os.path.exists(path) else "missing"
        if h != p[2]: bad.append((p[1], h))
    elif os.path.exists(path):
        bad.append((p[1], "still there"))
print(len(bad), bad[:5])
"""
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=self.env(), timeout=600)
        self.assertEqual(out.returncode, 0, out.stderr[-3000:])
        self.assertTrue(out.stdout.startswith("0 "), out.stdout)
        return len(want)

    def test_crashes_during_checkpoints_lose_nothing_acknowledged(self):
        rnd = random.Random(42)
        total_ckpt = 0
        for rnd_i in range(4):
            ws = [subprocess.Popen([sys.executable, "-c", self.WRITER, self.prefix, self.done, str(s)], env=self.env(),
                                   cwd=self.tmp, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                  for s in (rnd_i * 2, rnd_i * 2 + 1)]
            time.sleep(rnd.uniform(2.0, 5.0))
            st = [_stats(self.lib, a) for a in self.srv.mdts.split(",") + self.srv.osts.split(",")]
            total_ckpt += sum(s["checkpoint"]["checkpoints"] for s in st)
            self.assertTrue(all(s["checkpoint"]["failures"] == 0 for s in st), [s["checkpoint"] for s in st])
            self.srv.kill()
            for w in ws:
                w.wait(timeout=120)
            self.srv.start()
            n = self.verify()
        self.assertGreater(total_ckpt, 4)
        self.assertGreater(n, 50)
        # what stays on disk: the checkpoint and the segments after it,
        # not the whole history
        segs = _segments(self.tmp)
        per_server = {}
        for p in segs:
            per_server.setdefault(os.path.basename(p).split(".")[0], []).append(p)
        self.assertTrue(all(len(v) <= 3 for v in per_server.values()), per_server)

    def test_the_log_stays_bounded_and_restarts_from_the_checkpoint(self):
        code = f"""
import os
P = {self.prefix!r}
os.makedirs(P + "/m")
for i in range(3000):
    with open(P + "/m/f%05d" % i, "wb") as f: f.write(b"%05d" % i * 7)
for i in range(0, 3000, 3): os.unlink(P + "/m/f%05d" % i)
"""
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=self.env(), timeout=600)
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        time.sleep(1.0)  # the last checkpoints
        st = [_stats(self.lib, a) for a in self.srv.mdts.split(",") + self.srv.osts.split(",")]
        self.assertTrue(all(s["checkpoint"]["checkpoints"] >= 2 for s in st[:2]), [s["checkpoint"] for s in st])
        self.assertTrue(all(s["checkpoint"]["segments_deleted"] >= 1 for s in st[:2]), [s["checkpoint"] for s in st])
        # what is left of the log is the current segment (its files are
        # zeros written ahead in direct mode: their size says nothing)
        logged = sum(s["log"]["bytes"] for s in st[:2])
        left = sum(s["checkpoint"]["segment_bytes"] for s in st[:2])
        self.assertLess(left, logged / 4, (left, logged))
        per_server = {}
        for p in _segments(self.tmp):
            per_server.setdefault(os.path.basename(p).split(".")[0], []).append(os.path.basename(p))
        self.assertTrue(all(len(v) <= 2 for k, v in per_server.items() if k.startswith("mdt")), per_server)
        self.srv.kill()
        self.srv.start()
        self.assertIn("checkpoint", self.srv.err + "")  # the previous life's stderr mentions them
        check = f"""
import os
P = {self.prefix!r}
names = sorted(os.listdir(P + "/m"))
assert names == ["f%05d" % i for i in range(3000) if i % 3], len(names)
for n in names:
    i = int(n[1:])
    assert open(P + "/m/" + n, "rb").read() == b"%05d" % i * 7, n
print("ok", len(names))
"""
        out = subprocess.run([sys.executable, "-c", check], capture_output=True, text=True, env=self.env(), timeout=600)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr[-2000:])
        self.assertEqual(out.stdout.strip(), "ok 2000")
        st = [_stats(self.lib, a) for a in self.srv.mdts.split(",")]
        # most of the history came from the checkpoint, not the log
        self.assertTrue(all(s["checkpoint"]["segment"] >= 2 for s in st), [s["checkpoint"] for s in st])


if __name__ == "__main__":
    unittest.main()
