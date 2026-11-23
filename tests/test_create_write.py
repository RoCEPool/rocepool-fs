"""One MDT request per small file: open(O_CREAT) + write + close sends a
single create_write (MDT op 15) instead of a create at the open and a
write_inline at the close (ROCEPOOL_DEFER_CREATE=1, on in IO500 runs).

mdtest-hard's write phase on 6 x i3en (SC26, run gh-41438027754) took
2.0 MDT requests per file and was bound by each rank's latency, one file
after another.

Checked here, through the interception library over native servers on
logs and tiers:
- mdtest's shape takes one MDT request per file, and the files read back;
- durable at close: an MDT killed right after the closes keeps every file
  (IO500 rule 6);
- this process sees its own file before the first write (stat, listdir,
  unlink), and an unwritten file exists after its close;
- O_EXCL, O_TRUNC and O_APPEND opens are not deferred; IOR's shared file
  (every rank O_CREAT|O_TRUNC) keeps every rank's data;
- files past the inline limit, writes at an offset, and an existing file
  opened O_CREAT keep their contents;
- deferral off (the default outside IO500) sends what it sent before.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tests.test_durability import _Servers, _stats  # noqa: E402


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class CreateWriteTests(unittest.TestCase):
    port = 24800

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.srv = _Servers(self.lib, self.tmp, self.port, nm=2, no=2, env={"ROCEPOOL_MDT_ASYNC": "1"})
        self.srv.start()
        self.addCleanup(self.srv.stop)
        self.prefix = os.path.join(self.tmp, "rp")
        type(self).port += 20

    def run_client(self, code, defer=True, check=True):
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                   ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                   ROCEPOOL_MDT_ASYNC="1", ROCEPOOL_ATTR_TTL="0", ROCEPOOL_INTERCEPT_LOG="1",
                   ROCEPOOL_DEFER_CREATE="1" if defer else "0")
        pre = "import os, sys\nP = %r\n" % self.prefix
        r = subprocess.run([sys.executable, "-c", pre + code], capture_output=True, text=True, env=env, cwd=self.tmp,
                           timeout=300)
        if check:
            self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-3000:])
        return r

    def mdt_ops(self):
        """MDT requests by op, summed over the MDTs"""
        out = {}
        for a in self.srv.mdts.split(","):
            for k, v in _stats(self.lib, a).get("by_op", {}).items():
                out[int(k)] = out.get(int(k), 0) + v
        return out

    def delta(self, before):
        after = self.mdt_ops()
        return {k: after.get(k, 0) - before.get(k, 0) for k in set(after) | set(before) if after.get(k, 0) != before.get(k, 0)}

    MDTEST = """
data = bytes(range(256)) * 16
for i in range(40):
    fd = os.open(P + "/d/f%03d" % i, os.O_CREAT | os.O_WRONLY, 0o644)   # as mdtest's create: no O_TRUNC, no O_EXCL
    os.write(fd, data[:3901])
    os.close(fd)
"""

    def test_mdtest_shape_is_one_mdt_request_per_file(self):
        self.run_client('os.makedirs(P + "/d")')
        before = self.mdt_ops()
        r = self.run_client(self.MDTEST)
        d = self.delta(before)
        self.assertEqual(d.get(15), 40, d)                      # create_write
        self.assertEqual(d.get(2, 0) + d.get(7, 0), 0, d)       # no create, no write_inline
        self.assertRegex(r.stderr, r"creates_with_data=40 creates_plain=0 creates_pending=0")
        out = self.run_client("""
print(sorted(os.listdir(P + "/d")) == ["f%03d" % i for i in range(40)],
      all(open(P + "/d/f%03d" % i, "rb").read() == (bytes(range(256)) * 16)[:3901] for i in range(40)))
""", defer=False)
        self.assertEqual(out.stdout.split()[-2:], ["True", "True"], out.stderr[-2000:])

    def test_deferral_off_sends_what_it_sent_before(self):
        self.run_client('os.makedirs(P + "/d")', defer=False)
        before = self.mdt_ops()
        self.run_client(self.MDTEST, defer=False)
        d = self.delta(before)
        self.assertEqual(d.get(15, 0), 0, d)
        self.assertEqual(d.get(2), 40, d)
        self.assertEqual(d.get(7), 40, d)

    def test_closed_files_survive_an_mdt_crash(self):
        self.run_client('os.makedirs(P + "/d")')
        self.run_client(self.MDTEST)
        self.srv.kill()          # SIGKILL right after the closes: nothing more is flushed
        self.srv.start()
        out = self.run_client("""
print(sorted(os.listdir(P + "/d")) == ["f%03d" % i for i in range(40)],
      all(open(P + "/d/f%03d" % i, "rb").read() == (bytes(range(256)) * 16)[:3901] for i in range(40)))
""", defer=False)
        self.assertEqual(out.stdout.split()[-2:], ["True", "True"], out.stderr[-2000:])

    def test_this_process_sees_its_file_before_the_first_write(self):
        r = self.run_client("""
os.makedirs(P + "/d")
fd = os.open(P + "/d/a", os.O_CREAT | os.O_WRONLY)
seen = [os.path.exists(P + "/d/a"), os.listdir(P + "/d")]
os.write(fd, b"abc"); os.close(fd)
fd = os.open(P + "/d/b", os.O_CREAT | os.O_WRONLY); os.close(fd)          # never written
fd = os.open(P + "/d/c", os.O_CREAT | os.O_WRONLY)
os.unlink(P + "/d/c"); os.close(fd)                                        # unlinked while open
fd = os.open(P + "/d/e", os.O_CREAT | os.O_WRONLY)
st = os.stat(P + "/d/e"); os.close(fd)                                     # stat while open (rename: not served)
print(seen, sorted(os.listdir(P + "/d")), open(P + "/d/a", "rb").read(), os.path.getsize(P + "/d/b"))
""")
        self.assertEqual(r.stdout.strip(), "[True, ['a']] ['a', 'b', 'e'] b'abc' 0", r.stderr[-2000:])
        out = self.run_client('print(sorted(os.listdir(P + "/d")))', defer=False)   # another process too
        self.assertEqual(out.stdout.strip(), "['a', 'b', 'e']")

    def test_excl_trunc_append_are_not_deferred(self):
        self.run_client('os.makedirs(P + "/d")')
        before = self.mdt_ops()
        r = self.run_client("""
for name, fl in (("x", os.O_EXCL), ("t", os.O_TRUNC), ("p", os.O_APPEND)):
    fd = os.open(P + "/d/" + name, os.O_CREAT | os.O_WRONLY | fl)
    os.write(fd, b"12345"); os.close(fd)
try:
    os.open(P + "/d/x", os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    print("no EEXIST")
except FileExistsError:
    print("EEXIST")
""")
        self.assertEqual(r.stdout.strip(), "EEXIST")
        d = self.delta(before)
        self.assertEqual(d.get(15, 0), 0, d)
        self.assertRegex(r.stderr, r"creates_with_data=0 creates_plain=0")

    def test_shared_file_opened_by_every_rank_keeps_every_ranks_data(self):
        """IOR's write phase: every rank opens one file O_CREAT|O_TRUNC (not
        deferred), writes its own block, closes; then each rank's block is there"""
        self.run_client('os.makedirs(P + "/d")')
        code = """
import time
r = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ["RANK"])
fd = os.open(P + "/d/shared", os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
time.sleep(0.5)                         # every rank's open comes before any rank's write
os.pwrite(fd, bytes([65 + r]) * 50000, r * 50000)
os.close(fd)
"""
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                   ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                   ROCEPOOL_MDT_ASYNC="1", ROCEPOOL_ATTR_TTL="0", ROCEPOOL_DEFER_CREATE="1")
        ps = [subprocess.Popen([sys.executable, "-c", "import os, sys\nP = %r\n" % self.prefix + code],
                               env=dict(env, RANK=str(r)), cwd=self.tmp) for r in range(4)]
        self.assertEqual([p.wait(timeout=120) for p in ps], [0] * 4)
        out = self.run_client("""
b = open(P + "/d/shared", "rb").read()
print(len(b), b == b"".join(bytes([65 + r]) * 50000 for r in range(4)))
""", defer=False)
        self.assertEqual(out.stdout.split(), ["200000", "True"], out.stderr[-2000:])

    def test_large_offset_and_existing_files_keep_their_contents(self):
        r = self.run_client("""
import random
rnd = random.Random(3)
os.makedirs(P + "/d")
big = rnd.randbytes(300000)                                   # past the inline limit: to the OSTs
fd = os.open(P + "/d/big", os.O_CREAT | os.O_WRONLY); os.write(fd, big); os.close(fd)
fd = os.open(P + "/d/off", os.O_CREAT | os.O_WRONLY); os.pwrite(fd, b"z", 10); os.close(fd)
fd = os.open(P + "/d/two", os.O_CREAT | os.O_WRONLY); os.write(fd, b"ab"); os.write(fd, b"cd"); os.close(fd)
fd = os.open(P + "/d/two", os.O_CREAT | os.O_WRONLY); os.write(fd, b"X"); os.close(fd)   # existing inline: overwrite at 0
fd = os.open(P + "/d/big", os.O_CREAT | os.O_WRONLY); os.write(fd, b"Q"); os.close(fd)   # existing striped
print(open(P + "/d/big", "rb").read() == b"Q" + big[1:], open(P + "/d/off", "rb").read() == b"\\0" * 10 + b"z",
      open(P + "/d/two", "rb").read())
""")
        self.assertEqual(r.stdout.strip(), "True True b'Xbcd'", r.stderr[-2000:])
        out = self.run_client("""
print(open(P + "/d/two", "rb").read(), os.path.getsize(P + "/d/big"), os.path.getsize(P + "/d/off"))
""", defer=False)
        self.assertEqual(out.stdout.strip(), "b'Xbcd' 300000 11", out.stderr[-2000:])

    def test_a_file_left_open_at_exit_exists(self):
        self.run_client("""
os.makedirs(P + "/d")
fd = os.open(P + "/d/left", os.O_CREAT | os.O_WRONLY)   # never closed: the exit creates it
""")
        out = self.run_client('print(os.listdir(P + "/d"))', defer=False)
        self.assertEqual(out.stdout.strip(), "['left']", out.stderr[-2000:])


if __name__ == "__main__":
    unittest.main()
