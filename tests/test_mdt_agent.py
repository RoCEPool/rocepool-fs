"""The node agent (roce/native/rocepool_agent.c, ROCEPOOL_AGENT): a node's
create_writes to the MDTs, several processes' at a time, in one batch (MDT
op 16). Opt-in; IO500 runs turn it on with io500_run --agent.

Checked here, through the interception library over native servers on
logs and tiers:
- many processes' small files go out in fewer MDT requests than files, each
  file still one create_write at the MDT, and every file reads back;
- durable at close: an MDT killed right after the closes keeps every file
  (IO500 rule 6), the batch's requests replayed from the log one by one;
- a client whose agent is not there, or goes away mid-run, sends its
  requests itself and loses nothing;
- the agent's answers: an entry, the MDT's refusal (IS_DIR), or "send it
  yourself" when it has none (no MDT there, a malformed request).
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tests.test_durability import _Servers, _stats  # noqa: E402

RANK = """
import os, sys
P, r, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
data = bytes(range(256)) * 16
for i in range(n):
    fd = os.open(P + "/d/r%d.f%03d" % (r, i), os.O_CREAT | os.O_WRONLY, 0o644)
    os.write(fd, data[:3901])
    os.close(fd)
"""

CHECK = """
import os, sys
P, ranks, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
want = sorted("r%d.f%03d" % (r, i) for r in range(ranks) for i in range(n))
data = (bytes(range(256)) * 16)[:3901]
names = sorted(os.listdir(P + "/d"))
print(names == want, all(open(P + "/d/" + f, "rb").read() == data for f in want))
"""


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class AgentTests(unittest.TestCase):
    port = 25600

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_agent, ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()
        cls.agent_bin = ensure_agent()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.srv = _Servers(self.lib, self.tmp, self.port, nm=2, no=2, env={"ROCEPOOL_MDT_ASYNC": "1"})
        self.srv.start()
        self.addCleanup(self.srv.stop)
        self.prefix = os.path.join(self.tmp, "rp")
        self.sock = os.path.join(self.tmp, "agent.sock")
        self.agent_stats = os.path.join(self.tmp, "agent.json")
        self.agent = None
        type(self).port += 20

    def start_agent(self):
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, ROCEPOOL_AGENT=self.sock,
                   ROCEPOOL_AGENT_STATS=self.agent_stats)
        self.agent = subprocess.Popen([self.agent_bin], env=env, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self.stop_agent)
        for _ in range(200):
            if (self.sock.startswith("@") and "listening" in (self.agent.stderr.readline() or "")) \
                    or os.path.exists(self.sock):
                return
            time.sleep(0.01)
        self.fail("agent did not start: " + self.agent.stderr.read())

    def stop_agent(self):
        if self.agent and self.agent.poll() is None:
            self.agent.send_signal(signal.SIGTERM)
            self.agent.wait(timeout=10)

    def env(self, agent=True):
        e = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                 ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                 ROCEPOOL_MDT_ASYNC="1", ROCEPOOL_ATTR_TTL="0", ROCEPOOL_INTERCEPT_LOG="1",
                 ROCEPOOL_DEFER_CREATE="1")
        if agent:
            e["ROCEPOOL_AGENT"] = self.sock
        return e

    def py(self, code, *args, agent=False):
        r = subprocess.run([sys.executable, "-c", code, self.prefix, *map(str, args)], capture_output=True,
                           text=True, env=self.env(agent), cwd=self.tmp, timeout=300)
        self.assertEqual(r.returncode, 0, r.stdout[-2000:] + r.stderr[-3000:])
        return r

    def ranks(self, ranks, n, agent=True):
        ps = [subprocess.Popen([sys.executable, "-c", RANK, self.prefix, str(r), str(n)], env=self.env(agent),
                               cwd=self.tmp, stderr=subprocess.PIPE, text=True) for r in range(ranks)]
        errs = [p.communicate(timeout=300)[1] for p in ps]
        self.assertEqual([p.returncode for p in ps], [0] * ranks, "\n".join(e[-1500:] for e in errs))
        return errs

    def mdt_ops(self):
        out = {}
        for a in self.srv.mdts.split(","):
            for k, v in _stats(self.lib, a).get("by_op", {}).items():
                out[int(k)] = out.get(int(k), 0) + v
        return out

    def check(self, ranks, n):
        out = self.py(CHECK, ranks, n)
        self.assertEqual(out.stdout.split()[-2:], ["True", "True"], out.stderr[-2000:])

    def test_many_ranks_fewer_mdt_requests(self):
        self.py('import os, sys; os.makedirs(sys.argv[1] + "/d")')
        self.start_agent()
        before = self.mdt_ops()
        errs = self.ranks(16, 40)
        after = self.mdt_ops()
        d = {k: after.get(k, 0) - before.get(k, 0) for k in set(after) | set(before)}
        self.assertEqual(d.get(15), 16 * 40, d)                 # every file one create_write
        self.assertEqual(d.get(2, 0) + d.get(7, 0), 0, d)
        self.assertGreater(d.get(16, 0), 0, d)
        self.assertLess(d.get(16, 0), 16 * 40, d)               # in fewer MDT requests
        answered = sum(int(e.split("agent_answered=")[1].split()[0]) for e in errs if "agent_answered=" in e)
        self.assertEqual(answered, 16 * 40)
        self.stop_agent()
        st = json.load(open(self.agent_stats))
        self.assertEqual(st["requests"], 16 * 40)
        self.assertEqual(st["batches"], d[16])
        self.check(16, 40)

    def test_closed_files_survive_an_mdt_crash(self):
        self.py('import os, sys; os.makedirs(sys.argv[1] + "/d")')
        self.sock = "@rocepool-agent-test-%d" % self.port      # the abstract namespace, as io500_run uses
        self.start_agent()
        self.ranks(8, 30)
        self.srv.kill()          # SIGKILL right after the closes
        self.srv.start()
        self.check(8, 30)

    def test_no_agent_running_sends_directly(self):
        self.py('import os, sys; os.makedirs(sys.argv[1] + "/d")')
        errs = self.ranks(4, 20)                                  # ROCEPOOL_AGENT set, nothing listening
        self.assertTrue(all("agent_answered=0" in e for e in errs), errs[0][-500:])
        self.check(4, 20)

    def test_agent_gone_mid_run_loses_nothing(self):
        self.py('import os, sys; os.makedirs(sys.argv[1] + "/d")')
        self.start_agent()
        ps = [subprocess.Popen([sys.executable, "-c", RANK, self.prefix, str(r), "300"], env=self.env(True),
                               cwd=self.tmp, stderr=subprocess.PIPE, text=True) for r in range(6)]
        time.sleep(1.0)
        self.agent.kill()
        errs = [p.communicate(timeout=300)[1] for p in ps]
        self.assertEqual([p.returncode for p in ps], [0] * 6, "\n".join(e[-1500:] for e in errs))
        self.check(6, 300)

    def agent_call(self, mdt: str, path: str, value: bytes) -> bytes:
        import socket
        c = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        c.connect(self.sock)
        c.send(bytes([len(mdt)]) + mdt.encode() + len(path).to_bytes(4, "little") + path.encode() + value)
        r = c.recv(70000)
        c.close()
        return r

    def test_agent_protocol_answers_and_refusals(self):
        """the agent's own protocol: an entry (0), the MDT's refusal (1), and
        2 -- send it yourself -- for what it cannot answer"""
        from fs import mdt_wire
        self.py('import os, sys; os.makedirs(sys.argv[1] + "/d/sub")')
        self.start_agent()
        mdts = self.srv.mdts.split(",")
        ok = err = 0
        for m in mdts:      # the directory lives on one of them: IS_DIR there, a new file elsewhere
            cw = mdt_wire.encode_request({"op": "create_write", "path": "", "offset": 0, "data": b"q",
                                          "layout": None, "inline_ok": True})
            r = self.agent_call(m, "/d/sub", cw)
            if r[0] == 1:
                self.assertEqual(r[1:].split(b"\0")[0], b"IS_DIR")
                err += 1
            else:
                self.assertEqual(r[0], 0, r)
                self.assertEqual(mdt_wire.decode_entry(r[1:])[0]["size"], 1)
                ok += 1
        self.assertEqual(err, 1)
        self.assertEqual(self.agent_call("127.0.0.1:1", "/d/z", b"\x0f\x02" + bytes(8) + b"q"), b"\x02")  # no MDT there
        self.assertEqual(self.agent_call(mdts[0], "", b"")[:1], b"\x02")                                   # malformed

if __name__ == "__main__":
    unittest.main()
