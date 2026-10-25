"""The libfabric (EFA) server serves each endpoint with several threads:
an endpoint is a queue pair, which EFA has few of per host, so the first
EFA IO500 -- one thread per endpoint, one endpoint per 4 cores -- ran at
most 9 requests at a time per server on 36 cores, the hosts half idle.
Here 2 endpoints x 4 threads against slow writes must run 8 at once."""
import json
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CLIENT = r'''
import json, sys, threading
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
from roce.remote_engine import RemoteEngine
from roce.protocol import Frame, Op
ost = sys.argv[2]
def work(t):
    e = RemoteEngine(ost, RoCETransport())
    for i in range(6):
        e.put(f"k{t}.{i}", b"x" * 1000)
ts = [threading.Thread(target=work, args=(t,)) for t in range(16)]
[x.start() for x in ts]; [x.join() for x in ts]
st = json.loads(RoCETransport().send_request(ost, Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)
print("RESULT " + json.dumps({"inflight_max": st["inflight_max"], "reqs": st["reqs"]}))
'''


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OfiServerThreadsTests(unittest.TestCase):
    def test_threads_share_an_endpoint(self):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_softverbs_lib
        was = os.environ.get("ROCEPOOL_BACKEND")
        os.environ["ROCEPOOL_BACKEND"] = "ofi"  # the libfabric transport (its tcp provider here)
        try:
            lib = ensure_softverbs_lib()
        finally:
            if was is None:
                os.environ.pop("ROCEPOOL_BACKEND")
            else:
                os.environ["ROCEPOOL_BACKEND"] = was
        env = dict(os.environ, ROCE_TRANSPORT_LIB=lib, PYTHONPATH=ROOT, ROCE_OFI_PROVIDER="tcp",
                   ROCE_OFI_SERVER_EPS="2", ROCE_OFI_SERVER_THREADS="4", ROCEPOOL_TEST_OST_WRITE_DELAY_MS="150")
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                                "23520", "1", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
        try:
            _, _, ost = srv.stdout.readline().split()
            # the client side: one process with 16 threads, enough endpoints of its own
            r = subprocess.run([sys.executable, "-c", CLIENT, ROOT, ost], env=dict(env, ROCE_OFI_CLIENT_EPS="16"), capture_output=True, text=True,
                               timeout=300)
        finally:
            srv.stdin.close()
            srv.wait(timeout=20)
        line = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None)
        self.assertIsNotNone(line, r.stdout + r.stderr)
        d = json.loads(line[7:])
        self.assertGreaterEqual(d["reqs"], 96)
        self.assertGreater(d["inflight_max"], 2)   # more than one per endpoint
        self.assertLessEqual(d["inflight_max"], 8)



@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OfiNoWaitObjectTests(unittest.TestCase):
    """EFA has no CQ wait objects (fi_efa(7)): a thread with nothing to do
    polls. It used to sleep 50 us between polls (~100 us with the default
    timer slack) after a 100 us spin, so a request reaching an idle server
    thread, or a reply a client waited for, paid ~90 us (i3en calibration:
    p50 127 us without the spin, 35 us with). Here the tcp provider with no
    wait object, as on EFA; the waits now yield the core for a while
    instead, and requests and async replies must stay correct."""

    def test_correct_and_no_sleep_in_the_latency(self):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_softverbs_lib
        was = os.environ.get("ROCEPOOL_BACKEND")
        os.environ["ROCEPOOL_BACKEND"] = "ofi"
        try:
            lib = ensure_softverbs_lib()
        finally:
            if was is None:
                os.environ.pop("ROCEPOOL_BACKEND")
            else:
                os.environ["ROCEPOOL_BACKEND"] = was
        env = dict(os.environ, ROCE_TRANSPORT_LIB=lib, PYTHONPATH=ROOT, ROCE_OFI_PROVIDER="tcp", ROCE_OFI_CQ_WAIT="none",
                   ROCE_OFI_SERVER_EPS="2", ROCE_POLL_SPIN_US="0")
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                                "23540", "1", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
        try:
            _, _, ost = srv.stdout.readline().split()
            code = r'''
import json, sys, time
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
from roce.remote_engine import RemoteEngine
from roce.protocol import Frame, Op
e = RemoteEngine(sys.argv[2], RoCETransport())
for i in range(50):
    e.put("w%d" % i, bytes([i]) * (100 + 300 * i))
ok = all(e.get("w%d" % i) == bytes([i]) * (100 + 300 * i) for i in range(50))
lat = []
for i in range(400):  # one at a time: every request finds the server idle
    t = time.perf_counter(); e.get("w1"); lat.append(time.perf_counter() - t); time.sleep(0.0003)
lat.sort()
st = json.loads(RoCETransport().send_request(sys.argv[2], Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)
print("RESULT " + json.dumps({"ok": ok, "p50_us": lat[200] * 1e6, "async": st["transport"]["async_replies"]}))
'''
            runs = {}
            for y in ("0", "2000"):  # 0: sleep at once (the old wait, minus the spin)
                r = subprocess.run([sys.executable, "-c", code, ROOT, ost], env=dict(env, ROCE_POLL_YIELD_US=y),
                                   capture_output=True, text=True, timeout=300)
                line = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None)
                self.assertIsNotNone(line, r.stdout + r.stderr)
                runs[y] = json.loads(line[7:])
        finally:
            srv.stdin.close()
            srv.wait(timeout=20)
        for d in runs.values():
            self.assertTrue(d["ok"])
        self.assertGreater(runs["2000"]["async"], 0)  # small replies went async
        # the server's threads (spawned with the yield default) answer an idle
        # request without a sleep; the client side differs only in its own wait
        # here (2 cores): p50 ~80-115 us with the yield, ~115-200 us without
        self.assertLess(runs["2000"]["p50_us"], 2000, runs)
        self.assertLessEqual(runs["2000"]["p50_us"], 1.1 * runs["0"]["p50_us"], runs)


if __name__ == "__main__":
    unittest.main()
