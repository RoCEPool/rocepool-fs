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
import tempfile
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
    instead (server threads only, and only while their waits are short),
and requests and async replies must stay correct."""

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
        # only the server's threads yield (clients spin, then sleep: their
        # program needs the cores), so the client setting changes nothing
        # measurable here; the bound catches a millisecond sleep
        self.assertLess(runs["2000"]["p50_us"], 2000, runs)



@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OfiInlinePayloadTests(unittest.TestCase):
    """Writes up to 64 KiB go inside the request (the server does not pull
    them: on EFA without RDMA read in hardware a pull progresses only while
    the client polls). Payloads around the limit, both ways, byte-exact."""

    def test_payloads_around_the_inline_limit(self):
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
        env = dict(os.environ, ROCE_TRANSPORT_LIB=lib, PYTHONPATH=ROOT, ROCE_OFI_PROVIDER="tcp", ROCE_OFI_CQ_WAIT="none")
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                                "23580", "1", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
        try:
            _, _, ost = srv.stdout.readline().split()
            code = r'''
import random, sys
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
from roce.remote_engine import RemoteEngine
e = RemoteEngine(sys.argv[2], RoCETransport())
ok = True
for n in (16000, 16384, 47008, 60000, 65400, 65536, 65600, 70000, 1 << 20):
    data = random.Random(n).randbytes(n)
    e.put("p%d" % n, data)
    ok = ok and e.get("p%d" % n) == data
    e.write_at("w%d" % n, 1000, data)            # a WRITE_AT carrying it
    ok = ok and e.get("w%d" % n) == bytes(1000) + data
import json
from roce.protocol import Frame, Op
t = json.loads(RoCETransport().send_request(sys.argv[2], Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)["transport"]
# payloads over 64 KiB were pulled by the server, from a client on its host
print("PULLS", t["pull_local_n"], t["pull_remote_n"], t["pull_local_bytes"])
print("RESULT", ok)
'''
            r = subprocess.run([sys.executable, "-c", code, ROOT, ost], env=env, capture_output=True, text=True, timeout=300)
        finally:
            srv.stdin.close()
            srv.wait(timeout=20)
        self.assertIn("RESULT True", r.stdout, r.stdout + r.stderr)
        pulls = next(l for l in r.stdout.splitlines() if l.startswith("PULLS")).split()
        self.assertGreaterEqual(int(pulls[1]), 6, r.stdout)  # 65600, 70000, 1 MiB: put + write_at each
        self.assertEqual(int(pulls[2]), 0, r.stdout)
        self.assertGreater(int(pulls[3]), 1 << 20, r.stdout)


if __name__ == "__main__":
    unittest.main()


PIPE_CLIENT = r'''
import hashlib, json, sys, threading, time
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
from roce.remote_engine import RemoteEngine
from roce.protocol import Frame, Op
ost, nthreads, per = sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
bad = []
def data(t, i):
    seed = hashlib.sha256(f"{t}.{i}".encode()).digest()
    return (seed * ((1 << 20) // 32 + 1))[:(1 << 20) - 7 * i]
def work(t):
    e = RemoteEngine(ost, RoCETransport())
    for i in range(per):
        e.put(f"big{t}.{i}", data(t, i))
    for i in range(per):
        if e.get(f"big{t}.{i}") != data(t, i):
            bad.append((t, i))
t0 = time.time()
ts = [threading.Thread(target=work, args=(t,)) for t in range(nthreads)]
[x.start() for x in ts]; [x.join() for x in ts]
st = json.loads(RoCETransport().send_request(ost, Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)
tr = st["transport"]
print("RESULT " + json.dumps({"bad": bad, "seconds": round(time.time() - t0, 2), "reqs": st["reqs"],
                              "pulls": tr["pull_local_n"] + tr["pull_remote_n"],
                              "queue_us": round(tr["queue_ns"] / max(1, tr["reqs"]) / 1e3),
                              **{k: tr[k] for k in ("pull_pipeline", "pull_pipeline_max", "big_replies", "big_replies_async")}}))
'''


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OfiPipelineTests(unittest.TestCase):
    """An endpoint's thread no longer waits for a request's payload pull or
    for a large reply's send: it posts them and takes the next request
    (6 x i3en: 1 MiB writes queued 15 ms behind other requests' 2 ms pulls,
    1 MiB reads behind 1.25 ms sends). One endpoint, one thread, 12 clients
    writing and reading megabytes: several pulls and several large replies
    in flight at once, every byte right -- and with both turned off
    (ROCE_OFI_PULL_PIPELINE=1, ROCE_OFI_BIG_REPLIES=0) the old path still
    works."""

    def run_once(self, port, extra):
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
        env = dict(os.environ, ROCE_TRANSPORT_LIB=lib, PYTHONPATH=ROOT, ROCE_OFI_PROVIDER="tcp",
                   ROCE_OFI_SERVER_EPS="1", **extra)
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                                str(port), "1", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
        try:
            _, _, ost = srv.stdout.readline().split()
            r = subprocess.run([sys.executable, "-c", PIPE_CLIENT, ROOT, ost, "12", "6"],
                               env=dict(env, ROCE_OFI_CLIENT_EPS="12"), capture_output=True, text=True, timeout=600)
        finally:
            srv.stdin.close()
            srv.wait(timeout=30)
        line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULT ")), None)
        self.assertIsNotNone(line, r.stdout + r.stderr)
        return json.loads(line[7:])

    def test_pulls_and_large_replies_in_flight(self):
        d = self.run_once(23560, {})
        print(f"\n    pipelined: {d}", file=sys.stderr)
        self.assertEqual(d["bad"], [])
        self.assertGreaterEqual(d["pulls"], 72)             # every PUT's megabyte was pulled
        self.assertEqual((d["pull_pipeline"], d["big_replies"]), (4, 4))
        self.assertGreater(d["pull_pipeline_max"], 1)       # more than one pull in flight on the one thread
        self.assertLessEqual(d["pull_pipeline_max"], 4)
        self.assertGreater(d["big_replies_async"], 0)       # GET replies sent without waiting

    def test_turned_off_is_the_waiting_path(self):
        d = self.run_once(23570, {"ROCE_OFI_PULL_PIPELINE": "1", "ROCE_OFI_BIG_REPLIES": "0"})
        print(f"\n    waiting:   {d}", file=sys.stderr)
        self.assertEqual(d["bad"], [])
        self.assertGreaterEqual(d["pulls"], 72)
        self.assertEqual((d["pull_pipeline"], d["pull_pipeline_max"], d["big_replies_async"]), (1, 0, 0))

    def test_a_small_pool_gets_fewer_in_flight(self):
        d = self.run_once(23580, {"ROCE_BUFPOOL_MAX": "12"})  # (2 + pipe + big) * 1 + 8 <= 12
        self.assertEqual(d["bad"], [])
        self.assertEqual((d["pull_pipeline"], d["big_replies"]), (2, 0))


BIGREAD_CLIENT = r'''
import hashlib, json, sys, threading, time
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
from roce.remote_engine import RemoteEngine
from roce.protocol import Frame, Op
ost, nobj, nthreads = sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
def data(i):
    return (hashlib.sha256(b"%d" % i).digest() * ((1 << 20) // 32))[:(1 << 20) - 3 * i]
e = RemoteEngine(ost, RoCETransport())
for i in range(nobj):
    e.put(f"o{i}", data(i))
bad = []
def work(t):
    ee = RemoteEngine(ost, RoCETransport())
    for i in range(t, nobj, nthreads):
        _, b = ee.read_at(f"o{i}", 0, 1 << 20)          # a whole stripe: a large reply
        if b != data(i):
            bad.append(i)
        _, b = ee.read_at(f"o{i}", 1000, 47008)          # a piece
        if b != data(i)[1000:1000 + 47008]:
            bad.append(-i - 1)
t0 = time.time()
ts = [threading.Thread(target=work, args=(t,)) for t in range(nthreads)]
[x.start() for x in ts]; [x.join() for x in ts]
dt = time.time() - t0
st = json.loads(RoCETransport().send_request(ost, Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)
print("RESULT " + json.dumps({"bad": bad, "seconds": round(dt, 2), "async_tier_reads": st.get("async_tier_reads"),
                              "tier_reads": st.get("tier_reads"), "deferred": st["transport"].get("deferred")}))
'''


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OfiLargeTierReadTests(unittest.TestCase):
    """A READ_AT of a megabyte only on the tier is read by the OST's tier
    readers while the endpoint's thread goes on; its large reply is held in
    its own buffer (ROCE_OFI_DEFER_BIG per thread) until the read is in.
    6 x i3en, ior-easy-read: 1.2 ms per request reading the tier in the
    endpoint's thread, threads 92% busy. Here every tier read waits 40 ms
    (ROCEPOOL_TEST_OST_TIER_READ_DELAY_US: a device's latency without its CPU),
    one endpoint thread, 12 clients: held, the reads overlap; with
    ROCE_OFI_DEFER_BIG=0 the megabytes are read one after another."""

    def run_once(self, port, extra):
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
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        env = dict(os.environ, ROCE_TRANSPORT_LIB=lib, PYTHONPATH=ROOT, ROCE_OFI_PROVIDER="tcp",
                   ROCE_OFI_SERVER_EPS="1", ROCEPOOL_OST_TIER=f"file:{d}/t{{n}}:1", ROCEPOOL_OST_RAM_CACHE_MIB="1",
                   ROCEPOOL_TEST_OST_TIER_READ_DELAY_US="40000", **extra)
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                                str(port), "1", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
        try:
            _, _, ost = srv.stdout.readline().split()
            r = subprocess.run([sys.executable, "-c", BIGREAD_CLIENT, ROOT, ost, "36", "12"],
                               env=dict(env, ROCE_OFI_CLIENT_EPS="12"), capture_output=True, text=True, timeout=600)
        finally:
            srv.stdin.close()
            srv.wait(timeout=30)
        line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULT ")), None)
        self.assertIsNotNone(line, r.stdout + r.stderr)
        return json.loads(line[7:])

    def test_large_tier_reads_overlap(self):
        held = self.run_once(23600, {})
        waited = self.run_once(23610, {"ROCE_OFI_DEFER_BIG": "0"})
        print(f"\n    held: {held}\n    waited: {waited}", file=sys.stderr)
        self.assertEqual(held["bad"], [])
        self.assertEqual(waited["bad"], [])
        self.assertGreaterEqual(held["async_tier_reads"], 60)       # the megabytes too, not only the pieces
        self.assertLessEqual(waited["async_tier_reads"], 40)        # the pieces alone
        # 36 megabytes read 40 ms each: one after another >= 1.4 s on the one thread
        self.assertGreater(waited["seconds"], 1.4)
        self.assertLess(held["seconds"], waited["seconds"] / 2)
