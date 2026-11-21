"""Each client's requests are handled, and answered, in the order it sent
them, also when the libfabric server pulls a payload in the background
(ROCE_OFI_PULL_PIPELINE, on by default) or sends a large reply in the
background.

The first EFA run with hardware RDMA read (g6.8xlarge, 2026-11-21) failed
here: a stream connection sent a 300 KB PUT and then a GET of the same key;
the server answered the GET while the PUT's payload was still being pulled,
so the GET found nothing and its reply ("ERROR") came back before PUT_OK.
libfabric's tcp provider pulls the same way, so the order is checked here
on it: one connection writes and reads back objects above the 64 KiB inline
limit without waiting between them, while other clients keep the server's
pull pipeline busy."""
import json
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

STREAM_CLIENT = r'''
import hashlib, json, sys, threading
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCEStream, RoCETransport
from roce.remote_engine import RemoteEngine
from roce.protocol import Frame, Op, HEADER_SIZE
from roce.pool_node import _pack_put_value
peer, rounds, others = sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
stop = False
def busy(t):
    # other clients: payload pulls of their own in flight on the server
    e = RemoteEngine(peer, RoCETransport())
    i = 0
    while not stop:
        e.put(f"other{t}.{i % 4}", bytes([t]) * (1 << 20))
        i += 1
ts = [threading.Thread(target=busy, args=(t,), daemon=True) for t in range(others)]
[x.start() for x in ts]
def value(i, size):
    seed = hashlib.sha256(str(i).encode()).digest()
    return (seed * (size // 32 + 1))[:size]
bad = []
with RoCEStream.connect(peer) as s:
    for r in range(rounds):
        size = (70_000, 300_000, 1 << 20, 65_000)[r % 4]
        v = value(r, size)
        key = f"order/{r}"
        # PUT, then GET of the same key, then a small PUT: all sent before
        # any reply is read
        s.sendall(Frame(op=Op.PUT, request_id=3 * r, key=key, value=_pack_put_value(v, None)).encode())
        s.sendall(Frame(op=Op.GET, request_id=3 * r + 1, key=key, value=b"").encode())
        s.sendall(Frame(op=Op.PUT, request_id=3 * r + 2, key=key + "/s", value=_pack_put_value(b"s", None)).encode())
        for rid, want in ((3 * r, Op.PUT_OK), (3 * r + 1, Op.GET_OK), (3 * r + 2, Op.PUT_OK)):
            header = b""
            while len(header) < HEADER_SIZE:
                header += s.recv(HEADER_SIZE - len(header))
            n = Frame.peek_body_length(header)
            body = b""
            while len(body) < n:
                body += s.recv(n - len(body))
            f = Frame.decode(header + body)
            ok = f.op == want and f.request_id == rid and (want != Op.GET_OK or f.value == v)
            if not ok:
                bad.append({"round": r, "size": size, "want": [want.name, rid], "got": [Op(f.op).name, f.request_id]})
stop = True
st = json.loads(RoCETransport().send_request(peer, Frame(op=Op.STATS, request_id=1, key="", value=b"")).value)
tr = st.get("transport", {})
print("RESULT " + json.dumps({"bad": bad[:8], "nbad": len(bad),
                              "pull_pipeline_max": tr.get("pull_pipeline_max"),
                              "pulls": tr.get("pull_local_n", 0) + tr.get("pull_remote_n", 0)}), flush=True)
import os; os._exit(0)
'''


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OfiPerClientOrderTests(unittest.TestCase):
    def run_once(self, port, extra, others):
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
                   ROCE_OFI_SERVER_EPS="1", ROCE_OFI_CLIENT_EPS="8", **extra)
        # a native OST (roce/native/rocepool_servers.c)
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                                str(port), "0", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
        try:
            line = srv.stdout.readline()
            self.assertTrue(line.startswith("READY"), line)
            addr = line.split()[-1]
            r = subprocess.run([sys.executable, "-c", STREAM_CLIENT, ROOT, addr, "24", str(others)],
                               env=env, capture_output=True, text=True, timeout=600)
        finally:
            srv.stdin.close()
            try:
                srv.wait(timeout=30)
            except subprocess.TimeoutExpired:
                srv.kill()
        line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULT ")), None)
        self.assertIsNotNone(line, r.stdout + r.stderr[-3000:])
        return json.loads(line[7:])

    def test_one_connection_in_order_while_pulls_are_pipelined(self):
        d = self.run_once(23590, {}, others=3)
        print(f"\n    pipelined: {d}", file=sys.stderr)
        self.assertEqual(d["nbad"], 0, d["bad"])
        self.assertGreater(d["pulls"], 24)                 # the payloads were pulled
        self.assertGreater(d["pull_pipeline_max"], 1)      # with other clients' pulls in flight

    def test_one_connection_alone(self):
        d = self.run_once(23600, {}, others=0)
        self.assertEqual(d["nbad"], 0, d["bad"])

    def test_turned_off_is_in_order_too(self):
        d = self.run_once(23610, {"ROCE_OFI_PULL_PIPELINE": "1", "ROCE_OFI_BIG_REPLIES": "0"}, others=0)
        self.assertEqual(d["nbad"], 0, d["bad"])


if __name__ == "__main__":
    unittest.main()
