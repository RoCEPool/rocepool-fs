"""RoCETransport pooled persistent connections (tier B of docs/ABC_UPGRADE.md)
over the real softverbs-backed library: reuse, the opt-out, concurrent
callers, a server restarted under a pooled connection, a mismatched reply,
server stop draining an in-flight reply, and idle expiry. Runs in a
subprocess (tests/fakes/pool_scenario.py): the library loads once per process."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@unittest.skipUnless(shutil.which("gcc"), "needs gcc to build the library")
class ConnPoolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import build_softverbs
        cls.tmp = tempfile.mkdtemp()
        lib = build_softverbs(os.path.join(cls.tmp, "libroce_pool_test.so"))
        out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "pool_scenario.py"), ROOT, "19800"],
                             env=dict(os.environ, ROCE_TRANSPORT_LIB=lib), capture_output=True, text=True, timeout=180)
        line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
        assert line, f"scenario produced no result:\n{out.stdout[-2000:]}\n{out.stderr[-2000:]}"
        cls.res = json.loads(line[len("RESULT "):])
        out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "pool_cap_scenario.py"), ROOT, "19890"],
                             env=dict(os.environ, ROCE_TRANSPORT_LIB=lib, ROCE_BUFPOOL_MAX="2"),
                             capture_output=True, text=True, timeout=180)
        line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
        assert line, f"cap scenario produced no result:\n{out.stdout[-2000:]}\n{out.stderr[-2000:]}"
        cls.cap = json.loads(line[len("RESULT "):])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_no_scenario_error(self):
        self.assertNotIn("error", self.res, self.res.get("error"))

    def test_sequential_requests_reuse_one_connection(self):
        self.assertEqual(self.res["pooled"], {"ok": True, "connects": 1})

    def test_pool_opt_out_connects_per_request(self):
        self.assertEqual(self.res["unpooled_connects"], 50)

    def test_concurrent_callers_get_their_own_replies(self):
        c = self.res["concurrent"]
        self.assertEqual(c["bad"], [])
        self.assertLessEqual(c["connects"], 8)
        self.assertLessEqual(c["idle"], 8)

    def test_stale_connection_after_server_restart_is_retried(self):
        self.assertTrue(self.res["restart"]["ok"], self.res["restart"])
        self.assertEqual(self.res["restart"]["connects"], 2)

    def test_mismatched_reply_is_an_error_and_connection_dropped(self):
        m = self.res["mismatch"]
        self.assertTrue(m["raised"])
        self.assertIn("out of step", m["msg"])
        self.assertEqual(m["idle"], 0)

    def test_server_stop_drains_in_flight_reply(self):
        d = self.res["drain"]
        self.assertEqual(d["reply"], "drained", d)
        self.assertIsNone(d["err"])

    def test_idle_connections_expire(self):
        self.assertEqual(self.res["idle_expiry_connects"], 2)

    def test_receive_memory_bounded_by_srq_not_connections(self):
        b = self.res["bounded"]
        self.assertEqual(b["open_connections"], 64)
        # server SRQ (32) + a few in-flight send/recv buffers; the old design
        # needed 2 frame buffers per connection per side (256 here)
        self.assertLessEqual(b["buffers_after"] - b["buffers_before"], 40, b)

    def test_srq_exhaustion_is_backpressure_not_failure(self):
        r = self.res["srq_backpressure"]
        self.assertEqual(r["errors"], [])
        self.assertEqual(r["ok"], 120)
        self.assertTrue(r["all_matched"])

    def test_pipelining_peer_disconnecting_does_not_leak_srq_buffers(self):
        self.assertTrue(self.res["srq_after_disconnects"]["ok"], self.res["srq_after_disconnects"])

    def test_pool_at_cap_serves_two_buffer_requests_without_deadlock(self):
        c = self.cap
        self.assertEqual(c["stuck"], 0, c)
        self.assertEqual(c["errors"], [], c)
        self.assertEqual(c["ok"], 80, c)
        self.assertTrue(c["all_matched"])


if __name__ == "__main__":
    unittest.main()
