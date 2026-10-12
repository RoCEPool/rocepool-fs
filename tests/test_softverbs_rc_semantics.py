"""roce_transport.c (unmodified) built against softverbs, the userspace RC
emulator in roce/native/softverbs/. These tests pin down both directions:

  - the fixed transport works (round trip, pipelined stream), AND
  - the emulator really enforces RC rules, i.e. it FAILS the things real
    hardware fails: RNR with rnr_retry_count=0 (built on purpose with the
    old value), a SEND larger than the peer's posted RECV buffer, and a
    connect to a node with no listener.

Without the second half, "passes on softverbs" would mean nothing.
Runs over 127.0.0.1 in the current network namespace; the two-namespace
crossover-cable run is tools/run_crossover_rdma.py.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NATIVE = os.path.join(ROOT, "roce", "native")
SCENARIO = os.path.join(ROOT, "tests", "fakes", "softverbs_scenario.py")


def _build(out: str, *defines: str) -> None:
    sys.path.insert(0, ROOT)
    from roce.native.build_lib import build_softverbs
    build_softverbs(out, *defines)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc to build the softverbs library")
@unittest.skipIf(os.environ.get("ROCEPOOL_BACKEND") == "ofi", "RC queue-pair semantics of the softverbs emulator: the ofi backend has no queue pairs")
class SoftverbsRCSemanticsTests(unittest.TestCase):
    _port = 19600

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        cls.lib = os.path.join(cls.tmp, "libroce_fixed.so")
        cls.lib_rnr0 = os.path.join(cls.tmp, "libroce_rnr0.so")
        _build(cls.lib)
        _build(cls.lib_rnr0, "-DROCE_RNR_RETRY_INFINITE=0")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _run(self, scenario: str, lib: str, trace: bool = False, **env) -> str:
        SoftverbsRCSemanticsTests._port += 1
        if trace:
            env["SOFTVERBS_TRACE"] = "1"
        out = subprocess.run([sys.executable, SCENARIO, ROOT, scenario, str(self._port)],
                             env=dict(os.environ, ROCE_TRANSPORT_LIB=lib, **env),
                             capture_output=True, text=True, timeout=60)
        last = (out.stdout.strip().splitlines() or ["ERR no output: " + out.stderr[-500:]])[-1]
        return last + ("\n" + out.stderr if trace else "")

    def test_request_response_round_trip(self):
        self.assertEqual(self._run("roundtrip", self.lib), "OK")

    def test_pipelined_sends_survive_rnr_with_the_fix(self):
        self.assertEqual(self._run("pipelined", self.lib), "OK")

    def test_pipelined_sends_survive_rnr_on_a_one_buffer_srq(self):
        # servers receive through a shared receive queue; with ONE buffer the
        # 2nd and 3rd pipelined SENDs are NAKed RNR and must be retransmitted
        self.assertEqual(self._run("pipelined", self.lib, ROCE_SRQ_DEPTH="1"), "OK")

    def test_negative_control_rnr_retry_zero_fails_like_real_hardware(self):
        result = self._run("pipelined", self.lib_rnr0, ROCE_SRQ_DEPTH="1")
        self.assertTrue(result.startswith("ERR"), result)
        self.assertIn("RNR retry counter exceeded", result)

    def test_negative_control_send_larger_than_peer_recv_buffer_is_rejected(self):
        result = self._run("oversize", self.lib)
        self.assertIn("remote invalid request error", result)

    def test_bulk_goes_by_rdma_read_and_rdma_write_with_immediate(self):
        result = self._run("bulk", self.lib, trace=True)
        self.assertTrue(result.startswith("OK"), result)
        self.assertIn("RDMA READ served: 300", result)
        self.assertIn("RDMA WRITE with immediate placed: 300", result)

    def test_request_larger_than_the_servers_frame_limit_fails_cleanly(self):
        result = self._run("too-big-for-server", self.lib)
        self.assertTrue(result.startswith("ERR peer unreachable"), result)

    def test_negative_control_connect_to_no_listener_is_a_cm_reject(self):
        result = self._run("no-listener", self.lib)
        self.assertIn("RDMA_CM_EVENT_REJECTED", result)


if __name__ == "__main__":
    unittest.main()
