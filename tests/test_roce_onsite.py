"""tools/roce_onsite.py end to end without RDMA hardware: a fake bnxt_re
sysfs/configfs tree for the preflight, the softverbs library standing in for
the real one (--lib), single-host mode. Proves the orchestration -- step
order, scenario evaluation, the --real crossover run, the report -- not the
hardware (tools/roce_onsite.py on site does that)."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tests.test_roce_preflight import fake_host  # noqa: E402


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OnsiteOrchestrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_softverbs_lib
        cls.tmp = tempfile.mkdtemp()
        cfg = fake_host(os.path.join(cls.tmp, "host"), ip="127.0.0.1")
        cls.report = os.path.join(cls.tmp, "onsite.json")
        cls.out = subprocess.run(
            [sys.executable, os.path.join(ROOT, "tools", "roce_onsite.py"), "--force", "--bench", "none",
             "--sysfs-root", os.path.join(cls.tmp, "host"), "--configfs-root", cfg,
             "--lib", ensure_softverbs_lib(), "--report", cls.report],
            capture_output=True, text=True, timeout=900, cwd=cls.tmp)
        with open(cls.report) as f:
            cls.rep = json.load(f)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)
        for f in ("roce_env.sh", "onsite_topology.json", "onsite_crossover.json"):
            try:
                os.remove(os.path.join(ROOT, f))
            except OSError:
                pass

    def test_preflight_ran_and_found_the_fake_bnxt_re(self):
        pf = self.rep["steps"]["preflight"]["preflight"]
        self.assertEqual(pf["device"], "bnxt_re0")
        self.assertEqual(pf["ip"], "127.0.0.1")

    def test_transport_scenarios_pass(self):
        self.assertEqual(self.rep["steps"]["transport"]["status"], "PASS", self.rep["steps"]["transport"])

    def test_functional_phases_pass_in_single_host_real_mode(self):
        f = self.rep["steps"]["functional"]
        self.assertEqual(f["status"], "PASS", f)
        for ph in ("smoke", "pool", "mpi", "hpc", "stream"):
            self.assertIn(f"[PASS] {ph}", f["detail"])

    def test_overall_verdict_reflects_step_failures(self):
        failed = [s for s in self.rep["steps"].values() if s["status"] == "FAIL"]
        self.assertEqual(self.rep["verdict"], "FAIL" if failed else self.rep["verdict"])


if __name__ == "__main__":
    unittest.main()


class SpinCalibrationTests(unittest.TestCase):
    """roce_onsite.calibrate_spin: the choice, and one real measurement."""

    def test_choice(self):
        from tools.roce_onsite import calibrate_spin

        def fake(spin):  # sleep/wake-up costs until the spin covers the request gap
            p50 = {0: 48, 20: 46, 50: 12, 100: 9, 200: 9, 400: 9}[spin]
            return {1: {"ops_per_s": 1e6 / p50, "p50_us": p50}, 8: {"ops_per_s": 8e6 / (p50 + 2), "p50_us": p50}}
        cal = calibrate_spin("x", {}, "x", runner=fake)
        self.assertEqual(cal["chosen"], 100)  # 200/400 are no better: the shortest within 3% wins
        bad = calibrate_spin("x", {}, "x", runner=lambda s: {"error": "boom"})
        self.assertIsNone(bad["chosen"])

    def test_bulk_choice(self):
        """1 MiB puts/gets pick the bulk-transfer spin: the shortest within 3%
        of the best (the small spin, measured on 64-byte gets, says nothing
        about transfers that progress only while the waiting side polls)."""
        from tools.roce_onsite import calibrate_bulk_spin

        def fake(bs):
            put = {100: 900, 500: 2400, 2000: 2450}[bs]
            return {"put": {"ops_per_s": put}, "get": {"ops_per_s": put * 1.5}}
        self.assertEqual(calibrate_bulk_spin("x", {}, "x", runner=fake)["chosen"], 500)
        self.assertIsNone(calibrate_bulk_spin("x", {}, "x", runner=lambda b: {"error": "boom"})["chosen"])

    @unittest.skipUnless(shutil.which("gcc"), "needs gcc")
    def test_real_measurement_on_the_emulator(self):
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        from tools.roce_onsite import calibrate_spin
        env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), PYTHONPATH=ROOT)
        cal = calibrate_spin("127.0.0.1", env, ensure_kv_lib()[1], port=21490, secs=0.3, candidates=(0, 50))
        self.assertIn(cal["chosen"], (0, 50), cal)
        for r in cal["table"].values():
            self.assertGreater(r[8]["ops_per_s"], 0)


class PeerPrecheckTests(unittest.TestCase):
    """the checks that make a misconfigured --peers fail in seconds, not hang"""

    @staticmethod
    def fake(uid="0", host="node02", addrs="127.0.0.1 10.10.0.12 10.0.0.150", rc=0, err=""):
        out = f"ROCEPOOL_PROBE\n{uid}\n{host}\n{addrs}"
        return lambda peer: subprocess.CompletedProcess([], rc, out if rc == 0 else "", err)

    def run_pc(self, peers, **kw):
        from tools.roce_onsite import peer_precheck
        import socket
        local = kw.pop("local_host", None)
        orig = socket.gethostname
        if local:
            socket.gethostname = lambda: local
        try:
            return peer_precheck(peers, "10.10.0.11", runner=self.fake(**kw), pinger=lambda ip: True)
        finally:
            socket.gethostname = orig

    def test_good_peer(self):
        st, d, info = self.run_pc([("root@n2", "10.10.0.12")], local_host="node01")
        self.assertEqual(st, "PASS", d)

    def test_typo_ip_is_corrected_to_the_roce_subnet(self):
        st, d, info = self.run_pc([("root@n2", "10.0.0.12")], local_host="node01")
        self.assertEqual(st, "WARN", d)
        self.assertEqual(info["root@n2"]["roce_ip"], "10.10.0.12")
        self.assertIn("10.0.0.12", d)

    def test_unfixable_ip_fails(self):
        st, d, _ = self.run_pc([("root@n2", "172.16.0.9")], addrs="127.0.0.1 10.0.0.150", local_host="node01")
        self.assertEqual(st, "FAIL")
        self.assertIn("not one of its addresses", d)

    def test_non_root_and_same_hostname_and_no_ssh(self):
        st, d, _ = self.run_pc([("admin@n2", "10.10.0.12")], uid="1000", host="admin", local_host="admin")
        self.assertEqual(st, "FAIL")
        self.assertIn("not root", d)
        self.assertIn("share a hostname", d)
        st, d, _ = self.run_pc([("root@n2", "10.10.0.12")], rc=255, err="root@n2: Permission denied (publickey).")
        self.assertEqual(st, "FAIL")
        self.assertIn("ssh-copy-id", d)
