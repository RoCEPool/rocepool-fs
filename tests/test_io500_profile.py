"""io500_run's phase profiles: perf on node 0, inside a phase, on real hosts
(default: mdtest-hard-write outside --valid runs). Where EFA runs spend a
file's CPU -- the provider, the wait loop, the client, the servers -- is
what no local emulator reproduces."""
import os
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import io500_run  # noqa: E402

PERF = """
    23.10%  mdtest        libfabric.so.1          [.] efa_rdm_ep_progress
    11.52%  mdtest        libc.so.6               [.] __memcpy_avx_unaligned_erms
     9.00%  rocepool_serv libfabric.so.1          [.] efa_rdm_ep_progress
     4.25%  mdtest        librocepool_intercept.so [.] ep_wait
     2.00%  mdtest        [kernel.kallsyms]       [k] __schedule
     0.40%  swapper       [kernel.kallsyms]       [k] intel_idle
"""


class FakeCluster:
    def __init__(self, perf=True):
        self.perf, self.calls = perf, []

    def run(self, i, argv, **kw):
        self.calls.append((i, argv[-1]))
        class R:
            pass
        r = R()
        if "PERF_OK" in argv[-1]:
            r.stdout = "PERF_OK\n" if self.perf else "PERF_MISSING\n"
        else:
            r.stdout = PERF
        return r


class ProfileTests(unittest.TestCase):
    def test_parse(self):
        p = io500_run.parse_perf_report(PERF)
        self.assertEqual(len(p["top"]), 6)
        self.assertEqual(p["top"][0], {"pct": 23.1, "comm": "mdtest", "dso": "libfabric.so.1", "sym": "efa_rdm_ep_progress"})
        self.assertEqual(p["by_comm"]["mdtest"], 40.87)
        self.assertEqual(p["by_dso"]["libfabric.so.1"], 32.1)
        self.assertEqual(list(p["by_dso"])[0], "libfabric.so.1")      # largest first

    def test_default_phases(self):
        self.assertEqual(io500_run.profile_phases("auto", real=True, valid=False), ["mdtest-hard-write"])
        self.assertEqual(io500_run.profile_phases("auto", real=True, valid=True), [])     # the scored run: never
        self.assertEqual(io500_run.profile_phases("auto", real=False, valid=False), [])   # the simulation: no perf
        self.assertEqual(io500_run.profile_phases("none", real=True, valid=False), [])
        self.assertEqual(io500_run.profile_phases("ior-hard-write, mdtest-hard-write", True, False),
                         ["ior-hard-write", "mdtest-hard-write"])

    def test_profiles_the_phase_into_the_report(self):
        with tempfile.TemporaryDirectory() as work:
            c = FakeCluster()
            pr = io500_run.PhaseProfiler(c, ["mdtest-hard-write"], work, stonewall=10)
            pr.delay = 0
            pr.phase_started("ior-easy-write")                   # not asked for
            pr.phase_started("mdtest-hard-write")
            pr.phase_started("mdtest-hard-write")                # once per phase
            res = pr.finish()
            self.assertEqual(list(res), ["mdtest-hard-write"])
            self.assertEqual(res["mdtest-hard-write"]["top"][0]["sym"], "efa_rdm_ep_progress")
            self.assertEqual(res["mdtest-hard-write"]["window_s"], 5)
            self.assertTrue(os.path.exists(os.path.join(work, "profile_mdtest-hard-write.txt")))
            recs = [cmd for _, cmd in c.calls if "perf record" in cmd]
            self.assertEqual(len(recs), 1)
            self.assertIn("-a -F 199", recs[0])

    def test_without_perf_the_run_goes_on(self):
        with tempfile.TemporaryDirectory() as work:
            pr = io500_run.PhaseProfiler(FakeCluster(perf=False), ["mdtest-hard-write"], work, stonewall=60)
            pr.phase_started("mdtest-hard-write")
            t0 = time.monotonic()
            self.assertEqual(pr.finish(), {"mdtest-hard-write": {"error": "perf not available on node 0"}})
            self.assertLess(time.monotonic() - t0, 1)

    def test_nothing_asked_nothing_run(self):
        c = FakeCluster()
        pr = io500_run.PhaseProfiler(c, [], "/nonexistent", stonewall=60)
        pr.phase_started("mdtest-hard-write")
        self.assertEqual(pr.finish(), {})
        self.assertEqual(c.calls, [])


if __name__ == "__main__":
    unittest.main()
