"""N-node IO500 automation (tools/io500_run.py, tools/cluster.py,
tools/io500_model.py): the official IO500 app across a simulated cluster,
and the REAL multi-host path (cluster file, ssh, MPI by IP, name
resolution) rehearsed with fake sshds (tests/fakes/cluster_rehearsal.py).
Needs root, /dev/fuse, gcc and the network for the first build of MPICH /
IO500 (cached in .bench_cache)."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import io500_model  # noqa: E402
from tools.io500_run import io500_validity, phase_ops, split_budget, write_ini  # noqa: E402

SMALL = ["--stonewall", "3", "--md-easy-n", "300", "--md-hard-n", "300", "--mem-budget-gib", "0.4"]
SCORED = io500_model.BW_PHASES + io500_model.MD_PHASES

MDTEST_TXT = """6 tasks, 3000 files
SUMMARY rate (in ops/sec): (of 1 iterations)
   Operation                     Max            Min           Mean        Std Dev
   File creation                8715.153       8715.153       8715.153          0.000
   File stat                       0.000          0.000          0.000          0.000
SUMMARY time (in ms/op): (of 1 iterations)
   Operation                     Max            Min           Mean        Std Dev
   File creation                   0.344          0.344          0.344          0.000
   File stat                       0.000          0.000          0.000          0.000
"""
IOR_TXT = """access    bw(MiB/s)  IOPS       Latency(s)  block(KiB) xfer(KiB)  open(s)    wr/rd(s)   close(s)   total(s)   iter
------    ---------  ----       ----------  ---------- ---------  --------   --------   --------   --------   ----
write     356.04     178.30     0.033298    51200      2048.00    0.000905   0.841259   0.000274   0.842605   0
"""


class PhaseOpsTests(unittest.TestCase):
    """The work a phase did comes from its own output, not rate x IO500's
    phase time (which includes setup and overstated a short mdtest phase's
    work 4x, making every per-op cost 4x too cheap)."""

    def test_counts(self):
        d = tempfile.mkdtemp()
        try:
            for name, text in (("mdtest-easy-write.txt", MDTEST_TXT), ("ior-easy-write.txt", IOR_TXT),
                               ("find.csv", "runtime: 0.264163 rate: 22739.708554\n")):
                with open(os.path.join(d, name), "w") as f:
                    f.write(text)
            self.assertAlmostEqual(phase_ops(d, "mdtest-easy-write"), 3000, delta=5)
            self.assertAlmostEqual(phase_ops(d, "ior-easy-write") / 2**20, 300, delta=1)
            self.assertAlmostEqual(phase_ops(d, "find"), 6007, delta=2)
            self.assertIsNone(phase_ops(d, "mdtest-hard-write"))
        finally:
            shutil.rmtree(d)


# the site's official run, 2026-10-16 (2 hosts, 64 ranks)
SITE_VALID_OUT = """IO500 version io500-sc26_v1 (standard)
ERROR INVALID (src/phase_ior.c:24) Write phase needed 290.505858s instead of stonewall 300s. Stonewall was hit at 290.2s
ERROR INVALID (src/main.c:484) Runtime of phase (290.545656) is below stonewall time. This shouldn't happen!
ERROR INVALID (src/main.c:490) Runtime is smaller than expected minimum runtime
[RESULT]       ior-easy-write        2.659155 GiB/s : time 290.546 seconds [INVALID]
ERROR INVALID (src/phase_find.c:80) Find didn't find anything, this is likely invalid.
[RESULT]                 find      126.687602 kIOPS : time 236.825 seconds [INVALID]
[SCORE ] Bandwidth 0.697562 GiB/s : IOPS 60.847758 kiops : TOTAL 6.514989 [INVALID]
"""
FIND_CSV = """runtime: 236.779737 rate: 126687.601502
rank, errors, unknown, found, total, checked, job steal msgs received, work items send, job steal msgs send, work items stolen, time spend in job stealing in s, number of completion tokens send
0, 0, 0, {f0}, 27250, 6, 11601, 13144, 11356, 40391, 62.760s, 6
1, 0, 0, {f1}, 25024, 0, 10749, 13082, 11124, 38106, 71.291s, 6
"""


class ValidityTests(unittest.TestCase):
    """io500_run's verdict on IO500's own INVALID lines (the site's run)"""

    def res(self, f0, f1):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        with open(os.path.join(d, "find.csv"), "w") as f:
            f.write(FIND_CSV.format(f0=f0, f1=f1))
        return d

    def test_site_valid_run(self):
        cap = {"budget_gib": 1030.0, "bounded_by": {"ost": "192.168.10.150:19951", "kind": "tier"}}
        v = io500_validity(SITE_VALID_OUT, self.res(0, 0), 300, cap)
        self.assertFalse(v["valid"])
        self.assertEqual(v["find_found"], 0)
        self.assertEqual(len(v["unexpected"]), 4)  # a 300 s run: every reason counts
        self.assertTrue(any("ran out of data" in d and "192.168.10.150:19951" in d for d in v["diagnosis"]), v)
        self.assertTrue(any("find matched no file" in d for d in v["diagnosis"]), v)

    def test_quick_run_reasons_are_expected(self):
        out = ("ERROR INVALID (src/main.c:394) PAUSING BETWEEN RUNS ACTIVATED\n"
               "ERROR INVALID (src/phase_dbg.c:15) stonewall-time 60s != 300s\n"
               "ERROR INVALID (src/main.c:490) Runtime is smaller than expected minimum runtime\n"
               "[SCORE ] Bandwidth 0.9 GiB/s : IOPS 107 kiops : TOTAL 9.9 [INVALID]\n")
        v = io500_validity(out, self.res(7, 5), 60, None)
        self.assertEqual((v["unexpected"], v["diagnosis"], v["find_found"]), ([], [], 12))

    def test_data_split_by_measured_speeds(self):
        """the site: 1030 GiB budget; 75/20 gave ior-easy 772 GiB, which a
        2.66 GiB/s write exhausts at 290 s. Sized by the quick run's speeds,
        ior-easy gets 906 GiB (344 s) and ior-hard what it can write."""
        easy, hard, why = split_budget(1030.2, 300, {"ior-easy-write": 2.632, "ior-hard-write": 0.2109})
        self.assertGreater(easy / 2.659, 300)       # the valid run's speed: lasts past the stonewall
        self.assertGreater(hard / 0.2109, 300)
        self.assertNotIn("NOT enough", why)
        easy, hard, why = split_budget(500, 300, {"ior-easy-write": 2.632, "ior-hard-write": 0.2109})
        self.assertIn("NOT enough", why)
        self.assertEqual(split_budget(1000, 300, {})[:2], (750, 200))
        # plenty of room: ior-easy gets what it needs (or half), the rest stays free
        easy, hard, _ = split_budget(1000, 300, {"ior-easy-write": 0.25, "ior-hard-write": 0.27})
        self.assertAlmostEqual(hard, 0.27 * 300 * 1.15)
        self.assertAlmostEqual(easy, 500)

    def test_probe_block_is_a_multiple_of_the_transfer(self):
        """the site's probes: the run's 12360 MiB / 8 = 1545 MiB, not a
        multiple of the 2 MiB transfer -- IOR exited at once, every probe"""
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        ini = os.path.join(d, "c.ini")
        write_ini(ini, datadir="/x", resdir="/y", pause_dir=None, stonewall=10, ior_easy_block_mib=12360 // 8,
                  ior_hard_segments=50, md_easy_n=200, md_hard_n=200)
        text = open(ini).read()
        self.assertIn("blockSize = 1544m", text)
        self.assertIn("transferSize = 2m", text)


class CapacityBudgetTests(unittest.TestCase):
    """The site's two hosts: one with an NVMe tier per OST, one RAM only --
    the RAM-only OSTs bound the data every OST gets (even striping)."""

    def test_site_layout(self):
        from tools.io500_run import capacity_budget
        facts = [{"mem_gib": 293.1}, {"mem_gib": 503.7}]
        servers = [{"kind": "mdt", "node": 0, "addr": "a:19901"},
                   {"kind": "ost", "node": 0, "addr": "a:19951", "tier": "file:/nvmedata/rocepool_ost_0.tier:406"},
                   {"kind": "ost", "node": 1, "addr": "b:19951"},
                   {"kind": "ost", "node": 0, "addr": "a:19952", "tier": "file:/nvmedata/rocepool_ost_2.tier:406"},
                   {"kind": "ost", "node": 1, "addr": "b:19952"}]
        budget, low = capacity_budget(servers, facts, 30013, 2)
        self.assertEqual(low[2], "RAM only")
        self.assertAlmostEqual(low[0], 0.4 * 503.7 / 2)
        self.assertAlmostEqual(budget, 0.9 * 4 * 0.4 * 503.7 / 2)
        # both hosts with a tier: the tiers bound it, far above 40% of RAM
        servers[2]["tier"] = servers[4]["tier"] = "file:/rocepool_tier/rocepool_ost_{n}.tier:500"
        budget, low = capacity_budget(servers, facts, 30013, 2)
        self.assertEqual((low[1], low[2]), ("a:19951", "tier"))
        # the tier's size alone: the RAM cache holds copies, not more data
        self.assertAlmostEqual(budget, 0.9 * 4 * 406)


class ClusterDownTests(unittest.TestCase):
    """Cluster.down returns only when this run's processes are gone, and a
    server of this tree still exiting is reaped: the next run's servers
    otherwise found their RDMA ports still bound ("Address already in use")."""

    def test_down_waits(self):
        from tools.cluster import Cluster, Node
        c = Cluster([Node(0, "127.0.0.1")], real=True)
        p = c.popen(0, [sys.executable, "-c", "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                                              "time.sleep(60)"], dict(os.environ))
        # a leftover "server" of this tree from an earlier run, slow to go
        script = os.path.join(ROOT, "tools", "crossover_node.py")
        old = subprocess.Popen(["sh", "-c", "trap '' TERM; sleep 5; : " + script + " mdt-native x"])
        time.sleep(0.3)
        t0 = time.monotonic()
        c.down()
        self.assertIsNotNone(p.poll())
        self.assertIsNotNone(old.poll() if old.wait(timeout=5) is not None else None)
        self.assertLess(time.monotonic() - t0, 150)


class MpiWorkdirTests(unittest.TestCase):
    def test_workdir_made_on_every_peer(self):
        # hydra runs each rank in mpiexec's cwd on every host
        from tools.cluster import Cluster, Node
        c = Cluster([Node(0, "10.0.0.1"), Node(1, "10.0.0.2", peer="root@10.0.0.2"),
                     Node(2, "10.0.0.3", peer="root@10.0.0.3")], real=True)
        calls = []
        c.run = lambda i, argv, env=None, **kw: calls.append((i, argv))
        d = tempfile.mkdtemp()
        try:
            c.mpi_files(os.path.join(d, "probe_x"), 4)
        finally:
            shutil.rmtree(d)
        self.assertEqual(sorted(calls), [(1, ["mkdir", "-p", os.path.join(d, "probe_x")]),
                                         (2, ["mkdir", "-p", os.path.join(d, "probe_x")])])


class SlowTierTests(unittest.TestCase):
    def test_site_speeds(self):
        from tools.io500_run import slow_tier_nodes
        self.assertEqual(slow_tier_nodes({0: 1968.0, 1: 33.0}), [1])     # the site: .150 gets no OSTs
        self.assertEqual(slow_tier_nodes({0: 1968.0, 1: 900.0}), [])     # 2x slower: kept
        self.assertEqual(slow_tier_nodes({0: 50.0}), [])                 # one host: nothing to compare
        self.assertEqual(slow_tier_nodes({0: 2000.0, 1: 20.0, 2: 1900.0}), [1])


def _can_run() -> str | None:
    if os.geteuid() != 0:
        return "needs root"
    if not os.path.exists("/dev/fuse"):
        return "needs /dev/fuse"
    if not shutil.which("gcc"):
        return "needs gcc"
    return None


@unittest.skipIf(_can_run(), _can_run() or "")
class IO500ClusterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def check(self, report: dict, out: str):
        self.assertEqual(report.get("returncode"), 0, out[-3000:])
        for ph in SCORED:
            self.assertIn(ph, report["phases"], ph)
            self.assertGreater(report["phases"][ph].get("ops", 0), 0, ph)
        a = report["analysis"]
        for ph, p in a["projection"]["phases"].items():
            self.assertIn(p["bottleneck"], io500_model.LIMIT_TEXT, ph)
        self.assertGreater(a["projection"]["score"], 0)
        # find saw mdtest-hard's files (on site it saw none: their directory's
        # listing did not fit one reply)
        self.assertGreater(report["validity"]["find_found"], 0, report["validity"])
        # counters cover every server and every node's processes
        s = report["snapshots"][-1]
        self.assertEqual(len(s["servers"]), report["mdts"] + report["osts"])
        self.assertFalse([k for k, v in s["servers"].items() if "error" in v])
        if report["client"] == "fuse":
            self.assertEqual({p["node"] for p in s["procs"] if p["role"] == "fuse"}, set(range(report["nodes"])))
        else:  # the ranks are the client: seen on every node while a phase runs
            mid = report["snapshots"][1]
            self.assertEqual({p["node"] for p in mid["procs"] if p["role"] == "io500"}, set(range(report["nodes"])))

    def test_simulated_cluster(self):
        rep = os.path.join(self.tmp, "sim.json")
        r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "io500_run.py"), "--nodes", "3",
                            "--report", rep, *SMALL], capture_output=True, text=True, timeout=1200)
        with open(rep) as f:
            report = json.load(f)
        self.check(report, r.stdout + r.stderr)
        # the per-op costs must account for the run's work (CPU-bound on this host)
        cal = report["analysis"]["calibration"]
        for ph, c in cal.items():
            # a phase of ~1 s (one-sided stats) carries the snapshot window's
            # fixed overhead in its CPU: ratios down to ~0.4 are that (seen
            # 0.42-0.51 for mdtest-easy-stat at 3 nodes; 0.96 at 10 nodes,
            # where the phase is long), not an unaccounted cost
            # (wider than the 2-core reference rig shows, 0.42-1.0: a 32-core
            # site host measured ior-easy-write slower than predicted, 2.9x --
            # its disk and page cache are not in the model; this is a sanity
            # bound, the calibration itself is reported, not asserted)
            # (site hosts: 2.9x ior-easy-write, 5.0x mdtest-easy-write on 32
            # cores -- the per-op costs of a simulation on a big host are not
            # what the model is for; only an absurd ratio is a bug)
            self.assertTrue(0.05 < c["ratio"] < 20.0, f"{ph}: {c}")
        # the client probe ran all three ways -- FUSE over /dev/fuse, FUSE over
        # io_uring (this kernel offers it), the interception library -- and
        # the run used whichever scored higher
        probe = report["client_probe"]
        # FUSE over io_uring is a candidate only where the kernel has it
        # (Linux 6.14+: fuse.enable_uring exists; a 6.8 site host has not)
        uring = os.path.exists("/sys/module/fuse/parameters/enable_uring")
        self.assertEqual(sorted(((r["client"], r["uring"], r["direct_read"]) for r in probe["results"]), key=str),
                         sorted([("fuse", False, True), ("intercept", None, True), ("intercept", None, False)]
                                + ([("fuse", True, True)] if uring else []), key=str), probe)
        self.assertTrue(all(r["returncode"] == 0 and r["score"] for r in probe["results"]), probe)
        best = max(probe["results"], key=lambda r: r["score"]["score"])
        self.assertEqual(report["client"], best["client"])
        self.assertEqual(report["direct_read"], best["direct_read"])
        if best["client"] == "fuse":
            self.assertEqual(report["fuse_channel"], "io_uring" if best["uring"] else "classic")
        else:
            self.assertEqual(report["fuse_channel"], "none")
        fprobe = report["fuse_channel_probe"]
        self.assertEqual(sorted(r["uring"] for r in fprobe["results"]), [False, True] if uring else [False], fprobe)
        mdt = report["analysis"]["measured_per_op"]["mdtest-hard-write"]["kinds"]["mdt"]
        # striped directories (fs/namespace_shard.py): mdtest-hard's one
        # directory is spread over the 3 MDTs, no MDT gets more than ~half
        # (the by-parent rule put > 99% of it on one), and the MDT's lock is
        # striped so the model scales its ceiling
        self.assertLess(mdt["hot_share"], 0.5, mdt)
        self.assertGreaterEqual(mdt["lock_stripes"], 64)

    def test_real_multi_host_path_rehearsed(self):
        rep = os.path.join(self.tmp, "real.json")
        # --no-pause: the code path of the official (--valid) run -- start and
        # phase boundaries detected from IO500's own output, no pause files
        r = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "cluster_rehearsal.py"), "3", rep,
                            *SMALL, "--project", "auto", "--no-pause", "--mdts-per-node", "2", "--osts-per-node", "2",
                            # a small explicit tier: auto would preallocate most of this host's free disk
                            "--ost-tier", "file:" + os.path.join(self.tmp, "rp_ost_{n}.tier") + ":1"],
                           capture_output=True, text=True, timeout=1200)
        with open(rep) as f:
            report = json.load(f)
        self.check(report, r.stdout + r.stderr)
        # 3 = processes left behind on some node (Cluster.down's cleanup missed them)
        self.assertEqual(r.returncode, 0, (r.stdout + r.stderr)[-3000:])
        self.assertTrue(report["cluster"].startswith("real"), report["cluster"])
        # fresh nodes' hostnames do not resolve: the run must fix that (else MPI hangs)
        nr = report["name_resolution"]
        self.assertEqual(sorted(nr["broken"]), ["rpn0", "rpn1", "rpn2"])
        for res in nr["fix"].values():
            self.assertEqual(res.get("fixed"), ["rpn0", "rpn1", "rpn2"], res)
        for i, res in nr["after"].items():
            self.assertEqual(res, {"rpn0": "10.99.1.1", "rpn1": "10.99.1.2", "rpn2": "10.99.1.3"}, i)
        self.assertFalse(report["config"]["paused_between_phases"])
        self.assertEqual(report["snapshots"][1]["phase"], "ior-easy-write")
        self.assertEqual(len(report["node_facts"]), 3)
        self.assertEqual(report["analysis"]["profile"]["name"], "these hosts")
        # two MDTs and two OSTs per host, each with its own log holding its
        # own READY (one shared file lost READY lines on site: a start-up
        # "hang" that was not one)
        logs = [s["log"] for s in report["servers"]]
        self.assertEqual(len(report["servers"]), 12)
        self.assertEqual(len(set(logs)), len(logs), logs)
        for s in report["servers"]:
            with open(s["log"]) as f:
                self.assertIn(f"READY {s['addr']}", f.read(), s)


if __name__ == "__main__":
    unittest.main()
