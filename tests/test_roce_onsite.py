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


class ParallelShipTests(unittest.TestCase):
    """The tree goes to every peer at once (cluster_step's thread pool), and
    each peer first checks whether it shares this machine's filesystem by
    writing a token into the tree. The 6-node run 40984482522 lost one
    peer's copy: the token, created and removed in the tree's top directory
    while another peer's tar read it, made tar exit 1 ("file changed as we
    read it"), and with that peer gone there was no IO500. A fake ssh runs
    each "remote" command here, with the tree's path mapped to a directory
    per peer."""

    def setUp(self):
        import tools.roce_onsite as onsite
        self.onsite = onsite
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.src = os.path.join(self.tmp, "src")
        # the real tree is ~28 MB (mostly deps/*.tar.xz): its tar takes about
        # a second, long enough for other peers' checks to land inside it --
        # a tree of small files is archived before any can
        os.makedirs(os.path.join(self.src, "deps"))
        with open(os.path.join(self.src, "deps", "bundle.tar.xz"), "wb") as fh:
            fh.write(os.urandom(20 << 20))
        for d in range(20):
            os.makedirs(os.path.join(self.src, "pkg%d" % d))
            for f in range(30):
                with open(os.path.join(self.src, "pkg%d" % d, "m%d.py" % f), "w") as fh:
                    fh.write("x = %d\n" % f * 50)
        bindir = os.path.join(self.tmp, "bin")
        os.makedirs(bindir)
        fake = os.path.join(bindir, "ssh")
        with open(fake, "w") as fh:  # ssh [opts] peer cmd -> cmd here, the tree's path -> peers/<peer>
            fh.write(f"""#!/usr/bin/env python3
import os, subprocess, sys
peer, cmd = sys.argv[-2], sys.argv[-1]
dest = os.path.join({self.tmp!r}, "peers", peer.replace("@", "_"))
sys.exit(subprocess.call(["sh", "-c", cmd.replace({self.src!r}, dest)]))
""")
        os.chmod(fake, 0o755)
        self.old_path, self.old_root = os.environ["PATH"], onsite.ROOT
        os.environ["PATH"] = bindir + os.pathsep + self.old_path
        onsite.ROOT = self.src
        self.addCleanup(self.restore)

    def restore(self):
        os.environ["PATH"] = self.old_path
        self.onsite.ROOT = self.old_root

    def test_five_peers_at_once_all_get_the_tree(self):
        """Before the fix this failed in nearly every round (locally: 10 of
        10 rounds with 5 peers, 8 of 10 with 2): what lined the checks up
        against the copies on AWS is the ssh round trip before them
        (prepare() asks each peer for its C library), which differs per
        peer -- here a random delay of up to a second."""
        import concurrent.futures as cf
        import random
        import time
        peers = ["root@10.0.0.%d" % i for i in range(1, 6)]

        def ship(p):
            time.sleep(random.uniform(0, 1.0))
            return self.onsite.ship_same_path(p, timeout=120)
        for _ in range(3):
            with cf.ThreadPoolExecutor(len(peers)) as ex:
                out = list(ex.map(ship, peers))
            self.assertEqual(len(out), 5)
        for p in peers:
            d = os.path.join(self.tmp, "peers", p.replace("@", "_"))
            self.assertEqual(len(os.listdir(d)), 21, os.listdir(d))
        self.assertEqual(sorted(os.listdir(self.src)),
                         sorted(["pkg%d" % d for d in range(20)] + ["deps", ".bench_cache"]))
        self.assertEqual(os.listdir(os.path.join(self.src, ".bench_cache")), [])  # no token left behind

    def test_a_failed_copy_is_tried_again(self):
        calls = []
        real = self.onsite._tar_once

        def flaky(peer, args, timeout):
            calls.append(peer)
            return (255, 0, "Connection reset", "") if len(calls) == 1 else real(peer, args, timeout)

        self.onsite._tar_once = flaky
        self.addCleanup(setattr, self.onsite, "_tar_once", real)
        self.onsite.ship_same_path("root@10.0.0.9", timeout=120)
        self.assertEqual(len(calls), 2)


class RanksChoiceTests(unittest.TestCase):
    """the valid run's ranks per node: a second quick run with as many
    ranks as the hosts have cores (at most 48), the better score wins"""

    def report(self, d, name, rpn, score, cores=48):
        p = os.path.join(d, name)
        with open(p, "w") as f:
            json.dump({"ranks_per_node": rpn, "score": {"score": score} if score else {},
                       "node_facts": [{"cores": cores}, {"cores": cores + 16}]}, f)
        return p

    def test_more_and_pick(self):
        from tools.roce_onsite import more_ranks, pick_ranks
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        a = self.report(d, "a.json", 32, 80.0)
        self.assertEqual(more_ranks(a), 48)                                  # the smallest host's cores
        self.assertEqual(more_ranks(self.report(d, "x.json", 32, 80.0, cores=96)), 48)  # capped
        self.assertIsNone(more_ranks(self.report(d, "y.json", 32, 80.0, cores=32)))   # no more cores
        self.assertIsNone(more_ranks(self.report(d, "z.json", 32, None)))            # the run failed
        self.assertIsNone(more_ranks(os.path.join(d, "missing.json")))
        b = self.report(d, "b.json", 48, 90.0)
        self.assertEqual(pick_ranks([a, b])[:2], (48, b))
        c = self.report(d, "c.json", 48, 81.0)                               # within 2%: fewer ranks
        self.assertEqual(pick_ranks([a, c])[:2], (32, a))
        e = self.report(d, "e.json", 48, None)                               # the probe failed
        self.assertEqual(pick_ranks([a, e])[:2], (32, a))
        self.assertIn("-> the valid run uses 48", pick_ranks([a, b])[2])
        with self.assertRaises(ValueError):
            pick_ranks([e])


class EpsChoiceTests(unittest.TestCase):
    """the valid run's OFI client endpoints per rank (a rank's requests in
    flight): as many as the EFA queue pairs allow, measured against the
    default 2 in IOR-only quick runs, the better bandwidth wins, and the
    valid run's data sized by the chosen probe's write speeds"""

    def report(self, d, name, rpn=32, qp=1024, cores=48, eps=None, ior=None, md=None):
        p = os.path.join(d, name)
        phases = {}
        for k, v in (ior or {}).items():
            phases[k] = {"value": v}
        for k, v in (md or {}).items():
            phases[k] = {"value": v}
        with open(p, "w") as f:
            json.dump({"ranks_per_node": rpn, "config": {"client_eps": eps}, "phases": phases,
                       "node_facts": [{"cores": cores, "efa_max_qp": qp}, {"cores": cores, "efa_max_qp": qp and qp + 64}]}, f)
        return p

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, ignore_errors=True)

    def test_the_most_the_queue_pairs_allow(self):
        from tools.roce_onsite import client_eps_max
        # reserve: 2 servers x 12 endpoints (48 cores) + 2 per rank (MPI) + 16
        self.assertEqual(client_eps_max(self.report(self.d, "a.json", qp=1024)), 16)          # capped
        self.assertEqual(client_eps_max(self.report(self.d, "b.json", qp=256)), 4)             # (256-104)//32
        self.assertEqual(client_eps_max(self.report(self.d, "c.json", qp=256), 48), 2)         # 48 ranks: no room
        self.assertEqual(client_eps_max(self.report(self.d, "d.json", qp=None)), 2)            # not EFA / unknown
        self.assertEqual(client_eps_max(os.path.join(self.d, "missing.json")), 2)

    def test_probes_only_when_there_is_room(self):
        from tools.roce_onsite import eps_probes
        runs = eps_probes(self.report(self.d, "a.json", qp=256), 32, "60")
        self.assertEqual([r[0] for r in runs], ["io500-eps2", "io500-eps4"])
        self.assertEqual(runs[1][2], ["--stonewall", "60", "--phases",
                                      "ior-easy-write,ior-hard-write,ior-easy-read,ior-hard-read",
                                      "--client-eps", "4", "--ranks-per-node", "32"])
        self.assertEqual(eps_probes(self.report(self.d, "b.json", qp=None), None, "60"), [])

    def test_pick_and_rates(self):
        from tools.roce_onsite import merged_rates, pick_eps
        ior2 = {"ior-easy-write": 16.0, "ior-hard-write": 6.5, "ior-easy-read": 22.0, "ior-hard-read": 10.0}
        ior16 = {"ior-easy-write": 16.5, "ior-hard-write": 12.0, "ior-easy-read": 22.0, "ior-hard-read": 18.0}
        a = self.report(self.d, "e2.json", eps=2, ior=ior2)
        b = self.report(self.d, "e16.json", eps=16, ior=ior16)
        self.assertEqual(pick_eps([a, b])[:2], (16, b))
        self.assertIn("-> the valid run uses 16", pick_eps([a, b])[2])
        tie = self.report(self.d, "t.json", eps=16, ior={k: v * 1.01 for k, v in ior2.items()})
        self.assertEqual(pick_eps([a, tie])[:2], (2, a))                   # within 2%: fewer endpoints
        failed = self.report(self.d, "f.json", eps=16, ior={"ior-easy-write": 16.0})
        self.assertEqual(pick_eps([a, failed])[:2], (2, a))                # the probe did not finish
        with self.assertRaises(ValueError):
            pick_eps([failed])
        quick = self.report(self.d, "q.json", ior=ior2, md={"mdtest-easy-write": 440.0, "mdtest-hard-write": 180.0})
        from tools.io500_run import load_rates
        r = load_rates(merged_rates(quick, b, os.path.join(self.d, "rates.json")))
        self.assertEqual(r, {"ior-easy-write": 16.5, "ior-hard-write": 12.0,       # the chosen probe's
                             "mdtest-easy-write": 440.0, "mdtest-hard-write": 180.0})  # the quick run's

    def test_max_qp_of_ibv_devinfo(self):
        from tools.procstat import max_qp_of
        out = "hca_id:\trdmap0s6\n\ttransport:\t\t\tunknown (4)\n\t\tmax_mr_size:\t\t\t0x1000000000\n" \
              "\t\tmax_qp:\t\t\t\t1024\n\t\tmax_qp_wr:\t\t\t4096\n"
        self.assertEqual(max_qp_of(out), 1024)
        self.assertIsNone(max_qp_of("No IB devices found\n"))
