"""tools/rapidsse_jmx.py (RapidsSE's JMeter plans read into steps) and the
pieces of tools/rapidsse_tpch.py that need no RapidsSE build: the simulated
cluster's broadcast (RapidsSE's heartbeat) and the FIFO data generation."""
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools.rapidsse_jmx import parse  # noqa: E402

PLAN = os.path.join(ROOT, "tests", "fixtures", "rapidsse_plan.jmx")


class PlanTests(unittest.TestCase):
    def test_steps_in_jmeter_order(self):
        p = parse(PLAN)
        # main group in order (loop expanded x3, disabled Q2 group left out),
        # then the tearDown group although it comes first in the file
        self.assertEqual([s.name for s in p.steps],
                         ["loadT", "countT"] + ["sleep", "q1"] * 3 + ["dropT"])
        self.assertEqual([s.group for s in p.steps][-1], "teardown")
        self.assertIn("/Test Plan/Main Thread Group/Queries/Q2 Group", p.disabled)
        self.assertTrue(p.steps[1].select)
        self.assertFalse(p.steps[0].select)

    def test_variables_and_overrides(self):
        p = parse(PLAN)
        self.assertEqual(p.variables["JDBC_URL"], "jdbc:se://localhost:9123")
        self.assertEqual(p.steps[2].sql, "sleep 2 ;")
        p = parse(PLAN, overrides={"QUERY_LOOP_COUNT": "1", "JDBC_URL": "jdbc:se://h:1"})
        self.assertEqual([s.name for s in p.steps], ["loadT", "countT", "sleep", "q1", "dropT"])
        self.assertEqual(p.variables["JDBC_URL"], "jdbc:se://h:1")

    def test_assertion_attached_to_the_sampler_setting_the_variable(self):
        st = parse(PLAN).steps[1]
        self.assertEqual(st.variable, "tCount")
        self.assertEqual([(a.variable, a.expected, a.kind) for a in st.assertions], [("tCount_1", "42", "equals")])
        self.assertTrue(st.assertions[0].check("42"))
        self.assertTrue(st.assertions[0].check(" 42\n"))
        self.assertFalse(st.assertions[0].check("420"))

    def test_remap_of_data_paths(self):
        st = parse(PLAN, remap={"/mnt/disk": "/run/x/disk"}).steps[0]
        self.assertEqual(st.sql, 'dload t paths "/run/x/disk1/data1/t.tbl" "/run/x/disk2/data1/t.tbl";')

    def test_unknown_elements_are_refused(self):
        bad = open(PLAN).read().replace("<IfController ", "<WhileController ").replace("</IfController>",
                                                                                       "</WhileController>")
        with tempfile.NamedTemporaryFile("w", suffix=".jmx", delete=False) as f:
            f.write(bad)
        try:
            with self.assertRaises(ValueError):
                parse(f.name)
        finally:
            os.unlink(f.name)


@unittest.skipUnless(os.geteuid() == 0 and os.path.exists("/proc/self/ns/net"), "needs root (network namespaces)")
class SimulatedClusterBroadcastTests(unittest.TestCase):
    """RapidsSE's nodes find each other by UDP broadcast to 255.255.255.255:
    the simulated nodes need a route for it (a default route, as real hosts
    have), or the send fails and every node sees only itself."""

    def test_broadcast_reaches_the_other_node(self):
        from tools import netns_cluster
        netns_cluster.up(2)
        try:
            recv = subprocess.Popen(
                ["nsenter", "--net=/run/netns/rpn1", sys.executable, "-c",
                 "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.bind(('', 18888)); "
                 "s.settimeout(10); print('READY', flush=True); print(s.recvfrom(16)[0].decode())"],
                stdout=subprocess.PIPE, text=True)
            self.assertEqual(recv.stdout.readline().strip(), "READY")
            subprocess.run(["nsenter", "--net=/run/netns/rpn0", sys.executable, "-c",
                            "import socket; t = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); "
                            "t.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1); "
                            "t.sendto(b'heartbeat', ('255.255.255.255', 18888))"], check=True)
            out, _ = recv.communicate(timeout=15)
            self.assertEqual(out.strip(), "heartbeat")
        finally:
            netns_cluster.down(quiet=True)


class AnswerComparisonTests(unittest.TestCase):
    """RapidsSE's and DuckDB's answers print numbers and dates differently;
    the comparison must see through that, and nothing else."""

    def setUp(self):
        from tools.rapidsse_tpch import answers_match
        self.match = answers_match

    def test_formatting_differences_are_equal(self):
        a = [["A", "F", "37734107.00", "56586554400.73", "25.522005853257337", "1995-03-15"]]
        b = [["A", "F", "37734107", "56586554400.7300", "25.52200585325734", "1995-03-15 00:00:00"]]
        self.assertEqual(self.match(a, b), (True, "identical"))
        # a value rounded for printing equals the exact one at its precision
        self.assertTrue(self.match([["25.52"]], [["25.522005853257337"]])[0])
        self.assertFalse(self.match([["25.53"]], [["25.522005853257337"]])[0])

    def test_real_differences_are_found(self):
        self.assertFalse(self.match([["1", "2.50"]], [["1", "2.60"]])[0])
        self.assertFalse(self.match([["BRAZIL"]], [["BRAZIL"], ["CHINA"]])[0])
        self.assertFalse(self.match([["1995-03-15"]], [["1995-03-16"]])[0])
        self.assertFalse(self.match([["abc"]], [["abd"]])[0])

    def test_tpch_validation_rules(self):
        # spec 2.1.3.5: sums within 100, averages / ratios within 1%, counts exact
        from tools.rapidsse_tpch import answers_match, select_kinds
        kinds = select_kinds("select l_returnflag, sum(l_extendedprice * (1 - l_discount)) as rev, "
                             "avg(l_quantity) as q, count(*) as n, 100.00 * sum(a) / sum(b) as share from lineitem")
        self.assertEqual(kinds, ["value", "sum", "avg", "count", "ratio"])
        ref = [["A", "53758257134.8700", "25.522005", "1478493", "16.38"]]
        self.assertEqual(answers_match([["A", "53758257182.21", "25.52", "1478493", "16.40"]], ref, kinds, tpch=True),
                         (True, "within TPC-H's validation rules"))
        ok, why = answers_match([["A", "53758257382.21", "25.52", "1478493", "16.38"]], ref, kinds, tpch=True)
        self.assertFalse(ok)
        self.assertIn("column 2 (sum)", why)
        self.assertFalse(answers_match([["A", "53758257134.87", "25.52", "1478494", "16.38"]], ref, kinds,
                                       tpch=True)[0])
        # the exact comparison sees a 47.34 difference on a 5e10 sum
        self.assertFalse(answers_match([["A", "53758257182.21", "25.52", "1478493", "16.38"]], ref, kinds)[0])

    def test_select_list_with_subqueries(self):
        from tools.rapidsse_tpch import select_kinds
        self.assertEqual(select_kinds("select ps_partkey, sum(ps_supplycost * ps_availqty) as value from partsupp "
                                      "group by ps_partkey having sum(x) > (select sum(y) from t)"),
                         ["value", "sum"])
        self.assertEqual(select_kinds("select o_year, sum(case when nation = 'BRAZIL' then volume else 0 end) / "
                                      "sum(volume) as mkt_share from (select extract(year from o_orderdate) as o_year "
                                      "from orders) as all_nations"), ["value", "ratio"])

    def test_ties_in_another_order(self):
        a = [["x", "1.00"], ["y", "1.00"]]
        b = [["y", "1"], ["x", "1"]]
        self.assertEqual(self.match(a, b), (True, "same rows, ties in another order"))


class DuckdbSqlTests(unittest.TestCase):
    def test_interval_precision_dropped_only(self):
        from tools.rapidsse_duckdb import adapt
        q = "where l_shipdate <= date '1998-12-01' - interval '90' day (3) group by x"
        self.assertEqual(adapt(q), "where l_shipdate <= date '1998-12-01' - interval '90' day group by x")
        self.assertEqual(adapt("interval '1' year"), "interval '1' year")


class RealHostsBootstrapTests(unittest.TestCase):
    """On AWS hosts (HEAD_IP / PEER_IPS from tools/aws_cluster.py --command)
    the runner sets the cluster up itself with run.sh --bench none."""

    def test_run_sh_command_and_cluster_file(self):
        from unittest import mock
        import tools.rapidsse_tpch as t
        calls = []
        tmp = tempfile.mkdtemp()

        def fake_run(argv, **kw):
            calls.append(argv)
            if argv and argv[0].endswith("run.sh"):
                open(os.path.join(tmp, "onsite_cluster.json"), "w").write("{}")
            return subprocess.CompletedProcess(argv, 0)
        env = {"HEAD_IP": "10.0.0.1", "PEER_IPS": "10.0.0.2 10.0.0.3"}
        try:
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(t, "ROOT", tmp), \
                    mock.patch.object(t.subprocess, "run", fake_run), mock.patch.object(t.shutil, "which",
                                                                                     lambda x: "/usr/bin/" + x):
                os.environ.pop("ROCEPOOL_CLUSTER", None)
                peers = t.real_hosts_bootstrap()
                self.assertEqual(os.environ["ROCEPOOL_CLUSTER"], os.path.join(tmp, "onsite_cluster.json"))
            self.assertEqual(peers, ["10.0.0.2", "10.0.0.3"])
            self.assertEqual(calls[-1][1:], ["--peers", "root@10.0.0.2=10.0.0.2,root@10.0.0.3=10.0.0.3",
                                             "--ip", "10.0.0.1", "--bench", "none", "--skip-tests"])
        finally:
            shutil.rmtree(tmp)

    def test_not_on_real_hosts(self):
        from unittest import mock
        import tools.rapidsse_tpch as t
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HEAD_IP", None)
            self.assertEqual(t.real_hosts_bootstrap(), [])


@unittest.skipUnless(shutil.which("gcc") and shutil.which("make"), "needs a compiler for dbgen")
class ParallelGenerationTests(unittest.TestCase):
    """tools/rapidsse_gen.py: several chunks per path, several nodes -- the
    union of everything written is exactly dbgen's whole dataset."""

    def test_union_is_dbgens_dataset(self):
        rse = os.environ.get("RAPIDSSE_TREE", "")
        if not os.path.isdir(os.path.join(rse, "dbgen")):
            self.skipTest("set RAPIDSSE_TREE to a RapidsSE tree")
        from tools.rapidsse_tpch import ensure_dbgen
        dbgen = ensure_dbgen(rse)
        tmp = tempfile.mkdtemp()
        try:
            ref = os.path.join(tmp, "ref")
            os.makedirs(ref)
            subprocess.run([dbgen, "-b", os.path.join(os.path.dirname(dbgen), "dists.dss"), "-f", "-q", "-s", "0.01"],
                           cwd=ref, check=True)
            gen = os.path.join(ROOT, "tools", "rapidsse_gen.py")
            for node, first in ((1, 1), (2, 5)):
                paths = [os.path.join(tmp, f"n{node}p{k}") for k in (1, 2)]
                subprocess.run([sys.executable, gen, "--dbgen", dbgen, "--sf", "0.01", "--total", "8", "--first",
                                str(first), "--chunks-per-path", "2", "--jobs", "2", "--scratch",
                                os.path.join(tmp, f"s{node}"), "--paths", *paths], check=True, capture_output=True)
            for t in ("lineitem", "orders", "partsupp", "part", "customer", "supplier"):
                got = sorted(l for n in (1, 2) for k in (1, 2)
                             for l in open(os.path.join(tmp, f"n{n}p{k}", f"{t}.tbl")))
                self.assertEqual(got, sorted(open(os.path.join(ref, f"{t}.tbl"))), t)
            for t in ("nation", "region"):
                self.assertEqual(open(os.path.join(tmp, "n1p1", f"{t}.tbl")).read(),
                                 open(os.path.join(ref, f"{t}.tbl")).read())
            self.assertFalse(os.path.exists(os.path.join(tmp, "s1")))
        finally:
            shutil.rmtree(tmp)


if __name__ == "__main__":
    unittest.main()
