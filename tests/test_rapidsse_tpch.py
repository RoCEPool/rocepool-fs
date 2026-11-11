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


@unittest.skipUnless(shutil.which("mkfifo") and shutil.which("gcc"), "needs mkfifo")
class FifoGenerationTests(unittest.TestCase):
    """dbgen writes <table>.tbl.<chunk> into a FIFO; the stream lands in
    <table>.tbl with no rename (rocepool has none): same bytes as dbgen's file."""

    def test_fifo_stream_equals_dbgen_file(self):
        tmp = tempfile.mkdtemp()
        try:
            prog = os.path.join(tmp, "gen.sh")  # stands in for dbgen: writes table.tbl.<chunk> in cwd
            with open(prog, "w") as f:
                f.write("#!/bin/sh\nseq 1 50000 | sed 's/$/|row|/' > supplier.tbl.3\n")
            os.chmod(prog, 0o755)
            fifo_dir, out_dir, ref_dir = (os.path.join(tmp, d) for d in ("fifo", "out", "ref"))
            for d in (fifo_dir, out_dir, ref_dir):
                os.makedirs(d)
            f = f"{fifo_dir}/supplier.tbl.3"
            subprocess.run(["sh", "-ec", f"mkfifo {f} && cd {fifo_dir} && {{ cat {f} > {out_dir}/supplier.tbl & "
                                         f"c=$!; {prog}; r=$?; wait $c && [ $r = 0 ]; }} && rm -f {f}"], check=True)
            subprocess.run([prog], cwd=ref_dir, check=True)
            self.assertEqual(open(f"{out_dir}/supplier.tbl", "rb").read(),
                             open(f"{ref_dir}/supplier.tbl.3", "rb").read())
            self.assertEqual(os.listdir(fifo_dir), [])
        finally:
            shutil.rmtree(tmp)


if __name__ == "__main__":
    unittest.main()
