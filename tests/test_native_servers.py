"""Native C OST/MDT servers (roce/native/rocepool_servers.c) against the
Python reference servers (single-node PoolNode, fs/mdt_node.MDTServer):
the same op scripts run against both over the real softverbs-backed
RoCETransport and must produce identical outcomes (timestamps aside);
DistributedFileSystem's end-to-end checks run on each; and a CAS-increment
hammer checks the native store's atomicity under real thread concurrency.
Runs in a subprocess (tests/fakes/native_scenario.py) because the transport
library is loaded once per process."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@unittest.skipUnless(shutil.which("gcc"), "needs gcc to build the library")
class NativeServerParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import build_softverbs
        cls.tmp = tempfile.mkdtemp()
        lib = build_softverbs(os.path.join(cls.tmp, "libroce_native_test.so"))
        out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "native_scenario.py"), ROOT, "19700"],
                             env=dict(os.environ, ROCE_TRANSPORT_LIB=lib), capture_output=True, text=True, timeout=240)
        line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
        assert line, f"scenario produced no result:\n{out.stdout[-2000:]}\n{out.stderr[-2000:]}"
        cls.res = json.loads(line[len("RESULT "):])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_no_scenario_error(self):
        self.assertNotIn("error", self.res, self.res.get("error"))

    def test_mdt_table_registered_at_start_and_memory_reported(self):
        st = self.res["fresh_mdt_stats"]
        self.assertIs(st["table_registered"], True)
        self.assertEqual(st["reqs"], 0)  # nothing asked for the table: registered at start
        self.assertGreater(st["rss_bytes"], 0)
        self.assertIn("numa_free_bytes", st)

    def test_ost_matches_python_pool_node_exactly(self):
        self.assertEqual(self.res["ost_native"], self.res["ost_python"])

    def test_mdt_matches_python_mdt_exactly(self):
        self.assertEqual(self.res["mdt_native"], self.res["mdt_python"])

    def test_distributed_fs_end_to_end_on_native_servers(self):
        self.assertEqual(self.res["dfs_native"], [True] * len(self.res["dfs_native"]))
        self.assertEqual(self.res["dfs_python"], self.res["dfs_native"])

    def test_native_cas_is_atomic_under_concurrency(self):
        self.assertEqual(self.res["cas_native"], 8 * 40)

    def test_mdt_bucket_locks_keep_one_directory_consistent_under_concurrency(self):
        want = {"errors": [], "winners": 1, "listing_ok": True, "count_ok": True}
        self.assertEqual(self.res["mdt_hammer_native"], want)
        self.assertEqual(self.res["mdt_hammer_python"], want)


if __name__ == "__main__":
    unittest.main()
