"""Native KV client API (roce/native/rocepool_kv.c, Python binding roce/kv.py)
against native C OSTs and against the Python reference OSTs: semantics, CAS,
range ops, batches (one frame per OST), placement compatible with PoolNode."""
import json
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _scenario(env, osts):
    out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "kv_scenario.py"), ROOT, osts],
                         capture_output=True, text=True, env=env, timeout=180)
    line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
    assert line, out.stdout + out.stderr
    return json.loads(line[len("RESULT "):])


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class KVApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                   PYTHONPATH=ROOT)
        cls.results = {}
        for name, cmd in (("native", ["native_cluster.py", ROOT, "23600", "0", "4"]),
                          ("python", ["py_cluster.py", ROOT, "23700", "4"])):
            srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", cmd[0]), *cmd[1:]],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
            osts = srv.stdout.readline().split()[-1]
            try:
                cls.results[name] = _scenario(env, osts)
            finally:
                srv.stdin.close()
                srv.wait(timeout=20)

    def check(self, r):
        self.assertNotIn("error", r, r.get("error"))
        self.assertEqual(r["put_get"], [1, "one", 1])
        self.assertEqual(r["put_if_ok"], 2)
        self.assertEqual(r["put_if_stale"], "conflict")
        self.assertEqual(r["create_only"], "second rejected")
        self.assertEqual(r["missing"], "KeyError")
        self.assertEqual(r["write_read_at"], ["313200000068656c6c6f", "006865", ""])
        self.assertEqual(r["deleted"], "gone")
        self.assertLessEqual(r["mput_rpcs"], 4)  # one BATCH frame per OST
        self.assertLessEqual(r["mget_rpcs"], 4)
        self.assertTrue(r["mget_ok"])
        self.assertEqual(r["mdelete"], [100, 0])
        self.assertTrue(r["after_mdelete"])
        self.assertTrue(r["placement"])
        self.assertTrue(r["big_roundtrip"])

    def test_against_native_osts(self):
        self.check(self.results["native"])

    def test_against_python_reference_osts(self):
        self.check(self.results["python"])


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class KVEdgeTests(unittest.TestCase):
    """tests/fakes/kv_edge_scenario.py with a 1 MiB frame limit: version
    tokens (no ABA), BATCH validated before it runs, batches that must be
    split by request AND by reply size."""

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                   PYTHONPATH=ROOT, ROCE_MAX_FRAME_BYTES=str(1024 * 1024))
        cls.results = {}
        for name, cmd in (("native", ["native_cluster.py", ROOT, "23800", "0", "2"]),
                          ("python", ["py_cluster.py", ROOT, "23900", "2"])):
            srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", cmd[0]), *cmd[1:]],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
            osts = srv.stdout.readline().split()[-1]
            try:
                out = subprocess.run([sys.executable, os.path.join(ROOT, "tests", "fakes", "kv_edge_scenario.py"),
                                      ROOT, osts], capture_output=True, text=True, env=env, timeout=180)
                line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
                cls.results[name] = json.loads(line[7:]) if line else {"error": out.stdout + out.stderr}
            finally:
                srv.stdin.close()
                srv.wait(timeout=20)

    def check(self, r):
        self.assertNotIn("error", r, r.get("error"))
        self.assertEqual(r["tokens"], {"present_token_is_version": True, "absent_token_moved": True,
                                       "stale_absent_token": "rejected", "stale_present_token": "rejected",
                                       "recreated_version_above_old": True, "value": "fresh"})
        self.assertEqual(r["batch_val"], {"reply": "ERROR", "first_applied": False})
        self.assertTrue(r["mget_big"])
        self.assertEqual(r["mput_big"], {"stored": True, "huge": "errno 27", "others": True})
        self.assertEqual(r["py_chunks"], [30000, 30000])

    def test_native_osts(self):
        self.check(self.results["native"])

    def test_python_reference_osts(self):
        self.check(self.results["python"])


if __name__ == "__main__":
    unittest.main()
