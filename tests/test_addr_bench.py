"""tools/bench_addr.py's driver (roce/native/rocepool_addr_bench.c): a block
read by key (RPC, LEASE + one-sided read, cached lease) and by address (one
RDMA READ) returns the right block in every mode, against a native OST on
the RDMA emulator."""
import json
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
@unittest.skipUnless(os.environ.get("ROCEPOOL_BACKEND", "softverbs") == "softverbs", "softverbs backend")
class AddrBenchTests(unittest.TestCase):
    port = 23720

    def test_modes(self):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_addr_bench, ensure_softverbs_lib
        bench = ensure_addr_bench()
        env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), PYTHONPATH=ROOT)
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                ROOT, str(self.port), "0", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               text=True, env=env)
        try:
            ost = srv.stdout.readline().split()[-1]
            for size, threads in ((4096, 1), (65536, 2)):
                r = subprocess.run([bench, ost, str(size), "8", str(threads), "0.3"], capture_output=True, text=True,
                                   timeout=300, env=env)
                self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
                rows = [json.loads(l[7:]) for l in r.stdout.splitlines() if l.startswith("RESULT ")]
                self.assertEqual([x["mode"] for x in rows], ["rpc", "lease", "cached", "addr"])
                for x in rows:
                    self.assertEqual((x["block_bytes"], x["threads"], x["errors"]), (size, threads, 0), x)
                    self.assertGreater(x["ops_per_s"], 0, x)
        finally:
            srv.stdin.close()
            srv.wait(timeout=20)


if __name__ == "__main__":
    unittest.main()
