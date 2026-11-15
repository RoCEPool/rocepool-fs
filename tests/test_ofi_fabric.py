"""Which libfabric fabric the transport takes. A provider can offer one device
under several fabrics: libfabric 2.x lists EFA's "efa-direct" (no message
segmentation, no buffering of unexpected messages, RNR retried forever)
before "efa" on newer EFA devices, and a pool on it hung at its first write
across machines. On EFA the transport asks for "efa" by name; ROCE_OFI_FABRIC
overrides ("any": the provider's first). Here, on the tcp provider: the
fabric each process took is in its log, and a fabric the provider does not
offer is an error at once, naming it -- not a server that never answers."""
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OfiFabricTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_softverbs_lib
        was = os.environ.get("ROCEPOOL_BACKEND")
        os.environ["ROCEPOOL_BACKEND"] = "ofi"  # the libfabric transport (its tcp provider here)
        try:
            cls.lib = ensure_softverbs_lib()
        finally:
            if was is None:
                os.environ.pop("ROCEPOOL_BACKEND")
            else:
                os.environ["ROCEPOOL_BACKEND"] = was

    def serve(self, port, **env):
        e = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, ROCE_OFI_PROVIDER="tcp", **env)
        p = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                              str(port), "0", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, env=e)
        line = p.stdout.readline().split()
        if p.poll() is None:
            p.stdin.close()
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
        return line[:1] == ["READY"], p.stderr.read()

    def test_the_fabric_taken_is_logged(self):
        ready, err = self.serve(25100)
        self.assertTrue(ready, err)
        self.assertRegex(err, r"ofi: provider tcp fabric \S+ domain \S+ rail 0")

    def test_a_fabric_the_provider_does_not_offer_is_refused(self):
        ready, err = self.serve(25200, ROCE_OFI_FABRIC="efa-direct")
        self.assertFalse(ready)
        self.assertIn("fabric efa-direct", err)

    def test_any_takes_the_providers_first(self):
        ready, err = self.serve(25300, ROCE_OFI_FABRIC="any")
        self.assertTrue(ready, err)
        self.assertIn("ofi: provider tcp fabric", err)


if __name__ == "__main__":
    unittest.main()
