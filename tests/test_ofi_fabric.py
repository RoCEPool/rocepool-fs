"""Which libfabric fabric the transport takes. A provider can offer one device
under several fabrics: libfabric 2.x lists EFA's "efa-direct" (no message
segmentation, no buffering of unexpected messages, RNR retried forever)
before "efa" on newer EFA devices, and a pool on it hung at its first write
across machines. On EFA the transport asks for "efa" by name; ROCE_OFI_FABRIC
overrides ("any": the provider's first). Here, on the tcp provider: the
fabric each process took is in its log, and a fabric the provider does not
offer is an error at once, naming it -- not a server that never answers."""
import json
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
        import tempfile
        cls.tmp = tempfile.mkdtemp()

    def serve(self, port, **env):
        e = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, **{"ROCE_OFI_PROVIDER": "tcp", **env})
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


    def hints(self, port, **env):
        """every fi_getinfo the transport sends, through an LD_PRELOAD spy"""
        from roce.native.build_lib import find_libfabric
        prefix = find_libfabric()
        if not prefix:
            self.skipTest("no libfabric headers")
        spy = os.path.join(self.tmp, "spy.so")
        if not os.path.exists(spy):
            subprocess.run(["gcc", "-O2", "-fPIC", "-shared", f"-I{prefix}/include", "-o", spy,
                            os.path.join(ROOT, "tests", "fakes", "ofi_hints_spy.c"), "-ldl"], check=True)
        log = os.path.join(self.tmp, f"h{port}.jsonl")
        self.serve(port, LD_PRELOAD=spy, OFI_HINTS_SPY=log, **env)
        rows = [json.loads(x) for x in open(log).read().splitlines()] if os.path.exists(log) else []
        return [x for x in rows if ";" not in x["prov"]]      # not libfabric's own layered calls

    def test_efa_hints_name_the_efa_fabric(self):
        """no EFA here: the request is what is checked (the provider then says no)"""
        h = self.hints(25400, ROCE_OFI_PROVIDER="efa")
        self.assertTrue(h)
        self.assertEqual({(x["prov"], x["fabric"]) for x in h}, {("efa", "efa")})

    def test_gpu_memory_asks_for_device_registrations(self):
        """FI_HMEM only with FI_MR_HMEM: efa refuses FI_HMEM without it
        ("FI_HMEM capability requires device registrations")"""
        for i, prov in enumerate(("efa", "tcp")):
            h = self.hints(25500 + i, ROCE_OFI_PROVIDER=prov, ROCE_OFI_HMEM="1")
            self.assertTrue(h, prov)
            self.assertEqual({(x["hmem"], x["mr_hmem"]) for x in h}, {(1, 1)}, prov)
        h = self.hints(25600, ROCE_OFI_HMEM="0")
        self.assertEqual({(x["hmem"], x["mr_hmem"]) for x in h}, {(0, 0)})


if __name__ == "__main__":
    unittest.main()
