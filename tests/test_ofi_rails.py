"""Several rails on the libfabric transport: a host with several EFA devices
(hpc6id.32xlarge: one per network card, 200 Gb/s only with both) opens a
fabric domain per device; clients spread their connections over them and
each server serves on one (ROCE_OFI_RAIL). On one device ROCE_OFI_RAILS=2
opens a second, independent domain (its own address vector and memory
keys) -- so every cross-rail path runs here: a client domain talking to a
server on the other rail, requests the server pulls from the client's
memory (> 64 KiB) and replies it writes, in both directions, from many
threads at once."""
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CLIENT = r'''
import ctypes, hashlib, os, sys, threading
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
from roce.remote_engine import RemoteEngine
osts = sys.argv[2].split(",")
sizes = [100, 70_000, 300_000, 2_000_000]
bad = []
def work(t):
    for o in osts:
        e = RemoteEngine(o, RoCETransport())
        for k, n in enumerate(sizes):
            v = hashlib.sha256(f"{t}.{k}.{o}".encode()).digest() * (n // 32 + 1)
            v = v[:n]
            e.put(f"k{t}.{k}", v)
            got = e.get(f"k{t}.{k}")
            if got != v:
                bad.append((t, k, o, len(got or b"")))
ts = [threading.Thread(target=work, args=(t,)) for t in range(8)]
[x.start() for x in ts]; [x.join() for x in ts]
lib = ctypes.CDLL(os.environ["ROCE_TRANSPORT_LIB"])
print("RAILS", lib.roce_transport_rails())
print("DOMAINS", lib.roce_transport_domains_open())
print("BAD", bad)
'''


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class OfiRailsTests(unittest.TestCase):
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

    def serve(self, port, rail, rails, n_osts):
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, ROCE_OFI_PROVIDER="tcp",
                   ROCE_OFI_RAILS=str(rails), ROCE_OFI_RAIL=str(rail), ROCE_OFI_SERVER_EPS="2")
        p = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"), ROOT,
                              str(port), "0", str(n_osts)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, env=env)
        self.addCleanup(self.stop, p)
        line = p.stdout.readline().split()
        self.assertEqual(line[:1], ["READY"], p.stderr.read() if p.poll() is not None else line)
        return p, line[-1].split(",")

    def stop(self, p):
        if p.poll() is None:
            p.stdin.close()
            p.wait(timeout=20)

    def client(self, osts, rails):
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, ROCE_OFI_PROVIDER="tcp",
                   ROCE_OFI_RAILS=str(rails), ROCE_OFI_CLIENT_EPS="4")
        r = subprocess.run([sys.executable, "-c", CLIENT, ROOT, ",".join(osts)], env=env, capture_output=True,
                           text=True, timeout=300)
        out = {l.split(" ", 1)[0]: l.split(" ", 1)[1] for l in r.stdout.splitlines() if " " in l}
        self.assertIn("BAD", out, r.stdout + r.stderr)
        return out

    def test_two_rails_every_path(self):
        p0, o0 = self.serve(24600, 0, 2, 2)   # two OSTs on rail 0
        p1, o1 = self.serve(24700, 1, 2, 2)   # two on rail 1
        out = self.client(o0 + o1, 2)           # the client's 4 connections alternate rails
        self.assertEqual(out["RAILS"], "2")
        self.assertEqual(out["DOMAINS"], "2")  # both rails carried connections
        self.assertEqual(out["BAD"], "[]")
        p1.stdin.close()
        p1.wait(timeout=20)
        self.assertIn("serves on rail 1 of 2", p1.stderr.read())

    def test_numa_rail_without_a_pci_device_falls_back_to_rail_0(self):
        """ROCE_OFI_RAIL=numa picks the device on the server's NUMA node from
        its PCI address; a provider without one (tcp here) serves on rail 0"""
        env_was = os.environ.get("ROCEPOOL_NUMA_NODE")
        os.environ["ROCEPOOL_NUMA_NODE"] = "1"
        try:
            p, o = self.serve(24900, "numa", 2, 1)
        finally:
            if env_was is None:
                os.environ.pop("ROCEPOOL_NUMA_NODE")
            else:
                os.environ["ROCEPOOL_NUMA_NODE"] = env_was
        self.assertEqual(self.client(o, 2)["BAD"], "[]")
        p.stdin.close()
        p.wait(timeout=20)
        self.assertIn("serves on rail 0 of 2", p.stderr.read())

    def test_one_rail_unchanged(self):
        _, o = self.serve(24800, 0, 1, 2)
        out = self.client(o, 1)
        self.assertEqual(out["RAILS"], "1")
        self.assertEqual(out["DOMAINS"], "1")
        self.assertEqual(out["BAD"], "[]")


if __name__ == "__main__":
    unittest.main()
