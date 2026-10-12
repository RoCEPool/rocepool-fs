"""Thread and NUMA placement of the native servers: ROCEPOOL_NUMA_NODE pins
the process to that node's cores and memory (sched_setaffinity +
set_mempolicy, no libnuma), and every connection thread is pinned to one
of the allowed cores, round-robin (ROCE_PIN_THREADS=0 turns that off)."""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _status(pid, field):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith(field + ":"):
                return line.split(":", 1)[1].strip()
    return None


def _threads(pid):
    out = {}
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{tid}/comm") as f:
                name = f.read().strip()
            out[int(tid)] = (name, _status(f"{pid}/task/{tid}", "Cpus_allowed_list"))
        except OSError:
            pass
    return out


@unittest.skipUnless(shutil.which("gcc") and os.path.exists("/sys/devices/system/node/node0/cpulist"), "needs gcc, sysfs")
class NumaPinTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.tmp = tempfile.mkdtemp()
        with open("/sys/devices/system/node/node0/cpulist") as f:
            cls.node0 = f.read().strip()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def start(self, env_extra, port):
        env = dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, **env_extra)
        p = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                              ROOT, str(port), "1", "1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True, env=env)
        line = p.stdout.readline()
        if not line.startswith("READY"):  # (never read stderr eagerly: it blocks until the child exits)
            self.fail(line + p.stderr.read())
        _, mdts, osts = line.split()
        return p, mdts, osts

    def connect_some(self, osts, n=4):
        """n client processes, each its own connection, each leaves a thread on the server"""
        procs = []
        for i in range(n):
            procs.append(subprocess.Popen([sys.executable, "-c", f"""
import os, sys, time
sys.path.insert(0, {ROOT!r})
from roce.remote_engine import RemoteEngine
from roce.roce_transport import RoCETransport
e = RemoteEngine({osts!r}, RoCETransport())
e.put("k{i}", b"v")
sys.stdout.write("ok\\n"); sys.stdout.flush()
sys.stdin.readline()
"""], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                                          env=dict(os.environ, ROCE_TRANSPORT_LIB=self.lib)))
        for p in procs:
            self.assertEqual(p.stdout.readline().strip(), "ok")
        return procs

    def test_process_is_pinned_to_the_node_and_its_memory(self):
        p, mdts, osts = self.start({"ROCEPOOL_NUMA_NODE": "0"}, 23810)
        try:
            self.assertEqual(_status(p.pid, "Cpus_allowed_list"), self.node0)
            # (Mems_allowed_list is the cpuset -- all nodes on a multi-socket
            # host; the bind policy itself is read back by the server, below)
            clients = self.connect_some(osts)
            threads = _threads(p.pid)
            conn = [cpus for name, cpus in threads.values() if name in ("rt-conn", "rt-ofi")]  # RC: a thread per connection; ofi: per endpoint
            self.assertGreaterEqual(len(conn), 4, threads)
            ncores = os.cpu_count()
            if ncores and ncores > 1:
                # each connection thread on exactly one core, spread over the cores
                self.assertTrue(all("-" not in c and "," not in c for c in conn), conn)
                self.assertGreater(len(set(conn)), 1, conn)
            for c in clients:
                c.stdin.close(); c.wait(timeout=10)
        finally:
            p.stdin.close(); p.wait(timeout=10)
            err = p.stderr.read()
        self.assertIn("pinned to NUMA node 0", err)
        self.assertIn("memory preferred from it", err)  # MPOL_PREFERRED: one full node must not OOM-kill a server

    def test_pinning_off(self):
        p, mdts, osts = self.start({"ROCE_PIN_THREADS": "0"}, 23820)
        try:
            clients = self.connect_some(osts, 2)
            conn = [cpus for name, cpus in _threads(p.pid).values() if name in ("rt-conn", "rt-ofi")]  # RC: a thread per connection; ofi: per endpoint
            self.assertTrue(all(c == _status(p.pid, "Cpus_allowed_list") for c in conn), conn)
            for c in clients:
                c.stdin.close(); c.wait(timeout=10)
        finally:
            p.stdin.close(); p.wait(timeout=10)

    def test_unknown_node_is_ignored(self):
        p, mdts, osts = self.start({"ROCEPOOL_NUMA_NODE": "7"}, 23830)
        p.stdin.close(); p.wait(timeout=10)
        self.assertIn("no /sys/devices/system/node/node7", p.stderr.read())


if __name__ == "__main__":
    unittest.main()
