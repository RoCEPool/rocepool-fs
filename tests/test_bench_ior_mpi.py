"""ensure_mpi() returns an MPICH: the IO500 and IOR runs use hydra's
-launcher and mpichversion. A system mpicc from another MPI (OpenMPI from a
distribution, or AWS's EFA installer's /opt/amazon/openmpi) is passed over
for RoCEPool's own MPICH; a system MPICH is used as before.

arm-compare (2026-11-22) installed OpenMPI on GitHub's runners, and every
IO500 run there failed: "mpiexec: Error: unknown option -launcher"."""
import os
import shlex
import shutil
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import bench_ior  # noqa: E402


def exe(path: str, body: str) -> None:
    with open(path, "w") as f:
        f.write("#!/bin/sh\n" + body + "\n")
    os.chmod(path, 0o755)


class EnsureMpi(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.path = os.environ["PATH"]
        self.addCleanup(os.environ.__setitem__, "PATH", self.path)
        # RoCEPool's own MPICH, already built (an mpichversion that reports
        # the configure arguments ensure_mpi wants)
        own = os.path.join(self.tmp, "own")
        os.makedirs(os.path.join(own, "bin"))
        exe(os.path.join(own, "bin", "mpicc"), "exit 0")
        conf = " ".join(shlex.quote(a) for a in bench_ior.MPICH_CONFIGURE)
        exe(os.path.join(own, "bin", "mpichversion"), f"echo 'MPICH Version: 4.2.3'; echo \"MPICH configure: {conf}\"")
        self.own_bin = os.path.join(own, "bin")
        for name, val in (("MPICH_PREFIX", own), ("ensure_shared_check", lambda b: b)):
            old = getattr(bench_ior, name)
            setattr(bench_ior, name, val)
            self.addCleanup(setattr, bench_ior, name, old)

    def system(self, name: str, mpich: bool) -> str:
        d = os.path.join(self.tmp, name, "bin")
        os.makedirs(d)
        exe(os.path.join(d, "mpicc"), "exit 0")
        if mpich:
            exe(os.path.join(d, "mpichversion"), "echo 'MPICH Version: 4.1'")
        os.environ["PATH"] = d + os.pathsep + self.path
        return d

    def test_another_mpi_on_path_is_passed_over(self):
        self.system("openmpi", mpich=False)
        self.assertEqual(bench_ior.ensure_mpi(), self.own_bin)

    def test_a_system_mpich_is_used(self):
        d = self.system("mpich", mpich=True)
        self.assertEqual(bench_ior.ensure_mpi(), d)


if __name__ == "__main__":
    unittest.main()
