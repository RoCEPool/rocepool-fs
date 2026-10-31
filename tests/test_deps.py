"""deps/: every bundled source is there, unpacks to one directory, and is
what the builders use; ROCEPOOL_OFFLINE=1 refuses to download."""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import deps  # noqa: E402


class DepsTests(unittest.TestCase):
    def test_all_bundled_and_unpack(self):
        tmp = tempfile.mkdtemp()
        try:
            for name, marker in (("mpich", "configure"), ("io500", "prepare.sh"), ("ior", "configure"),
                                 ("libfabric", "configure"), ("rdma-core", "CMakeLists.txt")):
                self.assertTrue(deps.path(name), name)
                dest = os.path.join(tmp, name)
                self.assertTrue(deps.extract(name, dest))
                self.assertTrue(os.path.exists(os.path.join(dest, marker)), (name, marker))
            # IO500 as the list's tag releases it (rules 1, 2): its git
            # checkout, no local change, IOR / pfind at prepare.sh's pins,
            # their remotes on themselves (prepare.sh's git fetch, offline)
            io = os.path.join(tmp, "io500")
            for d in ("build/ior/.git", "build/pfind/.git", "build/pfind/lz4", "build/cdcl-schema-tools/.git"):
                self.assertTrue(os.path.exists(os.path.join(io, d)), d)
            from tools.io500_run import IO500_TAG, io500_build_info
            info = io500_build_info(io)
            self.assertTrue(info["pristine"], info)
            self.assertTrue(info["version"].startswith(IO500_TAG + "_v"), info)
            for r in ("build/ior", "build/pfind"):
                url = subprocess.run(["git", "remote", "get-url", "origin"], cwd=os.path.join(io, r),
                                     capture_output=True, text=True).stdout.strip()
                self.assertEqual(url, ".", r)
            self.assertFalse(os.path.exists(os.path.join(tmp, "mpich", "modules", "yaksa")))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_offline_refuses_download(self):
        old = deps.DEPS
        deps.DEPS = tempfile.mkdtemp()
        os.environ["ROCEPOOL_OFFLINE"] = "1"
        try:
            with self.assertRaisesRegex(RuntimeError, "ROCEPOOL_OFFLINE=1 and mpich is not bundled"):
                deps.extract("mpich", os.path.join(deps.DEPS, "x"))
            with self.assertRaisesRegex(RuntimeError, "would have to download"):
                deps.check_download("MPICH")
        finally:
            shutil.rmtree(deps.DEPS, ignore_errors=True)
            deps.DEPS = old
            del os.environ["ROCEPOOL_OFFLINE"]

    def test_build_sh_finds_the_bundle(self):
        out = subprocess.run(["bash", "-c", f"grep -c unpack_dep {os.path.join(ROOT, 'roce', 'native', 'build.sh')}"],
                             capture_output=True, text=True)
        self.assertGreaterEqual(int(out.stdout.strip()), 3)


if __name__ == "__main__":
    unittest.main()
