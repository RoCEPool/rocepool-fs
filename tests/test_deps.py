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
            io = os.path.join(tmp, "io500")
            for d in ("build/ior/configure", "build/pfind/lz4", "build/cdcl-schema-tools", "schema-tools"):
                self.assertTrue(os.path.exists(os.path.join(io, d)), d)
            prep = open(os.path.join(io, "prepare.sh")).read()
            self.assertIn('[ -d "$BUILD/$ior_dir" ]', prep)
            self.assertIn("[ -x configure ] || ./bootstrap", prep)
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
