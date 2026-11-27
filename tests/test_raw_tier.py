"""OSTs on raw block devices (the AWS hosts' "raw" NVMe layout,
/dev/rocepool-tier-<j>; here loop devices): the tier is the device itself,
its commit a device flush, the logs on a file system beside it. An
ior-hard-shaped write (shared file, 47008-byte pieces, fsync at close) reads
back after the servers are SIGKILLed and restarted, both with writes synced
one by one and with ROCEPOOL_OST_ASYNC."""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tests.test_durability import _Servers, _stats  # noqa: E402
from tests.test_ost_async import CHECK, PIECE, WRITER  # noqa: E402


def _losetup_ok() -> bool:
    if os.geteuid() != 0 or not shutil.which("losetup"):
        return False
    r = subprocess.run(["losetup", "-f"], capture_output=True, text=True)
    return r.returncode == 0


@unittest.skipUnless(shutil.which("gcc") and _losetup_ok(), "needs gcc and loop devices (root)")
class RawTierTests(unittest.TestCase):
    port = 27200

    @classmethod
    def setUpClass(cls):
        from roce.native.build_lib import ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        for n in range(3):
            img = os.path.join(self.tmp, f"img{n}")
            subprocess.run(["truncate", "-s", "1G", img], check=True)
            dev = subprocess.run(["losetup", "-f", "--show", img], capture_output=True, text=True,
                                 check=True).stdout.strip()
            self.addCleanup(subprocess.run, ["losetup", "-d", dev])
            os.symlink(dev, os.path.join(self.tmp, f"rocepool-tier-{n}"))
        self.srv = _Servers(self.lib, self.tmp, self.port, nm=1, no=3,
                            env={"ROCEPOOL_OST_TIER": f"file:{self.tmp}/rocepool-tier-{{n}}",
                                 "ROCEPOOL_OST_RAM_CACHE_MIB": "2"})   # most of the data from the devices
        self.srv.start()
        self.addCleanup(self.srv.stop)
        self.prefix = os.path.join(self.tmp, "rp")
        type(self).port += 20

    def env(self, async_on):
        return dict(os.environ, ROCE_TRANSPORT_LIB=self.lib, PYTHONPATH=ROOT, LD_PRELOAD=self.icp,
                    ROCEPOOL_PREFIX=self.prefix, ROCEPOOL_MDTS=self.srv.mdts, ROCEPOOL_OSTS=self.srv.osts,
                    ROCEPOOL_ATTR_TTL="0", ROCEPOOL_DIRTY_CAP_MIB="1", ROCEPOOL_STRIPE_SIZE=str(1 << 20),
                    ROCEPOOL_OST_ASYNC="1" if async_on else "0")

    def write_and_check(self, async_on, ranks=3, n=120):
        e = self.env(async_on)
        subprocess.run([sys.executable, "-c", 'import os, sys; os.makedirs(sys.argv[1] + "/d")', self.prefix],
                       env=e, check=True, timeout=120)
        ps = [subprocess.Popen([sys.executable, "-c", WRITER, self.prefix, str(r), str(ranks), str(n), "1"], env=e,
                               stderr=subprocess.PIPE, text=True) for r in range(ranks)]
        errs = [p.communicate(timeout=300)[1] for p in ps]
        self.assertEqual([p.returncode for p in ps], [0] * ranks, "\n".join(x[-1500:] for x in errs))
        sts = [_stats(self.lib, a) for a in self.srv.osts.split(",")]
        self.assertTrue(all(st["tier_capacity"] == 1 << 30 for st in sts), sts[0])   # the device's own size
        self.assertGreater(sum(st["tier_used"] for st in sts), ranks * n * PIECE // 2)
        self.srv.kill()
        self.srv.start()
        out = subprocess.run([sys.executable, "-c", CHECK, self.prefix, str(ranks), str(n)], env=self.env(False),
                             capture_output=True, text=True, timeout=300)
        self.assertEqual(out.stdout.split(), [str(ranks * n * PIECE), "0"], out.stderr[-2000:])

    def test_synced_writes_on_raw_devices(self):
        self.write_and_check(False)

    def test_async_writes_on_raw_devices(self):
        self.write_and_check(True)


if __name__ == "__main__":
    unittest.main()
