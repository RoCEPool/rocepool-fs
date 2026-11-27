"""The AWS hosts' instance-store NVMe layout (tools/aws_cluster.py
setup_script): by default every device split -- a small partition per
device, RAID 0, XFS at /nvmedata for the servers' logs; the rest raw, one
RAID 0 per NUMA node, /dev/rocepool-tier-<j> for the OSTs' tiers, so a tier's
commit is a device flush, not a file system's journal. A failing step falls
back to the old layout (one XFS over all of them).

The rendered script runs here against fake devices (block nodes made with
mknod) and stand-ins for sudo, sfdisk, mdadm, mkfs.xfs and mount that record
what they were asked; the paths are moved under a temporary root."""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import aws_cluster  # noqa: E402

SHIMS = {
    "sudo": 'exec "$@"',
    "wipefs": 'echo "wipefs $*" >> "$LOG"',
    "sfdisk": ('cat >/dev/null; echo "sfdisk $*" >> "$LOG"; [ -n "$SFDISK_FAIL" ] && exit 1; '
               'd=${@: -1}; p=$d; case $d in *[0-9]) p=${d}p;; esac; mknod ${p}1 b 7 $((RANDOM % 200 + 20)); '
               'mknod ${p}2 b 7 $((RANDOM % 200 + 20))'),
    "udevadm": "exit 0",
    "mdadm": ('echo "mdadm $*" >> "$LOG"; [ -n "$MDADM_FAIL" ] && exit 1; '
              'for a in "$@"; do case $a in */md*) mknod $a b 9 $((RANDOM % 200 + 20)); break;; esac; done'),
    "mkfs.xfs": 'echo "mkfs.xfs $*" >> "$LOG"',
    "mount": 'echo "mount $*" >> "$LOG"',
    "umount": 'echo "umount $*" >> "$LOG"',
    "mountpoint": "exit 1",
    "findmnt": "exit 1",
    "ln": 'echo "ln $*" >> "$LOG"; exec /bin/ln "$@"',
}


@unittest.skipUnless(os.geteuid() == 0, "needs mknod (root)")
class NvmeLayoutTests(unittest.TestCase):
    def script(self, layout="raw"):
        old = os.environ.get("ROCEPOOL_NVME_LAYOUT")
        os.environ["ROCEPOOL_NVME_LAYOUT"] = layout
        try:
            r = aws_cluster.Run.__new__(aws_cluster.Run)
            r.state = {"mode": "efa", "facts": {"nvme": 7500}}
            sc = r.setup_script()
        finally:
            if old is None:
                os.environ.pop("ROCEPOOL_NVME_LAYOUT", None)
            else:
                os.environ["ROCEPOOL_NVME_LAYOUT"] = old
        a = sc.index("apt_install mdadm")
        b = sc.index("\n", sc.index("if [ $n -gt 0 ] && ! mountpoint -q /nvmedata"))
        return sc[a:b]

    def run_layout(self, ndev, numa, layout="raw", **env):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        bind = os.path.join(tmp, "bin")
        os.makedirs(bind)
        for name, body in SHIMS.items():
            with open(os.path.join(bind, name), "w") as f:
                f.write("#!/bin/bash\n" + body + "\n")
            os.chmod(os.path.join(bind, name), 0o755)
        os.makedirs(os.path.join(tmp, "dev"))
        for i in range(ndev + 1):   # nvme0n1: the EBS system disk, not instance storage
            dev = os.path.join(tmp, "dev", f"nvme{i}n1")
            subprocess.run(["mknod", dev, "b", "7", str(100 + i)], check=True)
            m = os.path.join(tmp, "sys", "block", f"nvme{i}n1", "device")
            os.makedirs(m)
            with open(os.path.join(m, "model"), "w") as f:
                f.write("Amazon Elastic Block Store\n" if i == 0 else "Amazon EC2 NVMe Instance Storage\n")
        for j in range(numa):
            os.makedirs(os.path.join(tmp, "sys", "devices", "system", "node", f"node{j}"))
        body = self.script(layout).replace("/dev/", tmp + "/dev/").replace("/sys/", tmp + "/sys/") \
            .replace("/nvmedata", tmp + "/nvmedata")
        log = os.path.join(tmp, "log")
        sh = "set -euo pipefail\napt_install() { :; }\n" + body + "\n"
        r = subprocess.run(["bash", "-c", sh], capture_output=True, text=True, cwd=tmp,
                           env=dict(os.environ, PATH=bind + ":" + os.environ["PATH"], LOG=log, **env), timeout=60)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        calls = open(log).read().replace(tmp, "") if os.path.exists(log) else ""
        tiers = sorted(os.listdir(os.path.join(tmp, "dev")))
        links = {t: os.readlink(os.path.join(tmp, "dev", t)).replace(tmp, "") for t in tiers
                 if t.startswith("rocepool-tier-")}
        return r.stdout.replace(tmp, ""), calls, links

    def test_i3en_four_devices_one_numa_node(self):
        out, calls, links = self.run_layout(4, 1)
        self.assertIn("4 device(s): logs on XFS at /nvmedata, 1 raw tier(s)", out)
        self.assertIn("mdadm --create /dev/md0 --run --level=0 --raid-devices=4 /dev/nvme1n1p1 /dev/nvme2n1p1 "
                      "/dev/nvme3n1p1 /dev/nvme4n1p1", calls)
        self.assertIn("mkfs.xfs -f -q /dev/md0", calls)
        self.assertIn("--raid-devices=4 /dev/nvme1n1p2 /dev/nvme2n1p2 /dev/nvme3n1p2 /dev/nvme4n1p2", calls)
        self.assertEqual(links, {"rocepool-tier-0": "/dev/md10"})
        self.assertNotIn("nvme0n1", calls)                      # never the system disk
        self.assertEqual(calls.count("sfdisk"), 4)

    def test_hpc6id_four_devices_two_numa_nodes(self):
        out, calls, links = self.run_layout(4, 2)
        self.assertEqual(links, {"rocepool-tier-0": "/dev/md10", "rocepool-tier-1": "/dev/md11"})
        self.assertIn("mdadm --create /dev/md10 --run --level=0 --raid-devices=2 /dev/nvme1n1p2 /dev/nvme2n1p2", calls)
        self.assertIn("mdadm --create /dev/md11 --run --level=0 --raid-devices=2 /dev/nvme3n1p2 /dev/nvme4n1p2", calls)

    def test_two_devices_two_numa_nodes_one_device_each(self):
        out, calls, links = self.run_layout(2, 2)
        self.assertEqual(links, {"rocepool-tier-0": "/dev/nvme1n1p2", "rocepool-tier-1": "/dev/nvme2n1p2"})
        self.assertIn("--raid-devices=2 /dev/nvme1n1p1 /dev/nvme2n1p1", calls)
        self.assertNotIn("/dev/md10", calls)

    def test_devices_not_a_multiple_of_numa_nodes_one_tier(self):
        out, calls, links = self.run_layout(3, 2)
        self.assertEqual(links, {"rocepool-tier-0": "/dev/md10"})

    def test_a_failed_step_falls_back_to_one_xfs(self):
        out, calls, links = self.run_layout(2, 1, SFDISK_FAIL="1")
        self.assertIn("raw NVMe layout failed: XFS instead", out)
        self.assertIn("--raid-devices=2 /dev/nvme1n1 /dev/nvme2n1", calls)
        self.assertIn("XFS at /nvmedata", out)
        self.assertEqual(links, {})

    def test_xfs_layout_when_asked(self):
        out, calls, links = self.run_layout(2, 1, layout="xfs")
        self.assertNotIn("sfdisk", calls)
        self.assertEqual(links, {})
        self.assertIn("local NVMe: 2 device(s), XFS at /nvmedata", out)


if __name__ == "__main__":
    unittest.main()
