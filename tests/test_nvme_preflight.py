"""tools/nvme_preflight.py on fake sysfs trees: finds NVMe controllers, their
CMB / p2pmem, the RDMA NIC's PCIe relationship to them and the NVMe-backed
mounts, and recommends the tier file and the P2P memory only when the
kernel, driver and topology allow it."""
import os
import shutil
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import nvme_preflight  # noqa: E402


def w(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


def fake_host(root, *, kernel="6.8.0", nic_driver="mlx5_core", cmb=True, published=True, same_port=True,
              mount_free_gib=800, root_free_gib=0, lvm=False):
    """root port 0000:00:01.0 -> switch 0000:01:00.0 -> NIC 0000:02:00.0 and
    NVMe 0000:02:01.0 (same_port); the NVMe under another root port
    0000:00:03.0 otherwise. /proc/mounts has nvme0n1 on /data."""
    dev = os.path.join(root, "sys", "devices", "pci0000:00")
    nic_real = os.path.join(dev, "0000:00:01.0", "0000:01:00.0", "0000:02:00.0")
    nvme_real = os.path.join(dev, "0000:00:01.0", "0000:01:00.0", "0000:02:01.0") if same_port \
        else os.path.join(dev, "0000:00:03.0", "0000:04:00.0")
    nvme_addr = os.path.basename(nvme_real)
    os.makedirs(nic_real); os.makedirs(nvme_real)
    pci = os.path.join(root, "sys", "bus", "pci", "devices")
    os.makedirs(pci)
    os.symlink(nic_real, os.path.join(pci, "0000:02:00.0"))
    os.symlink(nvme_real, os.path.join(pci, nvme_addr))
    drv = os.path.join(root, "sys", "bus", "pci", "drivers", nic_driver)
    os.makedirs(drv)
    os.symlink(drv, os.path.join(nic_real, "driver"))
    ib = os.path.join(root, "sys", "class", "infiniband", "mlx5_0")
    os.makedirs(ib)
    os.symlink(nic_real, os.path.join(ib, "device"))
    nv = os.path.join(root, "sys", "class", "nvme", "nvme0")
    os.makedirs(nv)
    os.symlink(nvme_real, os.path.join(nv, "device"))
    w(os.path.join(nv, "model"), "Fake NVMe 7450\n")
    if cmb:
        w(os.path.join(nv, "cmb"), "cmbloc : 0x00000002\ncmbsz  : 0x00001f04\n")
        p2p = os.path.join(nvme_real, "p2pmem")
        w(os.path.join(p2p, "size"), str(64 << 20))
        w(os.path.join(p2p, "available"), str(64 << 20))
        w(os.path.join(p2p, "published"), "1" if published else "0")
        w(os.path.join(p2p, "allocate"), "")
    w(os.path.join(nv, "nvme0n1", "size"), str((3200 << 30) // 512))
    w(os.path.join(root, "proc", "sys", "kernel", "osrelease"), kernel + "\n")
    if lvm:  # Ubuntu's default install: / on LVM on nvme0n1p3, no /data
        w(os.path.join(root, "proc", "mounts"), "/dev/mapper/ubuntu--vg-ubuntu--lv / ext4 rw,relatime 0 0\n"
                                                 "/dev/nvme0n1p2 /boot ext4 rw 0 0\n")
        os.makedirs(os.path.join(root, "dev", "mapper"))
        os.symlink("../dm-0", os.path.join(root, "dev", "mapper", "ubuntu--vg-ubuntu--lv"))
        w(os.path.join(root, "dev", "dm-0"), "")
        os.makedirs(os.path.join(root, "sys", "class", "block", "dm-0", "slaves", "nvme0n1p3"))
    else:
        w(os.path.join(root, "proc", "mounts"), "/dev/sda1 / ext4 rw 0 0\n/dev/nvme0n1 /data xfs rw 0 0\n"
                                                 "/dev/sdb1 /backup xfs ro 0 0\n")
    os.makedirs(os.path.join(root, "data"), exist_ok=True)
    # free space comes from statvfs of <root>/data, <root>/: patch it in the tests
    nvme_preflight._FAKE_FREE = {os.path.join(root, "data"): mount_free_gib << 30,
                                 os.path.join(root, ""): root_free_gib << 30}
    return root


class NvmePreflightTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._statvfs = os.statvfs

        class _St:
            def __init__(self, free):
                self.f_bavail, self.f_frsize = free, 1

        def statvfs(path):
            fake = getattr(nvme_preflight, "_FAKE_FREE", {})
            return _St(fake[path]) if path in fake else self._statvfs(path)
        nvme_preflight.os.statvfs = statvfs

    def tearDown(self):
        nvme_preflight.os.statvfs = self._statvfs
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_recommends_tier_file_and_p2pmem_when_everything_lines_up(self):
        root = fake_host(os.path.join(self.tmp, "h"))
        p = nvme_preflight.probe(root, osts_per_node=2)
        self.assertEqual(p["nvme"][0]["pci"], "0000:02:01.0")
        self.assertTrue(p["nvme"][0]["has_cmb"])
        self.assertEqual(p["nvme"][0]["namespaces"], [{"name": "nvme0n1", "size_bytes": 3200 << 30}])
        self.assertEqual(p["rdma_nics"][0]["driver"], "mlx5_core")
        pair = p["pairs"][0]
        self.assertEqual(pair["common_upstream"], "0000:01:00.0")
        self.assertTrue(pair["same_root_port"])
        self.assertEqual(pair["hops"], 2)
        rec = p["recommendation"]
        self.assertEqual(rec["tier"], "file:/data/rocepool_ost_{n}.tier:320")  # 800 GiB * 0.8 / 2 OSTs
        self.assertEqual(rec["p2pmem"], "/sys/bus/pci/devices/0000:02:01.0/p2pmem")
        self.assertEqual(rec["notes"], [])
        text = nvme_preflight.render(p)
        self.assertIn("ROCEPOOL_P2PMEM=/sys/bus/pci/devices/0000:02:01.0/p2pmem", text)

    def test_no_p2p_across_root_ports(self):
        root = fake_host(os.path.join(self.tmp, "h"), same_port=False)
        p = nvme_preflight.probe(root)
        self.assertFalse(p["pairs"][0]["same_root_port"])
        self.assertIsNone(p["recommendation"]["p2pmem"])
        self.assertTrue(p["recommendation"]["tier"])

    def test_no_p2p_with_an_old_kernel_or_other_driver(self):
        root = fake_host(os.path.join(self.tmp, "h1"), kernel="5.15.0")
        self.assertIsNone(nvme_preflight.probe(root)["recommendation"]["p2pmem"])
        self.assertIn("older than 6.2", nvme_preflight.probe(root)["recommendation"]["notes"][0])
        root = fake_host(os.path.join(self.tmp, "h2"), nic_driver="bnxt_en")
        rec = nvme_preflight.probe(root)["recommendation"]
        self.assertIsNone(rec["p2pmem"])
        self.assertIn("bnxt_en", rec["notes"][0])

    def test_cmb_without_published_p2pmem_is_noted(self):
        root = fake_host(os.path.join(self.tmp, "h"), published=False)
        rec = nvme_preflight.probe(root)["recommendation"]
        self.assertIsNone(rec["p2pmem"])
        self.assertTrue(rec["tier"])

    def test_no_tier_without_room(self):
        root = fake_host(os.path.join(self.tmp, "h"), mount_free_gib=3)
        rec = nvme_preflight.probe(root)["recommendation"]
        self.assertIsNone(rec["tier"])
        self.assertIn("no file system with room", rec["notes"][0])

    def test_falls_back_to_a_local_file_system(self):
        root = fake_host(os.path.join(self.tmp, "h"), mount_free_gib=3, root_free_gib=500)
        rec = nvme_preflight.probe(root, osts_per_node=2)
        self.assertEqual(rec["recommendation"]["tier"], "file:/rocepool_tier/rocepool_ost_{n}.tier:125")
        self.assertEqual(rec["recommendation"]["tier_dir"], "/rocepool_tier")
        self.assertIn("not NVMe", rec["recommendation"]["notes"][0])

    def test_lvm_on_nvme_counts_as_nvme(self):
        root = fake_host(os.path.join(self.tmp, "h"), lvm=True, root_free_gib=700)
        p = nvme_preflight.probe(root, osts_per_node=2)
        self.assertEqual([(m["mountpoint"], m["namespace"]) for m in p["nvme_mounts"]], [("/", "nvme0n1")])
        self.assertEqual(p["recommendation"]["tier"], "file:/rocepool_tier/rocepool_ost_{n}.tier:175")
        self.assertFalse(any("not NVMe" in n for n in p["recommendation"]["notes"]))

    def test_measure_write(self):
        r = nvme_preflight.measure_write(os.path.join(self.tmp, "tier"), mib=8)
        # a number, or why not (tmpfs refuses O_DIRECT); never a leftover file
        self.assertTrue("mib_s" in r or "error" in r, r)
        self.assertEqual(os.listdir(os.path.join(self.tmp, "tier")), [])

    def test_this_host(self):
        p = nvme_preflight.probe("/")
        self.assertIn("recommendation", p)  # no NVMe here: nothing recommended, nothing crashes


if __name__ == "__main__":
    unittest.main()
