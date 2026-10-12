"""tools/roce_preflight.py decisions against fake sysfs/configfs trees shaped
like a Broadcom bnxt_re host, and a fake `dcb` -- so every PASS/WARN/FAIL
branch is exercised without RDMA hardware."""
import os
import shutil
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tools.roce_preflight import FAIL, PASS, WARN, Preflight  # noqa: E402


def w(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text + "\n")


def fake_host(root, *, ip="192.168.50.1", state="4: ACTIVE", mtu=9000, gid_type="RoCE v2",
              roce_mode="RoCE v2", ecn=None, roce_dscp=None):
    ib = os.path.join(root, "sys", "class", "infiniband", "bnxt_re0")
    w(os.path.join(ib, "device", "vendor"), "0x14e4")
    w(os.path.join(ib, "fw_ver"), "227.0.131.0")
    port = os.path.join(ib, "ports", "1")
    w(os.path.join(port, "state"), state)
    w(os.path.join(port, "phys_state"), "5: LinkUp" if "ACTIVE" in state else "3: Disabled")
    w(os.path.join(port, "link_layer"), "Ethernet")
    w(os.path.join(port, "rate"), "100 Gb/sec (4X EDR)")
    a, b, c, d = (int(x) for x in ip.split("."))
    # index 0: link-local v1 GID, index 1: v1 of the IP, index 3: v2 of the IP
    w(os.path.join(port, "gids", "0"), "fe80:0000:0000:0000:1234:56ff:fe78:9abc")
    w(os.path.join(port, "gid_attrs", "types", "0"), "IB/RoCE v1")
    mapped = f"0000:0000:0000:0000:0000:ffff:{a:02x}{b:02x}:{c:02x}{d:02x}"
    w(os.path.join(port, "gids", "1"), mapped)
    w(os.path.join(port, "gid_attrs", "types", "1"), "IB/RoCE v1")
    w(os.path.join(port, "gids", "3"), mapped)
    w(os.path.join(port, "gid_attrs", "types", "3"), gid_type)
    for i in ("0", "1", "3"):
        w(os.path.join(port, "gid_attrs", "ndevs", i), "ens1f0np0")
    w(os.path.join(port, "gids", "2"), "0000:0000:0000:0000:0000:0000:0000:0000")
    w(os.path.join(root, "sys", "class", "net", "ens1f0np0", "mtu"), str(mtu))
    cfg = os.path.join(root, "config")
    if roce_mode is not None:
        w(os.path.join(cfg, "rdma_cm", "bnxt_re0", "ports", "1", "default_roce_mode"), roce_mode)
    if ecn is not None:
        w(os.path.join(cfg, "bnxt_re", "bnxt_re0", "ports", "1", "cc", "ecn_enable"), ecn)
    if roce_dscp is not None:
        w(os.path.join(cfg, "bnxt_re", "bnxt_re0", "ports", "1", "cc", "roce_dscp"), roce_dscp)
    return cfg


def dcb(pfc_line=None, dscp_line=None):
    def run(argv):
        if argv[:2] == ["dcb", "pfc"]:
            return (0, pfc_line) if pfc_line is not None else (127, "")
        if argv[:2] == ["dcb", "app"]:
            return (0, dscp_line) if dscp_line is not None else (127, "")
        return 127, ""
    return run


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def pf(self, cfg, **kw):
        kw.setdefault("runner", dcb("prio-pfc 0:off 1:off 2:off 3:on 4:off 5:off 6:off 7:off", "dscp-prio 26:3 48:6"))
        kw.setdefault("kernel_release", "6.8.0-45-generic")
        kw.setdefault("ncpu", 192)
        kw.setdefault("euid", 1000)
        kw.setdefault("memlock", (-1, -1))  # RLIM_INFINITY
        return Preflight(sysfs_root=self.root, configfs_root=cfg, frame_bytes=1 << 20, **kw).run_all()

    def status(self, rep, name):
        return next(c for c in rep["checks"] if c["check"] == name)["status"]

    def test_healthy_broadcom_host_with_pfc(self):
        rep = self.pf(fake_host(self.root), ip="192.168.50.1")
        self.assertNotEqual(rep["verdict"], FAIL, rep["checks"])
        for n in ("device", "gid", "port", "mtu", "roce_mode", "lossless", "tuning"):
            self.assertEqual(self.status(rep, n), PASS, (n, rep["checks"]))
        self.assertEqual(rep["device"], "bnxt_re0")
        self.assertEqual(rep["netdev"], "ens1f0np0")
        self.assertEqual(rep["env"]["ROCE_TOS"], str(26 << 2))
        self.assertEqual(rep["env"]["ROCE_POLL_SPIN_US"], "20")
        gid = next(c for c in rep["checks"] if c["check"] == "gid")
        self.assertEqual(gid["index"], 3)  # the RoCE v2 GID, not the v1 one with the same address
        self.assertTrue(next(c for c in rep["checks"] if c["check"] == "device")["devices"][0]["broadcom"])

    def test_ip_autodetected_from_roce_v2_gid(self):
        rep = self.pf(fake_host(self.root, ip="10.1.2.3"), ip=None)
        self.assertEqual(rep["ip"], "10.1.2.3")

    def test_wrong_ip_fails_and_lists_available(self):
        rep = self.pf(fake_host(self.root), ip="192.168.99.9")
        self.assertEqual(self.status(rep, "gid"), FAIL)
        self.assertIn("192.168.50.1", next(c for c in rep["checks"] if c["check"] == "gid")["detail"])
        self.assertEqual(rep["verdict"], FAIL)

    def test_only_roce_v1_gid_fails(self):
        rep = self.pf(fake_host(self.root, gid_type="IB/RoCE v1"), ip="192.168.50.1")
        self.assertEqual(self.status(rep, "gid"), FAIL)

    def test_port_down_fails(self):
        rep = self.pf(fake_host(self.root, state="1: DOWN"), ip="192.168.50.1")
        self.assertEqual(self.status(rep, "port"), FAIL)

    def test_no_devices_fails(self):
        rep = Preflight(ip="1.2.3.4", sysfs_root=self.root, configfs_root=self.root, runner=dcb(),
                        kernel_release="6.8", euid=1000).run_all()
        self.assertEqual(self.status(rep, "device"), FAIL)
        self.assertEqual(rep["verdict"], FAIL)

    def test_standard_mtu_warns_with_roce_mtu(self):
        rep = self.pf(fake_host(self.root, mtu=1500), ip="192.168.50.1")
        mtu = next(c for c in rep["checks"] if c["check"] == "mtu")
        self.assertEqual(mtu["status"], WARN)
        self.assertEqual(mtu["roce_mtu"], 1024)

    def test_roce_v1_default_mode_fails_without_fix(self):
        rep = self.pf(fake_host(self.root, roce_mode="IB/RoCE v1"), ip="192.168.50.1")
        self.assertEqual(self.status(rep, "roce_mode"), FAIL)

    def test_fix_sets_roce_v2_as_root(self):
        cfg = fake_host(self.root, roce_mode="IB/RoCE v1")
        rep = self.pf(cfg, ip="192.168.50.1", fix=True, euid=0)
        self.assertEqual(self.status(rep, "roce_mode"), PASS)
        self.assertTrue(any("RoCE v2" in f for f in rep["fixes_applied"]))

    def test_no_pfc_but_bnxt_ecn_warns_and_derives_tos(self):
        cfg = fake_host(self.root, ecn="1", roce_dscp="26")
        rep = self.pf(cfg, ip="192.168.50.1", runner=dcb("prio-pfc 0:off 1:off 2:off 3:off", "dscp-prio 26:3"))
        self.assertEqual(self.status(rep, "lossless"), WARN)
        self.assertEqual(rep["env"]["ROCE_TOS"], "104")
        self.assertEqual(rep["env"]["ROCE_ACK_TIMEOUT"], "14")

    def test_neither_pfc_nor_ecn_warns(self):
        cfg = fake_host(self.root, ecn="0")
        rep = self.pf(cfg, ip="192.168.50.1", runner=dcb("prio-pfc 0:off 3:off", ""))
        c = next(c for c in rep["checks"] if c["check"] == "lossless")
        self.assertEqual(c["status"], WARN)
        self.assertIn("neither PFC nor ECN", c["detail"])

    def test_pfc_without_dscp_mapping_warns(self):
        rep = self.pf(fake_host(self.root), ip="192.168.50.1", runner=dcb("prio-pfc 3:on", "dscp-prio 10:1"))
        self.assertEqual(self.status(rep, "lossless"), WARN)
        self.assertNotIn("ROCE_TOS", rep["env"])

    def test_old_kernel_exports_fork_safe(self):
        rep = self.pf(fake_host(self.root), ip="192.168.50.1", kernel_release="4.18.0-513.el8")
        self.assertEqual(self.status(rep, "kernel"), WARN)
        self.assertEqual(rep["env"]["RDMAV_FORK_SAFE"], "1")

    def test_low_memlock_fails_with_needed_size(self):
        rep = self.pf(fake_host(self.root), ip="192.168.50.1", memlock=(64 << 10, 64 << 10))
        c = next(c for c in rep["checks"] if c["check"] == "memlock")
        self.assertEqual(c["status"], FAIL)
        self.assertEqual(c["needed_bytes"], (32 + 128) << 20)

    def test_small_host_does_not_busy_poll(self):
        rep = self.pf(fake_host(self.root), ip="192.168.50.1", ncpu=4)
        self.assertNotIn("ROCE_POLL_SPIN_US", rep["env"])



class Mlx5AndPowerTests(unittest.TestCase):
    """the first real site (ConnectX-5, 32 CPUs): mlx5's DCQCN is not bnxt_re's
    configfs, and CPU wake-up latency decides the RPC latency"""

    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def pf(self, cfg, **kw):
        kw.setdefault("runner", dcb())
        return Preflight(sysfs_root=self.root, configfs_root=cfg, frame_bytes=1 << 20, ip="192.168.50.1",
                         kernel_release="6.8.0", ncpu=32, memlock=(-1, -1), **kw).run_all()

    def status(self, rep, name):
        return next(c for c in rep["checks"] if c["check"] == name)

    def test_mlx5_dcqcn_is_seen(self):
        cfg = fake_host(self.root)
        for side in ("roce_rp", "roce_np"):
            for prio in range(8):
                w(os.path.join(self.root, "sys", "class", "net", "ens1f0np0", "ecn", side, "enable", str(prio)),
                  "1" if prio == 3 else "0")
        c = self.status(self.pf(cfg, euid=1000), "lossless")
        self.assertIn("mlx5 DCQCN on priorities [3]", c["detail"])

    def test_power_warns_and_fix_sets_performance(self):
        cfg = fake_host(self.root)
        for cpu in ("cpu0", "cpu1"):
            base = os.path.join(self.root, "sys", "devices", "system", "cpu", cpu)
            w(os.path.join(base, "cpufreq", "scaling_governor"), "powersave")
            for st, name, lat in (("state0", "POLL", "0"), ("state1", "C1", "2"), ("state2", "C6", "133")):
                w(os.path.join(base, "cpuidle", st, "name"), name)
                w(os.path.join(base, "cpuidle", st, "latency"), lat)
                w(os.path.join(base, "cpuidle", st, "disable"), "0")
        c = self.status(self.pf(cfg, euid=1000), "power")
        self.assertEqual(c["status"], WARN)
        self.assertIn("powersave", c["detail"])
        self.assertIn("C6 (133 us)", c["detail"])
        rep = self.pf(cfg, euid=0, fix=True)
        self.assertEqual(self.status(rep, "power")["status"], PASS, rep["checks"])
        gov = open(os.path.join(self.root, "sys", "devices", "system", "cpu", "cpu1", "cpufreq",
                                "scaling_governor")).read().strip()
        self.assertEqual(gov, "performance")
        dis = open(os.path.join(self.root, "sys", "devices", "system", "cpu", "cpu0", "cpuidle", "state2",
                                "disable")).read().strip()
        self.assertEqual(dis, "1")
        self.assertEqual(open(os.path.join(self.root, "sys", "devices", "system", "cpu", "cpu0", "cpuidle",
                                           "state1", "disable")).read().strip(), "0")  # shallow C1 kept
        self.assertEqual(len(rep["fixes_applied"]), 2)


class AwsDeviceTests(unittest.TestCase):
    """Soft-RoCE (rxe) and AWS EFA hosts."""

    def setUp(self):
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def run_pf(self, runner, instance_type="", ip=None):
        pf = Preflight(ip=ip, sysfs_root=self.root, configfs_root=os.path.join(self.root, "config"),
                       frame_bytes=1 << 20, runner=runner, kernel_release="6.8.0-1015-aws", ncpu=64, euid=0,
                       memlock=(-1, -1))
        pf.instance_type = instance_type
        return pf.run_all()

    @staticmethod
    def status(rep, name):
        return next(c for c in rep["checks"] if c["check"] == name)["status"]

    def efa_host(self):
        ib = os.path.join(self.root, "sys", "class", "infiniband", "efa_0")
        w(os.path.join(ib, "device", "vendor"), "0x1d0f")
        w(os.path.join(ib, "ports", "1", "state"), "4: ACTIVE")

    def test_efa_host_selects_ofi_backend(self):
        self.efa_host()

        def run(argv):
            if argv[0] == "fi_info":
                return 0, "provider: efa\n    fabric: efa\n    domain: rdmap0s6-rdm\n"
            if argv[:2] == ["ip", "-4"]:
                return 0, "1.1.1.1 via 172.31.0.1 dev ens5 src 172.31.7.9 uid 0\n"
            return 127, ""
        rep = self.run_pf(run, instance_type="c6in.32xlarge")
        self.assertEqual(rep["kind"], "efa")
        self.assertEqual(rep["backend"], "ofi")
        self.assertEqual(rep["ip"], "172.31.7.9")
        self.assertEqual(rep["env"]["ROCE_BACKEND"], "ofi")
        self.assertEqual(rep["env"]["ROCE_OFI_PROVIDER"], "efa")
        for n in ("device", "ip", "libfabric", "rdma_read", "lossless"):
            self.assertEqual(self.status(rep, n), PASS, (n, rep["checks"]))
        self.assertFalse([c for c in rep["checks"] if c["check"] in ("gid", "roce_mode")])
        self.assertNotEqual(rep["verdict"], FAIL)

    def test_efa_without_rdma_read_or_provider(self):
        self.efa_host()
        rep = self.run_pf(lambda argv: (127, ""), instance_type="c5n.18xlarge", ip="10.0.0.5")
        self.assertEqual(self.status(rep, "rdma_read"), WARN)
        self.assertEqual(self.status(rep, "libfabric"), FAIL)
        self.assertEqual(rep["verdict"], FAIL)

    def test_soft_roce_host(self):
        ib = os.path.join(self.root, "sys", "class", "infiniband", "rxe0")
        w(os.path.join(ib, "parent"), "ens5")
        port = os.path.join(ib, "ports", "1")
        w(os.path.join(port, "state"), "4: ACTIVE")
        w(os.path.join(port, "phys_state"), "5: LinkUp")
        w(os.path.join(port, "link_layer"), "Ethernet")
        w(os.path.join(port, "rate"), "100 Gb/sec (4X EDR)")
        w(os.path.join(port, "gids", "1"), "0000:0000:0000:0000:0000:ffff:ac1f:0709")
        w(os.path.join(port, "gid_attrs", "types", "1"), "RoCE v2")
        w(os.path.join(port, "gid_attrs", "ndevs", "1"), "ens5")
        w(os.path.join(self.root, "sys", "class", "net", "ens5", "mtu"), "9001")
        w(os.path.join(self.root, "config", "rdma_cm", "rxe0", "ports", "1", "default_roce_mode"), "RoCE v2")
        rep = self.run_pf(lambda argv: (127, ""))
        self.assertEqual(rep["kind"], "rxe")
        self.assertEqual(rep["backend"], "verbs")
        self.assertEqual(rep["ip"], "172.31.7.9")
        for n in ("device", "gid", "port", "mtu", "roce_mode", "lossless"):
            self.assertEqual(self.status(rep, n), PASS, (n, rep["checks"]))
        self.assertNotIn("ROCE_TOS", rep["env"])

    def test_soft_roce_is_created_when_asked(self):
        w(os.path.join(self.root, "sys", "class", "net", "ens5", "mtu"), "1500")
        calls = []

        def run(argv):
            calls.append(argv)
            if argv[:3] == ["ip", "-4", "-o"] and argv[3] == "addr":
                return 0, "1: lo    inet 127.0.0.1/8 scope host lo\n2: ens5    inet 172.31.7.9/20 brd x\n"
            if argv[:3] == ["rdma", "link", "add"]:
                os.makedirs(os.path.join(self.root, "sys", "class", "infiniband", "rxe0"))
                return 0, ""
            if argv[:3] == ["ip", "link", "set"]:
                return 0, ""
            return 0, ""
        pf = Preflight(ip="172.31.7.9", sysfs_root=self.root, configfs_root=os.path.join(self.root, "config"),
                       fix=True, runner=run, kernel_release="6.8.0", ncpu=8, euid=0, memlock=(-1, -1), soft_roce=True)
        pf.instance_type = ""
        rep = pf.run_all()
        self.assertIn(["rdma", "link", "add", "rxe0", "type", "rxe", "netdev", "ens5"], calls)
        self.assertIn(["ip", "link", "set", "dev", "ens5", "mtu", "9001"], calls)
        self.assertEqual(self.status(rep, "soft_roce"), PASS)
        self.assertTrue(any("rxe0" in f for f in rep["fixes_applied"]))

    def test_no_soft_roce_without_the_flag(self):
        pf = Preflight(ip="172.31.7.9", sysfs_root=self.root, configfs_root=os.path.join(self.root, "config"),
                       fix=True, runner=lambda a: (0, ""), kernel_release="6.8.0", ncpu=8, euid=0, memlock=(-1, -1))
        rep = pf.run_all()
        self.assertEqual(self.status(rep, "device"), FAIL)


if __name__ == "__main__":
    unittest.main()
