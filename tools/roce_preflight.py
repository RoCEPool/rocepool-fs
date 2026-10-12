"""On-site RoCE preflight: every check that decides whether this host can run
rocepool over real RDMA, as automatic PASS / WARN / FAIL decisions -- no
manual steps, no reading ibv_devinfo output by eye.

    python3 tools/roce_preflight.py --ip 192.168.50.1          # check
    sudo python3 tools/roce_preflight.py --ip 192.168.50.1 --fix  # + safe host-local fixes

Written for Broadcom NetXtreme-E (bnxt_re) RoCE NICs, and works for any
rdma-core device. What it decides:

  kernel       >= 5.13: fork() is safe with registered memory (Python
               spawns subprocesses); older -> RDMAV_FORK_SAFE=1 is exported
  memlock      RLIMIT_MEMLOCK covers what the transport pins (SRQ slabs +
               buffer pool; see roce/native/roce_transport.c); --fix raises
               it for this process tree (root) and prints the systemd line
  device       an RDMA device exists; Broadcom identified by PCI vendor 0x14e4
  gid          a RoCE v2 GID carries --ip (else FAIL, listing the IPs that do)
  port         that port is ACTIVE / LinkUp on an Ethernet link layer
  mtu          netdev MTU allows the 4096-byte RoCE MTU (jumbo frames) or WARN
  roce_mode    rdma_cm default_roce_mode is RoCE v2 (--fix sets it, root)
  lossless     PFC priority + DSCP mapping (iproute2 `dcb`), or bnxt_re ECN
               congestion control; derives ROCE_TOS so RoCE traffic lands in
               the lossless / congestion-controlled class
  tuning       ROCE_POLL_SPIN_US for hosts with cores to spare, ACK timeout

Three kinds of device, told apart automatically:
  roce   a RoCE NIC (bnxt_re, mlx5, ...): every check above
  rxe    Soft-RoCE (`rdma link add rxe0 type rxe netdev eth0`): RoCE v2 in
         the kernel over any Ethernet NIC -- e.g. an AWS instance. Same
         checks, except lossless: rxe retransmits in software, there is no
         PFC to configure (and none on AWS)
  efa    AWS Elastic Fabric Adapter: no RoCE, no GIDs, no rdma_cm. The
         verbs transport cannot run on it; the ofi backend (libfabric,
         roce/native/ofi_transport.c) does. Checks instead: the libfabric
         efa provider answers (fi_info), RDMA read support of the instance
         type (Nitro v4+; else the provider emulates reads with target CPU)
         and exports ROCE_BACKEND=ofi ROCE_OFI_PROVIDER=efa

Outputs: a JSON report (--json) and an env file (--env-out, default
./roce_env.sh) with the ROCE_* settings every rocepool process should run
with; tools/roce_onsite.py and run_crossover_rdma.py --real source it.

--sysfs-root / --configfs-root / a fake command runner exist so the
decision logic is unit-tested without hardware (tests/test_roce_preflight.py).
"""
from __future__ import annotations

import argparse
import ipaddress
import json
import os
import platform
import re
import resource
import shutil
import subprocess
import sys

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
BROADCOM_VENDOR = "0x14e4"
# instance families whose EFA does hardware RDMA read (Nitro v4 and later;
# AWS EFA documentation). Others work: libfabric emulates reads.
EFA_RDMA_READ_FAMILIES = ("c6in", "m6in", "m6idn", "r6in", "r6idn", "c6gn", "c7gn", "c7i", "c7a", "c7g", "m7i", "m7a",
                          "m7g", "r7i", "r7a", "r7g", "r7iz", "hpc6id", "hpc7a", "hpc7g", "i4i", "i7ie", "i8g", "i8ge",
                          "p4d", "p4de", "p5", "p5e", "p5en", "p6", "trn1", "trn1n", "trn2", "g6", "g6e", "c8g", "m8g",
                          "r8g", "c8gn", "m8i", "c8i", "r8i", "m8a", "c8a", "r8a", "x8g")
ROCE_MTUS = (4096, 2048, 1024, 512, 256)
# RoCEv2 headers on top of the RoCE MTU payload: Eth 14 + IP 20 + UDP 8 + BTH 12 + ICRC 4 (+ VLAN 4)
ROCE_OVERHEAD = 62


def _read(path: str) -> str | None:
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


def default_runner(argv: list[str]) -> tuple[int, str]:
    if not shutil.which(argv[0]):
        return 127, ""
    try:
        r = subprocess.run(argv, capture_output=True, text=True, timeout=20)
        return r.returncode, r.stdout + r.stderr
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, str(exc)


class Preflight:
    def __init__(self, *, ip: str | None, sysfs_root: str = "/", configfs_root: str = "/sys/kernel/config",
                 fix: bool = False, runner=default_runner, frame_bytes: int = 16 << 20,
                 srq_depth: int = 32, servers_per_process: int = 1, pool_max: int = 512,
                 kernel_release: str | None = None, ncpu: int | None = None, euid: int | None = None,
                 memlock: tuple[int, int] | None = None, soft_roce: bool = False):
        self.ip = ip
        self.sys = sysfs_root
        self.cfg = configfs_root
        self.fix = fix
        self.run = runner
        self.frame = frame_bytes
        self.srq_depth = srq_depth
        self.servers = servers_per_process
        self.pool_max = pool_max
        self.kernel = kernel_release or platform.release()
        self.ncpu = ncpu or os.cpu_count() or 1
        self.euid = os.geteuid() if euid is None else euid
        self.soft_roce = soft_roce  # --soft-roce: with --fix, create rxe0 when there is no RDMA device
        self._memlock = memlock  # injected (soft, hard) for tests; None = this process's real limit
        self.checks: list[dict] = []
        self.env: dict[str, str] = {}
        self.fixes: list[str] = []
        self.dev = self.port = self.netdev = None
        self.kind = None  # roce | rxe | efa
        self.instance_type = None  # injected for tests; None = ask the EC2 metadata service

    # -- helpers ------------------------------------------------------
    def add(self, name: str, status: str, detail: str, **data) -> None:
        self.checks.append({"check": name, "status": status, "detail": detail, **data})

    def p(self, *parts: str) -> str:
        return os.path.join(self.sys, "sys", *parts)

    # -- checks -------------------------------------------------------
    def check_kernel(self) -> None:
        m = re.match(r"(\d+)\.(\d+)", self.kernel)
        ver = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
        if ver >= (5, 13):
            self.add("kernel", PASS, f"{self.kernel}: fork() is safe with registered memory")
        else:
            self.env["RDMAV_FORK_SAFE"] = "1"
            self.add("kernel", WARN, f"{self.kernel} < 5.13: exporting RDMAV_FORK_SAFE=1 (fork with pinned memory)")

    def memlock_needed(self) -> int:
        srq = self.srq_depth * self.frame * self.servers
        # the pool grows to peak concurrency; budget for 128 frames of it
        return srq + min(self.pool_max, 128) * self.frame

    def check_memlock(self) -> None:
        need = self.memlock_needed()
        soft, hard = self._memlock or resource.getrlimit(resource.RLIMIT_MEMLOCK)
        inf = resource.RLIM_INFINITY
        ok = soft == inf or soft >= need
        if not ok and self.fix and self._memlock is None and (self.euid == 0 or hard == inf or hard >= need):
            try:
                resource.setrlimit(resource.RLIMIT_MEMLOCK, (inf if hard == inf or self.euid == 0 else hard,
                                                             inf if self.euid == 0 else hard))
                soft, hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)
                ok = soft == inf or soft >= need
                if ok:
                    self.fixes.append("raised RLIMIT_MEMLOCK for this process tree")
            except (ValueError, OSError):
                pass
        human = "unlimited" if soft == inf else f"{soft >> 20} MiB"
        if ok:
            self.add("memlock", PASS, f"locked-memory limit {human} >= {need >> 20} MiB needed", needed_bytes=need)
        else:
            self.add("memlock", FAIL,
                     f"locked-memory limit {human} < {need >> 20} MiB the transport registers (pins); "
                     "run as root with --fix, or set `ulimit -l unlimited` / systemd LimitMEMLOCK=infinity",
                     needed_bytes=need)

    def _gids(self, dev: str, port: str):
        base = self.p("class", "infiniband", dev, "ports", port)
        gdir = os.path.join(base, "gids")
        try:
            idxs = sorted(os.listdir(gdir), key=int)
        except OSError:
            return
        for i in idxs:
            gid = _read(os.path.join(gdir, i))
            typ = _read(os.path.join(base, "gid_attrs", "types", i))
            nd = _read(os.path.join(base, "gid_attrs", "ndevs", i))
            if not gid or not typ or gid.replace(":", "").strip("0") == "":
                continue
            try:
                v4 = ipaddress.IPv6Address(gid).ipv4_mapped
            except ValueError:
                v4 = None
            yield int(i), gid, typ, nd, (str(v4) if v4 else None)

    def make_soft_roce(self, ibdir: str) -> list[str]:
        """Soft-RoCE: rxe0 over the netdev that carries --ip (else the
        default route's), MTU raised to jumbo when the NIC allows (AWS: 9001)."""
        netdev = None
        if self.ip:
            rc, out = self.run(["ip", "-4", "-o", "addr", "show"])
            m = re.search(r"^\d+:\s+(\S+)\s+inet " + re.escape(self.ip) + r"/", out, re.M) if rc == 0 else None
            netdev = m.group(1) if m else None
        if not netdev:
            rc, out = self.run(["ip", "-4", "-o", "route", "get", "1.1.1.1"])
            m = re.search(r"\bdev (\S+)", out) if rc == 0 else None
            netdev = m.group(1) if m else None
        if not netdev:
            self.add("soft_roce", FAIL, "--soft-roce: cannot tell which network interface to put rxe on")
            return []
        self.run(["modprobe", "rdma_rxe"])
        rc, out = self.run(["rdma", "link", "add", "rxe0", "type", "rxe", "netdev", netdev])
        mtu = _read(self.p("class", "net", netdev, "mtu"))
        if mtu and int(mtu) < 4096 + ROCE_OVERHEAD:
            if self.run(["ip", "link", "set", "dev", netdev, "mtu", "9001"])[0] == 0:
                self.fixes.append(f"raised {netdev} MTU to 9001 for RoCE MTU 4096")
        try:
            devs = sorted(os.listdir(ibdir))
        except OSError:
            devs = []
        if devs:
            self.fixes.append(f"created Soft-RoCE device rxe0 on {netdev} (rdma link add)")
            self.add("soft_roce", PASS, f"rxe0 on {netdev}")
        else:
            self.add("soft_roce", FAIL, f"could not create rxe0 on {netdev}: {out.strip()[-300:]} (kernel without "
                     "rdma_rxe? Ubuntu: apt install linux-modules-extra-$(uname -r))")
        return devs

    def check_devices_and_gid(self) -> None:
        ibdir = self.p("class", "infiniband")
        try:
            devs = sorted(os.listdir(ibdir))
        except OSError:
            devs = []
        if not devs and self.soft_roce and self.fix and self.euid == 0:
            devs = self.make_soft_roce(ibdir)
        if not devs:
            self.add("device", FAIL, "no RDMA device (/sys/class/infiniband is empty): NIC RoCE firmware "
                     "feature or the bnxt_re driver is not loaded (modprobe bnxt_re)")
            return
        info = []
        for d in devs:
            vendor = _read(os.path.join(ibdir, d, "device", "vendor"))
            drv = os.path.basename(os.path.realpath(os.path.join(ibdir, d, "device", "driver"))) \
                if os.path.exists(os.path.join(ibdir, d, "device", "driver")) else None
            info.append({"name": d, "vendor": vendor, "pci_driver": drv, "fw_ver": _read(os.path.join(ibdir, d, "fw_ver")),
                         "broadcom": vendor == BROADCOM_VENDOR or d.startswith("bnxt_re")})
        for i in info:
            d = i["name"]
            soft = _read(os.path.join(ibdir, d, "parent")) is not None and i["pci_driver"] is None
            i["kind"] = "efa" if d.startswith("efa") or i["pci_driver"] == "efa" else (
                "rxe" if d.startswith("rxe") or (soft and not d.startswith("siw")) else "roce")
        efa = [i for i in info if i["kind"] == "efa"]
        if efa and not [i for i in info if i["kind"] != "efa"]:
            self.kind, self.dev = "efa", efa[0]["name"]
            self.add("device", PASS, f"{len(efa)} AWS EFA device(s): {', '.join(i['name'] for i in efa)} -- "
                     "the ofi (libfabric) transport backend", devices=info)
            return
        bc = [i for i in info if i["broadcom"]]
        self.add("device", PASS, f"{len(devs)} RDMA device(s): " + ", ".join(
            f"{i['name']} ({'Broadcom' if i['broadcom'] else i['vendor']}, fw {i['fw_ver']})" for i in info)
            + ("" if bc else " -- none is Broadcom; checks still apply"), devices=info)

        candidates = []
        for d in devs:
            try:
                ports = sorted(os.listdir(os.path.join(ibdir, d, "ports")), key=int)
            except OSError:
                ports = []
            for port in ports:
                for idx, gid, typ, nd, v4 in self._gids(d, port):
                    candidates.append({"dev": d, "port": port, "index": idx, "gid": gid, "type": typ,
                                       "netdev": nd, "ipv4": v4})
        v2 = [c for c in candidates if c["type"] == "RoCE v2" and c["ipv4"]]
        if self.ip:
            match = [c for c in v2 if c["ipv4"] == self.ip]
            if not match:
                have = sorted({c["ipv4"] for c in v2})
                self.add("gid", FAIL, f"no RoCE v2 GID carries {self.ip}; RoCE v2 IPv4 GIDs present: "
                         f"{have or 'none'} (is the IP configured on the RoCE port's netdev?)")
                return
            c = match[0]
        else:
            if not v2:
                self.add("gid", FAIL, "no RoCE v2 GID with an IPv4 address on any port")
                return
            c = v2[0]
            self.ip = c["ipv4"]
        self.dev, self.port, self.netdev = c["dev"], c["port"], c["netdev"]
        self.kind = next((i["kind"] for i in info if i["name"] == c["dev"]), "roce")
        if self.kind == "efa":
            self.kind = "roce"
        self.add("gid", PASS, f"{self.ip} is GID index {c['index']} (RoCE v2) on {c['dev']} port {c['port']} "
                 f"via {c['netdev']}", **c)

    def check_port(self) -> None:
        base = self.p("class", "infiniband", self.dev, "ports", self.port)
        state, phys = _read(os.path.join(base, "state")) or "", _read(os.path.join(base, "phys_state")) or ""
        ll, rate = _read(os.path.join(base, "link_layer")), _read(os.path.join(base, "rate"))
        if "ACTIVE" not in state or "LinkUp" not in phys:
            self.add("port", FAIL, f"{self.dev} port {self.port}: state '{state}', phys '{phys}' (cable/switch "
                     "port down or link not negotiated)")
        elif ll != "Ethernet":
            self.add("port", FAIL, f"{self.dev} port {self.port} link layer is {ll}, not Ethernet (not RoCE)")
        else:
            self.add("port", PASS, f"{self.dev} port {self.port} ACTIVE/LinkUp, {rate}")

    def check_mtu(self) -> None:
        mtu = _read(self.p("class", "net", self.netdev or "", "mtu")) if self.netdev else None
        if not mtu:
            self.add("mtu", WARN, f"could not read the MTU of {self.netdev}")
            return
        mtu = int(mtu)
        roce = next((m for m in ROCE_MTUS if m + ROCE_OVERHEAD <= mtu), 256)
        if roce == 4096:
            self.add("mtu", PASS, f"{self.netdev} MTU {mtu}: RoCE MTU 4096", roce_mtu=4096)
        else:
            self.add("mtu", WARN, f"{self.netdev} MTU {mtu} limits RoCE MTU to {roce}; jumbo frames (MTU 9000 on "
                     "the NIC AND the switch ports) give ~10-20% more large-transfer bandwidth", roce_mtu=roce)

    def check_roce_mode(self) -> None:
        base = os.path.join(self.cfg, "rdma_cm")
        if not os.path.isdir(base):
            self.add("roce_mode", WARN, "rdma_cm configfs not available (mount configfs / modprobe rdma_cm) -- "
                     "cannot confirm connections use RoCE v2")
            return
        devdir = os.path.join(base, self.dev)
        if not os.path.isdir(devdir) and self.fix and self.euid == 0:
            try:
                os.mkdir(devdir)
            except OSError:
                pass
        f = os.path.join(devdir, "ports", self.port, "default_roce_mode")
        cur = _read(f)
        if cur is None:
            self.add("roce_mode", WARN, f"{f} not readable (needs root to create {devdir})")
            return
        if cur != "RoCE v2" and self.fix and self.euid == 0:
            try:
                with open(f, "w") as fh:
                    fh.write("RoCE v2")
                self.fixes.append(f"set {f} = RoCE v2 (was {cur})")
                cur = _read(f)
            except OSError:
                pass
        if cur == "RoCE v2":
            self.add("roce_mode", PASS, "rdma_cm default_roce_mode = RoCE v2")
        else:
            self.add("roce_mode", FAIL, f"rdma_cm default_roce_mode = {cur!r}; RoCE v1 is not routable -- rerun with "
                     "--fix as root")

    def _dcb(self) -> tuple[list[int] | None, dict[int, int]]:
        rc, out = self.run(["dcb", "pfc", "show", "dev", self.netdev, "prio-pfc"])
        pfc = None
        if rc == 0:
            pfc = [int(p) for p, v in re.findall(r"(\d):(on|off)", out) if v == "on"]
        rc, out = self.run(["dcb", "app", "show", "dev", self.netdev, "dscp-prio"])
        dscp = {}
        if rc == 0:
            for d, pr in re.findall(r"(\d+):(\d)", out):
                dscp[int(d)] = int(pr)
        return pfc, dscp

    def _mlx5_ecn(self) -> list[int]:
        """priorities with DCQCN on (both reaction and notification point), mlx5 sysfs"""
        base = self.p("class", "net", self.netdev or "", "ecn")
        on = []
        for prio in range(8):
            rp = _read(os.path.join(base, "roce_rp", "enable", str(prio)))
            np_ = _read(os.path.join(base, "roce_np", "enable", str(prio)))
            if rp == "1" and np_ == "1":
                on.append(prio)
        return on

    def check_lossless(self) -> None:
        if self.kind == "rxe":
            self.env["ROCE_ACK_TIMEOUT"] = "14"
            self.add("lossless", PASS, f"{self.dev} is Soft-RoCE (rxe): RoCE v2 in the kernel over {self.netdev}; "
                     "loss is recovered by rxe's software retransmit, no PFC/DSCP to configure. "
                     "Exported ROCE_ACK_TIMEOUT=14")
            return
        pfc, dscp = self._dcb()
        cc = os.path.join(self.cfg, "bnxt_re", self.dev or "", "ports", self.port or "1", "cc")
        ecn = _read(os.path.join(cc, "ecn_enable"))
        roce_dscp = _read(os.path.join(cc, "roce_dscp"))
        mlx_ecn = self._mlx5_ecn()
        if ecn is None and mlx_ecn:
            ecn = "1"  # mlx5 DCQCN (reaction + notification point) on for these priorities
        if pfc:
            prios = sorted(pfc)
            cands = sorted(d for d, pr in dscp.items() if pr in prios)
            pick = 26 if 26 in cands else (int(roce_dscp, 0) if roce_dscp and int(roce_dscp, 0) in cands else
                                           (cands[0] if cands else None))
            if pick is None:
                self.add("lossless", WARN, f"PFC on priority {prios} but no DSCP maps to it (dcb app dscp-prio): "
                         "RoCE traffic would not use the lossless class")
                return
            self.env["ROCE_TOS"] = str(pick << 2)
            self.add("lossless", PASS, f"PFC on priority {dscp[pick]}; DSCP {pick} maps to it -> ROCE_TOS={pick << 2}"
                     + ("; ECN congestion control also on" if ecn == "1" else ""))
        elif ecn == "1":
            if roce_dscp:
                self.env["ROCE_TOS"] = str(int(roce_dscp, 0) << 2)
            self.env["ROCE_ACK_TIMEOUT"] = "14"
            self.add("lossless", WARN, f"no PFC; {'mlx5 DCQCN on priorities ' + str(mlx_ecn) if mlx_ecn else 'bnxt_re'} "
                     "ECN congestion control enabled (lossy RoCE with DCQCN-style "
                     "CC): fine at moderate load, expect retransmits under incast. Exported ROCE_ACK_TIMEOUT=14"
                     + (f", ROCE_TOS={int(roce_dscp, 0) << 2}" if roce_dscp else ""))
        elif pfc is None and ecn is None:
            self.add("lossless", WARN, "cannot read PFC (iproute2 `dcb` missing or unsupported) nor bnxt_re CC "
                     "configfs -- lossless configuration unverified")
        else:
            self.env["ROCE_ACK_TIMEOUT"] = "14"
            self.add("lossless", WARN, "neither PFC nor ECN congestion control is enabled: RoCE runs lossy with "
                     "go-back-N recovery -- throughput collapses under congestion. Enable PFC on the RoCE "
                     "priority (NIC + switch) or the NIC's ECN/DCQCN (bnxt_re cc configfs; mlx5 "
                     f"/sys/class/net/{self.netdev}/ecn/roce_rp|roce_np/enable/<prio>)")

    def _instance_type(self) -> str | None:
        if self.instance_type is not None:
            return self.instance_type or None
        rc, tok = self.run(["curl", "-s", "-m", "2", "-X", "PUT", "http://169.254.169.254/latest/api/token",
                            "-H", "X-aws-ec2-metadata-token-ttl-seconds: 60"])
        hdr = ["-H", f"X-aws-ec2-metadata-token: {tok.strip()}"] if rc == 0 and tok.strip() else []
        rc, out = self.run(["curl", "-s", "-m", "2", *hdr, "http://169.254.169.254/latest/meta-data/instance-type"])
        out = out.strip()
        return out if rc == 0 and re.fullmatch(r"[a-z0-9-]+\.[a-z0-9-]+", out) else None

    def _default_ip(self) -> str | None:
        rc, out = self.run(["ip", "-4", "-o", "route", "get", "1.1.1.1"])
        m = re.search(r"\bsrc (\d+\.\d+\.\d+\.\d+)", out) if rc == 0 else None
        return m.group(1) if m else None

    def check_efa(self) -> None:
        """EFA: the ofi backend. The IP is only the bootstrap socket's (any
        interface that reaches the peers): --ip, else the default route's."""
        self.env.update(ROCE_BACKEND="ofi", ROCE_OFI_PROVIDER="efa")
        if not self.ip:
            self.ip = self._default_ip()
        if not self.ip:
            self.add("ip", FAIL, "no --ip and no default route to take the bootstrap address from")
        else:
            self.add("ip", PASS, f"{self.ip}: the bootstrap/control address (data goes over {self.dev})")
        rc, out = self.run(["fi_info", "-p", "efa", "-t", "FI_EP_RDM"])
        if rc == 127:
            for d in ("/opt/amazon/efa/bin", "/usr/local/bin"):
                if os.path.exists(os.path.join(self.sys, d.lstrip("/"), "fi_info")):
                    rc, out = self.run([os.path.join(d, "fi_info"), "-p", "efa", "-t", "FI_EP_RDM"])
                    break
        if rc == 0 and "provider: efa" in out:
            self.add("libfabric", PASS, "libfabric's efa provider is up (fi_info -p efa -t FI_EP_RDM)")
        elif rc == 127:
            self.add("libfabric", FAIL, "fi_info not found: install the AWS EFA software (aws-efa-installer; "
                     "tools/aws_cluster.py does it) -- it puts libfabric in /opt/amazon/efa")
        else:
            self.add("libfabric", FAIL, f"libfabric has no usable efa provider: {out.strip()[-300:]} (EFA "
                     "installer missing, or the security group does not allow all traffic to itself)")
        it = self._instance_type()
        fam = it.split(".")[0] if it else None
        if fam and fam in EFA_RDMA_READ_FAMILIES:
            self.add("rdma_read", PASS, f"{it}: EFA does RDMA read in hardware -- one-sided reads cost the server "
                     "no CPU")
        elif fam:
            self.add("rdma_read", WARN, f"{it}: not known to do EFA RDMA read in hardware (Nitro v4+ does); "
                     "libfabric emulates reads with the target's CPU -- correct, slower one-sided reads; the "
                     "client probe in tools/io500_run.py decides between one-sided and RPC reads")
        else:
            self.add("rdma_read", WARN, "instance type unknown (no EC2 metadata): RDMA read support unverified")
        self.add("lossless", PASS, "EFA's SRD transport is reliable over AWS's lossy fabric: no PFC/DSCP to configure")

    def check_power(self) -> None:
        """Sleep/wake-up latency: a completion that arrives while the thread
        sleeps pays the CPU's wake-up (deep C-states: 50-200 us, more than the
        RDMA round trip itself). Governor `performance` and no deep idle
        states; --fix (root) sets both for this boot."""
        cpus = sorted(d for d in (os.listdir(self.p("devices", "system", "cpu"))
                                  if os.path.isdir(self.p("devices", "system", "cpu")) else [])
                      if re.fullmatch(r"cpu\d+", d))
        govs = {_read(self.p("devices", "system", "cpu", c, "cpufreq", "scaling_governor")) for c in cpus} - {None}
        deep = []  # (cpu, state, name, latency_us) enabled with exit latency > 20 us
        for c in cpus:
            idle = self.p("devices", "system", "cpu", c, "cpuidle")
            for st in sorted(os.listdir(idle)) if os.path.isdir(idle) else []:
                lat = _read(os.path.join(idle, st, "latency"))
                if lat and lat.isdigit() and int(lat) > 20 and _read(os.path.join(idle, st, "disable")) == "0":
                    deep.append((c, st, _read(os.path.join(idle, st, "name")), int(lat)))
        if self.fix and self.euid == 0:
            changed = 0
            if govs and govs != {"performance"}:
                for c in cpus:
                    f = self.p("devices", "system", "cpu", c, "cpufreq", "scaling_governor")
                    try:
                        with open(f, "w") as fh:
                            fh.write("performance")
                        changed += 1
                    except OSError:
                        pass
                if changed:
                    self.fixes.append(f"CPU frequency governor -> performance on {changed} CPUs (until reboot)")
            n = 0
            for c, st, _, _ in deep:
                try:
                    with open(self.p("devices", "system", "cpu", c, "cpuidle", st, "disable"), "w") as fh:
                        fh.write("1")
                    n += 1
                except OSError:
                    pass
            if n:
                self.fixes.append(f"disabled {n} deep idle states (exit latency > 20 us) (until reboot)")
            govs = {_read(self.p("devices", "system", "cpu", c, "cpufreq", "scaling_governor")) for c in cpus} - {None}
            deep = [d for d in deep if _read(self.p("devices", "system", "cpu", d[0], "cpuidle", d[1], "disable")) == "0"]
        names = sorted({f"{d[2]} ({d[3]} us)" for d in deep})
        issues = []
        if govs and govs != {"performance"}:
            issues.append(f"CPU governor {'/'.join(sorted(govs))} (not performance)")
        if deep:
            issues.append(f"deep idle states enabled: {', '.join(names)}")
        if issues:
            self.add("power", WARN, "; ".join(issues) + " -- every sleeping thread woken by a completion pays it; "
                     "rerun with --fix as root (or set them in the BIOS: performance profile, C-states off)")
        else:
            self.add("power", PASS, f"governor {'/'.join(sorted(govs)) or 'n/a'}, no deep idle states enabled")

    def check_tuning(self) -> None:
        if self.ncpu >= 16:
            self.env["ROCE_POLL_SPIN_US"] = "20"
            self.add("tuning", PASS, f"{self.ncpu} CPUs: completion busy-poll 20 us before sleeping "
                     "(ROCE_POLL_SPIN_US=20; tools/roce_onsite.py then measures the best value on this NIC)")
        else:
            self.add("tuning", PASS, f"{self.ncpu} CPUs: no busy-poll (event-driven completions only)")

    def run_all(self) -> dict:
        self.check_kernel()
        self.check_memlock()
        self.check_power()
        self.check_devices_and_gid()
        if self.kind == "efa":
            self.check_efa()
        elif self.dev:
            self.check_port()
            self.check_mtu()
            self.check_roce_mode()
            self.check_lossless()
        self.check_tuning()
        worst = FAIL if any(c["status"] == FAIL for c in self.checks) else (
            WARN if any(c["status"] == WARN for c in self.checks) else PASS)
        return {"verdict": worst, "ip": self.ip, "device": self.dev, "kind": self.kind,
                "backend": "ofi" if self.kind == "efa" else "verbs", "port": self.port, "netdev": self.netdev,
                "checks": self.checks, "env": self.env, "fixes_applied": self.fixes}


def write_env(path: str, env: dict) -> None:
    with open(path, "w") as f:
        f.write("# written by tools/roce_preflight.py -- sourced by rocepool on-site runs\n")
        for k, v in sorted(env.items()):
            f.write(f"export {k}={v}\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--ip", help="this host's RoCE IP (default: the first RoCE v2 IPv4 GID)")
    ap.add_argument("--fix", action="store_true", help="apply safe host-local fixes (root)")
    ap.add_argument("--soft-roce", action="store_true",
                    help="with --fix: no RDMA device -> create Soft-RoCE rxe0 (e.g. on AWS without EFA)")
    ap.add_argument("--json", help="write the report here ('-' = stdout)")
    ap.add_argument("--env-out", default="roce_env.sh")
    ap.add_argument("--frame-bytes", type=int, default=int(os.environ.get("ROCE_MAX_FRAME_BYTES", 16 << 20)))
    ap.add_argument("--sysfs-root", default="/")
    ap.add_argument("--configfs-root", default="/sys/kernel/config")
    args = ap.parse_args()
    rep = Preflight(ip=args.ip, sysfs_root=args.sysfs_root, configfs_root=args.configfs_root, fix=args.fix,
                    frame_bytes=args.frame_bytes, soft_roce=args.soft_roce).run_all()
    for c in rep["checks"]:
        print(f"[{c['status']}] {c['check']:10s} {c['detail']}")
    for f in rep["fixes_applied"]:
        print(f"[FIXED] {f}")
    write_env(args.env_out, rep["env"])
    print(f"verdict: {rep['verdict']}   env -> {args.env_out}")
    if args.json:
        s = json.dumps(rep, indent=2)
        if args.json == "-":
            print(s)
        else:
            with open(args.json, "w") as f:
                f.write(s)
    return {PASS: 0, WARN: 0, FAIL: 1}[rep["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
