"""What this host offers the OST's capacity tier (roce/native/ost_tier.c) and
the P2P data path, from sysfs alone -- no device is touched:

  - NVMe controllers: PCI address, model, CMB (controller memory buffer),
    namespaces and their sizes, where they are mounted and the free space
  - P2P memory the kernel publishes for them (CONFIG_PCI_P2PDMA, Linux 6.2+:
    /sys/bus/pci/devices/<nvme>/p2pmem/{size,available,published}; an
    mmap of .../p2pmem/allocate hands out the drive's CMB)
  - RDMA NICs: PCI address and driver, and for every (NIC, NVMe) pair the
    PCIe relationship -- P2P DMA between them needs a common upstream port
    (same switch or root port) the kernel accepts, and a NIC driver that
    registers P2P pages (mlx5 since 6.2)

and a recommendation: ROCEPOOL_OST_TIER (a file on the NVMe-backed file
system with the most free space; a raw block device is never chosen by
itself -- pass one explicitly) and ROCEPOOL_P2PMEM (the p2pmem directory of
an NVMe with a published CMB under the same upstream port as the NIC).

    python3 tools/nvme_preflight.py [--sysfs-root /] [--osts-per-node 1] [--json]

tools/roce_onsite.py runs it on every host and puts the recommendation in
the cluster's environment; tools/io500_run.py --ost-tier auto does the same.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time


def _read(path: str, default: str = "") -> str:
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return default


def _pci_path(root: str, addr: str) -> list[str]:
    """PCI addresses from the root port down to `addr` (sysfs ancestry)."""
    real = os.path.realpath(os.path.join(root, "sys", "bus", "pci", "devices", addr))
    return [p for p in real.split(os.sep) if re.fullmatch(r"[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]", p)]


def nvme_controllers(root: str) -> list[dict]:
    base = os.path.join(root, "sys", "class", "nvme")
    out = []
    for name in sorted(os.listdir(base)) if os.path.isdir(base) else []:
        d = os.path.join(base, name)
        dev = os.path.realpath(os.path.join(d, "device"))
        addr = os.path.basename(dev)
        if not re.fullmatch(r"[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]", addr):
            addr = ""
        cmb = _read(os.path.join(d, "cmb"))  # "cmbloc : 0x... cmbsz : 0x..." when the drive has one
        p2p = os.path.join(root, "sys", "bus", "pci", "devices", addr, "p2pmem") if addr else ""
        p2pmem = None
        if p2p and os.path.isdir(p2p):
            p2pmem = {"dir": "/" + os.path.relpath(p2p, root) if root != "/" else p2p,
                      "size": int(_read(os.path.join(p2p, "size"), "0") or 0),
                      "available": int(_read(os.path.join(p2p, "available"), "0") or 0),
                      "published": _read(os.path.join(p2p, "published"), "0") == "1"}
        namespaces = []
        for ns in sorted(os.listdir(d)):
            if not re.fullmatch(name + r"n\d+", ns):
                continue
            sectors = int(_read(os.path.join(d, ns, "size"), "0") or 0)
            namespaces.append({"name": ns, "size_bytes": sectors * 512})
        out.append({"name": name, "pci": addr, "model": _read(os.path.join(d, "model")),
                    "cmb": cmb, "has_cmb": bool(cmb) and "cmbsz" in cmb, "p2pmem": p2pmem, "namespaces": namespaces,
                    "pci_path": _pci_path(root, addr) if addr else []})
    return out


def rdma_nics(root: str) -> list[dict]:
    base = os.path.join(root, "sys", "class", "infiniband")
    out = []
    for name in sorted(os.listdir(base)) if os.path.isdir(base) else []:
        dev = os.path.realpath(os.path.join(base, name, "device"))
        addr = os.path.basename(dev)
        drv = os.path.basename(os.path.realpath(os.path.join(root, "sys", "bus", "pci", "devices", addr, "driver")))
        out.append({"name": name, "pci": addr, "driver": drv, "pci_path": _pci_path(root, addr)})
    return out


def _leaves(root: str, blk: str, depth: int = 0) -> list[str]:
    """the physical block devices under a block device: itself, or what its
    device-mapper / md stack (LVM, LUKS, RAID) sits on (sysfs slaves)"""
    slaves = os.path.join(root, "sys", "class", "block", blk, "slaves")
    try:
        under = sorted(os.listdir(slaves))
    except OSError:
        under = []
    if not under or depth > 8:
        return [blk]
    out = []
    for u in under:
        out += _leaves(root, u, depth + 1)
    return out


def _block_name(root: str, dev: str) -> str:
    """/dev/mapper/vg-lv -> dm-0 (the symlink's target), /dev/md0 -> md0"""
    real = os.path.realpath(os.path.join(root, dev.lstrip("/")) if root != "/" else dev)
    return os.path.basename(real)


LOCAL_FS = ("ext4", "xfs", "btrfs", "ext3", "f2fs")


def mounts(root: str, nvme_only: bool = True) -> list[dict]:
    """mounted file systems on NVMe namespaces -- directly, on a partition, or
    through device-mapper / md (LVM, the Ubuntu installer's default, puts the
    root file system on /dev/mapper/...). nvme_only=False: every writable
    local block-device file system (the fallback tier)."""
    out, seen = [], set()
    for line in _read(os.path.join(root, "proc", "mounts")).splitlines():
        parts = line.split()
        if len(parts) < 4 or not parts[0].startswith("/dev/"):
            continue
        dev, mnt, fstype, opts = parts[0], parts[1], parts[2], parts[3].split(",")
        if "ro" in opts or fstype not in LOCAL_FS or mnt.startswith(("/boot", "/snap")) or (dev, mnt) in seen:
            continue
        seen.add((dev, mnt))
        leaves = _leaves(root, _block_name(root, dev)) if not dev.startswith("/dev/nvme") else [os.path.basename(dev)]
        nv = [m.group(1) for m in (re.match(r"(nvme\d+n\d+)", x) for x in leaves) if m]
        if nvme_only and not nv:
            continue
        try:
            st = os.statvfs(mnt if root == "/" else os.path.join(root, mnt.lstrip("/")))
            free = st.f_bavail * st.f_frsize
        except OSError:
            free = 0
        out.append({"device": dev, "namespace": nv[0] if nv else "", "mountpoint": mnt, "fstype": fstype,
                    "free_bytes": free, "via": leaves if leaves != [os.path.basename(dev)] else None})
    return out


def relation(nic: dict, nvme: dict) -> dict:
    """PCIe relationship of a NIC and an NVMe: the common upstream port and
    the hop count between them. P2P needs a common port below the host
    bridge (a switch or the same root port); across root complexes most
    platforms refuse it (the kernel's p2pdma distance says so at run time)."""
    a, b = nic.get("pci_path", []), nvme.get("pci_path", [])
    common = 0
    while common < len(a) and common < len(b) and a[common] == b[common]:
        common += 1
    return {"common_upstream": a[common - 1] if common else None,
            "hops": (len(a) - common) + (len(b) - common),
            "same_root_port": common >= 1 and len(a) > 1 and len(b) > 1 and a[0] == b[0]}


def probe(root: str = "/", osts_per_node: int = 1) -> dict:
    kernel = _read(os.path.join(root, "proc", "sys", "kernel", "osrelease"))
    nvmes = nvme_controllers(root)
    nics = rdma_nics(root)
    mnts = mounts(root)
    pairs = [{"nic": n["name"], "nvme": v["name"], **relation(n, v)} for n in nics for v in nvmes]
    rec: dict = {"tier": None, "p2pmem": None, "notes": []}
    # the tier: a file on the NVMe-backed file system with the most room;
    # none: the local file system with the most room (slower than NVMe, but
    # a RAM-only OST cannot hold a 300 s write phase and the run would be
    # INVALID); a raw namespace is never written without being asked
    best = max(mnts, key=lambda m: m["free_bytes"], default=None)
    min_free = (16 << 30) * osts_per_node
    if not (best and best["free_bytes"] > min_free):
        other = max(mounts(root, nvme_only=False), key=lambda m: m["free_bytes"], default=None)
        if other and other["free_bytes"] > min_free:
            best = other
            rec["notes"].append(f"no NVMe-backed file system with room: the tier goes on {other['mountpoint']} "
                                f"({other['device']}, {other['free_bytes'] >> 30} GiB free; not NVMe -- slower)")
        else:
            best = None
    if best:
        # a data file system: 80% of its free space; the root file system
        # (the OS lives there): half
        share = 0.5 if best["mountpoint"] == "/" else 0.8
        per_ost_gib = max(4, int(best["free_bytes"] * share / osts_per_node) >> 30)
        d = best["mountpoint"] if best["namespace"] and best["mountpoint"] != "/" else \
            os.path.join(best["mountpoint"], "rocepool_tier")
        rec["tier"] = f"file:{os.path.join(d, 'rocepool_ost_{n}.tier')}:{per_ost_gib}"
        rec["tier_dir"] = d
        rec["tier_mount"] = best
    elif nvmes:
        rec["notes"].append("NVMe present but no file system with room on it (and no other local one): mount one, "
                            "or pass ROCEPOOL_OST_TIER=file:/dev/nvmeXnY explicitly to use a raw namespace")
    else:
        rec["notes"].append("no NVMe controller and no local file system with room: the OST keeps everything in RAM")
    # P2P: a published CMB on an NVMe under the same upstream port as a NIC
    # whose driver can register P2P pages
    kv = tuple(int(x) for x in re.findall(r"\d+", kernel)[:2]) if kernel else (0, 0)
    for v in nvmes:
        p = v.get("p2pmem")
        if not p or not p["published"] or p["size"] <= 0:
            continue
        for n in nics:
            r = relation(n, v)
            if r["same_root_port"] and r["common_upstream"]:
                ok_driver = n["driver"].startswith("mlx5")
                if kv >= (6, 2) and ok_driver:
                    rec["p2pmem"] = p["dir"]
                    rec["p2p_pair"] = {"nic": n["name"], "nvme": v["name"], **r}
                else:
                    rec["notes"].append(f"{v['name']} publishes {p['size'] >> 20} MiB of CMB under the same port as "
                                        f"{n['name']}, but " + ("the kernel is older than 6.2" if kv < (6, 2) else
                                        f"driver {n['driver']} is not known to register P2P pages") +
                                        ": not used")
    if not rec["p2pmem"] and any(v["has_cmb"] for v in nvmes) and not any(v.get("p2pmem") for v in nvmes):
        rec["notes"].append("an NVMe has a CMB but the kernel publishes no p2pmem for it (CONFIG_PCI_P2PDMA off, "
                            "or the drive's CMB is not usable for data): the tier runs through host memory")
    return {"kernel": kernel, "nvme": nvmes, "rdma_nics": nics, "nvme_mounts": mnts, "pairs": pairs,
            "recommendation": rec}


def render(p: dict) -> str:
    lines = [f"kernel {p['kernel']}"]
    for v in p["nvme"]:
        ns = ", ".join(f"{n['name']} {n['size_bytes'] >> 30} GiB" for n in v["namespaces"])
        pm = v["p2pmem"]
        lines.append(f"  NVMe {v['name']} {v['pci']} {v['model']!r}: {ns or 'no namespace'}; CMB "
                     f"{'yes' if v['has_cmb'] else 'no'}; p2pmem "
                     f"{(str(pm['size'] >> 20) + ' MiB' + (' published' if pm['published'] else ' unpublished')) if pm else 'none'}")
    for n in p["rdma_nics"]:
        lines.append(f"  RDMA NIC {n['name']} {n['pci']} driver {n['driver']}")
    for m in p["nvme_mounts"]:
        lines.append(f"  mount {m['device']} on {m['mountpoint']} ({m['fstype']}): {m['free_bytes'] >> 30} GiB free")
    for q in p["pairs"]:
        lines.append(f"  {q['nic']} <-> {q['nvme']}: common upstream {q['common_upstream'] or 'none'}, "
                     f"{q['hops']} hops, {'same root port' if q['same_root_port'] else 'different root ports'}")
    r = p["recommendation"]
    lines.append(f"  -> ROCEPOOL_OST_TIER={r['tier'] or '(none)'}")
    lines.append(f"  -> ROCEPOOL_P2PMEM={r['p2pmem'] or '(none)'}")
    for note in r["notes"]:
        lines.append(f"  note: {note}")
    return "\n".join(lines)


def measure_write(d: str, mib: int = 1024) -> dict:
    """O_DIRECT 1 MiB writes into the tier's directory (dd, then removed):
    what the OST's direct writes will get from this file system. A slow
    drive under a host's OSTs bounds the whole cluster's write bandwidth
    (data is striped evenly), and queues long enough to time out."""
    path = os.path.join(d, ".rocepool_write_probe")
    try:
        os.makedirs(d, exist_ok=True)
        t0 = time.monotonic()
        r = subprocess.run(["dd", "if=/dev/zero", f"of={path}", "bs=1M", f"count={mib}", "oflag=direct",
                            "conv=fsync"], capture_output=True, text=True, timeout=300)
        dt = time.monotonic() - t0
        if r.returncode:
            return {"error": (r.stderr or r.stdout).strip()[-200:]}
        return {"mib_s": round(mib / dt, 1), "seconds": round(dt, 2)}
    except (OSError, subprocess.SubprocessError) as exc:
        return {"error": str(exc)}
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--sysfs-root", default="/", help=argparse.SUPPRESS)
    ap.add_argument("--osts-per-node", type=int, default=1)
    ap.add_argument("--json", action="store_true", help="one JSON line (what the automation reads)")
    ap.add_argument("--measure", action="store_true", help="also write 1 GiB (O_DIRECT) into the tier's directory")
    args = ap.parse_args()
    p = probe(args.sysfs_root, args.osts_per_node)
    rec = p["recommendation"]
    if args.measure and rec.get("tier"):
        d = rec.get("tier_dir") or os.path.dirname(rec["tier"][5:].rsplit(":", 1)[0])
        rec["tier_write"] = measure_write(d)
    print(json.dumps(p) if args.json else render(p))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
