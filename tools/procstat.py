"""CPU accounting of this project's processes on one host, for bottleneck
analysis (tools/io500_run.py). Prints JSON with one record per process:

    {"pid", "role": mdt|ost|fuse|io500, "addr", "mntns",
     "cpu": {"<thread group>": {"user": s, "sys": s}}}

Thread groups come from the thread names set in the C code: "emulator"
(softverbs sv-*, the RDMA emulator -- absent on real NICs), "server"
(rt-* connection threads: transport + request handling), "fuse_worker",
"fuse_io", and "other". `mntns` (the mount-namespace inode) tells the
nodes of a simulated cluster apart; on real hosts each host reports itself.

    python3 tools/procstat.py
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time

TICK = os.sysconf("SC_CLK_TCK")


def _group(comm: str) -> str:
    if comm.startswith("sv-"):
        return "emulator"
    if comm.startswith("rt-"):
        return "server"
    if comm.startswith("fuse-worker"):
        return "fuse_worker"
    if comm.startswith("fuse-io"):
        return "fuse_io"
    return "other"


def _times(path: str) -> tuple[str, float, float]:
    raw = open(path).read()
    comm = raw[raw.index("(") + 1:raw.rindex(")")]
    f = raw[raw.rindex(")") + 2:].split()
    return comm, int(f[11]) / TICK, int(f[12]) / TICK  # utime, stime


def _role(argv: list[str]) -> tuple[str | None, str]:
    joined = " ".join(argv)
    if "crossover_node.py" in joined:
        for a, r in (("mdt-native", "mdt"), ("ost-native", "ost")):
            if a in argv:
                return r, argv[argv.index(a) + 1]
    if argv and os.path.basename(argv[0]) == "rocepool-fuse":
        return "fuse", ""
    if argv and os.path.basename(argv[0]) == "io500":
        return "io500", ""
    return None, ""


def host_cpu() -> dict:
    """Host-wide CPU seconds by state (/proc/stat), incl. idle and softirq
    (kernel networking is often charged there, not to a process)."""
    f = open("/proc/stat").readline().split()[1:]
    names = ["user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"]
    return {**{n: int(v) / TICK for n, v in zip(names, f)}, "cores": os.cpu_count()}  # (no dict | : peers may run python < 3.9)


def snapshot() -> list[dict]:
    out = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            argv = open(f"/proc/{pid}/cmdline", "rb").read().split(b"\0")
            role, addr = _role([a.decode(errors="replace") for a in argv if a])
            if not role:
                continue
            mntns = os.readlink(f"/proc/{pid}/ns/mnt")
            cpu: dict[str, dict[str, float]] = {}
            for tid in os.listdir(f"/proc/{pid}/task"):
                try:
                    comm, u, s = _times(f"/proc/{pid}/task/{tid}/stat")
                except OSError:
                    continue
                g = cpu.setdefault(_group(comm), {"user": 0.0, "sys": 0.0})
                g["user"] += u
                g["sys"] += s
            # threads that already exited are only in the process totals
            _, pu, ps = _times(f"/proc/{pid}/stat")
            tu = sum(g["user"] for g in cpu.values())
            ts = sum(g["sys"] for g in cpu.values())
            if pu + ps > tu + ts:
                g = cpu.setdefault("exited", {"user": 0.0, "sys": 0.0})
                g["user"] += max(0.0, pu - tu)
                g["sys"] += max(0.0, ps - ts)
            rss_kb = 0
            try:
                with open(f"/proc/{pid}/statm") as f:
                    rss_kb = int(f.read().split()[1]) * (os.sysconf("SC_PAGE_SIZE") // 1024)
            except (OSError, ValueError, IndexError):
                pass
            out.append({"pid": int(pid), "role": role, "addr": addr, "mntns": mntns, "cpu": cpu, "rss_kb": rss_kb})
        except (OSError, ValueError):
            continue
    _fuse_stats([r for r in out if r["role"] == "fuse"])
    return out


def _read_stats(path: str) -> dict | None:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _fuse_stats(recs: list[dict], timeout: float = 3.0) -> None:
    """rocepool-fuse dumps its per-opcode counters on SIGUSR1 (to
    /tmp/rocepool-fuse-<pid>.stats.json); wait for each fresh dump."""
    before = {}
    for r in recs:
        path = f"/tmp/rocepool-fuse-{r['pid']}.stats.json"
        before[r["pid"]] = (_read_stats(path) or {}).get("seq", 0)
        try:
            os.kill(r["pid"], signal.SIGUSR1)
        except OSError:
            pass
    deadline = time.monotonic() + timeout
    for r in recs:
        path = f"/tmp/rocepool-fuse-{r['pid']}.stats.json"
        while True:
            st = _read_stats(path)
            if st and st.get("seq", 0) > before[r["pid"]]:
                r["fuse_stats"] = st
                break
            if time.monotonic() > deadline:
                break
            time.sleep(0.01)


def efa_max_qp() -> int | None:
    """Queue pairs an EFA device has (ibv_devinfo -v: max_qp), the smallest
    of the host's EFA devices -- shared by every process on the host: each
    OFI endpoint (servers', ranks', MPI's) takes one. None without EFA."""
    import subprocess
    base = "/sys/class/infiniband"
    try:
        devs = [d for d in os.listdir(base)
                if os.path.basename(os.path.realpath(os.path.join(base, d, "device", "driver"))) == "efa"]
    except OSError:
        return None
    qps = []
    for d in devs:
        try:
            r = subprocess.run(["ibv_devinfo", "-v", "-d", d], capture_output=True, text=True, timeout=20)
        except (OSError, subprocess.TimeoutExpired):
            continue
        q = max_qp_of(r.stdout)
        if q:
            qps.append(q)
    return min(qps) if qps else None


def max_qp_of(devinfo: str) -> int | None:
    """max_qp from `ibv_devinfo -v` output"""
    for line in devinfo.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] == "max_qp:" and parts[1].isdigit():
            return int(parts[1])
    return None


def facts(ip: str | None) -> dict:
    """Hardware facts for the projection profile: cores, RAM, and the speed of
    the NIC that holds `ip`."""
    import fcntl
    import socket
    import struct
    mem = avail = 0.0
    for line in open("/proc/meminfo"):
        if line.startswith("MemTotal:"):
            mem = int(line.split()[1]) / 2**20
        elif line.startswith("MemAvailable:"):
            avail = int(line.split()[1]) / 2**20
    out = {"cores": os.cpu_count(), "mem_gib": round(mem, 1), "mem_avail_gib": round(avail or mem, 1),
           "nic_gbps": None, "netdev": None,
           "nvme_count": 0, "nvme_gib": 0.0,
           "numa_nodes": len([d for d in os.listdir("/sys/devices/system/node") if d.startswith("node")])
           if os.path.isdir("/sys/devices/system/node") else 1}
    try:  # the NVMe namespaces (the OST tier's capacity, tools/nvme_preflight.py has the rest)
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from tools.nvme_preflight import nvme_controllers
        ns = [n for c in nvme_controllers("/") for n in c["namespaces"]]
        out["nvme_count"] = len(ns)
        out["nvme_gib"] = round(sum(n["size_bytes"] for n in ns) / 2**30, 1)
    except Exception:  # noqa: BLE001
        pass
    out["efa_max_qp"] = efa_max_qp()
    if ip:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for _, name in socket.if_nameindex():
            try:
                a = fcntl.ioctl(s.fileno(), 0x8915, struct.pack("256s", name.encode()[:15]))  # SIOCGIFADDR
            except OSError:
                continue
            if socket.inet_ntoa(a[20:24]) == ip:
                out["netdev"] = name
                try:
                    mbps = int(open(f"/sys/class/net/{name}/speed").read())
                    if mbps > 0:  # -1 = unknown (veth, some virtual NICs)
                        out["nic_gbps"] = mbps / 1000
                except (OSError, ValueError):
                    pass
    return out


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--facts":
        json.dump(facts(sys.argv[2] if len(sys.argv) > 2 else None), sys.stdout)
        print()
        sys.exit(0)
    json.dump({"host": host_cpu(), "procs": snapshot()}, sys.stdout)
    print()
