"""An N-node "cluster" inside one Linux host, for rehearsing multi-node runs
(tools/io500_run.py) before real hardware: N network namespaces on one
Linux bridge (a switch), plus one MOUNT namespace per node, so every node
has its own FUSE mount at the same path -- exactly like N real hosts each
mounting /mnt/rocepool. Speaks rtnetlink directly (no iproute2 needed),
like tools/netlink_veth.py. Needs root.

    python3 tools/netns_cluster.py up 10    # rpn0..rpn9 = 10.99.1.1..10, bridge 10.99.1.254
    python3 tools/netns_cluster.py down

Run a process on node i:  nsenter --net=/run/netns/rpn<i> --mount=/run/rocepool_mntns/rpn<i> <cmd>
"""
from __future__ import annotations

import os
import socket
import struct
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.netlink_veth import (IFF_UP, IFLA_IFNAME, IFLA_INFO_DATA, IFLA_INFO_KIND, IFLA_LINKINFO,
                                IFLA_NET_NS_FD, NETNS_DIR, NLM_F_CREATE, NLM_F_EXCL, RTM_NEWADDR, RTM_NEWLINK,
                                VETH_INFO_PEER, IFA_ADDRESS, IFA_LOCAL, _attr, _ifinfo, _request, _setns)

MNTNS_DIR = "/run/rocepool_mntns"
BRIDGE = "rpbr0"
SUBNET = "10.99.1"
BRIDGE_IP = f"{SUBNET}.254"
PREFIX = 24
IFLA_MASTER = 10
STATE = "/run/rocepool_cluster_n"


def node_name(i: int) -> str:
    return f"rpn{i}"


def node_ip(i: int) -> str:
    return f"{SUBNET}.{i + 1}"


def _addr(ifname: str, addr: str) -> None:
    ip = socket.inet_aton(addr)
    body = (struct.pack("=BBBBI", socket.AF_INET, PREFIX, 0, 0, socket.if_nametoindex(ifname))
            + _attr(IFA_LOCAL, ip) + _attr(IFA_ADDRESS, ip))
    _request(RTM_NEWADDR, NLM_F_CREATE | NLM_F_EXCL, body)


def _link_up(ifname: str, master: int | None = None) -> None:
    body = _ifinfo(socket.if_nametoindex(ifname), IFF_UP, IFF_UP)
    if master is not None:
        body += _attr(IFLA_MASTER, struct.pack("=I", master))
    _request(RTM_NEWLINK, 0, body)


def _inside(ns: str, fn) -> None:
    pid = os.fork()
    if pid == 0:
        code = 0
        try:
            _setns(os.path.join(NETNS_DIR, ns))
            fn()
        except Exception as exc:  # noqa: BLE001
            print(f"[{ns}] {exc}", file=sys.stderr)
            code = 1
        os._exit(code)
    _, status = os.waitpid(pid, 0)
    if os.waitstatus_to_exitcode(status) != 0:
        raise RuntimeError(f"configuring {ns} failed")


def _private_dir(path: str) -> None:
    """A directory that can hold persistent mount-namespace bind mounts."""
    os.makedirs(path, exist_ok=True)
    mounts = open("/proc/self/mountinfo").read()
    if f" {path} " not in mounts:
        subprocess.run(["mount", "--bind", path, path], check=True)
    subprocess.run(["mount", "--make-private", path], check=True)


def up(n: int) -> list[str]:
    down(quiet=True)
    from tools import netlink_veth
    netlink_veth.down(quiet=True)  # same address plan family; never both
    os.makedirs(NETNS_DIR, exist_ok=True)
    _private_dir(MNTNS_DIR)
    _request(RTM_NEWLINK, NLM_F_CREATE | NLM_F_EXCL,
             _ifinfo() + _attr(IFLA_IFNAME, BRIDGE.encode() + b"\0")
             + _attr(IFLA_LINKINFO, _attr(IFLA_INFO_KIND, b"bridge\0")))
    _link_up(BRIDGE)
    _addr(BRIDGE, BRIDGE_IP)
    br = socket.if_nametoindex(BRIDGE)
    for i in range(n):
        ns = node_name(i)
        path = os.path.join(NETNS_DIR, ns)
        open(path, "w").close()
        subprocess.run(["unshare", f"--net={path}", "true"], check=True)
        mpath = os.path.join(MNTNS_DIR, ns)
        open(mpath, "w").close()
        subprocess.run(["unshare", f"--mount={mpath}", "--propagation", "private", "true"], check=True)
        fd = os.open(path, os.O_RDONLY)
        try:
            peer = _ifinfo() + _attr(IFLA_IFNAME, b"eth0\0") + _attr(IFLA_NET_NS_FD, struct.pack("=I", fd))
            linkinfo = _attr(IFLA_INFO_KIND, b"veth\0") + _attr(IFLA_INFO_DATA, _attr(VETH_INFO_PEER, peer))
            _request(RTM_NEWLINK, NLM_F_CREATE | NLM_F_EXCL,
                     _ifinfo() + _attr(IFLA_IFNAME, f"rpv{i}".encode() + b"\0") + _attr(IFLA_LINKINFO, linkinfo))
        finally:
            os.close(fd)
        _link_up(f"rpv{i}", master=br)
        ip = node_ip(i)
        _inside(ns, lambda ip=ip: (_link_up("lo"), _link_up("eth0"), _addr("eth0", ip)))
    with open(STATE, "w") as f:
        f.write(str(n))
    print(f"netns cluster up: {n} nodes {node_ip(0)}..{node_ip(n - 1)} on bridge {BRIDGE} ({BRIDGE_IP})")
    return [node_name(i) for i in range(n)]


def _kill_inside(paths: list[str]) -> None:
    """Kill every process still inside one of these namespaces (a failed MPI
    start can leave ranks behind; they would keep the namespace and its veth
    alive, and the next `up` would fail with "File exists")."""
    import signal
    inos = set()
    for p in paths:
        try:
            inos.add(os.stat(p).st_ino)
        except OSError:
            pass
    if not inos:
        return
    for pid in os.listdir("/proc"):
        if not pid.isdigit() or int(pid) == os.getpid():
            continue
        for kind in ("net", "mnt"):
            try:
                if os.stat(f"/proc/{pid}/ns/{kind}").st_ino in inos:
                    os.kill(int(pid), signal.SIGKILL)
                    break
            except OSError:
                pass


def down(quiet: bool = False) -> None:
    n = 0
    try:
        n = int(open(STATE).read())
    except (OSError, ValueError):
        pass
    _kill_inside([os.path.join(base, node_name(i)) for i in range(max(n, 64)) for base in (MNTNS_DIR, NETNS_DIR)])
    import time
    time.sleep(0.2)
    for i in range(max(n, 64)):
        ns = node_name(i)
        for base in (MNTNS_DIR, NETNS_DIR):
            path = os.path.join(base, ns)
            if os.path.exists(path):
                subprocess.run(["umount", "-l", path], capture_output=True)
                try:
                    os.unlink(path)
                except OSError:
                    pass
    RTM_DELLINK = 17
    for _, name in socket.if_nameindex():  # host ends of veths whose namespace outlived us, then the bridge
        if name.startswith("rpv") or name == BRIDGE:
            try:
                _request(RTM_DELLINK, 0, _ifinfo(socket.if_nametoindex(name)))
            except OSError:
                pass
    try:
        os.unlink(STATE)
    except OSError:
        pass
    if not quiet:
        print("netns cluster down")


def enter(i: int) -> list[str]:
    """argv prefix that runs a command on node i (its net + mount namespace)."""
    ns = node_name(i)
    return ["nsenter", f"--net={os.path.join(NETNS_DIR, ns)}", f"--mount={os.path.join(MNTNS_DIR, ns)}"]


if __name__ == "__main__":
    if sys.argv[1] == "up":
        up(int(sys.argv[2]) if len(sys.argv) > 2 else 10)
    else:
        down()
