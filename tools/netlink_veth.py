"""Builds a two-node "crossover cable" topology inside one Linux host, with
no `ip`/iproute2 binary required (it isn't installable in some sandboxes):
two network namespaces (nodeA, nodeB) joined by one veth pair -- a
point-to-point virtual Ethernet link with no bridge/switch in between,
which is exactly a back-to-back cable between two NICs.

Speaks rtnetlink directly (RTM_NEWLINK / RTM_NEWADDR). Needs root.

    python3 tools/netlink_veth.py up     # create nsA/nsB + veth, 10.99.0.1 <-> 10.99.0.2
    python3 tools/netlink_veth.py down   # tear it all down
Run a process on a node with:  nsenter --net=/run/netns/rocepool_nodeA <cmd>
"""
from __future__ import annotations

import ctypes
import os
import socket
import struct
import subprocess
import sys

NETNS_DIR = "/run/netns"
NODES = {  # ns name -> (veth name, address)
    "rocepool_nodeA": ("rpA", "10.99.0.1"),
    "rocepool_nodeB": ("rpB", "10.99.0.2"),
}
PREFIX = 30

RTM_NEWLINK, RTM_NEWADDR = 16, 20
NLM_F_REQUEST, NLM_F_ACK, NLM_F_EXCL, NLM_F_CREATE = 1, 4, 0x200, 0x400
NLMSG_ERROR = 2
IFLA_IFNAME, IFLA_LINKINFO, IFLA_NET_NS_FD = 3, 18, 28
IFLA_INFO_KIND, IFLA_INFO_DATA, VETH_INFO_PEER = 1, 2, 1
IFA_ADDRESS, IFA_LOCAL = 1, 2
IFF_UP = 1
CLONE_NEWNET = 0x40000000


def _attr(t: int, payload: bytes) -> bytes:
    raw = struct.pack("=HH", 4 + len(payload), t) + payload
    return raw + b"\0" * ((4 - len(raw) % 4) % 4)


def _ifinfo(index: int = 0, flags: int = 0, change: int = 0) -> bytes:
    return struct.pack("=BxHiII", socket.AF_UNSPEC, 0, index, flags, change)


def _request(msg_type: int, flags: int, body: bytes) -> None:
    s = socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, socket.NETLINK_ROUTE)
    try:
        s.bind((0, 0))
        hdr = struct.pack("=IHHII", 16 + len(body), msg_type, flags | NLM_F_REQUEST | NLM_F_ACK, 1, 0)
        s.send(hdr + body)
        reply = s.recv(65536)
        _, rtype, _, _, _ = struct.unpack("=IHHII", reply[:16])
        if rtype == NLMSG_ERROR:
            (err,) = struct.unpack("=i", reply[16:20])
            if err != 0:
                raise OSError(-err, os.strerror(-err))
    finally:
        s.close()


def _setns(path: str) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    fd = os.open(path, os.O_RDONLY)
    try:
        if libc.setns(fd, CLONE_NEWNET) != 0:
            raise OSError(ctypes.get_errno(), "setns failed")
    finally:
        os.close(fd)


def _configure_inside(ns: str, ifname: str, addr: str) -> None:
    """Runs in a forked child so the parent stays in the root namespace."""
    pid = os.fork()
    if pid == 0:
        code = 0
        try:
            _setns(os.path.join(NETNS_DIR, ns))
            for name in ("lo", ifname):
                _request(RTM_NEWLINK, 0, _ifinfo(socket.if_nametoindex(name), IFF_UP, IFF_UP))
            idx = socket.if_nametoindex(ifname)
            ip = socket.inet_aton(addr)
            body = struct.pack("=BBBBI", socket.AF_INET, PREFIX, 0, 0, idx) + _attr(IFA_LOCAL, ip) + _attr(IFA_ADDRESS, ip)
            _request(RTM_NEWADDR, NLM_F_CREATE | NLM_F_EXCL, body)
        except Exception as exc:  # noqa: BLE001
            print(f"[{ns}] configure failed: {exc}", file=sys.stderr)
            code = 1
        os._exit(code)
    _, status = os.waitpid(pid, 0)
    if os.waitstatus_to_exitcode(status) != 0:
        raise RuntimeError(f"configuring {ns} failed")


def up() -> None:
    down(quiet=True)
    os.makedirs(NETNS_DIR, exist_ok=True)
    for ns in NODES:
        open(os.path.join(NETNS_DIR, ns), "w").close()  # bind-mount target must exist
        subprocess.run(["unshare", f"--net={os.path.join(NETNS_DIR, ns)}", "true"], check=True)
    (ns_a, (if_a, _)), (ns_b, (if_b, _)) = NODES.items()
    fd_a = os.open(os.path.join(NETNS_DIR, ns_a), os.O_RDONLY)
    fd_b = os.open(os.path.join(NETNS_DIR, ns_b), os.O_RDONLY)
    try:
        peer = _ifinfo() + _attr(IFLA_IFNAME, if_b.encode() + b"\0") + _attr(IFLA_NET_NS_FD, struct.pack("=I", fd_b))
        linkinfo = _attr(IFLA_INFO_KIND, b"veth\0") + _attr(IFLA_INFO_DATA, _attr(VETH_INFO_PEER, peer))
        body = (_ifinfo() + _attr(IFLA_IFNAME, if_a.encode() + b"\0")
                + _attr(IFLA_NET_NS_FD, struct.pack("=I", fd_a)) + _attr(IFLA_LINKINFO, linkinfo))
        _request(RTM_NEWLINK, NLM_F_CREATE | NLM_F_EXCL, body)
    finally:
        os.close(fd_a)
        os.close(fd_b)
    for ns, (ifname, addr) in NODES.items():
        _configure_inside(ns, ifname, addr)
    print("crossover link up: " + "  <==veth==>  ".join(f"{ns}:{i}/{a}" for ns, (i, a) in NODES.items()))


def down(quiet: bool = False) -> None:
    # Deleting a namespace destroys the veth end inside it, which destroys
    # the peer too -- no separate RTM_DELLINK needed.
    for ns in NODES:
        path = os.path.join(NETNS_DIR, ns)
        if os.path.exists(path):
            subprocess.run(["umount", path], check=False, capture_output=True)
            os.unlink(path)
    if not quiet:
        print("crossover link down")


if __name__ == "__main__":
    {"up": up, "down": down}[sys.argv[1]]()
