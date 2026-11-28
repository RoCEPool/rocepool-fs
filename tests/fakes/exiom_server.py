"""An exiom memory server pair, emulated: the control protocol of
exiom/connect.py (RoCEPool/exiom-native-fiber) and the data path of its
RoCE target, for the exiom backend of the global address client library
(roce/native/rocepool_ga_exiom.c) without an FPGA.

    python3 exiom_server.py ROOT TENANTS ENDPOINT [ENDPOINT...]

TENANTS: "name:pd:bytes,..." (one region each). ENDPOINT: "ip:port", the
control port of one path to the same memory (primary, backup, ...). Prints
"READY <endpoint>|<endpoint>..." and runs until stdin closes.

What is emulated:
- control: connect / disconnect, alloc / commit / lookup / free by key with
  versions, leases (space of an old value held until its leases end) and
  tenants -- the semantics of exiom/connect.py, messages 4-byte big-endian
  length + JSON;
- data: each connect creates a queue pair of the transport library
  connected by hand (roce_raw_qp_*), responder only, on the IP of the
  path's endpoint; the tenant's region is registered memory, its rkey and
  address what connect answers. Reads and writes are the client's one-sided
  RDMA; this process's CPU does not see them (softverbs serves them).
- tests only: {"op": "emu_cut", "endpoint": i} cuts path i (its control
  listener and queue pairs go away, as when a link or a server fails).

Not emulated: the FPGA itself (exiom's own tests run its RTL), durability,
the page table and its misses.
"""
import ctypes
import json
import mmap
import os
import socket
import struct
import sys
import threading
import time


class RawAttr(ctypes.Structure):
    _fields_ = [("qpn", ctypes.c_uint32), ("psn", ctypes.c_uint32), ("ip", ctypes.c_uint32),
                ("mac", ctypes.c_uint8 * 6), ("sport", ctypes.c_uint16)]


def load(lib_path):
    lib = ctypes.CDLL(lib_path)
    P = ctypes.c_void_p
    lib.roce_raw_qp_create.restype, lib.roce_raw_qp_create.argtypes = P, [ctypes.c_char_p, ctypes.POINTER(RawAttr)]
    lib.roce_raw_qp_connect.restype = ctypes.c_int
    lib.roce_raw_qp_connect.argtypes = [P, ctypes.POINTER(RawAttr), ctypes.c_int, ctypes.c_uint32]
    lib.roce_raw_qp_mem_rkey.restype = ctypes.c_int
    lib.roce_raw_qp_mem_rkey.argtypes = [P, P, ctypes.POINTER(ctypes.c_uint32)]
    lib.roce_raw_qp_destroy.restype, lib.roce_raw_qp_destroy.argtypes = None, [P]
    lib.roce_mem_register.restype, lib.roce_mem_register.argtypes = P, [P, ctypes.c_size_t]
    lib.roce_last_error.restype = ctypes.c_char_p
    return lib


def ip_int(ip):
    return struct.unpack(">I", socket.inet_aton(ip))[0]


class Allocator:
    """first fit, 64-byte aligned, free ranges merged (exiom/connect.py)"""

    def __init__(self, base, length, align=64):
        self.align, self.free = align, [(base, length)]

    def size(self, n):
        return max(self.align, -(-n // self.align) * self.align)

    def alloc(self, n):
        size = self.size(n)
        for i, (s, room) in enumerate(self.free):
            if room >= size:
                self.free[i:i + 1] = [(s + size, room - size)] if room > size else []
                return s
        return None

    def release(self, start, n):
        merged = []
        for s, room in sorted(self.free + [(start, self.size(n))]):
            if merged and merged[-1][0] + merged[-1][1] == s:
                merged[-1] = (merged[-1][0], merged[-1][1] + room)
            else:
                merged.append((s, room))
        self.free = merged


class Server:
    def __init__(self, lib, tenants, endpoints, lease=1.0, qps=64):
        self.lib, self.lease, self.lock = lib, lease, threading.Lock()
        self.endpoints = endpoints
        self.free_qps = list(range(qps))
        self.tenants = {}
        self.version = 0
        self.quarantine = []
        self.qps = {}          # endpoint -> [raw queue pair]
        self.keys_of_qp = {}
        for name, pd, size in tenants:
            mem = mmap.mmap(-1, size)
            addr = ctypes.addressof(ctypes.c_char.from_buffer(mem))
            reg = lib.roce_mem_register(addr, size)
            self.tenants[name] = dict(pd=pd, mem=mem, va=addr, length=size, reg=reg, rkey=None,
                                      alloc=Allocator(addr, size), keys={}, pending={})
        self.listeners = []
        for i, (ip, port) in enumerate(endpoints):
            s = socket.create_server((ip, port))
            self.listeners.append(s)
            threading.Thread(target=self.accept, args=(i, s), daemon=True).start()

    # -- connections

    def accept(self, i, sock):
        while True:
            try:
                conn, _ = sock.accept()
            except OSError:
                return
            threading.Thread(target=self.serve, args=(i, conn), daemon=True).start()

    def serve(self, i, conn):
        def recv(n):
            b = b""
            while len(b) < n:
                c = conn.recv(n - len(b))
                if not c:
                    raise ConnectionError
                b += c
            return b
        try:
            while True:
                n = struct.unpack(">I", recv(4))[0]
                reply = self.handle(i, json.loads(recv(n)))
                data = json.dumps(reply, sort_keys=True).encode()
                if self.listeners[i] is None:
                    return              # path cut while this was served: no answer
                conn.sendall(struct.pack(">I", len(data)) + data)
        except (ConnectionError, OSError, ValueError):
            pass
        finally:
            conn.close()

    def handle(self, i, m):
        with self.lock:
            self.release()
            try:
                fn = getattr(self, "op_" + str(m.get("op")), None)
                if fn is None:
                    raise ValueError(f"Unknown operation {m.get('op')!r}.")
                r = fn(i, m)
                r["ok"] = True
                return r
            except (ValueError, KeyError) as e:
                return dict(ok=False, error=str(e))

    def tenant(self, m):
        if m.get("tenant") not in self.tenants:
            raise ValueError(f"Unknown tenant {m.get('tenant')!r}.")
        return self.tenants[m["tenant"]]

    # -- operations (exiom/connect.py)

    def op_connect(self, i, m):
        t = self.tenant(m)
        if not self.free_qps:
            raise ValueError("No queue pair left.")
        ip = self.endpoints[i][0]
        me = RawAttr()
        qp = self.lib.roce_raw_qp_create(ip.encode(), ctypes.byref(me))
        if not qp:
            raise ValueError("queue pair: " + self.lib.roce_last_error().decode())
        peer = RawAttr(qpn=m["qpn"], psn=m["psn"], ip=m["ip"])
        if self.lib.roce_raw_qp_connect(qp, ctypes.byref(peer), 0, 1024):
            self.lib.roce_raw_qp_destroy(qp)
            raise ValueError("connect: " + self.lib.roce_last_error().decode())
        if t["rkey"] is None:
            rkey = ctypes.c_uint32()
            if self.lib.roce_raw_qp_mem_rkey(qp, t["reg"], ctypes.byref(rkey)):
                raise ValueError("region: " + self.lib.roce_last_error().decode())
            t["rkey"] = rkey.value
        self.free_qps.pop(0)
        self.qps.setdefault(i, []).append(qp)
        return dict(qpn=me.qpn, ip=ip_int(ip), mac=0, rkey=t["rkey"], va=t["va"], length=t["length"], pmtu=1024)

    def op_alloc(self, i, m):
        t = self.tenant(m)
        n = int(m["length"])
        if n <= 0 or n % 4:
            raise ValueError("Length must be a positive multiple of 4 bytes.")
        va = t["alloc"].alloc(n)
        if va is None:
            raise ValueError("No space left.")
        self.version += 1
        t["pending"][(m["key"], self.version)] = (va, n)
        return dict(key=m["key"], version=self.version, va=va, length=n, rkey=t["rkey"] or 0)

    def op_commit(self, i, m):
        t = self.tenant(m)
        place = t["pending"].pop((m["key"], m["version"]), None)
        if place is None:
            raise ValueError("No such allocation.")
        old = t["keys"].get(m["key"])
        t["keys"][m["key"]] = (m["version"],) + place
        if old:
            self.quarantine.append((time.monotonic() + self.lease, m["tenant"], old[1], old[2]))
        return {}

    def op_lookup(self, i, m):
        t = self.tenant(m)
        if m["key"] not in t["keys"]:
            raise ValueError("No such key.")
        v, va, n = t["keys"][m["key"]]
        return dict(key=m["key"], version=v, va=va, length=n, rkey=t["rkey"] or 0, lease=self.lease)

    def op_free(self, i, m):
        t = self.tenant(m)
        if m["key"] not in t["keys"]:
            raise ValueError("No such key.")
        v, va, n = t["keys"].pop(m["key"])
        self.quarantine.append((time.monotonic() + self.lease, m["tenant"], va, n))
        return {}

    def op_emu_cut(self, i, m):
        e = int(m["endpoint"])
        if self.listeners[e] is not None:
            self.listeners[e].close()
            self.listeners[e] = None
        for qp in self.qps.pop(e, []):
            self.lib.roce_raw_qp_destroy(qp)
        return {}

    def release(self):
        now, keep = time.monotonic(), []
        for until, name, va, n in self.quarantine:
            if until <= now:
                self.tenants[name]["alloc"].release(va, n)
            else:
                keep.append((until, name, va, n))
        self.quarantine = keep


def main():
    root, tenants, endpoints = sys.argv[1], sys.argv[2], sys.argv[3:]
    sys.path.insert(0, root)
    eps = [(e.rsplit(":", 1)[0], int(e.rsplit(":", 1)[1])) for e in endpoints]
    raw_port = os.environ.get("SOFTVERBS_RAW_PORT", "4791")
    os.environ["SOFTVERBS_RAW_LISTEN"] = ",".join(f"{ip}:{raw_port}" for ip, _ in eps)
    from roce.native.build_lib import ensure_softverbs_lib
    lib = load(os.environ.get("ROCE_TRANSPORT_LIB") or ensure_softverbs_lib())
    ts = []
    for t in tenants.split(","):
        name, pd, size = t.split(":")
        ts.append((name, int(pd), int(size)))
    Server(lib, ts, eps, lease=float(os.environ.get("EXIOM_EMU_LEASE", "1.0")))
    print("READY " + "|".join(endpoints), flush=True)
    sys.stdin.read()


if __name__ == "__main__":
    main()
