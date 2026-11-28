"""Python binding for the global address client library
(roce/native/rocepool_ga.h): the memory of all the OSTs as one address
space, read and written by address straight into and out of GPU memory.

    from roce.ga import GlobalMemory
    ga = GlobalMemory(["10.0.0.1:19951", "10.0.0.2:19952"])   # the OST list, the same everywhere
    base = ga.alloc(1 << 30, name="kv-pool-0")      # a 1 GiB region: its first byte's address
    buf = ga.register_device(tensor.data_ptr(), tensor.numel() * tensor.element_size())
    buf.write(base + 4096, offset=0, length=65536)  # GPU -> the region (the OST pulls it)
    buf.read(base + 4096, offset=0, length=65536)   # the region -> GPU: one RDMA READ, no OST CPU
    buf.mread([a0, a1, a2], offsets=[...], lengths=[...])   # every OST in parallel
    ga.lookup("kv-pool-0")                          # another process: (address, size)
    ga.free(base)

An address is an int: OST << 56 | region << 40 | offset. Regions are not
durable (they live while their OST process runs). Library: the KV client
library (roce.kv), which carries rocepool_ga.c.
"""
from __future__ import annotations

import ctypes
import errno
import os

OFFSET_BITS = 40


def make_address(ost: int, region: int, offset: int = 0) -> int:
    return (ost << 56) | ((region & 0xFFFF) << OFFSET_BITS) | (offset & ((1 << OFFSET_BITS) - 1))


def split_address(addr: int) -> tuple[int, int, int]:
    return addr >> 56, (addr >> OFFSET_BITS) & 0xFFFF, addr & ((1 << OFFSET_BITS) - 1)


_lib = None


def _load():
    global _lib
    if _lib is not None:
        return _lib
    from roce import kv
    lib = kv._load()
    P, S, U64, C, I = ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint64, ctypes.c_char_p, ctypes.c_int
    PU64, PS, PI = ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_int)
    lib.rpga_open.restype, lib.rpga_open.argtypes = P, [C]
    lib.rpga_close.restype, lib.rpga_close.argtypes = None, [P]
    lib.rpga_osts.restype, lib.rpga_osts.argtypes = I, [P]
    lib.rpga_alloc.restype, lib.rpga_alloc.argtypes = I, [P, I, C, U64, I, PU64]
    lib.rpga_lookup.restype, lib.rpga_lookup.argtypes = I, [P, I, C, PU64, PU64]
    lib.rpga_free.restype, lib.rpga_free.argtypes = I, [P, U64]
    lib.rpga_region_size.restype, lib.rpga_region_size.argtypes = I, [P, U64, PU64]
    lib.rpga_register_dev.restype, lib.rpga_register_dev.argtypes = P, [P, P, S]
    lib.rpga_register_host.restype, lib.rpga_register_host.argtypes = P, [P, P, S]
    lib.rpga_deregister.restype, lib.rpga_deregister.argtypes = None, [P]
    lib.rpga_mem_direct.restype, lib.rpga_mem_direct.argtypes = I, [P]
    lib.rpga_read.restype, lib.rpga_read.argtypes = I, [P, U64, P, S, S]
    lib.rpga_write.restype, lib.rpga_write.argtypes = I, [P, U64, P, S, S]
    lib.rpga_mread.restype, lib.rpga_mread.argtypes = I, [P, S, PU64, P, PS, PS, PI]
    lib.rpga_mwrite.restype, lib.rpga_mwrite.argtypes = I, [P, S, PU64, P, PS, PS, PI]
    lib.rpga_stats.restype, lib.rpga_stats.argtypes = None, [P, PU64, PU64, PU64, PU64, PU64]
    _lib = lib
    return lib


def _err(rc: int, what: str) -> OSError:
    e = -rc
    if e == errno.ENOENT:
        return FileNotFoundError(e, f"{what}: no such region")
    if e == errno.EEXIST:
        return FileExistsError(e, f"{what}: the region exists")
    return OSError(e, f"{what}: {os.strerror(e)}")


def _check(rc: int, what: str) -> None:
    if rc:
        raise _err(rc, what)


class GlobalMemory:
    def __init__(self, osts: list[str]):
        self._lib = _load()
        self._h = self._lib.rpga_open(",".join(osts).encode())
        if not self._h:
            raise ValueError("no OSTs")

    def close(self) -> None:
        if self._h:
            self._lib.rpga_close(self._h)
            self._h = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @property
    def osts(self) -> int:
        return self._lib.rpga_osts(self._h)

    def alloc(self, size: int, *, name: str | None = None, ost: int = -1, exclusive: bool = False) -> int:
        a = ctypes.c_uint64()
        _check(self._lib.rpga_alloc(self._h, ost, name.encode() if name else None, size, int(exclusive),
                                    ctypes.byref(a)), f"alloc {name or ''}")
        return a.value

    def lookup(self, name: str, *, ost: int = -1) -> tuple[int, int]:
        a, s = ctypes.c_uint64(), ctypes.c_uint64()
        _check(self._lib.rpga_lookup(self._h, ost, name.encode(), ctypes.byref(a), ctypes.byref(s)), f"lookup {name}")
        return a.value, s.value

    def free(self, addr: int) -> None:
        _check(self._lib.rpga_free(self._h, addr), "free")

    def size(self, addr: int) -> int:
        s = ctypes.c_uint64()
        _check(self._lib.rpga_region_size(self._h, addr, ctypes.byref(s)), "size")
        return s.value

    def register_device(self, ptr: int, nbytes: int) -> "Buffer":
        m = self._lib.rpga_register_dev(self._h, ptr, nbytes)
        if not m:
            raise ValueError("not device memory of the runtime (CUDA, or ROCEPOOL_DEV_EMU=1)")
        return Buffer(self, m, ptr, nbytes)

    def register_host(self, ptr: int, nbytes: int) -> "Buffer":
        m = self._lib.rpga_register_host(self._h, ptr, nbytes)
        if not m:
            raise ValueError("cannot register")
        return Buffer(self, m, ptr, nbytes)

    def stats(self) -> dict:
        v = [ctypes.c_uint64() for _ in range(5)]
        self._lib.rpga_stats(self._h, *[ctypes.byref(x) for x in v])
        return dict(zip(("read_direct", "read_rpc", "write_direct", "write_bounced", "rpcs"), (x.value for x in v)))


class Buffer:
    """GPU or host memory registered with the NIC once"""

    def __init__(self, ga: GlobalMemory, m: int, ptr: int, nbytes: int):
        self._ga, self._m, self.ptr, self.nbytes = ga, m, ptr, nbytes

    @property
    def direct(self) -> bool:
        return bool(self._ga._lib.rpga_mem_direct(self._m))

    def close(self) -> None:
        if self._m:
            self._ga._lib.rpga_deregister(self._m)
            self._m = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def read(self, addr: int, *, offset: int, length: int) -> None:
        _check(self._ga._lib.rpga_read(self._ga._h, addr, self._m, offset, length), "read")

    def write(self, addr: int, *, offset: int, length: int) -> None:
        _check(self._ga._lib.rpga_write(self._ga._h, addr, self._m, offset, length), "write")

    def _many(self, fn, addrs, offsets, lengths) -> list[int]:
        n = len(addrs)
        A = (ctypes.c_uint64 * n)(*addrs)
        O = (ctypes.c_size_t * n)(*offsets)
        L = (ctypes.c_size_t * n)(*lengths)
        st = (ctypes.c_int * n)()
        fn(self._ga._h, n, A, self._m, O, L, st)
        return list(st)

    def mread(self, addrs: list[int], *, offsets: list[int], lengths: list[int]) -> list[int]:
        """status per transfer (0 or -errno)"""
        return self._many(self._ga._lib.rpga_mread, addrs, offsets, lengths)

    def mwrite(self, addrs: list[int], *, offsets: list[int], lengths: list[int]) -> list[int]:
        return self._many(self._ga._lib.rpga_mwrite, addrs, offsets, lengths)
