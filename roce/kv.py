"""Python binding for the native KV client API (roce/native/rocepool_kv.h).

    from roce.kv import KVClient
    kv = KVClient(["10.0.0.1:19951", "10.0.0.2:19952"])      # the OST list
    kv.put("user:42", b"...")
    kv.get("user:42")                    -> bytes          (KeyError if absent)
    kv.get_versioned("user:42")          -> (version, bytes)
    kv.put_if("user:42", b"...", expected_version=3)       (VersionConflict)
    kv.mget(["a", "b", "c"])             -> [bytes | None, ...]   one frame per OST
    kv.mput({"a": b"1", "b": b"2"})
    kv.write_at / read_at / delete / mdelete

GPU memory (KV-cache blocks straight between the OSTs and the GPU, no host copy):

    buf = kv.register_device(tensor.data_ptr(), tensor.numel() * tensor.element_size())
    buf.mput(["p7/b0", "p7/b1"], offsets=[0, blk], lengths=[blk, blk])   # prefill: store
    n = kv.cached_prefix(["p7/b0", "p7/b1", "p7/b2"])  -> 2                # how many leading blocks exist
    buf.mget(["p7/b0", "p7/b1"], offsets=[0, blk], caps=[blk, blk])      # decode / reuse: load
    buf.direct                           -> True when the NIC writes into the GPU itself

Every call goes straight from C to the owning OST (one round trip; the
batched calls one frame per OST, all OSTs in parallel) -- no FUSE, no
kernel crossing, no Python in the data path beyond the call itself.
Library: env ROCEPOOL_KV_LIB, else roce/native/librocepool_kv.so (real
rdma-core build, roce/native/build.sh), else the emulator build.
"""
from __future__ import annotations

import ctypes
import errno
import os

from storage_engine.base import VersionConflict

_lib = None


def _load():
    global _lib
    if _lib is not None:
        return _lib
    path = os.environ.get("ROCEPOOL_KV_LIB")
    if not path:
        real = os.path.join(os.path.dirname(os.path.abspath(__file__)), "native", "librocepool_kv.so")
        if os.path.exists(real):
            path = real
        else:
            from roce.native.build_lib import ensure_kv_lib
            path = ensure_kv_lib()[0]
    lib = ctypes.CDLL(path)
    P, S, U64, C = ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint64, ctypes.c_char_p
    PP, PS, PU64 = ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_uint64)
    lib.rpkv_open.restype, lib.rpkv_open.argtypes = P, [C, C]
    lib.rpkv_close.restype, lib.rpkv_close.argtypes = None, [P]
    lib.rpkv_free.restype, lib.rpkv_free.argtypes = None, [P]
    lib.rpkv_rpcs.restype, lib.rpkv_rpcs.argtypes = U64, [P]
    lib.rpkv_put.argtypes = [P, C, C, S, PU64]
    lib.rpkv_put_if.argtypes = [P, C, C, S, U64, PU64]
    lib.rpkv_get.argtypes = [P, C, PP, PS, PU64]
    lib.rpkv_delete.argtypes = [P, C]
    lib.rpkv_write_at.argtypes = [P, C, U64, C, S]
    lib.rpkv_read_at.argtypes = [P, C, U64, S, PP, PS]
    lib.rpkv_mput.argtypes = [P, S, ctypes.POINTER(C), ctypes.POINTER(C), PS, ctypes.POINTER(ctypes.c_int)]
    lib.rpkv_mget.argtypes = [P, S, ctypes.POINTER(C), PP, PS, ctypes.POINTER(ctypes.c_int)]
    lib.rpkv_mdelete.argtypes = [P, S, ctypes.POINTER(C), ctypes.POINTER(ctypes.c_int)]
    PI = ctypes.POINTER(ctypes.c_int)
    lib.rpkv_dev_register.restype, lib.rpkv_dev_register.argtypes = P, [P, P, S]
    lib.rpkv_dev_deregister.restype, lib.rpkv_dev_deregister.argtypes = None, [P]
    lib.rpkv_dev_direct.restype, lib.rpkv_dev_direct.argtypes = ctypes.c_int, [P]
    lib.rpkv_put_dev.argtypes = [P, C, P, S, S, PU64]
    lib.rpkv_get_dev.argtypes = [P, C, P, S, S, PS]
    lib.rpkv_mput_dev.argtypes = [P, S, ctypes.POINTER(C), P, PS, PS, PI]
    lib.rpkv_mget_dev.argtypes = [P, S, ctypes.POINTER(C), P, PS, PS, PS, PI]
    lib.rpkv_mcontains.argtypes = [P, S, ctypes.POINTER(C), PI]
    lib.rpkv_dev_stats.restype, lib.rpkv_dev_stats.argtypes = None, [P, PU64, PU64, PU64, PU64]
    lib.rp_devemu_alloc.restype, lib.rp_devemu_alloc.argtypes = P, [S]
    lib.rp_devemu_free.restype, lib.rp_devemu_free.argtypes = None, [P]
    lib.rp_devemu_copy_to.restype, lib.rp_devemu_copy_to.argtypes = ctypes.c_int, [P, C, S]
    lib.rp_devemu_copy_from.restype, lib.rp_devemu_copy_from.argtypes = ctypes.c_int, [P, P, S]
    for f in ("rpkv_put", "rpkv_put_if", "rpkv_get", "rpkv_delete", "rpkv_write_at", "rpkv_read_at",
              "rpkv_mput", "rpkv_mget", "rpkv_mdelete", "rpkv_put_dev", "rpkv_get_dev", "rpkv_mput_dev",
              "rpkv_mget_dev", "rpkv_mcontains"):
        getattr(lib, f).restype = ctypes.c_int
    _lib = lib
    return lib


def _check(rc: int, key: str, expected: int | None = None) -> None:
    if rc == 0:
        return
    if rc == -errno.ENOENT:
        raise KeyError(key)
    if rc == -errno.EAGAIN:
        raise VersionConflict(key, expected if expected is not None else -1, -1)
    raise OSError(-rc, f"{os.strerror(-rc)} ({key})")


class KVClient:
    def __init__(self, osts: list[str], namespace: str = "kv/"):
        self._lib = _load()
        self._h = self._lib.rpkv_open(",".join(osts).encode(), namespace.encode())
        if not self._h:
            raise ValueError("rpkv_open failed (empty OST list?)")

    def close(self) -> None:
        if self._h:
            self._lib.rpkv_close(self._h)
            self._h = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @property
    def rpcs(self) -> int:
        return self._lib.rpkv_rpcs(self._h)

    def put(self, key: str, value: bytes) -> int:
        v = ctypes.c_uint64()
        _check(self._lib.rpkv_put(self._h, key.encode(), value, len(value), ctypes.byref(v)), key)
        return v.value

    def put_if(self, key: str, value: bytes, *, expected_version: int) -> int:
        v = ctypes.c_uint64()
        _check(self._lib.rpkv_put_if(self._h, key.encode(), value, len(value), expected_version, ctypes.byref(v)),
               key, expected_version)
        return v.value

    def get_versioned(self, key: str) -> tuple[int, bytes]:
        p, n, v = ctypes.c_void_p(), ctypes.c_size_t(), ctypes.c_uint64()
        _check(self._lib.rpkv_get(self._h, key.encode(), ctypes.byref(p), ctypes.byref(n), ctypes.byref(v)), key)
        try:
            return v.value, ctypes.string_at(p, n.value)
        finally:
            self._lib.rpkv_free(p)

    def get(self, key: str) -> bytes:
        return self.get_versioned(key)[1]

    def delete(self, key: str) -> None:
        _check(self._lib.rpkv_delete(self._h, key.encode()), key)

    def write_at(self, key: str, offset: int, data: bytes) -> None:
        _check(self._lib.rpkv_write_at(self._h, key.encode(), offset, data, len(data)), key)

    def read_at(self, key: str, offset: int, length: int) -> bytes:
        p, n = ctypes.c_void_p(), ctypes.c_size_t()
        _check(self._lib.rpkv_read_at(self._h, key.encode(), offset, length, ctypes.byref(p), ctypes.byref(n)), key)
        try:
            return ctypes.string_at(p, n.value)
        finally:
            self._lib.rpkv_free(p)

    @staticmethod
    def _keys(keys):
        arr = (ctypes.c_char_p * len(keys))(*[k.encode() for k in keys])
        return arr, (ctypes.c_int * len(keys))()

    def mput(self, items: dict[str, bytes]) -> None:
        keys = list(items)
        karr, st = self._keys(keys)
        vals = (ctypes.c_char_p * len(keys))(*[items[k] for k in keys])
        lens = (ctypes.c_size_t * len(keys))(*[len(items[k]) for k in keys])
        _check(self._lib.rpkv_mput(self._h, len(keys), karr, vals, lens, st), "mput")
        for k, s in zip(keys, st):
            _check(s, k)

    def mget(self, keys: list[str]) -> list[bytes | None]:
        karr, st = self._keys(keys)
        vals = (ctypes.c_void_p * len(keys))()
        lens = (ctypes.c_size_t * len(keys))()
        _check(self._lib.rpkv_mget(self._h, len(keys), karr, vals, lens, st), "mget")
        out = []
        for i, k in enumerate(keys):
            if st[i] == -errno.ENOENT:
                out.append(None)
                continue
            _check(st[i], k)
            out.append(ctypes.string_at(vals[i], lens[i]))
            self._lib.rpkv_free(vals[i])
        return out

    def mdelete(self, keys: list[str]) -> list[bool]:
        karr, st = self._keys(keys)
        _check(self._lib.rpkv_mdelete(self._h, len(keys), karr, st), "mdelete")
        return [s == 0 for s in st]

    # ---- GPU memory -------------------------------------------------------
    def register_device(self, ptr: int, nbytes: int) -> "DeviceBuffer":
        """GPU memory at device pointer `ptr` (e.g. tensor.data_ptr()) for the
        *_dev transfers; the whole allocation is registered with the NIC once."""
        return DeviceBuffer(self, ptr, nbytes)

    def contains(self, keys: list[str]) -> list[bool]:
        """one BATCH round trip per OST; also caches the keys' leases, so a
        DeviceBuffer.mget right after moves data only"""
        karr, st = self._keys(keys)
        _check(self._lib.rpkv_mcontains(self._h, len(keys), karr, st), "contains")
        return [bool(x) for x in st]

    def cached_prefix(self, block_keys: list[str]) -> int:
        """how many leading blocks of a prompt are stored"""
        n = 0
        for present in self.contains(block_keys):
            if not present:
                break
            n += 1
        return n

    def dev_stats(self) -> dict:
        v = [ctypes.c_uint64() for _ in range(4)]
        self._lib.rpkv_dev_stats(self._h, *[ctypes.byref(x) for x in v])
        return dict(zip(("direct_bytes", "bounced_bytes", "leases", "lease_hits"), (x.value for x in v)))


class DeviceBuffer:
    """A registered GPU allocation (KVClient.register_device). Offsets are
    bytes from the pointer it was registered with."""

    def __init__(self, client: KVClient, ptr: int, nbytes: int):
        self._c, self._lib = client, client._lib
        self.ptr, self.nbytes = ptr, nbytes
        self._h = self._lib.rpkv_dev_register(client._h, ptr, nbytes)
        if not self._h:
            raise ValueError(f"0x{ptr:x}: not device memory of the CUDA runtime (or ROCEPOOL_DEV_EMU=1 emulator)")

    @property
    def direct(self) -> bool:
        return bool(self._lib.rpkv_dev_direct(self._h))

    def close(self) -> None:
        if self._h:
            self._lib.rpkv_dev_deregister(self._h)
            self._h = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def put(self, key: str, offset: int, length: int) -> int:
        v = ctypes.c_uint64()
        _check(self._lib.rpkv_put_dev(self._c._h, key.encode(), self._h, offset, length, ctypes.byref(v)), key)
        return v.value

    def get(self, key: str, offset: int, cap: int) -> int:
        """the value into the buffer at offset; its length (KeyError if absent)"""
        n = ctypes.c_size_t()
        _check(self._lib.rpkv_get_dev(self._c._h, key.encode(), self._h, offset, cap, ctypes.byref(n)), key)
        return n.value

    def mput(self, keys: list[str], offsets: list[int], lengths: list[int]) -> None:
        karr, st = KVClient._keys(keys)
        n = len(keys)
        offs, lens = (ctypes.c_size_t * n)(*offsets), (ctypes.c_size_t * n)(*lengths)
        _check(self._lib.rpkv_mput_dev(self._c._h, n, karr, self._h, offs, lens, st), "mput")
        for k, s in zip(keys, st):
            _check(s, k)

    def mget(self, keys: list[str], offsets: list[int], caps: list[int]) -> list[int | None]:
        """each value into the buffer at its offset; lengths (None: absent)"""
        karr, st = KVClient._keys(keys)
        n = len(keys)
        offs, caps_ = (ctypes.c_size_t * n)(*offsets), (ctypes.c_size_t * n)(*caps)
        lens = (ctypes.c_size_t * n)()
        _check(self._lib.rpkv_mget_dev(self._c._h, n, karr, self._h, offs, caps_, lens, st), "mget")
        out = []
        for i, k in enumerate(keys):
            if st[i] == -errno.ENOENT:
                out.append(None)
                continue
            _check(st[i], k)
            out.append(lens[i])
        return out


class EmulatedDevice:
    """The emulated GPU of the library (ROCEPOOL_DEV_EMU=1; tests and
    benchmarks without a GPU): memory the CPU cannot touch -- a stray CPU
    access faults -- that the RDMA emulator reaches as a NIC reaches a GPU."""

    def __init__(self):
        self._lib = _load()

    def alloc(self, nbytes: int) -> int:
        p = self._lib.rp_devemu_alloc(nbytes)
        if not p:
            raise MemoryError(nbytes)
        return p

    def free(self, ptr: int) -> None:
        self._lib.rp_devemu_free(ptr)

    def write(self, ptr: int, data: bytes) -> None:
        if self._lib.rp_devemu_copy_to(ptr, data, len(data)):
            raise ValueError("outside an emulated allocation")

    def read(self, ptr: int, n: int) -> bytes:
        buf = ctypes.create_string_buffer(n)
        if self._lib.rp_devemu_copy_from(buf, ptr, n):
            raise ValueError("outside an emulated allocation")
        return buf.raw
