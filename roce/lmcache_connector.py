"""RoCEPool as a KV-cache store for inference engines, through LMCache.

vLLM and SGLang offload KV cache through LMCache. LMCache's storage tier
("L2") takes external backends in two ways, and this module serves both:

``native_plugin`` -- LMCache loads a connector object exposing
``event_fd / submit_batch_get / submit_batch_set / submit_batch_exists /
submit_batch_delete / drain_completions / close``:

    {"type": "native_plugin",
     "module_path": "roce.lmcache_connector",
     "class_name": "RoCEPoolConnector",
     "adapter_params": {"osts": "10.0.0.1:19951,10.0.0.2:19952"}}

``plugin`` (recommended) -- the same connector inside LMCache's own
native-connector adapter, plus zero copy: LMCache hands the adapter its
pinned L1 arena, which is registered with the NIC once, so every load is a
one-sided RDMA READ straight into the arena and every store an RDMA READ by
the OST out of it (no CPU copy on either side):

    {"type": "plugin",
     "module_path": "roce.lmcache_connector",
     "class_name": "RoCEPoolL2Adapter",
     "adapter_params": {"osts": "10.0.0.1:19951,10.0.0.2:19952",
                        "max_capacity_gb": 0}}

adapter_params: ``osts`` (default: env ROCEPOOL_OSTS), ``namespace``
(default "lmcache/"), ``workers`` (operations in flight, default 4),
``part_mb`` (largest part a chunk is split into, default 4).

The data path is C (roce/native/rocepool_lmc.c over the KV client); this
module only passes pointers and collects completions.
"""
from __future__ import annotations

import ctypes
import errno
import os
import threading
from typing import Any, Sequence

OP_GET, OP_SET, OP_EXISTS, OP_DELETE = 0, 1, 2, 3

_lib = None


def _load():
    global _lib
    if _lib is not None:
        return _lib
    from roce.kv import _load as kv_load  # same library: librocepool_kv.so

    lib = kv_load()
    P, S, C = ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p
    lib.rplmc_open.restype, lib.rplmc_open.argtypes = P, [C, C, ctypes.c_int, S]
    lib.rplmc_close.restype, lib.rplmc_close.argtypes = None, [P]
    lib.rplmc_event_fd.restype, lib.rplmc_event_fd.argtypes = ctypes.c_int, [P]
    lib.rplmc_ack.restype, lib.rplmc_ack.argtypes = None, [P]
    lib.rplmc_register.restype, lib.rplmc_register.argtypes = ctypes.c_int, [P, P, S]
    lib.rplmc_submit.restype = ctypes.c_int64
    lib.rplmc_submit.argtypes = [P, ctypes.c_int, S, ctypes.POINTER(C), ctypes.POINTER(P), ctypes.POINTER(S)]
    lib.rplmc_pop.restype = ctypes.c_int
    lib.rplmc_pop.argtypes = [P, ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_int),
                              ctypes.POINTER(ctypes.c_int), ctypes.POINTER(S), ctypes.POINTER(ctypes.c_uint8), S,
                              C, S]
    lib.rplmc_stats.restype, lib.rplmc_stats.argtypes = None, [P, ctypes.POINTER(ctypes.c_uint64)]
    _lib = lib
    return lib


def _pin(buf: Any, writable: bool) -> tuple[int, int, Any]:
    """(address, length, keep-alive) of a contiguous byte buffer. The
    keep-alive object must outlive the operation."""
    mv = memoryview(buf)
    if not mv.contiguous:
        raise ValueError("buffer is not contiguous")
    mv = mv.cast("B") if mv.format != "B" or mv.ndim != 1 else mv
    n = mv.nbytes
    if n == 0:
        return 0, 0, mv
    if mv.readonly:
        if writable:
            raise ValueError("read-only buffer given as a load target")
        copy = (ctypes.c_char * n).from_buffer_copy(mv)  # stores only: send a copy
        return ctypes.addressof(copy), n, copy
    arr = (ctypes.c_char * n).from_buffer(mv)
    return ctypes.addressof(arr), n, (arr, mv)


class RoCEPoolConnector:
    """LMCache native-connector contract over the RoCEPool KV store."""

    def __init__(self, osts: str | Sequence[str] | None = None, namespace: str = "lmcache/", workers: int = 4,
                 part_mb: float = 4, **_ignored: Any) -> None:
        if osts is None:
            osts = os.environ.get("ROCEPOOL_OSTS", "")
        if not isinstance(osts, str):
            osts = ",".join(osts)
        if not osts:
            raise ValueError("RoCEPoolConnector: no OSTs (adapter_params 'osts' or env ROCEPOOL_OSTS)")
        self._lib = _load()
        self._h = self._lib.rplmc_open(osts.encode(), namespace.encode(), int(workers),
                                       int(float(part_mb) * (1 << 20)))
        if not self._h:
            raise ValueError(f"RoCEPoolConnector: cannot open {osts!r}")
        self._fd = int(self._lib.rplmc_event_fd(self._h))
        self._lock = threading.Lock()
        self._pending: dict[int, Any] = {}   # future id -> buffers kept alive
        self._cap = 1024
        self._res = (ctypes.c_uint8 * self._cap)()
        self._err = ctypes.create_string_buffer(256)
        self.osts = osts
        self.namespace = namespace

    # -- registration --------------------------------------------------------
    def register_region(self, ptr: int, size: int) -> bool:
        """Register [ptr, ptr+size) for zero-copy transfers. True: the NIC
        reaches it; False: accepted, its transfers are copied."""
        rc = self._lib.rplmc_register(self._h, ctypes.c_void_p(int(ptr)), int(size))
        if rc < 0:
            raise OSError(-rc, f"register_region: {os.strerror(-rc)}")
        return rc == 1

    # -- contract ------------------------------------------------------------
    def event_fd(self) -> int:
        return self._fd

    def _submit(self, op: int, keys: Sequence[str], bufs: Sequence[Any] | None) -> int:
        if self._h is None:
            raise RuntimeError("RoCEPoolConnector is closed")
        n = len(keys)
        if bufs is not None and len(bufs) != n:
            raise ValueError("keys and buffers differ in length")
        karr = (ctypes.c_char_p * max(n, 1))(*[k.encode() if isinstance(k, str) else bytes(k) for k in keys])
        keep: list[Any] = [karr]
        parr = larr = None
        if bufs is not None:
            parr = (ctypes.c_void_p * max(n, 1))()
            larr = (ctypes.c_size_t * max(n, 1))()
            for i, b in enumerate(bufs):
                addr, ln, alive = _pin(b, writable=(op == OP_GET))
                parr[i], larr[i] = addr, ln
                keep.append(alive)
            keep += [parr, larr]
        with self._lock:
            fid = int(self._lib.rplmc_submit(self._h, op, n, karr, parr, larr))
            if fid < 0:
                raise OSError(-fid, f"submit: {os.strerror(-fid)}")
            self._pending[fid] = keep
        return fid

    def submit_batch_get(self, keys: Sequence[str], memoryviews: Sequence[Any]) -> int:
        return self._submit(OP_GET, keys, memoryviews)

    def submit_batch_set(self, keys: Sequence[str], memoryviews: Sequence[Any]) -> int:
        return self._submit(OP_SET, keys, memoryviews)

    def submit_batch_exists(self, keys: Sequence[str]) -> int:
        return self._submit(OP_EXISTS, keys, None)

    def submit_batch_delete(self, keys: Sequence[str]) -> int:
        return self._submit(OP_DELETE, keys, None)

    def drain_completions(self) -> list[tuple[int, bool, str, list[bool] | None]]:
        """[(future id, ok, error, per-key results)] of everything finished."""
        out: list[tuple[int, bool, str, list[bool] | None]] = []
        if self._h is None:
            return out
        self._lib.rplmc_ack(self._h)
        fid, op, ok, n = ctypes.c_uint64(), ctypes.c_int(), ctypes.c_int(), ctypes.c_size_t()
        while True:
            rc = self._lib.rplmc_pop(self._h, ctypes.byref(fid), ctypes.byref(op), ctypes.byref(ok),
                                     ctypes.byref(n), self._res, self._cap, self._err, len(self._err))
            if rc == -errno.ENOSPC:
                self._cap = max(n.value, 2 * self._cap)
                self._res = (ctypes.c_uint8 * self._cap)()
                continue
            if rc <= 0:
                break
            results = [bool(self._res[i]) for i in range(n.value)]
            with self._lock:
                self._pending.pop(fid.value, None)
            out.append((fid.value, bool(ok.value), self._err.value.decode(errors="replace"), results))
        return out

    def stats(self) -> dict[str, int]:
        v = (ctypes.c_uint64 * 8)()
        self._lib.rplmc_stats(self._h, v)
        names = ("ops", "keys", "bytes_stored", "bytes_loaded", "bytes_zero_copy", "bytes_copied",
                 "parts_failed", "regions")
        return dict(zip(names, list(v)))

    def close(self) -> None:
        if self._h is not None:
            self._lib.rplmc_close(self._h)  # finishes queued operations first
            self._h = None
            self._pending.clear()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


# -- LMCache "plugin" adapter (zero copy into LMCache's L1 arena) -------------
try:
    from lmcache.v1.distributed.api import MemoryResidency, MemorySegment, MemoryTags
    from lmcache.v1.distributed.l2_adapters.native_connector_l2_adapter import NativeConnectorL2Adapter
except ImportError:  # LMCache not installed: the connector above still works
    NativeConnectorL2Adapter = None

if NativeConnectorL2Adapter is not None:

    class RoCEPoolL2Adapter(NativeConnectorL2Adapter):  # type: ignore[misc, valid-type]
        """LMCache's native-connector adapter over RoCEPoolConnector, with
        the L1 arena registered for zero-copy transfers."""

        def __init__(self, params: dict | None = None, l1_memory_desc: Any = None, **_kw: Any) -> None:
            params = dict(params or {})
            cap_gb = float(params.pop("max_capacity_gb", 0) or 0)
            client = RoCEPoolConnector(**params)
            zero_copy = False
            if l1_memory_desc is not None:
                size = getattr(l1_memory_desc, "stable_registration_size", None) or l1_memory_desc.size
                zero_copy = client.register_region(l1_memory_desc.ptr, size)
            super().__init__(client, MemoryTags(MemorySegment.STORAGE, MemoryResidency.REMOTE),
                             max_capacity_gb=cap_gb, type_name="rocepool",
                             extra_status={"osts": client.osts, "namespace": client.namespace,
                                           "zero_copy_l1": zero_copy})
            self.rocepool = client

        def report_status(self) -> dict[str, Any]:
            """LMCache's status (GET /status on its HTTP server) with the
            connector's live counters under "rocepool": bytes stored and
            loaded, bytes moved zero-copy and bytes copied."""
            status = super().report_status()
            try:
                status["rocepool"] = self.rocepool.stats()
            except Exception:  # closed
                pass
            return status
