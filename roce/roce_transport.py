"""RoCE (RDMA over Converged Ethernet) transport -- ctypes binding over the
compiled roce/native/libroce_transport.so.

*** TESTING STATUS: run over an RC emulator, NOT over RDMA hardware. ***

What HAS run: this module plus roce/native/roce_transport.c (unmodified),
built against softverbs (roce/native/softverbs/) -- a userspace model of
RC queue-pair semantics that enforces the rules real hardware enforces
(ACK-gated SEND completion, RNR NAK + requester retry, RECV length and
lkey checks, flush-on-error) -- between two network nodes joined by a
back-to-back veth link (tools/run_crossover_rdma.py): the smoke test,
a 3-node PoolNode cluster, a 4-rank MPI communicator and a pipelined
RoCEStream, all passing. tests/test_softverbs_rc_semantics.py also checks
the emulator FAILS what hardware fails. That run found three real bugs in
this binding and the C beneath it -- see docs/CROSSOVER_RDMA_TEST.md.

What has NOT run: a real RDMA NIC, kernel verbs, Soft-RoCE (rdma_rxe),
RoCEv2 on the wire, or any real RDMA performance measurement. None of
those exist in the environment this was written in (no /dev/infiniband,
no rdma_rxe module, no rdma-core packages -- confirmed directly). Follow
docs/ROCE_DEPLOYMENT_NOTES.md on real hardware before trusting it with
real data.
"""
from __future__ import annotations

import ctypes
import os
import threading
import time
from pathlib import Path

from roce.protocol import Frame, ProtocolError
from roce.transport_base import PeerUnreachable, RequestHandler, ServerHandle, Transport

# 16MB default; must match on both ends of a connection. Overridable via
# ROCE_MAX_FRAME_BYTES for deployments that store single objects bigger than
# that over PoolNode's whole-object put/get (PoolNode has no chunking of its
# own -- fs/striping.py's stripe objects are how a large FILE normally stays
# under this, but a flat PoolNode-backed mount, e.g. tools/bench_ior.py's
# "--backend pool", has no striping layer under it, so a single write_range
# flush of a file bigger than the frame can legitimately need a larger cap).
_DEFAULT_MAX_FRAME_BYTES = int(os.environ.get("ROCE_MAX_FRAME_BYTES", 16 * 1024 * 1024))


class RoCENotAvailable(Exception):
    """Raised when the native library can't be loaded -- e.g. it hasn't
    been built yet, or this machine has no RDMA support. Distinguished
    from PeerUnreachable/TransportError so callers can tell "this
    transport isn't usable on this machine at all" apart from "a specific
    peer is down"."""


def _load_library() -> ctypes.CDLL:
    env_path = os.environ.get("ROCE_TRANSPORT_LIB")
    candidates = [env_path] if env_path else []
    candidates.append(str(Path(__file__).resolve().parent / "native" / "libroce_transport.so"))
    last_error = None
    for candidate in candidates:
        if not candidate or not Path(candidate).is_file():
            last_error = FileNotFoundError(candidate)
            continue
        try:
            return ctypes.CDLL(candidate)
        except OSError as exc:  # e.g. missing librdmacm.so.1 at runtime
            last_error = exc
    raise RoCENotAvailable(
        "could not load libroce_transport.so "
        f"(tried: {candidates}; last error: {last_error}). "
        "Build it first: roce/native/build.sh (requires rdma-core-dev "
        "headers and, to actually work, real or Soft-RoCE RDMA hardware). "
        "It has never run on RDMA hardware -- see this module's docstring."
    )


class _Lib:
    """Lazily-loaded, process-wide handle with argtypes/restype declared
    to exactly match roce_transport.h -- this part (unlike struct layouts)
    is simple enough to get right without the real headers in hand, since
    it's just scalar/pointer argument shapes.
    """
    _lib: ctypes.CDLL | None = None
    _lock = threading.Lock()

    @classmethod
    def get(cls) -> ctypes.CDLL:
        with cls._lock:
            if cls._lib is None:
                lib = _load_library()
                lib.roce_last_error.restype = ctypes.c_char_p
                lib.roce_last_error.argtypes = []

                lib.roce_client_connect.restype = ctypes.c_void_p
                lib.roce_client_connect.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_size_t]

                lib.roce_client_send_request.restype = ctypes.c_int
                lib.roce_client_send_request.argtypes = [
                    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t,
                    ctypes.POINTER(ctypes.POINTER(ctypes.c_uint8)), ctypes.POINTER(ctypes.c_size_t),
                    ctypes.c_int,
                ]

                lib.roce_client_close.restype = None
                lib.roce_client_close.argtypes = [ctypes.c_void_p]

                lib.roce_client_stream_send.restype = ctypes.c_int
                lib.roce_client_stream_send.argtypes = [
                    ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_int,
                ]

                lib.roce_client_stream_recv.restype = ctypes.c_int
                lib.roce_client_stream_recv.argtypes = [
                    ctypes.c_void_p,
                    ctypes.POINTER(ctypes.POINTER(ctypes.c_uint8)), ctypes.POINTER(ctypes.c_size_t),
                    ctypes.c_int,
                ]

                # Return type is c_void_p, not POINTER(c_uint8): ctypes rejects
                # pointer-typed callback results outright ("invalid result
                # type for callback function") -- found the first time this
                # ever ran, over softverbs (see roce/native/softverbs/).
                HANDLER_CFUNC = ctypes.CFUNCTYPE(
                    ctypes.c_void_p,  # returns malloc()'d response buffer address
                    ctypes.POINTER(ctypes.c_uint8), ctypes.c_size_t,  # request, request_len
                    ctypes.POINTER(ctypes.c_size_t),  # *response_len out
                    ctypes.c_void_p,  # user_data
                )
                cls._HANDLER_CFUNC = HANDLER_CFUNC

                lib.roce_server_start.restype = ctypes.c_void_p
                lib.roce_server_start.argtypes = [ctypes.c_char_p, HANDLER_CFUNC, ctypes.c_void_p, ctypes.c_size_t]

                lib.roce_server_local_address.restype = ctypes.c_int
                lib.roce_server_local_address.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]

                lib.roce_server_stop.restype = None
                lib.roce_server_stop.argtypes = [ctypes.c_void_p]

                # tier-B additions; optional so an older library still loads
                if hasattr(lib, "roce_client_connect_shared"):
                    lib.roce_client_connect_shared.restype = ctypes.c_void_p
                    lib.roce_client_connect_shared.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_size_t]
                if hasattr(lib, "roce_client_is_alive"):
                    lib.roce_client_is_alive.restype = ctypes.c_int
                    lib.roce_client_is_alive.argtypes = [ctypes.c_void_p]
                if hasattr(lib, "roce_bufpool_stats"):
                    lib.roce_bufpool_stats.restype = None
                    lib.roce_bufpool_stats.argtypes = [ctypes.POINTER(ctypes.c_uint64), ctypes.POINTER(ctypes.c_uint32),
                                                       ctypes.POINTER(ctypes.c_uint32)]

                # native C OST/MDT servers (roce/native/rocepool_servers.c);
                # optional so an older library without them still loads
                if hasattr(lib, "rocepool_native_start"):
                    lib.rocepool_native_start.restype = ctypes.c_void_p
                    lib.rocepool_native_start.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_size_t]
                    lib.rocepool_native_local_address.restype = ctypes.c_int
                    lib.rocepool_native_local_address.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
                    lib.rocepool_native_stats.restype = None
                    lib.rocepool_native_stats.argtypes = [ctypes.c_void_p] + [ctypes.POINTER(ctypes.c_uint64)] * 3
                    lib.rocepool_native_stop.restype = None
                    lib.rocepool_native_stop.argtypes = [ctypes.c_void_p]

                cls._lib = lib
            return cls._lib

    @classmethod
    def handler_cfunctype(cls):
        cls.get()  # ensure initialized
        return cls._HANDLER_CFUNC


_LIBC: ctypes.CDLL | None = None


def _libc() -> ctypes.CDLL:
    """libc with malloc/free prototypes declared. Without restype=c_void_p,
    ctypes assumes malloc returns a C int and silently truncates the 64-bit
    pointer -- the original binding called CDLL(None).malloc() exactly that
    way."""
    global _LIBC
    if _LIBC is None:
        libc = ctypes.CDLL(None)
        libc.malloc.restype = ctypes.c_void_p
        libc.malloc.argtypes = [ctypes.c_size_t]
        libc.free.restype = None
        libc.free.argtypes = [ctypes.c_void_p]
        _LIBC = libc
    return _LIBC


def _last_error(lib: ctypes.CDLL) -> str:
    raw = lib.roce_last_error()
    return raw.decode("utf-8", "replace") if raw else "(no error detail)"


class _RoCEServerHandle(ServerHandle):
    def __init__(self, lib: ctypes.CDLL, server_ptr: int, address: str, keepalive):
        self._lib = lib
        self._server_ptr = server_ptr
        self._address = address
        self._keepalive = keepalive  # keeps the ctypes CFUNCTYPE callback alive
        # ...and so does this registry, until stop(): a caller that drops the
        # handle of a running server (fire-and-forget serve()) must not free
        # the callback its connection threads are still calling.
        _RoCEServerHandle._running[id(self)] = keepalive
        self._stopped = False

    _running: dict = {}

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        # roce_server_stop() returns only after every connection thread has
        # exited (bounded drain), so the callback can be released after it.
        self._lib.roce_server_stop(self._server_ptr)
        _RoCEServerHandle._running.pop(id(self), None)

    def local_address(self) -> str:
        return self._address


_ROCE_ERR_NOT_DELIVERED = -2  # roce_transport.h ROCE_ERR_NOT_DELIVERED
# ROCE_TRACE_SLOW_MS=<ms>: log every request slower than that (and every
# failed attempt) to stderr -- how an intermittent stall gets attributed.
_TRACE_SLOW_MS = float(os.environ.get("ROCE_TRACE_SLOW_MS", "0") or 0)


def _trace_slow(peer: str, frame: Frame, t0: float, trace: list, force: bool = False) -> None:
    ms = 1000 * (time.monotonic() - t0)
    if force or ms >= _TRACE_SLOW_MS:
        import sys
        print(f"[roce-slow] {time.time():.3f} pid={os.getpid()} tid={threading.get_ident()} peer={peer} "
              f"op={frame.op.name} req={frame.request_id} key={frame.key[:40]!r} {ms:.1f}ms :: {' | '.join(trace)}",
              file=sys.stderr, flush=True)


class _ConnPool:
    """Idle client connections per peer, reused LIFO (the most recently used
    connection is the one most likely still warm). A connection is only
    ever in the pool while nobody is using it, so one connection never
    carries two requests at once -- the C side's per-connection buffers are
    single-request scratch space. Connections idle longer than
    `idle_close_s` are closed by the client (the server never closes an
    idle connection on its own, so it can never drop a request that is
    already on the wire)."""

    def __init__(self, lib, *, max_idle: int | None = None, idle_close_s: float | None = None):
        self._lib = lib
        self.max_idle = max_idle if max_idle is not None else int(os.environ.get("ROCE_POOL_MAX_IDLE", "8"))
        self.idle_close_s = idle_close_s if idle_close_s is not None else float(os.environ.get("ROCE_POOL_IDLE_S", "30"))
        self._idle: dict[str, list[tuple[int, float]]] = {}
        self._lock = threading.Lock()

    def checkout(self, peer: str):
        now = time.monotonic()
        stale = []
        conn = None
        with self._lock:
            lst = self._idle.get(peer)
            while lst:
                c, t = lst.pop()
                if now - t > self.idle_close_s:
                    stale.append(c)
                    continue
                conn = c
                break
        for c in stale:
            self._lib.roce_client_close(c)
        return conn

    def checkin(self, peer: str, conn) -> None:
        with self._lock:
            lst = self._idle.setdefault(peer, [])
            if len(lst) < self.max_idle:
                lst.append((conn, time.monotonic()))
                return
        self._lib.roce_client_close(conn)

    def close_all(self) -> None:
        with self._lock:
            conns = [c for lst in self._idle.values() for c, _ in lst]
            self._idle.clear()
        for c in conns:
            self._lib.roce_client_close(c)


class NativeServerHandle(ServerHandle):
    """A server implemented entirely in C (roce/native/rocepool_servers.c):
    requests never enter Python, so it is not bound to one core by the GIL."""

    KINDS = {"ost": 1, "mdt": 2}

    def __init__(self, lib: ctypes.CDLL, ptr: int, address: str, kind: str):
        self._lib, self._ptr, self._address, self.kind = lib, ptr, address, kind

    def stop(self) -> None:
        self._lib.rocepool_native_stop(self._ptr)

    def local_address(self) -> str:
        return self._address

    def stats(self) -> dict:
        objs, byts, ops = ctypes.c_uint64(), ctypes.c_uint64(), ctypes.c_uint64()
        self._lib.rocepool_native_stats(self._ptr, ctypes.byref(objs), ctypes.byref(byts), ctypes.byref(ops))
        return {"objects": objs.value, "bytes": byts.value, "ops": ops.value}


class RoCETransport(Transport):
    """Implements the same Transport interface as LoopbackTransport, over
    real RDMA. See this module's docstring for testing status before use.
    """

    def __init__(self, *, max_frame_bytes: int = _DEFAULT_MAX_FRAME_BYTES, timeout_ms: int = 5000,
                 pool: bool | None = None):
        self._lib = _Lib.get()
        self.max_frame_bytes = max_frame_bytes
        self.timeout_ms = timeout_ms
        if pool is None:
            pool = os.environ.get("ROCE_CONN_POOL", "1") != "0"
        self._pool = _ConnPool(self._lib) if pool else None
        self.connects = 0  # connections opened by this transport (tests/benchmarks read it)

    def _connect(self, peer: str):
        # request/response connections borrow frame buffers from the
        # process-wide registered pool instead of owning 2 x max_frame_bytes
        connect = getattr(self._lib, "roce_client_connect_shared", None) or self._lib.roce_client_connect
        conn = connect(peer.encode(), self.timeout_ms, self.max_frame_bytes)
        if not conn:
            raise PeerUnreachable(peer, RoCENotAvailable(_last_error(self._lib)))
        self.connects += 1
        return conn

    def send_request(self, peer: str, frame: Frame, *, timeout_s: float = 5.0) -> Frame:
        """Pooled persistent connections (ROCE_CONN_POOL=0 restores one
        connection per request).

        History: pooling was first tried and reverted because a held-open
        connection could go silent under the hpc crossover phase. It is
        back after the concurrency work in docs/CONCURRENCY_CEILING.md fixed
        the transport underneath, plus the teardown/stop/SRQ fixes listed
        in docs/ABC_UPGRADE.md, re-verified against the same hpc
        reproduction.

        Retry rule, the thing that makes reuse safe for non-idempotent
        requests (CAS puts, exclusive creates, MPI sends): a request is only
        re-sent on a fresh connection when the C layer reports
        ROCE_ERR_NOT_DELIVERED: the connection was already known dead before
        anything was posted (a pending CM DISCONNECTED, or its RECV flushed),
        no registered buffer could be had, or posting failed. Once the SEND
        is posted every failure -- a flushed SEND included, since a QP going
        to error flushes WRs that were transmitted but not yet ACKed -- may
        have executed remotely and is surfaced as PeerUnreachable, exactly
        as the connect-per-request path always did. A reply whose request_id does
        not match is treated as a broken connection, never returned."""
        request_bytes = frame.encode()
        attempts = 1 + (self._pool.max_idle if self._pool else 0)
        t0 = time.monotonic() if _TRACE_SLOW_MS else 0.0
        trace = [] if _TRACE_SLOW_MS else None
        for _ in range(attempts):
            conn = self._pool.checkout(peer) if self._pool else None
            reused = conn is not None
            if conn is None:
                tc = time.monotonic()
                conn = self._connect(peer)
                if trace is not None:
                    trace.append(f"connect {1000 * (time.monotonic() - tc):.1f}ms")
            response_ptr = ctypes.POINTER(ctypes.c_uint8)()
            response_len = ctypes.c_size_t()
            rc = self._lib.roce_client_send_request(
                conn, request_bytes, len(request_bytes),
                ctypes.byref(response_ptr), ctypes.byref(response_len),
                int(timeout_s * 1000),
            )
            if trace is not None:
                trace.append(f"send reused={reused} rc={rc}")
            if rc != 0:
                err = _last_error(self._lib)
                if trace is not None:
                    trace.append(err)
                    _trace_slow(peer, frame, t0, trace, force=True)
                self._lib.roce_client_close(conn)
                if rc == _ROCE_ERR_NOT_DELIVERED:
                    continue  # provably never reached the peer: safe to resend on a new connection
                raise PeerUnreachable(peer, RuntimeError(err))
            try:
                raw = ctypes.string_at(response_ptr, response_len.value)
            finally:
                # response_out was malloc()'d by the C side (see
                # roce_transport.h) -- free it via libc, not Python.
                _libc().free(response_ptr)
            try:
                resp = Frame.decode(raw)
            except ProtocolError:
                self._lib.roce_client_close(conn)
                raise
            if resp.request_id != frame.request_id:
                self._lib.roce_client_close(conn)
                raise PeerUnreachable(peer, RuntimeError(
                    f"reply request_id {resp.request_id} != {frame.request_id}: connection out of step"))
            if self._pool:
                self._pool.checkin(peer, conn)
            else:
                self._lib.roce_client_close(conn)
            if trace is not None:
                _trace_slow(peer, frame, t0, trace)
            return resp
        raise PeerUnreachable(peer, RuntimeError("request not deliverable on any pooled or fresh connection"))

    def bufpool_stats(self) -> dict:
        """Process-wide registered buffer pool totals (this process's C library)."""
        if not hasattr(self._lib, "roce_bufpool_stats"):
            return {}
        rb, n, used = ctypes.c_uint64(), ctypes.c_uint32(), ctypes.c_uint32()
        self._lib.roce_bufpool_stats(ctypes.byref(rb), ctypes.byref(n), ctypes.byref(used))
        return {"registered_bytes": rb.value, "buffers": n.value, "in_use": used.value}

    def close(self) -> None:
        """Close pooled idle connections (optional; they also close with the process)."""
        if self._pool:
            self._pool.close_all()

    def serve_native(self, kind: str, bind_address: str) -> NativeServerHandle:
        """Start a native C server: kind "ost" (wire-compatible with a
        single-node PoolNode, reachable via roce.remote_engine.RemoteEngine)
        or "mdt" (fs/mdt_wire.py protocol, reachable via fs.mdt_node.MDTClient)."""
        if not hasattr(self._lib, "rocepool_native_start"):
            raise RoCENotAvailable("this libroce_transport build has no native servers (rocepool_servers.c)")
        ptr = self._lib.rocepool_native_start(NativeServerHandle.KINDS[kind], bind_address.encode(),
                                              self.max_frame_bytes)
        if not ptr:
            raise RoCENotAvailable(_last_error(self._lib))
        buf = ctypes.create_string_buffer(128)
        if self._lib.rocepool_native_local_address(ptr, buf, len(buf)) != 0:
            raise RoCENotAvailable(_last_error(self._lib))
        return NativeServerHandle(self._lib, ptr, buf.value.decode(), kind)

    def serve(self, bind_address: str, handler: RequestHandler) -> ServerHandle:
        handler_cfunctype = _Lib.handler_cfunctype()

        def c_handler(request_ptr, request_len, response_len_out, _user_data):
            try:
                raw = ctypes.string_at(request_ptr, request_len)
                request_frame = Frame.decode(bytes(raw))
                response_frame = handler(request_frame)
                response_bytes = response_frame.encode()
            except (ProtocolError, Exception):  # noqa: BLE001 -- must never raise across the C boundary
                return None
            response_len_out[0] = len(response_bytes)
            # Ownership: the C side's serve_one_connection() frees this
            # after copying it into the connection's send buffer (see
            # roce_transport.h's roce_request_handler doc comment) -- must
            # hand over a malloc'd buffer, not a ctypes-managed one that
            # Python's GC could reclaim first.
            owned = _libc().malloc(max(1, len(response_bytes)))
            ctypes.memmove(owned, response_bytes, len(response_bytes))  # one memcpy; the old (c_uint8*n)(*bytes) unpacked byte-by-byte in Python
            return owned

        callback = handler_cfunctype(c_handler)
        server_ptr = self._lib.roce_server_start(
            bind_address.encode(), callback, None, self.max_frame_bytes)
        if not server_ptr:
            raise RoCENotAvailable(_last_error(self._lib))

        buf = ctypes.create_string_buffer(128)
        if self._lib.roce_server_local_address(server_ptr, buf, len(buf)) != 0:
            raise RoCENotAvailable(_last_error(self._lib))
        address = buf.value.decode("utf-8")

        return _RoCEServerHandle(self._lib, server_ptr, address, keepalive=callback)


class RoCEStream:
    """A raw duplex byte stream over one RDMA connection, presenting the
    same `sendall(data)` / `recv(nbytes) -> bytes` shape as a connected
    `socket.socket` -- the exact subset `wlp/framing.py`'s `send_frame`/
    `recv_frame`/`recv_exact` already use against a real TCP socket in
    `wlp/wlp_client.py`. This is what lets `wlp_client.py` run unmodified
    logic over RDMA: it (and `wlp/framing.py`) never see a `roce_conn_t`
    or an RDMA verb, only something that looks like a connected socket.

    Why this needs an adapter at all: RDMA SEND/RECV preserve message
    boundaries (each `roce_client_stream_recv()` call returns exactly one
    peer `roce_client_stream_send()` call's worth of bytes), whereas a TCP
    socket's `recv(n)` is a byte stream with no such boundaries and may
    return fewer than `n` bytes even when more are coming. `wlp/framing.py`
    already loops (`recv_exact`) to handle TCP's short reads, so the
    simplest correct adapter is to buffer: pull one whole RDMA message at
    a time into `_leftover_recv` and hand out `recv(n)`-sized slices of it,
    resembling a TCP short read whenever a caller asks for less than a
    full RDMA message. `send()`/`sendall()` need no such translation --
    one `sendall()` call from `wlp/framing.py` is already one logical WLP
    message, which maps onto exactly one RDMA SEND.

    Like the rest of `roce/roce_transport.py`, this is written against the
    public rdma-core API and syntax/link-checked against the project's
    hand-written stub headers (`roce/native/test_stubs/`), but has never
    been run against real RDMA traffic -- see this module's top docstring.
    """

    def __init__(self, lib: ctypes.CDLL, conn_ptr: int, *, timeout_ms: int = 5000):
        self._lib = lib
        self._conn = conn_ptr
        self._timeout_ms = timeout_ms
        self._leftover = b""
        self._closed = False

    @classmethod
    def connect(cls, peer: str, *, max_frame_bytes: int = _DEFAULT_MAX_FRAME_BYTES,
                timeout_ms: int = 5000) -> "RoCEStream":
        lib = _Lib.get()
        conn = lib.roce_client_connect(peer.encode(), timeout_ms, max_frame_bytes)
        if not conn:
            raise RoCENotAvailable(_last_error(lib))
        return cls(lib, conn, timeout_ms=timeout_ms)

    def sendall(self, data: bytes) -> None:
        if self._closed:
            raise RoCENotAvailable("RoCEStream is closed")
        rc = self._lib.roce_client_stream_send(self._conn, data, len(data), self._timeout_ms)
        if rc != 0:
            raise RoCENotAvailable(_last_error(self._lib))

    # alias: some callers (like a raw socket) use send() for a single
    # write that is expected to go out whole, same contract as sendall()
    # here since one Python-level call is always one RDMA SEND.
    send = sendall

    def recv(self, nbytes: int) -> bytes:
        if self._closed:
            raise RoCENotAvailable("RoCEStream is closed")
        if not self._leftover:
            data_ptr = ctypes.POINTER(ctypes.c_uint8)()
            data_len = ctypes.c_size_t()
            rc = self._lib.roce_client_stream_recv(
                self._conn, ctypes.byref(data_ptr), ctypes.byref(data_len), self._timeout_ms)
            if rc != 0:
                raise RoCENotAvailable(_last_error(self._lib))
            try:
                self._leftover = ctypes.string_at(data_ptr, data_len.value)
            finally:
                _libc().free(data_ptr)
        chunk, self._leftover = self._leftover[:nbytes], self._leftover[nbytes:]
        return chunk

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._lib.roce_client_close(self._conn)

    def __enter__(self) -> "RoCEStream":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()
