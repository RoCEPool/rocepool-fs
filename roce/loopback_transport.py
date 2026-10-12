"""Real, working TCP transport implementing the Transport interface.

This is genuine, tested networking code -- not a mock. It exists so
pool_node.py's replication/lookup logic can be developed and CI-tested on
any machine, with no RDMA hardware required. It carries the exact same
roce.protocol.Frame bytes that RoCETransport would carry over a queue
pair, so a test that passes against this transport is exercising real
protocol logic, not a stub.

It is deliberately NOT trying to imitate RDMA semantics (no one-sided
READ/WRITE, no completion queue) -- it's a plain synchronous
request/response transport. That's an accurate description of what it
is: a development/CI stand-in, not a performance model of RoCE.
"""
from __future__ import annotations

import selectors
import socket
import threading

from roce.protocol import Frame, HEADER_SIZE, ProtocolError
from roce.transport_base import PeerUnreachable, RequestHandler, ServerHandle, Transport


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("peer closed connection mid-frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _recv_frame(sock: socket.socket) -> Frame:
    header = _recv_exact(sock, HEADER_SIZE)
    body_len = Frame.peek_body_length(header)
    body = _recv_exact(sock, body_len) if body_len else b""
    return Frame.decode(header + body)


class _LoopbackServerHandle(ServerHandle):
    def __init__(self, sock: socket.socket, thread: threading.Thread, address: str, stop_event: threading.Event):
        self._sock = sock
        self._thread = thread
        self._address = address
        self._stop_event = stop_event

    def stop(self) -> None:
        # Signal-and-join, rather than closing the listener socket from
        # this (the caller's) thread while accept_loop might be blocked
        # inside accept() on it -- on Linux that race can leave the fd
        # number recycled into a *different* new socket before the old
        # thread's blocked syscall fully unwinds, which is exactly what
        # caused "Address already in use" on an immediate rebind during
        # development of this transport (reproduced and confirmed via
        # tests/test_pool_node.py's multi-node cluster teardown/setup).
        # accept_loop owns closing self._sock, only after it observes the
        # stop_event and returns from its (bounded-timeout) accept wait.
        self._stop_event.set()
        self._thread.join(timeout=5.0)

    def local_address(self) -> str:
        return self._address


class LoopbackTransport(Transport):
    def send_request(self, peer: str, frame: Frame, *, timeout_s: float = 5.0) -> Frame:
        host, _, port_s = peer.partition(":")
        try:
            port = int(port_s)
        except ValueError as exc:
            raise TransportErrorBadPeer(peer) from exc
        try:
            with socket.create_connection((host, port), timeout=timeout_s) as sock:
                sock.settimeout(timeout_s)
                sock.sendall(frame.encode())
                return _recv_frame(sock)
        except (OSError, ProtocolError, ConnectionError) as exc:
            raise PeerUnreachable(peer, exc) from exc

    def serve(self, bind_address: str, handler: RequestHandler) -> ServerHandle:
        host, _, port_s = bind_address.partition(":")
        port = int(port_s) if port_s else 0
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host or "127.0.0.1", port))
        listener.listen(64)
        listener.setblocking(False)
        actual_host, actual_port = listener.getsockname()
        address = f"{actual_host}:{actual_port}"
        stop_event = threading.Event()

        def accept_loop():
            selector = selectors.DefaultSelector()
            selector.register(listener, selectors.EVENT_READ)
            try:
                while not stop_event.is_set():
                    for _key, _mask in selector.select(timeout=0.2):
                        try:
                            conn, _ = listener.accept()
                        except OSError:
                            continue
                        threading.Thread(target=_serve_one_connection, args=(conn, handler), daemon=True).start()
            finally:
                selector.close()
                listener.close()  # only ever closed here, by the thread that owns it

        thread = threading.Thread(target=accept_loop, daemon=True)
        thread.start()
        return _LoopbackServerHandle(listener, thread, address, stop_event)


def _serve_one_connection(conn: socket.socket, handler: RequestHandler) -> None:
    try:
        with conn:
            conn.settimeout(30.0)
            while True:
                try:
                    request = _recv_frame(conn)
                except ConnectionError:
                    return  # client closed; normal end of a one-shot request
                response = handler(request)
                conn.sendall(response.encode())
    except (OSError, ProtocolError):
        return


class TransportErrorBadPeer(PeerUnreachable):
    pass

