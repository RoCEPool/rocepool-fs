"""Proves WlpClient.from_stream()'s duck-typing claim for real: wraps a
real TCP socket to WlpTestServer in a minimal object that exposes ONLY
`sendall`/`recv`/`close` -- deliberately not a socket.socket subclass, so
if wlp_client.py or wlp/framing.py ever called any other socket method
(settimeout, makefile, fileno, ...) this would raise AttributeError
instead of silently working by inheritance.

This is what stands in for `roce.roce_transport.RoCEStream` here: that
class has the identical `sendall`/`recv`/`close` surface (see its
docstring in roce/roce_transport.py), but actually exercising it needs
real RDMA hardware this sandbox does not have (see roce_transport.py's
module docstring and this project's README for how that was confirmed).
Proving the *adapter boundary* -- that WlpClient and wlp/framing.py never
reach past sendall/recv/close no matter what's on the other end -- is
exactly what's left to verify without that hardware, and it is what makes
"RoCEStream is a drop-in for the TCP path" a checked claim rather than an
assumption.
"""
import socket
import unittest

from storage_engine.rapidsse_engine import RapidsSEEngine
from tests.fakes.wlp_test_server import WlpTestServer
from wlp.wlp_client import WlpClient


class _BareDuplexStream:
    """Exposes exactly sendall/recv/close -- nothing socket-specific."""

    def __init__(self, sock: socket.socket):
        self._sock = sock

    def sendall(self, data: bytes) -> None:
        self._sock.sendall(data)

    def recv(self, nbytes: int) -> bytes:
        return self._sock.recv(nbytes)

    def close(self) -> None:
        self._sock.close()


class WlpClientStreamAdapterTests(unittest.TestCase):
    def setUp(self):
        self.server = WlpTestServer()
        raw_sock = socket.create_connection((self.server.host, self.server.port), timeout=10.0)
        self.stream = _BareDuplexStream(raw_sock)
        self.client = WlpClient.from_stream(self.stream)

    def tearDown(self):
        self.client.close()
        self.server.stop()

    def test_handshake_and_query_round_trip_over_the_bare_stream_adapter(self):
        engine = RapidsSEEngine(self.client, placeholder="?")
        engine.put("adapter/key.txt", b"hello over a duck-typed stream")
        self.assertEqual(engine.get("adapter/key.txt"), b"hello over a duck-typed stream")

    def test_client_close_closes_the_underlying_stream(self):
        self.client.close()
        with self.assertRaises(OSError):
            self.stream._sock.send(b"x")
        # tearDown calls close() again; WlpClient.close() -> stream.close()
        # -> socket.close() must tolerate being called twice.
        self.client.close()


if __name__ == "__main__":
    unittest.main()
