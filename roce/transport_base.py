"""Abstract transport interface for node-to-node memory-pool traffic.

Exactly two implementations exist in this project:

  - loopback_transport.LoopbackTransport: real TCP sockets, fully tested.
    Stands in for RDMA during development/CI on machines with no RDMA NIC.

  - roce_transport.RoCETransport: written against the real ibverbs/
    rdma_cma C APIs via ctypes. Structurally correct against the public
    RDMA verbs standard (IBTA RoCEv2 + the Linux rdma-core userspace API),
    but this sandbox has no RDMA-capable NIC, no /dev/infiniband, and no
    network access to install rdma-core -- so it has NEVER BEEN RUN. Treat
    it as a from-the-spec starting point that needs real-hardware
    validation, not as tested code.

Both must satisfy this same send_request()/serve_forever() contract, which
is what lets pool_node.py's actual pooling logic be developed and tested
entirely against the loopback transport, then pointed at real RDMA later
with (in principle) no change above this interface.
"""
from __future__ import annotations

import abc
from typing import Callable

from roce.protocol import Frame

# A handler receives a request Frame and returns the response Frame.
RequestHandler = Callable[[Frame], Frame]


class TransportError(Exception):
    pass


class PeerUnreachable(TransportError):
    def __init__(self, peer: str, cause: Exception | None = None):
        super().__init__(f"peer unreachable: {peer}" + (f" ({cause})" if cause else ""))
        self.peer = peer
        self.cause = cause


class Transport(abc.ABC):
    @abc.abstractmethod
    def send_request(self, peer: str, frame: Frame, *, timeout_s: float = 5.0) -> Frame:
        """Send `frame` to `peer` and return its response frame.

        `peer` is an opaque address string in whatever form this transport
        uses (host:port for loopback; an RDMA device/GID/port identifier
        for RoCE). Must raise PeerUnreachable on connection failure/timeout,
        never return a malformed/partial Frame.
        """

    @abc.abstractmethod
    def serve(self, bind_address: str, handler: RequestHandler) -> "ServerHandle":
        """Start accepting requests on bind_address, dispatching each
        decoded Frame to `handler` and sending back whatever Frame it
        returns. Non-blocking: returns a handle the caller can stop()."""


class ServerHandle(abc.ABC):
    @abc.abstractmethod
    def stop(self) -> None:
        ...

    @abc.abstractmethod
    def local_address(self) -> str:
        ...
