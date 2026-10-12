"""A minimal MPI-style communicator: point-to-point send/recv plus the
handful of collectives (`barrier`, `bcast`, `gather`, `allgather`,
`reduce_sum`, `allreduce_sum`) that `mpi/mpi_file.py` needs for
collective file I/O -- built directly on this project's existing
`roce.transport_base.Transport` interface, so it runs over the same real,
tested loopback-TCP transport as `roce/pool_node.py` (and, in principle,
the same untested-but-real-API RoCE transport -- see that module's
docstring).

Scope, stated plainly (same standard as the rest of this project):

  - **Point-to-point is eager/always-acked**: `send` is a synchronous RPC
    that deposits the message into the receiver's inbox and returns once
    acked; there's no separate rendezvous protocol for large messages, no
    non-blocking `Isend`/`Irecv`, and no message ordering guarantee
    across different tags (only within one (src, tag) pair, which is a
    FIFO queue).
  - **Collectives are simple, centralized (root-coordinated) algorithms**
    (e.g. `bcast` is `size-1` point-to-point sends from the root; `gather`
    is `size-1` receives at the root), not the tree/butterfly algorithms
    real MPI implementations use for scale -- correct, not fast at large
    rank counts.
  - **Only one collective call may be in flight across a given
    Communicator at a time** (this mirrors real MPI's requirement that
    collective calls on the same communicator happen in the same order
    on every rank, but doesn't detect or guard against a caller violating
    that -- two concurrently-issued different collectives on the same
    communicator can cross-talk since fixed tag strings are reused
    across calls).
  - **`reduce_sum`/`allreduce_sum` implement `MPI_SUM` on 64-bit signed
    integers only** -- not a generic `MPI_Op` framework. Adding
    `MPI_MAX`/`MPI_MIN`/a custom reduction function is a mechanical
    extension of the same gather-then-combine shape, not included here.

TESTED how: tests/test_mpi_comm.py runs real multi-rank clusters, each
rank a real thread with its own real loopback-TCP server (same pattern as
tests/test_pool_node.py's `_make_cluster()`), doing real socket round
trips for every operation below -- not in-process function calls
standing in for network communication.
"""
from __future__ import annotations

import itertools
import struct
import threading
import time
from collections import defaultdict, deque

from roce.protocol import Frame, Op
from roce.transport_base import PeerUnreachable, Transport

_request_ids = itertools.count(1)

_BARRIER_TAG = "__barrier__"
_BCAST_TAG = "__bcast__"
_GATHER_TAG = "__gather__"
_REDUCE_TAG = "__reduce__"


def _encode_list(items: list[bytes]) -> bytes:
    out = bytearray()
    for item in items:
        out += struct.pack("<I", len(item))
        out += item
    return bytes(out)


def _decode_list(data: bytes, count: int) -> list[bytes]:
    items = []
    pos = 0
    for _ in range(count):
        (n,) = struct.unpack_from("<I", data, pos)
        pos += 4
        items.append(data[pos:pos + n])
        pos += n
    return items


class Communicator:
    """`rank` is this process's index into `addresses` (one bind address
    per rank, e.g. "127.0.0.1:5001"); `size` is `len(addresses)`."""

    def __init__(self, rank: int, addresses: list[str], transport: Transport,
                 *, wireup_timeout_s: float = 30.0):
        if not (0 <= rank < len(addresses)):
            raise ValueError(f"rank {rank} out of range for {len(addresses)} addresses")
        self.rank = rank
        self.size = len(addresses)
        self.addresses = list(addresses)
        self._transport = transport
        self._inbox: dict[tuple[int, str], deque[bytes]] = defaultdict(deque)
        self._cv = threading.Condition()
        # Wire-up: ranks launched as separate processes (the normal case,
        # and what tools/run_crossover_rdma.py does across two network
        # nodes) start listening at slightly different times. The first
        # send to a rank that isn't listening yet must not be lost, so
        # before the first real send to each peer we probe it with an
        # idempotent MPI_PING until it answers (retrying a real MPI_SEND
        # instead could duplicate a message whose reply was lost).
        self._wireup_timeout_s = wireup_timeout_s
        self._reachable: set[int] = {rank}
        self._server = transport.serve(addresses[rank], self._handle)

    def close(self) -> None:
        self._server.stop()

    # -- point to point --
    def _handle(self, frame: Frame) -> Frame:
        if frame.op == Op.MPI_PING:
            return Frame(op=Op.MPI_PING_OK, request_id=frame.request_id, key="", value=b"")
        if frame.op != Op.MPI_SEND:
            return Frame(op=Op.ERROR, request_id=frame.request_id, key="unsupported_op", value=b"")
        src_str, tag = frame.key.split(":", 1)
        src = int(src_str)
        with self._cv:
            self._inbox[(src, tag)].append(frame.value)
            self._cv.notify_all()
        return Frame(op=Op.MPI_SEND_OK, request_id=frame.request_id, key="", value=b"")

    def _wireup(self, dest: int) -> None:
        deadline = time.monotonic() + self._wireup_timeout_s
        delay = 0.02
        while True:
            try:
                resp = self._transport.send_request(
                    self.addresses[dest], Frame(op=Op.MPI_PING, request_id=next(_request_ids), key="", value=b""))
                if resp.op == Op.MPI_PING_OK:
                    self._reachable.add(dest)
                    return
            except PeerUnreachable:
                pass
            if time.monotonic() >= deadline:
                raise PeerUnreachable(self.addresses[dest], TimeoutError(
                    f"rank {self.rank}: rank {dest} not listening after {self._wireup_timeout_s}s wire-up"))
            time.sleep(delay)
            delay = min(delay * 2, 0.5)

    def send(self, dest: int, tag: str, data: bytes) -> None:
        if dest not in self._reachable:
            self._wireup(dest)
        frame = Frame(op=Op.MPI_SEND, request_id=next(_request_ids),
                       key=f"{self.rank}:{tag}", value=data)
        self._transport.send_request(self.addresses[dest], frame)

    def recv(self, src: int, tag: str, *, timeout_s: float = 30.0) -> bytes:
        with self._cv:
            got = self._cv.wait_for(lambda: bool(self._inbox[(src, tag)]), timeout=timeout_s)
            if not got:
                raise TimeoutError(f"rank {self.rank}: recv(src={src}, tag={tag!r}) timed out")
            return self._inbox[(src, tag)].popleft()

    # -- collectives --
    def barrier(self) -> None:
        if self.rank == 0:
            for r in range(1, self.size):
                self.recv(r, _BARRIER_TAG)
            for r in range(1, self.size):
                self.send(r, _BARRIER_TAG, b"go")
        else:
            self.send(0, _BARRIER_TAG, b"here")
            self.recv(0, _BARRIER_TAG)

    def bcast(self, data: bytes | None, root: int = 0) -> bytes:
        if self.rank == root:
            for r in range(self.size):
                if r != root:
                    self.send(r, _BCAST_TAG, data)
            return data
        return self.recv(root, _BCAST_TAG)

    def gather(self, data: bytes, root: int = 0) -> list[bytes] | None:
        if self.rank == root:
            results: list[bytes | None] = [None] * self.size
            results[root] = data
            for r in range(self.size):
                if r != root:
                    results[r] = self.recv(r, _GATHER_TAG)
            return results  # type: ignore[return-value]
        self.send(root, _GATHER_TAG, data)
        return None

    def allgather(self, data: bytes) -> list[bytes]:
        gathered = self.gather(data, root=0)
        if self.rank == 0:
            encoded = _encode_list(gathered)
        else:
            encoded = b""
        encoded = self.bcast(encoded, root=0)
        return _decode_list(encoded, self.size)

    def reduce_sum(self, value: int, root: int = 0) -> int | None:
        encoded = struct.pack("<q", value)
        gathered = self.gather(encoded, root=root)
        if self.rank != root:
            return None
        return sum(struct.unpack("<q", v)[0] for v in gathered)

    def allreduce_sum(self, value: int) -> int:
        total = self.reduce_sum(value, root=0)
        encoded = struct.pack("<q", total) if self.rank == 0 else b""
        encoded = self.bcast(encoded, root=0)
        return struct.unpack("<q", encoded)[0]
