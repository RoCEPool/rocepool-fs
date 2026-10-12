"""Wire protocol for node-to-node memory-pool operations.

Deliberately transport-agnostic: this module defines message framing and
encode/decode only. Both the RDMA/RoCE transport (roce/roce_transport.py,
untested here -- no RDMA hardware in this environment) and the loopback
TCP transport (roce/loopback_transport.py, real and tested) carry these
exact same bytes. That's what makes the loopback tests a genuine test of
the pooling PROTOCOL logic, not just a stand-in that happens to also pass.

Frame format (fixed 24-byte header + payload):

    offset  size  field
    0       4     magic        b"RCP1"
    4       1     op           uint8, one of the Op values below
    5       3     reserved     zero
    8       8     request_id   uint64, little-endian
    16      4     key_len      uint32, little-endian
    20      4     value_len    uint32, little-endian
    24      key_len          key bytes (UTF-8)
    24+key_len  value_len     value bytes (opaque)

Every request gets exactly one response, matched by request_id. This is
intentionally simple (no pipelining, no multiplexed streams) -- a real RoCE
deployment would want to issue many requests in flight per queue pair, but
that's a performance optimization on top of this framing, not a change to
it, and is out of scope for an initial framework.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

MAGIC = b"RCP1"
_HEADER = struct.Struct("<4sBxxxQII")  # magic, op, pad(3), request_id, key_len, value_len
HEADER_SIZE = _HEADER.size
assert HEADER_SIZE == 24, HEADER_SIZE


class Op(IntEnum):
    PUT = 1
    PUT_OK = 2
    GET = 3
    GET_OK = 4
    DELETE = 5
    DELETE_OK = 6
    STAT = 7
    STAT_OK = 8
    LIST_PREFIX = 9
    LIST_PREFIX_OK = 10
    # "thick KV" OST ops (roce/native/rocepool_servers.c + PoolNode):
    #   WRITE_AT  value: u64 LE offset + data -> atomic in-place write (creates,
    #             zero-fills gaps, bumps version); reply = PUT_OK body
    #   GET_V     reply: u64 BE version + data (one snapshot)
    #   READ_AT   value: u64 LE offset + u32 LE length; reply: u64 BE version + bytes
    #   TRUNCATE  value: u64 LE size -> shrink if longer; reply = PUT_OK body (0/0 if absent)
    #   VTOKEN    reply: u64 BE version token -- the object's version, or when
    #             absent its delete watermark; never reused for one key, so
    #             PUT with marker 2 (expected = a token) cannot succeed after
    #             a delete/recreate in between (no ABA; used by the inline spill)
    #   BATCH     value: u32 LE n + n x (u8 op, u32 LE klen, u32 LE vlen, key, value);
    #             reply: u32 LE n + n x (u8 op, u32 LE vlen, value)
    WRITE_AT = 11
    WRITE_AT_OK = 12
    GET_V = 13
    GET_V_OK = 14
    READ_AT = 15
    READ_AT_OK = 16
    TRUNCATE = 17
    TRUNCATE_OK = 18
    MPI_SEND = 20    # mpi/comm.py: point-to-point message deposit (eager, always acked)
    MPI_SEND_OK = 21
    # mpi/comm.py wire-up probe: idempotent, safe to retry until a peer is listening
    MPI_PING = 22
    MPI_PING_OK = 23
    # fs/mdt_node.py: one metadata-server operation; `value` is a JSON body
    # naming the sub-operation (lookup/create/set_size/unlink/readdir)
    MDT = 30
    MDT_OK = 31
    BATCH = 40       # see the thick-KV block above (20/21 are taken by MPI)
    BATCH_OK = 41
    VTOKEN = 42
    VTOKEN_OK = 43
    # native servers only: counters for bottleneck analysis (tools/io500_run.py);
    # reply = JSON {kind, reqs, handler_cpu_ns, handler_wall_ns, bytes_in,
    # bytes_out, inflight_max, by_op, + MDT lock_wait_ns/lock_hold_ns}
    STATS = 50
    STATS_OK = 51
    ERROR = 255


class ProtocolError(Exception):
    pass


@dataclass(frozen=True)
class Frame:
    op: Op
    request_id: int
    key: str
    value: bytes = b""

    def encode(self) -> bytes:
        key_bytes = self.key.encode("utf-8")
        header = _HEADER.pack(MAGIC, int(self.op), self.request_id, len(key_bytes), len(self.value))
        return header + key_bytes + self.value

    @staticmethod
    def decode(buf: bytes) -> "Frame":
        if len(buf) < HEADER_SIZE:
            raise ProtocolError(f"buffer too short for header: {len(buf)} < {HEADER_SIZE}")
        magic, op, request_id, key_len, value_len = _HEADER.unpack(buf[:HEADER_SIZE])
        if magic != MAGIC:
            raise ProtocolError(f"bad magic: {magic!r}")
        try:
            op = Op(op)
        except ValueError as exc:
            raise ProtocolError(f"unknown op byte: {op}") from exc
        expected_total = HEADER_SIZE + key_len + value_len
        if len(buf) != expected_total:
            raise ProtocolError(f"frame length mismatch: buffer={len(buf)} expected={expected_total}")
        key = buf[HEADER_SIZE:HEADER_SIZE + key_len].decode("utf-8")
        value = bytes(buf[HEADER_SIZE + key_len:])
        return Frame(op=op, request_id=request_id, key=key, value=value)

    @staticmethod
    def peek_body_length(header_bytes: bytes) -> int:
        """How many more bytes to read after the 24-byte header, given the
        header alone -- lets a stream transport read framed messages
        without needing a length-prefixed outer envelope."""
        if len(header_bytes) != HEADER_SIZE:
            raise ProtocolError("peek_body_length requires exactly the header bytes")
        _, _, _, key_len, value_len = _HEADER.unpack(header_bytes)
        return key_len + value_len
