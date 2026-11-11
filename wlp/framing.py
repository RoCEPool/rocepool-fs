"""WLP wire framing: the 8-byte header sent before every message body,
matching `src/wlp/thrift/supports/WlpHeader.hpp` exactly:

    union WlpHeader {
      struct {
        u4 m_uMsgSize : 32;  // big-endian on the wire (htonl/ntohl)
        u1 m_uMsgType : 8;
        u4 m_uSpare   : 24;
      };
      i8 m_iAsI8;
    };

i.e. 4 bytes big-endian message length, 1 byte message type, 3 unused
bytes -- 8 bytes total, followed immediately by the Compact-Protocol-
serialized struct body (no further Thrift message envelope; see
compact_protocol.py's module docstring for how that was confirmed against
`AdaptorWL.cpp`).

The length counts the 8-byte header too: the sender writes
`vMsgSize(m_pTransport->iBytesWritten())` after header and body went into
one buffer (AdaptorWL.cpp vSerialiseAndSendMsg), and the receiver takes
`iMsgBodySize = iMsgSize() - C_WLPHEADER_SIZE` (vDecodePayload). An earlier
version sent the body length alone; RapidsSE then read 8 bytes too few and
dropped the connection at the first message.
"""
from __future__ import annotations

import socket
import struct

HEADER_SIZE = 8
_HEADER_STRUCT = struct.Struct(">IB3x")  # size(u32 BE), type(u8), 3 pad bytes


def pack_header(msg_type: int, body_size: int) -> bytes:
    return _HEADER_STRUCT.pack(HEADER_SIZE + body_size, msg_type)


def unpack_header(raw: bytes) -> tuple[int, int]:
    """Returns (msg_type, body_size)."""
    msg_size, msg_type = _HEADER_STRUCT.unpack(raw)
    if msg_size < HEADER_SIZE:
        raise ConnectionError(f"WLP message size {msg_size} is smaller than its own header")
    return msg_type, msg_size - HEADER_SIZE


def send_frame(sock: socket.socket, msg_type: int, body: bytes) -> None:
    sock.sendall(pack_header(msg_type, len(body)) + body)


def recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("WLP peer closed connection mid-message")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def recv_frame(sock: socket.socket) -> tuple[int, bytes]:
    """Returns (msg_type, body_bytes)."""
    header = recv_exact(sock, HEADER_SIZE)
    msg_type, body_size = unpack_header(header)
    body = recv_exact(sock, body_size) if body_size else b""
    return msg_type, body
