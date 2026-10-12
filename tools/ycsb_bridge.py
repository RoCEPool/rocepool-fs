"""TCP bridge exposing this project's storage layer (a `PoolNode`, or any
single `StorageEngine`) to real, unmodified upstream YCSB
(https://github.com/brianfrankcooper/YCSB) via a small custom Java DB
binding (`tools/ycsb_java/RocepoolClient.java`).

Why a bridge instead of a Java rewrite of the wire protocol: `PoolNode`'s
real protocol (roce/protocol.py's 24-byte `Frame` header, hashing, routing)
is Python-native and already thoroughly tested (tests/test_pool_node.py,
the crossover-cable rig). Reimplementing it in Java would test a SECOND,
parallel implementation instead of this project's actual code path.
Running YCSB against a thin bridge in front of the real `PoolNode` exercises
the real thing; the bridge itself is protocol glue, kept intentionally
dumb (fixed-width binary framing, no business logic) and covered by
`tests/test_ycsb_bridge.py`.

Wire protocol (all multi-byte integers big-endian, all strings UTF-8):

Request:
    1B  opcode        1=READ 2=INSERT 3=UPDATE 4=DELETE 5=SCAN
    2B  table_len + table bytes
    2B  key_len + key bytes
    -- READ only --
    1B  all_fields (1 = every field, 0 = the named subset below)
    2B  field_count + for each: 2B name_len + name bytes
    -- SCAN only --
    4B  recordcount
    1B  all_fields (1 = every field, 0 = the named subset below)
    2B  field_count + for each: 2B name_len + name bytes
    -- INSERT/UPDATE only --
    2B  field_count + for each: 2B name_len + name bytes, 4B value_len + value bytes

Response:
    1B  status        0=OK 1=NOT_FOUND 2=ERROR
    -- ERROR --
    2B  message_len + message bytes
    -- OK, READ --
    2B  field_count + for each: 2B name_len + name, 4B value_len + value
    -- OK, SCAN --
    4B  record_count + for each record: 2B field_count + fields as above
    -- OK, INSERT/UPDATE/DELETE --
    (nothing further)

One record is stored as one PoolNode object at key `f"{table}\\x00{key}"`,
itself the same field-name/field-value encoding as a READ response body (so
_encode_fields/_decode_fields do double duty as the wire codec and the
on-disk record codec) -- keeping the bridge from needing any serialization
library on either side.
"""
from __future__ import annotations

import argparse
import os
import socket
import struct
import sys
import threading

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from storage_engine.base import ObjectNotFound  # noqa: E402

OP_READ, OP_INSERT, OP_UPDATE, OP_DELETE, OP_SCAN = 1, 2, 3, 4, 5
ST_OK, ST_NOT_FOUND, ST_ERROR = 0, 1, 2


def _record_key(table: str, key: str) -> str:
    return f"ycsb/{table}\x00{key}"


def _recvn(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed mid-message")
        buf.extend(chunk)
    return bytes(buf)


def _encode_fields(fields: dict[str, bytes]) -> bytes:
    out = [struct.pack(">H", len(fields))]
    for name, value in fields.items():
        nb = name.encode()
        out.append(struct.pack(">H", len(nb)) + nb)
        out.append(struct.pack(">I", len(value)) + value)
    return b"".join(out)


def _decode_fields(buf: bytes, offset: int) -> tuple[dict[str, bytes], int]:
    (count,) = struct.unpack_from(">H", buf, offset)
    offset += 2
    fields: dict[str, bytes] = {}
    for _ in range(count):
        (nlen,) = struct.unpack_from(">H", buf, offset)
        offset += 2
        name = buf[offset:offset + nlen].decode()
        offset += nlen
        (vlen,) = struct.unpack_from(">I", buf, offset)
        offset += 4
        value = buf[offset:offset + vlen]
        offset += vlen
        fields[name] = value
    return fields, offset


def _recv_field_names(conn: socket.socket) -> list[str] | None:
    """Reads the `all_fields` + name-list header straight off the wire
    (as opposed to `_decode_fields`, which decodes an already-buffered
    field/value record -- a different shape, see the wire protocol
    docstring above)."""
    (all_fields,) = struct.unpack(">B", _recvn(conn, 1))
    (count,) = struct.unpack(">H", _recvn(conn, 2))
    names = []
    for _ in range(count):
        (nlen,) = struct.unpack(">H", _recvn(conn, 2))
        names.append(_recvn(conn, nlen).decode())
    return None if all_fields else names


class Bridge:
    """Wraps a `_Backend`-shaped object (`PoolNode` or any `StorageEngine`
    -- both expose put/get/delete/stat/list_prefix, see fs/posix_api.py's
    `_Backend` protocol) with the wire protocol above."""

    def __init__(self, backend):
        self.backend = backend

    def handle_read(self, table: str, key: str, want: list[str] | None) -> tuple[int, bytes]:
        try:
            raw = self.backend.get(_record_key(table, key))
        except ObjectNotFound:
            return ST_NOT_FOUND, b""
        fields, _ = _decode_fields(raw, 0)
        if want is not None:
            fields = {k: v for k, v in fields.items() if k in want}
        return ST_OK, _encode_fields(fields)

    def handle_scan(self, table: str, startkey: str, recordcount: int, want: list[str] | None) -> tuple[int, bytes]:
        prefix = f"ycsb/{table}\x00"
        metas = sorted(self.backend.list_prefix(prefix), key=lambda m: m.key)
        out = []
        started = False
        for meta in metas:
            record_key = meta.key[len(prefix):]
            if not started:
                if record_key < startkey:
                    continue
                started = True
            if len(out) >= recordcount:
                break
            try:
                raw = self.backend.get(meta.key)
            except ObjectNotFound:
                continue
            fields, _ = _decode_fields(raw, 0)
            if want is not None:
                fields = {k: v for k, v in fields.items() if k in want}
            out.append(fields)
        body = [struct.pack(">I", len(out))]
        for fields in out:
            body.append(_encode_fields(fields))
        return ST_OK, b"".join(body)

    def handle_insert(self, table: str, key: str, fields: dict[str, bytes]) -> tuple[int, bytes]:
        self.backend.put(_record_key(table, key), _encode_fields(fields))
        return ST_OK, b""

    def handle_update(self, table: str, key: str, fields: dict[str, bytes]) -> tuple[int, bytes]:
        rk = _record_key(table, key)
        try:
            raw = self.backend.get(rk)
            existing, _ = _decode_fields(raw, 0)
        except ObjectNotFound:
            existing = {}
        existing.update(fields)
        self.backend.put(rk, _encode_fields(existing))
        return ST_OK, b""

    def handle_delete(self, table: str, key: str) -> tuple[int, bytes]:
        try:
            self.backend.delete(_record_key(table, key))
        except ObjectNotFound:
            return ST_NOT_FOUND, b""
        return ST_OK, b""


def _serve_conn(bridge: Bridge, conn: socket.socket) -> None:
    try:
        while True:
            try:
                header = _recvn(conn, 1)
            except ConnectionError:
                return
            (opcode,) = struct.unpack(">B", header)
            (tlen,) = struct.unpack(">H", _recvn(conn, 2))
            table = _recvn(conn, tlen).decode()
            (klen,) = struct.unpack(">H", _recvn(conn, 2))
            key = _recvn(conn, klen).decode()

            try:
                if opcode == OP_READ:
                    want = _recv_field_names(conn)
                    status, body = bridge.handle_read(table, key, want)
                elif opcode == OP_SCAN:
                    (recordcount,) = struct.unpack(">I", _recvn(conn, 4))
                    want = _recv_field_names(conn)
                    status, body = bridge.handle_scan(table, key, recordcount, want)
                elif opcode in (OP_INSERT, OP_UPDATE):
                    (fcount,) = struct.unpack(">H", _recvn(conn, 2))
                    fields: dict[str, bytes] = {}
                    for _ in range(fcount):
                        (nlen,) = struct.unpack(">H", _recvn(conn, 2))
                        name = _recvn(conn, nlen).decode()
                        (vlen,) = struct.unpack(">I", _recvn(conn, 4))
                        value = _recvn(conn, vlen)
                        fields[name] = value
                    handler = bridge.handle_insert if opcode == OP_INSERT else bridge.handle_update
                    status, body = handler(table, key, fields)
                elif opcode == OP_DELETE:
                    status, body = bridge.handle_delete(table, key)
                else:
                    status, body = ST_ERROR, f"unknown opcode {opcode}".encode()
            except Exception as exc:  # noqa: BLE001 -- report to the client, keep serving
                status, body = ST_ERROR, str(exc).encode()

            if status == ST_ERROR:
                conn.sendall(struct.pack(">B", status) + struct.pack(">H", len(body)) + body)
            else:
                conn.sendall(struct.pack(">B", status) + body)
    finally:
        conn.close()


def serve_forever(bridge: Bridge, host: str, port: int, ready_msg: str = "READY") -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(64)
    print(f"{ready_msg} {host}:{port}", flush=True)
    try:
        while True:
            conn, _ = srv.accept()
            threading.Thread(target=_serve_conn, args=(bridge, conn), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=19900)
    ap.add_argument("--backend", choices=("local", "pool"), default="local")
    ap.add_argument("--pool-node", help="this node's own address, e.g. 10.99.0.1:19801 (--backend pool only)")
    ap.add_argument("--pool-nodes", help="comma-separated list of every node's address (--backend pool only)")
    args = ap.parse_args()

    if args.backend == "local":
        from storage_engine.memory_engine import MemoryEngine
        backend = MemoryEngine()
    else:
        from roce.pool_node import PoolNode
        from roce.roce_transport import RoCETransport
        from storage_engine.memory_engine import MemoryEngine
        nodes = args.pool_nodes.split(",")
        node = PoolNode(local_address=args.pool_node, nodes=nodes, engine=MemoryEngine(), transport=RoCETransport())
        node.start()
        backend = node

    serve_forever(Bridge(backend), args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
