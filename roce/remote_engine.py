"""RemoteEngine: a StorageEngine that lives on ONE specific remote node.

PoolNode spreads keys over a cluster by hash. Lustre-style striping
(fs/striping.py) needs the opposite: object N of a file goes to OST
number k, chosen by the layout, not by a hash. RemoteEngine is that
"this OST, over the network" handle: every call is one request to the
node at `address`, over whatever Transport is given (RoCETransport for
RDMA, LoopbackTransport for TCP). The server side is an ordinary
PoolNode serving only itself (nodes=[address]), so an OSS is just
`PoolNode(local_address=a, nodes=[a], engine=MemoryEngine(), ...)`.

    osts = [RemoteEngine(a, RoCETransport()) for a in ost_addresses]
    fs   = StripedStorage(osts)        # a Lustre-like client mount

Wire encoding and error mapping are PoolNode's own (same frames, same
VERSION_CONFLICT -> VersionConflict mapping), so a CAS rejected by a
remote OST raises the same exception a local engine would.
"""
from __future__ import annotations

import itertools
import os

from roce.pool_node import (
    _pack_put_value, _raise_remote, _unpack_list_prefix_value, _unpack_put_ok_value, _unpack_stat_value,
    pack_batch_request, unpack_batch_reply,
)
from roce.protocol import Frame, Op
from roce.transport_base import Transport
from storage_engine.base import ObjectMeta, ObjectNotFound, PutResult, StorageEngine

_ids = itertools.count(1)
_DEFAULT_FRAME = int(os.environ.get("ROCE_MAX_FRAME_BYTES", 16 * 1024 * 1024))


class RemoteEngine(StorageEngine):
    def __init__(self, address: str, transport: Transport):
        self.address = address
        self.transport = transport

    def _call(self, op: Op, key: str, value: bytes = b"") -> Frame:
        resp = self.transport.send_request(self.address, Frame(op=op, request_id=next(_ids), key=key, value=value))
        if resp.op == Op.ERROR:
            _raise_remote(resp)
        return resp

    def put(self, key: str, data: bytes, *, expected_version: int | None = None) -> PutResult:
        version, size = _unpack_put_ok_value(self._call(Op.PUT, key, _pack_put_value(data, expected_version)).value)
        return PutResult(key=key, version=version, size=size)

    def get(self, key: str) -> bytes:
        return self._call(Op.GET, key).value

    def stat(self, key: str) -> ObjectMeta:
        return _unpack_stat_value(key, self._call(Op.STAT, key).value)

    def delete(self, key: str) -> None:
        self._call(Op.DELETE, key)

    def list_prefix(self, prefix: str) -> list[ObjectMeta]:
        return _unpack_list_prefix_value(self._call(Op.LIST_PREFIX, prefix).value)

    def exists(self, key: str) -> bool:
        try:
            self.stat(key)
            return True
        except ObjectNotFound:
            return False

    def total_bytes(self) -> int:
        return sum(m.size for m in self.list_prefix(""))

    # -- thick-KV ops: one round trip each (see roce/protocol.py) ----------
    def get_versioned(self, key: str) -> tuple[int, bytes]:
        v = self._call(Op.GET_V, key).value
        return int.from_bytes(v[:8], "big"), v[8:]

    def read_at(self, key: str, offset: int, length: int) -> tuple[int, bytes]:
        v = self._call(Op.READ_AT, key, offset.to_bytes(8, "little") + length.to_bytes(4, "little")).value
        return int.from_bytes(v[:8], "big"), v[8:]

    def write_at(self, key: str, offset: int, data: bytes) -> PutResult:
        version, size = _unpack_put_ok_value(self._call(Op.WRITE_AT, key, offset.to_bytes(8, "little") + data).value)
        return PutResult(key=key, version=version, size=size)

    def truncate_object(self, key: str, size: int) -> PutResult:
        version, sz = _unpack_put_ok_value(self._call(Op.TRUNCATE, key, size.to_bytes(8, "little")).value)
        return PutResult(key=key, version=version, size=sz)

    def version_token(self, key: str) -> int:
        return int.from_bytes(self._call(Op.VTOKEN, key).value[:8], "big")

    def put_token(self, key: str, data: bytes, token: int) -> PutResult:
        version, size = _unpack_put_ok_value(self._call(Op.PUT, key, b"\x02" + token.to_bytes(8, "big") + data).value)
        return PutResult(key=key, version=version, size=size)

    def batch(self, subs: list[tuple[int, str, bytes]]) -> list[Frame]:
        """Several requests to this OST in ONE frame, executed in order.
        Returns one reply Frame per sub-request (op may be ERROR). A list
        too big for one frame goes as several, each at most half a frame."""
        limit = getattr(self.transport, "max_frame_bytes", _DEFAULT_FRAME) // 2
        out: list[Frame] = []
        i = 0
        while i < len(subs):
            j, size = i, 4
            while j < len(subs):
                item = 9 + len(subs[j][1].encode("utf-8")) + len(subs[j][2])
                if j > i and size + item > limit:
                    break
                size += item
                j += 1
            chunk = subs[i:j]
            replies = unpack_batch_reply(self._call(Op.BATCH, "", pack_batch_request(chunk)).value)
            out += [Frame(op=Op(op), request_id=0, key=chunk[k][1], value=v) for k, (op, v) in enumerate(replies)]
            i = j
        return out
