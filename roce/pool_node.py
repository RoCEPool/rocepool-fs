"""Multi-node shared memory pool.

This is the actual "内存共享池文件系统" logic: given a local StorageEngine
and a Transport (loopback for testing, RoCE for real deployment -- see
roce_transport.py's module docstring for why that one is untested here),
PoolNode makes a *key* look like it lives in one flat namespace across all
nodes, by owning a subset of the keyspace locally and forwarding requests
for everything else to whichever peer owns it.

Scope of this initial framework, stated plainly:

  - Ownership is static hash-based partitioning over a FIXED node list
    given at construction time. There is no membership protocol, no
    rebalancing when a node joins/leaves, and no consistent-hashing ring
    (which would make rebalancing touch O(1/n) of the keyspace instead of
    ~all of it). A production version needs one; this framework does not
    include it.
  - There is no replication. Each key lives on exactly one node. If that
    node is down, the key is unavailable -- there is no redundancy layer
    here. That is the next thing to build on top of this, not something
    silently assumed to already exist.
  - Ownership changes are NOT handled online: if you reconstruct a PoolNode
    with a different `nodes` list, previously-placed keys are not moved,
    they are just no longer found at their old owner by the new hash
    function. Call rehash_and_migrate() (not implemented here) before
    changing membership in a real deployment.

None of that is hidden -- it is exactly what "initial framework" means.
"""
from __future__ import annotations

import hashlib
import itertools

from roce.protocol import Frame, Op
from roce.transport_base import Transport
from storage_engine.base import (
    ObjectMeta, ObjectNotFound, PutResult, StorageEngine, StorageError, VersionConflict,
)

_request_ids = itertools.count(1)


class PoolError(Exception):
    pass


class RemoteError(PoolError):
    """The owning peer's handler reported an error; `code` is one of the
    ProtocolError-adjacent string names, not a raw exception -- across a
    network boundary we only trust a small closed vocabulary of failures."""
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def _owner_index(key: str, node_count: int) -> int:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % node_count


class PoolNode:
    def __init__(self, *, local_address: str, nodes: list[str], engine: StorageEngine, transport: Transport):
        if local_address not in nodes:
            raise ValueError("local_address must be a member of nodes")
        self.local_address = local_address
        self.nodes = sorted(nodes)  # sorted so every node computes the same owner for a key
        self.engine = engine
        self.transport = transport
        self._server_handle = None

    # -- lifecycle -----------------------------------------------------
    def start(self) -> str:
        """Begin accepting forwarded requests from peers. Returns the
        actual bound address (useful when local_address used port 0)."""
        self._server_handle = self.transport.serve(self.local_address, self._handle_peer_request)
        return self._server_handle.local_address()

    def stop(self) -> None:
        if self._server_handle is not None:
            self._server_handle.stop()
            self._server_handle = None

    def owner_of(self, key: str) -> str:
        return self.nodes[_owner_index(key, len(self.nodes))]

    def is_local(self, key: str) -> bool:
        return self.owner_of(key) == self.local_address

    # -- client-facing pool operations ----------------------------------
    def put(self, key: str, data: bytes, *, expected_version: int | None = None) -> PutResult:
        if self.is_local(key):
            return self.engine.put(key, data, expected_version=expected_version)
        version_bytes = b"" if expected_version is None else str(expected_version).encode()
        frame = Frame(op=Op.PUT, request_id=next(_request_ids), key=key,
                       value=_pack_put_value(data, expected_version))
        response = self._forward(key, frame)
        if response.op == Op.ERROR:
            _raise_remote(response)
        version, size = _unpack_put_ok_value(response.value)
        return PutResult(key=key, version=version, size=size)

    def get(self, key: str) -> bytes:
        if self.is_local(key):
            return self.engine.get(key)
        response = self._forward(key, Frame(op=Op.GET, request_id=next(_request_ids), key=key))
        if response.op == Op.ERROR:
            _raise_remote(response)
        return response.value

    def delete(self, key: str) -> None:
        if self.is_local(key):
            self.engine.delete(key)
            return
        response = self._forward(key, Frame(op=Op.DELETE, request_id=next(_request_ids), key=key))
        if response.op == Op.ERROR:
            _raise_remote(response)

    def stat(self, key: str) -> ObjectMeta:
        if self.is_local(key):
            return self.engine.stat(key)
        response = self._forward(key, Frame(op=Op.STAT, request_id=next(_request_ids), key=key))
        if response.op == Op.ERROR:
            _raise_remote(response)
        return _unpack_stat_value(key, response.value)

    def exists(self, key: str) -> bool:
        """Implemented in terms of stat() -- deliberately no separate wire
        op, since STAT already round-trips to the owning node and an
        ObjectNotFound response is unambiguous."""
        try:
            self.stat(key)
            return True
        except ObjectNotFound:
            return False

    def list_prefix(self, prefix: str) -> list[ObjectMeta]:
        """Unlike put/get/delete/stat, a prefix listing cannot be routed to
        a single owner: keys under one directory prefix are scattered
        across the cluster by hash, not co-located. This fans the query
        out to every node (including self, handled locally with no network
        hop) and merges the results.

        This is the one pool operation whose cost scales with cluster size
        rather than being O(1) hops -- a real deployment with many nodes
        would want a smarter index rather than an every-node broadcast for
        every directory listing. Left as-is for this initial framework;
        noted here rather than hidden.
        """
        merged: list[ObjectMeta] = []
        for node_address in self.nodes:
            if node_address == self.local_address:
                merged.extend(self.engine.list_prefix(prefix))
                continue
            response = self.transport.send_request(
                node_address, Frame(op=Op.LIST_PREFIX, request_id=next(_request_ids), key=prefix))
            if response.op == Op.ERROR:
                _raise_remote(response)
            merged.extend(_unpack_list_prefix_value(response.value))
        merged.sort(key=lambda m: m.key)
        return merged

    def _forward(self, key: str, frame: Frame) -> Frame:
        owner = self.owner_of(key)
        return self.transport.send_request(owner, frame)

    # -- server side: handle a request forwarded from a peer -------------
    def _handle_peer_request(self, frame: Frame) -> Frame:
        try:
            if frame.op == Op.PUT:
                if frame.value[:1] == b"\x02":  # token CAS (see Op.VTOKEN)
                    result = self.engine.put_token(frame.key, frame.value[9:],
                                                   int.from_bytes(frame.value[1:9], "big"))
                else:
                    data, expected_version = _unpack_put_value(frame.value)
                    result = self.engine.put(frame.key, data, expected_version=expected_version)
                return Frame(op=Op.PUT_OK, request_id=frame.request_id, key=frame.key,
                             value=_pack_put_ok_value(result.version, result.size))
            if frame.op == Op.GET:
                data = self.engine.get(frame.key)
                return Frame(op=Op.GET_OK, request_id=frame.request_id, key=frame.key, value=data)
            if frame.op == Op.DELETE:
                self.engine.delete(frame.key)
                return Frame(op=Op.DELETE_OK, request_id=frame.request_id, key=frame.key)
            if frame.op == Op.STAT:
                meta = self.engine.stat(frame.key)
                return Frame(op=Op.STAT_OK, request_id=frame.request_id, key=frame.key,
                             value=_pack_stat_value(meta))
            if frame.op == Op.LIST_PREFIX:
                metas = self.engine.list_prefix(frame.key)
                return Frame(op=Op.LIST_PREFIX_OK, request_id=frame.request_id, key=frame.key,
                             value=_pack_list_prefix_value(metas))
            # thick-KV ops (encodings in roce/protocol.py), same as the native OST
            if frame.op == Op.WRITE_AT:
                off = int.from_bytes(frame.value[:8], "little")
                r = self.engine.write_at(frame.key, off, frame.value[8:])
                return Frame(op=Op.WRITE_AT_OK, request_id=frame.request_id, key=frame.key,
                             value=_pack_put_ok_value(r.version, r.size))
            if frame.op in (Op.GET_V, Op.READ_AT):
                if frame.op == Op.GET_V:
                    version, data = self.engine.get_versioned(frame.key)
                else:
                    off = int.from_bytes(frame.value[:8], "little")
                    ln = int.from_bytes(frame.value[8:12], "little")
                    version, data = self.engine.read_at(frame.key, off, ln)
                return Frame(op=Op(frame.op + 1), request_id=frame.request_id, key=frame.key,
                             value=version.to_bytes(8, "big") + bytes(data))
            if frame.op == Op.TRUNCATE:
                r = self.engine.truncate_object(frame.key, int.from_bytes(frame.value[:8], "little"))
                return Frame(op=Op.TRUNCATE_OK, request_id=frame.request_id, key=frame.key,
                             value=_pack_put_ok_value(r.version, r.size))
            if frame.op == Op.VTOKEN:
                token = self.engine.version_token(frame.key)
                return Frame(op=Op.VTOKEN_OK, request_id=frame.request_id, key=frame.key,
                             value=token.to_bytes(8, "big"))
            if frame.op == Op.BATCH:
                try:  # validate the whole frame before running any of it
                    subs = unpack_batch_request(frame.value)
                except (IndexError, ValueError, UnicodeDecodeError):
                    subs = None
                if subs is None or any(op == Op.BATCH or op not in Op._value2member_map_ for op, _, _ in subs):
                    return _error_frame(frame, "STORAGE_ERROR", "malformed BATCH")
                replies = []
                for op, key, value in subs:
                    r = self._handle_peer_request(Frame(op=Op(op), request_id=frame.request_id, key=key,
                                                        value=value))
                    replies.append((int(r.op), r.value))
                return Frame(op=Op.BATCH_OK, request_id=frame.request_id, key=frame.key,
                             value=pack_batch_reply(replies))
            return _error_frame(frame, "UNSUPPORTED_OP", f"unsupported op {frame.op}")
        except ObjectNotFound:
            return _error_frame(frame, "NOT_FOUND", f"no such object: {frame.key}")
        except VersionConflict as exc:
            return _error_frame(frame, "VERSION_CONFLICT", f"{exc.expected}:{exc.actual}")
        except StorageError as exc:
            return _error_frame(frame, "STORAGE_ERROR", str(exc))


def pack_batch_request(subs: list[tuple[int, str, bytes]]) -> bytes:
    out = bytearray(len(subs).to_bytes(4, "little"))
    for op, key, value in subs:
        k = key.encode("utf-8")
        out += bytes([int(op)]) + len(k).to_bytes(4, "little") + len(value).to_bytes(4, "little") + k + value
    return bytes(out)


def unpack_batch_request(buf: bytes) -> list[tuple[int, str, bytes]]:
    n, off, out = int.from_bytes(buf[:4], "little"), 4, []
    for _ in range(n):
        op = buf[off]
        kl = int.from_bytes(buf[off + 1:off + 5], "little")
        vl = int.from_bytes(buf[off + 5:off + 9], "little")
        off += 9
        if off + kl + vl > len(buf):
            raise ValueError("truncated BATCH")
        out.append((op, buf[off:off + kl].decode("utf-8"), bytes(buf[off + kl:off + kl + vl])))
        off += kl + vl
    return out


def pack_batch_reply(replies: list[tuple[int, bytes]]) -> bytes:
    out = bytearray(len(replies).to_bytes(4, "little"))
    for op, value in replies:
        out += bytes([op]) + len(value).to_bytes(4, "little") + value
    return bytes(out)


def unpack_batch_reply(buf: bytes) -> list[tuple[int, bytes]]:
    n, off, out = int.from_bytes(buf[:4], "little"), 4, []
    for _ in range(n):
        op = buf[off]
        vl = int.from_bytes(buf[off + 1:off + 5], "little")
        out.append((op, bytes(buf[off + 5:off + 5 + vl])))
        off += 5 + vl
    return out


def _error_frame(frame: Frame, code: str, message: str) -> Frame:
    return Frame(op=Op.ERROR, request_id=frame.request_id, key=frame.key,
                  value=f"{code}\x00{message}".encode("utf-8"))


class RemoteVersionConflict(VersionConflict, RemoteError):
    """A CAS rejected by the key's owning peer. It IS a VersionConflict --
    so `put(expected_version=...)` raises the same type whether the key
    lives on this node or another one (callers such as
    fs/striping.py's _bump_total_size CAS loop catch VersionConflict and
    retry) -- and is still a RemoteError for callers that catch that.

    Found by running PoolNode across two real network nodes
    (tools/run_crossover_rdma.py): the remote path used to raise a bare
    RemoteError, so the exception type depended on which node happened to
    own the key."""

    def __init__(self, key: str, expected: int, actual: int):
        # Bypass both parents' cooperative __init__ (their super() chains
        # would dispatch into each other through the MRO); set fields directly.
        Exception.__init__(self, f"VERSION_CONFLICT: version conflict on {key!r}: expected {expected}, found {actual}")
        self.key, self.expected, self.actual = key, expected, actual
        self.code = "VERSION_CONFLICT"


def _raise_remote(frame: Frame) -> None:
    code, _, message = frame.value.decode("utf-8").partition("\x00")
    if code == "NOT_FOUND":
        raise ObjectNotFound(frame.key)
    if code == "VERSION_CONFLICT":
        expected, _, actual = message.partition(":")
        raise RemoteVersionConflict(frame.key, int(expected), int(actual))
    raise RemoteError(code, message)


# -- small binary sub-encodings carried inside Frame.value ----------------
# (kept deliberately simple/explicit rather than pulling in a serialization
# library, since the whole point of this layer is to stay auditable)

def _pack_put_value(data: bytes, expected_version: int | None) -> bytes:
    marker = b"\x01" if expected_version is not None else b"\x00"
    version_bytes = (expected_version or 0).to_bytes(8, "big")
    return marker + version_bytes + data


def _unpack_put_value(value: bytes) -> tuple[bytes, int | None]:
    marker, version_bytes, data = value[:1], value[1:9], value[9:]
    expected_version = int.from_bytes(version_bytes, "big") if marker == b"\x01" else None
    return data, expected_version


def _pack_put_ok_value(version: int, size: int) -> bytes:
    return version.to_bytes(8, "big") + size.to_bytes(8, "big")


def _unpack_put_ok_value(value: bytes) -> tuple[int, int]:
    return int.from_bytes(value[:8], "big"), int.from_bytes(value[8:16], "big")


def _pack_stat_value(meta: ObjectMeta) -> bytes:
    import json
    return json.dumps({"size": meta.size, "created_at": meta.created_at,
                        "updated_at": meta.updated_at, "version": meta.version,
                        "is_dir": meta.is_dir}).encode("utf-8")


def _unpack_stat_value(key: str, value: bytes) -> ObjectMeta:
    import json
    d = json.loads(value.decode("utf-8"))
    return ObjectMeta(key=key, size=d["size"], created_at=d["created_at"],
                       updated_at=d["updated_at"], version=d["version"], is_dir=d["is_dir"])


def _pack_list_prefix_value(metas: list[ObjectMeta]) -> bytes:
    import json
    return json.dumps([{"key": m.key, "size": m.size, "created_at": m.created_at,
                         "updated_at": m.updated_at, "version": m.version, "is_dir": m.is_dir}
                        for m in metas]).encode("utf-8")


def _unpack_list_prefix_value(value: bytes) -> list[ObjectMeta]:
    import json
    rows = json.loads(value.decode("utf-8"))
    return [ObjectMeta(key=r["key"], size=r["size"], created_at=r["created_at"],
                        updated_at=r["updated_at"], version=r["version"], is_dir=r["is_dir"])
            for r in rows]
