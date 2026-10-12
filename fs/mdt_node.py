"""MDT: a dedicated metadata server role, separate from the OSTs that hold
file data (the Lustre MDS/MDT vs OSS/OST split).

What lives here: the namespace (which names exist in which directory,
file-vs-directory), per-file size/times, and each file's stripe layout.
What does NOT live here: any file bytes -- those are stripe objects on the
OSTs (roce/pool_node.py serving a single-node keyspace, unchanged).

Why a separate role instead of more PoolNode keys: on the original POSIX
path every metadata question was answered by generic KV get/stat calls on
whatever node the key hash picked (a file's existence = stat of the file
key AND stat of a `<path>/.dir` marker key; a directory listing = a
list_prefix broadcast to EVERY node). Here each metadata operation is ONE
request to ONE server that answers it from an in-memory dict under a lock
-- including operations that were read-modify-write CAS loops before
(growing a file's size to max(current, end) used to be stat+get+put with
retries on OST[0]; `set_size` does it server-side atomically).

Concurrency model: the transport's server side already runs one thread
per connection (roce_transport.c `accept_loop`/`serve_one_connection`,
LoopbackTransport likewise); all handler work is a few dict operations
under one lock, so the lock is held for microseconds. Scale-out across
cores/nodes comes from running SEVERAL MDT processes, each owning a shard
of the namespace (fs/namespace_shard.py) -- one Python process per MDT
means one GIL per MDT, not one GIL for the whole metadata service.
"""
from __future__ import annotations

import concurrent.futures

import itertools
import os
import threading
import time

from fs import mdt_wire
from fs.namespace_shard import NamespaceShard, name_of, normalize, parent_of
from roce.protocol import Frame, Op
from roce.transport_base import Transport

_ids = itertools.count(1)


class MDTError(Exception):
    def __init__(self, code: str, message: str = ""):
        super().__init__(f"{code}: {message}")
        self.code = code


def _entry(is_dir: bool, layout: dict | None, now: float) -> dict:
    return {"is_dir": is_dir, "size": 0, "mtime": now, "ctime": now, "layout": layout,
            "inline": False, "frozen": False, "data": b"", "iver": 0}


def inline_limit(e: dict) -> int:
    """ROCEPOOL_INLINE_MAX (default 32 KiB, 0 disables), never more than one
    stripe -- same rule as the native MDT (roce/native/rocepool_servers.c)."""
    lim = max(0, int(os.environ.get("ROCEPOOL_INLINE_MAX", "32768") or 0))
    lay = e.get("layout")
    return min(lim, lay["stripe_size"]) if lay and lay.get("stripe_size") else lim


def _public(e: dict) -> dict:
    return {k: v for k, v in e.items() if k not in ("data", "iver")}


class MDTServer:
    def __init__(self, transport: Transport):
        self.transport = transport
        self._lock = threading.Lock()
        # parent dir path -> {child name -> entry}. Only parents that hash to
        # THIS MDT ever appear here (the client routes by namespace_shard).
        self._children: dict[str, dict[str, dict]] = {}
        self._handle = None
        self.ops_served = 0

    def start(self, bind_address: str) -> str:
        self._handle = self.transport.serve(bind_address, self._handle_request)
        return self._handle.local_address()

    def stop(self) -> None:
        if self._handle is not None:
            self._handle.stop()
            self._handle = None

    # -- request dispatch --
    def _handle_request(self, frame: Frame) -> Frame:
        if frame.op != Op.MDT:
            return _err(frame, "UNSUPPORTED_OP", str(frame.op))
        try:
            req = mdt_wire.decode_request(frame.key, frame.value)
            result = self.apply(req)
            if req["op"] == "readdir":
                body = mdt_wire.encode_readdir(result)
            elif req["op"] == "child_count":
                body = mdt_wire.encode_count(result)
            elif req["op"] in ("read_inline", "freeze"):
                body = mdt_wire.encode_inline(*result)
            else:
                body = mdt_wire.encode_entry(result)
            return Frame(op=Op.MDT_OK, request_id=frame.request_id, key=frame.key, value=body)
        except MDTError as exc:
            return _err(frame, exc.code, str(exc))
        except Exception as exc:  # noqa: BLE001 -- never let one bad request kill the server thread
            return _err(frame, "MDT_INTERNAL", repr(exc))

    def apply(self, req: dict):
        """The whole metadata state machine, callable directly (tests) or via
        the wire. Every branch runs under one lock: each op is atomic."""
        op = req["op"]
        with self._lock:
            self.ops_served += 1
            if op == "lookup":
                path = normalize(req["path"])
                e = self._children.get(parent_of(path), {}).get(name_of(path))
                if e is None:
                    raise MDTError("NOT_FOUND", path)
                return _public(e)
            if op == "create":
                path = normalize(req["path"])
                siblings = self._children.setdefault(parent_of(path), {})
                existing = siblings.get(name_of(path))
                if existing is not None:
                    if req.get("exclusive", True) or existing["is_dir"] != req["is_dir"]:
                        raise MDTError("EXISTS", path)
                    return _public(existing)
                e = _entry(req["is_dir"], req.get("layout"), time.time())
                e["inline"] = not req["is_dir"] and bool(req.get("inline_ok")) and inline_limit(e) > 0
                siblings[name_of(path)] = e
                return _public(e)
            if op == "set_size":
                path = normalize(req["path"])
                e = self._children.get(parent_of(path), {}).get(name_of(path))
                if e is None:
                    raise MDTError("NOT_FOUND", path)
                if e["is_dir"]:
                    raise MDTError("IS_DIR", path)
                want = max(e["size"], req["size"]) if req.get("mode", "max") == "max" else req["size"]
                if e["inline"]:  # the inline bytes ARE the file: resize them (truncate only)
                    if e["frozen"]:
                        raise MDTError("FROZEN", path)
                    if req.get("mode", "max") == "max":
                        # a size-growing write went to the OSTs on stale cached
                        # attributes: tell the writer to redo it inline
                        raise MDTError("INLINE", path)
                    if want > inline_limit(e):
                        raise MDTError("INLINE_FULL", path)
                    e["data"] = e["data"][:want] + b"\x00" * max(0, want - len(e["data"]))
                    e["iver"] += 1
                e["size"] = want
                e["mtime"] = time.time()
                return _public(e)
            if op == "unlink":
                path = normalize(req["path"])
                siblings = self._children.get(parent_of(path), {})
                e = siblings.get(name_of(path))
                if e is None:
                    raise MDTError("NOT_FOUND", path)
                if e["is_dir"] != req["is_dir"]:
                    raise MDTError("IS_DIR" if e["is_dir"] else "NOT_DIR", path)
                del siblings[name_of(path)]
                return _public(e)
            if op == "readdir":
                d = normalize(req["path"])
                return [dict(_public(v), name=k) for k, v in sorted(self._children.get(d, {}).items())]
            if op == "child_count":
                return len(self._children.get(normalize(req["path"]), {}))
            if op in ("write_inline", "read_inline", "freeze", "spill_commit"):
                return self._inline_op(op, req)
            raise MDTError("UNSUPPORTED_OP", op)

    def _inline_op(self, op: str, req: dict):
        """Data-on-MDT (protocol in fs/mdt_wire.py). Caller holds the lock."""
        path = normalize(req["path"])
        e = self._children.get(parent_of(path), {}).get(name_of(path))
        if e is None:
            raise MDTError("NOT_FOUND", path)
        if e["is_dir"]:
            raise MDTError("IS_DIR", path)
        if op == "read_inline":
            off, ln = req["offset"], req["length"]
            return e["iver"], _public(e), (e["data"][off:off + ln] if e["inline"] else b"")
        if op == "freeze":
            if e["inline"]:
                e["frozen"] = True  # inline bytes immutable from here on
            return e["iver"], _public(e), (e["data"] if e["inline"] else b"")
        if op == "spill_commit":
            if e["inline"] and not e["frozen"]:
                raise MDTError("NOT_FROZEN", path)
            if e["inline"]:
                e["inline"] = e["frozen"] = False
                e["data"] = b""
            return _public(e)
        # write_inline
        if not e["inline"]:
            raise MDTError("NOT_INLINE", path)
        if e["frozen"]:
            raise MDTError("FROZEN", path)
        off, data = req["offset"], req["data"]
        end = off + len(data)
        if end > inline_limit(e):
            raise MDTError("INLINE_FULL", path)
        buf = bytearray(e["data"])
        if len(buf) < end:
            buf.extend(b"\x00" * (end - len(buf)))
        buf[off:end] = data
        e["data"] = bytes(buf)
        e["size"] = len(buf)
        e["iver"] += 1
        e["mtime"] = time.time()
        return _public(e)


def _err(frame: Frame, code: str, message: str) -> Frame:
    return Frame(op=Op.ERROR, request_id=frame.request_id, key=frame.key,
                 value=f"{code}\x00{message}".encode())


class MDTClient:
    """Routes each metadata op to the MDT that owns it (fs/namespace_shard.py)."""

    def __init__(self, transport: Transport, shard: NamespaceShard):
        self.transport = transport
        self.shard = shard
        self.rpcs = 0

    def _call(self, mdt: str, req: dict):
        self.rpcs += 1
        resp = self.transport.send_request(
            mdt, Frame(op=Op.MDT, request_id=next(_ids), key=req.get("path", ""),
                       value=mdt_wire.encode_request(req)))
        if resp.op == Op.ERROR:
            code, _, msg = resp.value.decode().partition("\x00")
            raise MDTError(code, msg)
        if req["op"] == "readdir":
            return mdt_wire.decode_readdir(resp.value)
        if req["op"] == "child_count":
            return mdt_wire.decode_count(resp.value)
        if req["op"] in ("read_inline", "freeze"):
            return mdt_wire.decode_inline(resp.value)
        return mdt_wire.decode_entry(resp.value)[0]

    def lookup(self, path: str) -> dict | None:
        try:
            return self._call(self.shard.mdt_for(path), {"op": "lookup", "path": path})
        except MDTError as exc:
            if exc.code == "NOT_FOUND":
                return None
            raise

    def create(self, path: str, *, is_dir: bool, layout: dict | None, exclusive: bool = True,
               inline_ok: bool = False) -> dict:
        return self._call(self.shard.mdt_for(path),
                          {"op": "create", "path": path, "is_dir": is_dir, "layout": layout, "exclusive": exclusive,
                           "inline_ok": inline_ok})

    # -- Data-on-MDT (fs/mdt_wire.py) --
    def write_inline(self, path: str, offset: int, data: bytes) -> dict:
        return self._call(self.shard.mdt_for(path), {"op": "write_inline", "path": path, "offset": offset,
                                                     "data": data})

    def read_inline(self, path: str, offset: int, length: int) -> tuple[int, dict, bytes]:
        return self._call(self.shard.mdt_for(path), {"op": "read_inline", "path": path, "offset": offset,
                                                     "length": length})

    def freeze(self, path: str) -> tuple[int, dict, bytes]:
        return self._call(self.shard.mdt_for(path), {"op": "freeze", "path": path})

    def spill_commit(self, path: str) -> dict:
        return self._call(self.shard.mdt_for(path), {"op": "spill_commit", "path": path})

    def set_size(self, path: str, size: int, *, mode: str = "max") -> dict:
        return self._call(self.shard.mdt_for(path), {"op": "set_size", "path": path, "size": size, "mode": mode})

    def unlink(self, path: str, *, is_dir: bool) -> dict:
        return self._call(self.shard.mdt_for(path), {"op": "unlink", "path": path, "is_dir": is_dir})

    def _fan_out(self, req: dict) -> list:
        """The same request to every MDT holding part of a directory, in
        parallel (striped directories); one call when it is a single MDT."""
        mdts = self.shard.mdts_for_children(req["path"])
        if len(mdts) == 1:
            return [self._call(mdts[0], req)]
        with concurrent.futures.ThreadPoolExecutor(len(mdts)) as ex:
            return list(ex.map(lambda m: self._call(m, req), mdts))

    def readdir(self, dir_path: str) -> list[dict]:
        parts = self._fan_out({"op": "readdir", "path": dir_path})
        if len(parts) == 1:
            return parts[0]
        return sorted((e for part in parts for e in part), key=lambda e: e["name"])

    def child_count(self, dir_path: str) -> int:
        return sum(self._fan_out({"op": "child_count", "path": dir_path}))
