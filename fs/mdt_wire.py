"""Binary wire format for MDT requests/responses (carried in a Frame with
op=MDT / MDT_OK; the path travels in Frame.key).

Fixed little-endian layouts, no JSON: the same bytes are produced and
parsed by fs/mdt_node.py (Python MDT, kept as the reference
implementation) and roce/native/rocepool_servers.c (native C MDT).
Changing a layout here means changing it there too;
tests/test_native_servers.py runs one test suite against both.

Request value:
  u8 op   1 lookup | 2 create | 3 set_size | 4 unlink | 5 readdir | 6 child_count
          7 write_inline | 8 read_inline | 9 freeze | 10 spill_commit
  create:       u8 is_dir, u8 exclusive, u8 flags (bit0 has_layout, bit1 inline_ok),
                [u32 stripe_count, u64 stripe_size, u32 stripe_offset]
  set_size:     u8 mode (0 = max, 1 = exact), u64 size
  unlink:       u8 is_dir
  write_inline: u64 offset, data
  read_inline:  u64 offset, u32 length

Response value:
  entry:        u8 is_dir, u8 flags (bit0 has_layout, bit1 inline, bit2 frozen),
                u64 size, f64 mtime, f64 ctime,
                [u32 stripe_count, u64 stripe_size, u32 stripe_offset]
  read_inline / freeze: u64 inline_version, entry, inline bytes (if inline)

Data-on-MDT (inline small files). A file created with inline_ok keeps its
bytes IN its MDT entry (size == len(bytes)) while it fits the inline limit
(server env ROCEPOOL_INLINE_MAX, default 32 KiB, at most one stripe): one
MDT round trip creates+writes it, one reads it -- no OST involved
(mdtest-hard's 3901-byte files). Growing past the limit (INLINE_FULL) moves
it to the OSTs without a lock and without losing concurrent writes:
  1. freeze        -> the inline bytes become immutable (writes now get
                      FROZEN); returns them. Idempotent.
  2. read stripe 0's OST version v0 (0 if absent), re-check the entry is
     still frozen (freeze again: idempotent), then PUT stripe 0 = the frozen
     bytes with expected_version=v0. A VERSION_CONFLICT means something
     wrote stripe 0 in between -- a helper, or a writer after another
     helper's commit -- so go back to step 2 (which then sees the commit and
     stops). Checking the MDT after reading v0 is what makes it safe: any
     post-commit write bumps the version past v0.
  3. spill_commit  -> entry becomes a normal striped file. Idempotent.
Any client that sees FROZEN runs 1-3 itself, so a crashed spiller never
blocks a file. set_size(max) on an inline entry answers INLINE: a writer
whose cached attributes wrongly said "striped" redoes its write inline.
  readdir:      u32 count, then count x (u16 name_len, name bytes, entry)
  child_count:  u64
Errors: Frame op=ERROR, value "CODE\\0message" (NOT_FOUND, EXISTS, IS_DIR,
NOT_DIR, UNSUPPORTED_OP, MDT_INTERNAL).
"""
from __future__ import annotations

import struct

OP_LOOKUP, OP_CREATE, OP_SET_SIZE, OP_UNLINK, OP_READDIR, OP_CHILD_COUNT = 1, 2, 3, 4, 5, 6
OP_WRITE_INLINE, OP_READ_INLINE, OP_FREEZE, OP_SPILL_COMMIT = 7, 8, 9, 10
_OP_NAMES = {OP_LOOKUP: "lookup", OP_CREATE: "create", OP_SET_SIZE: "set_size", OP_UNLINK: "unlink",
             OP_READDIR: "readdir", OP_CHILD_COUNT: "child_count", OP_WRITE_INLINE: "write_inline",
             OP_READ_INLINE: "read_inline", OP_FREEZE: "freeze", OP_SPILL_COMMIT: "spill_commit"}
_OP_CODES = {v: k for k, v in _OP_NAMES.items()}

_ENTRY_HEAD = struct.Struct("<BBQdd")
_LAYOUT = struct.Struct("<IQI")
_SET_SIZE = struct.Struct("<BQ")
_U16 = struct.Struct("<H")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")


def _layout_bytes(layout: dict | None, inline_ok: bool = False) -> bytes:
    flags = 2 if inline_ok else 0
    if not layout:
        return bytes([flags])
    return bytes([flags | 1]) + _LAYOUT.pack(layout["stripe_count"], layout["stripe_size"], layout["stripe_offset"])


def encode_request(req: dict) -> bytes:
    op = _OP_CODES[req["op"]]
    if op == OP_CREATE:
        return bytes([op, 1 if req["is_dir"] else 0, 1 if req.get("exclusive", True) else 0]) \
            + _layout_bytes(req.get("layout"), req.get("inline_ok", False))
    if op == OP_SET_SIZE:
        return bytes([op]) + _SET_SIZE.pack(0 if req.get("mode", "max") == "max" else 1, req["size"])
    if op == OP_UNLINK:
        return bytes([op, 1 if req["is_dir"] else 0])
    if op == OP_WRITE_INLINE:
        return bytes([op]) + _U64.pack(req["offset"]) + req["data"]
    if op == OP_READ_INLINE:
        return bytes([op]) + _U64.pack(req["offset"]) + _U32.pack(req["length"])
    return bytes([op])


def decode_request(path: str, value: bytes) -> dict:
    op = value[0]
    req = {"op": _OP_NAMES.get(op, f"unknown-{op}"), "path": path}
    if op == OP_CREATE:
        req["is_dir"] = bool(value[1])
        req["exclusive"] = bool(value[2])
        req["inline_ok"] = bool(value[3] & 2)
        if value[3] & 1:
            sc, ss, so = _LAYOUT.unpack_from(value, 4)
            req["layout"] = {"stripe_count": sc, "stripe_size": ss, "stripe_offset": so}
        else:
            req["layout"] = None
    elif op == OP_SET_SIZE:
        mode, size = _SET_SIZE.unpack_from(value, 1)
        req["mode"] = "max" if mode == 0 else "exact"
        req["size"] = size
    elif op == OP_UNLINK:
        req["is_dir"] = bool(value[1])
    elif op == OP_WRITE_INLINE:
        req["offset"] = _U64.unpack_from(value, 1)[0]
        req["data"] = bytes(value[9:])
    elif op == OP_READ_INLINE:
        req["offset"] = _U64.unpack_from(value, 1)[0]
        req["length"] = _U32.unpack_from(value, 9)[0]
    return req


def encode_entry(e: dict) -> bytes:
    lay = e.get("layout")
    flags = (1 if lay else 0) | (2 if e.get("inline") else 0) | (4 if e.get("frozen") else 0)
    head = _ENTRY_HEAD.pack(1 if e["is_dir"] else 0, flags, e["size"], e["mtime"], e["ctime"])
    return head + (_LAYOUT.pack(lay["stripe_count"], lay["stripe_size"], lay["stripe_offset"]) if lay else b"")


def decode_entry(buf: bytes, off: int = 0) -> tuple[dict, int]:
    is_dir, flags, size, mtime, ctime = _ENTRY_HEAD.unpack_from(buf, off)
    off += _ENTRY_HEAD.size
    layout = None
    if flags & 1:
        sc, ss, so = _LAYOUT.unpack_from(buf, off)
        off += _LAYOUT.size
        layout = {"stripe_count": sc, "stripe_size": ss, "stripe_offset": so}
    return {"is_dir": bool(is_dir), "size": size, "mtime": mtime, "ctime": ctime, "layout": layout,
            "inline": bool(flags & 2), "frozen": bool(flags & 4)}, off


def encode_inline(iver: int, e: dict, data: bytes) -> bytes:
    return _U64.pack(iver) + encode_entry(e) + data


def decode_inline(buf: bytes) -> tuple[int, dict, bytes]:
    (iver,) = _U64.unpack_from(buf, 0)
    e, off = decode_entry(buf, 8)
    return iver, e, bytes(buf[off:])


def encode_readdir(entries: list[dict]) -> bytes:
    out = bytearray(_U32.pack(len(entries)))
    for e in entries:
        name = e["name"].encode()
        out += _U16.pack(len(name)) + name + encode_entry(e)
    return bytes(out)


def decode_readdir(buf: bytes) -> list[dict]:
    (n,) = _U32.unpack_from(buf, 0)
    off, out = 4, []
    for _ in range(n):
        (ln,) = _U16.unpack_from(buf, off)
        off += 2
        name = buf[off:off + ln].decode()
        off += ln
        e, off = decode_entry(buf, off)
        e["name"] = name
        out.append(e)
    return out


def encode_count(n: int) -> bytes:
    return _U64.pack(n)


def decode_count(buf: bytes) -> int:
    return _U64.unpack_from(buf, 0)[0]
