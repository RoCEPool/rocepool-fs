"""Minimal, from-spec Apache Thrift Compact Protocol codec (read + write),
plus a tiny declarative struct layer on top of it.

Why hand-rolled instead of `pip install thrift`: the `thrift` PyPI package
is not obtainable in this sandbox (confirmed -- `pip install thrift` fails
with "No matching distribution found", while ordinary packages like
`requests` install fine, so this is specific to that package, not a
general network outage). Rather than block on that, this implements the
subset of the publicly documented Compact Protocol wire format
(THRIFT-110) actually needed by RapidsSE's own WLP v5 message definitions
(bool, i16, i32, i64, double, string/binary, struct, list-of-struct,
enum-as-i32) -- verified against the real `.thrift` IDL files shipped in
the uploaded RapidsSE source
(`src/wlp/thrift/messageDefinitions/{compatibility,v5}/*.thrift`) and
against how RapidsSE's own C++ server actually frames a message
(`src/store/AdaptorWL.cpp::vSerialiseAndSendMsg`, `src/wlp/thrift/supports/
WlpHeader.hpp`, `BufferTransport.hpp`): an 8-byte header (4-byte
big-endian message size, 1-byte message type, 3 bytes unused) followed by
the Thrift struct written directly with TCompactProtocol -- no separate
Thrift message envelope (no method name / seqid), confirmed by
`pMsg->write(m_pProtocol.get())` being called directly rather than
`writeMessageBegin(...)`.

TESTED how: tests/test_compact_protocol.py round-trips every primitive
type and nested struct/list shape this module supports through
encode->decode and asserts byte-for-byte and value-for-value equality;
tests/test_wlp_protocol.py round-trips full WLP messages (with the 8-byte
frame header) the same way. tests/test_rapidsse_wlp_engine.py drives a
real TCP client/server pair (tests/fakes/wlp_test_server.py) speaking this
exact wire format end-to-end. What this does NOT prove: byte-for-byte
compatibility with RapidsSE's actual C++ TCompactProtocol implementation,
since nothing in this sandbox can run that C++ code (no compiler toolchain
exercised against it, no live cluster). This module was written directly
against the public Compact Protocol spec and cross-checked against the
.thrift IDL field IDs/types, which is the strongest verification available
without a running RapidsSE server to test against.
"""
from __future__ import annotations

import struct as _struct
from dataclasses import dataclass, field as dc_field
from typing import Any, ClassVar

# --- Thrift TType ids (as used in the .thrift-derived field specs below) ---
T_BOOL = "bool"
T_I16 = "i16"
T_I32 = "i32"
T_I64 = "i64"
T_DOUBLE = "double"
T_STRING = "string"   # decodes to str on read
T_BINARY = "binary"   # same wire encoding as T_STRING, decodes to raw bytes on read
T_STRUCT = "struct"
T_LIST = "list"        # list of T_STRUCT elements only (all this protocol needs)
T_ENUM = "enum"         # wire-encoded exactly like i32

# --- Compact protocol type ids (distinct from TType ids -- see THRIFT-110) ---
_CT_BOOLEAN_TRUE = 0x01
_CT_BOOLEAN_FALSE = 0x02
_CT_I16 = 0x04
_CT_I32 = 0x05
_CT_I64 = 0x06
_CT_DOUBLE = 0x07
_CT_BINARY = 0x08
_CT_LIST = 0x09
_CT_STRUCT = 0x0C

_NON_BOOL_CTYPE = {
    T_I16: _CT_I16, T_I32: _CT_I32, T_I64: _CT_I64, T_ENUM: _CT_I32,
    T_DOUBLE: _CT_DOUBLE, T_STRING: _CT_BINARY, T_BINARY: _CT_BINARY, T_STRUCT: _CT_STRUCT,
    T_LIST: _CT_LIST,
}


def zigzag_encode(n: int) -> int:
    return (n << 1) if n >= 0 else (((-n) << 1) - 1)


def zigzag_decode(z: int) -> int:
    return (z >> 1) if (z & 1) == 0 else -((z + 1) >> 1)


class CompactWriter:
    """Serialises one Thrift struct body (no message envelope) using the
    Compact Protocol. Field-id delta tracking uses a stack so nested
    structs each get their own "last field id" scope, exactly as real
    Thrift generated code does.
    """

    def __init__(self) -> None:
        self._buf = bytearray()
        self._last_id_stack: list[int] = [0]

    def bytes(self) -> bytes:
        return bytes(self._buf)

    # -- primitives --
    def _write_varint(self, n: int) -> None:
        while True:
            byte = n & 0x7F
            n >>= 7
            if n:
                self._buf.append(byte | 0x80)
            else:
                self._buf.append(byte)
                return

    def _write_zigzag(self, n: int) -> None:
        self._write_varint(zigzag_encode(n))

    def _write_byte(self, b: int) -> None:
        self._buf.append(b & 0xFF)

    # -- struct framing --
    def write_struct_begin(self) -> None:
        self._last_id_stack.append(0)

    def write_struct_end(self) -> None:
        self._last_id_stack.pop()

    def write_field_stop(self) -> None:
        self._write_byte(0x00)

    def _write_field_header(self, ttype: str, field_id: int, bool_value: bool | None = None) -> None:
        last_id = self._last_id_stack[-1]
        delta = field_id - last_id
        if ttype == T_BOOL:
            ctype = _CT_BOOLEAN_TRUE if bool_value else _CT_BOOLEAN_FALSE
        else:
            ctype = _NON_BOOL_CTYPE[ttype]
        if 0 < delta <= 15:
            self._write_byte((delta << 4) | ctype)
        else:
            self._write_byte(ctype)
            self._write_zigzag(field_id)
        self._last_id_stack[-1] = field_id

    # -- field writers (each writes header + value, bool is header-only) --
    def write_bool_field(self, field_id: int, value: bool) -> None:
        self._write_field_header(T_BOOL, field_id, bool_value=value)

    def write_i16_field(self, field_id: int, value: int) -> None:
        self._write_field_header(T_I16, field_id)
        self._write_zigzag(value)

    def write_i32_field(self, field_id: int, value: int) -> None:
        self._write_field_header(T_I32, field_id)
        self._write_zigzag(value)

    def write_enum_field(self, field_id: int, value: int) -> None:
        self._write_field_header(T_ENUM, field_id)
        self._write_zigzag(value)

    def write_i64_field(self, field_id: int, value: int) -> None:
        self._write_field_header(T_I64, field_id)
        self._write_zigzag(value)

    def write_double_field(self, field_id: int, value: float) -> None:
        self._write_field_header(T_DOUBLE, field_id)
        self._buf.extend(_struct.pack("<d", value))

    def write_string_field(self, field_id: int, value: str | bytes) -> None:
        self._write_field_header(T_STRING, field_id)
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        self._write_varint(len(raw))
        self._buf.extend(raw)

    def write_binary_field(self, field_id: int, value: bytes) -> None:
        self._write_field_header(T_BINARY, field_id)
        raw = bytes(value)
        self._write_varint(len(raw))
        self._buf.extend(raw)

    def write_struct_field(self, field_id: int, value: "ThriftStruct") -> None:
        self._write_field_header(T_STRUCT, field_id)
        value.write(self)

    def write_list_field(self, field_id: int, values: list) -> None:
        self._write_field_header(T_LIST, field_id)
        self._write_list_body(values)

    def _write_list_body(self, values: list) -> None:
        size = len(values)
        if size < 15:
            self._write_byte((size << 4) | _CT_STRUCT)
        else:
            self._write_byte((15 << 4) | _CT_STRUCT)
            self._write_varint(size)
        for v in values:
            v.write(self)


class CompactReader:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self._last_id_stack: list[int] = [0]

    def remaining(self) -> int:
        return len(self._data) - self._pos

    def _read_byte(self) -> int:
        b = self._data[self._pos]
        self._pos += 1
        return b

    def _read_varint(self) -> int:
        shift = 0
        result = 0
        while True:
            b = self._read_byte()
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                return result
            shift += 7

    def _read_zigzag(self) -> int:
        return zigzag_decode(self._read_varint())

    def read_struct_begin(self) -> None:
        self._last_id_stack.append(0)

    def read_struct_end(self) -> None:
        self._last_id_stack.pop()

    def read_field_header(self) -> tuple[int, int, bool | None]:
        """Returns (field_id, ctype, bool_value_or_None). field_id == -1
        and ctype == 0 signals the STOP marker."""
        byte = self._read_byte()
        if byte == 0x00:
            return -1, 0, None
        ctype = byte & 0x0F
        delta = (byte >> 4) & 0x0F
        if delta == 0:
            field_id = self._read_zigzag()
        else:
            field_id = self._last_id_stack[-1] + delta
        self._last_id_stack[-1] = field_id
        if ctype == _CT_BOOLEAN_TRUE:
            return field_id, T_BOOL, True
        if ctype == _CT_BOOLEAN_FALSE:
            return field_id, T_BOOL, False
        return field_id, ctype, None

    def read_i16(self) -> int:
        return self._read_zigzag()

    def read_i32(self) -> int:
        return self._read_zigzag()

    def read_i64(self) -> int:
        return self._read_zigzag()

    def read_double(self) -> float:
        raw = self._data[self._pos:self._pos + 8]
        self._pos += 8
        return _struct.unpack("<d", raw)[0]

    def read_string(self) -> str:
        return self.read_binary().decode("utf-8")

    def read_binary(self) -> bytes:
        n = self._read_varint()
        raw = self._data[self._pos:self._pos + n]
        self._pos += n
        return bytes(raw)

    def read_list_header(self) -> int:
        byte = self._read_byte()
        size = (byte >> 4) & 0x0F
        # element ctype (byte & 0x0F) is not needed -- every list this
        # protocol uses is a list of structs, per the .thrift IDL.
        if size == 15:
            size = self._read_varint()
        return size


class ThriftStruct:
    """Base class for a generated-style Thrift struct. Subclasses declare
    `_FIELDS: ClassVar[list[tuple[int, str, str, type | None]]]` as
    (field_id, ttype, attr_name, nested_class_or_None), matching field ids
    exactly as declared in the corresponding .thrift file. `write`/`read`
    are generic over that spec -- this mirrors what Thrift's own compiler
    generates, just table-driven instead of code-generated per struct.
    """
    _FIELDS: ClassVar[list[tuple[int, str, str, Any]]] = []

    def write(self, writer: CompactWriter) -> None:
        writer.write_struct_begin()
        for field_id, ttype, attr, _nested in self._FIELDS:
            value = getattr(self, attr, None)
            if value is None:
                continue
            if ttype == T_BOOL:
                writer.write_bool_field(field_id, value)
            elif ttype == T_I16:
                writer.write_i16_field(field_id, value)
            elif ttype == T_I32:
                writer.write_i32_field(field_id, value)
            elif ttype == T_I64:
                writer.write_i64_field(field_id, value)
            elif ttype == T_ENUM:
                writer.write_enum_field(field_id, int(value))
            elif ttype == T_DOUBLE:
                writer.write_double_field(field_id, value)
            elif ttype == T_STRING:
                writer.write_string_field(field_id, value)
            elif ttype == T_BINARY:
                writer.write_binary_field(field_id, value)
            elif ttype == T_STRUCT:
                writer.write_struct_field(field_id, value)
            elif ttype == T_LIST:
                writer.write_list_field(field_id, value)
            else:
                raise ValueError(f"unsupported ttype {ttype!r}")
        writer.write_field_stop()
        writer.write_struct_end()

    @classmethod
    def read(cls, reader: CompactReader) -> "ThriftStruct":
        spec_by_id = {f[0]: f for f in cls._FIELDS}
        obj = cls()
        reader.read_struct_begin()
        while True:
            field_id, ctype_or_ttype, bool_value = reader.read_field_header()
            if field_id == -1:
                break
            spec = spec_by_id.get(field_id)
            if spec is None:
                raise ValueError(f"{cls.__name__}: unknown field id {field_id}")
            _, ttype, attr, nested = spec
            if ttype == T_BOOL:
                setattr(obj, attr, bool_value)
            elif ttype in (T_I16,):
                setattr(obj, attr, reader.read_i16())
            elif ttype in (T_I32, T_ENUM):
                setattr(obj, attr, reader.read_i32())
            elif ttype == T_I64:
                setattr(obj, attr, reader.read_i64())
            elif ttype == T_DOUBLE:
                setattr(obj, attr, reader.read_double())
            elif ttype == T_STRING:
                setattr(obj, attr, reader.read_string())
            elif ttype == T_BINARY:
                setattr(obj, attr, reader.read_binary())
            elif ttype == T_STRUCT:
                setattr(obj, attr, nested.read(reader))
            elif ttype == T_LIST:
                size = reader.read_list_header()
                setattr(obj, attr, [nested.read(reader) for _ in range(size)])
            else:
                raise ValueError(f"unsupported ttype {ttype!r}")
        reader.read_struct_end()
        return obj
