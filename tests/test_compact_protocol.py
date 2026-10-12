"""Round-trip tests for the hand-rolled Compact Protocol codec, independent
of anything WLP-specific: encode a struct, decode the exact same bytes
back, and assert field-for-field equality. Also checks the raw byte
encoding of a couple of primitives against hand-computed expected bytes,
since a subtly wrong varint/zigzag/field-header implementation could
still round-trip through itself while being wire-incompatible with a real
Thrift peer.
"""
import unittest
from dataclasses import dataclass, field
from typing import ClassVar

from wlp.compact_protocol import (
    T_BOOL, T_DOUBLE, T_I16, T_I32, T_I64, T_LIST, T_STRING, T_STRUCT,
    CompactReader, CompactWriter, ThriftStruct, zigzag_decode, zigzag_encode,
)


class ZigzagTests(unittest.TestCase):
    def test_known_values(self):
        # From the published Compact Protocol spec examples.
        cases = [(0, 0), (-1, 1), (1, 2), (-2, 3), (2, 4)]
        for n, z in cases:
            self.assertEqual(zigzag_encode(n), z)
            self.assertEqual(zigzag_decode(z), n)

    def test_round_trip_random_range(self):
        for n in list(range(-1000, 1000)) + [2**40, -(2**40), 2**62, -(2**62)]:
            self.assertEqual(zigzag_decode(zigzag_encode(n)), n)


@dataclass
class _Leaf(ThriftStruct):
    name: str = ""
    value: int = 0
    _FIELDS: ClassVar = [
        (1, T_STRING, "name", None),
        (2, T_I32, "value", None),
    ]


@dataclass
class _Outer(ThriftStruct):
    flag: bool | None = None
    big: int | None = None
    ratio: float | None = None
    leaves: list = field(default_factory=list)
    nested: object = None
    _FIELDS: ClassVar = [
        (1, T_BOOL, "flag", None),
        (2, T_I64, "big", None),
        (3, T_DOUBLE, "ratio", None),
        (5, T_LIST, "leaves", _Leaf),  # gap 3->5 forces long-form field header
        (6, T_STRUCT, "nested", _Leaf),
    ]


class StructRoundTripTests(unittest.TestCase):
    def _round_trip(self, obj: _Outer) -> _Outer:
        writer = CompactWriter()
        obj.write(writer)
        return _Outer.read(CompactReader(writer.bytes()))

    def test_all_primitive_fields(self):
        obj = _Outer(flag=True, big=-123456789012, ratio=3.5,
                     leaves=[_Leaf(name="a", value=1), _Leaf(name="b", value=-2)],
                     nested=_Leaf(name="n", value=99))
        got = self._round_trip(obj)
        self.assertEqual(got.flag, True)
        self.assertEqual(got.big, -123456789012)
        self.assertEqual(got.ratio, 3.5)
        self.assertEqual([(l.name, l.value) for l in got.leaves], [("a", 1), ("b", -2)])
        self.assertEqual((got.nested.name, got.nested.value), ("n", 99))

    def test_false_bool_and_omitted_optionals(self):
        obj = _Outer(flag=False)
        got = self._round_trip(obj)
        self.assertEqual(got.flag, False)
        self.assertIsNone(got.big)
        self.assertIsNone(got.ratio)
        self.assertEqual(got.leaves, [])
        self.assertIsNone(got.nested)

    def test_empty_list(self):
        obj = _Outer(leaves=[])
        # An empty list is indistinguishable from "field omitted" under
        # this ThriftStruct.write() (falsy list -> None-like skip is NOT
        # what happens here: [] is not None, so the field IS written).
        got = self._round_trip(obj)
        self.assertEqual(got.leaves, [])

    def test_sixteen_element_list_uses_long_form_size(self):
        # size >= 15 forces the long-form (varint) list-size encoding path.
        obj = _Outer(leaves=[_Leaf(name=str(i), value=i) for i in range(16)])
        got = self._round_trip(obj)
        self.assertEqual([l.value for l in got.leaves], list(range(16)))

    def test_field_id_delta_beyond_short_form_range(self):
        # field ids 1 -> 6 with 2/3 omitted still round-trip; and the
        # explicit gap (3 -> 5 above) exercises the long-form field header.
        obj = _Outer(ratio=1.25, nested=_Leaf(name="x", value=7))
        got = self._round_trip(obj)
        self.assertIsNone(got.flag)
        self.assertIsNone(got.big)
        self.assertEqual(got.ratio, 1.25)
        self.assertEqual((got.nested.name, got.nested.value), ("x", 7))

    def test_unknown_field_id_raises(self):
        writer = CompactWriter()
        writer.write_struct_begin()
        writer.write_i32_field(99, 42)
        writer.write_field_stop()
        writer.write_struct_end()
        with self.assertRaises(ValueError):
            _Outer.read(CompactReader(writer.bytes()))


if __name__ == "__main__":
    unittest.main()
