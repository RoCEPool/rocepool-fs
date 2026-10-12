"""wlp/ (the WLP v5 client) checked against the RapidsSE sources it was
written from, kept verbatim in third_party/rapidsse/: every message type id,
every struct field id and name and primitive type, and the 8-byte header."""
import glob
import inspect
import os
import re
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from wlp import compact_protocol as cp  # noqa: E402
from wlp import framing, messages  # noqa: E402

DEFS = os.path.join(ROOT, "third_party", "rapidsse", "src", "wlp", "thrift", "messageDefinitions")
PRIMITIVE = {"string": {cp.T_STRING, cp.T_BINARY}, "binary": {cp.T_STRING, cp.T_BINARY}, "i32": {cp.T_I32},
             "i64": {cp.T_I64}, "i16": {cp.T_I16}, "bool": {cp.T_BOOL}, "double": {cp.T_DOUBLE}}


def thrift_enum(path: str) -> dict:
    text = open(path).read()
    return {m.group(1): int(m.group(2)) for m in re.finditer(r"^\s*(\w+)\s*=\s*(\d+)", text, re.M)}


def thrift_structs() -> dict:
    out = {}
    for path in glob.glob(os.path.join(DEFS, "*", "*.thrift")):
        text = re.sub(r"//[^\n]*", "", open(path).read())
        for m in re.finditer(r"struct\s+(\w+)\s*\{(.*?)\}", text, re.S):
            out[m.group(1)] = {int(f.group(1)): (f.group(3), f.group(2).strip())
                               for f in re.finditer(r"(\d+)\s*:\s*(?:required|optional)?\s*([\w<>, .]+?)\s+(\w+)[ \t]*[=;,\n]",
                                                    m.group(2) + ";")}  # the last field may lack its ";"
    return out


class AgainstRapidsSETests(unittest.TestCase):
    def test_vendored_sources_present(self):
        for f in ("LICENSE", "COPYRIGHT", "UPSTREAM.md", "src/store/AdaptorWL.cpp",
                  "src/wlp/thrift/supports/WlpHeader.hpp", "src/wlp/thrift/messageDefinitions/v5/MessageTypes.thrift"):
            self.assertTrue(os.path.exists(os.path.join(ROOT, "third_party", "rapidsse", f)), f)

    def test_message_type_ids(self):
        ids = thrift_enum(os.path.join(DEFS, "v5", "MessageTypes.thrift"))
        consts = {k[4:]: v for k, v in vars(messages).items() if k.startswith("MSG_")}
        self.assertTrue(consts)
        for name, value in consts.items():
            camel = "".join(w.capitalize() for w in name.lower().split("_"))
            self.assertIn(camel, ids, name)
            self.assertEqual(value, ids[camel], name)

    def test_struct_fields(self):
        idl = thrift_structs()
        checked = 0
        for name, cls in inspect.getmembers(messages, inspect.isclass):
            fields = getattr(cls, "_FIELDS", None)
            if fields is None or name not in idl:
                continue
            for fid, ttype, attr, _ in fields:
                self.assertIn(fid, idl[name], f"{name}.{attr}: field id {fid} not in the IDL")
                want_name, want_type = idl[name][fid]
                self.assertEqual(attr, want_name, f"{name} field {fid}")
                if want_type in PRIMITIVE:
                    self.assertIn(ttype, PRIMITIVE[want_type], f"{name}.{attr}: {want_type}")
                elif want_type.startswith("list<"):
                    self.assertEqual(ttype, cp.T_LIST, f"{name}.{attr}")
                checked += 1
        self.assertGreater(checked, 10)

    def test_header(self):
        hpp = open(os.path.join(ROOT, "third_party", "rapidsse", "src", "wlp", "thrift", "supports",
                                "WlpHeader.hpp")).read()
        self.assertRegex(hpp, r"m_uMsgSize\s*:\s*32")
        self.assertRegex(hpp, r"m_uMsgType\s*:\s*8")
        self.assertEqual(framing.HEADER_SIZE, 8)
        self.assertEqual(framing.pack_header(51, 0x010203), b"\x00\x01\x02\x03\x33\x00\x00\x00")


if __name__ == "__main__":
    unittest.main()
