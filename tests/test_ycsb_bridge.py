"""Tests for tools/ycsb_bridge.py's wire protocol and Bridge logic, run
without any Java/YCSB involved -- a raw socket client speaks the same
bytes RocepoolClient.java (tools/ycsb_java/RocepoolClient.java) does, so a
bug in the framing shows up here before it ever reaches the real benchmark.
"""
import socket
import struct
import threading
import time
import unittest

from storage_engine.memory_engine import MemoryEngine
from tools.ycsb_bridge import (
    OP_DELETE, OP_INSERT, OP_READ, OP_SCAN, OP_UPDATE,
    ST_ERROR, ST_NOT_FOUND, ST_OK,
    Bridge, serve_forever,
)


def _start_server():
    backend = MemoryEngine()
    bridge = Bridge(backend)
    port = _free_port()
    t = threading.Thread(target=serve_forever, args=(bridge, "127.0.0.1", port), daemon=True)
    t.start()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.2)
            s.close()
            return port
        except OSError:
            time.sleep(0.02)
    raise RuntimeError("bridge did not come up")


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _fields_bytes(fields: dict[str, bytes]) -> bytes:
    out = [struct.pack(">H", len(fields))]
    for name, value in fields.items():
        nb = name.encode()
        out.append(struct.pack(">H", len(nb)) + nb)
        out.append(struct.pack(">I", len(value)) + value)
    return b"".join(out)


def _names_bytes(names: list[str] | None) -> bytes:
    if names is None:
        return struct.pack(">B", 1) + struct.pack(">H", 0)
    out = [struct.pack(">B", 0), struct.pack(">H", len(names))]
    for n in names:
        nb = n.encode()
        out.append(struct.pack(">H", len(nb)) + nb)
    return b"".join(out)


class _Client:
    """A minimal client, standing in for RocepoolClient.java, so these
    tests exercise the exact bytes on the wire."""

    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)

    def close(self):
        self.sock.close()

    def _recvn(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError()
            buf.extend(chunk)
        return bytes(buf)

    def _read_status_and_body_prefix(self):
        (status,) = struct.unpack(">B", self._recvn(1))
        if status == ST_ERROR:
            (mlen,) = struct.unpack(">H", self._recvn(2))
            return status, self._recvn(mlen)
        return status, None

    def _read_fields(self) -> dict[str, bytes]:
        (count,) = struct.unpack(">H", self._recvn(2))
        fields = {}
        for _ in range(count):
            (nlen,) = struct.unpack(">H", self._recvn(2))
            name = self._recvn(nlen).decode()
            (vlen,) = struct.unpack(">I", self._recvn(4))
            fields[name] = self._recvn(vlen)
        return fields

    def insert(self, table, key, fields):
        msg = (struct.pack(">B", OP_INSERT) + struct.pack(">H", len(table)) + table.encode() +
               struct.pack(">H", len(key)) + key.encode() + _fields_bytes(fields))
        self.sock.sendall(msg)
        status, err = self._read_status_and_body_prefix()
        return status, err

    def update(self, table, key, fields):
        msg = (struct.pack(">B", OP_UPDATE) + struct.pack(">H", len(table)) + table.encode() +
               struct.pack(">H", len(key)) + key.encode() + _fields_bytes(fields))
        self.sock.sendall(msg)
        return self._read_status_and_body_prefix()

    def delete(self, table, key):
        msg = (struct.pack(">B", OP_DELETE) + struct.pack(">H", len(table)) + table.encode() +
               struct.pack(">H", len(key)) + key.encode())
        self.sock.sendall(msg)
        return self._read_status_and_body_prefix()

    def read(self, table, key, fields=None):
        msg = (struct.pack(">B", OP_READ) + struct.pack(">H", len(table)) + table.encode() +
               struct.pack(">H", len(key)) + key.encode() + _names_bytes(fields))
        self.sock.sendall(msg)
        status, err = self._read_status_and_body_prefix()
        if status != ST_OK:
            return status, err
        return status, self._read_fields()

    def scan(self, table, startkey, recordcount, fields=None):
        msg = (struct.pack(">B", OP_SCAN) + struct.pack(">H", len(table)) + table.encode() +
               struct.pack(">H", len(startkey)) + startkey.encode() +
               struct.pack(">I", recordcount) + _names_bytes(fields))
        self.sock.sendall(msg)
        status, err = self._read_status_and_body_prefix()
        if status != ST_OK:
            return status, err
        (count,) = struct.unpack(">I", self._recvn(4))
        records = [self._read_fields() for _ in range(count)]
        return status, records


class YcsbBridgeWireProtocolTests(unittest.TestCase):
    def setUp(self):
        self.port = _start_server()
        self.client = _Client(self.port)

    def tearDown(self):
        self.client.close()

    def test_insert_then_read_roundtrips_all_fields(self):
        status, _ = self.client.insert("usertable", "user1", {"field0": b"hello", "field1": b"world"})
        self.assertEqual(status, ST_OK)
        status, fields = self.client.read("usertable", "user1")
        self.assertEqual(status, ST_OK)
        self.assertEqual(fields, {"field0": b"hello", "field1": b"world"})

    def test_read_missing_key_is_not_found(self):
        status, _ = self.client.read("usertable", "nope")
        self.assertEqual(status, ST_NOT_FOUND)

    def test_read_with_field_subset_filters(self):
        self.client.insert("usertable", "u2", {"a": b"1", "b": b"2", "c": b"3"})
        status, fields = self.client.read("usertable", "u2", fields=["a", "c"])
        self.assertEqual(status, ST_OK)
        self.assertEqual(fields, {"a": b"1", "c": b"3"})

    def test_update_merges_fields_without_clobbering_others(self):
        self.client.insert("usertable", "u3", {"a": b"1", "b": b"2"})
        status, _ = self.client.update("usertable", "u3", {"b": b"NEW"})
        self.assertEqual(status, ST_OK)
        _, fields = self.client.read("usertable", "u3")
        self.assertEqual(fields, {"a": b"1", "b": b"NEW"})

    def test_update_on_missing_key_creates_it(self):
        # YCSB semantics vary by DB; this bridge treats update-of-missing
        # as an upsert, matching PoolNode.put's own no-expected-version
        # (unconditional) semantics.
        status, _ = self.client.update("usertable", "u4", {"x": b"y"})
        self.assertEqual(status, ST_OK)
        _, fields = self.client.read("usertable", "u4")
        self.assertEqual(fields, {"x": b"y"})

    def test_delete_then_read_is_not_found(self):
        self.client.insert("usertable", "u5", {"a": b"1"})
        status, _ = self.client.delete("usertable", "u5")
        self.assertEqual(status, ST_OK)
        status, _ = self.client.read("usertable", "u5")
        self.assertEqual(status, ST_NOT_FOUND)

    def test_delete_missing_key_is_not_found(self):
        status, _ = self.client.delete("usertable", "never-existed")
        self.assertEqual(status, ST_NOT_FOUND)

    def test_scan_returns_records_in_key_order_from_start_key(self):
        for i in range(5):
            self.client.insert("usertable", f"user{i}", {"v": str(i).encode()})
        status, records = self.client.scan("usertable", "user1", 3)
        self.assertEqual(status, ST_OK)
        self.assertEqual([r["v"] for r in records], [b"1", b"2", b"3"])

    def test_scan_stops_at_recordcount_even_with_more_data(self):
        for i in range(10):
            self.client.insert("usertable", f"user{i:02d}", {"v": str(i).encode()})
        status, records = self.client.scan("usertable", "user00", 4)
        self.assertEqual(status, ST_OK)
        self.assertEqual(len(records), 4)

    def test_two_tables_do_not_collide_on_the_same_key(self):
        self.client.insert("tableA", "same-key", {"which": b"A"})
        self.client.insert("tableB", "same-key", {"which": b"B"})
        _, fa = self.client.read("tableA", "same-key")
        _, fb = self.client.read("tableB", "same-key")
        self.assertEqual(fa["which"], b"A")
        self.assertEqual(fb["which"], b"B")

    def test_binary_values_survive_the_round_trip(self):
        blob = bytes(range(256)) * 4  # includes NUL bytes and every byte value
        self.client.insert("usertable", "bin", {"payload": blob})
        _, fields = self.client.read("usertable", "bin")
        self.assertEqual(fields["payload"], blob)

    def test_connection_can_be_reused_for_many_sequential_requests(self):
        # YCSB opens one DB (and so one bridge connection) per client
        # thread and reuses it for its whole run -- the bridge must not
        # need a fresh connection per operation.
        for i in range(50):
            status, _ = self.client.insert("usertable", f"k{i}", {"v": str(i).encode()})
            self.assertEqual(status, ST_OK)
        for i in range(50):
            status, fields = self.client.read("usertable", f"k{i}")
            self.assertEqual(status, ST_OK)
            self.assertEqual(fields["v"], str(i).encode())


class YcsbBridgeConcurrentConnectionsTests(unittest.TestCase):
    def test_many_concurrent_clients_do_not_corrupt_each_others_records(self):
        port = _start_server()
        errors = []

        def worker(n):
            try:
                c = _Client(port)
                for i in range(20):
                    key = f"w{n}-{i}"
                    status, _ = c.insert("usertable", key, {"owner": str(n).encode()})
                    assert status == ST_OK
                for i in range(20):
                    key = f"w{n}-{i}"
                    status, fields = c.read("usertable", key)
                    assert status == ST_OK, (n, i, status)
                    assert fields["owner"] == str(n).encode(), (n, i, fields)
                c.close()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
