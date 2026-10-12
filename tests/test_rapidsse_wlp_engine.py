"""End-to-end test: RapidsSEEngine driven by the REAL WlpClient, talking
over a REAL TCP socket, in the REAL WLP wire format (framing + Compact
Protocol), to WlpTestServer (tests/fakes/wlp_test_server.py). Nothing here
is mocked -- bytes genuinely go over a loopback socket and get genuinely
decoded on the other side.

This is the WLP-backed counterpart to test_rapidsse_engine.py (which used
an injected DB-API2 client directly, skipping the wire entirely). Reusing
the same StorageEngineContractTests suite here means the exact same
correctness contract is checked whether RapidsSEEngine is driven directly
or over the wire -- if the protocol layer silently corrupted a key,
value, or version number, these tests would fail even though the DB-API2-
level tests still pass.
"""
import unittest

from storage_engine.rapidsse_engine import RapidsSEEngine
from tests.fakes.wlp_test_server import WlpTestServer
from tests.test_storage_engine_contract import StorageEngineContractTests
from wlp.wlp_client import WlpClient


class RapidsSEWlpEngineContractTests(StorageEngineContractTests):
    def setUp(self):
        self.server = WlpTestServer()
        self.client = WlpClient(self.server.host, self.server.port)
        self.engine = RapidsSEEngine(self.client, placeholder="?")

    def tearDown(self):
        self.client.close()
        self.server.stop()


class RapidsSEWlpEngineSpecificTests(unittest.TestCase):
    def setUp(self):
        self.server = WlpTestServer()
        self.client = WlpClient(self.server.host, self.server.port)
        self.engine = RapidsSEEngine(self.client, placeholder="?")

    def tearDown(self):
        self.client.close()
        self.server.stop()

    def test_binary_object_value_round_trips_exact_bytes_over_the_wire(self):
        payload = bytes(range(256)) * 100  # 25.6KB, forces multiple varint-length reads
        self.engine.put("blob.bin", payload)
        self.assertEqual(self.engine.get("blob.bin"), payload)

    def test_unicode_key_and_value_round_trip_over_the_wire(self):
        self.engine.put("目录/文件.txt", "内容 with emoji 🚀".encode("utf-8"))
        self.assertEqual(self.engine.get("目录/文件.txt").decode("utf-8"), "内容 with emoji 🚀")

    def test_many_objects_list_prefix_over_the_wire(self):
        for i in range(20):
            self.engine.put(f"dir/item-{i:03d}", str(i).encode())
        metas = self.engine.list_prefix("dir/")
        self.assertEqual(len(metas), 20)
        self.assertEqual([m.key for m in metas], sorted(m.key for m in metas))

    def test_two_independent_connections_share_server_state(self):
        # Proves the server -- not client-local state -- is the source of
        # truth, the way a real multi-connection RapidsSE deployment would
        # need to behave.
        other_client = WlpClient(self.server.host, self.server.port)
        other_engine = RapidsSEEngine(other_client, placeholder="?", ensure_schema=False)
        try:
            self.engine.put("shared-key", b"from-first-connection")
            self.assertEqual(other_engine.get("shared-key"), b"from-first-connection")
        finally:
            other_client.close()


if __name__ == "__main__":
    unittest.main()
