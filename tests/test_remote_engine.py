"""RemoteEngine held to the same StorageEngine contract as every other
engine, over a real socket to a real single-node PoolNode OST server."""
import unittest

from roce.loopback_transport import LoopbackTransport
from roce.pool_node import PoolNode
from roce.remote_engine import RemoteEngine
from storage_engine.base import VersionConflict
from storage_engine.memory_engine import MemoryEngine
from tests.test_storage_engine_contract import StorageEngineContractTests

_port = [18700]


class RemoteEngineContractTests(StorageEngineContractTests):
    def setUp(self):
        _port[0] += 1
        addr = f"127.0.0.1:{_port[0]}"
        self.ost = PoolNode(local_address=addr, nodes=[addr], engine=MemoryEngine(), transport=LoopbackTransport())
        self.ost.start()
        self.engine = RemoteEngine(addr, LoopbackTransport())

    def tearDown(self):
        self.ost.stop()

    def test_remote_cas_conflict_is_a_version_conflict(self):
        v = self.engine.put("k", b"1").version
        self.engine.put("k", b"2", expected_version=v)
        with self.assertRaises(VersionConflict):
            self.engine.put("k", b"3", expected_version=v)


if __name__ == "__main__":
    unittest.main()
