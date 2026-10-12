import unittest

from roce.loopback_transport import LoopbackTransport
from roce.pool_node import PoolNode, RemoteError
from storage_engine.base import VersionConflict
from storage_engine.base import ObjectNotFound
from storage_engine.memory_engine import MemoryEngine


def _make_cluster(n: int, *, base_port: int = 18800) -> list[PoolNode]:
    """Bring up `n` real PoolNodes, each with its own engine and its own
    listening socket, that all agree on the same node list.

    Uses fixed ports (base_port..base_port+n) rather than a discover-then-
    rebind dance over ephemeral (port 0) sockets: binding, closing, and
    immediately re-binding the SAME ephemeral port for several sockets in
    quick succession is a real TOCTOU race (confirmed by this suite
    actually hitting "Address already in use" during development) --
    fixed ports in a private test range sidestep it entirely.
    """
    addresses = [f"127.0.0.1:{base_port + i}" for i in range(n)]
    pool_nodes = []
    for addr in addresses:
        node = PoolNode(local_address=addr, nodes=addresses, engine=MemoryEngine(), transport=LoopbackTransport())
        bound = node.start()
        assert bound == addr, (bound, addr)
        pool_nodes.append(node)
    return pool_nodes


class PoolNodeSingleNodeTests(unittest.TestCase):
    """A single-node pool must behave like a plain local StorageEngine --
    everything is always local."""

    def setUp(self):
        self.node = PoolNode(local_address="127.0.0.1:0", nodes=["127.0.0.1:0"],
                              engine=MemoryEngine(), transport=LoopbackTransport())
        # single-node: no peer traffic possible, so start()/stop() aren't required,
        # but exercise them anyway since a real deployment always calls them.
        self.node.local_address = self.node.start()
        self.node.nodes = [self.node.local_address]
        self.addCleanup(self.node.stop)

    def test_put_get_local(self):
        self.node.put("k", b"v")
        self.assertEqual(self.node.get("k"), b"v")

    def test_get_missing_raises_object_not_found_locally(self):
        with self.assertRaises(ObjectNotFound):
            self.node.get("missing")


class PoolNodeMultiNodeTests(unittest.TestCase):
    """Real multi-node test: several PoolNodes on real ports, real hashing
    decides which node actually owns a given key, and a client can PUT/GET
    a key through ANY node in the cluster regardless of who owns it."""

    def setUp(self):
        self.nodes = _make_cluster(4)
        self.addCleanup(lambda: [n.stop() for n in self.nodes])

    def test_every_node_agrees_on_the_owner_of_a_given_key(self):
        owners = {node.owner_of("shared-key") for node in self.nodes}
        self.assertEqual(len(owners), 1, "all nodes must compute the same owner for the same key")

    def test_put_through_non_owner_is_forwarded_and_readable_from_any_node(self):
        # Find a key this specific node does NOT own, to force forwarding.
        entry_node = self.nodes[0]
        key = _find_key_not_owned_by(entry_node, self.nodes, start="probe-")
        self.assertFalse(entry_node.is_local(key), "test setup: need a genuinely remote key")

        entry_node.put(key, b"stored-via-forwarding")

        # Readable through every node in the cluster, not just the owner.
        for node in self.nodes:
            self.assertEqual(node.get(key), b"stored-via-forwarding")

        # And actually landed on disk at exactly the owner, nowhere else.
        owner_address = entry_node.owner_of(key)
        owner_node = next(n for n in self.nodes if n.local_address == owner_address)
        for node in self.nodes:
            if node is owner_node:
                self.assertTrue(node.engine.exists(key))
            else:
                self.assertFalse(node.engine.exists(key), "key must live on exactly one node")

    def test_delete_through_non_owner_removes_from_owner(self):
        entry_node = self.nodes[1]
        key = _find_key_not_owned_by(entry_node, self.nodes, start="del-")
        entry_node.put(key, b"x")
        entry_node.delete(key)
        for node in self.nodes:
            with self.assertRaises(ObjectNotFound):
                node.get(key)

    def test_version_conflict_propagates_across_the_network_as_remote_error(self):
        entry_node = self.nodes[2]
        key = _find_key_not_owned_by(entry_node, self.nodes, start="cas-")
        entry_node.put(key, b"v1")
        with self.assertRaises(RemoteError) as ctx:
            entry_node.put(key, b"v2-with-wrong-expected-version", expected_version=999)
        self.assertEqual(ctx.exception.code, "VERSION_CONFLICT")

    def test_remote_version_conflict_is_the_same_type_as_a_local_one(self):
        # Same call, same exception type, regardless of which node owns the key.
        entry_node = self.nodes[2]
        remote_key = _find_key_not_owned_by(entry_node, self.nodes, start="cas-r-")
        local_key = next(f"cas-l-{i}" for i in range(10_000) if entry_node.is_local(f"cas-l-{i}"))
        for key in (remote_key, local_key):
            entry_node.put(key, b"v1")
            with self.assertRaises(VersionConflict) as ctx:
                entry_node.put(key, b"stale", expected_version=999)
            self.assertEqual((ctx.exception.expected, ctx.exception.actual), (999, 1))

    def test_keys_distribute_across_more_than_one_node(self):
        """Sanity check that hashing isn't secretly degenerate (e.g.
        everything landing on node 0)."""
        owners = {self.nodes[0].owner_of(f"distribution-check-{i}") for i in range(200)}
        self.assertGreater(len(owners), 1, "200 keys all landed on one node -- hash distribution looks broken")


def _find_key_not_owned_by(node: PoolNode, all_nodes: list[PoolNode], *, start: str) -> str:
    for i in range(10000):
        candidate = f"{start}{i}"
        if not node.is_local(candidate):
            return candidate
    raise AssertionError("could not find a non-local key in 10000 tries")


if __name__ == "__main__":
    unittest.main()
