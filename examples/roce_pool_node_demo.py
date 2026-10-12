"""Demonstrates swapping RoCE/RDMA in for TCP as the transport underneath
PoolNode (the memory-pool cluster) and mpi.comm.Communicator (point-to-
point + collectives), with NO other code change -- both classes only ever
call the roce.transport_base.Transport interface (`send_request`/
`serve`), never anything TCP-specific, so LoopbackTransport and
RoCETransport are interchangeable from their point of view.

Run this as-is and it will raise RoCENotAvailable with a clear message:
this sandbox has no RDMA-capable NIC, no rdma-core dev headers, and no
network access to install them (all confirmed by direct inspection -- see
this project's README and roce/roce_transport.py's module docstring), so
roce/native/libroce_transport.so has never been built here. On a real
RoCE-capable machine, first run `roce/native/build.sh` (builds and prints
its own next verification steps), then this script will actually stand up
a 3-node pool and a 3-rank MPI communicator over real RDMA traffic.

Usage: python3 examples/roce_pool_node_demo.py
"""
from __future__ import annotations

import sys

sys.path.insert(0, __file__.rsplit("/examples/", 1)[0])

from mpi.comm import Communicator
from roce.pool_node import PoolNode
from roce.roce_transport import RoCENotAvailable, RoCETransport
from storage_engine.memory_engine import MemoryEngine


def main() -> int:
    try:
        # Each node/rank gets its OWN RoCETransport instance (it opens its
        # own connections and its own listening resources) -- same pattern
        # as passing a fresh LoopbackTransport() per node in the existing
        # tests (see tests/test_pool_node.py, tests/test_mpi_comm.py).
        addresses = ["127.0.0.1:19100", "127.0.0.1:19101", "127.0.0.1:19102"]

        nodes = [
            PoolNode(local_address=addr, nodes=addresses, engine=MemoryEngine(), transport=RoCETransport())
            for addr in addresses
        ]
        print(f"started {len(nodes)}-node pool over RoCE/RDMA (no TCP in the data path)")

        nodes[0].put("hello", b"world")
        assert nodes[1].get("hello") == b"world"
        print("cross-node put/get over RDMA: OK")

        comms = [Communicator(rank=r, addresses=addresses, transport=RoCETransport()) for r in range(3)]
        comms[0].bcast(b"rdma mpi bcast", root=0)
        print("MPI-style bcast over RDMA: OK")

        for c in comms:
            c.close()
        return 0
    except RoCENotAvailable as exc:
        print(f"RoCE/RDMA not available in this environment: {exc}", file=sys.stderr)
        print(
            "This is expected on a machine without an RDMA-capable NIC or "
            "rdma-core dev headers -- see roce/native/build.sh.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
