"""Python reference OSTs (single-node PoolNodes) on 127.0.0.1, one process.
argv: ROOT base_port n. Prints "READY <addr,...>", serves until stdin closes."""
import sys

sys.path.insert(0, sys.argv[1])
from roce.pool_node import PoolNode  # noqa: E402
from roce.roce_transport import RoCETransport  # noqa: E402
from storage_engine.memory_engine import MemoryEngine  # noqa: E402

port, n = int(sys.argv[2]), int(sys.argv[3])
addrs = []
for i in range(n):
    a = f"127.0.0.1:{port + i}"
    PoolNode(local_address=a, nodes=[a], engine=MemoryEngine(), transport=RoCETransport()).start()
    addrs.append(a)
print("READY", ",".join(addrs), flush=True)
sys.stdin.read()
import os  # noqa: E402
os._exit(0)
