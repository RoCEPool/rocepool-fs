"""Starts n_mdts native MDTs + n_osts native OSTs on 127.0.0.1 in this one
process, prints "READY <mdt,...> <ost,...>" and serves until stdin closes.
argv: ROOT base_port n_mdts n_osts. Used by tests/test_native_fuse.py."""
import sys

sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport  # noqa: E402

port, nm, no = int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
t = RoCETransport()
mdts = [t.serve_native("mdt", f"127.0.0.1:{port + i}").local_address() for i in range(nm)]
osts = [t.serve_native("ost", f"127.0.0.1:{port + 50 + i}").local_address() for i in range(no)]
print("READY", ",".join(mdts), ",".join(osts), flush=True)
sys.stdin.read()
import os  # noqa: E402
os._exit(0)
