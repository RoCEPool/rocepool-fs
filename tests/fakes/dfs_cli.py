"""The Python client (DistributedFileSystem) as a one-shot CLI, to check the
native C FUSE client interoperates with it on the same servers.
argv: ROOT mdts osts cmd args...
  sha PATH            -> sha256 hex of the file's bytes
  write PATH SIZE     -> writes deterministic bytes (seed = PATH), prints sha
  ostcounts           -> JSON list: stripe objects per OST
  stripes PATH        -> number of stripe objects of PATH across all OSTs
  attr PATH           -> {"size", "inline"} from the MDT
  mdtcounts DIR       -> JSON list: entries of DIR on each MDT
  extend PATH SIZE    -> extends PATH with zeros to SIZE, prints null
  refreeze PATH DATA  -> remove PATH, recreate it inline holding DATA, freeze
                         it (a spill in flight), prints null
Prints one line: RESULT <json>."""
import hashlib
import json
import os
import random
import sys

sys.path.insert(0, sys.argv[1])
from fs.posix_api import DistributedFileSystem  # noqa: E402
from roce.remote_engine import RemoteEngine  # noqa: E402
from roce.roce_transport import RoCETransport  # noqa: E402

mdts, osts, cmd = sys.argv[2].split(","), sys.argv[3].split(","), sys.argv[4]
engines = [RemoteEngine(a, RoCETransport()) for a in osts]
dfs = DistributedFileSystem(mdt_transport=RoCETransport(), mdts=mdts, osts=engines, attr_ttl_s=0)
if cmd == "sha":
    out = hashlib.sha256(dfs.read_file(sys.argv[5])).hexdigest()
elif cmd == "write":
    data = random.Random(sys.argv[5]).randbytes(int(sys.argv[6]))
    dfs.write_file(sys.argv[5], data, create_parents=True)
    out = hashlib.sha256(data).hexdigest()
elif cmd == "stripes":
    out = sum(len(e.list_prefix(sys.argv[5] + ".stripe.")) for e in engines)
elif cmd == "attr":
    a = dfs.getattr(sys.argv[5])
    out = None if a is None else {"size": a.size, "inline": a.inline}
elif cmd == "extend":  # append zeros up to SIZE (an extending write by another node)
    a = dfs.getattr(sys.argv[5])
    dfs.write_range(sys.argv[5], a.size, b"\0" * (int(sys.argv[6]) - a.size))
    out = None
elif cmd == "refreeze":
    dfs.remove_file(sys.argv[5])
    dfs.write_file(sys.argv[5], sys.argv[6].encode())
    dfs.mdt.freeze(sys.argv[5])
    out = None
elif cmd == "mdtcounts":  # entries of DIR held by each MDT (striped directories)
    out = [len(dfs.mdt._call(m, {"op": "readdir", "path": sys.argv[5]})) for m in mdts]
elif cmd == "ostcounts":
    out = [len([m for m in e.list_prefix("") if ".stripe." in m.key]) for e in engines]
print("RESULT " + json.dumps(out), flush=True)
os._exit(0)
