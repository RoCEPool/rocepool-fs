"""Native KV API scenario against a set of OSTs. argv: ROOT osts_csv.
Prints RESULT <json>. Used by tests/test_kv_api.py (run once against native
C OSTs and once against the Python reference OSTs)."""
import hashlib
import json
import os
import sys

sys.path.insert(0, sys.argv[1])
from roce.kv import KVClient  # noqa: E402
from storage_engine.base import VersionConflict  # noqa: E402

osts = sys.argv[2].split(",")
res = {}
try:
    kv = KVClient(osts, namespace="t/")
    v1 = kv.put("alpha", b"one")
    res["put_get"] = [v1, kv.get("alpha").decode(), kv.get_versioned("alpha")[0]]
    res["put_if_ok"] = kv.put_if("alpha", b"two", expected_version=v1)
    try:
        kv.put_if("alpha", b"three", expected_version=v1)
        res["put_if_stale"] = "accepted"
    except VersionConflict:
        res["put_if_stale"] = "conflict"
    try:
        kv.put_if("fresh", b"x", expected_version=0)
        kv.put_if("fresh", b"y", expected_version=0)
        res["create_only"] = "accepted twice"
    except VersionConflict:
        res["create_only"] = "second rejected"
    try:
        kv.get("missing")
        res["missing"] = "found?"
    except KeyError:
        res["missing"] = "KeyError"
    kv.write_at("blob", 5, b"hello")
    kv.write_at("blob", 0, b"12")
    res["write_read_at"] = [kv.get("blob").hex(), kv.read_at("blob", 4, 3).hex(), kv.read_at("blob", 100, 3).hex()]
    kv.delete("alpha")
    try:
        kv.get("alpha")
        res["deleted"] = "still there"
    except KeyError:
        res["deleted"] = "gone"
    items = {f"k{i}": os.urandom(i % 300) for i in range(500)}
    before = kv.rpcs
    kv.mput(items)
    res["mput_rpcs"] = kv.rpcs - before
    before = kv.rpcs
    got = kv.mget(list(items) + ["nope1", "nope2"])
    res["mget_rpcs"] = kv.rpcs - before
    res["mget_ok"] = got[:500] == list(items.values()) and got[500:] == [None, None]
    res["mdelete"] = [sum(kv.mdelete(list(items)[:100])), sum(kv.mdelete(["k0", "k1"]))]
    res["after_mdelete"] = kv.mget(["k0", "k100"])[0] is None and kv.mget(["k100"])[0] == items["k100"]
    # placement = roce/pool_node.py's rule, so the key is on exactly that OST
    from roce.remote_engine import RemoteEngine
    from roce.roce_transport import RoCETransport
    owner = int.from_bytes(hashlib.sha256(b"t/k100").digest()[:8], "big") % len(osts)
    res["placement"] = RemoteEngine(osts[owner], RoCETransport()).get("t/k100") == items["k100"]
    big = os.urandom(3 * 1024 * 1024)
    kv.put("big", big)
    res["big_roundtrip"] = kv.get("big") == big
    kv.close()
except Exception:  # noqa: BLE001
    import traceback
    res["error"] = traceback.format_exc()
print("RESULT " + json.dumps(res), flush=True)
os._exit(0)
