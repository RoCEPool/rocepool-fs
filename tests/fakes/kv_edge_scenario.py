"""Edge cases of the thick-KV OST protocol, run with a small frame limit
(ROCE_MAX_FRAME_BYTES = 1 MiB) against native or Python OSTs.
argv: ROOT osts_csv. Prints RESULT <json>. Used by tests/test_kv_api.py.

  tokens     VTOKEN / PUT-by-token: no ABA across delete + recreate
  batch_val  a malformed BATCH runs none of its sub-requests
  mget_big   mget whose values together exceed a frame (reply-size cap)
  mput_big   mput with an item over half a frame, and one over a whole frame
  py_chunks  RemoteEngine.batch splits a list bigger than a frame
"""
import json
import os
import sys

sys.path.insert(0, sys.argv[1])
from roce.kv import KVClient  # noqa: E402
from roce.pool_node import pack_batch_request  # noqa: E402
from roce.protocol import Frame, Op  # noqa: E402
from roce.remote_engine import RemoteEngine  # noqa: E402
from roce.roce_transport import RoCETransport  # noqa: E402
from storage_engine.base import ObjectNotFound, VersionConflict  # noqa: E402

osts = sys.argv[2].split(",")
res = {}
try:
    eng = RemoteEngine(osts[0], RoCETransport())

    # -- tokens --------------------------------------------------------
    t_absent = eng.version_token("aba/k")
    r1 = eng.put("aba/k", b"first")
    tok = eng.version_token("aba/k")
    eng.delete("aba/k")
    t_gone = eng.version_token("aba/k")
    out = {"present_token_is_version": tok == r1.version, "absent_token_moved": t_gone != t_absent}
    # the stale spill helper: prepared against "absent" before the file was
    # written + deleted -- a plain expected=0 CAS would succeed now (ABA)
    try:
        eng.put_token("aba/k", b"stale", t_absent)
        out["stale_absent_token"] = "accepted"
    except VersionConflict:
        out["stale_absent_token"] = "rejected"
    try:
        eng.put_token("aba/k", b"stale", tok)
        out["stale_present_token"] = "accepted"
    except VersionConflict:
        out["stale_present_token"] = "rejected"
    r2 = eng.put_token("aba/k", b"fresh", eng.version_token("aba/k"))
    out["recreated_version_above_old"] = r2.version > r1.version
    out["value"] = eng.get("aba/k").decode()
    res["tokens"] = out

    # -- a malformed BATCH runs nothing ----------------------------------
    good = pack_batch_request([(Op.PUT, "bv/one", b"\x00" * 9 + b"x")])
    bad = (2).to_bytes(4, "little") + good[4:] + bytes([Op.PUT]) + (5).to_bytes(4, "little") + (99).to_bytes(4, "little") + b"bv/tw"
    resp = eng.transport.send_request(eng.address, Frame(op=Op.BATCH, request_id=7, key="", value=bad))
    try:
        eng.get("bv/one")
        applied = True
    except ObjectNotFound:
        applied = False
    res["batch_val"] = {"reply": Op(resp.op).name, "first_applied": applied}

    # -- batches vs a 1 MiB frame -----------------------------------------
    kv = KVClient(osts, namespace="edge/")
    vals = {f"m{i}": bytes([i]) * (300 * 1024) for i in range(8)}  # 2.4 MiB total
    for k, v in vals.items():
        kv.put(k, v)
    got = kv.mget(list(vals) + ["nope"])
    res["mget_big"] = got[:8] == list(vals.values()) and got[8] is None
    items = {"s1": b"a", "half": b"h" * (700 * 1024), "s2": b"b"}
    kv.mput(items)
    res["mput_big"] = {"stored": kv.mget(list(items)) == list(items.values())}
    try:
        kv.mput({"s3": b"c", "huge": b"x" * (2 * 1024 * 1024), "s4": b"d"})
        res["mput_big"]["huge"] = "accepted?"
    except OSError as exc:
        res["mput_big"]["huge"] = f"errno {exc.errno}"
    except Exception as exc:  # noqa: BLE001
        res["mput_big"]["huge"] = type(exc).__name__ + ": " + str(exc)
    res["mput_big"]["others"] = kv.mget(["s3", "s4"]) == [b"c", b"d"]
    kv.close()

    # -- the Python client's BATCH chunking ------------------------------
    subs = [(Op.DELETE, f"chunk/{i:06d}/" + "p" * 40, b"") for i in range(30000)]  # ~1.9 MiB of requests
    replies = eng.batch(subs)
    res["py_chunks"] = [len(replies), sum(r.op == Op.ERROR for r in replies)]
except Exception:  # noqa: BLE001
    import traceback
    res["error"] = traceback.format_exc()
print("RESULT " + json.dumps(res), flush=True)
os._exit(0)
