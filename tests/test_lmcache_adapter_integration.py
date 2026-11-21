"""RoCEPool's KV-cache connector driven by LMCache's own L2-adapter code:
its config parser and factory build the adapter (types "plugin" and
"native_plugin"), and LMCache's NativeConnectorL2Adapter runs store,
lookup-and-lock, load, unlock and delete against native OSTs.

Skipped where LMCache is not importable. Checked: chunks of any size
round-trip; lookups and loads report per key; deletes reach the store;
usage accounting follows; with "plugin" the L1 arena is registered and no
byte is copied by the CPU, with "native_plugin" transfers are copied.
"""
import json
import os
import shutil
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DRIVER = r'''
import ctypes, json, mmap, random, sys, time
sys.path.insert(0, sys.argv[1])
OSTS = sys.argv[2]
from lmcache.v1.distributed.api import ObjectKey  # noqa: E402
from lmcache.v1.distributed.internal_api import L1MemoryDesc  # noqa: E402
from lmcache.v1.distributed.l2_adapters import create_l2_adapter  # noqa: E402
from lmcache.v1.distributed.l2_adapters.config import get_l2_adapter_config_class  # noqa: E402


class Obj:
    """A non-tensor MemoryObj as the adapter sees it: its bytes via byte_array."""

    def __init__(self, mv):
        self.byte_array = mv


def until(fn, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        v = fn()
        if v is not None:
            return v
        time.sleep(0.005)
    raise TimeoutError


def bits(bm, n):
    return [bm.test(i) for i in range(n)]


ARENA = 96 << 20
arena = mmap.mmap(-1, ARENA)
base = ctypes.addressof((ctypes.c_char * ARENA).from_buffer(arena))
desc = L1MemoryDesc(ptr=base, size=ARENA, align_bytes=4096)
sizes = [4096, 300 << 10, 2 << 20, (13 << 20) + 4096]     # one chunk spans 14 parts of 1 MiB
out = {}
for typ, cls in (("plugin", "RoCEPoolL2Adapter"), ("native_plugin", "RoCEPoolConnector")):
    cfg = get_l2_adapter_config_class(typ).from_dict(
        {"type": typ, "module_path": "roce.lmcache_connector", "class_name": cls,
         "adapter_params": {"osts": OSTS, "namespace": f"lmc-{typ}/", "part_mb": 1}})
    ad = create_l2_adapter(cfg, desc)
    r = {"adapter": type(ad).__name__}
    keys = [ObjectKey(chunk_hash=bytes([i + 1]) * 32, model_name="llama-3-70b", kv_rank=0) for i in range(len(sizes))]
    miss = ObjectKey(chunk_hash=b"\xee" * 32, model_name="llama-3-70b", kv_rank=0)
    src, off = [], 0
    for i, n in enumerate(sizes):
        arena[off:off + n] = random.Random(i).randbytes(n)
        src.append((off, n))
        off += n
    half = ARENA // 2
    srcs = [Obj(memoryview(arena)[o:o + n]) for o, n in src]
    dsts = [Obj(memoryview(arena)[half + o:half + o + n]) for o, n in src]

    tid = ad.submit_store_task(keys, srcs)
    res = until(lambda: ad.pop_completed_store_tasks().get(tid))
    r["store_ok"], r["store_bytes"] = res.is_successful(), res.bytes_transferred()

    tid = ad.submit_lookup_and_lock_task(keys + [miss], {})
    r["lookup"] = bits(until(lambda: ad.query_lookup_and_lock_result(tid)), len(keys) + 1)

    tid = ad.submit_load_task(keys, dsts)
    r["load"] = bits(until(lambda: ad.query_load_result(tid)), len(keys))
    r["equal"] = all(arena[o:o + n] == arena[half + o:half + o + n] for o, n in src)

    tid = ad.submit_load_task([miss], [Obj(memoryview(arena)[half:half + 4096])])
    r["load_missing"] = bits(until(lambda: ad.query_load_result(tid)), 1)

    ad.submit_unlock(keys)
    ad.delete(keys[:2])
    tid = ad.submit_lookup_and_lock_task(keys, {})
    r["lookup_after_delete"] = bits(until(lambda: ad.query_lookup_and_lock_result(tid)), len(keys))
    u = ad.get_usage()
    r["usage_bytes"] = u.total_bytes_used
    full = ad.report_status()
    r["status"] = {k: v for k, v in full.items() if k in ("is_healthy", "type", "zero_copy_l1")}
    r["status_counters"] = sorted(full.get("rocepool", {}))
    conn = getattr(ad, "rocepool", None) or ad._client
    st = conn.stats()
    r["zero_copy"], r["copied"] = st["bytes_zero_copy"], st["bytes_copied"]
    ad.close()
    out[typ] = r
out["total"] = sum(sizes)
print("RESULT " + json.dumps(out))
'''


def lmcache_importable(env) -> bool:
    probe = "import lmcache.v1.distributed.l2_adapters, lmcache.lmcache_native"
    return subprocess.run([sys.executable, "-c", probe], capture_output=True, env=env).returncode == 0


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class LMCacheAdapterIntegration(unittest.TestCase):
    port = 23790

    def test_lmcache_drives_the_connector(self):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        extra = os.environ.get("PYTHONPATH", "")
        env = dict(os.environ, ROCE_TRANSPORT_LIB=ensure_softverbs_lib(), ROCEPOOL_KV_LIB=ensure_kv_lib()[0],
                   PYTHONPATH=os.pathsep.join(p for p in (ROOT, extra) if p))
        if not lmcache_importable(env):
            self.skipTest("LMCache is not installed")
        srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                ROOT, str(self.port), "0", "4"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               text=True, env=env)
        try:
            osts = srv.stdout.readline().split()[-1]
            out = subprocess.run([sys.executable, "-c", DRIVER, ROOT, osts], capture_output=True, text=True,
                                 timeout=300, env=env)
        finally:
            srv.stdin.close()
            srv.wait(timeout=20)
        self.assertEqual(out.returncode, 0, out.stdout[-2000:] + out.stderr[-4000:])
        r = json.loads(next(l for l in out.stdout.splitlines() if l.startswith("RESULT "))[7:])
        total = r["total"]
        for typ in ("plugin", "native_plugin"):
            x = r[typ]
            self.assertEqual((x["store_ok"], x["store_bytes"]), (True, total), typ)
            self.assertEqual(x["lookup"], [True, True, True, True, False], typ)
            self.assertEqual(x["load"], [True] * 4, typ)
            self.assertTrue(x["equal"], typ)
            self.assertEqual(x["load_missing"], [False], typ)
            self.assertEqual(x["lookup_after_delete"], [False, False, True, True], typ)
            self.assertEqual(x["usage_bytes"], total - 4096 - (300 << 10), typ)
            self.assertTrue(x["status"]["is_healthy"], typ)
        self.assertEqual(r["plugin"]["adapter"], "RoCEPoolL2Adapter")
        self.assertIn("bytes_zero_copy", r["plugin"]["status_counters"], "live counters in LMCache's status")
        self.assertTrue(r["plugin"]["status"]["zero_copy_l1"])
        self.assertEqual((r["plugin"]["zero_copy"], r["plugin"]["copied"]), (2 * total, 0))
        self.assertEqual((r["native_plugin"]["zero_copy"], r["native_plugin"]["copied"]), (0, 2 * total))


if __name__ == "__main__":
    unittest.main()
