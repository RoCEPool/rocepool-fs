"""Runs the native-vs-Python server parity checks in their own process (the
transport library is loaded once per process). argv: ROOT base_port.
Prints one line: RESULT <json>. Used by tests/test_native_servers.py."""
import json
import random
import sys
import threading

sys.path.insert(0, sys.argv[1])

from fs.mdt_node import MDTClient, MDTError, MDTServer  # noqa: E402
from fs.namespace_shard import NamespaceShard  # noqa: E402
from fs.posix_api import DirectoryNotEmpty, DistributedFileSystem, FileExistsInPool, StripeReadCache  # noqa: E402
from roce.pool_node import PoolNode  # noqa: E402
from roce.remote_engine import RemoteEngine  # noqa: E402
from roce.roce_transport import RoCETransport  # noqa: E402
from storage_engine.base import ObjectNotFound, VersionConflict  # noqa: E402
from storage_engine.memory_engine import MemoryEngine  # noqa: E402

port = int(sys.argv[2])
HOST = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"  # the RoCE IP when run on site
tr = RoCETransport()
handles = []


def addr():
    global port
    port += 1
    return f"{HOST}:{port}"


def py_ost():
    a = addr()
    n = PoolNode(local_address=a, nodes=[a], engine=MemoryEngine(), transport=RoCETransport())
    n.start()
    handles.append(n)
    return a


def py_mdt():
    a = addr()
    m = MDTServer(RoCETransport())
    m.start(a)
    handles.append(m)
    return a


def native(kind):
    h = tr.serve_native(kind, addr())
    handles.append(h)
    return h.local_address()


def ost_script(eng):
    """A fixed op sequence; returns comparable outcomes (no timestamps)."""
    out = []

    def rec(name, fn):
        try:
            r = fn()
            out.append([name, "ok", r])
        except VersionConflict as e:
            out.append([name, "VersionConflict", [e.expected, e.actual]])
        except ObjectNotFound:
            out.append([name, "ObjectNotFound"])
    rec("get-missing", lambda: eng.get("a"))
    rec("stat-missing", lambda: eng.stat("a"))
    rec("delete-missing", lambda: eng.delete("a"))
    rec("put1", lambda: [eng.put("a", b"hello").version, eng.put("a", b"hello").size])
    rec("get", lambda: eng.get("a").decode())
    rec("cas-good", lambda: eng.put("a", b"v3", expected_version=2).version)
    rec("cas-stale", lambda: eng.put("a", b"x", expected_version=1).version)
    rec("cas-create-exists", lambda: eng.put("a", b"x", expected_version=0).version)
    rec("cas-create-new", lambda: eng.put("new", b"n", expected_version=0).version)
    rec("empty", lambda: [eng.put("empty", b"").size, eng.get("empty").hex()])
    rec("big", lambda: len(eng.get("big")) if eng.put("big", bytes(range(256)) * 4096) else None)
    rec("stat", lambda: (lambda m: [m.size, m.version, m.is_dir, m.created_at > 0, m.updated_at >= m.created_at])(eng.stat("a")))
    for k in ["dir/b", "dir/a", "dir/c\"q", "dir/été", "dir2/x", "di"]:
        eng.put(k, k.encode())
    rec("list", lambda: [[m.key, m.size, m.version] for m in eng.list_prefix("dir/")])
    rec("list-all", lambda: [m.key for m in eng.list_prefix("")])
    rec("exists", lambda: [eng.exists("a"), eng.exists("zzz")])
    rec("delete", lambda: eng.delete("a"))
    rec("get-after-delete", lambda: eng.get("a"))
    rec("recreate-version-restarts", lambda: eng.put("a", b"z").version)
    # thick-KV ops
    rec("write_at-new-gap", lambda: [eng.write_at("w", 5, b"xy").version, eng.get("w").hex()])
    rec("write_at-overwrite", lambda: [eng.write_at("w", 1, b"ABC").size, eng.get("w").hex()])
    rec("write_at-extend", lambda: [eng.write_at("w", 9, b"Z").size, eng.stat("w").version])
    rec("get_versioned", lambda: (lambda r: [r[0], r[1].hex()])(eng.get_versioned("w")))
    rec("get_versioned-missing", lambda: eng.get_versioned("nope"))
    rec("read_at", lambda: (lambda r: [r[0], r[1].hex()])(eng.read_at("w", 2, 4)))
    rec("read_at-past-end", lambda: (lambda r: [r[0], r[1].hex()])(eng.read_at("w", 50, 4)))
    rec("read_at-missing", lambda: eng.read_at("nope", 0, 4))
    rec("truncate-shrink", lambda: [eng.truncate_object("w", 3).size, eng.get("w").hex()])
    rec("truncate-noop-longer", lambda: [eng.truncate_object("w", 100).size, eng.stat("w").version])
    rec("truncate-missing", lambda: [eng.truncate_object("nope", 1).version, eng.exists("nope")])
    from roce.protocol import Op
    rec("batch", lambda: [[int(r.op), r.value.hex()] for r in eng.batch([
        (Op.PUT, "b1", b"\x00" + (0).to_bytes(8, "big") + b"one"),
        (Op.WRITE_AT, "b1", (3).to_bytes(8, "little") + b"-two"),
        (Op.GET, "b1", b""),
        (Op.GET, "missing", b""),
        (Op.DELETE, "b1", b""),
        (Op.GET_V, "b1", b"")])])
    return out


def mdt_script(cli):
    out = []

    def rec(name, fn):
        try:
            r = fn()
            if isinstance(r, dict):
                r = {k: v for k, v in r.items() if k not in ("mtime", "ctime")}
            elif isinstance(r, list) and all(isinstance(e, dict) for e in r):
                r = [{k: v for k, v in e.items() if k not in ("mtime", "ctime")} for e in r]
            out.append([name, "ok", r])
        except MDTError as e:
            out.append([name, e.code])
    lay = {"stripe_count": 4, "stripe_size": 1 << 20, "stripe_offset": 3}
    rec("lookup-missing", lambda: cli.lookup("d"))
    rec("mkdir", lambda: cli.create("d", is_dir=True, layout=None))
    rec("mkdir-again", lambda: cli.create("d", is_dir=True, layout=None))
    rec("create", lambda: cli.create("d/f", is_dir=False, layout=lay))
    rec("create-nonexcl", lambda: cli.create("d/f", is_dir=False, layout=None, exclusive=False))
    rec("create-nonexcl-type-clash", lambda: cli.create("d", is_dir=False, layout=None, exclusive=False))
    rec("normalize", lambda: cli.lookup("/d//./f/"))
    rec("size-max", lambda: cli.set_size("d/f", 100))
    rec("size-max-lower", lambda: cli.set_size("d/f", 10))
    rec("size-exact", lambda: cli.set_size("d/f", 10, mode="exact"))
    rec("size-dir", lambda: cli.set_size("d", 5))
    rec("size-missing", lambda: cli.set_size("d/nope", 5))
    for n in ["zeta", "alpha", "Beta", "é", "m"]:
        cli.create(f"d/{n}", is_dir=False, layout=lay)
    rec("readdir", lambda: cli.readdir("d"))
    rec("readdir-empty", lambda: cli.readdir("nothing"))
    rec("child_count", lambda: cli.child_count("d"))
    rec("unlink-wrong-type", lambda: cli.unlink("d/f", is_dir=True))
    rec("unlink", lambda: cli.unlink("d/f", is_dir=False))
    rec("unlink-missing", lambda: cli.unlink("d/f", is_dir=False))
    rec("rmdir-type", lambda: cli.unlink("d", is_dir=False))
    # Data-on-MDT
    def strip(t):
        iver, e, data = t
        return [iver, {k: v for k, v in e.items() if k not in ("mtime", "ctime")}, data.hex()]
    rec("create-inline", lambda: cli.create("d/s", is_dir=False, layout=lay, inline_ok=True))
    rec("write-inline", lambda: cli.write_inline("d/s", 3, b"abc"))
    rec("write-inline-overlap", lambda: cli.write_inline("d/s", 0, b"XYZW"))
    rec("read-inline", lambda: strip(cli.read_inline("d/s", 1, 100)))
    rec("set-size-inline-shrink", lambda: cli.set_size("d/s", 2, mode="exact"))
    rec("set-size-inline-grow-max", lambda: cli.set_size("d/s", 5, mode="max"))
    rec("set-size-inline-grow", lambda: cli.set_size("d/s", 5, mode="exact"))
    rec("write-inline-full", lambda: cli.write_inline("d/s", 40000, b"x"))
    rec("commit-not-frozen", lambda: cli.spill_commit("d/s"))
    rec("freeze", lambda: strip(cli.freeze("d/s")))
    rec("freeze-again", lambda: strip(cli.freeze("d/s")))
    rec("write-frozen", lambda: cli.write_inline("d/s", 0, b"q"))
    rec("set-size-frozen", lambda: cli.set_size("d/s", 1, mode="exact"))
    rec("commit", lambda: cli.spill_commit("d/s"))
    rec("commit-again", lambda: cli.spill_commit("d/s"))
    rec("write-not-inline", lambda: cli.write_inline("d/s", 0, b"q"))
    rec("read-not-inline", lambda: strip(cli.read_inline("d/s", 0, 10)))
    rec("create-no-inline-flag", lambda: cli.create("d/t", is_dir=False, layout=lay))
    rec("inline-dir", lambda: cli.write_inline("d", 0, b"x"))
    rec("create-inline-then-readdir", lambda: cli.create("d/u", is_dir=False, layout=lay, inline_ok=True)
        and cli.readdir("d"))
    rec("unlink-inline", lambda: cli.unlink("d/u", is_dir=False))
    return out


def dfs_script(mdts, osts):
    dfs = DistributedFileSystem(mdt_transport=RoCETransport(), mdts=mdts,
                                osts=[RemoteEngine(a, RoCETransport()) for a in osts],
                                stripe_size=64 * 1024, attr_ttl_s=0)
    rng = random.Random(7)
    data = rng.randbytes(700_000)
    dfs.mkdir("x/y", parents=True)
    dfs.write_file("x/y/f", data)
    ok = [dfs.read_file("x/y/f") == data]
    dfs.write_range("x/y/f", 123_456, b"PATCH" * 1000)
    want = bytearray(data)
    want[123_456:123_456 + 5000] = b"PATCH" * 1000
    ok.append(dfs.read_file("x/y/f") == bytes(want))
    rc = StripeReadCache()
    ok.append(b"".join(dfs.read_range("x/y/f", o, 65_000, stripe_cache=rc) for o in range(0, 700_000, 65_000)) == bytes(want))
    dfs.truncate("x/y/f", 1000)
    dfs.truncate("x/y/f", 5000)
    ok.append(dfs.read_file("x/y/f") == bytes(want[:1000]) + b"\x00" * 4000)
    try:
        dfs.rmdir("x/y")
        ok.append(False)
    except DirectoryNotEmpty:
        ok.append(True)
    try:
        dfs.mkdir("x/y")
        ok.append(False)
    except FileExistsInPool:
        ok.append(True)
    # concurrent disjoint writers into one file
    dfs.create_file("x/shared")
    blocks = {i: bytes([65 + i % 26]) * 9_000 for i in range(24)}
    ts = [threading.Thread(target=dfs.write_range, args=("x/shared", i * 9_000, b)) for i, b in blocks.items()]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    ok.append(dfs.read_file("x/shared") == b"".join(blocks[i] for i in range(24)))
    dfs.remove_file("x/y/f")
    dfs.remove_file("x/shared")
    ok.append(dfs.listdir("x/y") == [] and dfs.listdir("x") == ["y"])
    dfs.batcher.close()
    return ok


def cas_hammer(ost_addr, threads=8, per=40):
    """Atomicity under real concurrency: N threads CAS-increment one counter."""
    eng = RemoteEngine(ost_addr, RoCETransport())
    eng.put("ctr", b"0")

    def worker():
        e = RemoteEngine(ost_addr, RoCETransport())
        for _ in range(per):
            while True:
                v = e.stat("ctr").version
                n = int(e.get("ctr"))
                try:
                    e.put("ctr", str(n + 1).encode(), expected_version=v)
                    break
                except VersionConflict:
                    continue
    ts = [threading.Thread(target=worker) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return int(eng.get("ctr"))


def mdt_hammer(mdt_addr, threads=8, per=150):
    """The native MDT's per-directory bucket locks under real concurrency:
    N threads create, size, look up and unlink their own files in ONE
    directory (mdtest-hard's shape) while others readdir it. Checks: every
    thread's ops succeed, no entry is lost or duplicated, counts agree, and
    exclusive creates of the SAME name by all threads admit exactly one."""
    c = MDTClient(RoCETransport(), NamespaceShard([mdt_addr]))
    c.create("hot", is_dir=True, layout=None)
    errors, winners = [], []
    lk = threading.Lock()

    def worker(t):
        me = MDTClient(RoCETransport(), NamespaceShard([mdt_addr]))
        try:
            for i in range(per):
                path = f"hot/t{t}-{i}"
                me.create(path, is_dir=False, layout=None, inline_ok=True)
                me.set_size(path, i, mode="exact")
                assert me.lookup(path)["size"] == i
                if i % 3 == 0:
                    me.unlink(path, is_dir=False)
                if i % 50 == 0:
                    names = [e["name"] for e in me.readdir("hot")]
                    assert len(names) == len(set(names)), "duplicate names in readdir"
            try:
                me.create("hot/same", is_dir=False, layout=None, exclusive=True)
                with lk:
                    winners.append(t)
            except MDTError as exc:
                assert exc.code == "EXISTS", exc.code
        except Exception as exc:  # noqa: BLE001
            with lk:
                errors.append(f"t{t}: {exc!r}")
    ts = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    names = sorted(e["name"] for e in c.readdir("hot"))
    want = sorted([f"t{t}-{i}" for t in range(threads) for i in range(per) if i % 3] + ["same"])
    return {"errors": errors, "winners": len(winners), "listing_ok": names == want,
            "count_ok": c.child_count("hot") == len(want)}


res = {}
try:
    # a fresh MDT: its lookup table is registered at start-up, before any
    # client asked for it (lazily, the first request paid seconds on site)
    fresh = tr.serve_native("mdt", addr())
    handles.append(fresh)
    from roce.protocol import Frame, Op
    r = RoCETransport().send_request(fresh.local_address(), Frame(op=Op.STATS, request_id=1, key="", value=b""))
    res["fresh_mdt_stats"] = json.loads(r.value)
    p_ost, n_ost = py_ost(), native("ost")
    res["ost_python"] = ost_script(RemoteEngine(p_ost, RoCETransport()))
    res["ost_native"] = ost_script(RemoteEngine(n_ost, RoCETransport()))
    p_mdt, n_mdt = py_mdt(), native("mdt")
    res["mdt_python"] = mdt_script(MDTClient(RoCETransport(), NamespaceShard([p_mdt])))
    res["mdt_native"] = mdt_script(MDTClient(RoCETransport(), NamespaceShard([n_mdt])))
    res["dfs_native"] = dfs_script([native("mdt"), native("mdt")], [native("ost") for _ in range(4)])
    res["dfs_python"] = dfs_script([py_mdt(), py_mdt()], [py_ost() for _ in range(4)])
    res["cas_native"] = cas_hammer(native("ost"))
    res["mdt_hammer_native"] = mdt_hammer(native("mdt"))
    res["mdt_hammer_python"] = mdt_hammer(py_mdt())
    res["native_stats"] = [h.stats() for h in handles if hasattr(h, "stats")]
except Exception as exc:  # noqa: BLE001
    import traceback
    res["error"] = traceback.format_exc()
print("RESULT " + json.dumps(res), flush=True)
import os  # noqa: E402
os._exit(0)
