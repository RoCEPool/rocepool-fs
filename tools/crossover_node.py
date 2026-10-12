"""One process = one node, run inside one network namespace of the
crossover-cable topology (tools/netlink_veth.py) by
tools/run_crossover_rdma.py. Uses RoCETransport only -- no TCP transport
anywhere in the data path this process controls.

Roles (argv[1]):
  pool-server <me> <all>      PoolNode member; serves until stdin closes
                              (<all> == <me>: a single-node OST/OSS)
  mdt-server <me>             fs/mdt_node.py metadata server; serves until
                              stdin closes
  ost-native <me>             native C OST (roce/native/rocepool_servers.c)
  mdt-native <me>             native C MDT (same file); both serve until
                              stdin closes
  pool-workload <me> <all>    PoolNode member that also drives a workload
                              across the cluster, prints one JSON result
  mpi <rank> <all>            one MPI rank; runs the collectives suite,
                              prints one JSON result
  hpc <rank> <all> <osts>     MPI rank + Lustre-style client over OST
                              servers; runs the Lustre/MPI-IO/MPI suite
  stream <peer>               RoCEStream client against a pool-server's
                              request/response endpoint
<all> is a comma-separated list of host:port.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mpi.comm import Communicator  # noqa: E402
from roce.pool_node import PoolNode, _pack_put_value  # noqa: E402
from roce.protocol import HEADER_SIZE, Frame, Op  # noqa: E402
from roce.roce_transport import RoCEStream, RoCETransport  # noqa: E402
from storage_engine.base import VersionConflict  # noqa: E402
from storage_engine.memory_engine import MemoryEngine  # noqa: E402


def _emit(obj) -> None:
    print("RESULT " + json.dumps(obj), flush=True)


def _payload(i: int) -> bytes:
    # sizes 0 B .. ~256 KB, deterministic, content-addressable
    size = (i * 7919) % (256 * 1024) if i % 5 else 0
    seed = hashlib.sha256(str(i).encode()).digest()
    return (seed * (size // 32 + 1))[:size]


def _install_mem_probe(engine=None) -> None:
    """kill -USR1 <pid>: print one MEMPROBE JSON line -- RSS/threads, glibc
    mallinfo2 (in-use vs freed-but-retained heap vs mmapped), what the
    engine actually stores, and tracemalloc's top allocation sites when
    PYTHONTRACEMALLOC is set. Used by tools/stress_concurrency.py
    --mem-probe to attribute server memory under load."""
    import ctypes
    import signal
    import threading
    import tracemalloc

    class _MallInfo2(ctypes.Structure):
        _fields_ = [(n, ctypes.c_size_t) for n in
                    ("arena", "ordblks", "smblks", "hblks", "hblkhd", "usmblks", "fsmblks",
                     "uordblks", "fordblks", "keepcost")]

    def _probe(*_):
        try:
            libc = ctypes.CDLL("libc.so.6")
            libc.mallinfo2.restype = _MallInfo2
            mi = libc.mallinfo2()
            status = open("/proc/self/status").read()
            rss = int(next(l for l in status.splitlines() if l.startswith("VmRSS:")).split()[1]) // 1024
            out = {"rss_mb": rss, "threads": threading.active_count(),
                   "malloc_in_use_mb": round(mi.uordblks / 2**20, 1),
                   "malloc_free_retained_mb": round(mi.fordblks / 2**20, 1),
                   "malloc_mmapped_mb": round(mi.hblkhd / 2**20, 1),
                   "malloc_arena_mb": round(mi.arena / 2**20, 1)}
            if engine is not None:
                out["engine_bytes_mb"] = round(engine.total_bytes() / 2**20, 1)
                out["engine_objects"] = len(getattr(engine, "_objects", {}))
            if tracemalloc.is_tracing():
                cur, peak = tracemalloc.get_traced_memory()
                out["py_traced_mb"] = round(cur / 2**20, 1)
                out["py_traced_peak_mb"] = round(peak / 2**20, 1)
                out["py_top"] = [f"{st.traceback[0].filename.rsplit('/', 2)[-1]}:{st.traceback[0].lineno} "
                                 f"{st.size / 2**20:.1f}MiB x{st.count}"
                                 for st in tracemalloc.take_snapshot().statistics("lineno")[:8]]
            print("MEMPROBE " + json.dumps(out), flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"MEMPROBE_ERROR {exc!r}", flush=True)
    signal.signal(signal.SIGUSR1, _probe)


def pool_server(me: str, all_nodes: list[str]) -> None:
    engine = MemoryEngine()
    node = PoolNode(local_address=me, nodes=all_nodes, engine=engine, transport=RoCETransport())
    _install_mem_probe(engine)
    node.start()
    print(f"READY {me}", flush=True)
    sys.stdin.read()  # orchestrator closes stdin to stop us
    node.stop()


def native_server(kind: str, me: str) -> None:
    """ost-native / mdt-native: the C servers in roce/native/rocepool_servers.c
    -- no Python on the request path. SIGUSR1 prints the same MEMPROBE line
    shape as the Python servers (RSS, malloc stats, stored bytes)."""
    import signal
    tr = RoCETransport()
    handle = tr.serve_native(kind, me)

    def _probe(*_):
        import ctypes
        st = handle.stats()
        status = open("/proc/self/status").read()
        rss = int(next(l for l in status.splitlines() if l.startswith("VmRSS:")).split()[1]) // 1024
        threads = int(next(l for l in status.splitlines() if l.startswith("Threads:")).split()[1])

        class _MI(ctypes.Structure):
            _fields_ = [(n, ctypes.c_size_t) for n in ("arena", "ordblks", "smblks", "hblks", "hblkhd",
                                                       "usmblks", "fsmblks", "uordblks", "fordblks", "keepcost")]
        libc = ctypes.CDLL("libc.so.6")
        libc.mallinfo2.restype = _MI
        mi = libc.mallinfo2()
        print("MEMPROBE " + json.dumps({
            "rss_mb": rss, "threads": threads, "malloc_in_use_mb": round(mi.uordblks / 2**20, 1),
            "malloc_free_retained_mb": round(mi.fordblks / 2**20, 1), "malloc_mmapped_mb": round(mi.hblkhd / 2**20, 1),
            "engine_bytes_mb": round(st["bytes"] / 2**20, 1), "engine_objects": st["objects"], "ops": st["ops"]}),
            flush=True)
    signal.signal(signal.SIGUSR1, _probe)
    print(f"READY {me}", flush=True)
    sys.stdin.read()
    handle.stop()


def mdt_server(me: str) -> None:
    from fs.mdt_node import MDTServer
    server = MDTServer(RoCETransport())
    server.start(me)
    print(f"READY {me}", flush=True)
    sys.stdin.read()
    server.stop()


def pool_workload(me: str, all_nodes: list[str]) -> None:
    node = PoolNode(local_address=me, nodes=all_nodes, engine=MemoryEngine(), transport=RoCETransport())
    node.start()
    print(f"READY {me}", flush=True)
    sys.stdin.readline()  # orchestrator says "go" once every node is up

    n = 120
    owners: dict[str, int] = {}
    t0 = time.monotonic()
    for i in range(n):
        key = f"xover/{me}/obj{i:04d}"
        owners[node.owner_of(key)] = owners.get(node.owner_of(key), 0) + 1
        node.put(key, _payload(i))
    t_put = time.monotonic() - t0

    t0 = time.monotonic()
    mismatches = [i for i in range(n) if node.get(f"xover/{me}/obj{i:04d}") != _payload(i)]
    t_get = time.monotonic() - t0

    # compare-and-swap across the link: stale version must be rejected remotely
    cas_key = f"xover/{me}/cas"
    v1 = node.put(cas_key, b"v1").version
    node.put(cas_key, b"v2", expected_version=v1)
    try:
        node.put(cas_key, b"stale", expected_version=v1)
        cas_ok = False
    except VersionConflict:
        cas_ok = node.get(cas_key) == b"v2"

    listed = len(node.list_prefix(f"xover/{me}/"))
    node.delete(f"xover/{me}/obj0000")
    deleted_ok = not node.exists(f"xover/{me}/obj0000")

    _emit({
        "role": "pool-workload", "me": me, "objects": n,
        "bytes": sum(len(_payload(i)) for i in range(n)),
        "owners": owners, "remote_owned": sum(v for k, v in owners.items() if k != me),
        "get_mismatches": mismatches, "cas_ok": cas_ok,
        "list_prefix_count": listed, "delete_ok": deleted_ok,
        "put_s": round(t_put, 3), "get_s": round(t_get, 3),
    })
    sys.stdin.read()
    node.stop()


def mpi_rank(rank: int, all_nodes: list[str]) -> None:
    comm = Communicator(rank=rank, addresses=all_nodes, transport=RoCETransport())
    size = len(all_nodes)
    comm.barrier()
    b = comm.bcast(b"hello-from-root" if rank == 0 else None, root=0)
    g = comm.gather(f"r{rank}".encode(), root=0)
    ag = comm.allgather(f"r{rank}@{all_nodes[rank]}".encode())
    total = comm.allreduce_sum(rank + 1)
    nxt, prv = (rank + 1) % size, (rank - 1) % size
    comm.send(nxt, "ring", f"token-from-{rank}".encode())
    ring = comm.recv(prv, "ring")
    comm.barrier()
    _emit({
        "role": "mpi", "rank": rank, "addr": all_nodes[rank],
        "bcast": b.decode(), "gather": [x.decode() for x in g] if g else None,
        "allgather": [x.decode() for x in ag], "allreduce_sum": total,
        "ring_recv": ring.decode(),
    })
    comm.barrier()  # nobody closes its server while a peer may still send
    comm.close()


def stream_client(peer: str) -> None:
    """Exercises roce_client_stream_send/recv over the wire, and RoCEStream's
    byte-stream buffering, by reading the response back in small pieces."""
    results = []
    with RoCEStream.connect(peer) as s:
        for i, size in enumerate((0, 1, 4096, 300_000)):
            value = bytes((i + j) % 251 for j in range(size))
            s.sendall(Frame(op=Op.PUT, request_id=i, key=f"stream/{i}",
                            value=_pack_put_value(value, None)).encode())
            s.sendall(Frame(op=Op.GET, request_id=100 + i, key=f"stream/{i}", value=b"").encode())
            # two responses are now queued; read each back in 7-byte slices
            for expect_op in (Op.PUT_OK, Op.GET_OK):
                header = b""
                while len(header) < HEADER_SIZE:
                    header += s.recv(min(7, HEADER_SIZE - len(header)))
                body_len = Frame.peek_body_length(header)
                body = b""
                while len(body) < body_len:
                    body += s.recv(min(7, body_len - len(body)))
                f = Frame.decode(header + body)
                ok = (f.op == expect_op) and (expect_op != Op.GET_OK or f.value == value)
                results.append({"size": size, "expect": expect_op.name, "got": Op(f.op).name, "ok": ok})
    _emit({"role": "stream", "peer": peer, "checks": len(results),
           "all_ok": all(r["ok"] for r in results), "detail": results})


# ---------------------------------------------------------------------------
# "hpc": one MPI rank that is also a Lustre-style client. Every rank mounts
# the SAME striped namespace over RemoteEngine handles to the OST servers
# (like a shared Lustre mount on every compute node), then runs the Lustre,
# MPI-IO and MPI basics below. Each check is recorded, not asserted, so one
# failure doesn't hide the rest.
# ---------------------------------------------------------------------------
def _data(seed: str, n: int) -> bytes:
    out, block, i = bytearray(), b"", 0
    while len(out) < n:
        block = hashlib.sha256(f"{seed}:{i}".encode()).digest()
        out += block
        i += 1
    return bytes(out[:n])


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()[:16]


def hpc_rank(rank: int, ranks: list[str], osts_addr: list[str]) -> None:
    from fs.striping import StripeLayout, StripedStorage, _stripe_key
    from mpi.mpi_file import MPIFile
    from roce.remote_engine import RemoteEngine

    comm = Communicator(rank=rank, addresses=ranks, transport=RoCETransport())
    n = comm.size
    osts = [RemoteEngine(a, RoCETransport()) for a in osts_addr]
    fs = StripedStorage(osts, default_layout=StripeLayout(stripe_count=len(osts), stripe_size=64 * 1024))
    checks: list[dict] = []

    def check(name: str, ok: bool, detail=None) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    def run(name: str, fn) -> None:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 -- record, keep going, stay in lockstep
            check(name + " (exception)", False, f"{type(exc).__name__}: {exc}")

    def objects_on(ost_index: int, path: str) -> int:
        prefix = _stripe_key(path, 0)[:-8]  # "<path>.stripe."
        return sum(m.size for m in osts[ost_index].list_prefix(prefix))

    # ---- Lustre basics -----------------------------------------------------
    WIDE = "/lustre/wide.bin"
    wide = bytearray(_data("wide", 1024 * 1024))

    def l1():
        if rank == 0:
            fs.put(WIDE, bytes(wide), layout=StripeLayout(stripe_count=4, stripe_size=64 * 1024, stripe_offset=0))
    run("L1 setstripe+write (rank0/nodeA)", l1)
    comm.barrier()

    def l1r():
        lay = fs.get_layout(WIDE)
        check("L1 getstripe seen by every client", (lay.stripe_count, lay.stripe_size, lay.stripe_offset) == (4, 65536, 0),
              [lay.stripe_count, lay.stripe_size, lay.stripe_offset])
        check("L1 stripe placement round-robin OST0..3", fs.stripe_locations(WIDE) == [0, 1, 2, 3] * 4)
        check("L1 cross-node read coherence (sha)", _sha(fs.get(WIDE)) == _sha(bytes(wide)))
        per_ost = [objects_on(i, WIDE) for i in range(4)]
        check("L2 bytes land on OSTs on BOTH nodes, 256KiB each", per_ost == [256 * 1024] * 4, per_ost)
    run("L1/L2", l1r)
    comm.barrier()

    NARROW = "/lustre/narrow.bin"
    narrow = _data("narrow", 200 * 1024)

    def l3():
        if rank == 1:
            fs.put(NARROW, narrow, layout=StripeLayout(stripe_count=2, stripe_size=32 * 1024, stripe_offset=1))
    run("L3 setstripe -c 2 -i 1 (rank1/nodeB)", l3)
    comm.barrier()

    def l3r():
        check("L3 only OST1,OST2 used", set(fs.stripe_locations(NARROW)) == {1, 2}, fs.stripe_locations(NARROW))
        check("L3 unused OSTs hold nothing for the file", objects_on(0, NARROW) == 0 and objects_on(3, NARROW) == 0)
        check("L3 content", fs.get(NARROW) == narrow)
    run("L3", l3r)
    comm.barrier()

    patch = _data("patch", 10_000)
    at = 64 * 1024 - 5000  # straddles the stripe 0 / stripe 1 boundary (OST0 on nodeA, OST1 on nodeB)
    wide[at:at + len(patch)] = patch

    def l4():
        if rank == 2:
            fs.write_range(WIDE, at, patch)
    run("L4 overwrite across a stripe boundary", l4)
    comm.barrier()
    run("L4", lambda: (check("L4 read_range of patched span", fs.read_range(WIDE, at, len(patch)) == patch),
                      check("L4 whole file after patch", _sha(fs.get(WIDE)) == _sha(bytes(wide)))))
    comm.barrier()

    tail = _data("tail", 70_000)

    def l5():
        if rank == 3:
            fs.write_range(WIDE, len(wide), tail)
    run("L5 append", l5)
    wide += tail
    comm.barrier()
    run("L5", lambda: (check("L5 size after append seen by all", fs.total_size(WIDE) == len(wide), fs.total_size(WIDE)),
                      check("L5 content after append", _sha(fs.get(WIDE)) == _sha(bytes(wide)))))
    comm.barrier()

    SPARSE = "/lustre/sparse.bin"

    def l6():
        if rank == 0:
            fs.put(SPARSE, b"head")
            fs.write_range(SPARSE, 300_000, b"tail")
    run("L6 write past EOF", l6)
    comm.barrier()

    def l6r():
        got = fs.get(SPARSE)
        check("L6 sparse file size", len(got) == 300_004, len(got))
        check("L6 hole reads as zeros", got[:4] == b"head" and got[4:300_000] == bytes(299_996) and got[-4:] == b"tail")
    run("L6", l6r)
    comm.barrier()

    BIG = "/lustre/big.bin"
    big = _data("big", 8 * 1024 * 1024)

    def l7():
        if rank == 1:
            fs.put(BIG, big, layout=StripeLayout(stripe_count=4, stripe_size=1024 * 1024))
    run("L7 8MiB file, 1MiB stripes (rank1/nodeB)", l7)
    comm.barrier()
    run("L7", lambda: check("L7 8MiB integrity on every client", _sha(fs.get(BIG)) == _sha(big)))
    comm.barrier()

    def l8():
        if rank == 0:
            fs.delete(NARROW)
    run("L8 unlink", l8)
    comm.barrier()
    run("L8", lambda: check("L8 unlink: gone for all clients, objects freed on OSTs",
                            not fs.exists(NARROW) and all(objects_on(i, NARROW) == 0 for i in range(4))))
    comm.barrier()

    # ---- MPI-IO on the striped file system --------------------------------
    R = 300 * 1024  # segment size NOT a multiple of the 64KiB stripe: neighbours share stripes

    def m1():
        f = MPIFile.open(comm, fs, "/lustre/n1_segmented.bin")
        f.set_view(disp=rank * R)
        f.write_at_all(0, _data(f"seg{rank}", R))
        whole = MPIFile(comm, fs, "/lustre/n1_segmented.bin").read_at_all(0, n * R)
        expect = b"".join(_data(f"seg{r}", R) for r in range(n))
        check("M1 N-1 segmented write_at_all/read_at_all", whole == expect,
              [i for i in range(n) if whole[i * R:(i + 1) * R] != expect[i * R:(i + 1) * R]])
        check("M2 get_size_all after collective write", f.get_size_all() == n * R)
    run("M1/M2", m1)
    comm.barrier()

    B, K = 16 * 1024, 16  # 16KiB blocks, 4 per stripe: every stripe written by all 4 ranks at once

    def m3():
        f = MPIFile.open(comm, fs, "/lustre/n1_strided.bin")
        comm.barrier()
        for k in range(K):
            f.write_at((k * n + rank) * B, _data(f"blk{rank}.{k}", B))
        comm.barrier()
        whole = f.read_at_all(0, K * n * B)
        bad = [(k, r) for k in range(K) for r in range(n)
               if whole[(k * n + r) * B:(k * n + r + 1) * B] != _data(f"blk{r}.{k}", B)]
        check("M3 N-1 strided (4 ranks share every stripe)", not bad, {"lost_blocks": len(bad), "of": K * n})
    run("M3", m3)
    comm.barrier()

    def m4():
        f = MPIFile.open(comm, fs, f"/lustre/nn/rank{rank}.bin")
        f.write_at(0, _data(f"own{rank}", 200_000))
        comm.barrier()
        ok = all(fs.get(f"/lustre/nn/rank{r}.bin") == _data(f"own{r}", 200_000) for r in range(n))
        check("M4 N-N file per process, read by everyone", ok)
    run("M4", m4)
    comm.barrier()

    # ---- MPI basics ---------------------------------------------------------
    def p1():
        if rank == 0:
            for i in range(100):
                comm.send(1, "seq", str(i).encode())
        if rank == 1:
            got = [int(comm.recv(0, "seq")) for _ in range(100)]
            check("P1 point-to-point ordering, 100 msgs nodeA->nodeB", got == list(range(100)))
    run("P1", p1)
    comm.barrier()

    def p2():
        if rank == 3:
            comm.send(2, "b", b"second-tag")
            comm.send(2, "a", b"first-tag")
        if rank == 2:
            a, b = comm.recv(3, "a"), comm.recv(3, "b")
            check("P2 tag matching (receive out of send order)", (a, b) == (b"first-tag", b"second-tag"))
    run("P2", p2)
    comm.barrier()

    run("P3", lambda: check("P3 bcast 4MiB from root 3 (nodeB)",
                            _sha(comm.bcast(_data("bc", 4 * 1024 * 1024) if rank == 3 else None, root=3))
                            == _sha(_data("bc", 4 * 1024 * 1024))))
    comm.barrier()

    def p4():
        vals = [-7, 1_000_000_000_000, 3, -2]
        red = comm.reduce_sum(vals[rank], root=1)
        if rank == 1:
            check("P4 reduce_sum to root 1 (negatives, >32-bit)", red == sum(vals), red)
        check("P4 allreduce_sum", comm.allreduce_sum(vals[rank]) == sum(vals))
        g = comm.gather(b"x" * (rank * 1000), root=2)
        if rank == 2:
            check("P4 gather variable sizes", [len(x) for x in g] == [0, 1000, 2000, 3000])
        check("P4 allgather", comm.allgather(str(rank).encode()) == [str(r).encode() for r in range(n)])
    run("P4", p4)

    comm.barrier()
    _emit({"role": "hpc", "rank": rank, "addr": ranks[rank], "checks": checks})
    comm.barrier()
    comm.close()


if __name__ == "__main__":
    role = sys.argv[1]
    if role == "pool-server":
        pool_server(sys.argv[2], sys.argv[3].split(","))
    elif role in ("ost-native", "mdt-native"):
        native_server(role.split("-")[0], sys.argv[2])
    elif role == "mdt-server":
        mdt_server(sys.argv[2])
    elif role == "pool-workload":
        pool_workload(sys.argv[2], sys.argv[3].split(","))
    elif role == "mpi":
        mpi_rank(int(sys.argv[2]), sys.argv[3].split(","))
    elif role == "hpc":
        hpc_rank(int(sys.argv[2]), sys.argv[3].split(","), sys.argv[4].split(","))
    elif role == "stream":
        stream_client(sys.argv[2])
    else:
        raise SystemExit(f"unknown role {role}")
