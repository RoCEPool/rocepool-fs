"""Automated IOR/mdtest benchmark against this project's POSIX layer. One
command, no manual steps: builds whatever's missing (MPICH, then IOR --
real upstream sources, unmodified), mounts the filesystem, runs a small
matrix, verifies every byte IOR wrote, and writes a JSON report.

    python3 tools/bench_ior.py                 # needs root (real kernel FUSE mount)
    python3 tools/bench_ior.py --backend pool   # distributed: PoolNode across
                                                 # the crossover-cable two-node link
                                                 # (see tools/run_crossover_rdma.py)
    python3 tools/bench_ior.py --quick          # smaller matrix, for a fast check

IOR and mdtest are the real upstream binaries (https://github.com/hpc/ior),
the same tools IO500 and vendor comparisons (Lustre, GPFS, WEKA...) use --
nothing about them is written for or tuned to this project. They talk
POSIX to whatever is mounted at the mountpoint; this project's own code
is only the FUSE driver underneath.

What "--backend pool" tests: fs/posix_api.py's `_Backend` protocol
(put/get/delete/stat/list_prefix/exists with expected_version) is exactly
what `PoolNode` already implements, so PoolNode can sit under
PooledFileSystem directly -- mounting it exposes a real 3-node
RoCETransport-backed pool as one POSIX filesystem, and IOR's file-per-
process pattern then genuinely fans out across the cable (each file's
owner is chosen by key hash). IOR's default single-shared-file pattern
does NOT parallelize this way (there is no striping under PoolNode, only
under fs/striping.py, which isn't POSIX-mounted here) -- the report says
which pattern ran.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
CACHE = os.path.join(ROOT, ".bench_cache")
MPICH_PREFIX = os.path.join(CACHE, "mpich")
IOR_SRC = os.path.join(CACHE, "ior_src")
MPICH_TARBALL_URL = "https://github.com/pmodels/mpich/releases/download/v4.2.3/mpich-4.2.3.tar.gz"


def _run(cmd, **kw):
    print("+ " + " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True, **kw)


class _Intercept:
    """--client intercept: no mount. IOR/mdtest ranks run on node A with the
    LD_PRELOAD interception library (roce/native/rocepool_intercept.c) and
    serve paths under `prefix` themselves; the bench's own mkdir/rm on the
    data go through a preloaded python."""

    def __init__(self, topo, prefix: str, env: dict, lib: str, mdts: list[str], osts: list[str], attr_ttl_s: float):
        self.topo, self.prefix = topo, prefix
        self.env = dict(env, LD_PRELOAD=lib, ROCEPOOL_PREFIX=prefix, ROCEPOOL_MDTS=",".join(mdts),
                        ROCEPOOL_OSTS=",".join(osts), ROCEPOOL_ATTR_TTL=str(attr_ttl_s), ROCEPOOL_IO_THREADS="4")

    def wrap(self, argv: list[str]) -> list[str]:
        return self.topo.launch["A"].cmd(argv)

    def py(self, code: str, *args: str) -> None:
        subprocess.run(self.wrap([sys.executable, "-c", code, *args]), env=self.env, check=True, timeout=600)

    def makedirs(self, path: str) -> None:
        self.py("import os, sys; os.makedirs(sys.argv[1], exist_ok=True)", path)

    def remove(self, path: str) -> None:
        self.py("import os, sys\ntry: os.remove(sys.argv[1])\nexcept FileNotFoundError: pass", path)


ICP: _Intercept | None = None  # set by mount_dist(client="intercept")


def ensure_mpi() -> str:
    """Returns the bin/ dir holding mpicc/mpirun, building MPICH from
    real upstream source if this machine has none. Cached under
    .bench_cache/ so a second run is instant."""
    system_mpicc = shutil.which("mpicc")
    if system_mpicc:
        return os.path.dirname(system_mpicc)
    bin_dir = os.path.join(MPICH_PREFIX, "bin")
    if os.path.exists(os.path.join(bin_dir, "mpicc")):
        return bin_dir
    from tools import deps
    os.makedirs(CACHE, exist_ok=True)
    srcdir = os.path.join(CACHE, "mpich-src")
    if not os.path.exists(os.path.join(srcdir, "configure")) and not deps.extract("mpich", srcdir):
        tarball = os.path.join(CACHE, "mpich.tar.gz")
        if not os.path.exists(tarball):
            deps.check_download("MPICH")
            _run(["curl", "-sSL", "-o", tarball, MPICH_TARBALL_URL])
        _run(["tar", "xzf", tarball, "-C", CACHE])
        if os.path.exists(srcdir):
            shutil.rmtree(srcdir)
        os.rename(os.path.join(CACHE, "mpich-4.2.3"), srcdir)
    # ch3:sock + the dataloop datatype engine: what the bundled, trimmed
    # source keeps (no yaksa / ucx / libfabric modules) -- and all IOR needs
    _run(["./configure", f"--prefix={MPICH_PREFIX}", "--disable-fortran", "--with-device=ch3:sock",
          "--with-datatype-engine=dataloop"], cwd=srcdir)
    _run(["make", "-j", str(os.cpu_count() or 2)], cwd=srcdir)
    _run(["make", "install"], cwd=srcdir)
    return bin_dir


def ensure_ior(mpi_bin: str) -> tuple[str, str]:
    """Returns (ior_binary, mdtest_binary), building real upstream IOR
    from source against the MPI found/built above if not already built."""
    ior_bin = os.path.join(IOR_SRC, "src", "ior")
    mdtest_bin = os.path.join(IOR_SRC, "src", "mdtest")
    if os.path.exists(ior_bin) and os.path.exists(mdtest_bin):
        return ior_bin, mdtest_bin
    from tools import deps
    if not os.path.exists(IOR_SRC) and not deps.extract("ior", IOR_SRC):
        deps.check_download("IOR")
        _run(["git", "clone", "--depth", "1", "https://github.com/hpc/ior.git", IOR_SRC])
    env = dict(os.environ, PATH=f"{mpi_bin}:{os.environ['PATH']}")
    if not os.path.exists(os.path.join(IOR_SRC, "configure")):  # the bundled copy comes bootstrapped
        _run(["./bootstrap"], cwd=IOR_SRC, env=env)
    _run(["./configure", "CC=mpicc"], cwd=IOR_SRC, env=env)
    _run(["make", "-j", str(os.cpu_count() or 2)], cwd=IOR_SRC, env=env)
    return ior_bin, mdtest_bin


def mount_local(mountpoint: str) -> subprocess.Popen:
    os.makedirs(mountpoint, exist_ok=True)
    script = os.path.join(ROOT, "examples", "mount_real_kernel_demo.py")
    p = subprocess.Popen([sys.executable, "-u", script, mountpoint],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if subprocess.run(["mountpoint", "-q", mountpoint]).returncode == 0:
            return p
        if p.poll() is not None:
            raise RuntimeError("mount process exited: " + (p.stdout.read() if p.stdout else ""))
        time.sleep(0.1)
    raise TimeoutError("filesystem did not mount in time")


def unmount_local(mountpoint: str, proc: subprocess.Popen) -> None:
    subprocess.run(["umount", mountpoint], capture_output=True)
    proc.wait(timeout=10)


def mount_pool(mountpoint: str) -> tuple[subprocess.Popen, list[subprocess.Popen]]:
    """A 3-node PoolNode cluster over the crossover-cable veth link
    (tools/netlink_veth.py) via RoCETransport, with one node mounted as a
    real kernel FUSE filesystem -- so IOR's POSIX calls genuinely cross
    the cable to reach files owned by the other two nodes."""
    from tools import netlink_veth
    netlink_veth.up()
    nodes = ["10.99.0.1:19801", "10.99.0.2:19801", "10.99.0.2:19802"]
    ns = ["rocepool_nodeA", "rocepool_nodeB", "rocepool_nodeB"]
    from roce.native.build_lib import ensure_softverbs_lib
    soft = ensure_softverbs_lib()  # rebuilds if any source is newer than the .so
    # PoolNode has no chunking of its own (unlike fs/striping.py's OSTs):
    # write_range's CAS put (fs/posix_api.py) sends the WHOLE current object
    # every time, so a single un-striped file bigger than RoCETransport's
    # max_frame_bytes literally cannot be stored over this path at its
    # default (16 MiB). IOR's matrix writes up to a 32 MiB single-shared-
    # file block per rank, so this pool mount needs a bigger cap -- found by
    # running the matrix for real: "request_len ... exceeds max_frame_bytes
    # 16777216" once a file crossed 16 MiB. Both ends of every RoCETransport
    # connection must agree, so every subprocess launched with `env` below
    # (both backers and the mounter) gets the same value.
    env = dict(os.environ, ROCE_TRANSPORT_LIB=soft, PYTHONPATH=ROOT,
              ROCE_MAX_FRAME_BYTES=str(128 * 1024 * 1024))
    helper = os.path.join(ROOT, "tools", "crossover_node.py")
    backers = [subprocess.Popen(
        ["nsenter", f"--net=/run/netns/{ns[i]}", sys.executable, "-u", helper, "pool-server", nodes[i], ",".join(nodes)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, cwd=ROOT)
        for i in (1, 2)]
    for p in backers:
        line = p.stdout.readline()
        if not line.startswith("READY"):
            raise RuntimeError(f"OST/pool backer did not come up: {line}{p.stdout.read()}")

    os.makedirs(mountpoint, exist_ok=True)
    mount_script = f"""
import sys; sys.path.insert(0, {ROOT!r})
from fs.fuse_lowlevel import FuseKernelMount, FuseLowLevelDaemon
from fs.posix_api import PooledFileSystem
from roce.pool_node import PoolNode
from roce.roce_transport import RoCETransport
node = PoolNode(local_address={nodes[0]!r}, nodes={nodes!r}, engine=__import__('storage_engine.memory_engine', fromlist=['MemoryEngine']).MemoryEngine(), transport=RoCETransport())
node.start()
mount = FuseKernelMount({mountpoint!r})
print("READY", flush=True)
FuseLowLevelDaemon(mount.fd, PooledFileSystem(node)).run()
"""
    # The mount daemon's own stdout/stderr (tracebacks from FuseLowLevelDaemon's
    # per-request catch-all, see fs/fuse_lowlevel.py's _dispatch) go to a log
    # file rather than a PIPE: a PIPE fills up and blocks the daemon mid-request
    # once it prints enough (every 500-error traceback, say) since nothing here
    # was draining it, which silently wedges IOR's I/O instead of surfacing the
    # real error. main() prints this file's tail whenever a case fails.
    mounter_log_path = os.path.join(CACHE, "pool_mounter.log")
    os.makedirs(CACHE, exist_ok=True)
    mounter_log = open(mounter_log_path, "w")
    mounter = subprocess.Popen(
        ["nsenter", f"--net=/run/netns/{ns[0]}", sys.executable, "-u", "-c", mount_script],
        stdin=subprocess.PIPE, stdout=mounter_log, stderr=subprocess.STDOUT, text=True, env=env, cwd=ROOT)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if subprocess.run(["mountpoint", "-q", mountpoint]).returncode == 0:
            return mounter, backers
        if mounter.poll() is not None:
            raise RuntimeError("pool mount exited: " + open(mounter_log_path).read())
        time.sleep(0.1)
    raise TimeoutError("pool filesystem did not mount in time")


def start_dist_servers(*, n_mdts: int = 2, n_osts: int = 4, extra_env: dict | None = None,
                       wrapper: list[str] | None = None, native: bool = True, topo=None):
    """Bring up the two nodes (tools/topology.py: the crossover rig's
    namespaces by default, real RoCE hosts under ROCEPOOL_TOPOLOGY) and
    `n_mdts` MDT + `n_osts` OST server processes alternating between them.
    Returns (server Popens, [(node, mdt_addr)], [(node, ost_addr)], env).
    Shared by mount_dist and tools/stress_concurrency.py."""
    from tools.topology import Topology
    topo = topo or Topology.current()
    topo.up()
    cycle = ["A", "B"]
    mdts = [(cycle[i % 2], topo.ep(cycle[i % 2], 19901 + i)) for i in range(n_mdts)]
    osts = [(cycle[i % 2], topo.ep(cycle[i % 2], 19951 + i)) for i in range(n_osts)]
    # No object on this path is ever bigger than one stripe (1 MiB), so the
    # default 16 MiB frame cap is ample -- mount_pool needed 128 MiB only
    # because PoolNode stores a whole un-striped file as ONE object.
    env = topo.env(extra_env)
    helper = os.path.join(ROOT, "tools", "crossover_node.py")
    servers = []
    for node, addr in mdts:
        servers.append(topo.popen(node, [helper, *(["mdt-native", addr] if native else ["mdt-server", addr])], env,
                                  wrapper=wrapper, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True))
    for node, addr in osts:
        servers.append(topo.popen(node, [helper, *(["ost-native", addr] if native else ["pool-server", addr, addr])],
                                  env, wrapper=wrapper, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True))
    for p in servers:
        seen = ""
        while True:  # a debugger wrapper may print its own banner first
            line = p.stdout.readline()
            if line.startswith("READY"):
                break
            seen += line
            if not line or len(seen) > 20000:
                raise RuntimeError(f"MDT/OST server did not come up: {seen}{p.stdout.read() if not line else ''}")
    return servers, mdts, osts, env


def mount_dist(mountpoint: str, *, fuse_threads: int = 4, attr_ttl_s: float = 3.0,
               n_osts: int = 4, n_mdts: int = 2, native: bool = True,
               client: str = "native", native_threads: int = 16) -> tuple[subprocess.Popen, list[subprocess.Popen]]:
    """The redesigned distributed layout over the same crossover-cable link:
    `n_mdts` metadata servers (fs/mdt_node.py) and `n_osts` single-node OST
    servers (roce/pool_node.py, unchanged), split across both network
    namespaces, with the client mounted in rocepool_nodeA as a
    DistributedFileSystem (fs/posix_api.py) -- metadata sharded across the
    MDTs and cached client-side, file bytes striped across every OST, the
    multi-OST work fanned out concurrently, the FUSE daemon multi-threaded.
    Unlike mount_pool, the client holds no data itself: every byte and
    every metadata op crosses to a server process."""
    from tools.topology import Topology
    topo = Topology.current()
    servers, mdts, osts, env = start_dist_servers(n_mdts=n_mdts, n_osts=n_osts, native=native, topo=topo)
    if client == "intercept":
        global ICP
        from roce.native.build_lib import ensure_intercept_lib
        lib = os.environ.get("ROCEPOOL_INTERCEPT_LIB") or (topo.intercept_lib if topo.real else ensure_intercept_lib())
        ICP = _Intercept(topo, mountpoint, env, lib, [a for _, a in mdts], [a for _, a in osts], attr_ttl_s)
        ICP.makedirs(mountpoint)  # the pool's root: nothing on the host
        return None, servers
    os.makedirs(mountpoint, exist_ok=True)
    if client == "native":
        # roce/native/rocepool_fuse.c: the same layout/protocol as the Python
        # client below, in C (no GIL), with 1 MiB kernel requests
        mounter_log_path = os.path.join(CACHE, "pool_mounter.log")
        os.makedirs(CACHE, exist_ok=True)
        client_bin = os.environ.get("ROCEPOOL_FUSE_BIN") or topo.fuse_client  # override: A/B comparisons
        mounter = topo.popen_cmd("A", [client_bin, "--mount", mountpoint,
                                       "--mdts", ",".join(a for _, a in mdts), "--osts", ",".join(a for _, a in osts),
                                       "--attr-ttl", str(attr_ttl_s), "--threads", str(native_threads)],
                                 env, stdin=subprocess.PIPE, stdout=open(mounter_log_path, "w"),
                                 stderr=subprocess.STDOUT, text=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if subprocess.run(["mountpoint", "-q", mountpoint]).returncode == 0:
                return mounter, servers
            if mounter.poll() is not None:
                raise RuntimeError("native client exited: " + open(mounter_log_path).read())
            time.sleep(0.1)
        raise TimeoutError("native client did not mount in time")
    mount_script = f"""
import sys; sys.path.insert(0, {ROOT!r})
from fs.fuse_lowlevel import FuseKernelMount, FuseLowLevelDaemon
from fs.posix_api import DistributedFileSystem
from roce.remote_engine import RemoteEngine
from roce.roce_transport import RoCETransport
dfs = DistributedFileSystem(mdt_transport=RoCETransport(), mdts={[a for _, a in mdts]!r},
                            osts=[RemoteEngine(a, RoCETransport()) for a in {[a for _, a in osts]!r}],
                            attr_ttl_s={attr_ttl_s!r})
mount = FuseKernelMount({mountpoint!r})
print("READY", flush=True)
FuseLowLevelDaemon(mount.fd, dfs).run(threads={fuse_threads!r})
"""
    mounter_log_path = os.path.join(CACHE, "pool_mounter.log")
    os.makedirs(CACHE, exist_ok=True)
    mounter_log = open(mounter_log_path, "w")
    mounter = topo.popen("A", ["-c", mount_script], env, stdin=subprocess.PIPE, stdout=mounter_log,
                         stderr=subprocess.STDOUT, text=True)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if subprocess.run(["mountpoint", "-q", mountpoint]).returncode == 0:
            return mounter, servers
        if mounter.poll() is not None:
            raise RuntimeError("dist mount exited: " + open(mounter_log_path).read())
        time.sleep(0.1)
    raise TimeoutError("dist filesystem did not mount in time")


def unmount_pool(mountpoint, mounter, backers) -> None:
    from tools.topology import Topology
    if mounter is not None:
        subprocess.run(["umount", mountpoint], capture_output=True)
        try:
            mounter.wait(timeout=10)
        except subprocess.TimeoutExpired:
            mounter.kill()
    for p in backers:
        try:
            p.stdin.close()
        except OSError:
            pass
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
    Topology.current().down()


def _mpirun(mpi_bin: str, n: int) -> list[str]:
    cmd = [os.path.join(mpi_bin, "mpirun"), "-n", str(n)]
    return ICP.wrap(cmd) if ICP else cmd


def _bench_env() -> dict | None:
    return ICP.env if ICP else None


def run_ior_case(mpi_bin, ior_bin, mountpoint, *, name, tasks, block, xfer, file_per_process, segments=1,
                 timeout=900) -> dict:
    path = os.path.join(mountpoint, f"iordata_{name}")
    if file_per_process:
        (ICP.makedirs if ICP else lambda p: os.makedirs(p, exist_ok=True))(path)
        target = os.path.join(path, "f")
    else:
        target = path
        if ICP:
            ICP.remove(target)
        elif os.path.exists(target):
            os.remove(target)
    cmd = _mpirun(mpi_bin, tasks) + [ior_bin, "-a", "POSIX", "-b", block, "-t", xfer, "-s", str(segments),
                                     "-w", "-r", "-W", "-R", "-G", "42", "-o", target]
    if file_per_process:
        cmd.insert(cmd.index(ior_bin) + 1, "-F")
    t0 = time.monotonic()
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=_bench_env())
    elapsed = time.monotonic() - t0
    out = proc.stdout
    result = {"name": name, "tasks": tasks, "block": block, "xfer": xfer, "segments": segments,
             "file_per_process": file_per_process, "elapsed_s": round(elapsed, 2),
             "returncode": proc.returncode, "ok": proc.returncode == 0 and "WARNING" not in out
             and "ERROR" not in out and "incorrect" not in out.lower()}
    # IOR prints two tables that both have lines starting with "write"/
    # "read": the per-test "Results:" table (bw(MiB/s), IOPS, ...) and the
    # later "Summary of all tests:" table (Max(MiB), Min(MiB), Mean(MiB),
    # ...) -- an entirely different set of columns that also happen to
    # start with the operation name. Scanning past "Summary of all tests"
    # used to let that second table's Max(MiB)/Min(MiB) silently overwrite
    # the real bw/IOPS parsed from the first (identical when there's only
    # one repetition, which is how this bug hid: write_iops == write_bw_mib_s).
    for line in out.splitlines():
        if line.strip().startswith("Summary of all tests"):
            break
        parts = line.split()
        if line.startswith("write") and len(parts) >= 3:
            result["write_bw_mib_s"] = float(parts[1])
            result["write_iops"] = float(parts[2])
        elif line.startswith("read") and len(parts) >= 3:
            result["read_bw_mib_s"] = float(parts[1])
            result["read_iops"] = float(parts[2])
    if not result["ok"]:
        result["stdout_tail"] = out.strip().splitlines()[-15:]
        result["stderr_tail"] = proc.stderr.strip().splitlines()[-15:]
    return result


def run_mdtest(mpi_bin, mdtest_bin, mountpoint, *, tasks=2, n=200, timeout=180, hard=False) -> dict:
    """hard=True: IO500 mdtest-hard's pattern -- every file 3901 bytes
    written and read back (-w/-e 3901), all ranks' files in ONE directory,
    each rank reading its neighbour's files (-N 1)."""
    path = os.path.join(mountpoint, "mdtest_hard" if hard else "mdtest_data")
    (ICP.makedirs if ICP else lambda p: os.makedirs(p, exist_ok=True))(path)
    cmd = _mpirun(mpi_bin, tasks) + [mdtest_bin, "-d", path, "-n", str(n), "-F"]
    if hard:
        cmd += ["-w", "3901", "-e", "3901", "-N", "1"]
    t0 = time.monotonic()
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=_bench_env())
    elapsed = time.monotonic() - t0
    rates = {}
    in_table = False
    for line in proc.stdout.splitlines():
        if line.strip().startswith("Operation"):
            in_table = True
            continue
        if in_table and line.strip().startswith("---"):
            continue
        if in_table:
            parts = line.split()
            if len(parts) >= 3 and not line.strip():
                break
            if len(parts) >= 3:
                op = " ".join(parts[:-4]) if len(parts) > 5 else parts[0]
                try:
                    rates[op] = float(parts[-4])
                except (ValueError, IndexError):
                    pass
    return {"tasks": tasks, "n": n, "elapsed_s": round(elapsed, 2), "hard": hard,
            "returncode": proc.returncode, "ops_per_sec": rates}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", choices=("local", "pool", "dist"), default="local",
                    help="local: single-node MemoryEngine, this container only. "
                         "pool: 3-node PoolNode over RoCETransport across the crossover cable. "
                         "dist: MDTs + striped OSTs (the redesigned layout) over the same cable.")
    ap.add_argument("--fuse-threads", type=int, default=4, help="dist only: FUSE daemon worker threads")
    ap.add_argument("--attr-ttl", type=float, default=3.0, help="dist only: client attribute-cache TTL, 0 = off")
    ap.add_argument("--osts", type=int, default=4, help="dist only: number of OST servers")
    ap.add_argument("--mdts", type=int, default=2, help="dist only: number of MDT servers")
    ap.add_argument("--python-servers", action="store_true",
                    help="dist only: Python MDT/OST servers instead of the native C ones")
    ap.add_argument("--client", choices=("native", "python", "intercept"), default="native",
                    help="dist only: native C FUSE client (roce/native/rocepool_fuse.c), the Python one, or the "
                         "LD_PRELOAD interception library in the IOR/mdtest ranks (no mount)")
    ap.add_argument("--native-threads", type=int, default=16, help="dist only: native client FUSE threads")
    ap.add_argument("--quick", action="store_true", help="smaller matrix")
    ap.add_argument("--scale", type=int, default=1,
                    help="multiply every case's MPI task count (and mdtest's) by this -- a scaling run")
    ap.add_argument("--mdtest-n", type=int, default=200, help="mdtest files per task")
    ap.add_argument("--hard", action="store_true",
                    help="IO500 'hard' pair only: ior-hard (47008-byte strided shared-file I/O) + mdtest-hard "
                         "(3901-byte files, one directory)")
    ap.add_argument("--hard-segments", type=int, default=200, help="ior-hard segments per task")
    ap.add_argument("--report", default=os.path.join(ROOT, "ior_bench_report.json"))
    args = ap.parse_args()
    if os.geteuid() != 0:
        print("needs root (real kernel FUSE mount)", file=sys.stderr)
        return 2

    print("== ensuring MPI ==", flush=True)
    mpi_bin = ensure_mpi()
    print(f"mpicc: {mpi_bin}", flush=True)
    print("== ensuring IOR/mdtest (real upstream github.com/hpc/ior) ==", flush=True)
    ior_bin, mdtest_bin = ensure_ior(mpi_bin)
    print(f"ior: {ior_bin}", flush=True)

    mountpoint = "/mnt/rocepool_iorbench"
    mounter = backers = None
    if args.backend == "local":
        proc = mount_local(mountpoint)
    elif args.backend == "dist":
        mounter, backers = mount_dist(mountpoint, fuse_threads=args.fuse_threads, attr_ttl_s=args.attr_ttl,
                                      n_osts=args.osts, n_mdts=args.mdts, native=not args.python_servers,
                                      client=args.client, native_threads=args.native_threads)
    else:
        mounter, backers = mount_pool(mountpoint)

    matrix = [{"name": "ior_hard", "tasks": 4, "block": "47008", "xfer": "47008", "segments": args.hard_segments,
               "file_per_process": False}] if args.hard else [
        {"name": "shared_1m", "tasks": 2, "block": "4m", "xfer": "1m", "file_per_process": False},
        {"name": "shared_64k", "tasks": 2, "block": "2m", "xfer": "64k", "file_per_process": False},
        {"name": "fpp_1m", "tasks": 4, "block": "2m", "xfer": "1m", "file_per_process": True},
    ]
    if not args.quick and not args.hard:
        matrix += [
            {"name": "shared_4m_large", "tasks": 2, "block": "32m", "xfer": "4m", "file_per_process": False},
            {"name": "fpp_256k", "tasks": 4, "block": "4m", "xfer": "256k", "file_per_process": True},
        ]

    for case in matrix:
        case["tasks"] *= max(1, args.scale)
    report = {"benchmark": "IOR/mdtest (real upstream github.com/hpc/ior, unmodified)", "scale": args.scale,
              "backend": args.backend, "mountpoint": mountpoint, "cases": []}
    if args.backend == "dist":
        report["dist_config"] = {"fuse_threads": args.fuse_threads, "attr_ttl_s": args.attr_ttl,
                                 "osts": args.osts, "mdts": args.mdts,
                                 "servers": "python" if args.python_servers else "native",
                                 "client": args.client}
    try:
        for case in matrix:
            print(f"-- IOR case: {case['name']} --", flush=True)
            report["cases"].append(run_ior_case(mpi_bin, ior_bin, mountpoint, **case))
        print("-- mdtest --", flush=True)
        report["mdtest"] = run_mdtest(mpi_bin, mdtest_bin, mountpoint, tasks=(4 if args.hard else 2) * max(1, args.scale),
                                      n=args.mdtest_n, timeout=1800, hard=args.hard)
    finally:
        if args.backend == "local":
            unmount_local(mountpoint, proc)
        else:
            unmount_pool(mountpoint, mounter, backers)

    report["ok"] = all(c["ok"] for c in report["cases"])
    if not report["ok"] and args.backend in ("pool", "dist"):
        mounter_log_path = os.path.join(CACHE, "pool_mounter.log")
        if os.path.exists(mounter_log_path):
            print("-- pool mount daemon log (tail, see failures above) --", flush=True)
            print("".join(open(mounter_log_path).readlines()[-40:]), flush=True)
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2)
    for c in report["cases"]:
        status = "PASS" if c["ok"] else "FAIL"
        print(f"[{status}] {c['name']:16s} write {c.get('write_bw_mib_s','-'):>8} MiB/s "
              f"({c.get('write_iops','-'):>8} IOPS)   "
              f"read {c.get('read_bw_mib_s','-'):>8} MiB/s ({c.get('read_iops','-'):>8} IOPS)")
    md = report.get("mdtest", {})
    md_status = "PASS" if md.get("returncode") == 0 else "FAIL"
    print(f"[{md_status}] mdtest           " +
          "  ".join(f"{op}={rate:,.0f}/s" for op, rate in md.get("ops_per_sec", {}).items()))
    print(f"report: {args.report}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
