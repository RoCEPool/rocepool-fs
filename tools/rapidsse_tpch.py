"""RapidsSE (the Rapids Storage Engine) on rocepool storage, and RapidsSE's own
TPC-H SF1 regression test run on it -- one command, unattended:

    sudo python3 tools/rapidsse_tpch.py --rapidsse /path/to/RapidsSE        # simulated 2-node cluster
    sudo ROCEPOOL_CLUSTER=cluster.json python3 tools/rapidsse_tpch.py ...   # real hosts

What it does:
  1. builds RapidsSE from its unmodified source if needed (offline: the
     tarballs it needs come from its Git LFS; ANTLR without its test suite,
     whose googletest download is the only thing that needs the internet)
     and TPC-H dbgen from the same tree
  2. starts rocepool on every node (one MDT + one OST per node, the native C
     servers) and mounts the native FUSE client at the same path everywhere
  3. gives every RapidsSE node its "disks" on rocepool: the plan's
     /mnt/disk1, /mnt/disk2 (data files) and the cluster config's
     disk_paths (seData, RapidsSE's unload/reload directory) become
     <mount>/rapidsse/<NODE>/disk1, disk2, bind-mounted at one path on each
     node, so every node reads its own share of the data from the shared pool
     exactly as it would from its local disks
  4. generates the TPC-H data with dbgen, chunked across nodes and paths the
     way RapidsSE's bootstrap/generateData.sh does (nodes x paths chunks,
     node-major, nation/region in each node's first path), written straight
     into rocepool
  5. starts one RapidsSE process per node with the command line of its
     bootstrap (boot.fn startCurrentNode) and waits, like waitForClusterBoot,
     until every node reports both sockets to every other node
  6. runs the test plan (default: bamboo/perftest/configs/tests/
     tpch-sf1-2D2F.jmx, the SF1 test of RapidsSE's daily regression job) step
     by step as JMeter would (tools/rapidsse_jmx.py), over RapidsSE's own JDBC
     driver (shell/sejdbc-*.jar) -- or over rocepool's WLP client (wlp/) with
     --client wlp -- stopping at the first failed step or assertion
  7. --baseline local: the same run again with the "disks" on node-local
     storage instead, for the comparison
  8. writes rapidsse_tpch_report.json: every step's time and result, load
     throughput, query times (min / median of the plan's loops), each node's
     data on rocepool, and the comparison

Everything it starts is stopped again, also on failure.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import select as select_mod
import os
import shlex
import shutil
import socket
import statistics
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools.cluster import Cluster  # noqa: E402
from tools.rapidsse_jmx import parse  # noqa: E402

MNT = "/mnt/rocepool_rapidsse"
# where each node sees its own "disks" (a bind mount of its directory on rocepool,
# or of a local directory for --baseline local)
NODE_ROOT = "/run/rocepool_rapidsse/node"
DEFAULT_PLAN = "bamboo/perftest/configs/tests/tpch-sf1-2D2F.jmx"
# the plan's hard-coded data paths (cluster-2N32P64T480G.config: disk_paths
# /mnt/disk1/seData:/mnt/disk2/seData; the plans load /mnt/disk<d>/data<f>/<table>.tbl)
DISKS, FILES_PER_DISK = 2, 2
BIG_TABLES = ["customer", "lineitem", "orders", "supplier", "part", "partsupp"]
SMALL_TABLES = ["nation", "region"]
TABLE_CODE = {"customer": "c", "lineitem": "L", "orders": "O", "partsupp": "S", "part": "P", "supplier": "s",
              "nation": "n", "region": "r"}
CACHE = os.path.join(ROOT, ".bench_cache", "rapidsse")  # same path on every host, like the tree
INTERNAL_PORT, CLIENT_PORT, HEARTBEAT_PORT = 5541, 9123, 8888
# RapidsSE builds with -Werror (makefile.linux.in) and was written for gcc 11;
# gcc >= 12 warns about more (-Woverloaded-virtual in -Wall, maybe-uninitialized,
# ...). The warnings stay on and visible in the build log, only not fatal; the
# source is unchanged. (-Werror=div-by-zero, its own extra, is kept.)
MAKE_VARS = ["LINUX_WARNINGS=-Wfatal-errors -Wno-attributes", "LINUX_ERRORS=-Werror=div-by-zero"]


def log(msg: str) -> None:
    print(msg, flush=True)


def sh(argv, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(argv, check=True, **kw)


# --------------------------------------------------------------------------
# 1. build
def _lfs_pointer(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(40).startswith(b"version https://git-lfs")
    except OSError:
        return True


def ensure_rapidsse(src: str, jobs: int) -> str:
    """RapidsSE's binary (src/../bin/rapidsse), built from the unmodified tree."""
    exe = os.path.join(src, "bin", "rapidsse")
    if os.access(exe, os.X_OK):
        return exe
    need = [os.path.join(src, "thirdparty", "thrift", "thrift-0.18.1.tar.gz"),
            os.path.join(src, "thirdparty", "thrift", "boost_dependency_1_80_0.tar.gz")]
    if any(_lfs_pointer(p) for p in need):
        log("== RapidsSE: fetching its Git LFS files (thirdparty tarballs, JDBC driver)")
        subprocess.run(["git", "-C", src, "lfs", "pull", "--include", "thirdparty/**,shell/**"], check=False)
        missing = [p for p in need if _lfs_pointer(p)]
        if missing:
            raise RuntimeError("RapidsSE's build needs these Git LFS files, which this clone does not have: "
                               f"{missing}. The mirror (github.com/RoCEPool/RapidsSE) copies them from upstream; "
                               "or restore them from its release 'backup' (rapidsse-lfs.tar).")
    # src/generateVersion.sh needs `git describe --match v*`: a shallow clone has no tags
    if subprocess.run(["git", "-C", src, "describe", "--long", "--match=v*"], capture_output=True).returncode:
        log("== RapidsSE: fetching the version tags its build stamps in")
        subprocess.run(["git", "-C", src, "fetch", "--tags", "--depth=5000", "origin"], check=False)
    antlr = os.path.join(src, "thirdparty", "antlr")
    if not (os.path.exists(os.path.join(antlr, "lib", "libantlr4-runtime.a"))
            and os.path.exists(os.path.join(antlr, "include", "antlr4-runtime.h"))):
        # thirdparty/antlr/Makefile's own recipe, minus ANTLR's unit tests (their
        # googletest is downloaded at configure time); the library is the same.
        # Headers copied here, not with the recipe's cpio (often not installed)
        log("== RapidsSE: building ANTLR runtime (without its unit tests)")
        work = os.path.join(CACHE, "antlr_build")
        shutil.rmtree(work, ignore_errors=True)
        os.makedirs(work)
        sh(["unzip", "-q", "-o", os.path.join(antlr, "antlr4-cpp-runtime-4.13.1-source.zip"), "-d", work])
        sh(["cmake", "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_C_COMPILER=gcc", "-DCMAKE_CXX_COMPILER=g++",
            "-DANTLR_BUILD_CPP_TESTS=OFF", "CMakeLists.txt"], cwd=work, stdout=subprocess.DEVNULL)
        sh(["make", f"-j{jobs}"], cwd=work, stdout=subprocess.DEVNULL)
        os.makedirs(os.path.join(antlr, "lib"), exist_ok=True)
        lib = next(iter(glob.glob(os.path.join(work, "**", "libantlr4-runtime.a"), recursive=True)))
        shutil.copy(lib, os.path.join(antlr, "lib"))
        rt = os.path.join(work, "runtime", "src")
        for dirpath, _, files in os.walk(rt):
            for fn in files:
                if fn.endswith(".h"):
                    dst = os.path.join(antlr, "include", os.path.relpath(dirpath, rt))
                    os.makedirs(dst, exist_ok=True)
                    shutil.copy(os.path.join(dirpath, fn), dst)
    log(f"== RapidsSE: make release (-j{jobs}); the first build takes a while")
    blog = os.path.join(CACHE, "rapidsse_build.log")
    os.makedirs(CACHE, exist_ok=True)
    with open(blog, "w") as f:
        r = subprocess.run(["make", f"-j{jobs}", "release", *MAKE_VARS], cwd=os.path.join(src, "src"), stdout=f,
                           stderr=subprocess.STDOUT)
    if r.returncode or not os.access(exe, os.X_OK):
        raise RuntimeError(f"RapidsSE build failed: {blog}\n" + open(blog).read()[-3000:])
    return exe


def ensure_dbgen(src: str) -> str:
    """TPC-H dbgen from RapidsSE's tree (dbgen/), built in a copy."""
    out = os.path.join(CACHE, "dbgen")
    exe = os.path.join(out, "dbgen")
    if os.access(exe, os.X_OK):
        return exe
    shutil.rmtree(out, ignore_errors=True)
    shutil.copytree(os.path.join(src, "dbgen"), out)
    for f in ("dbgen", "qgen"):
        if os.path.exists(os.path.join(out, f)):
            os.remove(os.path.join(out, f))
    subprocess.run(["make", "clean"], cwd=out, capture_output=True)
    # the makefile's own settings (DATABASE=ORACLE MACHINE=LINUX WORKLOAD=TPCH);
    # -fcommon: gcc >= 10 rejects the old tentative definitions otherwise
    sh(["make", "dbgen", "CFLAGS=-g -DDBNAME=\\\"dss\\\" -DLINUX -DORACLE -DTPCH -DRNG_TEST "
        "-D_FILE_OFFSET_BITS=64 -fcommon -w"], cwd=out, stdout=subprocess.DEVNULL)
    return exe


def ensure_jdbc_runner(src: str) -> tuple[str, str] | None:
    jars = [j for j in glob.glob(os.path.join(src, "shell", "sejdbc-*.jar")) if not _lfs_pointer(j)]
    if not jars or not shutil.which("javac"):
        return None
    out = os.path.join(CACHE, "jdbc")
    os.makedirs(out, exist_ok=True)
    java = os.path.join(ROOT, "tools", "rapidsse_jdbc", "JdbcSteps.java")
    cls = os.path.join(out, "JdbcSteps.class")
    if not os.path.exists(cls) or os.path.getmtime(cls) < os.path.getmtime(java):
        sh(["javac", "-d", out, java], env={k: v for k, v in os.environ.items() if k != "JAVA_TOOL_OPTIONS"})
    return sorted(jars)[-1], out


# --------------------------------------------------------------------------
# 6. clients
class JdbcClient:
    def __init__(self, url: str, jar: str, classes: str, wrap=lambda a: a):
        # the JVM banner of JAVA_TOOL_OPTIONS etc. goes to stderr; stdout carries the protocol
        env = {k: v for k, v in os.environ.items() if k not in ("JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS")}
        self.p = subprocess.Popen(wrap(["java", "-cp", f"{jar}:{classes}", "JdbcSteps", url]), stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        line = self.p.stdout.readline().decode()
        if not line.startswith("CONNECTED"):
            self.p.kill()
            raise RuntimeError(f"JDBC connect to {url} failed: {line}{self.p.stderr.read().decode()[-2000:]}")
        self.describe = "RapidsSE JDBC driver (" + os.path.basename(jar) + "): " + line.split("\t", 1)[1].strip()

    def table(self, sql: str) -> list[list[str]]:
        """A select's whole result (rows of column strings)."""
        r = self.run(sql, True, whole=True)
        if not r["ok"]:
            raise RuntimeError(r["error"])
        return [row.split("\x1f") for row in r["first"].split("\x1e")] if r["rows"] else []

    def run(self, sql: str, select: bool, whole: bool = False, timeout: float | None = None) -> dict:
        b = sql.encode()
        t0 = time.monotonic()
        self.p.stdin.write(f"{'A' if whole else 'S' if select else 'U'} {len(b)}\n".encode() + b)
        self.p.stdin.flush()
        if timeout and not select_mod.select([self.p.stdout], [], [], timeout)[0]:
            self.p.kill()
            return {"ok": False, "ms": int((time.monotonic() - t0) * 1000), "rows": 0, "first": "",
                    "error": f"no answer in {timeout:.0f} s (step timeout); the client was closed", "timeout": True}
        line = self.p.stdout.readline().decode().rstrip("\n")
        if not line:
            raise RuntimeError("JDBC runner exited: " + self.p.stderr.read().decode()[-2000:])
        ok, ms, rows, first, err, digest = (line.split("\t") + [""] * 6)[:6]
        return {"ok": ok == "1", "ms": int(ms), "rows": int(rows), "first": first, "error": err, "digest": digest}

    def close(self) -> None:
        try:
            self.p.stdin.write(b"Q\n")
            self.p.stdin.close()
            self.p.wait(timeout=30)
        except Exception:  # noqa: BLE001
            self.p.kill()


class WlpStepClient:
    def __init__(self, host: str, port: int):
        from wlp.wlp_client import WlpClient
        self.c = WlpClient(host, port)
        self.describe = "rocepool wlp/ (WLP v5)"

    def run(self, sql: str, select: bool, timeout: float | None = None) -> dict:
        t0 = time.monotonic()
        try:
            if timeout and hasattr(self.c, "_sock"):
                self.c._sock.settimeout(timeout)
            self.c.execute(sql)
            rows = self.c.fetchall() if select else []
            h = hashlib.sha256()
            for row in rows:
                for k, v in enumerate(row):
                    h.update(str(v).encode() + (b"\x1e" if k == len(row) - 1 else b"\x1f"))
            return {"ok": True, "ms": int((time.monotonic() - t0) * 1000), "rows": len(rows),
                    "first": str(rows[0][0]) if rows and rows[0] else "", "error": "",
                    "digest": h.hexdigest() if select else ""}
        except Exception as exc:  # noqa: BLE001
            from wlp.wlp_client import WlpError
            # a server-reported error leaves the dialog complete; anything else
            # (timeout, decode error, lost connection) may leave it half-read:
            # the caller reconnects
            return {"ok": False, "ms": int((time.monotonic() - t0) * 1000), "rows": 0, "first": "",
                    "error": f"{type(exc).__name__}: {exc}", "broken": not isinstance(exc, WlpError)}

    def table(self, sql: str) -> list[list[str]]:
        self.c.execute(sql)
        return [[str(v) for v in row] for row in self.c.fetchall()]

    def close(self) -> None:
        self.c.close()


# --------------------------------------------------------------------------
class Run:
    """One RapidsSE cluster on one kind of storage, the plan run against it."""

    def __init__(self, args, cluster: Cluster, env: dict, storage: str, mdts: str = "", osts: str = ""):
        self.args, self.cluster, self.env, self.storage = args, cluster, env, storage
        self.n = len(cluster.nodes)
        self.work = os.path.join(args.workdir, storage)
        self.se: list[subprocess.Popen] = []
        self.binds: list[int] = []
        self._made: set[int] = set()

    def node_name(self, i: int) -> str:
        return f"NODE{i + 1}"

    def node_dir(self, i: int) -> str:
        """Where node i's disks really are: rocepool, or this node's local storage."""
        if self.storage == "rocepool":
            return os.path.join(MNT, "rapidsse", self.node_name(i))
        return os.path.join(self.args.local_dir, self.node_name(i))

    def run_on(self, i: int, argv: list[str], **kw):
        return self.cluster.run(i, argv, self.env, **kw)

    # 3. disks
    def bind_disks(self) -> None:
        for i in range(self.n):
            d = self.node_dir(i)
            mk = [os.path.join(d, f"disk{k}", sub) for k in range(1, DISKS + 1)
                  for sub in [f"data{f}" for f in range(1, FILES_PER_DISK + 1)] + ["seData"]]
            self.run_on(i, ["mkdir", "-p", *mk, NODE_ROOT], net=False, check=True)
            self.run_on(i, ["sh", "-c", f"mountpoint -q {NODE_ROOT} && umount -l {NODE_ROOT}; true"], net=False)
            self.run_on(i, ["mount", "--bind", d, NODE_ROOT], net=False, check=True)
            self.binds.append(i)
        log(f"== [{self.storage}] each node's /mnt/disk1, /mnt/disk2 -> {NODE_ROOT}/disk1, disk2 = "
            + ("<rocepool>/rapidsse/NODE<k>" if self.storage == "rocepool" else f"{self.args.local_dir}/NODE<k>"))

    def unbind(self) -> None:
        for i in self.binds:
            self.run_on(i, ["umount", "-l", NODE_ROOT], net=False, capture_output=True)
        self.binds = []

    # 4. data
    def generate(self, dbgen: str) -> dict:
        """bootstrap/generateData.sh's layout: total chunks = nodes x paths
        (one chunk per path at SF1), node i's paths get chunks offset+1..,
        nation/region whole in each node's first path."""
        paths = [f"{NODE_ROOT}/disk{d}/data{f}" for d in range(1, DISKS + 1) for f in range(1, FILES_PER_DISK + 1)]
        total = self.n * len(paths)
        dists = os.path.join(os.path.dirname(dbgen), "dists.dss")
        sf = self.args.sf
        t0 = time.monotonic()
        procs = []
        for i in range(self.n):
            # dbgen -C/-S writes <table>.tbl.<chunk> in its working directory,
            # and generateData.sh then renames it to <table>.tbl. rocepool's
            # metadata protocol has no rename, so dbgen writes into a local FIFO
            # of that name and the stream goes straight into <table>.tbl: one
            # write, no copy on local disk (byte-identical to dbgen's file).
            # Same method for both storages, so the comparison stays fair.
            fifo = f"/run/rocepool_rapidsse/dbgen_{self.node_name(i)}"
            script = [f"rm -rf {fifo}", f"mkdir -p {fifo}"]
            for j, p in enumerate(paths):
                chunk = i * len(paths) + j + 1
                for t in BIG_TABLES:
                    f = f"{fifo}/{t}.tbl.{chunk}"
                    script.append(f"mkfifo {f} && cd {fifo} && {{ cat {f} > {p}/{t}.tbl & c=$!; "
                                  f"{dbgen} -b {dists} -f -q -s {sf} -C {total} -S {chunk} -T {TABLE_CODE[t]}; "
                                  f"r=$?; wait $c && [ $r = 0 ]; }} && rm -f {f}")
            for t in SMALL_TABLES:
                script.append(f"cd {paths[0]} && {dbgen} -b {dists} -f -q -s {sf} -T {TABLE_CODE[t]}")
            script.append(f"rm -rf {fifo}")
            procs.append(self.cluster.popen(i, ["sh", "-ec", " ; ".join(script)], self.env,
                                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT))
        for i, p in enumerate(procs):
            out = p.communicate()[0].decode(errors="replace")
            if p.returncode:
                raise RuntimeError(f"dbgen on node {i} failed: {out[-2000:]}")
        secs = time.monotonic() - t0
        sizes = {}
        for i in range(self.n):
            r = self.run_on(i, ["sh", "-c", f"du -sb {NODE_ROOT}/disk*/data* | awk '{{s+=$1}} END {{print s}}'"],
                            net=False, capture_output=True, text=True)
            sizes[self.node_name(i)] = int((r.stdout or "0").strip() or 0)
        total_b = sum(sizes.values())
        log(f"== [{self.storage}] TPC-H SF{sf} generated: {total} chunks, {total_b / 2**30:.2f} GiB in {secs:.0f} s "
            f"({total_b / 2**20 / secs:.0f} MiB/s written)")
        return {"chunks": total, "bytes": total_b, "bytes_per_node": sizes, "seconds": round(secs, 1)}

    # 5. RapidsSE
    def rse_dir(self, i: int) -> str:
        """An install-shaped directory per node (bin/ with the binary, commands/,
        sen/, munge/, tmp/), as startSE.sh runs it: the binary resolves
        ../commands, ../sen and ../tmp from its working directory bin/. Made on
        the node itself (the tree is at the same path on every host)."""
        d = os.path.join(self.work, self.node_name(i))
        if i not in self._made:
            links = [("bin/rapidsse", self.args.exe), ("commands", os.path.join(self.args.rapidsse, "commands")),
                     ("sen", os.path.join(self.args.rapidsse, "sen")),
                     ("munge", os.path.join(self.args.rapidsse, "munge"))]
            cmd = f"mkdir -p {shlex.quote(d)}/bin {shlex.quote(d)}/tmp && " + " && ".join(
                f"ln -sfn {shlex.quote(t)} {shlex.quote(os.path.join(d, n))}" for n, t in links)
            self.run_on(i, ["sh", "-c", cmd], net=False, check=True)
            self._made.add(i)
        return d

    def _in_bin(self, i: int, argv: list[str], senv: dict | None = None) -> list[str]:
        """argv run from node i's bin/ directory, with RapidsSE's environment
        variables (boot.fn exports them before starting it)."""
        pre = " ".join(f"{k}={shlex.quote(v)}" for k, v in (senv or {}).items())
        return ["sh", "-c", f"cd {shlex.quote(os.path.join(self.rse_dir(i), 'bin'))} && exec env {pre} "
                            + " ".join(shlex.quote(a) for a in argv)]

    def se_argv(self, i: int, extra: list[str]) -> list[str]:
        """bootstrap/boot.fn startCurrentNode's command line."""
        node = self.cluster.nodes[i]
        d = self.rse_dir(i)
        argv = [os.path.join(d, "bin", "rapidsse"), "--partitionsPerNode", str(self.args.partitions),
                "--threads", str(self.args.threads), "--size", f"{self.args.mem_mib}MB",
                "--node", self.node_name(i), node.ip, str(INTERNAL_PORT), str(CLIENT_PORT),
                "--name", self.node_name(i), "--start", "--munged", "--script", "--metadata",
                "--commands", os.path.join(d, "commands"), "--cid", str(self.args.cid),
                "--heartbeat", self.args.heartbeat, str(HEARTBEAT_PORT)]
        for k in range(1, DISKS + 1):  # disk_paths /mnt/disk<k>/seData
            argv += ["--disk", f"{NODE_ROOT}/disk{k}/seData"]
        return argv + extra

    SE_ENV = {"SE_ENV_METACLUE": "1", "SE_ENV_INTKEY": "1"}

    def start_se(self) -> None:
        os.makedirs(self.work, exist_ok=True)  # the logs are written here (ssh brings remote output back)
        for i in range(self.n):
            argv = self._in_bin(i, self.se_argv(i, []), self.SE_ENV)  # makes the node's directory first
            logf = open(os.path.join(self.work, f"{self.node_name(i)}_rapidsse.log"), "w")
            self.se.append(self.cluster.popen(i, argv, self.env,
                                              stdin=subprocess.DEVNULL, stdout=logf, stderr=subprocess.STDOUT))
        self.wait_boot()

    def se_log(self, i: int) -> str:
        try:
            return open(os.path.join(self.work, f"{self.node_name(i)}_rapidsse.log"), errors="replace").read()
        except OSError:
            return ""

    def wait_boot(self) -> None:
        """boot.fn waitForClusterBoot's test -- every node has both its send and
        its receive socket to the cluster -- on the rows of RapidsSE's `status`
        command (nodeName, regionName, status, isSendSocket, isRecvSocket).
        boot.fn gets them by attaching a second rapidsse process to the node's
        shared memory (--command status); here that process's exit took the
        server down with it (orderly exit, then SIGSEGV in its cleanup), so the
        same command goes over the client port instead, as the test plans
        send `space;`."""
        deadline = time.monotonic() + self.args.boot_timeout
        names = [self.node_name(i) for i in range(self.n)]
        last = "(client port not open yet)"
        while time.monotonic() < deadline:
            for i, p in enumerate(self.se):
                if p.poll() is not None:
                    raise RuntimeError(f"RapidsSE on {self.node_name(i)} exited ({p.returncode}): "
                                       + self.se_log(i)[-3000:])
            if self._port_open():
                try:
                    c = self.client()
                    try:
                        rows = c.table("status;")
                    finally:
                        c.close()
                    last = repr(rows)
                    ok = {r[0] for r in rows if len(r) >= 5 and r[3].lower() == "true" and r[4].lower() == "true"}
                    if all(nm in ok for nm in names):
                        log(f"== [{self.storage}] RapidsSE cluster up: {self.n} nodes cross-connected, "
                            f"{self.args.partitions} partitions x {self.args.threads} threads x "
                            f"{self.args.mem_mib} MiB region each")
                        return
                except Exception as exc:  # noqa: BLE001
                    last = f"{type(exc).__name__}: {exc}"
            time.sleep(2)
        raise RuntimeError(f"RapidsSE cluster did not boot in {self.args.boot_timeout} s; last status: {last[-3000:]}")

    def client(self):
        """A client of the cluster, on node 0 (inside its namespace when simulated)."""
        ip = self.cluster.nodes[0].ip
        if self.args.client == "jdbc":
            jar, classes = self.args.jdbc
            c = self._ns_jdbc(f"jdbc:se://{ip}:{CLIENT_PORT}", jar, classes)
        else:
            c = WlpStepClient(ip, CLIENT_PORT) if not self.cluster.nodes[0].sim else self._ns_wlp(ip)
        self.client_describe = c.describe
        return c

    def _port_open(self) -> bool:
        cmd = [sys.executable, "-c", f"import socket; socket.create_connection(('{self.cluster.nodes[0].ip}', "
                                     f"{CLIENT_PORT}), 3).close()"]
        return self.run_on(0, cmd, capture_output=True).returncode == 0

    def stop_se(self) -> None:
        for i in range(len(self.se)):
            d = self.rse_dir(i)
            try:
                self.run_on(i, self._in_bin(i, [os.path.join(d, "bin", "rapidsse"), "--name", self.node_name(i),
                                                "--stop"], self.SE_ENV), capture_output=True, timeout=60)
            except Exception:  # noqa: BLE001
                pass
        for p in self.se:
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        # a killed node leaves its shared-memory region (/SE_<node>) behind
        for i in range(len(self.se)):
            self.run_on(i, ["rm", "-f", f"/dev/shm/SE_{self.node_name(i)}"], net=False)
        self.se = []

    # 6. the plan
    def run_plan(self) -> dict:
        args = self.args
        remap = {"/mnt/disk": f"{NODE_ROOT}/disk"}
        plan = parse(args.plan_path, overrides=dict(args.jvars), remap=remap)
        client = self.client()
        results, failed = [], None
        main = [s for s in plan.steps if s.group == "main"]
        teardown = [s for s in plan.steps if s.group == "teardown"]

        def step(st, timeout):
            nonlocal client
            if client is None:
                client = self.client()
            r = client.run(st.sql, st.select, timeout=timeout)
            if r.get("timeout") or r.get("broken"):
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass
                client = None
            r.update(name=st.name, group=st.group, select=st.select)
            if r["ok"] and st.assertions:
                r["assertions"] = [{"variable": a.variable, "expected": a.expected, "actual": r["first"],
                                    "ok": a.check(r["first"])} for a in st.assertions]
                if not all(a["ok"] for a in r["assertions"]):
                    r["ok"] = False
                    r["error"] = "assertion failed: " + ", ".join(
                        f"{a['variable']} = {a['actual']!r}, expected {a['expected']!r}" for a in r["assertions"]
                        if not a["ok"])
            results.append(r)
            mark = "ok " if r["ok"] else "FAIL"
            log(f"   [{self.storage}] {mark} {st.name:<28} {r['ms']:>8} ms"
                + (f"  rows={r['rows']}" if st.select else "") + (f"  {r['error'][:300]}" if not r["ok"] else ""))
            return r

        try:
            for st in main:
                r = step(st, args.step_timeout)
                if not r["ok"]:
                    # on_sample_error=stoptest: the rest of the main group is skipped
                    failed = r
                    break
            # the tearDown group runs regardless (failures there are reported, not fatal)
            for st in teardown:
                try:
                    step(st, min(args.step_timeout, 300))
                except Exception as exc:  # noqa: BLE001
                    results.append({"name": st.name, "group": "teardown", "ok": False, "ms": 0, "rows": 0,
                                    "first": "", "error": f"{type(exc).__name__}: {exc}"})
        finally:
            if client is not None:
                client.close()
        return {"plan": os.path.relpath(args.plan_path, args.rapidsse), "client": self.client_describe,
                "variables": plan.variables, "remap": remap, "steps": results,
                "not_executed": plan.skipped, "disabled_in_plan": plan.disabled,
                "passed": failed is None, "first_failure": failed}

    def _ns_jdbc(self, url, jar, classes):
        """The JDBC client on node 0 (in its network namespace when simulated)."""
        node = self.cluster.nodes[0]
        return JdbcClient(url, jar, classes, wrap=(lambda a: node.wrap(a)) if node.sim else (lambda a: a))

    def _ns_wlp(self, ip):
        # a WLP client inside node 0's network namespace: setns in this process
        # would move the whole runner, so a socket opened in a forked helper
        # is passed back over a unix socket pair
        a, b = socket.socketpair()
        pid = os.fork()
        if pid == 0:
            try:
                from tools.netlink_veth import _setns
                from tools.netns_cluster import NETNS_DIR, node_name
                _setns(os.path.join(NETNS_DIR, node_name(0)))
                s = socket.create_connection((ip, CLIENT_PORT), 10)
                socket.send_fds(b, [b"x"], [s.fileno()])
            finally:
                os._exit(0)
        _, fds, _, _ = socket.recv_fds(a, 1, 1)
        os.waitpid(pid, 0)
        s = socket.socket(fileno=fds[0])
        from wlp.wlp_client import WlpClient
        c = WlpStepClient.__new__(WlpStepClient)
        s.settimeout(None)
        c.c = WlpClient.from_stream(s)
        c.describe = "rocepool wlp/ (WLP v5)"
        return c


def summarize(res: dict, gen: dict) -> dict:
    steps = res["steps"]
    by = {}
    for s in steps:
        if s["ok"]:
            by.setdefault(s["name"], []).append(s["ms"])
    loads = {n: v[0] for n, v in by.items() if n.startswith("load")}
    load_ms = sum(loads.values())
    queries = {n: {"runs_ms": v, "min_ms": min(v), "median_ms": statistics.median(v)}
               for n, v in by.items() if n[:1] == "q" and n[1:].isdigit()}
    return {"load_ms": load_ms, "load_mib_s": round(gen["bytes"] / 2**20 / (load_ms / 1000), 1) if load_ms else None,
            "load_ms_by_table": loads, "queries": queries,
            "queries_total_median_ms": sum(q["median_ms"] for q in queries.values()),
            "row_counts": {s["name"]: s.get("assertions") for s in steps if s.get("assertions")}}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rapidsse", required=True, help="RapidsSE source tree (a clone of github.com/RoCEPool/RapidsSE, "
                                                      "branch master)")
    ap.add_argument("--plan", default=DEFAULT_PLAN, help=f"test plan, relative to --rapidsse (default {DEFAULT_PLAN})")
    ap.add_argument("--nodes", type=int, default=2, help="simulated cluster size (the plan's cluster: 2 nodes; "
                                                         "ignored with ROCEPOOL_CLUSTER)")
    ap.add_argument("--sf", default="1", help="TPC-H scale factor (must match the plan's row-count assertions)")
    ap.add_argument("--partitions", type=int, default=None, help="RapidsSE partitions per node (default: cores)")
    ap.add_argument("--threads", type=int, default=None, help="RapidsSE threads per node (default: 2 x partitions)")
    ap.add_argument("--mem-mib", type=int, default=None, help="RapidsSE region per node, MiB (default: from free RAM)")
    ap.add_argument("--ost-tier", default="auto",
                    help="where the OSTs keep the data: ram, a ROCEPOOL_OST_TIER spec (file:/path/ost{node}.img:GiB, "
                         "'{node}' = node index), or auto: a file tier under /var/tmp when simulated (every node "
                         "shares this host's RAM with RapidsSE), RAM on real hosts")
    ap.add_argument("--step-timeout", type=int, default=1800,
                    help="seconds a step may take before it counts as failed (a hung query fails the run "
                         "instead of hanging it)")
    ap.add_argument("--heartbeat", default="BROADCAST", help="RapidsSE heartbeat address (BROADCAST or multicast ip)")
    ap.add_argument("--cid", type=int, default=2223687)
    ap.add_argument("--client", choices=["auto", "jdbc", "wlp"], default="auto",
                    help="auto: RapidsSE's JDBC driver if its jar and javac are here, else rocepool's WLP client")
    ap.add_argument("--baseline", choices=["none", "local"], default="local",
                    help="also run with the disks on node-local storage, for the comparison")
    ap.add_argument("--local-dir", default="/var/tmp/rocepool_rapidsse_local",
                    help="node-local directory for --baseline local")
    ap.add_argument("-J", dest="jvars", action="append", default=[], type=lambda s: tuple(s.split("=", 1)),
                    help="plan property override NAME=VALUE (like jmeter -J), e.g. -J QUERY_LOOP_COUNT=1")
    ap.add_argument("--boot-timeout", type=int, default=300)
    ap.add_argument("--workdir", default=os.path.join(ROOT, "rapidsse_tpch_work"))
    ap.add_argument("--report", default=os.path.join(ROOT, "rapidsse_tpch_report.json"))
    ap.add_argument("--jobs", type=int, default=os.cpu_count() or 2, help="build parallelism")
    args = ap.parse_args()
    if os.geteuid() != 0:
        print("needs root (FUSE mounts, namespaces)", file=sys.stderr)
        return 2
    args.rapidsse = os.path.abspath(args.rapidsse)
    args.plan_path = os.path.join(args.rapidsse, args.plan)
    args.exe = ensure_rapidsse(args.rapidsse, args.jobs)
    dbgen = ensure_dbgen(args.rapidsse)
    jdbc = ensure_jdbc_runner(args.rapidsse) if args.client in ("auto", "jdbc") else None
    if args.client == "jdbc" and not jdbc:
        print("--client jdbc: no sejdbc jar (Git LFS) or no javac", file=sys.stderr)
        return 2
    args.client, args.jdbc = ("jdbc" if jdbc else "wlp"), jdbc

    cluster = Cluster.current(args.nodes)
    n = len(cluster.nodes)
    cores = os.cpu_count() or 2
    # simulated: every node shares this host's cores and RAM
    share = n if not cluster.real else 1
    args.partitions = args.partitions or max(1, cores // share)
    args.threads = args.threads or 2 * args.partitions
    if args.ost_tier == "auto":
        args.ost_tier = "ram" if cluster.real else f"file:/var/tmp/rocepool_rapidsse_tier/ost{{node}}.img:{max(4, 3 * int(float(args.sf)))}"
    if args.mem_mib is None:
        avail = int(open("/proc/meminfo").read().split("MemAvailable:")[1].split()[0]) / 1024
        # what is left after the data held in rocepool's RAM (SF1 ~1.1 GiB; with
        # a file tier only its cache) and 1.5 GiB headroom, split over the nodes
        data = 1.2 * 1024 * float(args.sf) if args.ost_tier == "ram" else 256
        args.mem_mib = max(1024, int((avail - data - 1536) / share) // 64 * 64)
    report = {"what": "RapidsSE on rocepool: RapidsSE's own TPC-H regression test", "nodes": n,
              "cluster": cluster.describe if hasattr(cluster, "describe") else "", "sf": args.sf,
              "rapidsse": {"tree": args.rapidsse, "commit": subprocess.run(
                  ["git", "-C", args.rapidsse, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
                  "version": subprocess.run(["git", "-C", args.rapidsse, "describe", "--long", "--match=v*"],
                                            capture_output=True, text=True).stdout.strip(),
                  "partitions_per_node": args.partitions, "threads_per_node": args.threads,
                  "region_mib_per_node": args.mem_mib},
              "runs": {}}
    os.makedirs(args.workdir, exist_ok=True)
    cluster.up()
    env = cluster.env()
    if not cluster.real:
        env.update({"ROCEPOOL_RPC_TIMEOUT_MS": "60000"})
    env.setdefault("ROCE_TRANSPORT_LIB", cluster.lib)
    env["PYTHONPATH"] = ROOT
    procs, fuse, tier_files = [], [], []
    try:
        # 2. rocepool: one MDT + one OST per node, FUSE client on every node
        helper = os.path.join(ROOT, "tools", "crossover_node.py")
        servers = []
        for node in cluster.nodes:
            servers.append({"kind": "mdt", "node": node.index, "addr": f"{node.ip}:19901"})
            servers.append({"kind": "ost", "node": node.index, "addr": f"{node.ip}:19951"})
        tier_files = []
        for s in servers:
            s["env"] = dict(env)
            if s["kind"] == "ost" and args.ost_tier != "ram":
                spec = args.ost_tier.replace("{node}", str(s["node"]))
                s["env"].update(ROCEPOOL_OST_TIER=spec, ROCEPOOL_OST_RAM_CACHE_MIB="128")
                path = spec[5:].rsplit(":", 1)[0] if spec.startswith("file:") else ""
                if path and not path.startswith("/dev/"):
                    cluster.run(s["node"], ["sh", "-c", f"mkdir -p {shlex.quote(os.path.dirname(path))} && "
                                                        f"rm -f {shlex.quote(path)}"], env, net=False)
                    tier_files.append((s["node"], path))
        for s in servers:
            s["log"] = os.path.join(args.workdir, f"{s['kind']}{s['node']}.log")
            procs.append(cluster.popen(s["node"], [sys.executable, "-u", helper, f"{s['kind']}-native", s["addr"]],
                                       s["env"],
                                       stdin=subprocess.PIPE, stdout=open(s["log"], "w"), stderr=subprocess.STDOUT))
        deadline = time.monotonic() + 120
        for s, p in zip(servers, procs):
            while "READY" not in open(s["log"]).read():
                if p.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError(f"{s['kind']} on node {s['node']} did not start: {open(s['log']).read()[-2000:]}")
                time.sleep(0.2)
        mdts = ",".join(s["addr"] for s in servers if s["kind"] == "mdt")
        osts = ",".join(s["addr"] for s in servers if s["kind"] == "ost")
        for node in cluster.nodes:
            cluster.run(node.index, ["mkdir", "-p", MNT], env, net=False, check=True)
            fuse.append(cluster.popen(node.index, [cluster.fuse_client, "--mount", MNT, "--mdts", mdts, "--osts", osts,
                                                   "--attr-ttl", "3"], env, stdin=subprocess.PIPE,
                                      stdout=open(os.path.join(args.workdir, f"client{node.index}.log"), "w"),
                                      stderr=subprocess.STDOUT))
        deadline = time.monotonic() + 60
        for node in cluster.nodes:
            while cluster.run(node.index, ["mountpoint", "-q", MNT], env, net=False).returncode:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"rocepool client did not mount on node {node.index}")
                time.sleep(0.3)
        log(f"== rocepool up: {n} MDT, {n} OST, native client mounted at {MNT} on all {n} nodes")
        report["rocepool"] = {"mdts": mdts, "osts": osts, "mount": MNT, "client": "native FUSE (rocepool-fuse)",
                              "ost_data": args.ost_tier if args.ost_tier == "ram" else
                              f"tier {args.ost_tier} (128 MiB RAM cache per OST)"}

        storages = ["rocepool"] + (["local"] if args.baseline == "local" else [])
        for storage in storages:
            run = Run(args, cluster, env, storage)
            entry = report["runs"].setdefault(storage, {})
            try:
                run.bind_disks()
                entry["data"] = run.generate(dbgen)
                run.start_se()
                entry["plan"] = run.run_plan()
                entry["summary"] = summarize(entry["plan"], entry["data"])
            except Exception as exc:  # noqa: BLE001
                entry["error"] = f"{type(exc).__name__}: {exc}"
                log(f"== [{storage}] ERROR: {entry['error'][:3000]}")
            finally:
                run.stop_se()
                entry["rapidsse_log_tail"] = {run.node_name(i): run.se_log(i)[-1500:] for i in range(n)}
                run.unbind()
                if storage == "local":
                    shutil.rmtree(args.local_dir, ignore_errors=True)
        r, l = report["runs"].get("rocepool", {}), report["runs"].get("local", {})
        if r.get("summary") and l.get("summary"):
            rs, ls = r["summary"], l["summary"]
            report["comparison"] = {
                "load_ms": {"rocepool": rs["load_ms"], "local": ls["load_ms"]},
                "queries_total_median_ms": {"rocepool": rs["queries_total_median_ms"],
                                            "local": ls["queries_total_median_ms"]},
                "per_query_median_ms": {q: {"rocepool": rs["queries"][q]["median_ms"],
                                            "local": ls["queries"].get(q, {}).get("median_ms")} for q in rs["queries"]}}
            # the same answers? (every select's whole result, by digest)
            def digests(run):
                return {(s["name"], k): s.get("digest") for k, s in enumerate(
                    x for x in run["plan"]["steps"] if x.get("select") and x["ok"])}
            dr, dl = digests(r), digests(l)
            same = [k for k in dr if dr[k] and dr[k] == dl.get(k)]
            differ = sorted({k[0] for k in dr if dr[k] and dl.get(k) and dr[k] != dl[k]})
            report["comparison"]["answers"] = {"compared": len([k for k in dr if dr[k] and dl.get(k)]),
                                               "identical": len(same), "differ": differ}
        report["passed"] = bool(r.get("plan", {}).get("passed"))
    finally:
        for node in cluster.nodes:
            cluster.run(node.index, ["umount", "-l", MNT], env, net=False, capture_output=True)
        for p in fuse + procs:
            try:
                if p.stdin:
                    p.stdin.close()
                p.wait(timeout=15)
            except Exception:  # noqa: BLE001
                p.kill()
        for node, path in tier_files:
            cluster.run(node, ["rm", "-f", path], env, net=False)
        cluster.down()
        with open(args.report, "w") as f:
            json.dump(report, f, indent=1)
        log(f"== report: {args.report}")
    rr = report["runs"].get("rocepool", {})
    if rr.get("summary"):
        s = rr["summary"]
        log(f"== RapidsSE on rocepool: plan {'PASSED' if report['passed'] else 'FAILED'}; load {s['load_ms'] / 1000:.1f} s "
            f"({s['load_mib_s']} MiB/s), queries (median of loops) {s['queries_total_median_ms'] / 1000:.1f} s")
    return 0 if report.get("passed") else 1


if __name__ == "__main__":
    sys.exit(main())
