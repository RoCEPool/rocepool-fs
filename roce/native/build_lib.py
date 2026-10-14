"""One place that knows how to build the native libraries and binaries for
tests and harnesses (the deployment build is roce/native/build.sh).

Two transport backends, chosen by ROCEPOOL_BACKEND:
  softverbs (default)  roce_transport.c (RC queue pairs over rdma_cm) linked
                       against softverbs, the userspace RC emulator
  ofi                  ofi_transport.c on libfabric reliable-datagram
                       endpoints -- the AWS EFA backend -- tested here with
                       libfabric's "tcp" provider (ROCE_OFI_PROVIDER)

Every ensure_*() keeps its name and returns the chosen backend's artifact,
so every harness and test runs on either backend unchanged. Each rebuilds
whenever a source or header is newer than its output.
"""
from __future__ import annotations

import glob
import os

import subprocess
import tempfile

NATIVE = os.path.dirname(os.path.abspath(__file__))
LIBFABRIC_TAG = "v2.1.0"


def backend() -> str:
    b = os.environ.get("ROCEPOOL_BACKEND", "softverbs")
    if b not in ("softverbs", "ofi"):
        raise ValueError(f"ROCEPOOL_BACKEND={b}: softverbs or ofi")
    return b


def _outdir() -> str:
    d = os.path.join(NATIVE, "softverbs" if backend() == "softverbs" else "ofi")
    os.makedirs(d, exist_ok=True)
    return d


# ---- libfabric (the ofi backend) -------------------------------------------
def find_libfabric() -> str | None:
    """A libfabric install prefix: ROCE_LIBFABRIC, the AWS EFA installer's
    /opt/amazon/efa, a build in ~/.cache/rocepool, or the system's."""
    cands = [os.environ.get("ROCE_LIBFABRIC", ""), "/opt/amazon/efa"]
    cands += sorted(glob.glob(os.path.expanduser("~/.cache/rocepool/libfabric-*")), reverse=True)
    cands += ["/usr/local", "/usr"]
    for p in cands:
        if not p:
            continue
        inc = os.path.join(p, "include", "rdma", "fabric.h")
        libs = glob.glob(os.path.join(p, "lib*", "libfabric.so*")) + \
            glob.glob(os.path.join(p, "lib", "x86_64-linux-gnu", "libfabric.so*"))
        if os.path.exists(inc) and libs:
            return p
    return None


def ensure_libfabric() -> str:
    """find_libfabric(), else build libfabric LIBFABRIC_TAG from source into
    ~/.cache/rocepool (tcp, rxm, shm providers; git + autotools)."""
    p = find_libfabric()
    if p:
        return p
    prefix = os.path.expanduser(f"~/.cache/rocepool/libfabric-{LIBFABRIC_TAG}")
    src = prefix + "-src"
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(NATIVE)))
    from tools import deps
    if not os.path.isdir(src) and not deps.extract("libfabric", src):
        deps.check_download("libfabric")
        subprocess.run(["git", "clone", "-q", "--depth", "1", "--branch", LIBFABRIC_TAG,
                        "https://github.com/ofiwg/libfabric.git", src], check=True)
    run = lambda *a: subprocess.run(list(a), cwd=src, check=True, stdout=subprocess.DEVNULL)  # noqa: E731
    if not os.path.exists(os.path.join(src, "configure")):  # the bundled copy comes with it
        run("./autogen.sh")
    run("./configure", f"--prefix={prefix}", "--disable-static", "--enable-efa=no", "--enable-verbs=no",
        "--enable-psm2=no", "--enable-psm3=no", "--enable-opx=no", "--enable-usnic=no", "--enable-ucx=no",
        "--enable-lpp=no")
    run("make", f"-j{os.cpu_count() or 2}")
    run("make", "install")
    return prefix


def _libdir(prefix: str) -> str:
    for d in ("lib64", "lib", os.path.join("lib", "x86_64-linux-gnu")):
        if glob.glob(os.path.join(prefix, d, "libfabric.so*")):
            return os.path.join(prefix, d)
    return os.path.join(prefix, "lib")


def _transport() -> tuple[list[str], list[str], list[str]]:
    """(transport sources, compile flags, link flags) of the backend"""
    if backend() == "softverbs":
        return ([os.path.join(NATIVE, "roce_transport.c"), os.path.join(NATIVE, "softverbs", "softverbs.c")],
                ["-I", os.path.join(NATIVE, "test_stubs")], [])
    prefix = ensure_libfabric()
    lib = _libdir(prefix)
    return ([os.path.join(NATIVE, "ofi_transport.c")], ["-I", os.path.join(prefix, "include")],
            ["-L", lib, f"-Wl,-rpath,{lib}", "-lfabric"])


HEADERS = [os.path.join(NATIVE, "roce_transport.h"), os.path.join(NATIVE, "rocepool_servers.h"),
           os.path.join(NATIVE, "ost_tier.h"),
           os.path.join(NATIVE, "rocepool_client.h"), os.path.join(NATIVE, "rocepool_fs.h"),
           os.path.join(NATIVE, "test_stubs", "rdma", "rdma_cma.h"),
           os.path.join(NATIVE, "test_stubs", "infiniband", "verbs.h")]
ALL_TRANSPORTS = [os.path.join(NATIVE, "roce_transport.c"), os.path.join(NATIVE, "ofi_transport.c"),
                  os.path.join(NATIVE, "softverbs", "softverbs.c")]
SERVER_SOURCES = [os.path.join(NATIVE, "rocepool_servers.c"), os.path.join(NATIVE, "ost_tier.c")]
CC = ["gcc", "-O2", "-g", "-Wall", "-Wextra", "-Werror", "-pthread"]


def _stamp() -> str:
    """what an artifact was built with: a binary built elsewhere (a newer
    glibc, another compiler) must be rebuilt here, whatever its mtime"""
    import platform
    try:
        cc = subprocess.run(["gcc", "-dumpfullversion"], capture_output=True, text=True).stdout.strip()
    except OSError:
        cc = "?"
    return f"{platform.machine()} {'-'.join(platform.libc_ver())} gcc-{cc} {backend()}"


def _mark(out: str) -> None:
    with open(out + ".buildinfo", "w") as f:
        f.write(_stamp() + "\n")


def _stale(out: str, sources: list[str]) -> bool:
    newest = max(os.path.getmtime(p) for p in sources + HEADERS + [os.path.abspath(__file__)] if os.path.exists(p))
    if not os.path.exists(out) or os.path.getmtime(out) < newest:
        return True
    try:
        with open(out + ".buildinfo") as f:
            return f.read().strip() != _stamp()
    except OSError:
        return True


# ---- the transport library (+ native servers), loaded by Python ----------------
def _lib_path() -> str:
    name = "libroce_transport_softverbs.so" if backend() == "softverbs" else "libroce_transport_ofi.so"
    return os.path.join(_outdir(), name)


SOFT_LIB = os.path.join(NATIVE, "softverbs", "libroce_transport_softverbs.so")  # softverbs backend's path


def build_softverbs(out: str | None = None, *defines: str) -> str:
    """The transport + native servers shared library of the current backend
    (the name predates the ofi backend)."""
    out = out or _lib_path()
    srcs, cflags, ldflags = _transport()
    subprocess.run([*CC, "-fPIC", "-shared", *defines, *cflags, *srcs, *SERVER_SOURCES, "-o", out, *ldflags],
                   check=True)
    return out


def ensure_softverbs_lib() -> str:
    out = _lib_path()
    if _stale(out, ALL_TRANSPORTS + SERVER_SOURCES):
        build_softverbs(out)
        _mark(out)
    return out


# ---- the native FUSE client -------------------------------------------------
CLIENT_SOURCES = [os.path.join(NATIVE, "rocepool_fs.c"), os.path.join(NATIVE, "rocepool_client.c")]
FUSE_MAIN = os.path.join(NATIVE, "rocepool_fuse.c")
FUSE_SOURCES = [FUSE_MAIN] + CLIENT_SOURCES + ALL_TRANSPORTS


def _fuse_path() -> str:
    return os.path.join(_outdir(), "rocepool-fuse")


FUSE_CLIENT = os.path.join(NATIVE, "softverbs", "rocepool-fuse")


def build_fuse_client(out: str | None = None) -> str:
    out = out or _fuse_path()
    srcs, cflags, ldflags = _transport()
    subprocess.run([*CC, *cflags, FUSE_MAIN, *CLIENT_SOURCES, *srcs, "-o", out, *ldflags], check=True)
    return out


def ensure_fuse_client() -> str:
    out = _fuse_path()
    if _stale(out, FUSE_SOURCES):
        build_fuse_client(out)
        _mark(out)
    return out


# ---- the native KV client library + its benchmark ----------------------------
KV_MAIN = os.path.join(NATIVE, "rocepool_kv.c")


def ensure_kv_lib() -> tuple[str, str]:
    lib, bench = os.path.join(_outdir(), "librocepool_kv.so"), os.path.join(_outdir(), "rocepool-kv-bench")
    srcs, cflags, ldflags = _transport()
    all_src = [KV_MAIN, os.path.join(NATIVE, "rocepool_client.c")] + ALL_TRANSPORTS + [os.path.join(NATIVE, "rocepool_kv.h")]
    for out, extra in ((lib, ["-fPIC", "-shared"]), (bench, ["-DRPKV_BENCH"])):
        if _stale(out, all_src):
            subprocess.run([*CC, *extra, *cflags, KV_MAIN, os.path.join(NATIVE, "rocepool_client.c"), *srcs,
                            "-o", out, *ldflags], check=True)
            _mark(out)
    return lib, bench


KV_LIB = os.path.join(NATIVE, "softverbs", "librocepool_kv.so")
KV_BENCH = os.path.join(NATIVE, "softverbs", "rocepool-kv-bench")

# ---- the LD_PRELOAD interception library -------------------------------------
INTERCEPT_MAIN = os.path.join(NATIVE, "rocepool_intercept.c")
INTERCEPT_LIB = os.path.join(NATIVE, "softverbs", "librocepool_intercept.so")


def build_intercept_lib(out: str | None = None, include: str | None = None,
                        extra_ld: list[str] | None = None, cc: str = "gcc") -> str:
    """The client core is compiled with hidden visibility so that only the
    libc entry points (open, read, ...) leave the .so -- nothing of ours can
    shadow a symbol in the host program."""
    out = out or os.path.join(_outdir(), "librocepool_intercept.so")
    srcs, cflags, ldflags = _transport()
    if include:
        cflags = ["-I", include]
    base = [cc, "-O2", "-g", "-Wall", "-Wextra", "-Werror", "-pthread", "-fPIC", "-std=gnu11", *cflags, "-I", NATIVE]
    objs = []
    with tempfile.TemporaryDirectory() as td:
        for src in CLIENT_SOURCES + srcs:
            o = os.path.join(td, os.path.basename(src) + ".o")
            subprocess.run([*base, "-fvisibility=hidden", "-c", src, "-o", o], check=True)
            objs.append(o)
        o = os.path.join(td, "intercept.o")
        subprocess.run([*base, "-c", INTERCEPT_MAIN, "-o", o], check=True)
        objs.append(o)
        subprocess.run([cc, "-shared", "-pthread", *objs, "-o", out, *(extra_ld or []), *ldflags, "-ldl", "-lpthread"],
                       check=True)
    return out


def ensure_intercept_lib() -> str:
    out = os.path.join(_outdir(), "librocepool_intercept.so")
    if _stale(out, [INTERCEPT_MAIN] + CLIENT_SOURCES + ALL_TRANSPORTS):
        build_intercept_lib(out)
        _mark(out)
    return out


# ---- GPUDirect Storage: the cuFile-compatible library -------------------------
CUFILE_MAIN = os.path.join(NATIVE, "rocepool_cufile.c")


def build_cufile_lib(out: str, cc: str = "gcc") -> str:
    """Like the interception library: the client core hidden, only the cuFile
    API (and rocepool's rp_cufile_* / rp_devemu_* extras) exported. A copy
    named libcufile.so.0 goes in cufile/ next to it, for LD_LIBRARY_PATH."""
    srcs, cflags, ldflags = _transport()
    base = [cc, "-O2", "-g", "-Wall", "-Wextra", "-Werror", "-pthread", "-fPIC", "-std=gnu11", *cflags, "-I", NATIVE]
    objs = []
    with tempfile.TemporaryDirectory() as td:
        for src in CLIENT_SOURCES + srcs + [CUFILE_MAIN]:
            o = os.path.join(td, os.path.basename(src) + ".o")
            subprocess.run([*base, "-fvisibility=hidden", "-c", src, "-o", o], check=True)
            objs.append(o)
        subprocess.run([cc, "-shared", "-pthread", "-Wl,-soname,libcufile.so.0", *objs, "-o", out, *ldflags,
                        "-ldl", "-lpthread"], check=True)
    d = os.path.join(os.path.dirname(out), "cufile")
    os.makedirs(d, exist_ok=True)
    import shutil
    shutil.copy2(out, os.path.join(d, "libcufile.so.0"))
    link = os.path.join(d, "libcufile.so")  # for -lcufile at link time
    if not os.path.islink(link):
        os.symlink("libcufile.so.0", link)
    return out


def ensure_cufile_lib() -> str:
    out = os.path.join(_outdir(), "librocepool_cufile.so")
    if _stale(out, [CUFILE_MAIN] + CLIENT_SOURCES + ALL_TRANSPORTS) or \
            not os.path.exists(os.path.join(_outdir(), "cufile", "libcufile.so.0")):
        build_cufile_lib(out)
        _mark(out)
    return out


if __name__ == "__main__":  # python3 roce/native/build_lib.py: build everything of the current backend
    for f in (ensure_softverbs_lib, ensure_fuse_client, ensure_intercept_lib, ensure_kv_lib, ensure_cufile_lib):
        print(f.__name__, f())

