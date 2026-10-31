"""Bundled third-party sources (deps/), so a site with no Internet access
builds everything from the package itself.

    deps/mpich-4.2.3-ch3.tar.xz     MPICH (ch3:sock; the parts other devices need removed)
    deps/io500-<tag>.tar.xz         the official IO500 app at the list's tag (tools/io500_run.IO500_TAG),
                                    unmodified, with its pinned IOR, pfind (+lz4), schema tools, as git
                                    checkouts (its own prepare.sh builds it; needs autotools for IOR)
    deps/ior-<hash>.tar.xz          upstream IOR/mdtest (tools/bench_ior.py), pre-bootstrapped
    deps/libfabric-2.1.0.tar.xz     libfabric (the ofi/EFA backend where /opt/amazon/efa is absent)
    deps/rdma-core-59.2.tar.xz      rdma-core (roce/native/build.sh where no dev package installs)

Every ensure_*() uses the bundled copy when it is there and only falls
back to downloading when it is not; ROCEPOOL_OFFLINE=1 forbids the
download (a clear error instead of a hang on a closed network).
tools/make_deps.sh rebuilds deps/ from upstream.
"""
from __future__ import annotations

import glob
import os
import shutil
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEPS = os.path.join(ROOT, "deps")
PATTERNS = {"mpich": "mpich-*.tar.xz", "io500": "io500-*.tar.xz", "ior": "ior-*.tar.xz",
            "libfabric": "libfabric-*.tar.xz", "rdma-core": "rdma-core-*.tar.xz"}


def offline() -> bool:
    return os.environ.get("ROCEPOOL_OFFLINE", "") in ("1", "yes", "true")


def path(name: str) -> str | None:
    hits = sorted(glob.glob(os.path.join(DEPS, PATTERNS[name])))
    return hits[-1] if hits else None


def extract(name: str, dest: str) -> bool:
    """Unpack the bundled `name` so its top directory becomes `dest`.
    False when it is not bundled (and downloading is allowed)."""
    tarball = path(name)
    if not tarball:
        if offline():
            raise RuntimeError(f"ROCEPOOL_OFFLINE=1 and {name} is not bundled in {DEPS} "
                               f"(expected {PATTERNS[name]}; tools/make_deps.sh builds it)")
        return False
    parent = os.path.dirname(os.path.abspath(dest))
    os.makedirs(parent, exist_ok=True)
    tmp = tempfile.mkdtemp(dir=parent, prefix=".deps-")
    try:
        subprocess.run(["tar", "xJf", tarball, "-C", tmp], check=True)
        tops = os.listdir(tmp)
        if len(tops) != 1:
            raise RuntimeError(f"{tarball}: expected one top directory, found {tops}")
        if os.path.exists(dest):
            shutil.rmtree(dest)
        os.rename(os.path.join(tmp, tops[0]), dest)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(f"[deps] {name}: unpacked the bundled {os.path.basename(tarball)}", flush=True)
    return True


def check_download(what: str) -> None:
    if offline():
        raise RuntimeError(f"ROCEPOOL_OFFLINE=1: would have to download {what}")
