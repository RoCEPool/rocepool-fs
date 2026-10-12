"""Mounts a PooledFileSystem at a real kernel mountpoint using
fs/fuse_lowlevel.py and blocks serving requests until killed or
unmounted. This is what was actually run in
docs/REAL_KERNEL_FUSE_TEST.md's verification session.

Requires: root (or CAP_SYS_ADMIN), and /dev/fuse present -- i.e. a real
Linux kernel with FUSE support, not a container that blocks it. Swap
MemoryEngine() for a RapidsSEEngine(WlpClient(...)) or a multi-node
PoolNode to mount those instead -- fs/fuse_lowlevel.py only depends on
the PooledFileSystem surface, not on which backend it wraps.

Usage:
    python3 examples/mount_real_kernel_demo.py /mnt/some/empty/dir
    # in another shell: use it with ordinary coreutils, then
    fusermount3 -u /mnt/some/empty/dir   # or: umount /mnt/some/empty/dir
"""
import sys

sys.path.insert(0, __file__.rsplit("/examples/", 1)[0])

from fs.fuse_lowlevel import FuseKernelMount, FuseLowLevelDaemon  # noqa: E402
from fs.posix_api import PooledFileSystem  # noqa: E402
from storage_engine.memory_engine import MemoryEngine  # noqa: E402


def main() -> None:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <empty-mountpoint-dir>", file=sys.stderr)
        raise SystemExit(2)
    mountpoint = sys.argv[1]

    pfs = PooledFileSystem(MemoryEngine())
    mount = FuseKernelMount(mountpoint)
    daemon = FuseLowLevelDaemon(mount.fd, pfs)
    print(f"mounted at {mountpoint}; unmount with `umount {mountpoint}` "
          f"(or fusermount3 -u) when done", file=sys.stderr)
    try:
        daemon.run()
    finally:
        mount.close()


if __name__ == "__main__":
    main()
