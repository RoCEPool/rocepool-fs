"""A real Linux kernel FUSE low-level driver for PooledFileSystem -- talks
directly to /dev/fuse using the kernel's own binary FUSE protocol
(no libfuse, no fusepy: neither is installable in this sandbox -- `pip
install fusepy` fails the same way `pip install thrift` did, and
`apt-get install python3-fuse`/`libfuse3-dev` both hit 403 from the
package mirror, confirmed directly. `pip install requests` and other
ordinary packages work fine, so this is specific package/mirror
availability, not a general network outage).

This was possible without any of those because the actual protocol this
needs is public and stable: the kernel's own UAPI header,
`/usr/include/linux/fuse.h` (present in this sandbox, `FUSE_KERNEL_MINOR_
VERSION 39`), defines every struct and opcode used below byte-for-byte.
Every struct layout, field order, and opcode number in this file was
copied directly from that header, not reconstructed from memory -- so
this is implementing the actual kernel ABI, not guessing at it.

Only the operations needed to exercise ordinary file/directory workflows
are implemented: INIT, LOOKUP, GETATTR, SETATTR (for truncate), OPENDIR,
READDIR, RELEASEDIR, OPEN, CREATE, READ, WRITE, FLUSH, RELEASE, MKDIR,
RMDIR, UNLINK, STATFS, FORGET/DESTROY. Anything else (symlinks, hard
links, xattrs, locking, rename) returns ENOSYS/appropriate errno, which a
real kernel client handles gracefully (falls back or reports "not
supported") rather than hanging.

Whole-file buffering: OPEN/CREATE reads (or starts) the whole file into
an in-memory buffer per file handle; WRITE/READ operate on that buffer;
RELEASE/FLUSH write the whole buffer back through PooledFileSystem. This
matches what this project already is (an in-memory pool filesystem) and
keeps the kernel-facing code simple; it is not a general-purpose
mmap/streaming I/O path.
"""
from __future__ import annotations

import ctypes
import errno
import os
import stat as statmod
import struct
import threading

from fs.posix_api import (
    DirectoryNotEmpty, FileExistsInPool, FileNotFoundInPool, IsADirectory,
    NotADirectory, PooledFileSystem,
)

# ---- opcodes, straight from linux/fuse.h enum fuse_opcode ----
FUSE_LOOKUP, FUSE_FORGET, FUSE_GETATTR, FUSE_SETATTR = 1, 2, 3, 4
FUSE_MKDIR, FUSE_UNLINK, FUSE_RMDIR = 9, 10, 11
FUSE_OPEN, FUSE_READ, FUSE_WRITE, FUSE_STATFS, FUSE_RELEASE = 14, 15, 16, 17, 18
FUSE_FLUSH, FUSE_INIT = 25, 26
FUSE_OPENDIR, FUSE_READDIR, FUSE_RELEASEDIR = 27, 28, 29
FUSE_ACCESS, FUSE_CREATE, FUSE_DESTROY = 34, 35, 38
FUSE_BATCH_FORGET = 42

FUSE_ROOT_ID = 1

# Ceiling for one coalesced write_range() call from _flush_handle, well
# under RoCETransport's 16 MiB max_frame_bytes (roce/roce_transport.py) so
# a large sequential write still crosses the network in bounded pieces
# instead of one oversized frame. Also just a sane cap on how much a single
# in-process bytearray merge/copy grows to for a local (MemoryEngine)
# backend, which has no frame limit of its own.
_MAX_COALESCED_RUN = 4 * 1024 * 1024

# ---- struct formats, field-for-field from linux/fuse.h ----
_IN_HDR = struct.Struct("<IIQQIIIHH")     # fuse_in_header, 40 bytes
_OUT_HDR = struct.Struct("<IiQ")           # fuse_out_header, 16 bytes
_ATTR = struct.Struct("<6Q10I")            # fuse_attr, 88 bytes
_ENTRY_OUT_HEAD = struct.Struct("<4QII")   # fuse_entry_out minus trailing fuse_attr
_ATTR_OUT_HEAD = struct.Struct("<QII")     # fuse_attr_out minus trailing fuse_attr
_INIT_IN = struct.Struct("<IIII")          # first 4 fields of fuse_init_in (rest ignored)
_INIT_OUT = struct.Struct("<4I2HII2HI7I")  # full fuse_init_out, 64 bytes
_GETATTR_IN = struct.Struct("<IIQ")
_MKDIR_IN = struct.Struct("<II")
_OPEN_IN = struct.Struct("<II")
_CREATE_IN = struct.Struct("<IIII")
_OPEN_OUT = struct.Struct("<QII")
_RELEASE_IN = struct.Struct("<QIIQ")
_FLUSH_IN = struct.Struct("<QIIQ")
_READ_IN = struct.Struct("<QQIIQII")
_WRITE_IN = struct.Struct("<QQIIQII")
_WRITE_OUT = struct.Struct("<II")
_SETATTR_IN = struct.Struct("<IIQQQQQQIIIIIIII")
_STATFS_OUT = struct.Struct("<5Q4I6I")  # struct fuse_kstatfs: 5x u64, bsize/namelen/frsize/padding, spare[6]

FATTR_SIZE = 1 << 3

DT_DIR, DT_REG = 4, 8


def _libc():
    return ctypes.CDLL("libc.so.6", use_errno=True)


class FuseKernelMount:
    """Opens /dev/fuse and mounts it at `mountpoint` via a direct mount(2)
    syscall (we're root with CAP_SYS_ADMIN in this sandbox, so the
    setuid fusermount3 helper isn't needed -- this mounts against the
    real kernel VFS exactly as fusermount3 would, just without the extra
    process hop)."""

    def __init__(self, mountpoint: str, fsname: str = "rocepool"):
        self.mountpoint = mountpoint
        os.makedirs(mountpoint, exist_ok=True)
        self.fd = os.open("/dev/fuse", os.O_RDWR)
        libc = _libc()
        rootmode = 0o40755
        data = (f"fd={self.fd},rootmode={rootmode:o},user_id=0,group_id=0,"
                f"allow_other,max_read=131072").encode()
        source = fsname.encode()
        target = mountpoint.encode()
        fstype = b"fuse"
        MS_NOSUID, MS_NODEV = 2, 4
        ret = libc.mount(ctypes.c_char_p(source), ctypes.c_char_p(target),
                          ctypes.c_char_p(fstype), ctypes.c_ulong(MS_NOSUID | MS_NODEV),
                          ctypes.c_char_p(data))
        if ret != 0:
            err = ctypes.get_errno()
            os.close(self.fd)
            raise OSError(err, f"mount(2) failed: {os.strerror(err)}")

    def close(self) -> None:
        try:
            os.close(self.fd)
        except OSError:
            pass
        libc = _libc()
        # MNT_DETACH: lazy unmount, tolerates the daemon having already
        # stopped reading from the fd.
        libc.umount2(ctypes.c_char_p(self.mountpoint.encode()), ctypes.c_int(2))


class _FileHandle:
    """`buf` is this fd's own local view of the file (what read() answers
    from, so a write is visible to a read on the SAME fd before any
    flush -- ordinary POSIX same-fd read-after-write). `dirty_ranges`
    records only the (offset, bytes) this fd actually wrote; flushing
    replays just those ranges through `PooledFileSystem.write_range`'s
    CAS-protected merge instead of overwriting the whole object with this
    fd's possibly-stale full snapshot -- see write_range's docstring for
    the lost-update bug that whole-object flush caused with two
    independently open()'d writers."""
    __slots__ = ("path", "buf", "dirty_ranges", "stripe_cache", "lock")

    def __init__(self, path: str, buf: bytearray, stripe_cache=None):
        self.path = path
        self.buf = buf
        self.dirty_ranges: list[tuple[int, bytes]] = []
        # range-I/O backends only (fs/posix_api.py DistributedFileSystem):
        # this handle's read cache of whole stripes, and a lock serialising
        # write/flush/read on ONE handle when the daemon runs multi-threaded.
        self.stripe_cache = stripe_cache
        self.lock = threading.Lock()


class FuseLowLevelDaemon:
    """Reads raw kernel FUSE requests from an open /dev/fuse fd and serves
    them against a PooledFileSystem. Runs a simple single-threaded
    read-dispatch-write loop -- correctness and traceability over
    throughput, matching this project's existing testing priorities.
    """

    def __init__(self, fd: int, pfs: PooledFileSystem):
        self.fd = fd
        self.pfs = pfs
        # A backend with range_io (DistributedFileSystem) serves reads per
        # byte range from the OSTs and truncates in place, instead of this
        # daemon holding each open file's WHOLE content in a per-handle
        # buffer read at open() time. PooledFileSystem keeps the original
        # whole-buffer behaviour unchanged.
        self._range_io = bool(getattr(pfs, "range_io", False))
        self._lock = threading.RLock()  # inode maps, handle table, fh counter
        self._next_ino = 2
        self._ino_to_path: dict[int, str] = {FUSE_ROOT_ID: ""}
        self._path_to_ino: dict[str, int] = {"": FUSE_ROOT_ID}
        self._next_fh = 1
        self._handles: dict[int, _FileHandle] = {}
        self._stop = threading.Event()

    # -- inode bookkeeping --
    def _ino_for_path(self, path: str) -> int:
        with self._lock:
            if path in self._path_to_ino:
                return self._path_to_ino[path]
            ino = self._next_ino
            self._next_ino += 1
            self._path_to_ino[path] = ino
            self._ino_to_path[ino] = path
            return ino

    def _path_for_ino(self, ino: int) -> str:
        with self._lock:
            return self._ino_to_path[ino]

    def _new_fh(self, handle: "_FileHandle") -> int:
        with self._lock:
            fh = self._next_fh
            self._next_fh += 1
            self._handles[fh] = handle
            return fh

    def _get_handle(self, fh: int) -> "_FileHandle":
        with self._lock:
            return self._handles[fh]

    def _pop_handle(self, fh: int):
        with self._lock:
            return self._handles.pop(fh, None)

    def _attr_bytes(self, path: str, attr=None) -> bytes:
        if self._range_io:
            attr = attr if attr is not None else self.pfs.getattr(path)
            if attr is None:
                raise FileNotFoundInPool(path)
            ino = self._ino_for_path(path)
            if attr.is_dir:
                mode, size, nlink = statmod.S_IFDIR | 0o755, 0, 2
            else:
                mode, size, nlink = statmod.S_IFREG | 0o644, attr.size, 1
            return _ATTR.pack(ino, size, (size + 511) // 512, 0, 0, 0, 0, 0, 0,
                               mode, nlink, 0, 0, 0, 4096, 0)
        is_dir = (path == "") or self.pfs._dir_exists(path)
        ino = self._ino_for_path(path)
        if is_dir:
            mode = statmod.S_IFDIR | 0o755
            size, nlink = 0, 2
        else:
            meta = self.pfs.stat_path(path)
            mode = statmod.S_IFREG | 0o644
            size, nlink = meta.size, 1
        return _ATTR.pack(ino, size, (size + 511) // 512, 0, 0, 0, 0, 0, 0,
                           mode, nlink, 0, 0, 0, 4096, 0)

    # -- wire helpers --
    def _reply(self, unique: int, body: bytes = b"", error: int = 0) -> None:
        out = _OUT_HDR.pack(_OUT_HDR.size + len(body), -error if error else 0, unique) + body
        os.write(self.fd, out)

    def _err(self, unique: int, errno_val: int) -> None:
        self._reply(unique, b"", errno_val)

    def run(self, threads: int = 1) -> None:
        """threads > 1: that many workers each read and dispatch requests
        from the same /dev/fuse fd (the kernel hands each request to exactly
        one reader -- the same model as libfuse's multi-threaded loop), so
        independent requests from different processes (IOR/mdtest ranks)
        are served concurrently instead of queueing behind one another."""
        if threads <= 1:
            self._run_loop()
            return
        workers = [threading.Thread(target=self._run_loop, daemon=True, name=f"fuse-{i}")
                   for i in range(threads)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                raw = os.read(self.fd, 1024 * 1024)
            except OSError as exc:
                if exc.errno in (errno.ENODEV, errno.EBADF):
                    return  # unmounted
                if exc.errno == errno.EINTR:
                    continue
                raise
            if not raw:
                return
            self._dispatch(raw)

    def stop(self) -> None:
        self._stop.set()

    def _dispatch(self, raw: bytes) -> None:
        hlen, opcode, unique, nodeid, uid, gid, pid, ext, pad = _IN_HDR.unpack_from(raw, 0)
        body = raw[_IN_HDR.size:hlen]
        try:
            handler = self._HANDLERS.get(opcode)
            if handler is None:
                self._err(unique, errno.ENOSYS)
                return
            handler(self, unique, nodeid, body)
        except FileNotFoundInPool:
            self._err(unique, errno.ENOENT)
        except FileExistsInPool:
            self._err(unique, errno.EEXIST)
        except IsADirectory:
            self._err(unique, errno.EISDIR)
        except NotADirectory:
            self._err(unique, errno.ENOTDIR)
        except DirectoryNotEmpty:
            self._err(unique, errno.ENOTEMPTY)
        except KeyError:
            self._err(unique, errno.ENOENT)
        except Exception:
            # Any other bug in a handler must not take the whole mount
            # down -- reply EIO for this one request and keep serving,
            # so a single-op defect is visible (and testable) as one
            # failed syscall rather than an unrecoverable unmount.
            import traceback
            traceback.print_exc()
            try:
                self._err(unique, errno.EIO)
            except OSError:
                pass

    # -- handlers --
    def _h_init(self, unique, nodeid, body):
        major, minor, max_readahead, flags = _INIT_IN.unpack_from(body, 0)
        out = _INIT_OUT.pack(7, min(minor, 31), max_readahead, 0,
                              0, 0, 131072, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        self._reply(unique, out)

    def _h_destroy(self, unique, nodeid, body):
        self._reply(unique)

    def _h_forget(self, unique, nodeid, body):
        pass  # FUSE_FORGET has no reply

    def _h_lookup(self, unique, nodeid, body):
        name = body.split(b"\x00", 1)[0].decode()
        parent = self._path_for_ino(nodeid)
        child = (parent + "/" + name) if parent else name
        if self._range_io:
            attr = self.pfs.getattr(child)
            if attr is None:
                self._err(unique, errno.ENOENT)
                return
            ino = self._ino_for_path(child)
            self._reply(unique, _ENTRY_OUT_HEAD.pack(ino, 1, 1, 1, 0, 0) + self._attr_bytes(child, attr))
            return
        if not self.pfs.exists(child):
            self._err(unique, errno.ENOENT)
            return
        ino = self._ino_for_path(child)
        head = _ENTRY_OUT_HEAD.pack(ino, 1, 1, 1, 0, 0)
        self._reply(unique, head + self._attr_bytes(child))

    def _h_getattr(self, unique, nodeid, body):
        path = self._path_for_ino(nodeid)
        if self._range_io:
            attr = self.pfs.getattr(path)
            if attr is None:
                self._err(unique, errno.ENOENT)
                return
            self._reply(unique, _ATTR_OUT_HEAD.pack(1, 0, 0) + self._attr_bytes(path, attr))
            return
        if not self.pfs.exists(path) and path != "":
            self._err(unique, errno.ENOENT)
            return
        head = _ATTR_OUT_HEAD.pack(1, 0, 0)
        self._reply(unique, head + self._attr_bytes(path))

    def _h_setattr(self, unique, nodeid, body):
        path = self._path_for_ino(nodeid)
        fields = _SETATTR_IN.unpack_from(body, 0)
        valid, size = fields[0], fields[3]
        if valid & FATTR_SIZE and self._range_io:
            self.pfs.truncate(path, size)
        elif valid & FATTR_SIZE:
            try:
                current = self.pfs.read_file(path)
            except FileNotFoundInPool:
                current = b""
            new_buf = bytearray(current)
            if size < len(new_buf):
                del new_buf[size:]
            else:
                new_buf.extend(b"\x00" * (size - len(new_buf)))
            self.pfs.write_file(path, bytes(new_buf))
        head = _ATTR_OUT_HEAD.pack(1, 0, 0)
        self._reply(unique, head + self._attr_bytes(path))

    def _h_mkdir(self, unique, nodeid, body):
        mode, umask = _MKDIR_IN.unpack_from(body, 0)
        name = body[_MKDIR_IN.size:].split(b"\x00", 1)[0].decode()
        parent = self._path_for_ino(nodeid)
        child = (parent + "/" + name) if parent else name
        self.pfs.mkdir(child)
        ino = self._ino_for_path(child)
        head = _ENTRY_OUT_HEAD.pack(ino, 1, 1, 1, 0, 0)
        self._reply(unique, head + self._attr_bytes(child))

    def _h_rmdir(self, unique, nodeid, body):
        name = body.split(b"\x00", 1)[0].decode()
        parent = self._path_for_ino(nodeid)
        child = (parent + "/" + name) if parent else name
        self.pfs.rmdir(child)
        self._reply(unique)

    def _h_unlink(self, unique, nodeid, body):
        name = body.split(b"\x00", 1)[0].decode()
        parent = self._path_for_ino(nodeid)
        child = (parent + "/" + name) if parent else name
        self.pfs.remove_file(child)
        self._reply(unique)

    def _h_opendir(self, unique, nodeid, body):
        fh = self._new_fh(_FileHandle(self._path_for_ino(nodeid), bytearray()))
        self._reply(unique, _OPEN_OUT.pack(fh, 0, 0))

    def _h_releasedir(self, unique, nodeid, body):
        fh, *_ = _RELEASE_IN.unpack_from(body, 0)
        self._pop_handle(fh)
        self._reply(unique)

    def _h_readdir(self, unique, nodeid, body):
        fh, offset, size, *_ = _READ_IN.unpack_from(body, 0)
        path = self._get_handle(fh).path
        entries = [(".", DT_DIR), ("..", DT_DIR)]
        if self._range_io:
            for a in self.pfs.listdir_attrs(path):
                entries.append((a.path.split("/")[-1], DT_DIR if a.is_dir else DT_REG))
        else:
            for name in self.pfs.listdir(path):
                child = (path + "/" + name) if path else name
                is_dir = self.pfs._dir_exists(child)
                entries.append((name, DT_DIR if is_dir else DT_REG))

        self_ino = self._ino_for_path(path)
        out = bytearray()
        for i, (name, dtype) in enumerate(entries):
            if i < offset:
                continue
            name_b = name.encode()
            if name in (".", ".."):
                ino = self_ino
            else:
                child = (path + "/" + name) if path else name
                ino = self._ino_for_path(child)
            rec = struct.pack("<QQII", ino, i + 1, len(name_b), dtype) + name_b
            pad = (-len(rec)) % 8
            rec += b"\x00" * pad
            if len(out) + len(rec) > size:
                break
            out += rec
        self._reply(unique, bytes(out))

    def _h_open(self, unique, nodeid, body):
        path = self._path_for_ino(nodeid)
        if self._range_io:
            attr = self.pfs._require_file(path)  # ENOENT/EISDIR now, not at first read
            if not getattr(attr, "inline", False):
                # close-to-open for striped files (as the C client): an open
                # revalidates the size another node may have extended
                self.pfs.cache.invalidate(path)
                self.pfs._require_file(path)
            from fs.posix_api import StripeReadCache
            fh = self._new_fh(_FileHandle(path, bytearray(), StripeReadCache()))
        else:
            data = self.pfs.read_file(path)
            fh = self._new_fh(_FileHandle(path, bytearray(data)))
        self._reply(unique, _OPEN_OUT.pack(fh, 0, 0))

    def _h_create(self, unique, nodeid, body):
        flags, mode, umask, open_flags = _CREATE_IN.unpack_from(body, 0)
        name = body[_CREATE_IN.size:].split(b"\x00", 1)[0].decode()
        parent = self._path_for_ino(nodeid)
        child = (parent + "/" + name) if parent else name
        if self._range_io:
            from fs.posix_api import StripeReadCache
            attr = self.pfs.create_file(child)
            ino = self._ino_for_path(child)
            fh = self._new_fh(_FileHandle(child, bytearray(), StripeReadCache()))
            self._reply(unique, _ENTRY_OUT_HEAD.pack(ino, 1, 1, 1, 0, 0) + self._attr_bytes(child, attr)
                        + _OPEN_OUT.pack(fh, 0, 0))
            return
        self.pfs.write_file(child, b"")
        ino = self._ino_for_path(child)
        fh = self._new_fh(_FileHandle(child, bytearray()))
        entry_head = _ENTRY_OUT_HEAD.pack(ino, 1, 1, 1, 0, 0)
        self._reply(unique, entry_head + self._attr_bytes(child) + _OPEN_OUT.pack(fh, 0, 0))

    def _h_read(self, unique, nodeid, body):
        fh, offset, size, *_ = _READ_IN.unpack_from(body, 0)
        handle = self._get_handle(fh)
        if self._range_io:
            with handle.lock:
                if handle.dirty_ranges:
                    self._flush_handle(handle)  # same-fd read-after-write: make our writes visible first
                chunk = self.pfs.read_range(handle.path, offset, size, stripe_cache=handle.stripe_cache)
            self._reply(unique, chunk)
            return
        chunk = bytes(handle.buf[offset:offset + size])
        self._reply(unique, chunk)

    def _h_write(self, unique, nodeid, body):
        fh, offset, size, write_flags, lock_owner, flags, padding = _WRITE_IN.unpack_from(body, 0)
        data = body[_WRITE_IN.size:_WRITE_IN.size + size]
        handle = self._get_handle(fh)
        if self._range_io:
            with handle.lock:
                handle.dirty_ranges.append((offset, data))
            self._reply(unique, _WRITE_OUT.pack(len(data), 0))
            return
        end = offset + len(data)
        if end > len(handle.buf):
            handle.buf.extend(b"\x00" * (end - len(handle.buf)))
        handle.buf[offset:end] = data
        handle.dirty_ranges.append((offset, data))
        self._reply(unique, _WRITE_OUT.pack(len(data), 0))

    def _flush_handle(self, handle: _FileHandle) -> None:
        """Replay this fd's writes through `write_range`, coalescing
        adjacent/overlapping ranges first.

        The kernel splits any write larger than FUSE's max_write (128 KiB
        here, see `_h_init`) into many separate FUSE_WRITE requests, so one
        IOR-style large sequential write shows up as hundreds of small
        `dirty_ranges` entries. `write_range` is a CAS read-modify-write of
        the WHOLE object (see its docstring), so replaying each fragment
        individually is O(fragments x file_size) -- quadratic in the file
        size for a fully sequential write. Reproduced live: a 64 MiB
        single-shared-file IOR run (2 ranks, 4 MiB transfers => 256 x
        128 KiB fragments per rank) pegged a CPU core and never finished
        (killed after minutes; the same pattern at 1-4 MiB totals is fast
        enough to hide it). Coalescing first turns the common case (one
        writer filling a contiguous region, in order or not) into O(1)
        write_range calls covering the merged span.

        Each merged run is then re-split at _MAX_COALESCED_RUN so no single
        write_range call grows unbounded: a backend reached over the
        network (PoolNode over RoCETransport, see roce/roce_transport.py)
        caps any one wire frame at max_frame_bytes (16 MiB by default), and
        a fully-coalesced 32+ MiB run would exceed that where the old,
        un-coalesced 128 KiB fragments never did -- found the same way as
        the hang above, running IOR's larger case against a real 3-node
        pool mount over the crossover cable: "close() failed" once the
        first coalesced write tried to cross the cable in one frame. Still
        far fewer, far larger calls than one per FUSE fragment."""
        if not handle.dirty_ranges:
            return
        merged: list[list[int, bytearray]] = []  # [start, bytearray-of-bytes]
        for offset, data in sorted(handle.dirty_ranges, key=lambda r: r[0]):
            end = offset + len(data)
            if merged and offset <= merged[-1][0] + len(merged[-1][1]):
                run_start, run_buf = merged[-1]
                rel = offset - run_start
                if end > run_start + len(run_buf):
                    run_buf.extend(b"\x00" * (end - (run_start + len(run_buf))))
                run_buf[rel:rel + len(data)] = data
            else:
                merged.append([offset, bytearray(data)])
        if self._range_io:
            # Striped backend: no single object ever exceeds one stripe, so
            # a whole merged run can go down in ONE write_range call, which
            # fans its stripes out across OSTs concurrently -- splitting it
            # at _MAX_COALESCED_RUN would only serialise that parallelism.
            for run_start, run_buf in merged:
                self.pfs.write_range(handle.path, run_start, bytes(run_buf))
            handle.dirty_ranges = []
            if handle.stripe_cache is not None:
                handle.stripe_cache.clear()
            return
        for run_start, run_buf in merged:
            for piece_start in range(0, len(run_buf), _MAX_COALESCED_RUN):
                piece = run_buf[piece_start:piece_start + _MAX_COALESCED_RUN]
                self.pfs.write_range(handle.path, run_start + piece_start, bytes(piece))
        handle.dirty_ranges = []

    def _h_flush(self, unique, nodeid, body):
        fh, *_ = _FLUSH_IN.unpack_from(body, 0)
        with self._lock:
            handle = self._handles.get(fh)
        if handle is not None:
            with handle.lock:
                self._flush_handle(handle)
        self._reply(unique)

    def _h_release(self, unique, nodeid, body):
        fh, *_ = _RELEASE_IN.unpack_from(body, 0)
        handle = self._pop_handle(fh)
        if handle is not None:
            with handle.lock:
                self._flush_handle(handle)
        self._reply(unique)

    def _h_statfs(self, unique, nodeid, body):
        # blocks, bfree, bavail, files, ffree (all Q), bsize, namelen, frsize, padding (I), spare[6] (I)
        out = _STATFS_OUT.pack(1 << 20, 1 << 20, 1 << 20, 1 << 20, 1 << 20,
                                4096, 255, 4096, 0, 0, 0, 0, 0, 0, 0)
        self._reply(unique, out)

    def _h_access(self, unique, nodeid, body):
        self._reply(unique)

    _HANDLERS = {
        FUSE_INIT: _h_init, FUSE_DESTROY: _h_destroy,
        FUSE_FORGET: _h_forget, FUSE_BATCH_FORGET: _h_forget,
        FUSE_LOOKUP: _h_lookup, FUSE_GETATTR: _h_getattr, FUSE_SETATTR: _h_setattr,
        FUSE_MKDIR: _h_mkdir, FUSE_RMDIR: _h_rmdir, FUSE_UNLINK: _h_unlink,
        FUSE_OPENDIR: _h_opendir, FUSE_READDIR: _h_readdir, FUSE_RELEASEDIR: _h_releasedir,
        FUSE_OPEN: _h_open, FUSE_CREATE: _h_create,
        FUSE_READ: _h_read, FUSE_WRITE: _h_write,
        FUSE_FLUSH: _h_flush, FUSE_RELEASE: _h_release,
        FUSE_STATFS: _h_statfs, FUSE_ACCESS: _h_access,
    }
