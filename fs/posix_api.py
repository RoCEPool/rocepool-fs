"""POSIX-flavored file operations layered on a StorageEngine or PoolNode.

Either a bare StorageEngine (single node) or a PoolNode (multi-node pool)
works here -- both satisfy put/get/delete/stat/list_prefix with the same
signatures (PoolNode simply forwards to whichever node owns the key), so
this layer is written against a minimal structural protocol, not either
concrete class.

This provides real file/directory semantics (paths, directory listing,
parent-must-exist-on-mkdir, EEXIST/ENOENT-shaped errors) on top of the
flat key/value StorageEngine namespace, by using "/"-joined keys and a
sentinel empty object with is_dir semantics tracked via a directory
registry. It's intentionally a THIN layer -- this is what a FUSE binding
(fs/fuse_binding.py) or any other file-facing frontend should sit on top
of, so file-semantics bugs get caught here, once, rather than in every
frontend.
"""
from __future__ import annotations

from typing import Protocol

from storage_engine.base import ObjectMeta, ObjectNotFound, PutResult, StorageError, VersionConflict

_WRITE_RANGE_MAX_ATTEMPTS = 1000


class _Backend(Protocol):
    def put(self, key: str, data: bytes, *, expected_version: int | None = None) -> PutResult: ...
    def get(self, key: str) -> bytes: ...
    def delete(self, key: str) -> None: ...
    def stat(self, key: str) -> ObjectMeta: ...
    def list_prefix(self, prefix: str) -> list[ObjectMeta]: ...
    def exists(self, key: str) -> bool: ...


class FileNotFoundInPool(Exception):
    def __init__(self, path: str):
        super().__init__(f"no such file or directory: {path!r}")
        self.path = path


class NotADirectory(Exception):
    def __init__(self, path: str):
        super().__init__(f"not a directory: {path!r}")
        self.path = path


class IsADirectory(Exception):
    def __init__(self, path: str):
        super().__init__(f"is a directory: {path!r}")
        self.path = path


class FileExistsInPool(Exception):
    def __init__(self, path: str):
        super().__init__(f"file exists: {path!r}")
        self.path = path


class DirectoryNotEmpty(Exception):
    def __init__(self, path: str):
        super().__init__(f"directory not empty: {path!r}")
        self.path = path


_DIR_MARKER_SUFFIX = "/.dir"


def _normalize(path: str) -> str:
    path = path.strip("/")
    parts = [p for p in path.split("/") if p not in ("", ".")]
    return "/".join(parts)


def _dir_key(path: str) -> str:
    return (path + _DIR_MARKER_SUFFIX) if path else ".dir"  # root


def _parent(path: str) -> str:
    return "/".join(path.split("/")[:-1])


class PooledFileSystem:
    """File-path-shaped operations on top of a flat pool/engine backend."""

    def __init__(self, backend: _Backend):
        self.backend = backend
        # Root always "exists" implicitly; mkdir("") is never required.

    def mkdir(self, path: str, *, parents: bool = False) -> None:
        path = _normalize(path)
        if not path:
            return  # root
        parent = _parent(path)
        if parent and not self._dir_exists(parent):
            if parents:
                self.mkdir(parent, parents=True)
            else:
                raise FileNotFoundInPool(parent)
        if self._dir_exists(path) or self.backend.exists(path):
            raise FileExistsInPool(path)
        self.backend.put(_dir_key(path), b"")

    def _dir_exists(self, path: str) -> bool:
        path = _normalize(path)
        return path == "" or self.backend.exists(_dir_key(path))

    def rmdir(self, path: str) -> None:
        path = _normalize(path)
        if not self._dir_exists(path):
            raise FileNotFoundInPool(path)
        children = self.listdir(path)
        if children:
            raise DirectoryNotEmpty(path)
        self.backend.delete(_dir_key(path))

    def write_file(self, path: str, data: bytes, *, create_parents: bool = False) -> PutResult:
        path = _normalize(path)
        if not path:
            raise IsADirectory(path)
        parent = _parent(path)
        if parent and not self._dir_exists(parent):
            if create_parents:
                self.mkdir(parent, parents=True)
            else:
                raise FileNotFoundInPool(parent)
        if self._dir_exists(path):
            raise IsADirectory(path)
        return self.backend.put(path, data)

    def write_range(self, path: str, offset: int, data: bytes, *, create_parents: bool = False) -> None:
        """CAS-protected partial write: merges `data` into whatever the
        CURRENT backend content is at `offset`, retrying if a concurrent
        writer's range changed the object first. This is what makes two
        independent writers extending/filling DISJOINT byte ranges of the
        SAME file safe -- the guarantee POSIX (and any shared-file MPI-IO
        access pattern, e.g. IOR's default "single shared file" mode)
        depends on.

        `write_file` (whole-object put) does NOT give that guarantee: two
        independently open()'d handles each hold their own local snapshot,
        and whichever one flushes last overwrites the object with only ITS
        bytes, silently erasing the other's. `fs/fuse_lowlevel.py` used to
        flush that way. Found by running real IOR (2 ranks, single shared
        file, disjoint 4 MiB halves) against a real kernel FUSE mount: the
        file's reported size was half what IOR had written, and the read
        phase hit a premature EOF. Same class of bug, same fix shape, as
        `fs/striping.py`'s `write_range` (found first there, via MPI-IO).
        """
        path = _normalize(path)
        if not data:
            return
        parent = _parent(path)
        if parent and not self._dir_exists(parent):
            if create_parents:
                self.mkdir(parent, parents=True)
            else:
                raise FileNotFoundInPool(parent)
        if self._dir_exists(path):
            raise IsADirectory(path)
        end = offset + len(data)
        for _ in range(_WRITE_RANGE_MAX_ATTEMPTS):
            try:
                version = self.backend.stat(path).version
                current = self.backend.get(path)
            except ObjectNotFound:
                version, current = 0, b""
            merged = bytearray(current)
            if len(merged) < end:
                merged.extend(b"\x00" * (end - len(merged)))
            merged[offset:end] = data
            try:
                self.backend.put(path, bytes(merged), expected_version=version)
                return
            except VersionConflict:
                continue  # a concurrent writer changed it first -- reread and reapply our bytes
        raise StorageError(f"write_range on {path!r}: gave up after {_WRITE_RANGE_MAX_ATTEMPTS} CAS attempts")

    def read_file(self, path: str) -> bytes:
        path = _normalize(path)
        if self._dir_exists(path):
            raise IsADirectory(path)
        try:
            return self.backend.get(path)
        except ObjectNotFound as exc:
            raise FileNotFoundInPool(path) from exc

    def remove_file(self, path: str) -> None:
        path = _normalize(path)
        if self._dir_exists(path):
            raise IsADirectory(path)
        try:
            self.backend.delete(path)
        except ObjectNotFound as exc:
            raise FileNotFoundInPool(path) from exc

    def stat_path(self, path: str) -> ObjectMeta:
        path = _normalize(path)
        if self._dir_exists(path):
            meta = self.backend.stat(_dir_key(path))
            return ObjectMeta(key=path, size=0, created_at=meta.created_at,
                               updated_at=meta.updated_at, version=meta.version, is_dir=True)
        try:
            return self.backend.stat(path)
        except ObjectNotFound as exc:
            raise FileNotFoundInPool(path) from exc

    def listdir(self, path: str) -> list[str]:
        """Immediate children only (one level), like os.listdir -- not a
        recursive dump of everything under the prefix."""
        path = _normalize(path)
        if not self._dir_exists(path):
            raise FileNotFoundInPool(path)
        prefix = (path + "/") if path else ""
        seen = set()
        names = []
        for meta in self.backend.list_prefix(prefix):
            rest = meta.key[len(prefix):]
            if rest == ".dir":
                continue  # the listed directory's OWN marker, not a child
            if rest.endswith(_DIR_MARKER_SUFFIX):
                rest = rest[: -len(_DIR_MARKER_SUFFIX)]
            if not rest:
                continue
            name = rest.split("/", 1)[0]
            if name not in seen:
                seen.add(name)
                names.append(name)
        return sorted(names)

    def exists(self, path: str) -> bool:
        path = _normalize(path)
        return self._dir_exists(path) or self.backend.exists(path)


# ---------------------------------------------------------------------------
# DistributedFileSystem: the same file-facing surface as PooledFileSystem,
# rebuilt on a Lustre-style split -- metadata on dedicated MDTs
# (fs/mdt_node.py, sharded by fs/namespace_shard.py, cached client-side by
# fs/attr_cache.py), file bytes striped across OSTs (fs/striping.py's
# layout arithmetic, fs/layout.py's per-file layout), with every multi-OST
# operation fanned out concurrently through roce/request_batcher.py.
#
# What changes vs PooledFileSystem for the same POSIX call:
#   - a shared file's bytes are no longer ONE object on ONE node rewritten
#     whole on every write_range (read full object + write full object per
#     4 MiB flush piece); each write touches only the stripes it overlaps,
#     on up to stripe_count OSTs at once, and a write that covers a whole
#     stripe is a single blind PUT with no read at all.
#   - existence/size/listing questions are one MDT request each (or zero,
#     on an attribute-cache hit) instead of 1-3 KV stats on hash-chosen
#     nodes, and a directory listing is one request to one MDT instead of
#     a list_prefix broadcast to every node.
# ---------------------------------------------------------------------------
import threading as _threading  # noqa: E402
from collections import OrderedDict as _OrderedDict  # noqa: E402

from fs.attr_cache import MISSING as _ATTR_MISSING, Attr, AttrCache  # noqa: E402
from fs.layout import choose_layout, layout_from_dict, layout_to_dict  # noqa: E402
from fs.mdt_node import MDTClient, MDTError  # noqa: E402
from fs.namespace_shard import NamespaceShard  # noqa: E402
from fs.striping import StripeLayout, _read_object_with_version, stripe_key, stripe_ranges  # noqa: E402
from roce.protocol import Op  # noqa: E402
from roce.request_batcher import RequestBatcher  # noqa: E402

_ROOT_ATTR = Attr(path="", is_dir=True, size=0, mtime=0.0, ctime=0.0, layout=None)
_STRIPE_CAS_MAX_ATTEMPTS = 1000


class StripeReadCache:
    """Per-open-file cache of whole stripe objects fetched for reads, with
    asynchronous readahead. Kernel FUSE reads arrive as 128 KiB requests;
    fetching the 1 MiB stripe each one falls in, once, and serving the next
    seven reads from memory avoids 8x read amplification. Readahead fetches
    the following stripes -- which sit on OTHER OSTs -- in the background
    while the current ones are being served, so a single sequential reader
    keeps every OST busy instead of stalling on each stripe in turn.

    Close-to-open consistency (the NFS contract): data written by another
    client after this file handle was opened may not be visible through
    it; reopening sees it. This handle's own writes clear it (see
    fs/fuse_lowlevel.py _flush_handle)."""

    def __init__(self, max_stripes: int = 64):
        self.max_stripes = max_stripes
        self._stripes: "_OrderedDict[int, bytes]" = _OrderedDict()
        self._inflight: dict = {}  # stripe index -> Future[bytes]
        self.lock = _threading.Lock()
        self._generation = 0  # bumped by clear(); stale in-flight results are dropped

    def get(self, idx: int) -> bytes | None:
        with self.lock:
            data = self._stripes.get(idx)
            if data is not None:
                self._stripes.move_to_end(idx)
            return data

    def put(self, idx: int, data: bytes, generation: int | None = None) -> None:
        with self.lock:
            if generation is not None and generation != self._generation:
                return
            self._stripes[idx] = data
            self._stripes.move_to_end(idx)
            while len(self._stripes) > self.max_stripes:
                self._stripes.popitem(last=False)

    def clear(self) -> None:
        with self.lock:
            self._stripes.clear()
            self._inflight.clear()
            self._generation += 1


class _LocalReply:
    __slots__ = ("op", "key", "value")

    def __init__(self, op, key, value=b""):
        self.op, self.key, self.value = op, key, value


def _ost_batch(ost, subs):
    """BATCH on a remote OST; the same sub-ops one by one on a local engine."""
    if hasattr(ost, "batch"):
        return ost.batch(subs)
    out = []
    for op, key, value in subs:
        try:
            if op == Op.DELETE:
                ost.delete(key)
            elif op == Op.TRUNCATE:
                ost.truncate_object(key, int.from_bytes(value[:8], "little"))
            else:
                raise StorageError(f"local batch: unsupported op {op}")
            out.append(_LocalReply(Op(int(op) + 1), key))
        except ObjectNotFound:
            out.append(_LocalReply(Op.ERROR, key, b"NOT_FOUND\x00"))
    return out


def _check_batch(replies, ok_missing: bool = True) -> None:
    """Raise if a BATCH sub-request failed (a missing object is fine for
    deletes/trims: it was a hole)."""
    for r in replies:
        if r.op == Op.ERROR:
            code = r.value.split(b"\x00", 1)[0].decode()
            if code == "NOT_FOUND" and ok_missing:
                continue
            raise StorageError(f"{code} on {r.key}")


def _inline_hint(attr) -> int:
    """The largest file that can be inline -- the servers' rule
    (fs/mdt_node.inline_limit), from the same ROCEPOOL_INLINE_MAX."""
    from fs.mdt_node import inline_limit
    return inline_limit({"layout": attr.layout})


def _engine_peer(engine) -> str:
    return getattr(engine, "address", None) or f"local-{id(engine)}"


class DistributedFileSystem:
    range_io = True  # tells fs/fuse_lowlevel.py to use read_range/truncate instead of whole-file buffers

    def __init__(self, *, mdt_transport, mdts: list[str], osts: list, stripe_count: int | None = None,
                 stripe_size: int | None = None, attr_ttl_s: float = 3.0,
                 batcher: RequestBatcher | None = None, readahead_stripes: int | None = None,
                 inline: bool = True):
        if not osts:
            raise ValueError("need at least one OST")
        self.osts = osts
        self.shard = NamespaceShard(mdts)
        self.mdt = MDTClient(mdt_transport, self.shard)
        self.cache = AttrCache(ttl_s=attr_ttl_s)
        self.batcher = batcher or RequestBatcher()
        self.stripe_count = stripe_count
        from fs.striping import DEFAULT_STRIPE_SIZE
        self.stripe_size = stripe_size or DEFAULT_STRIPE_SIZE
        self.readahead_stripes = readahead_stripes if readahead_stripes is not None else 2 * len(osts)
        # "thick KV" (roce/protocol.py, fs/mdt_wire.py): new files start as
        # Data-on-MDT; partial-stripe writes are one WRITE_AT, not a CAS loop
        self.inline = inline

    # -- metadata --------------------------------------------------------
    @staticmethod
    def _attr_from_entry(path: str, e: dict) -> Attr:
        return Attr(path=path, is_dir=e["is_dir"], size=e["size"], mtime=e["mtime"], ctime=e["ctime"],
                    layout=e.get("layout"), inline=bool(e.get("inline")))

    def getattr(self, path: str) -> Attr | None:
        path = _normalize(path)
        if path == "":
            return _ROOT_ATTR
        cached = self.cache.get(path)
        if cached is not _ATTR_MISSING:
            return cached
        e = self.mdt.lookup(path)
        attr = None if e is None else self._attr_from_entry(path, e)
        self.cache.put(path, attr)
        return attr

    def _require_parent_dir(self, path: str, create_parents: bool = False) -> None:
        parent = _parent(path)
        attr = self.getattr(parent)
        if attr is None:
            if create_parents:
                self.mkdir(parent, parents=True)
                return
            raise FileNotFoundInPool(parent)
        if not attr.is_dir:
            raise NotADirectory(parent)

    def _require_file(self, path: str) -> Attr:
        attr = self.getattr(path)
        if attr is None:
            raise FileNotFoundInPool(path)
        if attr.is_dir:
            raise IsADirectory(path)
        return attr

    def exists(self, path: str) -> bool:
        return self.getattr(path) is not None

    def _dir_exists(self, path: str) -> bool:
        attr = self.getattr(path)
        return attr is not None and attr.is_dir

    def stat_path(self, path: str) -> ObjectMeta:
        path = _normalize(path)
        attr = self.getattr(path)
        if attr is None:
            raise FileNotFoundInPool(path)
        return ObjectMeta(key=path, size=attr.size, created_at=attr.ctime, updated_at=attr.mtime,
                          version=0, is_dir=attr.is_dir)

    def mkdir(self, path: str, *, parents: bool = False) -> None:
        path = _normalize(path)
        if not path:
            return
        self._require_parent_dir(path, create_parents=parents)
        try:
            e = self.mdt.create(path, is_dir=True, layout=None)
        except MDTError as exc:
            if exc.code == "EXISTS":
                raise FileExistsInPool(path) from exc
            raise
        self.cache.put(path, self._attr_from_entry(path, e))

    def rmdir(self, path: str) -> None:
        path = _normalize(path)
        attr = self.getattr(path)
        if attr is None:
            raise FileNotFoundInPool(path)
        if not attr.is_dir:
            raise NotADirectory(path)
        # Children live on mdt_for_children(path), the entry itself on
        # mdt_for(path) -- two MDTs, no cross-MDT transaction: a create racing
        # this rmdir can slip in between the emptiness check and the unlink.
        # Documented simplification vs Lustre DNE's distributed transactions.
        if self.mdt.child_count(path):
            raise DirectoryNotEmpty(path)
        try:
            self.mdt.unlink(path, is_dir=True)
        except MDTError as exc:
            if exc.code == "NOT_FOUND":
                raise FileNotFoundInPool(path) from exc
            raise
        self.cache.put(path, None)

    def listdir(self, path: str) -> list[str]:
        return [a.path.split("/")[-1] for a in self.listdir_attrs(path)]

    def listdir_attrs(self, path: str) -> list[Attr]:
        """Readdir that also primes the attribute cache with every child's
        attributes (NFS READDIRPLUS's idea): the kernel follows a readdir
        with a LOOKUP per name, and those now hit the cache."""
        path = _normalize(path)
        attr = self.getattr(path)
        if attr is None:
            raise FileNotFoundInPool(path)
        if not attr.is_dir:
            raise NotADirectory(path)
        out = []
        for e in self.mdt.readdir(path):
            child = (path + "/" + e["name"]) if path else e["name"]
            a = self._attr_from_entry(child, e)
            self.cache.put(child, a)
            out.append(a)
        return out

    def create_file(self, path: str, *, create_parents: bool = False) -> Attr:
        path = _normalize(path)
        if not path:
            raise IsADirectory(path)
        self._require_parent_dir(path, create_parents=create_parents)
        layout = choose_layout(path, len(self.osts), stripe_count=self.stripe_count, stripe_size=self.stripe_size)
        try:
            e = self.mdt.create(path, is_dir=False, layout=layout_to_dict(layout), exclusive=False,
                                inline_ok=self.inline)
        except MDTError as exc:
            if exc.code == "EXISTS":
                raise IsADirectory(path) from exc  # exists as a directory
            raise
        attr = self._attr_from_entry(path, e)
        self.cache.put(path, attr)
        return attr

    # -- data --------------------------------------------------------------
    def _stripe_task(self, path: str, layout: StripeLayout, span, data: bytes):
        ost = self.osts[span.ost_index]
        key = stripe_key(path, span.stripe_index)
        chunk = data[span.buf_lo:span.buf_lo + span.length]
        if span.in_stripe_lo == 0 and span.in_stripe_hi == layout.stripe_size:
            ost.put(key, chunk)  # covers the whole stripe: nothing to preserve, no read needed
            return
        # one atomic WRITE_AT on the OST (was: stat/get/stat + CAS put, retried)
        ost.write_at(key, span.in_stripe_lo, chunk)

    # -- Data-on-MDT ----------------------------------------------------------
    def _spill(self, path: str) -> None:
        """Move an inline file to its stripes (protocol: fs/mdt_wire.py).
        Safe to run by any number of clients at once, and to finish a spill
        some other client started."""
        while True:
            _, e, data = self.mdt.freeze(path)
            if not e["inline"]:
                break
            layout = layout_from_dict(e["layout"])
            ost = self.osts[layout.ost_for_stripe(0, len(self.osts))]
            key = stripe_key(path, 0)
            # a version TOKEN, not a plain version: a plain "expected 0 =
            # absent" CAS could land after this file was truncated/unlinked
            # (stripe deleted -> absent again) and even recreated -- ABA
            v0 = ost.version_token(key)
            _, e, data = self.mdt.freeze(path)  # still frozen AFTER reading v0?
            if not e["inline"]:
                break
            try:
                ost.put_token(key, data, v0)
                break
            except VersionConflict:
                continue
        e = self.mdt.spill_commit(path)
        self.cache.put(path, self._attr_from_entry(path, e))

    def _refresh(self, path: str) -> Attr | None:
        self.cache.invalidate(path)
        return self.getattr(path)

    def write_range(self, path: str, offset: int, data: bytes, *, create_parents: bool = False) -> None:
        path = _normalize(path)
        if not data:
            return
        attr = self.getattr(path)
        if attr is None:
            attr = self.create_file(path, create_parents=create_parents)
        elif attr.is_dir:
            raise IsADirectory(path)
        while attr.inline:
            try:  # one MDT round trip, no OST at all
                if offset + len(data) > _inline_hint(attr):
                    # can't fit: spill first rather than ship the data to the
                    # MDT just to hear INLINE_FULL (and blow the frame limit)
                    raise MDTError("INLINE_FULL", path)
                e = self.mdt.write_inline(path, offset, data)
                self.cache.put(path, self._attr_from_entry(path, e))
                return
            except MDTError as exc:
                if exc.code in ("INLINE_FULL", "FROZEN"):
                    self._spill(path)
                elif exc.code != "NOT_INLINE":
                    raise
            attr = self._refresh(path)
            if attr is None:
                raise FileNotFoundInPool(path)
        layout = layout_from_dict(attr.layout)
        spans = stripe_ranges(layout, offset, len(data), len(self.osts))
        self.batcher.map(lambda sp: self._stripe_task(path, layout, sp, data), spans,
                         peer_of=lambda sp: _engine_peer(self.osts[sp.ost_index]))
        end = offset + len(data)
        if end > attr.size:
            try:
                e = self.mdt.set_size(path, end, mode="max")
            except MDTError as exc:
                if exc.code not in ("INLINE", "FROZEN"):
                    raise
                # our cached attributes said "striped" but the file is inline
                # (recreated by another client) or mid-spill: finish any
                # spill, then redo the write where the file now lives
                if exc.code == "FROZEN":
                    self._spill(path)
                self._refresh(path)
                return self.write_range(path, offset, data)
            self.cache.update_size(path, e["size"])

    def _fetch_stripes(self, path: str, layout: StripeLayout, indices: list[int]) -> dict[int, bytes]:
        blobs = self.batcher.map(lambda idx: self._get_stripe(path, layout, idx), indices,
                                 peer_of=lambda i: _engine_peer(self.osts[layout.ost_for_stripe(i, len(self.osts))]))
        return dict(zip(indices, blobs))

    def _get_stripe(self, path: str, layout: StripeLayout, idx: int) -> bytes:
        try:
            return self.osts[layout.ost_for_stripe(idx, len(self.osts))].get(stripe_key(path, idx))
        except ObjectNotFound:
            return b""  # a hole: never written

    def read_range(self, path: str, offset: int, length: int, *, stripe_cache: StripeReadCache | None = None) -> bytes:
        path = _normalize(path)
        attr = self._require_file(path)
        if attr.inline:
            # one MDT round trip; the reply also says whether it is still inline
            _, e, data = self.mdt.read_inline(path, offset, max(0, length))
            self.cache.put(path, self._attr_from_entry(path, e))
            if e["inline"]:
                return data
            attr = self._attr_from_entry(path, e)
        length = max(0, min(length, attr.size - offset))
        if length <= 0:
            return b""
        layout = layout_from_dict(attr.layout)
        spans = stripe_ranges(layout, offset, length, len(self.osts))
        have: dict[int, bytes] = {}
        if stripe_cache is None:
            # no cache to fill: read exactly the bytes asked for (READ_AT)
            def _span(sp):
                ost = self.osts[sp.ost_index]
                try:
                    return ost.read_at(stripe_key(path, sp.stripe_index), sp.in_stripe_lo, sp.length)[1]
                except ObjectNotFound:
                    return b""
            pieces = self.batcher.map(_span, spans, peer_of=lambda sp: _engine_peer(self.osts[sp.ost_index]))
            out = bytearray()
            for sp, piece in zip(spans, pieces):
                out += piece + b"\x00" * (sp.length - len(piece))
            return bytes(out)
        else:
            waits, missing = {}, []
            with stripe_cache.lock:
                gen = stripe_cache._generation
                for sp in spans:
                    blob = stripe_cache._stripes.get(sp.stripe_index)
                    if blob is not None:
                        have[sp.stripe_index] = blob
                    elif sp.stripe_index in stripe_cache._inflight:
                        waits[sp.stripe_index] = stripe_cache._inflight[sp.stripe_index]
                    else:
                        missing.append(sp.stripe_index)
            if missing:
                fetched = self._fetch_stripes(path, layout, missing)
                for idx, blob in fetched.items():
                    stripe_cache.put(idx, blob, gen)
                have.update(fetched)
            for idx, fut in waits.items():
                have[idx] = fut.result()
            self._schedule_readahead(path, layout, attr.size, spans[-1].stripe_index, stripe_cache)
        out = bytearray()
        for sp in spans:
            piece = have[sp.stripe_index][sp.in_stripe_lo:sp.in_stripe_hi]
            if len(piece) < sp.length:
                piece += b"\x00" * (sp.length - len(piece))
            out += piece
        return bytes(out)

    def _schedule_readahead(self, path: str, layout: StripeLayout, size: int, last_idx: int,
                            cache: StripeReadCache) -> None:
        if self.readahead_stripes <= 0:
            return
        last_stripe = (size - 1) // layout.stripe_size
        with cache.lock:
            gen = cache._generation
            todo = [i for i in range(last_idx + 1, min(last_stripe, last_idx + self.readahead_stripes) + 1)
                    if i not in cache._stripes and i not in cache._inflight]
            for idx in todo:
                def _fetch(idx=idx):
                    blob = self._get_stripe(path, layout, idx)
                    cache.put(idx, blob, gen)
                    with cache.lock:
                        if cache._generation == gen:
                            cache._inflight.pop(idx, None)
                    return blob
                cache._inflight[idx] = self.batcher.submit(
                    _fetch, peer=_engine_peer(self.osts[layout.ost_for_stripe(idx, len(self.osts))]))

    def read_file(self, path: str) -> bytes:
        attr = self._require_file(_normalize(path))
        return self.read_range(path, 0, attr.size)

    def write_file(self, path: str, data: bytes, *, create_parents: bool = False) -> PutResult:
        path = _normalize(path)
        attr = self.getattr(path)
        if attr is None:
            self.create_file(path, create_parents=create_parents)
        elif attr.is_dir:
            raise IsADirectory(path)
        else:
            self.truncate(path, 0)
        self.write_range(path, 0, data)
        return PutResult(key=path, version=0, size=len(data))

    def truncate(self, path: str, size: int) -> None:
        path = _normalize(path)
        attr = self._require_file(path)
        while attr.inline:
            try:
                e = self.mdt.set_size(path, size, mode="exact")  # resizes the inline bytes
                self.cache.put(path, self._attr_from_entry(path, e))
                return
            except MDTError as exc:
                if exc.code in ("INLINE_FULL", "FROZEN"):
                    self._spill(path)
                elif exc.code != "NOT_INLINE":
                    raise
            attr = self._refresh(path)
            if attr is None:
                raise FileNotFoundInPool(path)
        layout = layout_from_dict(attr.layout)
        ss = layout.stripe_size
        if size < attr.size:
            last_old = (attr.size - 1) // ss
            first_gone = (size + ss - 1) // ss  # first stripe entirely beyond the new size
            per_ost: dict[int, list] = {}
            for idx in range(size // ss if size % ss else first_gone, last_old + 1):
                key = stripe_key(path, idx)
                sub = (Op.DELETE, key, b"") if idx >= first_gone else \
                    (Op.TRUNCATE, key, (size - idx * ss).to_bytes(8, "little"))
                per_ost.setdefault(layout.ost_for_stripe(idx, len(self.osts)), []).append(sub)
            # one BATCH frame per OST, server-side trims (no read-modify-write)
            self.batcher.map(lambda item: _check_batch(_ost_batch(self.osts[item[0]], item[1])), list(per_ost.items()),
                             peer_of=lambda item: _engine_peer(self.osts[item[0]]))
        e = self.mdt.set_size(path, size, mode="exact")
        self.cache.update_size(path, e["size"], exact=True)

    def remove_file(self, path: str) -> None:
        path = _normalize(path)
        self._require_file(path)
        try:
            e = self.mdt.unlink(path, is_dir=False)
        except MDTError as exc:
            if exc.code == "NOT_FOUND":
                self.cache.put(path, None)
                raise FileNotFoundInPool(path) from exc
            if exc.code == "IS_DIR":
                raise IsADirectory(path) from exc
            raise
        self.cache.put(path, None)
        if e.get("inline") and not e.get("frozen"):
            return  # the bytes went away with the MDT entry
        if (e["size"] > 0 or e.get("frozen")) and e.get("layout"):
            layout = layout_from_dict(e["layout"])
            n = max(1, (e["size"] + layout.stripe_size - 1) // layout.stripe_size)
            per_ost: dict[int, list] = {}
            for idx in range(n):
                per_ost.setdefault(layout.ost_for_stripe(idx, len(self.osts)), []).append(
                    (Op.DELETE, stripe_key(path, idx), b""))
            # one BATCH frame per OST instead of one request per stripe
            self.batcher.map(lambda item: _check_batch(_ost_batch(self.osts[item[0]], item[1]), ok_missing=True),
                             list(per_ost.items()), peer_of=lambda item: _engine_peer(self.osts[item[0]]))
