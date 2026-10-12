"""Client-side metadata (attribute) cache.

Every stat()/lookup on the original POSIX path is at least one network
round trip (often several: PooledFileSystem.exists() checks the dir-marker
key and then the file key, each on whatever node the hash picks). This
caches the MDT's answer per path -- including "does not exist" -- so the
repeat lookups the kernel issues for one syscall, and the stat-after-create
pattern mdtest measures, don't all go back over the wire.

Consistency model, stated plainly: this client's OWN mutations update or
invalidate the cache immediately (create/unlink/mkdir/rmdir/write size),
so a single mount always sees its own changes. Changes made by ANOTHER
client are visible after at most `ttl_s` -- the same bounded-staleness
contract as NFS's attribute cache (`actimeo`), not Lustre's DLM-lock
coherence. `ttl_s=0` disables caching entirely (every lookup hits the MDT).
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class Attr:
    path: str
    is_dir: bool
    size: int
    mtime: float
    ctime: float
    layout: dict | None  # {"stripe_count","stripe_size","stripe_offset"} for files
    inline: bool = False  # data lives in the MDT entry (fs/mdt_wire.py Data-on-MDT); frozen counts too


_MISSING = object()


class AttrCache:
    def __init__(self, ttl_s: float = 3.0, max_entries: int = 100_000):
        self.ttl_s = ttl_s
        self.max_entries = max_entries
        self._lock = threading.Lock()
        self._entries: dict[str, tuple[float, object]] = {}  # path -> (expires_at, Attr | None)
        self.hits = 0
        self.misses = 0

    def get(self, path: str):
        """Returns an Attr, None (cached "does not exist"), or _MISSING
        (not cached / expired -- caller must ask the MDT)."""
        if self.ttl_s <= 0:
            self.misses += 1
            return _MISSING
        with self._lock:
            hit = self._entries.get(path)
            if hit is not None and hit[0] > time.monotonic():
                self.hits += 1
                return hit[1]
            if hit is not None:
                del self._entries[path]
            self.misses += 1
            return _MISSING

    def put(self, path: str, attr: Attr | None) -> None:
        if self.ttl_s <= 0:
            return
        with self._lock:
            if len(self._entries) >= self.max_entries:
                self._entries.clear()  # crude, bounded; this is a cache, not state
            self._entries[path] = (time.monotonic() + self.ttl_s, attr)

    def update_size(self, path: str, size: int, *, exact: bool = False) -> None:
        """exact=True for truncate. A write's update only grows the cached
        size: concurrent writers' MDT set_size(max) replies can arrive in
        either order, and the older, smaller one landing last made stat()
        and reads see a truncated file (IOR, 8 ranks, one shared file)."""
        with self._lock:
            hit = self._entries.get(path)
            if hit is not None and isinstance(hit[1], Attr):
                a = hit[1]
                new = size if exact else max(size, a.size)
                self._entries[path] = (hit[0], Attr(a.path, a.is_dir, new, time.time(), a.ctime, a.layout, a.inline))

    def invalidate(self, path: str) -> None:
        with self._lock:
            self._entries.pop(path, None)


MISSING = _MISSING
