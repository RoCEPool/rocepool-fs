"""Reference in-memory implementation of StorageEngine.

This is a REAL, working engine -- not a mock. It's what every test in this
project actually runs against, and it's a legitimate standalone engine for
single-node use. It is intentionally simple (a dict behind a lock) so its
correctness is easy to audit; a production engine (RapidsSE-backed or
otherwise) would swap in smarter concurrency control and eviction, but must
satisfy the exact same contract this one does -- which is why the test
suite in tests/test_storage_engine_contract.py is written against the
StorageEngine interface, not against this class, and should be re-run
against any replacement engine.
"""
from __future__ import annotations

import threading
import time
import zlib

from storage_engine.base import (
    ObjectExists, ObjectMeta, ObjectNotFound, PutResult, StorageEngine, VersionConflict,
)


class StorageEngineFull(Exception):
    """Raised when a put() would exceed the engine's configured capacity.

    (Defined here rather than in base.py because capacity enforcement is a
    reference-engine policy choice, not part of the abstract contract --
    other engines may size themselves differently.)
    """
    def __init__(self, key: str, incoming_bytes: int, current_bytes: int, max_bytes: int):
        super().__init__(f"put {key!r} ({incoming_bytes}B) would exceed capacity "
                          f"({current_bytes}B + {incoming_bytes}B > {max_bytes}B)")
        self.key = key


class _Entry:
    __slots__ = ("data", "version", "created_at", "updated_at", "is_dir")

    def __init__(self, data: bytes, version: int, created_at: float, is_dir: bool = False):
        self.data = data
        self.version = version
        self.created_at = created_at
        self.updated_at = created_at
        self.is_dir = is_dir


_WM_BUCKETS = 4096


def _bucket(key: str) -> int:
    return zlib.crc32(key.encode("utf-8", "surrogatepass")) % _WM_BUCKETS


class MemoryEngine(StorageEngine):
    def __init__(self, *, max_bytes: int | None = None):
        self._lock = threading.RLock()
        self._objects: dict[str, _Entry] = {}
        self._max_bytes = max_bytes
        self._total_bytes = 0
        # Delete watermarks (see StorageEngine.version_token): a key's
        # versions never repeat across delete/recreate -- a recreated key
        # starts above its bucket's watermark, and an absent key's token is
        # that watermark. Buckets bound the memory; sharing one only costs a
        # spurious conflict, never a wrong success.
        self._wm = [0] * _WM_BUCKETS

    def put(self, key: str, data: bytes, *, expected_version: int | None = None) -> PutResult:
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data must be bytes")
        with self._lock:
            existing = self._objects.get(key)
            current_version = existing.version if existing else 0
            if expected_version is not None and expected_version != current_version:
                raise VersionConflict(key, expected_version, current_version)
            new_size_delta = len(data) - (len(existing.data) if existing else 0)
            if self._max_bytes is not None and self._total_bytes + new_size_delta > self._max_bytes:
                raise StorageEngineFull(key, len(data), self.total_bytes(), self._max_bytes)
            now = time.time()
            new_version = current_version + 1 if existing else self._wm[_bucket(key)] + 1
            entry = _Entry(bytes(data), new_version, existing.created_at if existing else now)
            entry.updated_at = now
            self._objects[key] = entry
            self._total_bytes += new_size_delta
            return PutResult(key=key, version=new_version, size=len(data))

    def get(self, key: str) -> bytes:
        with self._lock:
            entry = self._objects.get(key)
            if entry is None:
                raise ObjectNotFound(key)
            return entry.data

    def stat(self, key: str) -> ObjectMeta:
        with self._lock:
            entry = self._objects.get(key)
            if entry is None:
                raise ObjectNotFound(key)
            return ObjectMeta(key=key, size=len(entry.data), created_at=entry.created_at,
                               updated_at=entry.updated_at, version=entry.version, is_dir=entry.is_dir)

    def delete(self, key: str) -> None:
        with self._lock:
            entry = self._objects.pop(key, None)
            if entry is None:
                raise ObjectNotFound(key)
            self._total_bytes -= len(entry.data)
            b = _bucket(key)
            self._wm[b] = max(self._wm[b], entry.version + 1)

    def list_prefix(self, prefix: str) -> list[ObjectMeta]:
        with self._lock:
            return sorted(
                (ObjectMeta(key=k, size=len(e.data), created_at=e.created_at,
                            updated_at=e.updated_at, version=e.version, is_dir=e.is_dir)
                 for k, e in self._objects.items() if k.startswith(prefix)),
                key=lambda m: m.key,
            )

    def exists(self, key: str) -> bool:
        with self._lock:
            return key in self._objects

    # -- thick-KV ops, atomic under the engine lock ----------------------
    def get_versioned(self, key: str) -> tuple[int, bytes]:
        with self._lock:
            entry = self._objects.get(key)
            if entry is None:
                raise ObjectNotFound(key)
            return entry.version, entry.data

    def read_at(self, key: str, offset: int, length: int) -> tuple[int, bytes]:
        with self._lock:
            entry = self._objects.get(key)
            if entry is None:
                raise ObjectNotFound(key)
            return entry.version, entry.data[offset:offset + length]

    def write_at(self, key: str, offset: int, data: bytes) -> PutResult:
        with self._lock:
            existing = self._objects.get(key)
            buf = bytearray(existing.data if existing else b"")
            if len(buf) < offset + len(data):
                buf.extend(b"\x00" * (offset + len(data) - len(buf)))
            buf[offset:offset + len(data)] = data
            return self.put(key, bytes(buf), expected_version=existing.version if existing else 0)

    def truncate_object(self, key: str, size: int) -> PutResult:
        with self._lock:
            existing = self._objects.get(key)
            if existing is None:
                return PutResult(key=key, version=0, size=0)
            if len(existing.data) <= size:
                return PutResult(key=key, version=existing.version, size=len(existing.data))
            return self.put(key, existing.data[:size], expected_version=existing.version)

    def version_token(self, key: str) -> int:
        with self._lock:
            entry = self._objects.get(key)
            return entry.version if entry is not None else self._wm[_bucket(key)]

    def put_token(self, key: str, data: bytes, token: int) -> PutResult:
        with self._lock:
            cur = self.version_token(key)
            if cur != token:
                raise VersionConflict(key, token, cur)
            return self.put(key, data)

    def total_bytes(self) -> int:
        with self._lock:
            return self._total_bytes
