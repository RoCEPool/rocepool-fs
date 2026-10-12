"""Pluggable storage-engine interface.

The whole point of this interface: the memory-pool file system and the RoCE
node-to-node pooling layer are written ONCE, against this interface, and
never against a specific engine's internals. Swapping in a different engine
(RapidsSE's, or anything else) later means implementing this interface --
nothing above this layer needs to change.

Deliberately small and block/object-oriented rather than SQL-shaped: a
pooled memory *file system* needs put/get/delete/list of named byte
objects, not a query planner. An analytical engine used as a backing store
would sit BEHIND an adapter that implements this interface using its own
storage primitives (e.g. mapping object keys to rows in an in-memory table)
-- that adapter is intentionally not included here, since it depends on an
engine this project does not bundle or redistribute code from.
"""
from __future__ import annotations

import abc
import time
from dataclasses import dataclass, field


class StorageError(Exception):
    """Base class for all storage-engine errors."""


class ObjectNotFound(StorageError):
    def __init__(self, key: str):
        super().__init__(f"object not found: {key!r}")
        self.key = key


class ObjectExists(StorageError):
    def __init__(self, key: str):
        super().__init__(f"object already exists: {key!r}")
        self.key = key


@dataclass(frozen=True)
class ObjectMeta:
    """Metadata for one stored object -- deliberately POSIX-flavored so the
    fs/ layer can map it onto stat() fields with no translation logic."""
    key: str
    size: int
    created_at: float
    updated_at: float
    version: int
    is_dir: bool = False


@dataclass(frozen=True)
class PutResult:
    key: str
    version: int
    size: int


class StorageEngine(abc.ABC):
    """Everything above this layer (fs/, roce/pool_node.py) depends only on
    this interface. Every method must be safe to call concurrently from
    multiple threads within one process -- the pool node may be serving
    several peer requests and local file-system calls at once.
    """

    @abc.abstractmethod
    def put(self, key: str, data: bytes, *, expected_version: int | None = None) -> PutResult:
        """Create or overwrite an object.

        expected_version, when given, makes this a compare-and-swap: the
        write only applies if the object's current version matches (or the
        object doesn't exist and expected_version == 0). Raises
        VersionConflict otherwise. This is the primitive the RoCE
        replication protocol uses to avoid last-writer-wins data loss
        across nodes.
        """

    @abc.abstractmethod
    def get(self, key: str) -> bytes:
        """Raises ObjectNotFound if the key does not exist."""

    @abc.abstractmethod
    def stat(self, key: str) -> ObjectMeta:
        """Raises ObjectNotFound if the key does not exist."""

    @abc.abstractmethod
    def delete(self, key: str) -> None:
        """Raises ObjectNotFound if the key does not exist."""

    @abc.abstractmethod
    def list_prefix(self, prefix: str) -> list[ObjectMeta]:
        """List all objects whose key starts with `prefix`, sorted by key."""

    @abc.abstractmethod
    def exists(self, key: str) -> bool:
        ...

    @abc.abstractmethod
    def total_bytes(self) -> int:
        """Current total size of all stored objects -- used by the pool
        node to report capacity/pressure to peers."""

    # -- "thick KV" operations ------------------------------------------
    # Generic versions built on the CAS primitive above, so every engine has
    # them; an engine that can do them atomically in place (MemoryEngine,
    # the native C OST) overrides them.
    def get_versioned(self, key: str) -> tuple[int, bytes]:
        """(version, data) as one consistent snapshot. ObjectNotFound if absent."""
        while True:
            before = self.stat(key).version
            data = self.get(key)
            if self.stat(key).version == before:
                return before, data

    def read_at(self, key: str, offset: int, length: int) -> tuple[int, bytes]:
        version, data = self.get_versioned(key)
        return version, bytes(data[offset:offset + length])

    def write_at(self, key: str, offset: int, data: bytes) -> PutResult:
        """Write `data` at `offset` (creating the object, zero-filling any gap)."""
        while True:
            try:
                version, old = self.get_versioned(key)
            except ObjectNotFound:
                version, old = 0, b""
            buf = bytearray(old)
            if len(buf) < offset + len(data):
                buf.extend(b"\x00" * (offset + len(data) - len(buf)))
            buf[offset:offset + len(data)] = data
            try:
                return self.put(key, bytes(buf), expected_version=version)
            except VersionConflict:
                continue

    # -- ABA-safe compare-and-swap ---------------------------------------
    # A plain `expected_version=0` means "absent", and a deleted-then-
    # recreated key restarts its history, so a CAS prepared against an old
    # state can land on a newer one (A -> deleted -> A'). A *version token*
    # names one state of a key uniquely: the version of a present object, or
    # the key's delete watermark when absent. Engines that track deletes
    # (MemoryEngine, the native C OST) never reuse a token; this generic
    # fallback (0 for absent) is only as strong as a plain CAS.
    def version_token(self, key: str) -> int:
        try:
            return self.stat(key).version
        except ObjectNotFound:
            return 0

    def put_token(self, key: str, data: bytes, token: int) -> PutResult:
        """Write only if the key is still in the state `token` named."""
        return self.put(key, data, expected_version=token)

    def truncate_object(self, key: str, size: int) -> PutResult:
        """Shrink the object to `size` if longer; never extends. PutResult(key, 0, 0) if absent."""
        while True:
            try:
                version, old = self.get_versioned(key)
            except ObjectNotFound:
                return PutResult(key=key, version=0, size=0)
            if len(old) <= size:
                return PutResult(key=key, version=version, size=len(old))
            try:
                return self.put(key, bytes(old[:size]), expected_version=version)
            except VersionConflict:
                continue


class VersionConflict(StorageError):
    def __init__(self, key: str, expected: int, actual: int):
        super().__init__(f"version conflict on {key!r}: expected {expected}, found {actual}")
        self.key = key
        self.expected = expected
        self.actual = actual
