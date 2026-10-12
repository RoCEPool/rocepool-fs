"""Lustre-style file striping: split one file's bytes across several
independent OSTs ("Object Storage Targets" in Lustre's terminology) so a
single file's I/O can fan out across multiple backends in parallel,
instead of the whole object living on one node the way
`roce/pool_node.py`'s consistent-hash ownership does.

What this deliberately mirrors from real Lustre, and what it doesn't:

  - **Stripe layout** (`StripeLayout`: `stripe_count`, `stripe_size`,
    `stripe_offset`) is the same idea as `lfs setstripe -c <count> -S
    <size> -i <offset>`: a file's bytes are cut into fixed-size stripes,
    and stripe `i` is placed on OST `(stripe_offset + i) % stripe_count`,
    round-robin, exactly like Lustre's RAID0-style layout. `get_layout`/
    `set_default_layout` mirror `lfs getstripe`/the inherited-striping-
    from-parent-directory behavior (simplified to a single explicit
    default here, not a full directory-EA-inheritance tree).
  - **Partial writes only touch the affected stripes** (read-modify-write
    per overlapping chunk), not a whole-file rewrite -- this is what
    makes striping actually parallelize I/O rather than defeat itself.
  - **What's NOT here**: no MDS/OSS process separation (the "OSTs" below
    are just a list of `StorageEngine`s reachable from this process --
    each can be a `roce.remote_engine.RemoteEngine` bound to one OST
    server over RoCE or TCP, which makes it a real network fan-out --
    tools/run_crossover_rdma.py runs it that way across two nodes); no OST failure/rebalancing/redundancy (RAID0 semantics: losing
    one OST loses every file that stripes across it, exactly like real
    Lustre without a replication layer on top); no quota, no HSM, no
    Lustre DLM. Instead of DLM extent locks, each stripe object is updated
    with compare-and-swap, which gives the guarantee that matters for
    MPI-IO: concurrent writes to disjoint byte ranges all survive, even
    when they share a stripe (see `write_range`).
"""
from __future__ import annotations

import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace

from storage_engine.base import ObjectNotFound, StorageEngine, StorageError, VersionConflict

DEFAULT_STRIPE_SIZE = 1 << 20  # 1MiB, Lustre's own historical default


@dataclass(frozen=True)
class StripeLayout:
    stripe_count: int = 1
    stripe_size: int = DEFAULT_STRIPE_SIZE
    stripe_offset: int = 0  # index of the first OST used, like `lfs setstripe -i`

    def ost_for_stripe(self, stripe_index: int, num_osts: int) -> int:
        # Two-step mapping, matching real Lustre RAID0 layout: a file
        # allocates exactly `stripe_count` OST *objects* (one per
        # participating OST), and stripe `i` is placed on OST-object
        # `i % stripe_count` (wrapping back to the same objects/OSTs once
        # the file has more stripes than participating OSTs) -- which
        # then maps to the actual pool OST index `stripe_offset + that`,
        # wrapped by the pool size. Collapsing this into one
        # `(stripe_offset + stripe_index) % num_osts` (an earlier version
        # of this method did exactly that) is wrong whenever
        # stripe_count < num_osts: it would keep visiting new pool OSTs
        # beyond the file's actual stripe_count instead of cycling back
        # over just the OSTs the file was laid out on.
        object_index = stripe_index % self.stripe_count
        return (self.stripe_offset + object_index) % num_osts


@dataclass(frozen=True)
class StripeSpan:
    """One stripe's share of a byte range: which stripe, which OST index,
    and where inside the stripe object / inside the caller's buffer."""
    stripe_index: int
    ost_index: int
    in_stripe_lo: int   # offset within the stripe object
    in_stripe_hi: int
    buf_lo: int         # offset within the caller's data buffer

    @property
    def length(self) -> int:
        return self.in_stripe_hi - self.in_stripe_lo


def stripe_ranges(layout: StripeLayout, offset: int, length: int, num_osts: int) -> list[StripeSpan]:
    """Split `[offset, offset+length)` into per-stripe spans. The same
    arithmetic StripedStorage.write_range/read_range do inline, pulled out
    so the POSIX path (fs/posix_api.py's DistributedFileSystem) and the
    MPI-IO path share one definition of where a byte lives."""
    if length <= 0:
        return []
    size = layout.stripe_size
    end = offset + length
    spans = []
    for idx in range(offset // size, (end - 1) // size + 1):
        s_start = idx * size
        lo = max(offset, s_start)
        hi = min(end, s_start + size)
        spans.append(StripeSpan(idx, layout.ost_for_stripe(idx, num_osts), lo - s_start, hi - s_start, lo - offset))
    return spans


def stripe_key(path: str, stripe_index: int) -> str:
    return _stripe_key(path, stripe_index)


def _lov_key(path: str) -> str:
    """Key for a file's layout metadata -- the analogue of Lustre's LOV
    (Logical Object Volume) extended attribute that records which OSTs a
    file's stripes live on."""
    return f"{path}.lov"


def _stripe_key(path: str, stripe_index: int) -> str:
    return f"{path}.stripe.{stripe_index:08d}"


class StripedStorage:
    """Presents a single flat `path -> bytes` object namespace (same
    put/get/delete/exists shape callers of a plain StorageEngine already
    expect) while physically spreading each file's data across `osts`.
    """

    def __init__(self, osts: list[StorageEngine], *, default_layout: StripeLayout | None = None):
        if not osts:
            raise ValueError("StripedStorage needs at least one OST")
        self.osts = osts
        self.default_layout = default_layout or StripeLayout(stripe_count=len(osts))

    # -- layout (LOV) management, the `lfs setstripe`/`lfs getstripe` analogue --
    def set_default_layout(self, layout: StripeLayout) -> None:
        if layout.stripe_count < 1 or layout.stripe_count > len(self.osts):
            raise ValueError(f"stripe_count must be in [1, {len(self.osts)}]")
        self.default_layout = layout

    def get_layout(self, path: str) -> StripeLayout:
        meta = self._read_lov(path)
        return StripeLayout(stripe_count=meta["stripe_count"], stripe_size=meta["stripe_size"],
                             stripe_offset=meta["stripe_offset"])

    def _read_lov(self, path: str) -> dict:
        meta, _version = self._read_lov_with_version(path)
        return meta

    def _read_lov_with_version(self, path: str) -> tuple[dict, int]:
        # get() and stat() are two separate calls, so a concurrent writer
        # could land between them; re-check the version hasn't moved
        # (retrying if it has) so the (content, version) pair returned is
        # a genuine consistent snapshot, not content from one write
        # paired with the version of a later one -- which would let
        # _bump_total_size's CAS "succeed" while silently discarding a
        # concurrent update it never actually saw.
        while True:
            try:
                before = self.osts[0].stat(_lov_key(path)).version
                raw = self.osts[0].get(_lov_key(path))
                after = self.osts[0].stat(_lov_key(path)).version
            except ObjectNotFound:
                raise ObjectNotFound(path)
            if before == after:
                return json.loads(raw), after

    def _write_lov(self, path: str, layout: StripeLayout, total_size: int, *,
                    expected_version: int | None = None) -> None:
        # The LOV record itself always lives on OST[0] -- Lustre's real
        # MDS/OST split keeps layout metadata physically separate from
        # stripe data too, just on a dedicated metadata server rather
        # than "OST index zero"; this is the same separation of concerns
        # at a smaller scale.
        payload = json.dumps({
            "stripe_count": layout.stripe_count, "stripe_size": layout.stripe_size,
            "stripe_offset": layout.stripe_offset, "total_size": total_size,
            "updated_at": time.time(),
        }).encode()
        self.osts[0].put(_lov_key(path), payload, expected_version=expected_version)

    def _bump_total_size(self, path: str, layout: StripeLayout, at_least: int) -> None:
        """Compare-and-swap loop raising `total_size` to `max(current,
        at_least)`. Needed because concurrent disjoint-region writers
        (exactly the `mpi/mpi_file.py` collective-write case: every rank
        writes a different byte range of the SAME file at the same time)
        each read the LOV record, compute their own new total, and would
        otherwise blind-overwrite each other's update with a stale value
        -- a real bug this file's own test suite caught (see
        tests/test_mpi_file.py's collective write test, which failed with
        a lost update before this CAS loop existed). This is metadata
        bookkeeping, not stripe data, so it's fixable even though
        genuinely overlapping stripe writes are still documented as
        racy in `write_range`'s own docstring."""
        while True:
            meta, version = self._read_lov_with_version(path)
            layout_now = StripeLayout(meta["stripe_count"], meta["stripe_size"], meta["stripe_offset"])
            new_total = max(meta["total_size"], at_least)
            if new_total == meta["total_size"]:
                return
            try:
                self._write_lov(path, layout_now, new_total, expected_version=version)
                return
            except VersionConflict:
                continue  # someone else updated it first -- re-read and retry

    def exists(self, path: str) -> bool:
        return self.osts[0].exists(_lov_key(path))

    def total_size(self, path: str) -> int:
        return self._read_lov(path)["total_size"]

    def delete(self, path: str) -> None:
        meta = self._read_lov(path)
        layout = StripeLayout(meta["stripe_count"], meta["stripe_size"], meta["stripe_offset"])
        n_stripes = _stripe_count_for_size(meta["total_size"], layout.stripe_size)

        def _delete_one(i: int) -> None:
            ost = self.osts[layout.ost_for_stripe(i, len(self.osts))]
            key = _stripe_key(path, i)
            if ost.exists(key):
                ost.delete(key)

        with ThreadPoolExecutor(max_workers=max(1, layout.stripe_count)) as pool:
            list(pool.map(_delete_one, range(n_stripes)))
        self.osts[0].delete(_lov_key(path))

    # -- whole-file convenience (creates with the current default layout) --
    def put(self, path: str, data: bytes, *, layout: StripeLayout | None = None) -> None:
        layout = layout or self.default_layout
        if self.exists(path):
            self.delete(path)
        self._write_lov(path, layout, 0)
        self.write_range(path, 0, data)

    def get(self, path: str) -> bytes:
        total = self.total_size(path)
        return self.read_range(path, 0, total)

    # -- partial, stripe-aware range I/O: the part that actually parallelizes --
    def write_range(self, path: str, offset: int, data: bytes) -> None:
        """Read-modify-write only the stripes that `[offset, offset+len(data))`
        overlaps, fanned out across OSTs concurrently.

        Concurrent writers to disjoint byte ranges of the same file are
        safe, including when those ranges share a stripe (the POSIX /
        Lustre guarantee that N-1 MPI-IO patterns rely on): each stripe
        object is updated with compare-and-swap on its version and retried
        on conflict, so no writer's bytes can be overwritten by a stale
        copy of the stripe. Real Lustre gets the same guarantee from DLM
        extent locks; this is the optimistic equivalent.

        Found by running N-1 MPI-IO across two network nodes
        (tools/run_crossover_rdma.py, phase hpc): the previous blind
        read-modify-write lost a whole 300KiB segment in the segmented
        pattern and 10 of 64 blocks in the strided one.

        Overlapping concurrent writes still end with one writer's bytes
        per overlapping byte, in no guaranteed order -- the same as POSIX.
        """
        if not self.exists(path):
            try:
                self._write_lov(path, self.default_layout, 0, expected_version=0)
            except VersionConflict:
                pass  # another concurrent writer created it first -- fine, use what's there
        meta = self._read_lov(path)
        layout = StripeLayout(meta["stripe_count"], meta["stripe_size"], meta["stripe_offset"])
        size = layout.stripe_size
        end = offset + len(data)

        first_stripe = offset // size
        last_stripe = (end - 1) // size if end > offset else first_stripe

        def _write_one(stripe_index: int) -> None:
            stripe_start = stripe_index * size
            stripe_end = stripe_start + size
            ost = self.osts[layout.ost_for_stripe(stripe_index, len(self.osts))]
            key = _stripe_key(path, stripe_index)
            lo = max(offset, stripe_start) - stripe_start
            hi = min(end, stripe_end) - stripe_start
            src_lo = max(offset, stripe_start) - offset
            chunk = data[src_lo:src_lo + (hi - lo)]
            for attempt in range(_STRIPE_CAS_MAX_ATTEMPTS):
                version, existing = _read_object_with_version(ost, key)
                existing = bytearray(existing)
                if len(existing) < hi:
                    existing.extend(b"\x00" * (hi - len(existing)))
                existing[lo:hi] = chunk
                try:
                    ost.put(key, bytes(existing), expected_version=version)
                    return
                except VersionConflict:
                    time.sleep(random.uniform(0, 0.002 * min(attempt + 1, 10)))  # de-synchronise contenders
            raise StorageError(f"stripe {key!r}: gave up after {_STRIPE_CAS_MAX_ATTEMPTS} CAS attempts")

        stripe_indices = list(range(first_stripe, last_stripe + 1))
        with ThreadPoolExecutor(max_workers=max(1, len(stripe_indices))) as pool:
            list(pool.map(_write_one, stripe_indices))

        self._bump_total_size(path, layout, end)

    def read_range(self, path: str, offset: int, length: int) -> bytes:
        meta = self._read_lov(path)
        layout = StripeLayout(meta["stripe_count"], meta["stripe_size"], meta["stripe_offset"])
        size = layout.stripe_size
        total = meta["total_size"]
        length = max(0, min(length, total - offset))
        if length <= 0:
            return b""
        end = offset + length
        first_stripe = offset // size
        last_stripe = (end - 1) // size

        def _read_one(stripe_index: int) -> tuple[int, bytes]:
            stripe_start = stripe_index * size
            ost = self.osts[layout.ost_for_stripe(stripe_index, len(self.osts))]
            key = _stripe_key(path, stripe_index)
            try:
                chunk = ost.get(key)
            except ObjectNotFound:
                chunk = b""
            lo = max(offset, stripe_start) - stripe_start
            hi = min(end, stripe_start + size) - stripe_start
            piece = chunk[lo:hi] if lo < len(chunk) else b""
            if len(piece) < (hi - lo):
                piece = piece + b"\x00" * ((hi - lo) - len(piece))
            return stripe_index, piece

        stripe_indices = list(range(first_stripe, last_stripe + 1))
        with ThreadPoolExecutor(max_workers=max(1, len(stripe_indices))) as pool:
            results = dict(pool.map(_read_one, stripe_indices))
        return b"".join(results[i] for i in stripe_indices)

    def stripe_locations(self, path: str) -> list[int]:
        """The `lfs getstripe` analogue: which OST index each stripe of
        this file physically lives on, in stripe order."""
        meta = self._read_lov(path)
        layout = StripeLayout(meta["stripe_count"], meta["stripe_size"], meta["stripe_offset"])
        n = _stripe_count_for_size(meta["total_size"], layout.stripe_size)
        return [layout.ost_for_stripe(i, len(self.osts)) for i in range(n)]


_STRIPE_CAS_MAX_ATTEMPTS = 1000


def _read_object_with_version(ost: StorageEngine, key: str) -> tuple[int, bytes]:
    """(version, content) as one consistent snapshot; version 0 = absent.
    Reads the version before and after the content and retries if they
    differ, so a concurrent writer landing in between can't pair stale
    content with a newer version (which would let the CAS succeed while
    silently discarding that writer's update)."""
    while True:
        try:
            before = ost.stat(key).version
            content = ost.get(key)
            after = ost.stat(key).version
        except ObjectNotFound:
            try:
                ost.stat(key)
            except ObjectNotFound:
                return 0, b""
            continue  # created while we looked; take a fresh snapshot
        if before == after:
            return before, content


def _stripe_count_for_size(total_size: int, stripe_size: int) -> int:
    if total_size <= 0:
        return 0
    return (total_size + stripe_size - 1) // stripe_size
