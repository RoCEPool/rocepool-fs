"""Client-side parallel request fan-out: many independent requests in
flight at once, each on its OWN short-lived connection.

Why this shape and not pipelining on one reused connection: holding a
RoCETransport connection open and reusing it was tried (see
docs/BENCHMARK_COMPARISON.md, "Connection pooling: tried, reverted") and
exposed a race in the softverbs emulator where a connection that sat idle
and then got busy again could stop ACKing. Every call here still goes
through the unchanged, connect-per-request `Transport.send_request()`
path; the concurrency comes from issuing N of those at the same time from
a shared worker pool, so N round trips overlap instead of adding up. The
C transport already handles one thread per connection on both ends, so
this needs no change underneath.

`max_per_peer` bounds how many of those concurrent connections target
any single peer at once, so one hot OST/MDT can't be flooded with
hundreds of simultaneous connects by a single client.
"""
from __future__ import annotations

import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, TypeVar

from roce.protocol import Frame
from roce.transport_base import Transport

T = TypeVar("T")
R = TypeVar("R")

DEFAULT_MAX_WORKERS = 16
DEFAULT_MAX_PER_PEER = 4


class RequestBatcher:
    def __init__(self, *, max_workers: int = DEFAULT_MAX_WORKERS, max_per_peer: int = DEFAULT_MAX_PER_PEER):
        self.max_workers = max_workers
        self.max_per_peer = max_per_peer
        # One persistent pool per batcher: spinning up a fresh
        # ThreadPoolExecutor on every call (what fs/striping.py does per
        # write_range) costs a thread create/join per stripe on the hot path.
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="rb")
        self._peer_sems: dict[str, threading.BoundedSemaphore] = defaultdict(
            lambda: threading.BoundedSemaphore(self.max_per_peer))
        self._sem_lock = threading.Lock()

    def _sem(self, peer: str) -> threading.BoundedSemaphore:
        with self._sem_lock:
            return self._peer_sems[peer]

    def map(self, fn: Callable[[T], R], items: Iterable[T], *, peer_of: Callable[[T], str] | None = None) -> list[R]:
        """Run `fn(item)` for every item concurrently; results come back in
        input order. Exceptions propagate (the first one raised, after all
        tasks have finished, so no task is left running against state the
        caller is about to tear down). With `peer_of`, each task first
        takes that peer's per-peer slot."""
        items = list(items)
        if not items:
            return []
        if len(items) == 1:
            return [fn(items[0])]  # no point paying a thread hop for one call

        def _run(item: T) -> R:
            if peer_of is None:
                return fn(item)
            sem = self._sem(peer_of(item))
            with sem:
                return fn(item)

        futures = [self._pool.submit(_run, it) for it in items]
        results, first_exc = [], None
        for f in futures:
            try:
                results.append(f.result())
            except BaseException as exc:  # noqa: BLE001 -- re-raised below
                results.append(None)
                if first_exc is None:
                    first_exc = exc
        if first_exc is not None:
            raise first_exc
        return results

    def submit(self, fn: Callable[[], R], *, peer: str | None = None):
        """Fire one task in the background (e.g. a readahead fetch); returns
        its Future. Holds the peer's slot for the task's duration."""
        def _run() -> R:
            if peer is None:
                return fn()
            with self._sem(peer):
                return fn()
        return self._pool.submit(_run)

    def batch_send(self, transport: Transport, requests: list[tuple[str, Frame]], *,
                   timeout_s: float = 5.0) -> list[Frame]:
        """Send every (peer, frame) concurrently; responses in input order."""
        return self.map(lambda pf: transport.send_request(pf[0], pf[1], timeout_s=timeout_s),
                        requests, peer_of=lambda pf: pf[0])

    def close(self) -> None:
        self._pool.shutdown(wait=True)
