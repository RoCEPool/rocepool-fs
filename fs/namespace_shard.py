"""Which MDT owns a path's metadata entry.

Two placement rules; every client of a cluster must use the same one
(ROCEPOOL_DIR_STRIPE, default 1):

  striped (ROCEPOOL_DIR_STRIPE=1, default) -- Lustre DNE phase-2 "striped
    directory" with every MDT as a stripe: a path's entry lives on the MDT
    chosen by hashing the WHOLE path, so one directory's entries are spread
    over all MDTs. readdir(d) / child_count(d) fan out to every MDT (in
    parallel) and merge. This removes the hot-MDT ceiling of mdtest-hard-
    style workloads (a million files in one directory: with the parent rule
    every one of their creates, stats and unlinks hit ONE MDT and its lock).

  by parent (ROCEPOOL_DIR_STRIPE=0) -- the DNE phase-1 "remote directory"
    idea: hash the PARENT directory, so every entry of one directory sits on
    the same MDT and readdir is one RPC. Directories spread across MDTs,
    files inside one directory do not.

Neither rebalances when the MDT list changes: the list is fixed at mount
time, same as PoolNode's node list, and its order must match everywhere.
"""
from __future__ import annotations

import hashlib
import os


def normalize(path: str) -> str:
    return "/".join(p for p in path.strip("/").split("/") if p not in ("", "."))


def parent_of(path: str) -> str:
    return "/".join(path.split("/")[:-1])


def name_of(path: str) -> str:
    return path.split("/")[-1]


def dir_stripe_default() -> bool:
    return os.environ.get("ROCEPOOL_DIR_STRIPE", "1") not in ("0", "false", "no", "off")


class NamespaceShard:
    def __init__(self, mdts: list[str], striped: bool | None = None):
        if not mdts:
            raise ValueError("need at least one MDT")
        self.mdts = list(mdts)  # order matters and must match on every client
        self.striped = dir_stripe_default() if striped is None else striped

    def _pick(self, s: str) -> str:
        digest = hashlib.sha256(s.encode()).digest()
        return self.mdts[int.from_bytes(digest[:8], "big") % len(self.mdts)]

    def mdt_for(self, path: str) -> str:
        """The MDT holding `path`'s own entry."""
        path = normalize(path)
        return self._pick(path) if self.striped else self._pick(parent_of(path))

    def mdts_for_children(self, dir_path: str) -> list[str]:
        """The MDTs holding entries INSIDE `dir_path` (all of them when striped)."""
        return list(self.mdts) if self.striped else [self._pick(normalize(dir_path))]

    def mdt_for_children(self, dir_path: str) -> str:
        """By-parent rule only: the one MDT holding the entries inside `dir_path`."""
        if self.striped:
            raise ValueError("striped directories: a directory's entries span every MDT (mdts_for_children)")
        return self._pick(normalize(dir_path))
