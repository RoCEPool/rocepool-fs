"""MPI-IO-style collective file operations (the `MPI_File_open`/
`MPI_File_write_at_all`/`MPI_File_set_view` family) layered on
`fs/striping.py`'s `StripedStorage` (the Lustre-like OST fan-out) and
`mpi/comm.py`'s `Communicator` -- the same pairing real HPC systems use
(MPI-IO's ROMIO implementation ships an `ad_lustre` driver specifically
because collective I/O and OST striping are meant to work together: each
rank's I/O should land on the OSTs its region of the file actually
stripes across, in parallel, rather than being serialized through one
process).

Scope, stated plainly (same standard as the rest of this project):

  - `write_at`/`read_at` are **individual** (one rank, no synchronization
    with the others) and go straight to `StripedStorage.write_range`/
    `read_range`, which already only touches the stripes/OSTs the
    requested byte range overlaps -- so a single rank's I/O is already
    parallel across OSTs when its region spans more than one stripe.
  - `write_at_all`/`read_at_all` are **collective**: every rank in the
    communicator must call them together (an MPI-IO requirement this
    class does not detect if violated, exactly like `Communicator`'s
    collectives). What they add over the individual calls is
    synchronization -- a barrier before (so no rank starts touching the
    file until every rank has arrived, avoiding torn reads of
    in-progress writes from an earlier collective) and a barrier after
    (so no rank returns, and no rank's *next* operation on the file
    starts, until every rank's I/O for this call has actually landed).
    Each rank still writes/reads its own region directly, so this is
    real parallel I/O across whichever OSTs those regions stripe over.
    What this is explicitly **not**: ROMIO's actual two-phase I/O
    optimization, which additionally reshuffles data in memory between
    ranks first so that a small number of "aggregator" processes each
    perform one large contiguous OST-aligned I/O instead of every rank
    doing its own smaller, possibly-unaligned one. That reshuffling step
    is a real, separate optimization on top of correct collective I/O,
    not included here.
  - `set_view(disp, etype_size=1)` supports only a flat, contiguous view
    (every subsequent `offset` is `disp + offset * etype_size`) -- not
    MPI's general derived-datatype filetypes (strided/nested/indexed
    views). A per-rank contiguous view is exactly what you'd use to give
    each rank a distinct non-overlapping slice of a shared file, which is
    what the collective tests below exercise.

TESTED how: tests/test_mpi_file.py runs a real multi-rank cluster (real
Communicators over real sockets, same as test_mpi_comm.py) collectively
writing and reading a single shared `StripedStorage` file backed by
several real `MemoryEngine` OSTs, and checks both byte-level correctness
of the assembled file AND that the ranks' data actually landed spread
across multiple distinct OSTs (not just that round-tripping happened to
work).
"""
from __future__ import annotations

from fs.striping import StripeLayout, StripedStorage
from mpi.comm import Communicator


class MPIFile:
    """One rank's open handle onto a shared, striped file. Every rank in
    `comm` must construct its own `MPIFile.open(...)` pointing at the
    same `path` on the same (shared) `StripedStorage` for a collective
    call to make sense -- `StripedStorage` here plays the role a shared
    Lustre mount plays in real MPI-IO: every rank sees the same
    underlying OSTs.
    """

    def __init__(self, comm: Communicator, storage: StripedStorage, path: str):
        self.comm = comm
        self.storage = storage
        self.path = path
        self._view_disp = 0
        self._view_etype_size = 1

    @classmethod
    def open(cls, comm: Communicator, storage: StripedStorage, path: str,
              *, layout: StripeLayout | None = None) -> "MPIFile":
        """Collective open: rank 0 creates the file (with `layout`, or
        `storage`'s default) if it doesn't exist yet; every rank barriers
        before returning, so no rank proceeds to I/O before the file's
        layout metadata (the LOV record) definitely exists.
        """
        comm.barrier()
        if comm.rank == 0 and not storage.exists(path):
            storage.put(path, b"", layout=layout)
        comm.barrier()
        return cls(comm, storage, path)

    def set_view(self, disp: int, etype_size: int = 1) -> None:
        self._view_disp = disp
        self._view_etype_size = etype_size

    def _absolute(self, offset: int) -> int:
        return self._view_disp + offset * self._view_etype_size

    # -- individual I/O --
    def write_at(self, offset: int, data: bytes) -> None:
        self.storage.write_range(self.path, self._absolute(offset), data)

    def read_at(self, offset: int, length: int) -> bytes:
        return self.storage.read_range(self.path, self._absolute(offset), length)

    # -- collective I/O --
    def write_at_all(self, offset: int, data: bytes) -> None:
        self.comm.barrier()
        self.write_at(offset, data)
        self.comm.barrier()

    def read_at_all(self, offset: int, length: int) -> bytes:
        self.comm.barrier()
        result = self.read_at(offset, length)
        self.comm.barrier()
        return result

    def get_size(self) -> int:
        return self.storage.total_size(self.path)

    def get_size_all(self) -> int:
        """Collective: every rank barriers, then reads the (by then
        guaranteed up to date) size -- use this rather than `get_size()`
        right after a `write_at_all` on a DIFFERENT rank's region, so you
        aren't racing that rank's still-in-flight write_at_all."""
        self.comm.barrier()
        return self.get_size()
