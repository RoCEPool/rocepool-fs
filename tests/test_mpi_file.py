"""Tests for mpi/mpi_file.py: real multi-rank collective I/O (real socket
Communicators, same as test_mpi_comm.py) against a shared StripedStorage
file backed by real MemoryEngine OSTs. Checks both that the assembled
file is byte-correct AND that the ranks' regions actually landed on
multiple distinct OSTs -- i.e. that this is genuinely parallel, striped
I/O, not an accidental single-OST bottleneck that happens to still be
correct.
"""
import threading
import unittest

from fs.striping import StripeLayout, StripedStorage
from mpi.comm import Communicator
from mpi.mpi_file import MPIFile
from roce.loopback_transport import LoopbackTransport
from storage_engine.memory_engine import MemoryEngine


def _make_comm_cluster(n: int, base_port: int) -> list[Communicator]:
    addresses = [f"127.0.0.1:{base_port + i}" for i in range(n)]
    return [Communicator(rank=r, addresses=addresses, transport=LoopbackTransport()) for r in range(n)]


def _run_on_all_ranks(comms, fn):
    results = [None] * len(comms)
    errors = [None] * len(comms)

    def _worker(i, comm):
        try:
            results[i] = fn(comm)
        except Exception as exc:  # noqa: BLE001
            errors[i] = exc

    threads = [threading.Thread(target=_worker, args=(i, c)) for i, c in enumerate(comms)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)
    for e in errors:
        if e is not None:
            raise e
    return results


class CollectiveFileIOTests(unittest.TestCase):
    NUM_RANKS = 4
    REGION_SIZE = 100

    def setUp(self):
        self.comms = _make_comm_cluster(self.NUM_RANKS, base_port=18950)
        self.osts = [MemoryEngine() for _ in range(4)]
        self.storage = StripedStorage(self.osts, default_layout=StripeLayout(stripe_count=4, stripe_size=32))

    def tearDown(self):
        for c in self.comms:
            c.close()

    def test_each_rank_writes_its_own_region_collectively_and_everyone_reads_it_back(self):
        region = self.REGION_SIZE

        def _work(comm):
            f = MPIFile.open(comm, self.storage, "shared.bin")
            f.set_view(disp=comm.rank * region)
            pattern = bytes([comm.rank]) * region
            f.write_at_all(0, pattern)
            # Every rank reads the WHOLE file back collectively, after
            # every rank's write_at_all has completed (write_at_all's own
            # trailing barrier guarantees that for the writer; each rank
            # doing its own read_at_all adds the cross-rank synchronization).
            whole = MPIFile(comm, self.storage, "shared.bin")
            return whole.read_at_all(0, region * self.NUM_RANKS)

        results = _run_on_all_ranks(self.comms, _work)

        expected = b"".join(bytes([r]) * region for r in range(self.NUM_RANKS))
        for i, got in enumerate(results):
            self.assertEqual(got, expected, f"rank {i} saw a different final file")

        # Confirm this was genuinely striped across OSTs, not accidentally
        # serialized onto one -- with stripe_size=32 and a 400-byte file
        # over 4 OSTs, every OST must have received real bytes.
        for i, ost in enumerate(self.osts):
            self.assertGreater(ost.total_bytes(), 0, f"OST {i} received no data")

    def test_individual_write_at_then_collective_read_at_all(self):
        # Only rank 0 writes (individual, not collective); every rank
        # still needs to see the same final content once they all
        # collectively read.
        def _work(comm):
            f = MPIFile.open(comm, self.storage, "single_writer.bin")
            if comm.rank == 0:
                f.write_at(0, b"written by rank zero only")
            return f.read_at_all(0, 26)

        results = _run_on_all_ranks(self.comms, _work)
        for got in results:
            self.assertEqual(got, b"written by rank zero only")

    def test_get_size_all_reflects_every_ranks_prior_write(self):
        region = self.REGION_SIZE

        def _work(comm):
            f = MPIFile.open(comm, self.storage, "sized.bin")
            f.set_view(disp=comm.rank * region)
            f.write_at_all(0, b"x" * region)
            return f.get_size_all()

        results = _run_on_all_ranks(self.comms, _work)
        self.assertEqual(results, [region * self.NUM_RANKS] * self.NUM_RANKS)


if __name__ == "__main__":
    unittest.main()
