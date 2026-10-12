"""Tests for mpi/comm.py, against real multi-rank clusters: each rank is
a real Communicator with its own real loopback-TCP server (same pattern
as tests/test_pool_node.py's `_make_cluster()`), so every send/recv/
barrier/bcast/gather/allgather/reduce below is a genuine socket round
trip between independent server threads, not an in-process stand-in.
"""
import threading
import unittest

from mpi.comm import Communicator
from roce.loopback_transport import LoopbackTransport


def _make_comm_cluster(n: int, *, base_port: int = 18900) -> list[Communicator]:
    addresses = [f"127.0.0.1:{base_port + i}" for i in range(n)]
    return [Communicator(rank=r, addresses=addresses, transport=LoopbackTransport()) for r in range(n)]


def _run_on_all_ranks(comms: list[Communicator], fn) -> list:
    """Runs fn(comm) concurrently on every rank's own thread (required for
    anything beyond point-to-point between two specific ranks, since e.g.
    a barrier deadlocks if every rank doesn't call it "at the same time").
    Returns each rank's return value, or raises the first exception.
    """
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
        t.join(timeout=10)
    for e in errors:
        if e is not None:
            raise e
    return results


class PointToPointTests(unittest.TestCase):
    def setUp(self):
        self.comms = _make_comm_cluster(3, base_port=18900)

    def tearDown(self):
        for c in self.comms:
            c.close()

    def test_send_recv_real_round_trip(self):
        def _sender(comm):
            if comm.rank == 0:
                comm.send(1, "greeting", b"hello from rank 0")

        def _receiver(comm):
            if comm.rank == 1:
                return comm.recv(0, "greeting")
            return None

        # Start the receiver's recv() in a thread first (it blocks), then send.
        result_holder = {}

        def _recv_thread():
            result_holder["msg"] = self.comms[1].recv(0, "greeting")

        t = threading.Thread(target=_recv_thread)
        t.start()
        self.comms[0].send(1, "greeting", b"hello from rank 0")
        t.join(timeout=5)
        self.assertEqual(result_holder["msg"], b"hello from rank 0")

    def test_recv_is_fifo_per_src_tag(self):
        for i in range(3):
            self.comms[0].send(1, "seq", f"msg-{i}".encode())
        received = [self.comms[1].recv(0, "seq") for _ in range(3)]
        self.assertEqual(received, [b"msg-0", b"msg-1", b"msg-2"])

    def test_recv_timeout_raises(self):
        with self.assertRaises(TimeoutError):
            self.comms[1].recv(0, "never-sent", timeout_s=0.3)


class CollectiveTests(unittest.TestCase):
    def setUp(self):
        self.comms = _make_comm_cluster(4, base_port=18920)

    def tearDown(self):
        for c in self.comms:
            c.close()

    def test_barrier_releases_every_rank(self):
        results = _run_on_all_ranks(self.comms, lambda c: (c.barrier(), c.rank)[1])
        self.assertEqual(sorted(results), [0, 1, 2, 3])

    def test_bcast_delivers_roots_data_to_everyone(self):
        def _work(comm):
            payload = b"broadcast-payload" if comm.rank == 2 else None
            return comm.bcast(payload, root=2)

        results = _run_on_all_ranks(self.comms, _work)
        self.assertEqual(results, [b"broadcast-payload"] * 4)

    def test_gather_collects_every_ranks_data_in_rank_order(self):
        def _work(comm):
            mine = f"rank-{comm.rank}".encode()
            return comm.gather(mine, root=0)

        results = _run_on_all_ranks(self.comms, _work)
        self.assertEqual(results[0], [b"rank-0", b"rank-1", b"rank-2", b"rank-3"])
        for r in (1, 2, 3):
            self.assertIsNone(results[r])

    def test_allgather_gives_every_rank_the_full_list(self):
        def _work(comm):
            return comm.allgather(f"r{comm.rank}".encode())

        results = _run_on_all_ranks(self.comms, _work)
        expected = [b"r0", b"r1", b"r2", b"r3"]
        for r in results:
            self.assertEqual(r, expected)

    def test_reduce_sum_and_allreduce_sum(self):
        def _work_reduce(comm):
            return comm.reduce_sum(comm.rank + 1, root=0)  # 1+2+3+4 = 10

        results = _run_on_all_ranks(self.comms, _work_reduce)
        self.assertEqual(results[0], 10)
        for r in (1, 2, 3):
            self.assertIsNone(results[r])

        def _work_allreduce(comm):
            return comm.allreduce_sum(comm.rank + 1)

        results2 = _run_on_all_ranks(self.comms, _work_allreduce)
        self.assertEqual(results2, [10, 10, 10, 10])


class WireupTests(unittest.TestCase):
    def test_send_to_a_rank_that_is_not_listening_yet_is_not_lost(self):
        # Separate-process launch order is arbitrary: rank 1 may send before
        # rank 0 has bound its server. Found running ranks as separate
        # processes across two network nodes (tools/run_crossover_rdma.py).
        import time as _time
        addrs = ["127.0.0.1:18991", "127.0.0.1:18992"]
        rank1 = Communicator(rank=1, addresses=addrs, transport=LoopbackTransport())
        errors = []

        def _late_sender():
            try:
                rank1.send(0, "early", b"sent before rank 0 existed")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t = threading.Thread(target=_late_sender)
        t.start()
        _time.sleep(0.5)  # rank 0 comes up well after the send started
        rank0 = Communicator(rank=0, addresses=addrs, transport=LoopbackTransport())
        t.join(timeout=10)
        try:
            self.assertEqual(errors, [])
            self.assertEqual(rank0.recv(1, "early", timeout_s=5), b"sent before rank 0 existed")
        finally:
            rank0.close()
            rank1.close()


if __name__ == "__main__":
    unittest.main()
