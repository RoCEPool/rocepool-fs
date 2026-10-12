"""Regression tests for fs/fuse_lowlevel.py's write-flush path.

Focused on `_flush_handle`'s range coalescing (see its docstring): the
kernel FUSE driver splits any write larger than max_write (128 KiB, set in
`_h_init`) into many small FUSE_WRITE requests, so one IOR-style large
sequential write arrives as hundreds of `dirty_ranges` fragments. Each
fragment used to be replayed through `PooledFileSystem.write_range`
individually -- a CAS read-modify-write of the WHOLE object -- making a
large sequential write O(fragments x file_size). Found live: a 64 MiB
single-shared-file IOR run pegged a CPU core and never finished.

These tests drive `_flush_handle` directly (the wire-protocol parsing in
`_h_write`/`_h_read` is exercised elsewhere via the real kernel mount, see
examples/mount_real_kernel_demo.py and tools/bench_ior.py) so they run
without a real /dev/fuse.
"""
import os
import unittest

from fs.fuse_lowlevel import FuseLowLevelDaemon, _FileHandle, _MAX_COALESCED_RUN
from fs.posix_api import PooledFileSystem
from storage_engine.memory_engine import MemoryEngine


class _CountingEngine(MemoryEngine):
    """Wraps MemoryEngine to count backend round-trips, so a test can
    assert the coalescing actually collapsed many small ranges into few
    CAS operations, not just that the final bytes came out right."""

    def __init__(self):
        super().__init__()
        self.get_calls = 0
        self.put_calls = 0

    def get(self, key):
        self.get_calls += 1
        return super().get(key)

    def put(self, key, data, *, expected_version=None):
        self.put_calls += 1
        return super().put(key, data, expected_version=expected_version)


def _daemon():
    engine = _CountingEngine()
    pfs = PooledFileSystem(engine)
    # _flush_handle only needs self.pfs; the fd is never touched by it.
    fd = os.open("/dev/null", os.O_WRONLY)
    daemon = FuseLowLevelDaemon(fd, pfs)
    return daemon, engine


class CoalescingCollapsesManySmallFragmentsTests(unittest.TestCase):
    def test_many_sequential_fragments_become_one_backend_round_trip(self):
        daemon, engine = _daemon()
        handle = _FileHandle("f", bytearray())
        # Simulate the kernel splitting one big sequential write into 256
        # x 4 KiB FUSE_WRITE fragments, exactly as it does at 128 KiB for a
        # multi-megabyte IOR transfer, just scaled down for a fast test.
        chunk = 4096
        count = 256
        payload = bytes((i % 251) for i in range(chunk * count))
        for i in range(count):
            offset = i * chunk
            handle.dirty_ranges.append((offset, payload[offset:offset + chunk]))

        daemon._flush_handle(handle)

        self.assertEqual(engine.put_calls, 1, "256 contiguous fragments should merge into one put()")
        self.assertEqual(pfs_get(daemon, "f"), payload)

    def test_out_of_order_but_contiguous_fragments_still_merge(self):
        daemon, engine = _daemon()
        handle = _FileHandle("f", bytearray())
        payload = bytes(range(200)) * 20  # 4000 bytes
        chunk = 400
        offsets = list(range(0, len(payload), chunk))
        # Shuffle the arrival order -- the kernel doesn't guarantee
        # in-order delivery of split fragments under reordering/retries.
        for offset in reversed(offsets):
            handle.dirty_ranges.append((offset, payload[offset:offset + chunk]))

        daemon._flush_handle(handle)

        self.assertEqual(engine.put_calls, 1)
        self.assertEqual(pfs_get(daemon, "f"), payload)

    def test_disjoint_ranges_stay_separate_calls_but_content_is_correct(self):
        daemon, engine = _daemon()
        handle = _FileHandle("f", bytearray())
        handle.dirty_ranges.append((0, b"AAAA"))
        handle.dirty_ranges.append((1000, b"BBBB"))  # far away -- must NOT merge with the first

        daemon._flush_handle(handle)

        self.assertEqual(engine.put_calls, 2, "non-adjacent ranges must not be merged into one write")
        content = pfs_get(daemon, "f")
        self.assertEqual(content[0:4], b"AAAA")
        self.assertEqual(content[1000:1004], b"BBBB")
        self.assertEqual(len(content), 1004)

    def test_overlapping_fragments_prefer_the_later_bytes(self):
        daemon, engine = _daemon()
        handle = _FileHandle("f", bytearray())
        # Same on-disk order FUSE would deliver two overlapping writes in:
        # first a filler, then a later write that overlaps and should win
        # over the overlapping portion.
        handle.dirty_ranges.append((0, b"XXXXXXXXXX"))
        handle.dirty_ranges.append((5, b"YYYYY"))

        daemon._flush_handle(handle)

        self.assertEqual(pfs_get(daemon, "f"), b"XXXXXYYYYY")


class LargeRunIsSplitAtTheFrameCapTests(unittest.TestCase):
    def test_a_run_bigger_than_the_cap_is_split_into_bounded_write_range_calls(self):
        # A run a bit over 2x the cap should become 3 write_range pieces,
        # none larger than the cap -- this is what keeps a single coalesced
        # write from exceeding RoCETransport's max_frame_bytes when the
        # backend is a PoolNode reached over the network (see
        # _flush_handle's docstring).
        daemon, engine = _daemon()
        handle = _FileHandle("f", bytearray())
        size = int(_MAX_COALESCED_RUN * 2.3)
        payload = bytes((i % 251) for i in range(size))
        # Deliver it as 64 KiB fragments, like the kernel would.
        frag = 64 * 1024
        for offset in range(0, size, frag):
            handle.dirty_ranges.append((offset, payload[offset:offset + frag]))

        daemon._flush_handle(handle)

        self.assertEqual(engine.put_calls, 3)
        self.assertEqual(pfs_get(daemon, "f"), payload)


def pfs_get(daemon: FuseLowLevelDaemon, path: str) -> bytes:
    return daemon.pfs.read_file(path)


if __name__ == "__main__":
    unittest.main()
