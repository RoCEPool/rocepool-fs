"""roce/native/wal.c: the servers' write-ahead log (IO500 rule 6) --
pipelined group commit, direct writes into pre-written zeros, and the
replay's handling of what a crash leaves behind (tests/wal_driver.c)."""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NATIVE = os.path.join(ROOT, "roce", "native")


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class WalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bin_dir = tempfile.mkdtemp()
        cls.drv = os.path.join(cls.bin_dir, "wal_driver")
        subprocess.run(["gcc", "-O2", "-Wall", "-pthread", "-I", NATIVE, os.path.join(ROOT, "tests", "wal_driver.c"),
                        os.path.join(NATIVE, "wal.c"), "-o", cls.drv], check=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.bin_dir, ignore_errors=True)

    def setUp(self):
        self.d = tempfile.mkdtemp(dir=ROOT)  # the repository's file system (ext4/xfs: direct I/O works)
        self.addCleanup(shutil.rmtree, self.d, ignore_errors=True)
        self.log = os.path.join(self.d, "t.log")

    MODE = {}  # unset: the calibration decides

    def run_drv(self, *args, env=None, ok=True):
        r = subprocess.run([self.drv, *map(str, args)], capture_output=True, text=True, timeout=300,
                           env={**os.environ, "ROCEPOOL_LOG_PREFILL_MIB": "4", **self.MODE, **(env or {})})
        if ok:
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        return r

    def dump(self):
        out = self.run_drv("dump", self.log).stdout.strip().splitlines()
        self.assertTrue(out[-1].startswith("replayed "), out[-3:])
        return out[:-1]

    def check_records(self, lines, threads, per):
        """every writer's records, each once, in its own order, intact"""
        got = {}
        for ln in lines:
            name, plen, same = ln.split()
            self.assertEqual(same, "1", ln)
            t, i = name.split(":")
            got.setdefault(t, []).append(int(i))
            k = int(t[1:])
            self.assertEqual(int(plen), (k * 7919 + int(i) * 104729) % 4000, ln)
        self.assertEqual(sorted(got), sorted(f"t{k}" for k in range(threads)))
        for t, idx in got.items():
            self.assertEqual(idx, list(range(per)), t)

    def test_pipelined_commits_replay_in_order(self):
        st = self.run_drv("write", self.log, 16, 300).stdout
        f = dict(zip(st.split()[::2], map(int, st.split()[1::2])))
        self.assertEqual(f["records"], 16 * 300)
        if self.MODE.get("ROCEPOOL_LOG_PIPELINE") == "4":
            self.assertGreater(f["inflight_max"], 1, st)   # commits overlapped
        if self.MODE.get("ROCEPOOL_LOG_DIRECT") == "1":
            self.assertEqual(f["direct"], 1, st)          # this file system takes direct I/O
            self.assertGreater(f["prefill"], 0, st)
        self.check_records(self.dump(), 16, 300)
        # a second life appends after the first one's records
        self.run_drv("write", self.log, 4, 100)
        lines = self.dump()
        self.assertEqual(len(lines), 16 * 300 + 4 * 100)

    def test_without_direct_io_and_one_commit_at_a_time(self):
        for env in ({"ROCEPOOL_LOG_DIRECT": "0", "ROCEPOOL_LOG_PIPELINE": "4"}, {"ROCEPOOL_LOG_PIPELINE": "1"}):
            os.path.exists(self.log) and os.remove(self.log)
            st = self.run_drv("write", self.log, 8, 200, env=env).stdout
            self.assertIn("records 1600", st)
            if "ROCEPOOL_LOG_DIRECT" in env:
                self.assertIn("direct 0", st)
            else:
                self.assertIn("inflight_max 1", st)
            self.check_records(self.dump(), 8, 200)

    def test_commits_a_crash_left_after_a_gap_never_come_back(self):
        """Commits in flight can reach the drive out of order: a later one
        whole, an earlier one not at all. The replay stops at the gap; the
        later one (never acknowledged) is cleared, so new records filling
        the gap are not followed by it."""
        self.run_drv("write", self.log, 2, 50)
        for gap in (4096, 300 << 10, 3 << 20):
            r = self.run_drv("stale", self.log, gap, 20)
            self.assertIn("stale at", r.stdout)
            lines = self.dump()
            self.assertFalse([ln for ln in lines if ln.startswith("stale")], lines[-3:])
            # new records over the gap, more than the gap held: still no stale ones
            self.run_drv("write", self.log, 4, 200)
            lines = self.dump()
            self.assertFalse([ln for ln in lines if ln.startswith("stale")])
        self.assertEqual(len([ln for ln in lines if ln.startswith("t")]), 2 * 50 + 3 * 4 * 200)
        # the worst case: the next life writes exactly the gap, so its last
        # commit ends where the leftovers begin -- contiguous, valid, and
        # never acknowledged; only clearing them keeps them out
        for blocks in (1, 3, 40):
            self.run_drv("stale", self.log, blocks * 4096, 20)
            self.run_drv("exact", self.log, 4088, blocks)  # 4096-byte records, one commit each
            lines = self.dump()
            self.assertFalse([ln for ln in lines if ln.startswith("stale")], (blocks, lines[-3:]))
            self.assertEqual(len([ln for ln in lines if ln.startswith("e:")]), sum((1, 3, 40)[:(1, 3, 40).index(blocks) + 1]))
        # no gap: a whole commit right after the durable ones -- written,
        # only its replies never went out -- is part of the log
        self.run_drv("stale", self.log, 0, 20)
        self.assertEqual(len([ln for ln in self.dump() if ln.startswith("stale")]), 20)

    def test_a_torn_last_commit_keeps_the_prefix(self):
        self.run_drv("write", self.log, 4, 100)
        n = len(self.dump())
        size = os.path.getsize(self.log)
        with open(self.log, "rb") as f:
            data = f.read()
        pos, last = 0, None  # the last record that is not a pad
        while pos + 8 <= len(data):
            ln, crc = int.from_bytes(data[pos:pos + 4], "little"), int.from_bytes(data[pos + 4:pos + 8], "little")
            if ln == 0 and crc == 0:
                break
            if not ln & 0x80000000:
                last = (pos, ln)
            pos += 8 + (ln & 0x7fffffff)
        at, ln = last
        with open(self.log, "r+b") as f:  # its second half garbage: a commit torn by a crash
            f.seek(at + 8 + ln // 2)
            f.write(os.urandom(ln - ln // 2))
        lines = self.dump()
        self.assertEqual(len(lines), n - 1)
        self.assertEqual(os.path.getsize(self.log), size)  # the zeros ahead stay
        self.run_drv("write", self.log, 2, 10)
        self.assertEqual(len(self.dump()), n - 1 + 20)

    def test_the_drive_decides_the_commit(self):
        """unset, the mode is measured on the log's drive and reported; set,
        it is as told"""
        if self.MODE:
            self.skipTest("the calibration: WalTests itself")
        r = self.run_drv("write", self.log, 4, 50)
        self.assertIn("commits/s measured here", r.stderr)
        self.assertRegex(r.stderr, r"-> (direct writes|appends), [14] in flight")
        self.assertFalse(os.path.exists(self.log + ".calibrate"))
        self.check_records(self.dump(), 4, 50)
        r = self.run_drv("write", self.log, 4, 50, env={"ROCEPOOL_LOG_PIPELINE": "2", "ROCEPOOL_LOG_DIRECT": "0"})
        self.assertNotIn("measured here", r.stderr)
        self.assertIn("direct 0 pipeline 2", r.stdout)

    def test_every_pad_size_around_a_block(self):
        """a commit is padded to its block: an 8-byte pad (empty payload),
        one under 8 (a block more), none at all -- replay reads past each
        (an 8-byte pad once stopped it, dropping everything after)"""
        for payload in (4080, 4081, 4087, 4088, 4089, 100, 8184):
            os.path.exists(self.log) and os.remove(self.log)
            self.run_drv("exact", self.log, payload, 5)
            self.run_drv("exact", self.log, payload, 5)  # a second life after the first's pads
            self.assertEqual(len(self.dump()), 10, payload)

    def test_an_oversized_record_is_refused(self):
        self.assertIn("lsn 0", self.run_drv("big", self.log).stdout)

    @unittest.skipUnless(os.geteuid() == 0, "needs root for a tmpfs")
    def test_on_tmpfs(self):
        """direct I/O or not (tmpfs takes it from Linux 6.6), the records
        are all there"""
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        subprocess.run(["mount", "-t", "tmpfs", "-o", "size=64m", "tmpfs", d], check=True)
        self.addCleanup(subprocess.run, ["umount", "-l", d])
        self.log = os.path.join(d, "t.log")
        st = self.run_drv("write", self.log, 8, 100).stdout
        self.assertIn("records 800", st)
        self.check_records(self.dump(), 8, 100)


class WalAppendTests(WalTests):
    MODE = {"ROCEPOOL_LOG_DIRECT": "0", "ROCEPOOL_LOG_PIPELINE": "1"}


class WalDirectTests(WalTests):
    MODE = {"ROCEPOOL_LOG_DIRECT": "1", "ROCEPOOL_LOG_PIPELINE": "1"}


class WalDirectPipelinedTests(WalTests):
    MODE = {"ROCEPOOL_LOG_DIRECT": "1", "ROCEPOOL_LOG_PIPELINE": "4"}


if __name__ == "__main__":
    unittest.main()
