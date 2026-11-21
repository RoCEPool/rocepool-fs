"""tools/aws_push_reports.sh: an AWS run's directory published to branch
aws-results/<run id> -- run again and again (mid-run write-backs, then the
end), never with the run's SSH key, keys, cluster.conf, the shipped tree or
files of 20 MB or more."""
import os
import shutil
import subprocess
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "tools", "aws_push_reports.sh")


@unittest.skipUnless(shutil.which("git"), "needs git")
class PushReportsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.remote = os.path.join(self.tmp, "remote.git")
        subprocess.run(["git", "init", "-q", "--bare", self.remote], check=True)
        self.run_dir = os.path.join(self.tmp, "aws_runs", "gh-1")
        os.makedirs(os.path.join(self.run_dir, "sub"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, name, data=b"x"):
        with open(os.path.join(self.run_dir, name), "wb") as f:
            f.write(data)

    def push(self, *extra):
        return subprocess.run(["bash", TOOL, "gh-1", self.run_dir, *extra], capture_output=True, text=True,
                              env=dict(os.environ, REMOTE_URL=self.remote), timeout=120)

    def published(self):
        out = subprocess.run(["git", "--git-dir", self.remote, "ls-tree", "-r", "--name-only", "aws-results/gh-1"],
                             capture_output=True, text=True, check=True).stdout
        return sorted(out.split())

    def test_publishes_reports_without_secrets_and_can_run_again(self):
        for name in ("kvcache_e2e_progress.json", "aws_run.out", "state.json", "sub/host0_kernel.txt",
                     "ssh_key", "run.pem", "id_ed25519", "cluster.conf", "rocepool.tar.gz"):
            self.write(name)
        self.write("big.bin", b"\0" * (21 << 20))
        r = self.push()
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertEqual(self.published(), ["gh-1/aws_run.out", "gh-1/kvcache_e2e_progress.json",
                                            "gh-1/state.json", "gh-1/sub/host0_kernel.txt"])
        # later: more files, and an extra one from outside the run directory
        self.write("kvcache_e2e_report.json")
        extra = os.path.join(self.tmp, "aws_run.log")
        with open(extra, "w") as f:
            f.write("log")
        r = self.push(extra)
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("gh-1/kvcache_e2e_report.json", self.published())
        self.assertIn("gh-1/aws_run.log", self.published())
        self.assertNotIn("gh-1/ssh_key", self.published())

    def test_no_run_directory_is_not_an_error(self):
        r = subprocess.run(["bash", TOOL, "gh-2", os.path.join(self.tmp, "nope")], capture_output=True, text=True,
                           env=dict(os.environ, REMOTE_URL=self.remote), timeout=60)
        self.assertEqual(r.returncode, 0)
        self.assertIn("no ", r.stdout)


if __name__ == "__main__":
    unittest.main()
