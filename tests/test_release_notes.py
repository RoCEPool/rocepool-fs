"""tools/release_notes.py: every release's notes come from its CHANGELOG.md
section; every version in VERSION's history has a section."""
import os
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


class ReleaseNotesTests(unittest.TestCase):
    def test_section_and_notes(self):
        from tools.release_notes import notes, section
        cl = "# Changelog\n\n## 2b\n\n- second\n\n## 2a\n\n- first\n"
        self.assertEqual(section(cl, "2b"), "- second")
        self.assertEqual(section(cl, "2a"), "- first")
        self.assertEqual(section(cl, "9z"), "")
        n = notes("2a", "abc123", "Owner/Repo", cl, image=True)
        self.assertTrue(n.startswith("- first"))
        self.assertIn("rocepool-full-2a.tar.gz", n)
        self.assertIn("ghcr.io/owner/repo:2a", n)
        self.assertNotIn("ghcr.io", notes("2a", "abc", "Owner/Repo", cl, image=False))

    def test_current_version_has_a_changelog_section(self):
        from tools.release_notes import section
        with open(os.path.join(ROOT, "VERSION")) as f:
            v = f.read().strip()
        with open(os.path.join(ROOT, "CHANGELOG.md")) as f:
            self.assertTrue(section(f.read(), v), f"CHANGELOG.md has no '## {v}' section")

    def test_every_released_version_in_history_has_a_section(self):
        if not os.path.isdir(os.path.join(ROOT, ".git")):
            self.skipTest("not a git checkout")
        from tools.release_notes import section
        log = subprocess.run(["git", "log", "--format=%H", "--", "VERSION"], cwd=ROOT, capture_output=True, text=True)
        with open(os.path.join(ROOT, "CHANGELOG.md")) as f:
            cl = f.read()
        for c in log.stdout.split():
            v = subprocess.run(["git", "show", f"{c}:VERSION"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
            self.assertTrue(section(cl, v), f"CHANGELOG.md has no '## {v}' section")


if __name__ == "__main__":
    unittest.main()
