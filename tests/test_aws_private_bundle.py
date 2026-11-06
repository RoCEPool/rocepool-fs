"""tools/aws_private_bundle.py: a private bundle reaches the runner only in
encrypted form and its reports come back the same way. Round trips both
ways, and the refusals: another key, a changed payload, no entry.sh, a
member that would land outside the tree."""
import base64
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "tools", "aws_private_bundle.py")
sys.path.insert(0, os.path.join(ROOT, "tools"))
import aws_private_bundle as apb  # noqa: E402


def tool(*args):
    return subprocess.run([sys.executable, TOOL, *args], capture_output=True, text=True, timeout=300)


class PrivateBundleTests(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.td, True)
        self.runner = os.path.join(self.td, "runner")
        self.owner = os.path.join(self.td, "owner")
        for d in (self.runner, self.owner):
            self.assertEqual(tool("keygen", "--dir", d).returncode, 0)
        self.src = os.path.join(self.td, "my-package")
        os.makedirs(os.path.join(self.src, "sub"))
        with open(os.path.join(self.src, "entry.sh"), "w") as f:
            f.write("#!/bin/bash\necho hi\n")
        self.payload = os.urandom(300000)
        with open(os.path.join(self.src, "sub", "data.bin"), "wb") as f:
            f.write(self.payload)

    def seal(self, out="sealed"):
        out = os.path.join(self.td, out)
        r = tool("seal-bundle", "--runner-pub", os.path.join(self.runner, "public.pem"), "--src", self.src,
                 "--reply-pub", os.path.join(self.owner, "public.pem"), "--out", out)
        self.assertEqual(r.returncode, 0, r.stderr)
        return out

    def test_round_trip_both_ways(self):
        out = self.seal()
        self.assertNotIn(self.payload[:64], open(os.path.join(out, "bundle.tar.enc"), "rb").read(), "encrypted")
        tree = os.path.join(self.td, "tree")
        r = tool("open", "--dir", self.runner, "--bundle", out, "--into", tree)
        self.assertEqual(r.returncode, 0, r.stderr)
        with open(os.path.join(tree, "private", "sub", "data.bin"), "rb") as f:
            self.assertEqual(f.read(), self.payload)
        self.assertTrue(os.path.isfile(os.path.join(tree, "private", "entry.sh")), "named private/ on the cluster")
        # the reports, for the owner's key
        rep = os.path.join(self.td, "reports")
        os.makedirs(rep)
        with open(os.path.join(rep, "r.json"), "w") as f:
            json.dump({"ok": 1}, f)
        stem = os.path.join(self.td, "out", "reports")
        self.assertEqual(tool("seal", "--src", rep, "--to", os.path.join(out, "reply.pub.pem"), "--out", stem).returncode, 0)
        got = os.path.join(self.td, "got")
        r = tool("open-reports", "--key", os.path.join(self.owner, "key.pem"), "--stem", stem, "--into", got)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.load(open(os.path.join(got, "reports", "r.json"))), {"ok": 1})

    def test_another_key_cannot_open_it(self):
        out = self.seal()
        r = tool("open", "--dir", self.owner, "--bundle", out, "--into", os.path.join(self.td, "t"))
        self.assertNotEqual(r.returncode, 0)
        self.assertFalse(os.path.exists(os.path.join(self.td, "t", "private")))

    def test_a_changed_payload_is_refused(self):
        out = self.seal()
        p = os.path.join(out, "bundle.tar.enc")
        raw = bytearray(open(p, "rb").read())
        raw[len(raw) // 2] ^= 0xFF
        open(p, "wb").write(bytes(raw))
        r = tool("open", "--dir", self.runner, "--bundle", out, "--into", os.path.join(self.td, "t"))
        self.assertNotEqual(r.returncode, 0)
        self.assertFalse(os.path.exists(os.path.join(self.td, "t", "private", "entry.sh")))

    def test_a_bundle_without_entry_sh_is_refused_at_sealing(self):
        os.unlink(os.path.join(self.src, "entry.sh"))
        r = tool("seal-bundle", "--runner-pub", os.path.join(self.runner, "public.pem"), "--src", self.src,
                 "--reply-pub", os.path.join(self.owner, "public.pem"), "--out", os.path.join(self.td, "x"))
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("entry.sh", r.stderr)

    def test_a_member_outside_the_tree_is_refused(self):
        evil = os.path.join(self.td, "evil")
        os.makedirs(evil)
        stem = os.path.join(self.td, "evil-sealed", "bundle")
        os.makedirs(os.path.dirname(stem))
        # hand-made payload with ../escape
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as t:
            data = b"x"
            ti = tarfile.TarInfo("../escape.txt")
            ti.size = len(data)
            t.addfile(ti, io.BytesIO(data))
        plain = os.path.join(self.td, "p.tgz")
        open(plain, "wb").write(buf.getvalue())
        pw = "a" * 64
        apb._run(["openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-iter", "200000", "-salt", "-in", plain,
                  "-out", stem + ".tar.enc", "-pass", "stdin"], input=pw.encode())
        blob = json.dumps({"pass": pw, "sha256": apb._sha256(apb.Path(plain)), "name": "private"}).encode()
        wrapped = apb._run(["openssl", "pkeyutl", "-encrypt", "-pubin", "-inkey",
                            os.path.join(self.runner, "public.pem"), *apb.OAEP], input=blob).stdout
        open(stem + ".key.enc", "wb").write(base64.b64encode(wrapped))
        r = tool("open", "--dir", self.runner, "--bundle", os.path.dirname(stem), "--into", os.path.join(self.td, "t"))
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("unsafe member", r.stderr)
        self.assertFalse(os.path.exists(os.path.join(self.td, "escape.txt")))


if __name__ == "__main__":
    unittest.main()
