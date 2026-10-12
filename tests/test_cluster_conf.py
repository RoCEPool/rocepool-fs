"""tools/cluster_conf.py: the file format, which line is this host, and
prepare() against a fake ssh -- key installed only where root cannot log in
yet (askpass + sudo -S, password never on a command line), duplicate
hostnames renamed to the file's names, the PEERS list for run.sh."""
import os
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import cluster_conf as cc  # noqa: E402

GOOD = """
# comment
[login]
user = admin
password = p#ss word

[nodes]
10.0.0.1  192.168.10.1  n01
10.0.0.2  192.168.10.2  n02   # trailing comment
10.0.0.3  192.168.10.3
"""


class R:
    def __init__(self, rc=0, out="", err=""):
        self.returncode, self.stdout, self.stderr = rc, out, err


class FakeCluster:
    """root_ok fails until a key was installed; hostnames per host."""

    def __init__(self, keyed, names):
        self.keyed, self.names, self.calls, self.lock = set(keyed), dict(names), [], threading.Lock()

    def __call__(self, cmd, **kw):
        with self.lock:
            self.calls.append((cmd, kw))
        if cmd[0] == "ssh-keygen":
            return R()
        if cmd[0] == "hostnamectl":
            self.names[None] = cmd[-1]
            return R()
        assert cmd[0] == "ssh", cmd
        dest, remote = cmd[-2], cmd[-1]
        user, host = dest.split("@", 1)
        if user != "root":  # the password login
            assert kw["env"]["SSH_ASKPASS_REQUIRE"] == "force"
            assert kw["input"] == kw["env"]["ROCEPOOL_SSH_PASSWORD"] + "\n"
            assert remote.startswith("sudo -S -p ''"), remote
            if kw["env"]["ROCEPOOL_SSH_PASSWORD"] != "p#ss word":
                return R(255, err="Permission denied (password).")
            with self.lock:
                self.keyed.add(host)
            return R()
        if host not in self.keyed:
            return R(255, err="Permission denied (publickey)")
        if remote == "true":
            return R()
        if remote == "hostname":
            return R(out=self.names[host] + "\n")
        if remote.startswith("hostnamectl set-hostname "):
            self.names[host] = remote.split()[-1]
            return R()
        raise AssertionError(remote)


class ParseTests(unittest.TestCase):
    def test_good(self):
        c = cc.parse(GOOD)
        self.assertEqual(c["user"], "admin")
        self.assertEqual(c["password"], "p#ss word")
        self.assertEqual([n["ip"] for n in c["nodes"]], ["192.168.10.1", "192.168.10.2", "192.168.10.3"])
        self.assertEqual([n["name"] for n in c["nodes"]], ["n01", "n02", None])

    def test_many_hosts(self):
        lines = "\n".join(f"10.1.{i // 250}.{i % 250 + 1} 192.168.{i // 250}.{i % 250 + 1} node{i:03d}"
                          for i in range(100))
        self.assertEqual(len(cc.parse("[nodes]\n" + lines)["nodes"]), 100)

    def test_errors(self):
        for text, msg in (("[nodes]\nh 1.2.3\n", "not an IPv4"),
                          ("[nodes]\nh 1.2.3.4\ng 1.2.3.4\n", "RoCE IP listed twice"),
                          ("[nodes]\nh 1.2.3.4 a\ng 1.2.3.5 a\n", "hostname listed twice"),
                          ("[nodes]\nh\n", "expected"),
                          ("[foo]\n", "unknown section"),
                          ("h 1.2.3.4\n", "outside a section"),
                          ("[login]\nuser = x\n", "no hosts"),
                          ("[login]\nport = 2\n[nodes]\nh 1.2.3.4\n", "unknown key"),
                          ("[nodes]\nh 1.2.3.4 bad_name\n", "valid hostname")):
            with self.assertRaisesRegex(cc.ConfError, msg):
                cc.parse(text)

    def test_split_local(self):
        c = cc.parse(GOOD)
        me, peers = cc.split_local(c, {"127.0.0.1", "10.0.0.2", "192.168.10.2"})
        self.assertEqual(me["name"], "n02")
        self.assertEqual([p["ssh"] for p in peers], ["10.0.0.1", "10.0.0.3"])
        with self.assertRaisesRegex(cc.ConfError, "exactly one"):
            cc.split_local(c, {"127.0.0.1"})


class PrepareTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.old_key = cc.KEY
        cc.KEY = os.path.join(self.tmp, "id_ed25519")
        with open(cc.KEY + ".pub", "w") as f:
            f.write("ssh-ed25519 AAAA test\n")
        open(cc.KEY, "w").close()
        self.log = []

    def tearDown(self):
        cc.KEY = self.old_key
        subprocess.run(["rm", "-rf", self.tmp])

    def nodes(self, n):
        text = "[login]\nuser = admin\npassword = p#ss word\n[nodes]\n" + "\n".join(
            f"h{i} 192.168.10.{i} node{i}" for i in range(1, n + 1))
        return cc.parse(text)

    def test_installs_only_where_needed_and_lists_peers(self):
        conf = self.nodes(40)
        fake = FakeCluster(keyed={f"h{i}" for i in range(2, 21)},
                           names={f"h{i}": f"node{i}" for i in range(2, 41)})
        peers = cc.prepare(conf, {"192.168.10.1"}, run=fake, log=self.log.append)
        self.assertEqual(peers, [(f"root@h{i}", f"192.168.10.{i}") for i in range(2, 41)])
        logins = [c for c, kw in fake.calls if c[-2].startswith("admin@")]
        self.assertEqual(sorted(c[-2] for c in logins), sorted(f"admin@h{i}" for i in range(21, 41)))
        for c, _ in fake.calls:  # the password is never on a command line
            self.assertNotIn("p#ss", " ".join(c))
        self.assertTrue(any("20 台" in m for m in self.log), self.log)

    def test_no_password_needed_when_keys_exist(self):
        conf = cc.parse("[nodes]\nh1 192.168.10.1\nh2 192.168.10.2\n")
        fake = FakeCluster(keyed={"h2"}, names={"h2": "b"})
        with unittest.mock.patch.object(cc.socket, "gethostname", return_value="a"):
            self.assertEqual(cc.prepare(conf, {"192.168.10.1"}, run=fake, log=self.log.append),
                             [("root@h2", "192.168.10.2")])

    def test_missing_password_asks_once(self):
        conf = cc.parse("[login]\nuser = admin\n[nodes]\nh1 192.168.10.1\nh2 192.168.10.2\nh3 192.168.10.3\n")
        fake = FakeCluster(keyed=set(), names={"h2": "b", "h3": "c"})
        asked = []

        def prompt(user, n):
            asked.append((user, n))
            return user, "p#ss word"
        with unittest.mock.patch.object(cc.socket, "gethostname", return_value="a"):
            cc.prepare(conf, {"192.168.10.1"}, password_prompt=prompt, run=fake, log=self.log.append)
        self.assertEqual(asked, [("admin", 2)])
        with self.assertRaisesRegex(cc.ConfError, "no \\[login\\] user / password"):
            cc.prepare(conf, {"192.168.10.1"}, run=FakeCluster(set(), {}), log=self.log.append)

    def test_wrong_password_reported_per_host(self):
        conf = self.nodes(3)
        conf["password"] = "wrong"
        fake = FakeCluster(keyed={"h2"}, names={"h2": "node2", "h3": "node3"})
        with self.assertRaisesRegex(cc.ConfError, "h3: Permission denied"):
            cc.prepare(conf, {"192.168.10.1"}, run=fake, log=self.log.append)

    def test_duplicate_hostnames_renamed_to_conf_names(self):
        conf = self.nodes(3)
        fake = FakeCluster(keyed={"h2", "h3"}, names={"h2": "ubuntu", "h3": "ubuntu", None: "ubuntu"})
        with unittest.mock.patch.object(cc.socket, "gethostname", return_value="ubuntu"):
            cc.prepare(conf, {"192.168.10.1"}, run=fake, log=self.log.append)
        self.assertEqual(fake.names, {"h2": "node2", "h3": "node3", None: "node1"})

    def test_duplicate_without_conf_name_is_an_error(self):
        conf = cc.parse("[nodes]\nh1 192.168.10.1 a\nh2 192.168.10.2\nh3 192.168.10.3\n")
        fake = FakeCluster(keyed={"h2", "h3"}, names={"h2": "x", "h3": "x"})
        with unittest.mock.patch.object(cc.socket, "gethostname", return_value="a"):
            with self.assertRaisesRegex(cc.ConfError, "add a third column"):
                cc.prepare(conf, {"192.168.10.1"}, run=fake, log=self.log.append)


class CliTests(unittest.TestCase):
    def test_check_and_secure(self):
        tmp = tempfile.mkdtemp()
        try:
            p = os.path.join(tmp, "cluster.conf")
            addrs = sorted(cc.local_addresses())
            if not addrs:
                self.skipTest("no IPv4 address on this host")
            ip = addrs[0]
            with open(p, "w") as f:
                f.write(f"[login]\nuser = x\npassword = y\n[nodes]\nlocalhost {ip}\n")
            os.chmod(p, 0o644)
            r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "cluster_conf.py"), "check", "--conf", p],
                               capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn("0 peers", r.stdout)
            cc.secure(p, log=lambda m: None)
            self.assertEqual(os.stat(p).st_mode & 0o777, 0o600)
        finally:
            subprocess.run(["rm", "-rf", tmp])

    def test_example_parses(self):
        with open(os.path.join(ROOT, "cluster.conf.example")) as f:
            c = cc.parse(f.read())
        self.assertEqual(len(c["nodes"]), 2)
        self.assertIsNone(c["password"])


import unittest.mock  # noqa: E402

if __name__ == "__main__":
    unittest.main()
