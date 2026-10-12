"""The cluster, written down once: cluster.conf lists every host (any number),
and `sudo ./run.sh` takes it from there -- no --peers, no questions.

    [login]
    user = admin            # a login with sudo on every host (needed once:
    password = ...          #   to give this host's root a key on the others)
    [nodes]
    # ssh address     RoCE IP           [hostname]
    10.0.0.11      10.10.0.11     node01
    10.0.0.12     10.10.0.12    node02
    ...

What `prepare` does, in parallel over all hosts (32 at a time):
  1. this host = the line whose RoCE IP is one of this host's addresses;
     every other line is a peer
  2. root SSH from this host to every peer without a password: hosts where
     it already works are left alone; on the others this host's root public
     key is installed into root's authorized_keys by logging in once as
     `user` with `password` (SSH_ASKPASS, `sudo -S` -- no sshpass, nothing
     typed). No password in the file and a terminal: asked once, for all.
  3. hostnames: MPI needs every host's to differ. Where two hosts share one
     and the file names them, the duplicates are renamed to the file's name
     (hostnamectl) -- the file's names are the cluster's names.
  4. prints `PEERS root@<ssh>=<roce ip>,...` for run.sh / roce_onsite.py

The file keeps a password: prepare makes it readable by root only (0600)
and says so; once every host has the key the password can be removed.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import getpass
import ipaddress
import os
import re
import shlex
import socket
import stat
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEY = "/root/.ssh/id_ed25519"
PARALLEL = 32
SSH_BATCH = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new"]


class ConfError(Exception):
    pass


def parse(text: str) -> dict:
    conf = {"user": None, "password": None, "nodes": []}
    section = None
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.split("#", 1)[0].strip()
        if section == "login" and re.match(r"\s*password\s*=", raw):
            line = raw.strip()  # a password may contain '#'

        if not line:
            continue
        m = re.fullmatch(r"\[(\w+)\]", line)
        if m:
            section = m.group(1).lower()
            if section not in ("login", "nodes"):
                raise ConfError(f"line {n}: unknown section [{section}] (login, nodes)")
            continue
        if section == "login":
            if "=" not in line:
                raise ConfError(f"line {n}: expected key = value in [login]")
            k, v = (x.strip() for x in line.split("=", 1))
            if k not in ("user", "password"):
                raise ConfError(f"line {n}: unknown key {k!r} in [login] (user, password)")
            conf[k] = v or None
        elif section == "nodes":
            parts = line.split()
            if len(parts) not in (2, 3):
                raise ConfError(f"line {n}: expected '<ssh address> <RoCE IP> [hostname]', got {raw.strip()!r}")
            try:
                ipaddress.IPv4Address(parts[1])
            except ValueError:
                raise ConfError(f"line {n}: {parts[1]!r} is not an IPv4 address (the RoCE IP)") from None
            if len(parts) == 3 and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,62}", parts[2]):
                raise ConfError(f"line {n}: {parts[2]!r} is not a valid hostname")
            conf["nodes"].append({"ssh": parts[0], "ip": parts[1], "name": parts[2] if len(parts) == 3 else None})
        else:
            raise ConfError(f"line {n}: outside a section ([login] / [nodes])")
    ips = [x["ip"] for x in conf["nodes"]]
    dup = sorted({i for i in ips if ips.count(i) > 1})
    if dup:
        raise ConfError(f"RoCE IP listed twice: {dup}")
    names = [x["name"] for x in conf["nodes"] if x["name"]]
    dupn = sorted({x for x in names if names.count(x) > 1})
    if dupn:
        raise ConfError(f"hostname listed twice: {dupn}")
    if not conf["nodes"]:
        raise ConfError("no hosts in [nodes]")
    return conf


def local_addresses() -> set[str]:
    out = set()
    try:
        r = subprocess.run(["ip", "-4", "-o", "addr", "show"], capture_output=True, text=True, timeout=10)
        out |= set(re.findall(r"inet (\d+\.\d+\.\d+\.\d+)/", r.stdout))
    except (OSError, subprocess.SubprocessError):
        pass
    try:
        r = subprocess.run(["hostname", "-I"], capture_output=True, text=True, timeout=10)
        out |= set(r.stdout.split())
    except (OSError, subprocess.SubprocessError):
        pass
    return out


def split_local(conf: dict, addrs: set[str]) -> tuple[dict, list[dict]]:
    mine = [x for x in conf["nodes"] if x["ip"] in addrs]
    if len(mine) != 1:
        raise ConfError(f"this host must be exactly one line of [nodes] (its RoCE IP among {sorted(addrs)}); "
                        f"matched {len(mine)}")
    return mine[0], [x for x in conf["nodes"] if x is not mine[0]]


# ---- root keys ----------------------------------------------------------------
def ensure_key(run=subprocess.run) -> str:
    if not os.path.exists(KEY):
        os.makedirs(os.path.dirname(KEY), mode=0o700, exist_ok=True)
        run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", f"rocepool@{socket.gethostname()}", "-f", KEY],
            check=True)
    with open(KEY + ".pub") as f:
        return f.read().strip()


def root_ok(host: str, run=subprocess.run) -> bool:
    try:
        return run(["ssh", *SSH_BATCH, f"root@{host}", "true"], capture_output=True, timeout=30).returncode == 0
    except subprocess.TimeoutExpired:
        return False


def install_key(host: str, user: str, password: str, pub: str, run=subprocess.run) -> str | None:
    """None on success, else why not. Logs in as `user` with `password`
    (OpenSSH's SSH_ASKPASS, forced: no terminal, no sshpass) and appends the
    key to root's authorized_keys through `sudo -S` (the password on stdin)."""
    fd, askpass = tempfile.mkstemp(prefix="rp-askpass-", suffix=".sh")
    try:
        with os.fdopen(fd, "w") as f:
            f.write("#!/bin/sh\nprintf '%s\\n' \"$ROCEPOOL_SSH_PASSWORD\"\n")
        os.chmod(askpass, stat.S_IRWXU)
        env = dict(os.environ, SSH_ASKPASS=askpass, SSH_ASKPASS_REQUIRE="force", DISPLAY=os.environ.get("DISPLAY", ":0"),
                   ROCEPOOL_SSH_PASSWORD=password)
        script = ("mkdir -p /root/.ssh && chmod 700 /root/.ssh && touch /root/.ssh/authorized_keys && "
                  f"(grep -qxF {shlex.quote(pub)} /root/.ssh/authorized_keys || "
                  f"echo {shlex.quote(pub)} >> /root/.ssh/authorized_keys) && chmod 600 /root/.ssh/authorized_keys")
        remote = f"sh -c {shlex.quote(script)}" if user == "root" else \
            f"sudo -S -p '' sh -c {shlex.quote(script)}"
        try:
            r = run(["ssh", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=10",
                     "-o", "NumberOfPasswordPrompts=1", "-o", "PreferredAuthentications=publickey,keyboard-interactive,password",
                     f"{user}@{host}", remote], input=password + "\n", capture_output=True, text=True, env=env,
                    timeout=60)
        except subprocess.TimeoutExpired:
            return "no answer within 60 s"
        if r.returncode:
            err = (r.stderr or r.stdout).strip().splitlines()
            return (err[-1] if err else f"exit {r.returncode}")[:300]
        return None
    finally:
        os.unlink(askpass)


def hostname_of(host: str, run=subprocess.run) -> str | None:
    try:
        r = run(["ssh", *SSH_BATCH, f"root@{host}", "hostname"], capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return None
    return r.stdout.strip().splitlines()[-1].strip() if r.returncode == 0 and r.stdout.strip() else None


def set_hostname(host: str | None, name: str, run=subprocess.run) -> bool:
    cmd = ["hostnamectl", "set-hostname", name]
    if host is None:
        return run(cmd, capture_output=True).returncode == 0
    return run(["ssh", *SSH_BATCH, f"root@{host}", " ".join(cmd)], capture_output=True, timeout=60).returncode == 0


def _err(msg: str) -> None:  # progress to stderr: stdout carries only the PEERS line
    print(msg, file=sys.stderr, flush=True)


def prepare(conf: dict, addrs: set[str], password_prompt=None, run=subprocess.run, log=_err) -> list[tuple[str, str]]:
    me, peers = split_local(conf, addrs)
    log(f"   本机：{me['ip']}；其余 {len(peers)} 台")
    if not peers:
        return []
    with cf.ThreadPoolExecutor(min(PARALLEL, len(peers))) as ex:
        ok = dict(zip((p["ssh"] for p in peers), ex.map(lambda p: root_ok(p["ssh"], run), peers)))
    need = [p for p in peers if not ok[p["ssh"]]]
    if need:
        user, password = conf.get("user"), conf.get("password")
        if not user or not password:
            if password_prompt is None:
                raise ConfError(f"root cannot log in without a password on {len(need)} host(s) "
                                f"({', '.join(p['ssh'] for p in need[:5])}{' ...' if len(need) > 5 else ''}), and "
                                "cluster.conf has no [login] user / password to install this host's key with")
            user, password = password_prompt(user, len(need))
        pub = ensure_key(run)
        log(f"   {len(need)} 台还不能以 root 免密登录：以 {user} 登录一次装公钥（并行）")
        with cf.ThreadPoolExecutor(min(PARALLEL, len(need))) as ex:
            errs = dict(zip((p["ssh"] for p in need), ex.map(lambda p: install_key(p["ssh"], user, password, pub, run),
                                                             need)))
        with cf.ThreadPoolExecutor(min(PARALLEL, len(need))) as ex:
            after = dict(zip((p["ssh"] for p in need), ex.map(lambda p: root_ok(p["ssh"], run), need)))
        bad = {h: errs[h] or "key installed but root still cannot log in (sshd PermitRootLogin no? set it to "
                              "prohibit-password)" for h in after if not after[h]}
        if bad:
            raise ConfError("root SSH could not be set up on:\n" + "\n".join(f"  {h}: {e}" for h, e in bad.items()))
    # hostnames: every host's must differ (MPI)
    with cf.ThreadPoolExecutor(min(PARALLEL, len(peers))) as ex:
        names = dict(zip((p["ssh"] for p in peers), ex.map(lambda p: hostname_of(p["ssh"], run), peers)))
    names[None] = socket.gethostname()
    by = {None: me, **{p["ssh"]: p for p in peers}}
    values = list(names.values())
    clash = {h: n for h, n in names.items() if n and values.count(n) > 1}
    if clash:
        unnamed = [h for h in clash if not by[h]["name"]]
        if unnamed:
            raise ConfError(f"hosts share a hostname {sorted(set(clash.values()))} and cluster.conf gives no name for "
                            f"{['this host' if h is None else h for h in unnamed]}: add a third column (hostname)")
        for h in clash:
            if by[h]["name"] != names[h]:
                if not set_hostname(h, by[h]["name"], run):
                    raise ConfError(f"could not set the hostname of {h or 'this host'} to {by[h]['name']}")
                log(f"   主机名：{h or '本机'} {names[h]} -> {by[h]['name']}（与其它节点重名，按 cluster.conf 改名）")
    return [(f"root@{p['ssh']}", p["ip"]) for p in peers]


def secure(path: str, log=_err) -> None:
    st = os.stat(path)
    if st.st_mode & 0o077:
        os.chmod(path, 0o600)
        log(f"   {path} 含密码：已改为仅 root 可读（600）")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("cmd", choices=("prepare", "check"))
    ap.add_argument("--conf", default=os.path.join(ROOT, "cluster.conf"))
    ap.add_argument("--peers", help="instead of a file: user@host=roce_ip,... (run.sh --peers)")
    a = ap.parse_args()
    try:
        if a.peers:
            nodes, users = [{"ssh": None, "ip": "<this host>", "name": None}], set()
            for item in a.peers.split(","):
                if not item.strip():
                    continue
                if "=" not in item:
                    raise ConfError(f"--peers entries are user@host=roce_ip, got {item!r}")
                who, ip = item.strip().split("=", 1)
                user, host = who.split("@", 1) if "@" in who else ("root", who)
                users.add(user)
                nodes.append({"ssh": host, "ip": ip, "name": None})
            nonroot = sorted(users - {"root"})
            conf = {"user": nonroot[0] if nonroot else None, "password": None, "nodes": nodes}
            addrs = {"<this host>"}
        else:
            with open(a.conf) as f:
                conf = parse(f.read())
            addrs = local_addresses()
        if a.cmd == "check":
            me, peers = split_local(conf, addrs)
            print(f"ok: this host {me['ip']}, {len(peers)} peers")
            return 0
        if conf.get("password") and not a.peers:
            secure(a.conf)

        def ask(user, n):
            if not sys.stdin.isatty():
                raise ConfError(f"{n} host(s) need this host's key installed and cluster.conf has no password "
                                "(and there is no terminal to ask on)")
            u = user
            if not u:
                sys.stderr.write("   有 sudo 权限的登录用户名（所有节点相同，例如 admin）：")
                sys.stderr.flush()
                u = sys.stdin.readline().strip()
            return u, getpass.getpass(f"   {u} 的密码（所有节点相同，只用这一次）：")
        peers = prepare(conf, addrs, password_prompt=ask)
    except (ConfError, OSError, subprocess.SubprocessError) as exc:
        print(f"   cluster.conf: {exc}", file=sys.stderr)
        return 1
    if not a.peers:
        print("LOCAL " + split_local(conf, addrs)[0]["ip"])
    print("PEERS " + ",".join(f"{p}={ip}" for p, ip in peers))
    return 0


if __name__ == "__main__":
    sys.exit(main())
