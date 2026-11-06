#!/usr/bin/env python3
"""A private bundle for an aws-test run: code that must not be published
(a partner's integration, say), run on the rented cluster, with its reports
returned encrypted to the bundle's owner. Nothing secret is stored anywhere:

  runner   keygen          a fresh RSA-4096 key pair for this run; only the
                           public half leaves the runner (a branch)
  owner    seal-bundle     the bundle (a directory) encrypted for that key,
                           plus the owner's own public key for the reply
  runner   open            decrypts it into the tree (it ships to the head
                           node with everything else)
  runner   seal            the run's reports encrypted for the owner's key
  owner    open-reports    decrypts them

Each payload is AES-256 (openssl enc, PBKDF2) under a random 256-bit
passphrase; the passphrase and the payload's sha256 are wrapped with RSA-OAEP
(SHA-256) for the recipient. A payload whose sha256 differs after decryption
is refused. Needs only python3, tar and the openssl CLI.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

OAEP = ["-pkeyopt", "rsa_padding_mode:oaep", "-pkeyopt", "rsa_oaep_md:sha256", "-pkeyopt", "rsa_mgf1_md:sha256"]


def _run(argv: list[str], **kw) -> subprocess.CompletedProcess:
    r = subprocess.run(argv, capture_output=True, **kw)
    if r.returncode != 0:
        raise SystemExit(f"{argv[0]} {argv[1] if len(argv) > 1 else ''} failed: "
                         f"{(r.stderr or b'').decode(errors='replace')[-400:]}")
    return r


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def keygen(d: Path) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    os.chmod(d, 0o700)
    key, pub = d / "key.pem", d / "public.pem"
    _run(["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:4096", "-out", str(key)])
    os.chmod(key, 0o600)
    _run(["openssl", "pkey", "-in", str(key), "-pubout", "-out", str(pub)])
    return pub


def seal(src: Path, to_pub: Path, out: Path) -> None:
    """src (a file or a directory, tarred) -> out.tar.enc + out.key.enc"""
    seal_as(src, src.name, to_pub, out)


def seal_as(src: Path, name: str, to_pub: Path, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as td:
        plain = Path(td) / "payload.tar.gz"
        with tarfile.open(plain, "w:gz") as t:
            t.add(str(src), arcname=name)
        passphrase = secrets.token_hex(32)
        _run(["openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-iter", "200000", "-salt", "-in", str(plain),
              "-out", f"{out}.tar.enc", "-pass", "stdin"], input=passphrase.encode())
        blob = json.dumps({"pass": passphrase, "sha256": _sha256(plain), "name": name}).encode()
        wrapped = _run(["openssl", "pkeyutl", "-encrypt", "-pubin", "-inkey", str(to_pub), *OAEP], input=blob).stdout
        Path(f"{out}.key.enc").write_bytes(base64.b64encode(wrapped) + b"\n")


def unseal(stem: Path, key: Path, into: Path) -> str:
    """stem.tar.enc + stem.key.enc -> extracted under `into`; returns the top name."""
    wrapped = base64.b64decode(Path(f"{stem}.key.enc").read_bytes())
    blob = json.loads(_run(["openssl", "pkeyutl", "-decrypt", "-inkey", str(key), *OAEP], input=wrapped).stdout)
    into.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as td:
        plain = Path(td) / "payload.tar.gz"
        _run(["openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2", "-iter", "200000", "-in", f"{stem}.tar.enc",
              "-out", str(plain), "-pass", "stdin"], input=blob["pass"].encode())
        if _sha256(plain) != blob["sha256"]:
            raise SystemExit("the payload does not match its sha256: refusing it")
        with tarfile.open(plain, "r:gz") as t:
            for m in t.getmembers():
                p = Path(m.name)
                if p.is_absolute() or ".." in p.parts or m.issym() or m.islnk():
                    raise SystemExit(f"unsafe member in the payload: {m.name}")
            if hasattr(tarfile, "data_filter"):      # Python 3.12+: refuse anything odd a second time
                t.extractall(into, filter="data")
            else:
                t.extractall(into)
    return blob["name"]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    k = sub.add_parser("keygen", help="runner: a key pair for this run")
    k.add_argument("--dir", required=True)
    sb = sub.add_parser("seal-bundle", help="owner: encrypt a bundle directory for a runner's key")
    sb.add_argument("--runner-pub", required=True)
    sb.add_argument("--src", required=True, help="the directory; it becomes <tree>/private/ on the cluster")
    sb.add_argument("--reply-pub", required=True, help="the owner's public key: the reports come back for it")
    sb.add_argument("--out", required=True, help="a directory: bundle.tar.enc, bundle.key.enc, reply.pub.pem")
    o = sub.add_parser("open", help="runner: decrypt the bundle into <into>/private")
    o.add_argument("--dir", required=True, help="the keygen directory")
    o.add_argument("--bundle", required=True, help="the directory seal-bundle wrote")
    o.add_argument("--into", required=True)
    s = sub.add_parser("seal", help="runner: encrypt the reports for the owner")
    s.add_argument("--src", required=True)
    s.add_argument("--to", required=True, help="reply.pub.pem")
    s.add_argument("--out", required=True, help="stem: <stem>.tar.enc + <stem>.key.enc")
    orp = sub.add_parser("open-reports", help="owner: decrypt the reports")
    orp.add_argument("--key", required=True)
    orp.add_argument("--stem", required=True)
    orp.add_argument("--into", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "keygen":
        print(keygen(Path(a.dir)))
    elif a.cmd == "seal-bundle":
        src = Path(a.src).resolve()
        if not (src / "entry.sh").is_file():
            raise SystemExit(f"{src}/entry.sh is missing: the cluster runs it")
        out = Path(a.out)
        # sealed under the name "private", whatever the directory is called
        seal_as(src, "private", Path(a.runner_pub), out / "bundle")
        (out / "reply.pub.pem").write_bytes(Path(a.reply_pub).read_bytes())
        print(json.dumps({"bundle": str(out), "bytes": (out / "bundle.tar.enc").stat().st_size}))
    elif a.cmd == "open":
        d = Path(a.dir)
        name = unseal(Path(a.bundle) / "bundle", d / "key.pem", Path(a.into))
        if name != "private" or not (Path(a.into) / "private" / "entry.sh").is_file():
            raise SystemExit("the bundle has no private/entry.sh")
        print(Path(a.into) / "private")
    elif a.cmd == "seal":
        seal(Path(a.src), Path(a.to), Path(a.out))
    elif a.cmd == "open-reports":
        print(Path(a.into) / unseal(Path(a.stem), Path(a.key), Path(a.into)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
