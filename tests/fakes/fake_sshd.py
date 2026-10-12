"""A minimal stand-in for sshd, for rehearsing multi-host runs on a simulated
cluster (tools/netns_cluster.py) where no real sshd is available: one
daemon per node, started INSIDE that node's network + mount + UTS
namespaces, so a command it runs is on that node -- reached over the
node's own IP, like `ssh root@node cmd`. Client: tests/fakes/ssh.

What it keeps from ssh, because the automation depends on it:
  - the command runs through `bash -c` with a fresh login-style
    environment (nothing leaks from the caller's environment)
  - stdin is forwarded (EOF closes it), stdout and stderr stay separate,
    the exit status comes back
  - when the client disconnects, the remote command is NOT killed: it only
    loses its stdin/stdout, like `ssh host cmd` without a tty (sshd sends
    SIGHUP only with a pty). Cleanup must be explicit, as on real hosts
    (tools/cluster.py Cluster.down).

Wire (TCP): client sends u32 len + command; then frames both ways:
u8 channel (0 stdin / 1 stdout / 2 stderr / 3 exit, 4 stdin-EOF) + u32 len + bytes.

    python3 tests/fakes/fake_sshd.py IP PORT
"""
import os
import signal
import socket
import struct
import subprocess
import sys
import threading

LOGIN_ENV = {"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME": "/root",
             "USER": "root", "LOGNAME": "root", "SHELL": "/bin/bash", "LANG": "C.UTF-8"}


def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise EOFError
        buf += chunk
    return buf


def send_frame(s, lock, ch, data):
    with lock:
        s.sendall(struct.pack("!BI", ch, len(data)) + data)


def session(conn):
    lock = threading.Lock()
    try:
        cmd = recv_exact(conn, struct.unpack("!I", recv_exact(conn, 4))[0]).decode()
    except EOFError:
        conn.close()
        return
    p = subprocess.Popen(["bash", "-c", cmd], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         env=LOGIN_ENV, cwd="/root" if os.path.isdir("/root") else "/", start_new_session=True)

    def pump(src, ch):
        for chunk in iter(lambda: os.read(src.fileno(), 65536), b""):
            try:
                send_frame(conn, lock, ch, chunk)
            except OSError:
                break

    outs = [threading.Thread(target=pump, args=(p.stdout, 1), daemon=True),
            threading.Thread(target=pump, args=(p.stderr, 2), daemon=True)]
    [t.start() for t in outs]

    def reader():
        try:
            while True:
                ch, n = struct.unpack("!BI", recv_exact(conn, 5))
                data = recv_exact(conn, n) if n else b""
                if ch == 0:
                    p.stdin.write(data)
                    p.stdin.flush()
                elif ch == 4:
                    p.stdin.close()
        except (EOFError, OSError, ValueError):
            # the client went away: like sshd without a pty, leave the command
            # running; it just loses its stdin
            try:
                p.stdin.close()
            except OSError:
                pass
    threading.Thread(target=reader, daemon=True).start()
    rc = p.wait()
    [t.join(timeout=5) for t in outs]
    try:
        send_frame(conn, lock, 3, struct.pack("!i", rc))
        conn.shutdown(socket.SHUT_WR)
    except OSError:
        pass
    conn.close()


def main():
    ip, port = sys.argv[1], int(sys.argv[2])
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((ip, port))
    srv.listen(256)
    print("READY", flush=True)
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=session, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
