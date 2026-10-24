"""Diagnosis (aws-test input `command`): IO500 mdtest-hard's shape on one
host -- N processes create small files in ONE directory through the
interception library for D seconds, against native MDTs/OSTs on this host
-- and report every error with its cause. Optionally idle first (the
gap ior-hard-write leaves before mdtest-hard).

    python3 tools/diag/big_dir_create.py --ip $HEAD_IP --procs 32 --seconds 400 [--idle 0]

Prints per-process counts, the first errors, and the servers' stderr."""
import argparse
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SERVERS = r'''
import sys, time
sys.path.insert(0, sys.argv[1])
from roce.roce_transport import RoCETransport
ip, port = sys.argv[2], int(sys.argv[3])
t = RoCETransport()
m = [t.serve_native("mdt", f"{ip}:{port + i}").local_address() for i in range(2)]
o = [t.serve_native("ost", f"{ip}:{port + 50 + i}").local_address() for i in range(2)]
print("READY", ",".join(m), ",".join(o), flush=True)
while True:
    time.sleep(60)
    try:
        import json
        print("STATS", json.dumps([h.stats() for h in t._native_handles] if hasattr(t, "_native_handles") else []), flush=True)
    except Exception:
        pass
'''
CHILD = r'''
import os, sys, time
r, secs, idle = int(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
d = "/mnt/rp/hard"
try: os.makedirs(d)
except FileExistsError: pass
time.sleep(idle)
end = time.time() + secs
i = errs = 0
first = []
while time.time() < end:
    try:
        fd = os.open(f"{d}/file.mdtest.{r}.{i}", os.O_CREAT | os.O_WRONLY, 0o664)
        os.write(fd, b"x" * 3901)
        os.close(fd)
    except OSError as e:
        errs += 1
        if len(first) < 3: first.append(f"{i} @{time.time() - end + secs:.0f}s: {e}")
        if errs > 50: break
    i += 1
print(f"rank {r}: created {i - errs}, errors {errs} {first}", flush=True)
'''


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ip", default=os.environ.get("HEAD_IP", "127.0.0.1"))
    ap.add_argument("--procs", type=int, default=32)
    ap.add_argument("--seconds", type=float, default=400)
    ap.add_argument("--idle", type=float, default=0)
    ap.add_argument("--port", type=int, default=24300)
    ap.add_argument("--client-host", help="run the client processes on this host over ssh (root@IP; the tree at the same "
                                          "path there): clients and servers on different hosts")
    a = ap.parse_args()
    env = dict(os.environ, PYTHONPATH=ROOT)
    env.setdefault("ROCE_TRANSPORT_LIB", os.path.join(ROOT, "roce/native/libroce_transport.so"))
    errf = open("/tmp/big_dir_servers.err", "w")
    srv = subprocess.Popen([sys.executable, "-u", "-c", SERVERS, ROOT, a.ip, str(a.port)], stdout=subprocess.PIPE,
                           stderr=errf, text=True, env=env)
    line = srv.stdout.readline().split()
    if not line or line[0] != "READY":
        print("servers did not start:", open("/tmp/big_dir_servers.err").read()[-2000:])
        return 1
    _, mdts, osts = line
    cenv = dict(env, LD_PRELOAD=os.path.join(ROOT, "roce/native/librocepool_intercept.so"), ROCEPOOL_PREFIX="/mnt/rp",
                ROCEPOOL_MDTS=mdts, ROCEPOOL_OSTS=osts)
    t0 = time.time()
    if a.client_host:
        import base64
        import shlex
        code = base64.b64encode(CHILD.encode()).decode()
        exports = " ".join(f"{k}={shlex.quote(cenv[k])}" for k in
                           ("PYTHONPATH", "ROCE_TRANSPORT_LIB", "LD_PRELOAD", "ROCEPOOL_PREFIX", "ROCEPOOL_MDTS",
                            "ROCEPOOL_OSTS") if k in cenv)
        extra = " ".join(f"{k}={shlex.quote(v)}" for k, v in os.environ.items() if k.startswith(("ROCE_", "FI_")))

        def launch(r):
            cmd = (f"ulimit -n 1048576 2>/dev/null; cd {shlex.quote(ROOT)} && env {exports} {extra} python3 -c "
                   f"\"$(echo {code} | base64 -d)\" {r} {a.seconds} {a.idle}")
            return subprocess.Popen(["ssh", "-o", "BatchMode=yes", a.client_host, cmd], stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, text=True)
        ps = [launch(r) for r in range(a.procs)]
    else:
        ps = [subprocess.Popen([sys.executable, "-c", CHILD, str(r), str(a.seconds), str(a.idle)], env=cenv,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for r in range(a.procs)]
    total = bad = 0
    for p in ps:
        out = p.communicate()[0]
        lines = out.strip().splitlines()
        res = [l for l in lines if l.startswith("rank ")]
        other = [l for l in lines if not l.startswith("rank ")]
        print(res[-1] if res else f"(no result, exit {p.returncode})", flush=True)
        for l in other[:6]:
            print("   ", l[:300])
        if res:
            total += int(res[-1].split("created ")[1].split(",")[0])
            bad += int(res[-1].split("errors ")[1].split()[0])
    print(f"TOTAL created {total}, errors {bad}, wall {time.time() - t0:.0f}s")
    srv.kill()
    errf.close()
    print("servers' stderr (last 3000 chars):\n" + open("/tmp/big_dir_servers.err").read()[-3000:])
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
