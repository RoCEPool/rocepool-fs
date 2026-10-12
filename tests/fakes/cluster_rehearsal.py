"""Rehearse the REAL multi-host path of tools/io500_run.py (a cluster file,
ssh to every peer, MPI across hosts by IP, per-host name resolution) on
one Linux host: N network + mount namespaces (tools/netns_cluster.py),
each node with its own hostname and a fake sshd (tests/fakes/fake_sshd.py,
reached over the node's IP), and the controller itself running on node 0
-- so every node, node 0 included, is an equal "host" as on site.

    python3 tests/fakes/cluster_rehearsal.py N REPORT [io500_run args...]

/etc/hosts is shared by the namespaces (like consistent DNS) and restored
afterwards, since the run may add its name-resolution block to it.
Exit status: io500_run's.
"""
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
from tools import netns_cluster  # noqa: E402
from tools.netns_cluster import MNTNS_DIR, NETNS_DIR, node_ip, node_name  # noqa: E402

FAKES = os.path.join(ROOT, "tests", "fakes")


def in_node(i: int, argv: list[str], hostname: str | None = None) -> list[str]:
    ns = node_name(i)
    pre = ["nsenter", f"--net={os.path.join(NETNS_DIR, ns)}", f"--mount={os.path.join(MNTNS_DIR, ns)}"]
    if hostname:
        return pre + ["unshare", "--uts", "sh", "-c", 'hostname "$0"; exec "$@"', hostname, *argv]
    return pre + argv


def main() -> int:
    n, report = int(sys.argv[1]), sys.argv[2]
    extra = sys.argv[3:]
    from roce.native.build_lib import ensure_fuse_client, ensure_intercept_lib, ensure_softverbs_lib
    lib, fuse, icp = ensure_softverbs_lib(), ensure_fuse_client(), ensure_intercept_lib()
    hosts_orig = open("/etc/hosts").read()
    sshds = []
    try:
        netns_cluster.up(n)
        for i in range(n):
            p = subprocess.Popen(in_node(i, [sys.executable, "-u", os.path.join(FAKES, "fake_sshd.py"), node_ip(i),
                                             "2222"], hostname=node_name(i)),
                                 stdout=subprocess.PIPE, text=True)
            assert p.stdout.readline().startswith("READY"), f"fake sshd on node {i} did not start"
            sshds.append(p)
        cfile = os.path.join(os.path.dirname(os.path.abspath(report)), "rehearsal_cluster.json")
        with open(cfile, "w") as f:
            json.dump({"nodes": [{"ip": node_ip(0)}] + [{"ip": node_ip(i), "peer": f"root@{node_ip(i)}"}
                                                        for i in range(1, n)],
                       "dir": ROOT, "lib": lib, "fuse_client": fuse, "intercept_lib": icp, "env": {}}, f, indent=1)
        env = dict(os.environ, PATH=f"{FAKES}:{os.environ['PATH']}", ROCEPOOL_CLUSTER=cfile)
        cmd = in_node(0, [sys.executable, "-u", os.path.join(ROOT, "tools", "io500_run.py"), "--report", report,
                          "--fuse-uring", "off", *extra], hostname=node_name(0))
        rc = subprocess.run(cmd, env=env, cwd=ROOT).returncode
        # io500_run must have stopped everything it started on every node
        # itself (Cluster.down) -- checked before this script's own teardown,
        # which would otherwise hide leftovers
        time.sleep(1)
        left = subprocess.run(["pgrep", "-af", "crossover_node.py (mdt|ost)-native|rocepool-fuse --mount|"
                               "io500_src/io500 |hydra_pmi_proxy"], capture_output=True, text=True).stdout.strip()
        if left:
            print(f"LEFTOVER processes after io500_run:\n{left}", flush=True)
            return rc or 3
        return rc
    finally:
        for p in sshds:
            p.kill()
        time.sleep(0.2)
        netns_cluster.down(quiet=True)
        with open("/etc/hosts", "w") as f:
            f.write(hosts_orig)
        try:
            os.unlink("/etc/hosts.rocepool.bak")
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
