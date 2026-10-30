"""The official IO500 benchmark (github.com/IO500/io500, unmodified) on an
N-node rocepool cluster, fully automated: deploy servers on every node,
mount the native C client on every node, run all IO500 phases with MPI
ranks on every node, snapshot every server's and process's counters
between phases, and write a report with a bottleneck analysis and a
projection onto real hardware (tools/io500_model.py).

    python3 tools/io500_run.py --nodes 10                    # simulated cluster (root)
    ROCEPOOL_CLUSTER=cluster.json python3 tools/io500_run.py # real hosts (tools/roce_onsite.py --peers)

Per-phase counters use IO500's own "pause-dir" hook (rank 0 waits before
each phase while a file named after it exists), so phase boundaries are
exact. IO500 marks such runs (and any shortened stonewall) as not valid
for submission; --valid runs the untouched official procedure instead
(stonewall 300 s, no pauses), with totals only.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import re
import select
import signal
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools.bench_ior import CACHE, ensure_mpi  # noqa: E402
from tools.cluster import Cluster  # noqa: E402

IO500_SRC = os.path.join(CACHE, "io500_src")
MNT = "/mnt/rocepool_io500"
PHASES = ["ior-easy-write", "mdtest-easy-write", "ior-hard-write", "mdtest-hard-write", "find",
          "ior-easy-read", "mdtest-easy-stat", "ior-hard-read", "mdtest-hard-stat", "mdtest-easy-delete",
          "mdtest-hard-read", "mdtest-hard-delete", "ior-rnd4K-easy-read", "find-easy", "find-hard",
          "mdworkbench-create", "mdworkbench-bench", "mdworkbench-delete", "ior-easy", "ior-hard",
          "mdtest-easy", "mdtest-hard", "mdworkbench", "timestamp", "opt", "debug"]
# WEKA reference (IO500 10-node production list): UVA "Afton", WekaIO
WEKA_AFTON = {"score": 105.94, "bw_gib_s": 33.28, "md_kiops": 337.29}


def ensure_io500(mpi_bin: str) -> str:
    """Official IO500 app + its pinned IOR/mdtest/pfind, built once."""
    exe = os.path.join(IO500_SRC, "io500")
    if os.path.exists(exe):
        return exe
    from tools import deps
    if not os.path.exists(IO500_SRC) and not deps.extract("io500", IO500_SRC):
        deps.check_download("IO500")
        subprocess.run(["git", "clone", "--depth", "1", "https://github.com/IO500/io500.git", IO500_SRC], check=True)
    env = dict(os.environ, PATH=f"{mpi_bin}:{os.environ['PATH']}")
    subprocess.run(["./prepare.sh"], cwd=IO500_SRC, env=env, check=True, stdout=subprocess.DEVNULL)
    return exe


def _mem_limit_gib() -> float:
    """what this host may use: the cgroup's memory limit when there is one
    (a container), else MemTotal"""
    limits = []
    try:
        with open("/proc/self/cgroup") as f:
            for line in f:
                parts = line.strip().split(":")
                if len(parts) < 3:
                    continue
                if parts[1] == "":  # cgroup v2
                    limits.append(f"/sys/fs/cgroup{parts[2]}/memory.max")
                elif parts[1] == "memory":  # v1
                    limits.append(f"/sys/fs/cgroup/memory{parts[2]}/memory.limit_in_bytes")
        for path in limits:
            try:
                with open(path) as f:
                    v = f.read().strip()
            except OSError:
                continue
            if v != "max" and v.isdigit() and int(v) < (1 << 60):
                return min(int(v) / 2**30, _mem_total_gib())
    except OSError:
        pass
    return _mem_total_gib()


def _mem_total_gib() -> float:
    for line in open("/proc/meminfo"):
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) / 1024 / 1024
    return 4.0


EASY_TRANSFER_MIB = 2  # ior-easy's transferSize


# IO500's phase sections, in run order; each takes "run = FALSE"
IO500_PHASES = ("ior-easy-write", "mdtest-easy-write", "ior-hard-write", "mdtest-hard-write", "find", "ior-easy-read",
                "mdtest-easy-stat", "ior-hard-read", "mdtest-hard-stat", "mdtest-easy-delete", "mdtest-hard-read",
                "mdtest-hard-delete", "ior-rnd4K-easy-read")


def write_ini(path: str, *, datadir: str, resdir: str, pause_dir: str | None, stonewall: int, ior_easy_block_mib: int,
              ior_hard_segments: int, md_easy_n: int, md_hard_n: int, phases: list | None = None) -> None:
    lines = ["[global]", f"datadir = {datadir}", "timestamp-datadir = FALSE", f"resultdir = {resdir}",
             "timestamp-resultdir = FALSE", "api = POSIX", "drop-caches = FALSE", "verbosity = 1", "",
             "[debug]", f"stonewall-time = {stonewall}"]
    if pause_dir:
        lines.append(f"pause-dir = {pause_dir}")
    # IOR aborts at once unless blockSize is a multiple of transferSize: a
    # probe's block (the run's / 8) of 1545 MiB made every client-mode probe
    # on site exit 255 -- the faster interception client was never measured
    easy_block = max(EASY_TRANSFER_MIB, ior_easy_block_mib // EASY_TRANSFER_MIB * EASY_TRANSFER_MIB)
    lines += ["", "[ior-easy]", f"transferSize = {EASY_TRANSFER_MIB}m", f"blockSize = {easy_block}m", "filePerProc = TRUE", "",
              "[mdtest-easy]", f"n = {md_easy_n}", "", "[ior-hard]", f"segmentCount = {ior_hard_segments}", "",
              "[mdtest-hard]", f"n = {md_hard_n}", ""]
    skip = [ph for ph in IO500_PHASES if phases and ph not in phases]  # only these (a targeted, not a valid run)
    if "find" not in skip and os.environ.get("ROCEPOOL_FIND_SPLIT", "1") != "0":
        # pfind -H 1: idle ranks take halves of a directory being read (by
        # telldir/seekdir position, which the intercept library spreads
        # evenly). Without it one rank read mdtest-hard's whole directory and
        # find stayed at ~1 M entries/s from 2 nodes to 4 (i3en, 2026-10-27).
        lines += ["[find]", "pfind-parallelize-single-dir-access-using-hashing = TRUE", ""]
    for ph in skip:
        lines += [f"[{ph}]", "run = FALSE", ""]
    with open(path, "w") as f:
        f.write("\n".join(lines))


class Snapshotter:
    """Counters of every server (STATS op) and every process (procstat) on
    every node, at one moment."""

    def __init__(self, cluster: Cluster, servers: list[dict]):
        self.cluster, self.servers = cluster, servers
        from roce.roce_transport import RoCETransport
        self.transport = RoCETransport()
        self.mnt_of = {}
        if not cluster.real:
            from tools.netns_cluster import MNTNS_DIR, node_name
            for n in cluster.nodes:
                self.mnt_of[f"mnt:[{os.stat(os.path.join(MNTNS_DIR, node_name(n.index))).st_ino}]"] = n.index

    def _server(self, s: dict) -> dict:
        from roce.protocol import Frame, Op
        try:
            r = self.transport.send_request(s["addr"], Frame(op=Op.STATS, request_id=1, key="", value=b""))
            return json.loads(r.value)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    def _procs(self, n) -> tuple[dict, list[dict]]:
        """(host CPU, process records) of node n; on failure ({"error"}, []):
        the node's data is then missing from this snapshot (reported, and
        the analysis says which phases lack it) instead of silently zero or
        aborting the benchmark."""
        try:
            if n.local:
                from tools.procstat import host_cpu, snapshot
                host, recs = host_cpu(), snapshot()
            else:
                r = self.cluster.run(n.index, ["python3", os.path.join(ROOT, "tools", "procstat.py")],
                                     capture_output=True, text=True, timeout=60)
                d = json.loads(r.stdout.strip().splitlines()[-1])
                host, recs = d["host"], d["procs"]
        except Exception as exc:  # noqa: BLE001 -- timeout, ssh failure, bad JSON
            return {"error": f"{type(exc).__name__}: {str(exc)[:200]}"}, []
        for rec in recs:
            rec["node"] = n.index
        return host, recs

    def take(self) -> dict:
        t = time.monotonic()
        with cf.ThreadPoolExecutor(16) as ex:
            srv = list(ex.map(self._server, self.servers))
            if self.cluster.real:
                got = list(ex.map(self._procs, self.cluster.nodes))
                hosts = {n.index: h for n, (h, _) in zip(self.cluster.nodes, got)}
                procs = [r for _, rs in got for r in rs]
            else:  # one kernel for all simulated nodes
                from tools.procstat import host_cpu, snapshot
                hosts = {"shared": host_cpu()}
                procs = [dict(r, node=self.mnt_of[r["mntns"]]) for r in snapshot() if r["mntns"] in self.mnt_of]
        return {"t": t, "servers": {s["addr"]: v for s, v in zip(self.servers, srv)}, "procs": procs,
                "hosts": hosts}


def node_facts(cluster: Cluster) -> list[dict]:
    """cores / RAM / NIC speed of every real host (tools/procstat.py --facts)."""
    out = []
    for node in cluster.nodes:
        r = cluster.run(node.index, ["python3", os.path.join(ROOT, "tools", "procstat.py"), "--facts", node.ip],
                        capture_output=True, text=True, timeout=60)
        try:
            out.append(json.loads(r.stdout))
        except ValueError:
            out.append({"cores": os.cpu_count(), "mem_gib": _mem_total_gib(), "nic_gbps": None})
    return out


def auto_profile(facts: list[dict], n: int, args) -> dict:
    """The model's profile for THESE hosts (smallest node wins), so its
    projection can be checked against what the run measured."""
    from tools.io500_model import PROFILES
    p = dict(PROFILES["10x32c-100g"], name="these hosts")
    p.update(nodes=n, cores=min(f["cores"] for f in facts), ram_gib=min(f["mem_gib"] for f in facts),
             ranks_per_node=args.ranks_per_node, client_threads=args.client_threads)
    speeds = [f["nic_gbps"] for f in facts if f.get("nic_gbps")]
    if speeds:
        p["nic_gbps"] = min(speeds)
    # the NVMe tier these hosts have (tools/nvme_preflight.py): the smallest
    # node's namespaces; write bandwidth stays the profile's estimate per drive
    nv = [f.get("nvme_gib", 0.0) for f in facts]
    if nv and min(nv) > 0:
        p["nvme_gib"] = min(nv)
        p["nvme_gib_s"] = 2.5 * min(f.get("nvme_count", 1) for f in facts)
    return p


def _parse_results(text: str) -> tuple[dict, dict]:
    phases, score = {}, {}
    for line in text.splitlines():
        m = re.match(r"\[RESULT(?:-invalid)?\]\s+(\S+)\s+([\d.]+)\s+(\S+)\s*:\s*time\s+([\d.]+)\s+seconds(.*)", line)
        if m:
            phases[m.group(1)] = {"value": float(m.group(2)), "unit": m.group(3), "time_s": float(m.group(4)),
                                  "invalid": "invalid" in line or "[INVALID]" in m.group(5)}
        m = re.match(r"\[SCORE(?:-invalid)?\s*\]\s+Bandwidth\s+([\d.]+)\s+GiB/s\s*:\s*IOPS\s+([\d.]+)\s+kiops\s*:\s*TOTAL\s+([\d.]+)",
                     line)
        if m:
            score = {"bw_gib_s": float(m.group(1)), "md_kiops": float(m.group(2)), "score": float(m.group(3))}
    return phases, score


_MD_ROW = {"write": "File creation", "stat": "File stat", "read": "File read", "delete": "File removal"}


def phase_ops(resdir: str, phase: str) -> float | None:
    """The work a phase actually did, from its own output in the IO500
    result directory: files for mdtest, bytes for ior, entries for find.
    (IO500's phase *time* includes setup, barriers and syncs, so rate x
    time overstates the work -- 4x for a short mdtest phase.)"""
    try:
        if phase == "find":
            m = re.search(r"runtime:\s*([\d.]+)\s+rate:\s*([\d.]+)", open(os.path.join(resdir, "find.csv")).read())
            return float(m.group(1)) * float(m.group(2)) if m else None
        text = open(os.path.join(resdir, phase + ".txt")).read()
    except OSError:
        return None
    if phase.startswith("mdtest"):
        row = _MD_ROW[phase.rsplit("-", 1)[1]]
        vals = re.findall(rf"^\s*{row}\s+([\d.]+)\s+[\d.]+\s+([\d.]+)", text, re.M)
        if len(vals) >= 2:  # SUMMARY rate (ops/s), then SUMMARY time (s): mean columns
            return float(vals[0][1]) * float(vals[1][1])
        return None
    kind = "write" if phase.endswith("write") else "read"
    m = re.search(rf"^{kind}\s+([\d.]+)\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+[\d.]+\s+([\d.]+)\s+[\d.]+\s+([\d.]+)",
                  text, re.M)
    return float(m.group(1)) * 2**20 * float(m.group(3)) if m else None  # bw(MiB/s) x total(s)


def io500_validity(text: str, resdir: str, stonewall: int, capacity: dict | None) -> dict:
    """Whether IO500 called its result valid, why not, and what that means
    here. A short (quick) run is invalid by design -- stonewall under 300 s,
    phases shorter than the minimum: those reasons are 'expected'; anything
    else is 'unexpected' and explained where the cause is known."""
    reasons = [m.group(2).strip() for m in re.finditer(r"ERROR INVALID \(([^)]*)\) (.*)", text)]
    short = stonewall < 300
    expected_pat = r"stonewall-time|PAUSING BETWEEN RUNS|Runtime is smaller than expected minimum runtime"
    unexpected = [r for r in reasons if not (short and re.search(expected_pat, r))]
    found = None
    try:
        with open(os.path.join(resdir, "find.csv")) as f:
            rows = [l.split(",") for l in f.read().splitlines()[2:] if l.strip()]
        found = sum(int(r[3]) for r in rows if len(r) > 3)
    except (OSError, ValueError):
        pass
    diagnosis = []
    if any("Write phase needed" in r or "below stonewall" in r for r in unexpected):
        # which write phase ran out: IO500 marks its [RESULT] line [INVALID]
        short_ph = [m.group(1) for m in re.finditer(r"^\[RESULT\]\s+(\S+)\s+[\d.]+ \S+ : time ([\d.]+) seconds",
                                                    text, re.M) if m.group(1).endswith("write")
                    and float(m.group(2)) < stonewall]
        for ph in short_ph or ["ior-easy-write"]:
            if ph.startswith("mdtest"):
                diagnosis.append(f"{ph} ran out of files before the stonewall: mdtest's n (files per rank) was "
                                 f"created in under {stonewall} s -- io500_run.py sizes n from the quick run's rate "
                                 "(--rates-from); --md-easy-n / --md-hard-n set it")
                continue
            b = (capacity or {}).get("bounded_by") or {}
            diagnosis.append(f"{ph} ran out of data before the stonewall: the IOR data budget "
                             f"({(capacity or {}).get('budget_gib', '?')} GiB) holds less than {stonewall} s at this "
                             "write speed" + (f"; it is bounded by OST {b.get('ost')} ({b.get('kind')}) -- more or "
                                              "faster storage on that host lifts it" if b else ""))
    if found == 0 or any("Find didn't find anything" in r for r in unexpected):
        diagnosis.append("find matched no file: it did not see mdtest-hard's files (a directory listing "
                         "that failed?) or their size/ctime did not match")
    score_line = next((l for l in text.splitlines() if l.startswith("[SCORE ]")), "")
    return {"valid": bool(score_line) and "INVALID" not in score_line, "reasons": reasons,
            "unexpected": unexpected, "find_found": found, "diagnosis": diagnosis}


def load_rates(path: str | None) -> dict:
    """ior-easy-write / ior-hard-write GiB/s from an earlier run's report"""
    if not path:
        return {}
    try:
        with open(path) as f:
            ph = json.load(f).get("phases", {})
        return {k: ph[k]["value"] for k in ("ior-easy-write", "ior-hard-write", "mdtest-easy-write", "mdtest-hard-write")
                if ph.get(k, {}).get("value")}
    except (OSError, ValueError, KeyError, TypeError):
        return {}


def md_files_per_rank(kiops: float | None, stonewall: int, ranks: int, floor: int, margin: float = 1.5) -> int:
    """mdtest's n (files per rank) for a write phase measured at `kiops`
    over all ranks: enough to still be creating files at the stonewall,
    with `margin` for a faster real run than the measuring one; never below
    `floor`."""
    if not kiops or ranks <= 0:
        return floor
    return max(floor, int(kiops * 1000 * stonewall * margin / ranks) + 1)


def split_budget(budget: float, stonewall: int, rates: dict, margin: float = 1.15) -> tuple[float, float, str]:
    """(GiB for ior-easy, GiB for ior-hard, why). Each write phase must still
    be writing when the stonewall hits, or IO500 calls the run INVALID (on
    site: ior-easy-write had 772 GiB, wrote 2.66 GiB/s, ran out at 290 s,
    while ior-hard kept 206 GiB it could never fill). With measured speeds:
    ior-hard gets what it writes in the stonewall (+15%), ior-easy the rest;
    without: 75% / 20%."""
    e, h = rates.get("ior-easy-write"), rates.get("ior-hard-write")
    if not e or not h:
        return 0.75 * budget, 0.2 * budget, "no measured speeds: 75% ior-easy, 20% ior-hard"
    need_e, need_h = e * stonewall * margin, h * stonewall * margin
    hard = min(need_h, 0.5 * budget)
    # ior-easy: what it needs, not all the rest -- the rest is the room the
    # later phases' data takes (mdtest-hard's files beyond the MDTs' inline budget)
    easy = min(0.95 * budget - hard, max(need_e, 0.5 * budget))
    why = (f"measured ior-easy-write {e:.2f} GiB/s, ior-hard-write {h:.3f} GiB/s: ior-hard {hard:.0f} GiB, "
           f"ior-easy {easy:.0f} GiB (needs {need_e:.0f} GiB for {stonewall} s)")
    if easy < e * stonewall * 1.02:  # the margin is a wish; this is the limit
        why += (f" -- NOT enough: ior-easy-write will run out after about {easy / e:.0f} s and IO500 will call "
                f"the run INVALID; the OSTs' capacity ({budget:.0f} GiB) is the limit")
    return easy, hard, why


def capacity_budget(servers: list[dict], facts: list[dict], ram_mib: float, osts_per_node: int):
    """(GiB IOR may write in total, the OST that bounds it as (GiB, addr,
    kind)): striping spreads data evenly, so the smallest OST times their
    number, with 10% headroom. An OST with a tier holds the tier's size (its
    RAM cache holds copies, not more data: the EFA run's tiers filled at
    94 of 94 GiB with 18.5 GiB cached); one without, its share of 40% of its
    host's RAM."""
    caps = []
    for s in (x for x in servers if x["kind"] == "ost"):
        m = re.search(r":(\d+)$", s.get("tier") or "")
        if m:
            caps.append((float(int(m.group(1))), s["addr"], "tier"))
        else:
            caps.append((0.4 * facts[s["node"]]["mem_gib"] / osts_per_node, s["addr"], "RAM only"))
    low = min(caps)
    return 0.9 * low[0] * len(caps), low


def slow_tier_nodes(speed: dict) -> list:
    """nodes whose tier writes at under 1/8 of the fastest node's speed
    (MiB/s by node); never all of them"""
    if len(speed) < 2:
        return []
    fast = max(speed.values())
    slow = sorted(i for i, v in speed.items() if v < fast / 8)
    return slow if len(slow) < len(speed) else []


# the kernel's word on a server that died: OOM-killer lines (dmesg, else the journal)
KERNEL_LOG_CMD = ("{ dmesg -T 2>/dev/null || journalctl -k --no-pager -n 3000 2>/dev/null; } | "
                  "grep -iE 'out of memory|oom|killed process|segfault|general protection|rocepool|python3' | tail -30")


def _file_line_until(path: str, p: subprocess.Popen, prefix: str, timeout: float) -> str:
    """the first line starting with `prefix` in the file p writes to, or what
    is there when the deadline passes or p exits first"""
    deadline = time.monotonic() + timeout
    while True:
        try:
            with open(path, errors="replace") as f:
                text = f.read()
        except OSError:
            text = ""
        for line in text.splitlines():
            if line.startswith(prefix):
                return line
        if p.poll() is not None:
            return text + f"[exited {p.returncode}]"
        if time.monotonic() > deadline:
            return text
        time.sleep(0.05)


def _readline_until(p: subprocess.Popen, prefix: str, timeout: float) -> str:
    """The first line starting with `prefix` from p's stdout (lines before it
    -- warnings, banners -- are skipped), or everything read if the deadline
    passes or the process exits first."""
    deadline = time.monotonic() + timeout
    seen = []
    while time.monotonic() < deadline:
        r, _, _ = select.select([p.stdout], [], [], max(0.0, deadline - time.monotonic()))
        if not r:
            break
        line = p.stdout.readline()
        if not line:
            break
        if line.startswith(prefix):
            return line
        seen.append(line)
    return "".join(seen) + ("" if p.poll() is None else f"[exited {p.returncode}]")


def main() -> int:
    # SIGTERM (e.g. roce_onsite.py's timeout) -> SystemExit, so the finally
    # below still unmounts and stops everything on every host
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--nodes", type=int, default=10, help="simulated cluster size (ignored with ROCEPOOL_CLUSTER)")
    ap.add_argument("--ranks-per-node", type=int, default=None,
                    help="MPI ranks per node (default: simulated 2; real min(cores, 32))")
    ap.add_argument("--mdts-per-node", type=int, default=None,
                    help="MDT servers per node (default: simulated 1; real: one per NUMA node, each pinned to it)")
    ap.add_argument("--osts-per-node", type=int, default=None, help="OST servers per node (default as above)")
    ap.add_argument("--direct-read", choices=["auto", "on", "off"], default="auto",
                    help="one-sided reads and lookups (client RDMA READs stripes and MDT entries, the server's "
                         "CPU idle): auto = the client probe tries the interception client both ways")
    ap.add_argument("--bind-ranks", choices=["auto", "numa", "core", "none"], default="auto",
                    help="MPI rank placement (hydra -bind-to): auto = numa on real hosts, none simulated")
    ap.add_argument("--client-threads", type=int, default=16, help="native client FUSE threads per node")
    ap.add_argument("--intercept-io-threads", type=int, default=4,
                    help="interception client: I/O threads per rank (stripe fan-out)")
    ap.add_argument("--attr-ttl", type=float, default=3.0)
    ap.add_argument("--stonewall", type=int, default=None, help="seconds per write phase (IO500 official: 300)")
    ap.add_argument("--rates-from", default=None,
                    help="a previous run's report (the on-site quick run): its ior-easy / ior-hard write speeds "
                         "size this run's data so each write phase lasts the stonewall")
    ap.add_argument("--mem-budget-gib", type=float, default=None,
                    help="bytes IOR may write in total (the store is in memory); default: 40%% of the "
                         "cluster's RAM")
    ap.add_argument("--md-easy-n", type=int, default=None, help="mdtest-easy files per rank (cap)")
    ap.add_argument("--md-hard-n", type=int, default=None, help="mdtest-hard files per rank (cap)")
    ap.add_argument("--valid", action="store_true", help="official procedure: 300 s stonewall, no pauses")
    ap.add_argument("--phases", default=None,
                    help="comma-separated IO500 phases to run, the others skipped (run = FALSE): a targeted "
                         "measurement, e.g. ior-easy-write,ior-hard-write; never a valid score")
    ap.add_argument("--no-pause", action="store_true",
                    help="no pause-dir (as --valid) with any stonewall: counters are taken at the phase headers")
    ap.add_argument("--project", default="10x32c-100g",
                    help="hardware to project onto: a tools/io500_model.py profile, a JSON file, or 'auto' "
                         "(real clusters: the hosts' own cores / RAM / NIC speed)")
    ap.add_argument("--fuse-uring", choices=["auto", "on", "off"], default="auto",
                    help="the client's FUSE channel: classic /dev/fuse, FUSE over io_uring (Linux 6.14+, "
                         "fuse.enable_uring), or auto: a short probe run each way on these hosts, the faster "
                         "one is used for the real run (both results go in the report)")
    ap.add_argument("--client", choices=["auto", "fuse", "intercept"], default="auto",
                    help="how the ranks reach the pool: the native FUSE client mounted on every node, the "
                         "LD_PRELOAD interception library inside every rank (no mount, no kernel on the I/O "
                         "path: roce/native/rocepool_intercept.c), or auto: a short probe run of each (and of "
                         "both FUSE channels), the fastest is used for the real run")
    ap.add_argument("--ost-tier", default="auto",
                    help="the OSTs' capacity tier (roce/native/ost_tier.c): 'none' (objects in RAM only), a spec "
                         "for every OST ('file:PATH[:GIB]' with {n} = OST number, 'spdk:TRADDR'), or 'auto': real "
                         "hosts run tools/nvme_preflight.py and use the NVMe-backed file system it recommends "
                         "(and the drive's P2P memory when NIC, drive and kernel allow it); simulated: none")
    ap.add_argument("--slow-tier", choices=["drop", "keep"], default="drop",
                    help="real hosts: a host whose tier writes at < 1/8 of the fastest host's speed gets no OSTs "
                         "(drop, default) or keeps them (keep)")
    ap.add_argument("--ost-ram-cache-mib", type=int, default=None,
                    help="RAM cache per OST in front of the tier (default: real 20%% of the host's RAM per OST, "
                         "simulated 64 MiB)")
    ap.add_argument("--report", default=os.path.join(ROOT, "io500_report.json"))
    args = ap.parse_args()
    # the servers, clients and test scenarios hold a socket per connection
    # and libfabric/verbs descriptors: a 1024-file soft limit (ssh's
    # default) ran out on site; raise it to the hard limit for us and our
    # children
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft != resource.RLIM_INFINITY and (hard == resource.RLIM_INFINITY or soft < hard):
            resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, 1 << 20) if hard != resource.RLIM_INFINITY else 1 << 20, hard))
    except (ValueError, OSError):
        pass
    if os.geteuid() != 0:
        print("needs root (FUSE mounts, namespaces)", file=sys.stderr)
        return 2

    cluster = Cluster.current(args.nodes)
    n = len(cluster.nodes)
    facts = node_facts(cluster) if cluster.real else []
    if args.ranks_per_node is None:
        args.ranks_per_node = min([f["cores"] for f in facts] + [32]) if facts else 2
    # thread-per-core / NUMA: on real hosts one MDT and one OST per NUMA
    # node, each pinned to its node (cores + memory), ranks bound to NUMA
    numa_nodes = min([f.get("numa_nodes") or 1 for f in facts]) if facts else 1
    if args.mdts_per_node is None:
        args.mdts_per_node = numa_nodes if cluster.real else 1
    if args.osts_per_node is None:
        args.osts_per_node = numa_nodes if cluster.real else 1
    bind = args.bind_ranks if args.bind_ranks != "auto" else ("numa" if cluster.real else "none")
    ranks = n * args.ranks_per_node
    stonewall = 300 if args.valid else (args.stonewall or (20 if not cluster.real else 60))
    # the store is in memory: IOR may write this much in total. Real hosts:
    # 40% of the cluster's RAM; the simulated cluster shares one host with 2N
    # clients' write buffers and the ranks, so 25% of it, and small write caps
    budget = args.mem_budget_gib or (0.4 if facts else 0.2) * (sum(f["mem_gib"] for f in facts) if facts
                                                                else _mem_limit_gib())
    easy_block = max(16, int(budget * 0.75 * 1024 / ranks) // 2 * 2)  # MiB per rank, a multiple of the 2m transfer
    hard_segs = max(100, int(budget * 0.2 * 2**30 / (ranks * 47008)))
    # simulated: every rank's files live in this one host's RAM too (mdtest-hard
    # keeps 3901 bytes per file inline on the MDTs)
    md_easy_n = args.md_easy_n or (1000000 if cluster.real else 8000)
    md_hard_n = args.md_hard_n or (1000000 if cluster.real else 8000)
    # mdtest stops at n files per rank: a write phase that creates them all
    # before the stonewall makes the run INVALID. The 6-node valid run
    # (40999521729) did: mdtest-easy-write created its 1 M files per rank at
    # 933 k/s over 192 ranks in 207 s. With the quick run's measured rates, n
    # covers the stonewall with a margin.
    md_rates = load_rates(args.rates_from)
    md_easy_n = args.md_easy_n or md_files_per_rank(md_rates.get("mdtest-easy-write"), stonewall, ranks, md_easy_n)
    md_hard_n = args.md_hard_n or md_files_per_rank(md_rates.get("mdtest-hard-write"), stonewall, ranks, md_hard_n)

    print(f"== {cluster.describe}", flush=True)
    print(f"== {ranks} MPI ranks, stonewall {stonewall}s, IOR data budget {budget:.1f} GiB "
          f"(ior-easy {easy_block} MiB/rank, ior-hard {hard_segs} segments/rank)", flush=True)
    mpi_bin = ensure_mpi()
    io500 = ensure_io500(mpi_bin)
    work = os.path.join(CACHE, "io500_run")
    # the previous run's outputs (the onsite quick run's, when this is the
    # official one) are kept next to it, one generation, for the results bundle
    subprocess.run(["rm", "-rf", work + "_prev"])
    if os.path.isdir(work):
        os.rename(work, work + "_prev")
    os.makedirs(os.path.join(work, "results"))
    report = {"benchmark": "IO500 (official app, github.com/IO500/io500)", "cluster": cluster.describe,
              "nodes": n, "ranks": ranks, "ranks_per_node": args.ranks_per_node,
              "mdts": n * args.mdts_per_node, "osts": n * args.osts_per_node, "stonewall_s": stonewall,
              "valid_procedure": args.valid, "cpu_cores_here": os.cpu_count(),
              "reference": {"WEKA_Afton_10node": WEKA_AFTON}}
    servers, fuse = [], []
    try:
        cluster.up()
        # simulated: every node's servers, clients and ranks share this host's
        # few cores -- small write caps, and an RPC timeout that outlasts a
        # scheduling stall of a thousand threads on two cores
        env = cluster.env() if cluster.real else cluster.env({"ROCEPOOL_DIRTY_CAP_MIB": "4", "ROCE_BUFPOOL_MAX": "48",
                                                              "ROCEPOOL_RPC_TIMEOUT_MS": "60000",
                                                              "ROCEPOOL_SCACHE_SLOTS": "8"})
        # MPI treats two hosts with the same name as one machine, and finds
        # ranks by host name; a one-host run has neither problem (and a host
        # whose own name resolves to 127.0.1.1 -- Ubuntu's default -- is fine)
        if cluster.real and n > 1:
            names = []
            for i in range(n):
                r = cluster.run(i, ["hostname"], env, capture_output=True, text=True, timeout=60)
                name = r.stdout.strip().splitlines()[-1].strip() if r.stdout.strip() else ""
                if r.returncode or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", name):
                    raise RuntimeError(f"could not get the hostname of node {i} ({cluster.nodes[i].ip}): "
                                       f"exit {r.returncode}, output {r.stdout[-300:]!r} {r.stderr[-300:]!r}")
                names.append(name)
            dup = sorted({h for h in names if names.count(h) > 1})
            if dup:
                raise RuntimeError(f"hosts share a hostname {dup} (cloned machines?): MPI would treat them as one "
                                   f"machine and fail to connect. Give every host its own hostname. All: {names}")
            report["name_resolution"] = cluster.ensure_names(names, env)
            fixed = {i: r["fixed"] for i, r in report["name_resolution"].items() if r.get("fixed")}
            if fixed:
                print(f"== fixed hostname resolution for MPI (/etc/hosts block) on nodes {fixed}", flush=True)
        # IO500's find compares file ctimes -- set by each MDT's host clock --
        # with a timestamp file on node 0: hosts whose clocks disagree by
        # more than a phase's length make it miss files. Measured, reported.
        if cluster.real and n > 1:
            offsets = {}
            for i in range(1, n):
                try:
                    t0 = time.time()
                    r = cluster.run(i, ["date", "+%s.%N"], env, capture_output=True, text=True, timeout=30)
                    t1 = time.time()
                    offsets[i] = round(float(r.stdout.strip().splitlines()[-1]) - (t0 + t1) / 2, 3)
                except (ValueError, IndexError, subprocess.SubprocessError, OSError):
                    pass
            report["clock_offsets_s"] = offsets
            off = {i: o for i, o in offsets.items() if abs(o) > 2}
            if off:
                print("== WARNING: host clocks differ from node 0's by " +
                      ", ".join(f"node {i}: {o:+.1f} s" for i, o in off.items()) +
                      " -- IO500's find compares times set on different hosts; enable NTP/chrony", flush=True)
        os.environ.update(ROCE_TRANSPORT_LIB=cluster.lib)
        py = sys.executable if not cluster.real else "python3"
        helper = os.path.join(ROOT, "tools", "crossover_node.py")
        for k in range(args.mdts_per_node):
            for node in cluster.nodes:
                servers.append({"kind": "mdt", "node": node.index, "addr": f"{node.ip}:{19901 + k}"})
        for k in range(args.osts_per_node):
            for node in cluster.nodes:
                servers.append({"kind": "ost", "node": node.index, "addr": f"{node.ip}:{19951 + k}"})
        # the capacity tier: what each node offers (tools/nvme_preflight.py)
        tier_env = {}  # node -> env for its OSTs
        report["ost_tier"] = {"mode": args.ost_tier, "nodes": {}}
        if args.ost_tier == "auto" and cluster.real:
            def _pf(node):
                return cluster.run(node.index, ["python3", os.path.join(ROOT, "tools", "nvme_preflight.py"), "--json",
                                                "--measure", "--osts-per-node", str(args.osts_per_node)], env,
                                   capture_output=True, text=True, timeout=400)
            with cf.ThreadPoolExecutor(min(32, n)) as ex:
                pfs = list(ex.map(_pf, cluster.nodes))
            for node, r in zip(cluster.nodes, pfs):
                try:
                    pf = json.loads(r.stdout.strip().splitlines()[-1])
                except (ValueError, IndexError):
                    pf = {"error": (r.stdout + r.stderr)[-500:], "recommendation": {"tier": None, "p2pmem": None, "notes": []}}
                rec = pf["recommendation"]
                report["ost_tier"]["nodes"][node.index] = {"recommendation": rec, "kernel": pf.get("kernel"),
                                                           "nvme": [v["name"] for v in pf.get("nvme", [])],
                                                           "error": pf.get("error")}
                if rec.get("tier"):
                    if rec.get("tier_dir"):
                        cluster.run(node.index, ["mkdir", "-p", rec["tier_dir"]], env, net=False, timeout=60)
                    tier_env[node.index] = {"ROCEPOOL_OST_TIER": rec["tier"]}
                    if rec.get("p2pmem"):
                        tier_env[node.index]["ROCEPOOL_P2PMEM"] = rec["p2pmem"]
            # data is striped evenly over every OST: a host whose storage is
            # much slower than the others' holds every write back (on site: 33
            # MiB/s next to 1968, the cluster wrote at 0.5 GiB/s and read at
            # 0.17). Such hosts keep their MDTs and clients but no OSTs.
            speed = {i: (report["ost_tier"]["nodes"][i]["recommendation"].get("tier_write") or {}).get("mib_s")
                     for i in tier_env}
            known = {i: v for i, v in speed.items() if v}
            if args.slow_tier == "drop":
                slow = slow_tier_nodes(known)
                fast = max(known.values(), default=0)
                if slow:
                    servers[:] = [x for x in servers if not (x["kind"] == "ost" and x["node"] in slow)]
                    for i in slow:
                        del tier_env[i]
                    report["ost_tier"]["dropped_slow_nodes"] = {i: known[i] for i in slow}
                    report["osts"] = sum(1 for x in servers if x["kind"] == "ost")
                    print(f"== OSTs left out on node(s) {slow}: their storage writes at "
                          + ", ".join(f"{known[i]:.0f}" for i in slow) + f" MiB/s vs {fast:.0f} MiB/s on the fastest "
                          "(striped data would wait for them); their MDTs and clients stay", flush=True)
            with_tier = sorted(tier_env)
            print(f"== OST tier: NVMe-backed on nodes {with_tier or 'none'}"
                  + (f", P2P memory on {[i for i in with_tier if 'ROCEPOOL_P2PMEM' in tier_env[i]]}" if with_tier else ""),
                  flush=True)
        elif args.ost_tier not in ("auto", "none"):
            for node in cluster.nodes:
                tier_env[node.index] = {"ROCEPOOL_OST_TIER": args.ost_tier}
        ram_mib = args.ost_ram_cache_mib
        if ram_mib is None:
            # 20% of the smallest host's RAM per OST, but never more than a
            # quarter of what that host has available now (other jobs, the
            # MDTs' pinned lookup tables, page cache in use) split over its OSTs
            ram_mib = 64 if not cluster.real else int(min(min(f["mem_gib"] * 0.2, f.get("mem_avail_gib", f["mem_gib"]) * 0.25)
                                                          for f in facts) * 1024 / args.osts_per_node)
        for k, s in enumerate(x for x in servers if x["kind"] == "ost"):
            # {n}: every OST its own file, numbered across the cluster (the
            # simulated nodes share one file system)
            if s["node"] in tier_env:
                s["env"] = {k2: v.replace("{n}", str(k)) for k2, v in tier_env[s["node"]].items()}
                s["env"]["ROCEPOOL_OST_RAM_CACHE_MIB"] = str(ram_mib)
                s["tier"] = s["env"]["ROCEPOOL_OST_TIER"]
        # the data budget from what the OSTs can hold: striping spreads the
        # data evenly, so the smallest OST bounds it (tier: its size + RAM
        # cache; RAM only: its share of 40% of its host's RAM). A budget
        # that a 300 s write phase outruns makes the run INVALID -- said here,
        # with the OST that bounds it, rather than discovered at the end.
        if cluster.real and facts and args.mem_budget_gib is None:
            budget, low = capacity_budget(servers, facts, ram_mib, args.osts_per_node)
            # mdtest-hard's 3901-byte files beyond the MDTs' inline budget go
            # to the OSTs (a 4 KiB block each): room for them comes off the
            # IOR budget -- the first EFA run filled every tier with IOR data
            # and mdtest-hard's spilled files then failed ("close failed",
            # tiers at 93.8 of 94 GiB)
            rates = load_rates(args.rates_from)
            hard_kiops = rates.get("mdtest-hard-write") or 150.0
            files = min(hard_kiops * 1000 * stonewall * 1.3, md_hard_n * ranks)
            inline_gib = min(f["mem_gib"] for f in facts) * 0.15 * n  # the MDTs' inline budgets, all hosts
            reserve = max(0.0, files * 4096 / 2**30 - inline_gib) * 1.25
            reserve = min(reserve, 0.5 * budget)
            budget -= reserve
            report["mdtest_hard_reserve_gib"] = round(reserve, 1)
            print(f"== OST room kept for mdtest-hard's files beyond the MDTs' inline budget: {reserve:.0f} GiB "
                  f"(~{files / 1e6:.1f} M files at {hard_kiops:.0f} kIOPS)", flush=True)
            easy_gib, hard_gib, why = split_budget(budget, stonewall, rates)
            easy_block = max(16, int(easy_gib * 1024 / ranks) // 2 * 2)
            hard_segs = max(100, int(hard_gib * 2**30 / (ranks * 47008)))
            report["capacity"] = {"budget_gib": round(budget, 1), "bounded_by": {"ost": low[1], "kind": low[2],
                                                                                   "gib": round(low[0], 1)},
                                  "ior_easy_gib": round(easy_block * ranks / 1024, 1),
                                  "ior_hard_gib": round(hard_gib, 1), "split": why}
            print(f"== IOR data split: {why}", flush=True)
            print(f"== IOR data budget {budget:.1f} GiB from the OSTs' capacity (smallest: {low[1]}, {low[2]}, "
                  f"{low[0]:.0f} GiB): ior-easy {easy_block} MiB/rank, ior-hard {hard_segs} segments/rank; a "
                  f"{stonewall} s ior-easy-write stays valid below {easy_block * ranks / 1024 / stonewall:.2f} GiB/s",
                  flush=True)
        # the MDTs' one-sided lookup table: room for every file of the run at
        # load <= 0.5 (256 B per slot; the simulation's RAM is this host's)
        n_mdt = n * args.mdts_per_node
        want = 2 * (md_easy_n + md_hard_n) * ranks // max(1, n_mdt) + 65536
        slots = 1 << (want - 1).bit_length()
        slots = min(slots, 1 << 26) if cluster.real else min(slots, 1 << 16)
        for s in servers:
            if s["kind"] == "mdt":
                s.setdefault("env", {})["ROCEPOOL_MDT_SLOTS"] = str(slots)
        report["mdt_slots"] = slots
        # the MDTs' inline file bytes (mdtest-hard's 3901-byte files live in
        # the MDT): bounded, or millions of them outgrow the host -- 15% of
        # the smallest host's RAM over its MDTs; beyond it files go striped
        if cluster.real and facts:
            inl_mib = int(min(f["mem_gib"] for f in facts) * 0.15 * 1024 / max(1, args.mdts_per_node))
            for s in servers:
                if s["kind"] == "mdt":
                    s["env"]["ROCEPOOL_MDT_INLINE_BUDGET_MIB"] = str(inl_mib)
            report["mdt_inline_budget_mib"] = inl_mib
        if cluster.real and numa_nodes > 1:
            for kind in ("mdt", "ost"):
                for node in cluster.nodes:
                    mine = [x for x in servers if x["kind"] == kind and x["node"] == node.index]
                    for j, s in enumerate(mine):
                        s.setdefault("env", {})["ROCEPOOL_NUMA_NODE"] = str(j % numa_nodes)
                        s["numa_node"] = j % numa_nodes
            print(f"== NUMA: {numa_nodes} nodes per host; each MDT/OST pinned to one, ranks bound to NUMA", flush=True)
        report["numa_nodes_per_host"] = numa_nodes
        # each server on a host pins its worker threads from its own core on
        # (every one counted from core 0: an MDT's and an OST's threads shared
        # the same cores) -- within its NUMA node's cores when it has one
        if cluster.real and facts and numa_nodes <= 1:
            for node in cluster.nodes:
                mine = [x for x in servers if x["node"] == node.index]
                cores = int((facts[node.index] if node.index < len(facts) else facts[0]).get("cores") or 0)
                for j, s in enumerate(mine):
                    if cores and len(mine) > 1:
                        s.setdefault("env", {})["ROCEPOOL_PIN_OFFSET"] = str(j * cores // len(mine))
        # millions of files: the MDT's allocations (and EFA's registrations)
        # take more memory mappings than the default vm.max_map_count
        # (65530) allows -- malloc then fails with RAM free
        if cluster.real:
            for node in cluster.nodes:
                cluster.run(node.index, ["sh", "-c", "[ $(cat /proc/sys/vm/max_map_count) -ge 1048576 ] || "
                                                     "sysctl -qw vm.max_map_count=1048576 || true"], env, net=False,
                            capture_output=True, timeout=60)
        # every server's output goes to its own file (a pipe nobody drains
        # would block a chatty server, and the file is what a crash leaves)
        procs = []
        for s in servers:
            # one file per server: two servers sharing one (several per node)
            # overwrote each other's output -- READY lines lost, start-up
            # misjudged as a hang
            s["log"] = os.path.join(work, f"{s['kind']}{s['node']}_{s['addr'].rsplit(':', 1)[1]}.log")
            procs.append(cluster.popen(s["node"], [py, "-u", helper, f"{s['kind']}-native", s["addr"]],
                                       dict(env, **s.get("env", {})),
                                       stdin=subprocess.PIPE, stdout=open(s["log"], "w"), stderr=subprocess.STDOUT))
        for s, p in zip(servers, procs):
            line = _file_line_until(s["log"], p, "READY", 120)
            if not line.startswith("READY"):
                raise RuntimeError(f"{s['kind']} on node {s['node']} ({s['addr']}) did not start within 120 s: {line[-2000:]}")
        mdts = ",".join(s["addr"] for s in servers if s["kind"] == "mdt")
        osts = ",".join(s["addr"] for s in servers if s["kind"] == "ost")
        print(f"== servers up: {len(servers)} ({mdts.count(',') + 1} MDT, {osts.count(',') + 1} OST)", flush=True)
        def mount_all(uring: bool, tag: str, direct: bool = True) -> None:
            for node in cluster.nodes:
                cluster.run(node.index, ["mkdir", "-p", MNT], env, check=True)
                log = open(os.path.join(work, f"client{node.index}{tag}.log"), "w")
                fuse.append(cluster.popen(node.index, [cluster.fuse_client, "--mount", MNT, "--mdts", mdts,
                                                       "--osts", osts, "--attr-ttl", str(args.attr_ttl),
                                                       "--threads", str(args.client_threads),
                                                       "--uring", "1" if uring else "0"],
                                          dict(env, ROCEPOOL_DIRECT_READ="1" if direct else "0"),
                                          stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT))
            deadline = time.monotonic() + 60
            pending = set(range(n))
            while pending and time.monotonic() < deadline:
                for i in list(pending):
                    if cluster.run(i, ["mountpoint", "-q", MNT], env, net=False).returncode == 0:
                        pending.discard(i)
                time.sleep(0.3)
            if pending:
                raise RuntimeError(f"client did not mount on nodes {sorted(pending)}")
            print(f"== native client mounted on all {n} nodes at {MNT} (FUSE over io_uring {'on' if uring else 'off'})",
                  flush=True)

        def unmount_all() -> None:
            for i in range(n):
                try:
                    cluster.run(i, ["umount", "-l", MNT], net=False, capture_output=True, timeout=30)
                except Exception:  # noqa: BLE001
                    pass
            while fuse:
                p = fuse.pop()
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    p.kill()

        # The interception client: nothing to mount. Every MPI rank gets the
        # library through mpiexec -genv (hydra sets it in the ranks, not in
        # mpiexec itself) and serves paths under MNT itself. Shell commands
        # on the data (mkdir, rm -rf) must run under the library too.
        icp_env = {"LD_PRELOAD": cluster.intercept_lib, "ROCEPOOL_PREFIX": MNT, "ROCEPOOL_MDTS": mdts,
                   "ROCEPOOL_OSTS": osts, "ROCEPOOL_ATTR_TTL": str(args.attr_ttl),
                   "ROCEPOOL_IO_THREADS": str(args.intercept_io_threads),
                   **{k: env[k] for k in ("ROCEPOOL_DIRTY_CAP_MIB", "ROCEPOOL_RPC_TIMEOUT_MS", "ROCEPOOL_SCACHE_SLOTS",
                                           "ROCE_BUFPOOL_MAX", "ROCE_TRANSPORT_LIB") if k in env}}
        icp_genv = [a for k, v in icp_env.items() for a in ("-genv", k, v)]
        bind_args = []
        if bind != "none":
            # hydra needs hwloc for -bind-to; use it only when this build has it
            if subprocess.run([os.path.join(mpi_bin, "mpiexec"), "-bind-to", bind, "-n", "1", "true"],
                              capture_output=True).returncode == 0:
                bind_args = ["-bind-to", bind]
            else:
                print(f"== mpiexec cannot -bind-to {bind} (no hwloc?): ranks unbound", flush=True)
        report["rank_binding"] = bind_args[1] if bind_args else "none"

        def client_up(mode: str, uring: bool, direct: bool, tag: str) -> list[str]:
            """brings the client up on every node; returns the extra mpiexec args"""
            if mode == "fuse":
                mount_all(uring, tag, direct)
                return []
            print(f"== interception client: ranks preload {os.path.basename(cluster.intercept_lib)}, "
                  f"paths under {MNT}, no mount, one-sided reads {'on' if direct else 'off'}", flush=True)
            return icp_genv + ["-genv", "ROCEPOOL_DIRECT_READ", "1" if direct else "0"]

        def client_down(mode: str) -> None:
            if mode == "fuse":
                unmount_all()

        def remove_tree(mode: str, path: str) -> None:
            if mode == "fuse":
                subprocess.run(["rm", "-rf", path], capture_output=True)
            else:
                cluster.run(0, ["python3", "-c", "import shutil, sys; shutil.rmtree(sys.argv[1], ignore_errors=True)",
                                path], dict(env, **icp_env), capture_output=True, timeout=600)

        def probe(mode: str, uring: bool, direct: bool) -> dict:
            """a short official run (no pauses, tiny stonewall): its score is
            the comparison; the data is removed afterwards"""
            tag = ("intercept" if mode == "intercept" else ("uring" if uring else "classic")) + ("" if direct else "_rpc")
            extra_mpi = client_up(mode, uring, direct, f"_probe_{tag}")
            pdir = os.path.join(work, f"probe_{tag}")
            os.makedirs(os.path.join(pdir, "results"))
            ini = os.path.join(pdir, "config.ini")
            write_ini(ini, datadir=os.path.join(MNT, "io500probe"), resdir=os.path.join(pdir, "results"),
                      pause_dir=None, stonewall=10, ior_easy_block_mib=max(16, easy_block // 8),
                      ior_hard_segments=max(50, hard_segs // 8), md_easy_n=max(200, md_easy_n // 20),
                      md_hard_n=max(200, md_hard_n // 20))
            hostfile, _, extra = cluster.mpi_files(pdir, args.ranks_per_node)
            cmd = [os.path.join(mpi_bin, "mpiexec"), *extra, *bind_args, *extra_mpi, "-f", hostfile, "-n", str(ranks), io500, ini]
            with open(os.path.join(pdir, "io500.out"), "w") as out:
                rc = subprocess.run(cmd, stdout=out, stderr=subprocess.STDOUT, cwd=pdir, env=env, timeout=1800).returncode
            text = open(os.path.join(pdir, "io500.out")).read()
            phases, score = _parse_results(text)
            remove_tree(mode, os.path.join(MNT, "io500probe"))
            client_down(mode)
            res = {"client": mode, "uring": uring if mode == "fuse" else None, "direct_read": direct,
                   "returncode": rc, "score": score, "phases": phases}
            if rc:  # why: the run's own error lines, and the clients' (a failed probe must say so)
                bad = [x for x in text.splitlines() if re.search(r"ERROR|error|fail|Abort|chdir|No such", x)]
                errs = bad[:3] + bad[3:][-2:]  # the first (the cause, usually) and the last
                for node in cluster.nodes:
                    try:
                        with open(os.path.join(work, f"client{node.index}_probe_{tag}.log"), errors="replace") as f:
                            errs += [f"node {node.index}: {x}" for x in f.read().splitlines()
                                     if x.startswith("rocepool:")][-2:]
                    except OSError:
                        pass
                res["errors"] = errs
            return res

        uring = args.fuse_uring == "on"
        mode = "intercept" if args.client == "intercept" else "fuse"
        kernel_offers = False
        if args.fuse_uring == "auto" and args.client != "intercept":
            # the kernel offers FUSE over io_uring only with fuse.enable_uring=Y
            # (a module parameter, N by default): turn it on on every node
            # when it exists -- we are root -- then check it took
            offers = []
            for node in cluster.nodes:
                r = cluster.run(node.index, ["sh", "-c", "p=/sys/module/fuse/parameters/enable_uring; "
                                                           "[ -f $p ] && { echo Y > $p 2>/dev/null; cat $p; }"],
                                env, capture_output=True, text=True)
                offers.append(r.stdout.strip() == "Y")
            kernel_offers = all(offers)
        direct = args.direct_read != "off"
        candidates = []
        if args.client != "intercept":
            if args.fuse_uring == "auto":
                candidates += [("fuse", False, direct)] + ([("fuse", True, direct)] if kernel_offers else [])
            else:
                candidates.append(("fuse", uring, direct))
        if args.client != "fuse":
            # the one-sided path is probed on the interception client: on and off
            candidates += [("intercept", False, direct)] + ([("intercept", False, False)] if args.direct_read == "auto" else [])
        if len(candidates) > 1:
            def name_of(c):
                base = {"fuse": "FUSE over io_uring" if c[1] else "FUSE, classic /dev/fuse",
                        "intercept": "interception library"}[c[0]]
                return base + (", one-sided reads" if c[2] else ", RPC reads")
            names = {c: name_of(c) for c in candidates}
            print("== client probe: " + " vs ".join(names[c] for c in candidates), flush=True)
            results = [probe(*c) for c in candidates]
            for c, r in zip(candidates, results):
                print(f"   {names[c]:<42}: score {r['score'].get('score', 0):.3f} "
                      f"(bw {r['score'].get('bw_gib_s', 0):.3f} GiB/s, md {r['score'].get('md_kiops', 0):.2f} kIOPS)"
                      + ("" if r["returncode"] == 0 else f"  [exit {r['returncode']}]"), flush=True)
                for e in r.get("errors", [])[-3:]:
                    print(f"      {e[:240]}", flush=True)
            ok = [(c, r) for c, r in zip(candidates, results) if r["returncode"] == 0 and r["score"]]
            best = max(ok, key=lambda cr: cr[1]["score"]["score"])[0] if ok else candidates[0]
            if not ok:
                print("   WARNING: every probe run failed (errors above): the real run uses the first candidate, "
                      "and will likely fail the same way", flush=True)
            mode, uring, direct = best
            fuse_results = [r for r in results if r["client"] == "fuse"]
            fuse_best = max((r for r in fuse_results if r["returncode"] == 0 and r["score"]),
                            key=lambda r: r["score"]["score"], default=None)
            report["fuse_channel_probe"] = {"results": fuse_results,
                                            "chosen": "io_uring" if (fuse_best and fuse_best["uring"]) else "classic"}
            if not kernel_offers and args.fuse_uring == "auto" and args.client != "intercept":
                report["fuse_channel_probe"]["reason"] = "kernel does not offer FUSE over io_uring"
            report["client_probe"] = {"results": results, "chosen": mode, "all_failed": not ok}
            print(f"== chosen: {names[best]}", flush=True)
        elif args.client != "intercept" and args.fuse_uring == "auto":
            report["fuse_channel_probe"] = {"results": [], "chosen": "classic",
                                            "reason": "kernel does not offer FUSE over io_uring"}
        report["client"] = mode
        report["direct_read"] = direct
        report["fuse_channel"] = ("io_uring" if uring else "classic") if mode == "fuse" else "none"
        mpi_extra = client_up(mode, uring, direct, "")

        pause = None if args.valid or args.no_pause else os.path.join(work, "pause")
        if pause:
            os.makedirs(pause)
            for ph in PHASES:
                open(os.path.join(pause, ph), "w").close()
        ini = os.path.join(work, "config.ini")
        write_ini(ini, datadir=os.path.join(MNT, "io500data"), resdir=os.path.join(work, "results"),
                  pause_dir=pause, stonewall=stonewall, ior_easy_block_mib=easy_block,
                  ior_hard_segments=hard_segs, md_easy_n=md_easy_n, md_hard_n=md_hard_n,
                  phases=[p.strip() for p in args.phases.split(",")] if args.phases else None)
        hostfile, _, extra = cluster.mpi_files(work, args.ranks_per_node)
        cmd = [os.path.join(mpi_bin, "mpiexec"), *extra, *bind_args, *mpi_extra, "-f", hostfile, "-n", str(ranks), io500, ini]
        snap = Snapshotter(cluster, servers)
        snaps = [("start", snap.take())]
        out_path = os.path.join(work, "io500.out")
        t0 = time.monotonic()
        with open(out_path, "w") as out:
            mpi = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, cwd=work, env=env)
        result_txt = os.path.join(work, "results", "result.txt")
        pos = 0
        started = False
        last_progress = time.monotonic()
        # a phase that shows no progress for this long is stuck (a dead
        # client leaves ranks in uninterruptible I/O): stop instead of
        # waiting forever. Valid write phases run 300 s by themselves.
        stall_s = max(1800, 6 * stonewall)
        while True:
            done = mpi.poll() is not None
            now = time.monotonic()
            if not started and not done and now - t0 > 300:
                mpi.kill()
                mpi.wait()
                raise RuntimeError("IO500 did not start in 300 s: MPI ranks could not start or reach each other "
                                   "(ssh between hosts, host name resolution, firewall on the MPI ports?). "
                                   f"Output: {open(out_path).read()[-1500:]}")
            if started and not done and now - last_progress > stall_s:
                mpi.terminate()
                try:
                    mpi.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    mpi.kill()
                    mpi.wait()
                raise RuntimeError(f"IO500 made no progress for {stall_s} s (last phase: {snaps[-1][0]}): stopped. "
                                   f"Client logs: {os.path.join(work, 'client*.log')}")
            # a server that died takes its data with it: every later phase
            # fails or hangs on it (on site: 20 more minutes of timeouts and
            # an empty find). Stop the run now; the report says which and why.
            lost = [s for s, p in zip(servers, procs) if p.poll() is not None]
            if lost and not done:
                print("== server died during the run: " + ", ".join(f"{s['kind']}@{s['addr']}" for s in lost)
                      + " -- stopping IO500", flush=True)
                mpi.terminate()
                try:
                    mpi.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    mpi.kill()
                    mpi.wait()
                break
            if os.path.exists(result_txt):
                with open(result_txt, "rb") as f:
                    f.seek(pos)
                    raw = f.read()
                # only whole lines: a line still being written is read next time
                if b"\n" in raw:
                    raw = raw[:raw.rindex(b"\n") + 1]
                    pos += len(raw)
                    last_progress = now
                else:
                    raw = b""
                chunk = raw.decode(errors="replace")
                for line in chunk.splitlines():
                    if line.startswith("; START"):
                        started = True
                    m = re.match(r"; Checking for pause file (.*)", line)
                    h = re.match(r"^\[([a-z0-9-]+)\]$", line.strip())
                    if m or (h and pause is None and h.group(1) in PHASES):
                        # pause mode: exact (rank 0 waits for us); valid mode: at
                        # the phase header, as close as an untouched run allows
                        ph = os.path.basename(m.group(1).strip()) if m else h.group(1)
                        started = True
                        snaps.append((ph, snap.take()))
                        print(f"   [{time.monotonic() - t0:7.1f}s] phase {ph}", flush=True)
                        if m:
                            try:
                                os.unlink(m.group(1).strip())
                            except OSError:
                                pass
            if done:
                break
            time.sleep(0.2)
        snaps.append(("end", snap.take()))
        # a server still running but no longer answering (the first EFA run:
        # an MDT missing from the final snapshot, its clients told "EOF")
        try:
            answered = {a for a, v in snaps[-1][1].get("servers", {}).items() if isinstance(v, dict) and v.get("kind")}
        except Exception:  # noqa: BLE001
            answered = None
        if answered is not None:
            mute = [s["addr"] for s, p in zip(servers, procs) if p.poll() is None and s["addr"] not in answered]
            if mute:
                report["unresponsive_servers"] = mute
                print("== server(s) running but not answering at the end: " + ", ".join(mute), flush=True)
        text = open(out_path).read()
        phases, score = _parse_results(text)
        for ph, r in phases.items():
            ops = phase_ops(os.path.join(work, "results"), ph)
            if ops:
                r["ops"] = ops
        dead = [dict(s, exit=p.poll(), log_tail=open(s["log"], errors="replace").read()[-1500:])
                for s, p in zip(servers, procs) if p.poll() is not None]
        if dead:
            # why: a SIGKILL nobody sent is the kernel's OOM killer (with
            # a NUMA-bound process, one node running out can be enough) -- its lines
            # from that host's kernel log go into the report
            klog = {}
            for node in sorted({d["node"] for d in dead}):
                try:
                    r = cluster.run(node, ["sh", "-c", KERNEL_LOG_CMD], env, net=False, capture_output=True,
                                    text=True, timeout=60)
                    klog[node] = r.stdout.strip().splitlines()[-30:]
                except (subprocess.SubprocessError, OSError) as exc:
                    klog[node] = [f"could not read the kernel log: {exc}"]
            for d in dead:
                d["kernel_log"] = klog.get(d["node"], [])
            report["dead_servers"] = dead
            print(f"== {len(dead)} server(s) died during the run: " +
                  ", ".join(f"{d['kind']}@{d['addr']} exit {d['exit']}" for d in dead), flush=True)
            for node, lines in klog.items():
                oom = [x for x in lines if re.search(r"out of memory|oom-kill|killed process", x, re.I)]
                if oom:
                    print(f"   node {node} kernel log: {oom[-1][-300:]}", flush=True)
        # an OST far slower than the others (its drive, its host) holds every
        # striped write back and, queued long enough, times requests out
        lat = {}
        first, last = snaps[0][1]["servers"], snaps[-1][1]["servers"]
        for a, b in last.items():
            if b.get("kind") == "ost" and "error" not in b and "error" not in first.get(a, {"error": 1}):
                n_req = b["reqs"] - first[a]["reqs"]
                if n_req > 100:
                    lat[a] = (b["handler_wall_ns"] - first[a]["handler_wall_ns"]) / n_req / 1e6
        if len(lat) > 1:
            med = sorted(lat.values())[len(lat) // 2]
            slow = {a: v for a, v in lat.items() if v > 10 * max(med, 0.5)}
            report["ost_latency_ms"] = {a: round(v, 2) for a, v in lat.items()}
            if slow:
                print("== slow OST(s): " + ", ".join(f"{a} {v:.1f} ms/request" for a, v in slow.items())
                      + f" (median {med:.1f} ms): their storage bounds the run", flush=True)
        report["validity"] = io500_validity(text, os.path.join(work, "results"), stonewall, report.get("capacity"))
        v = report["validity"]
        print(f"== IO500 says the result is {'VALID' if v['valid'] else 'INVALID'}"
              + (f"; find matched {v['find_found']} files" if v["find_found"] is not None else ""), flush=True)
        for r in v["unexpected"]:
            print(f"   invalid: {r}", flush=True)
        for d in v["diagnosis"]:
            print(f"   why: {d}", flush=True)
        report.update(returncode=mpi.returncode, phases=phases, score=score, wall_s=round(time.monotonic() - t0, 1),
                      io500_output_tail=text.splitlines()[-60:],
                      config={"paused_between_phases": pause is not None,
                              "ior_easy_block_mib_per_rank": easy_block, "ior_hard_segments_per_rank": hard_segs,
                              "mdtest_easy_n": md_easy_n, "mdtest_hard_n": md_hard_n,
                              "client_threads": args.client_threads, "attr_ttl_s": args.attr_ttl,
                              "client": mode, "intercept_io_threads": args.intercept_io_threads,
                              "ost_tier": args.ost_tier, "ost_ram_cache_mib": ram_mib},
                      servers=servers, snapshots=[{"phase": p, **s} for p, s in snaps])
        from tools import io500_model
        if args.project == "auto":
            prof = auto_profile(facts, n, args) if facts else io500_model.load_profile("10x32c-100g")
        else:
            prof = io500_model.load_profile(args.project)
        report["node_facts"] = facts
        report["analysis"] = io500_model.analyze(report, prof)
    finally:
        for i in range(n):
            try:
                cluster.run(i, ["umount", "-l", MNT], net=False, capture_output=True, timeout=30)
            except Exception:  # noqa: BLE001
                pass
        for p in fuse:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        cluster.down()
        # the OSTs' tier files hold this run's data only: removed with it (a
        # raw namespace -- file:/dev/... -- is left as it is)
        for s in servers:
            spec = s.get("tier") or ""
            if spec.startswith("file:") and cluster.real:
                path = spec[5:].rsplit(":", 1)[0] if re.search(r":\d+$", spec[5:]) else spec[5:]
                if not path.startswith("/dev/"):
                    try:
                        cluster.run(s["node"], ["rm", "-f", path], env, net=False, capture_output=True, timeout=120)
                    except (subprocess.SubprocessError, OSError):
                        pass
        with open(args.report, "w") as f:
            json.dump(report, f, indent=1)
    from tools import io500_model
    print(io500_model.render_text(report))
    print(f"report: {args.report}")
    return 0 if report.get("returncode") == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
