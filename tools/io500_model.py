"""Bottleneck analysis of an IO500 run (tools/io500_run.py) and projection
onto other hardware.

Measured, per IO500 phase, from counter deltas between phases:
  - CPU per operation of every component: client (FUSE daemon + the MPI
    ranks' own time incl. the kernel FUSE path), MDT and OST handlers
    (inside the request handler), server transport; the RDMA emulator's CPU
    (softverbs threads) is reported but NOT carried over -- a real NIC
    does that work
  - RPCs and bytes per operation, per server kind
  - load skew: the busiest MDT's / OST's share of that kind's requests, and
    the MDT lock hold time per request (the MDT has one lock, so it
    serializes)
An "operation" is IO500's own unit: a byte for ior phases, a file op for
mdtest/find.

Projected: each phase's throughput on a hardware profile is the MINIMUM
over these limits (a bottleneck, i.e. operational-law, model):
  cpu_cluster  all nodes' cores / total CPU per op
  cpu_hot_node the busiest node: its share of server work + its clients
  mdt_lock     1 / (hold time per op on the hottest MDT)
  nic          per-node link bandwidth for the bytes that cross the network
  latency      ranks x (1 / per-op latency: RPC round trips + CPU)
  capacity     (valid runs) a 300 s write phase must fit in the pool's RAM
The binding limit is reported as that phase's bottleneck. Profiles are
estimates of the target machines, not measurements -- edit them, or pass a
JSON file, to match the real hardware; the on-site run then replaces every
number with measured ones.
"""
from __future__ import annotations

import json
import math
import os

PROFILES = {
    # 10 servers like WEKA's 10-node IO500 entries: 2 sockets x 16 cores, 100 GbE RoCE, 512 GiB.
    # The fuse_* / lock_convoy / *_us values are engineering estimates for a
    # current Linux kernel and RoCE NIC, not measurements; the on-site run
    # measures the real thing and its report supersedes them. FUSE: stock
    # FUSE stops scaling past 8-16 threads and a CREATE round trip costs
    # ~100 us on a null filesystem, ~39 us of it wake-ups (RFUSE, FAST'24).
    "10x32c-100g": {"nodes": 10, "cores": 32, "cpu_speed": 1.0, "nic_gbps": 100, "ram_gib": 512,
                    "ranks_per_node": 32, "client_threads": 16, "rtt_us": 6.0, "rdma_msg_cpu_us": 2.0,
                    "memcpy_gb_s": 10.0, "cpu_util": 0.85, "nic_eff": 0.85, "mem_usable": 0.6,
                    "fuse_mount_kops": 150.0, "fuse_mount_gib_s": 4.0, "fuse_req_overhead_us": 25.0,
                    "lock_convoy": 1.5,
                    # the OST's NVMe tier per node (2 x 3.84 TB PCIe 4.0 drives), used when the run
                    # had the tier or the nvme_tier scenario adds it: capacity, and write bandwidth
                    # (an estimate until measured; 'auto' profiles take the hosts' real namespaces)
                    "nvme_gib": 7150.0, "nvme_gib_s": 5.0},
    "10x64c-200g": {"nodes": 10, "cores": 64, "cpu_speed": 1.0, "nic_gbps": 200, "ram_gib": 1024,
                    "ranks_per_node": 64, "client_threads": 32, "rtt_us": 5.0, "rdma_msg_cpu_us": 2.0,
                    "memcpy_gb_s": 12.0, "cpu_util": 0.85, "nic_eff": 0.85, "mem_usable": 0.6,
                    "fuse_mount_kops": 150.0, "fuse_mount_gib_s": 4.0, "fuse_req_overhead_us": 25.0,
                    "lock_convoy": 1.5, "nvme_gib": 14300.0, "nvme_gib_s": 8.0},
}
FUSE_OPS = {1: "LOOKUP", 2: "FORGET", 3: "GETATTR", 4: "SETATTR", 9: "MKDIR", 10: "UNLINK", 11: "RMDIR",
            14: "OPEN", 15: "READ", 16: "WRITE", 17: "STATFS", 18: "RELEASE", 20: "FSYNC", 25: "FLUSH",
            27: "OPENDIR", 28: "READDIR", 29: "RELEASEDIR", 35: "CREATE", 42: "BATCH_FORGET", 44: "READDIRPLUS"}
BW_PHASES = ["ior-easy-write", "ior-easy-read", "ior-hard-write", "ior-hard-read"]
MD_PHASES = ["mdtest-easy-write", "mdtest-easy-stat", "mdtest-easy-delete", "mdtest-hard-write",
             "mdtest-hard-stat", "mdtest-hard-read", "mdtest-hard-delete", "find"]
IOR_XFER = {"ior-easy-write": 2 * 2**20, "ior-easy-read": 2 * 2**20, "ior-hard-write": 47008, "ior-hard-read": 47008}
LIMIT_TEXT = {
    "cpu_cluster": "total CPU of all nodes",
    "cpu_hot_node": "CPU of the busiest node (hot server + its clients)",
    "client_threads": "client threads per node (FUSE workers, or the ranks themselves with the interception library)",
    "mdt_lock": "the hot MDT's lock(s): hold time per request / lock stripes",
    "nic": "NIC bandwidth",
    "latency": "per-rank latency (ranks x round trips)",
    "fuse_mount": "kernel FUSE channel per mount",
    "shared_file_lock": "one file's inode lock per node (kernel FUSE write path)",
    "capacity": "RAM to hold a 300 s write phase",
    "nvme_capacity": "NVMe tier capacity for a 300 s write phase",
    "nvme_bw": "NVMe tier write bandwidth",
}


def load_profile(name: str) -> dict:
    if os.path.exists(name):
        with open(name) as f:
            return json.load(f)
    return dict(PROFILES[name], name=name)


def _geo(v: list[float]) -> float:
    v = [x for x in v if x and x > 0]
    return math.exp(sum(map(math.log, v)) / len(v)) if v else 0.0


def _cpu(proc: dict) -> dict[str, float]:
    return {g: c["user"] + c["sys"] for g, c in proc["cpu"].items()}


def _split_exited(proc: dict, groups: dict[str, float], ex: float) -> None:
    """CPU of threads that ended between two snapshots: split like the
    process's live threads' cumulative CPU (connection threads come and go)."""
    if ex <= 0:
        return
    base = {g: v for g, v in _cpu(proc).items() if g not in ("exited", "other") and v > 0}
    if not base:
        base = {"emulator": 1.0, "server": 1.0} if proc["role"] in ("mdt", "ost") else \
               {"fuse_worker": 1.0} if proc["role"] == "fuse" else {"other": 1.0}
    tot = sum(base.values())
    for g, v in base.items():
        groups[g] = groups.get(g, 0.0) + ex * v / tot


def phase_deltas(report: dict) -> dict[str, dict]:
    """Counter deltas for each phase: snapshot at a phase's pause check to
    the next snapshot."""
    snaps = report["snapshots"]
    out = {}
    for a, b in zip(snaps, snaps[1:]):
        ph = a["phase"]
        if ph not in BW_PHASES + MD_PHASES:
            continue
        d = {"wall_s": b["t"] - a["t"], "servers": {}, "cpu": {}, "fuse": {}, "hosts": {}, "missing": []}
        for addr, sb in b["servers"].items():
            sa = a["servers"].get(addr, {})
            if "error" in sb or "error" in sa:
                d["missing"].append(f"server {addr}")
                continue
            d["servers"][addr] = {k: sb.get(k, 0) - sa.get(k, 0) for k in
                                  ("reqs", "handler_cpu_ns", "handler_wall_ns", "bytes_in", "bytes_out",
                                   "lock_hold_ns", "lock_wait_ns")}
            d["servers"][addr]["kind"] = sb["kind"]
            d["servers"][addr]["lock_stripes"] = sb.get("lock_stripes", 1)  # a property, not a counter
            if sb.get("tier"):  # the capacity tier: properties, and the phase's direct writes
                d["servers"][addr]["tier"] = sb["tier"]
                d["servers"][addr]["tier_capacity"] = sb.get("tier_capacity", 0)
                d["servers"][addr]["direct_writes"] = sb.get("direct_writes", 0) - sa.get("direct_writes", 0)
        for h, hb in (b.get("hosts") or {}).items():
            ha = (a.get("hosts") or {}).get(h)
            if "error" in hb or not ha or "error" in ha:
                d["missing"].append(f"node {h} processes")  # its CPU is not in this phase's costs
                continue
            d["hosts"][h] = {**{k: hb[k] - ha.get(k, 0) for k in hb if k != "cores"}, "cores": hb.get("cores", 1)}
        # PIDs repeat across hosts: a process is (node, pid)
        pa = {(p["node"], p["pid"]): p for p in a["procs"]}
        for p in b["procs"]:
            if (p["node"], p["pid"]) not in pa:
                continue
            ca, cb = _cpu(pa[(p["node"], p["pid"])]), _cpu(p)
            groups = {g: max(0.0, cb.get(g, 0) - ca.get(g, 0)) for g in cb}
            _split_exited(p, groups, groups.pop("exited", 0.0))
            key = (p["node"], p["role"])
            tgt = d["cpu"].setdefault(key, {})
            for g, v in groups.items():
                tgt[g] = tgt.get(g, 0.0) + v
            fa, fb = pa[(p["node"], p["pid"])].get("fuse_stats"), p.get("fuse_stats")
            if fa and fb:
                f = d["fuse"]
                for op, (cnt, ns) in fb["ops"].items():
                    c0, n0 = fa["ops"].get(op, (0, 0))
                    e = f.setdefault(int(op), [0, 0])
                    e[0] += cnt - c0
                    e[1] += ns - n0
        out[ph] = d
    return out


def measure(report: dict) -> dict[str, dict]:
    """Per-op costs of each phase."""
    res = {}
    n = report["nodes"]
    for ph, d in phase_deltas(report).items():
        r = report["phases"].get(ph)
        if not r:
            continue
        if r.get("ops"):  # the phase's own count (tools/io500_run.phase_ops)
            ops = r["ops"]
        elif r["unit"] == "GiB/s":  # fallback: overstates the work (time incl. setup)
            ops = r["value"] * 2**30 * r["time_s"]  # bytes
        else:
            ops = r["value"] * 1000 * r["time_s"]  # file ops
        if ops <= 0:
            continue
        cpu = {"client": 0.0, "ranks": 0.0, "server_handler": 0.0, "server_transport": 0.0, "emulator": 0.0}
        node_srv = [0.0] * n
        for (node, role), groups in d["cpu"].items():
            emu = groups.get("emulator", 0.0)
            cpu["emulator"] += emu
            rest = sum(v for g, v in groups.items() if g != "emulator")
            if role == "fuse":
                cpu["client"] += rest
            elif role == "io500":
                cpu["ranks"] += rest
            else:
                node_srv[node] += rest
        kinds = {}
        for addr, s in d["servers"].items():
            k = kinds.setdefault(s["kind"], {"reqs": 0, "cpu_ns": 0, "bytes": 0, "max_reqs": 0, "hold_ns": 0,
                                             "hot_hold_ns": 0, "hot_cpu_ns": 0})
            k["reqs"] += s["reqs"]
            k["cpu_ns"] += s["handler_cpu_ns"]
            k["bytes"] += s["bytes_in"] + s["bytes_out"]
            k["hold_ns"] += s.get("lock_hold_ns", 0)
            k["lock_stripes"] = max(k.get("lock_stripes", 1), s.get("lock_stripes", 1))
            if s.get("tier"):
                k["tier"] = s["tier"]
                k["tier_capacity"] = k.get("tier_capacity", 0) + s.get("tier_capacity", 0)
                k["direct_writes"] = k.get("direct_writes", 0) + s.get("direct_writes", 0)
            if s["reqs"] > k["max_reqs"]:
                k.update(max_reqs=s["reqs"], hot_hold_ns=s.get("lock_hold_ns", 0), hot_cpu_ns=s["handler_cpu_ns"])
        handler = sum(k["cpu_ns"] for k in kinds.values()) / 1e9
        srv_threads = sum(node_srv)
        cpu["server_handler"] = handler
        cpu["server_transport"] = max(0.0, srv_threads - handler)
        m = {"ops": ops, "nodes": n, "unit": "byte" if r["unit"] == "GiB/s" else "op", "measured": r["value"],
             "measured_unit": r["unit"], "time_s": r["time_s"],
             "cpu_s_per_op": {k: v / ops for k, v in cpu.items()},
             "emulator_share": cpu["emulator"] / max(1e-9, sum(cpu.values())), "kinds": {},
             "missing_data": d["missing"]}
        freqs = {FUSE_OPS.get(op, str(op)): c for op, (c, _) in d["fuse"].items() if c > 0}
        nreq = sum(freqs.values())
        m["fuse"] = {"reqs_per_op": nreq / ops, "mix_per_op": {k: v / ops for k, v in sorted(freqs.items(),
                                                                                               key=lambda x: -x[1])},
                     "client_cpu_us_per_req": cpu["client"] / nreq * 1e6 if nreq else 0.0,
                     "writes_per_op": freqs.get("WRITE", 0) / ops}
        busy = idle = 0.0
        for h in d["hosts"].values():
            idle += h.get("idle", 0) + h.get("iowait", 0)
            busy += sum(h.get(k, 0) for k in ("user", "nice", "system", "irq", "softirq", "steal"))
        if busy + idle:
            m["host_busy"] = busy / (busy + idle)
            m["host_softirq_share"] = sum(h.get("softirq", 0) for h in d["hosts"].values()) / max(1e-9, busy)
        for kind, k in kinds.items():
            share = k["max_reqs"] / k["reqs"] if k["reqs"] else 0.0
            m["kinds"][kind] = {"rpcs_per_op": k["reqs"] / ops, "bytes_per_op": k["bytes"] / ops,
                                "hot_share": share, "servers": sum(1 for s in d["servers"].values() if s["kind"] == kind),
                                "cpu_us_per_req": k["cpu_ns"] / 1e3 / k["reqs"] if k["reqs"] else 0.0,
                                "hold_us_per_req_hot": k["hot_hold_ns"] / 1e3 / k["max_reqs"] if k["max_reqs"] else 0.0,
                                "lock_stripes": k.get("lock_stripes", 1),
                                "hot_cpu_us_per_req": k["hot_cpu_ns"] / 1e3 / k["max_reqs"] if k["max_reqs"] else 0.0}
            if k.get("tier"):
                m["kinds"][kind].update(tier=k["tier"], tier_capacity=k["tier_capacity"],
                                        direct_writes_per_op=k.get("direct_writes", 0) / ops)
        res[ph] = m
    return res


def project(meas: dict[str, dict], prof: dict, sim_nodes: int) -> dict[str, dict]:
    N, C, s = prof["nodes"], prof["cores"], prof["cpu_speed"]
    cap_cpu = C * s * prof["cpu_util"]
    msg_s = prof["rdma_msg_cpu_us"] * 1e-6
    out = {}
    for ph, m in meas.items():
        c = m["cpu_s_per_op"]
        rpcs = sum(k["rpcs_per_op"] for k in m["kinds"].values())
        wire_bytes = sum(k["bytes_per_op"] for k in m["kinds"].values())
        # real RDMA transport instead of the emulator: a fixed cost per message
        # on each side + one copy of every byte on each side
        transport = rpcs * 2 * msg_s + wire_bytes * 2 / (prof["memcpy_gb_s"] * 1e9)
        client = c["client"] + c["ranks"]
        server = c["server_handler"]
        total = (client + server + transport) / s
        lim = {"cpu_cluster": N * cap_cpu / total if total > 0 else float("inf")}
        # the busiest node: its share of each server kind's work + 1/N of the clients
        hot = 0.0
        srv_req_s = 0.0  # server time per op seen by a waiting client
        for kind, k in m["kinds"].items():
            uniform = k["hot_share"] <= 2.0 / max(1, k["servers"])
            share = 1.0 / N if uniform else k["hot_share"]
            kind_cpu = k["rpcs_per_op"] * k["cpu_us_per_req"] * 1e-6
            kind_tr = k["rpcs_per_op"] * msg_s + k["bytes_per_op"] / (prof["memcpy_gb_s"] * 1e9)
            hot += share * (kind_cpu + kind_tr)
            srv_req_s += (kind_cpu + kind_tr) / s
            if kind == "mdt" and k["rpcs_per_op"]:
                # the MDT's lock(s): one lock serializes the MDT; with
                # lock_stripes independently locked buckets per directory
                # (rocepool_servers.c MD_BUCKETS) that many requests proceed
                # at once, so the ceiling scales with the stripes -- up to the
                # MDT's cores, which cpu_hot_node covers
                hold = k["hot_cpu_us_per_req"] * 1e-6 * prof["lock_convoy"] / s
                stripes = min(k.get("lock_stripes", 1), C)
                lim["mdt_lock"] = stripes / (hold * k["rpcs_per_op"] * share) if hold else float("inf")
        hot += (client + transport / 2) / N
        lim["cpu_hot_node"] = cap_cpu / (hot / s) if hot > 0 else float("inf")
        f = m.get("fuse", {})
        freq = f.get("reqs_per_op", 0.0)
        net_s = rpcs * prof["rtt_us"] * 1e-6
        # a FUSE worker thread is busy for the whole request, incl. its RPC
        # waits; with the interception library the rank itself is the client
        # thread (one in flight per rank), and there is no FUSE channel at all
        icp = bool(prof.get("intercept"))
        per_op_thread_s = c["client"] / s + freq * prof["fuse_req_overhead_us"] * 1e-6 + net_s + srv_req_s
        threads = prof["ranks_per_node"] if icp else prof["client_threads"]
        lim["client_threads"] = N * threads / per_op_thread_s if per_op_thread_s else float("inf")
        if m["unit"] == "byte":
            if not icp:
                lim["fuse_mount"] = N * prof["fuse_mount_gib_s"] * 2**30
            # converged nodes: a node sends its clients' bytes and receives its OSTs' share
            lim["nic"] = N * prof["nic_gbps"] / 8 * 1e9 * prof["nic_eff"] * N / max(1, N - 1)
            per_op = IOR_XFER.get(ph, 2**20)
        else:
            if freq and not icp:
                lim["fuse_mount"] = N * prof["fuse_mount_kops"] * 1000 / freq
            per_op = 1
        if ph == "ior-hard-write" and f.get("writes_per_op") and not icp:
            # every rank on a node writes the SAME file: the kernel's FUSE
            # write path holds that inode's lock across each WRITE request
            wreq_s = f["client_cpu_us_per_req"] * 1e-6 / s + prof["fuse_req_overhead_us"] * 1e-6
            lim["shared_file_lock"] = N / (wreq_s * f["writes_per_op"])
        lat = (net_s + (client + server + transport) / s + freq * prof["fuse_req_overhead_us"] * 1e-6) * per_op
        lim["latency"] = N * prof["ranks_per_node"] * per_op / lat if lat else float("inf")
        if ph in ("ior-easy-write", "ior-hard-write"):
            share_of_ram = 0.75 if ph == "ior-easy-write" else 0.25
            ost = m["kinds"].get("ost", {})
            if ost.get("tier") or prof.get("tier"):
                # the OSTs write through to NVMe (the run had the tier, or the
                # scenario adds it): the 300 s write phase must fit the drives
                # (the profile's; else what the run's servers reported, per
                # node) and their write bandwidth
                per_node = prof["nvme_gib"] * 2**30 if prof.get("nvme_gib") else ost.get("tier_capacity", 0) / max(1, m["nodes"])
                lim["nvme_capacity"] = N * per_node * share_of_ram / 300.0 if per_node else float("inf")
                lim["nvme_bw"] = N * prof["nvme_gib_s"] * 2**30
            else:
                lim["capacity"] = N * prof["ram_gib"] * 2**30 * prof["mem_usable"] * share_of_ram / 300.0
        best = min(lim, key=lim.get)
        x = lim[best]
        unit = "GiB/s" if m["unit"] == "byte" else "kIOPS"
        conv = (lambda v: v / 2**30) if m["unit"] == "byte" else (lambda v: v / 1000)
        second = sorted(((v, k) for k, v in lim.items() if k != best))[:1]
        out[ph] = {"value": conv(x), "unit": unit, "bottleneck": best,
                   "next_limit": {"name": second[0][1], "value": conv(second[0][0])} if second else None,
                   "limits": {k: conv(v) for k, v in lim.items()}}
    bw = _geo([out[p]["value"] for p in BW_PHASES if p in out])
    md = _geo([out[p]["value"] for p in MD_PHASES if p in out])
    return {"phases": out, "bw_gib_s": bw, "md_kiops": md, "score": math.sqrt(bw * md)}


# estimates the projection depends on, with a pessimistic and an optimistic value
SENSITIVITY = {"fuse_mount_kops": (100.0, 300.0), "fuse_mount_gib_s": (2.0, 6.0), "fuse_req_overhead_us": (40.0, 10.0),
               "nvme_gib_s": (3.0, 8.0),
               "lock_convoy": (2.5, 1.0), "rdma_msg_cpu_us": (4.0, 1.0), "cpu_speed": (0.7, 1.3),
               "rtt_us": (10.0, 3.0)}

# design changes, each as an edit to the measured per-op costs or the profile
SCENARIOS = [
    ("as_is", "as measured, valid 300 s run", {}),
    ("dir_sharding", "+ one directory's entries spread over all MDTs (name hash), finer MDT locks "
                     "(done: ROCEPOOL_DIR_STRIPE=1 + MD_BUCKETS; a no-op on a run that already has them)",
     {"uniform_mdt": True, "lock_stripes": 64}),
    ("parallel_writes", "+ FOPEN_PARALLEL_DIRECT_WRITES: no per-node inode lock on a shared file",
     {"no_shared_lock": True}),
    ("fuse_uring", "+ FUSE over io_uring (per-core queues, Linux 6.14+): ~3x per-mount rate, less wake-up cost "
                   "(implemented: io500_run.py probes it against /dev/fuse on the hosts and uses the faster; "
                   "a no-op on a run that already used it)",
     {"fuse_mount_kops": 3.0, "fuse_mount_gib_s": 2.0, "fuse_req_overhead_us": 0.4, "uring": True}),
    ("intercept", "+ the LD_PRELOAD interception library in every rank (no FUSE channel, no kernel on the "
                  "I/O path; implemented: io500_run.py --client auto probes it against FUSE and uses the faster; "
                  "a no-op on a run that already used it)",
     {"intercept": True}),
    ("one_sided", "+ one-sided reads and lookups (client RDMA READs stripes and MDT entries; implemented: "
                  "io500_run.py --direct-read auto probes it; a no-op on a run that already used it -- its "
                  "effect is in the measured per-op server CPU)", {}),
    ("nvme_tier", "+ the OST's NVMe tier for the 300 s write phases (implemented: roce/native/ost_tier.c, "
                  "io500_run.py --ost-tier auto; a no-op on a run that already had it -- its limits are then the "
                  "drives' capacity and write bandwidth)",
     {"tier": True}),
]


def _scenario(meas: dict, prof: dict, edits: dict, n: int) -> dict:
    m2 = json.loads(json.dumps(meas))
    p2 = dict(prof)
    if edits.get("uring") and prof.get("uring_measured"):
        edits = {k: v for k, v in edits.items() if k not in ("fuse_mount_kops", "fuse_mount_gib_s", "fuse_req_overhead_us")}
    for k, v in edits.items():
        if k in p2 and k not in ("intercept", "tier"):
            p2[k] = p2[k] * v
    if edits.get("intercept"):
        p2["intercept"] = True
    if edits.get("tier"):
        p2["tier"] = True
    if edits.get("uniform_mdt"):
        for m in m2.values():
            if "mdt" in m["kinds"]:
                m["kinds"]["mdt"]["hot_share"] = 1.0 / max(1, m["kinds"]["mdt"]["servers"])
                m["kinds"]["mdt"]["lock_stripes"] = max(m["kinds"]["mdt"].get("lock_stripes", 1),
                                                        edits.get("lock_stripes", 1))
    proj = project(m2, p2, n)
    for ph, r in proj["phases"].items():
        drop = [k for k, on in (("shared_file_lock", edits.get("no_shared_lock")),
                                ("capacity", edits.get("no_capacity"))) if on and k in r["limits"]]
        if drop:
            lim = {k: v for k, v in r["limits"].items() if k not in drop}
            r["bottleneck"] = min(lim, key=lim.get)
            r["value"] = lim[r["bottleneck"]]
    proj["bw_gib_s"] = _geo([proj["phases"][p]["value"] for p in BW_PHASES if p in proj["phases"]])
    proj["md_kiops"] = _geo([proj["phases"][p]["value"] for p in MD_PHASES if p in proj["phases"]])
    proj["score"] = math.sqrt(proj["bw_gib_s"] * proj["md_kiops"])
    return proj


def calibration(report: dict, meas: dict) -> dict:
    """Check of the measured per-op costs on a simulated run, where every
    node shares this host's cores and the run is CPU-bound: the cores
    divided by the total CPU per op (emulator included) must reproduce each
    phase's measured rate. A ratio far from 1 means CPU went unaccounted
    (or the phase was not CPU-bound), and the projection should not be
    trusted for that phase."""
    cores = report.get("cpu_cores_here") or os.cpu_count()
    out = {}
    for ph, m in meas.items():
        tot = sum(m["cpu_s_per_op"].values())
        if tot <= 0:
            continue
        unit = 2**30 if m["unit"] == "byte" else 1000
        out[ph] = {"predicted": cores / tot / unit, "measured": m["measured"], "ratio": cores / tot / unit / m["measured"]}
    return out


def analyze(report: dict, prof: dict) -> dict:
    prof = dict(prof)
    if report.get("client") == "intercept":
        # the ranks preloaded the interception library: the per-op client
        # CPU measured is theirs (role io500), there is no FUSE channel
        prof["intercept"] = True
    if report.get("fuse_channel") == "io_uring":
        # the run used FUSE over io_uring: the per-mount channel estimates
        # are those of the io_uring path (the "fuse_uring" scenario's
        # factors, still estimates until measured on many-core hosts)
        prof.update(fuse_mount_kops=prof["fuse_mount_kops"] * 3.0, fuse_mount_gib_s=prof["fuse_mount_gib_s"] * 2.0,
                    fuse_req_overhead_us=prof["fuse_req_overhead_us"] * 0.4, uring_measured=True)
    meas = measure(report)
    proj = project(meas, prof, report["nodes"])
    lo, hi = dict(prof), dict(prof)
    for k, (a, b) in SENSITIVITY.items():
        lo[k], hi[k] = a, b
    band = {"pessimistic": project(meas, lo, report["nodes"])["score"],
            "optimistic": project(meas, hi, report["nodes"])["score"]}
    one_at_a_time = {}
    for k, (a, b) in SENSITIVITY.items():
        one_at_a_time[k] = [project(meas, dict(prof, **{k: a}), report["nodes"])["score"],
                            project(meas, dict(prof, **{k: b}), report["nodes"])["score"]]
    scen, acc = [], {}
    for key, text, edits in SCENARIOS:
        acc.update(edits)  # cumulative: each row adds its fix to the ones above
        sp = _scenario(meas, prof, acc, report["nodes"])
        scen.append({"key": key, "text": text, "score": sp["score"], "bw_gib_s": sp["bw_gib_s"],
                     "md_kiops": sp["md_kiops"],
                     "bottlenecks": {ph: r["bottleneck"] for ph, r in sp["phases"].items()}})
    ref = report.get("reference", {}).get("WEKA_Afton_10node")
    res = {"measured_per_op": meas, "profile": prof, "projection": proj, "score_band": band,
           "sensitivity": one_at_a_time, "scenarios": scen}
    if str(report.get("cluster", "")).startswith("simulated"):
        res["calibration"] = calibration(report, meas)
    if ref:
        res["vs_weka"] = {"score_ratio": ref["score"] / proj["score"] if proj["score"] else None,
                          "bw_ratio": ref["bw_gib_s"] / proj["bw_gib_s"] if proj["bw_gib_s"] else None,
                          "md_ratio": ref["md_kiops"] / proj["md_kiops"] if proj["md_kiops"] else None}
    return res


def render_text(report: dict) -> str:
    a = report.get("analysis") or {}
    lines = [f"IO500 on {report.get('cluster')}", f"  measured score: {report.get('score')}"]
    meas, proj = a.get("measured_per_op", {}), a.get("projection", {})
    if meas:
        lines.append(f"  {'phase':20s} {'measured':>14s}  {'emu%':>4s}  {'rpc/op':>8s}  {'hot MDT':>7s}  "
                     f"{'projected':>10s}  bottleneck")
        for ph in BW_PHASES + MD_PHASES:
            m, p = meas.get(ph), proj.get("phases", {}).get(ph)
            if not m or not p:
                continue
            rpc = sum(k["rpcs_per_op"] for k in m["kinds"].values()) * (IOR_XFER.get(ph, 1) if m["unit"] == "byte" else 1)
            hot = m["kinds"].get("mdt", {}).get("hot_share", 0)
            lines.append(f"  {ph:20s} {m['measured']:9.3f} {m['measured_unit']:5s} {m['emulator_share'] * 100:4.0f}  "
                         f"{rpc:8.2f}  {hot * 100:6.0f}%  {p['value']:7.2f} {p['unit']:5s}  "
                         f"{LIMIT_TEXT[p['bottleneck']]}")
        prof = a["profile"]
        lines.append(f"  projected on {prof.get('name', 'profile')} ({prof['nodes']} nodes x {prof['cores']} cores, "
                     f"{prof['nic_gbps']} GbE): bw {proj['bw_gib_s']:.2f} GiB/s, md {proj['md_kiops']:.1f} kIOPS, "
                     f"score {proj['score']:.2f}")
        gaps = sorted({x for m in meas.values() for x in m.get("missing_data", [])})
        if gaps:
            lines.append(f"  WARNING: counters missing in some phases ({', '.join(gaps[:6])}): those phases' "
                         f"per-op costs are too LOW and their projections too optimistic")
        cal = a.get("calibration")
        if cal:
            rs = [c["ratio"] for c in cal.values()]
            lines.append(f"  model check on this run (cores / CPU per op vs measured): {min(rs):.2f} .. {max(rs):.2f}x "
                         f"over {len(rs)} phases (1.00 = the per-op costs account for all the work)")
        band = a.get("score_band")
        if band:
            lines.append(f"  score range over the estimated inputs: {band['pessimistic']:.1f} .. {band['optimistic']:.1f}")
        v = a.get("vs_weka")
        if v and v["score_ratio"]:
            lines.append(f"  vs WEKA Afton (score 105.94): {v['score_ratio']:.2f}x "
                         f"(bw {v['bw_ratio']:.2f}x, md {v['md_ratio']:.2f}x; >1 = WEKA ahead)")
        if a.get("scenarios"):
            lines.append("  what the fixes are worth (cumulative):")
            for sc in a["scenarios"]:
                lines.append(f"    {sc['score']:7.1f}  (bw {sc['bw_gib_s']:6.2f} GiB/s, md {sc['md_kiops']:7.1f} kIOPS)  "
                             f"{sc['text']}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    rep = json.load(open(sys.argv[1]))
    rep["analysis"] = analyze(rep, load_profile(sys.argv[2] if len(sys.argv) > 2 else "10x32c-100g"))
    print(render_text(rep))
