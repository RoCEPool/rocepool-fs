"""GPUDirect Storage on this host: what the GPUs and the RDMA NIC allow, and
how fast rocepool streams file data into GPU memory.

    python3 tools/bench_gds.py --inventory          # GPUs, driver, dma-buf / peermem, GPU<->NIC PCIe path
    python3 tools/bench_gds.py                      # + the benchmark (crossover rig, or ROCEPOOL_TOPOLOGY)
    python3 tools/bench_gds.py --emulate            # no GPU: the emulated device (tests)

The benchmark starts 1 MDT + N OSTs (tools/bench_ior.start_dist_servers),
writes one file through the interception library and reads it into GPU
memory with tests/fakes/gds_probe.c -- a program linked against
libcufile.so.0, i.e. rocepool's cuFile library (roce/native/cufile/):
  direct     the NIC writes the stripes straight into GPU memory (dma-buf or
             nvidia-peermem registration of the buffer)
  bounced    the same reads through host memory + cuMemcpy (ROCEPOOL_GDS_BOUNCE=1):
             what a storage system without GPUDirect support gets
  all GPUs   one reader per GPU at once (direct), aggregate GB/s
Report: gds_report.json (or --report).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PROBE_SRC = os.path.join(ROOT, "tests", "fakes", "gds_probe.c")


# ---- inventory -----------------------------------------------------------------
def _bdf(nvsmi_bus_id: str) -> str:
    """nvidia-smi's 00000000:3B:00.0 -> sysfs's 0000:3b:00.0"""
    dom, rest = nvsmi_bus_id.strip().split(":", 1)
    return f"{int(dom, 16):04x}:{rest.lower()}"


def pcie_path(bdf: str, sysfs: str = "/") -> list[str]:
    """the PCI devices from the root complex down to `bdf`"""
    p = os.path.realpath(os.path.join(sysfs, "sys/bus/pci/devices", bdf))
    parts = p.split(os.sep)
    i = next((k for k, x in enumerate(parts) if x.startswith("pci")), None)
    return parts[i:] if i is not None else []


def pcie_relation(a: list[str], b: list[str], numa_a: int = -1, numa_b: int = -1) -> str:
    """nvidia-smi topo's words: PIX/PXB (a PCIe switch between them, the best
    for GPUDirect), PHB (the same root complex), NODE (same NUMA node),
    SYS (across sockets)"""
    if not a or not b:
        return "unknown"
    if a[0] != b[0]:
        return "NODE" if numa_a == numa_b and numa_a >= 0 else "SYS"
    common = 0
    for x, y in zip(a, b):
        if x != y:
            break
        common += 1
    # [pci0000:xx, root port, switch up, switch down, device]: past the root port -> a switch
    if common >= 3:
        return "PIX" if common >= len(a) - 1 and common >= len(b) - 1 else "PXB"
    return "PHB"


def _read(path: str) -> str:
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return ""


def gpu_inventory(*, fix: bool = False, sysfs: str = "/", run=subprocess.run) -> dict:
    """GPUs (nvidia-smi), driver, whether GPU memory can be registered with
    the NIC (dma-buf: open kernel modules; or nvidia-peermem, loaded here
    when `fix`), and each GPU's PCIe relation to each RDMA device."""
    inv: dict = {"gpus": [], "driver": "", "open_kernel_modules": None, "peermem": False, "dmabuf": False,
                 "nics": {}, "notes": []}
    try:
        r = run(["nvidia-smi", "--query-gpu=index,name,pci.bus_id,memory.total,driver_version",
                 "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        r = None
    if r is None or r.returncode != 0:
        inv["notes"].append("no NVIDIA GPU (nvidia-smi not found or failed)")
        return inv
    for line in r.stdout.strip().splitlines():
        f = [x.strip() for x in line.split(",")]
        if len(f) < 5:
            continue
        bdf = _bdf(f[2])
        inv["gpus"].append({"index": int(f[0]), "name": f[1], "bdf": bdf, "mem_mib": int(float(f[3]) or 0),
                            "numa": int(_read(os.path.join(sysfs, "sys/bus/pci/devices", bdf, "numa_node")) or -1)})
        inv["driver"] = f[4]
    lic = run(["modinfo", "-F", "license", "nvidia"], capture_output=True, text=True, timeout=30)
    if lic.returncode == 0 and lic.stdout.strip():
        inv["open_kernel_modules"] = "MIT" in lic.stdout or "GPL" in lic.stdout
    # dma-buf export of GPU memory: the open kernel modules (R515+) on Linux 5.12+
    kv = tuple(int(x) for x in re.findall(r"\d+", os.uname().release)[:2])
    inv["dmabuf"] = bool(inv["open_kernel_modules"]) and kv >= (5, 12)
    mod = lambda: any(os.path.exists(os.path.join(sysfs, "sys/module", m))  # noqa: E731
                      for m in ("nvidia_peermem", "nv_peer_mem"))
    if not mod() and fix:
        run(["modprobe", "nvidia_peermem"], capture_output=True, text=True, timeout=60)
    inv["peermem"] = mod()
    if not inv["dmabuf"] and not inv["peermem"]:
        inv["notes"].append("GPU memory cannot be registered with the NIC (no dma-buf: proprietary kernel modules "
                            "or kernel < 5.12; no nvidia-peermem): GPU reads bounce through host memory")
    for dev in sorted(glob.glob(os.path.join(sysfs, "sys/class/infiniband/*"))):
        name = os.path.basename(dev)
        nbdf = os.path.basename(os.path.realpath(os.path.join(dev, "device")))
        npath = pcie_path(nbdf, sysfs)
        numa = int(_read(os.path.join(dev, "device", "numa_node")) or -1)
        inv["nics"][name] = {"bdf": nbdf, "numa": numa,
                             "gpus": {g["index"]: pcie_relation(pcie_path(g["bdf"], sysfs), npath, g["numa"], numa)
                                      for g in inv["gpus"]}}
    far = [f"GPU{g} - {n}" for n, v in inv["nics"].items() for g, rel in v["gpus"].items() if rel == "SYS"]
    if far and inv["gpus"]:
        inv["notes"].append("GPU and NIC on different CPU sockets (" + ", ".join(far[:4]) +
                            "): GPUDirect traffic crosses the socket link; pin each GPU's reader to its nearest NIC")
    return inv


# ---- benchmark -------------------------------------------------------------------
def build_probe(cufile_dir: str, out_dir: str) -> str:
    out = os.path.join(out_dir, "gds_probe")
    subprocess.run(["gcc", "-O2", "-Wall", PROBE_SRC, "-o", out, "-L", cufile_dir, "-l:libcufile.so.0", "-ldl",
                    f"-Wl,-rpath,{cufile_dir}"], check=True)
    return out


def parse_probe(out: str) -> dict:
    res: dict = {}
    for line in out.splitlines():
        f = line.split()
        if f[:1] == ["BENCH"]:
            res.update(bytes=int(f[1]), seconds=float(f[2]), gbps=float(f[3]))
        elif f[:1] == ["STATS"]:
            res.update(direct_bytes=int(f[1]), bounced_bytes=int(f[2]), posix_bytes=int(f[3]), runtime=f[4])
    return res


def bench(args, inv: dict) -> dict:
    from tools.bench_ior import start_dist_servers
    from tools.topology import Topology
    topo = Topology.current()
    if topo.real:
        native = os.path.dirname(os.path.abspath(topo.lib))
        icp, cufile_dir = os.path.join(native, "librocepool_intercept.so"), os.path.join(native, "cufile")
    else:
        from roce.native.build_lib import ensure_cufile_lib, ensure_intercept_lib
        icp = ensure_intercept_lib()
        cufile_dir = os.path.join(os.path.dirname(ensure_cufile_lib()), "cufile")
    tmp = tempfile.mkdtemp(prefix="rocepool_gds_")
    probe = build_probe(cufile_dir, tmp)
    servers, mdts, osts, env = start_dist_servers(n_mdts=1, n_osts=args.osts, topo=topo)
    size = args.size_mib << 20
    out: dict = {"topology": topo.describe, "file_mib": args.size_mib, "iters": args.iters, "osts": args.osts,
                 "results": []}
    try:
        cenv = dict(env, ROCEPOOL_PREFIX="/rocepool-gds", ROCEPOOL_MDTS=",".join(a for _, a in mdts),
                    ROCEPOOL_OSTS=",".join(a for _, a in osts), LD_LIBRARY_PATH=cufile_dir)
        if args.emulate:
            cenv["ROCEPOOL_DEV_EMU"] = "1"
        # the file, written by an ordinary program through the interception library
        w = subprocess.run(topo.launch["A"].cmd([sys.executable, "-c",
                                                 "import os\nfd=os.open('/rocepool-gds/bench.bin',os.O_WRONLY|"
                                                 f"os.O_CREAT|os.O_TRUNC,0o644)\nb=os.urandom(1<<24)\nn={size}\n"
                                                 "while n>0:\n    k=min(n,len(b)); os.write(fd,b[:k]); n-=k\n"
                                                 "os.close(fd)\n"]),
                           env=dict(cenv, LD_PRELOAD=icp), capture_output=True, text=True, timeout=1800)
        if w.returncode:
            raise RuntimeError(f"writing the benchmark file failed: {(w.stdout + w.stderr)[-800:]}")
        ngpu = len(inv["gpus"]) or 1

        def one(label: str, extra: dict, gpus: list[int]):
            procs = [subprocess.Popen(topo.launch["A"].cmd([probe, "bench", "/rocepool-gds/bench.bin", str(size),
                                                            str(args.iters)]),
                                      env=dict(cenv, LD_PRELOAD=icp, GDS_DEVICE=str(g), **extra),
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for g in gpus]
            t0 = time.monotonic()
            outs = [p.communicate(timeout=3600)[0] for p in procs]
            wall = time.monotonic() - t0
            rows = [parse_probe(o) for o in outs]
            bad = [o[-600:] for p, o, r in zip(procs, outs, rows) if p.returncode or "gbps" not in r]
            row = {"case": label, "gpus": gpus, "per_gpu": rows,
                   "gbps": round(sum(r.get("bytes", 0) for r in rows) / wall / 1e9, 3) if not bad else 0.0,
                   "direct_bytes": sum(r.get("direct_bytes", 0) for r in rows),
                   "bounced_bytes": sum(r.get("bounced_bytes", 0) for r in rows)}
            if bad:
                row["error"] = bad[0]
            out["results"].append(row)
            print(json.dumps({k: v for k, v in row.items() if k != "per_gpu"}), flush=True)

        one("direct", {}, [0])
        one("bounced", {"ROCEPOOL_GDS_BOUNCE": "1"}, [0])
        if ngpu > 1:
            one("direct_all_gpus", {}, list(range(ngpu)))
    finally:
        for s in servers:
            try:
                s.stdin.close()
            except OSError:
                pass
        for s in servers:
            try:
                s.wait(timeout=10)
            except subprocess.TimeoutExpired:
                s.kill()
        topo.down()
    return out


def summarize(rep: dict) -> tuple[str, str]:
    """(PASS/WARN/FAIL, one line) for the on-site report"""
    inv = rep["inventory"]
    if not inv["gpus"] and not rep.get("emulated"):
        return "PASS", "no NVIDIA GPU on this host: nothing to stream into (GPUDirect path covered by tests/test_gds.py)"
    res = {r["case"]: r for r in rep.get("bench", {}).get("results", [])}
    if any("error" in r for r in res.values()):
        return "FAIL", next(r["error"] for r in res.values() if "error" in r)[-400:]
    d, b = res.get("direct", {}), res.get("bounced", {})
    line = (f"{len(inv['gpus'])} GPU(s) ({inv['gpus'][0]['name']}); dma-buf {'yes' if inv['dmabuf'] else 'no'}, "
            f"peermem {'yes' if inv['peermem'] else 'no'}; " if inv["gpus"] else "emulated device; ")
    line += f"direct {d.get('gbps', 0)} GB/s, via host memory {b.get('gbps', 0)} GB/s"
    if "direct_all_gpus" in res:
        line += f", all GPUs {res['direct_all_gpus']['gbps']} GB/s"
    if inv["notes"] and inv["gpus"]:
        line += "; " + "; ".join(inv["notes"])
    direct_ok = d.get("direct_bytes", 0) > 0 and d.get("bounced_bytes", 1) == 0
    return ("PASS" if direct_ok else "WARN"), line


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--inventory", action="store_true", help="only the GPU / NIC inventory (JSON on stdout)")
    ap.add_argument("--emulate", action="store_true", help="no GPU: benchmark the emulated device")
    ap.add_argument("--size-mib", type=int, default=1024)
    ap.add_argument("--iters", type=int, default=3)
    ap.add_argument("--osts", type=int, default=4)
    ap.add_argument("--report", default=os.path.join(ROOT, "gds_report.json"))
    args = ap.parse_args()
    inv = gpu_inventory(fix=os.geteuid() == 0)
    if args.inventory:
        print(json.dumps(inv, indent=2))
        return 0
    rep: dict = {"when": time.strftime("%Y-%m-%d %H:%M:%S"), "inventory": inv, "emulated": args.emulate}
    if inv["gpus"] or args.emulate:
        rep["bench"] = bench(args, inv)
    rep["status"], rep["summary"] = summarize(rep)
    with open(args.report, "w") as f:
        json.dump(rep, f, indent=2)
    print(f"[{rep['status']}] gds {rep['summary']}")
    print(f"report: {args.report}")
    return 0 if rep["status"] != "FAIL" else 1


if __name__ == "__main__":
    sys.exit(main())
