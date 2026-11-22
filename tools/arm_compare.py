"""x86 vs ARM on GitHub's runners: medians of RoCEPool's own benchmarks.

    python3 tools/arm_compare.py summarize <dir> <label>    # one runner: <dir>/{kv,ior,hard}_<round>.json -> <dir>/summary.json
    python3 tools/arm_compare.py compare <x86 summary.json> <arm summary.json>

The runners are shared virtual machines (4 vCPUs each; on x86 a vCPU is a
hyperthread, on ARM a core), and the RDMA is the software emulator, so the
numbers only say how RoCEPool's code runs on each CPU, as a hint before
paying for AWS hosts. They are not a result to publish.
"""
from __future__ import annotations

import glob
import json
import os
import statistics
import sys


def kv_metrics(d: dict) -> dict:
    out = {}
    for r in d.get("results", []):
        if r.get("client") != "native" or r.get("value_bytes") != 4096:
            continue
        out[f"kv {r['mode']} {r['threads']} thr (ops/s)"] = r["ops_per_s"]
        if r["threads"] == 1:
            out[f"kv {r['mode']} 1 thr p50 (us, lower is better)"] = r["p50_us"]
    return out


def ior_metrics(d: dict, prefix: str) -> dict:
    out = {}
    for c in d.get("cases", []):
        if c.get("ok"):
            out[f"{prefix}{c['name']} write (MiB/s)"] = c["write_bw_mib_s"]
            out[f"{prefix}{c['name']} read (MiB/s)"] = c["read_bw_mib_s"]
    md = (d.get("mdtest") or {}).get("ops_per_sec") or {}
    tag = "mdtest-hard" if (d.get("mdtest") or {}).get("hard") else "mdtest-easy"
    for k in ("File creation", "File stat", "File read", "File removal"):
        if k in md:
            out[f"{tag} {k.lower()} (ops/s)"] = md[k]
    return out


def summarize(d: str, label: str) -> dict:
    rounds: dict[str, list[float]] = {}
    for kind, fn in (("kv", lambda x: kv_metrics(x)), ("ior", lambda x: ior_metrics(x, "ior ")),
                     ("hard", lambda x: ior_metrics(x, "ior "))):
        for f in sorted(glob.glob(os.path.join(d, f"{kind}_*.json"))):
            try:
                m = fn(json.load(open(f)))
            except (OSError, ValueError):
                continue
            for k, v in m.items():
                rounds.setdefault(k, []).append(float(v))
    cpu = ""
    try:
        for line in open(os.path.join(d, "cpu.txt")):
            if line.startswith(("Model name", "Architecture", "CPU(s):", "Thread(s) per core")):
                cpu += " ".join(line.split()) + "; "
    except OSError:
        pass
    s = {"label": label, "cpu": cpu.strip("; "), "rounds": {k: v for k, v in rounds.items()},
         "median": {k: statistics.median(v) for k, v in rounds.items()},
         "spread": {k: (max(v) - min(v)) / statistics.median(v) if statistics.median(v) else None
                    for k, v in rounds.items()}}
    json.dump(s, open(os.path.join(d, "summary.json"), "w"), indent=1)
    return s


def compare(x86: dict, arm: dict) -> str:
    lines = [f"x86: {x86['cpu']}", f"ARM: {arm['cpu']}", "",
             "| metric | x86 | ARM | ARM / x86 | spread x86 / ARM |", "|---|---|---|---|---|"]
    ratios = []
    for k in sorted(set(x86["median"]) & set(arm["median"])):
        a, b = x86["median"][k], arm["median"][k]
        if not a or not b:
            continue
        r = b / a
        lower_better = "lower is better" in k
        ratios.append(1 / r if lower_better else r)
        sx, sa = x86["spread"].get(k), arm["spread"].get(k)
        lines.append(f"| {k} | {a:,.1f} | {b:,.1f} | {r:.2f}{' (lower better)' if lower_better else ''} | "
                     f"{(sx or 0) * 100:.0f}% / {(sa or 0) * 100:.0f}% |")
    if ratios:
        g = statistics.geometric_mean(ratios)
        lines += ["", f"Geometric mean, ARM relative to x86 (above 1: ARM faster): {g:.2f} over {len(ratios)} metrics"]
    return "\n".join(lines)


def main() -> int:
    if len(sys.argv) >= 4 and sys.argv[1] == "summarize":
        s = summarize(sys.argv[2], sys.argv[3])
        print(json.dumps({"label": s["label"], "cpu": s["cpu"], "median": s["median"]}, indent=1))
        return 0
    if len(sys.argv) >= 4 and sys.argv[1] == "compare":
        print(compare(json.load(open(sys.argv[2])), json.load(open(sys.argv[3]))))
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
