"""KV-cache reuse through rocepool: how much of time-to-first-token (TTFT)
a cached prompt prefix saves, for a real model's KV geometry.

    python3 tools/bench_kvcache.py                        # crossover rig (or ROCEPOOL_TOPOLOGY: real hosts)
    python3 tools/bench_kvcache.py --model llama3-70b --prompt 8192 --prefix 6144
    python3 tools/bench_kvcache.py --emulate              # no GPU: the emulated device (tests)

Prefix caching and prefill/decode disaggregation both come down to this:
a request whose prompt starts with a prefix some earlier prefill already
computed loads that prefix's KV cache instead of recomputing it, so

    TTFT(cached)    = lookup + load(prefix KV) + prefill(prompt - prefix)
    TTFT(recompute) = prefill(prompt)

lookup and load are measured (roce/native/rocepool_kvcache_bench.c over the
KV API's GPU calls: rpkv_mcontains, then rpkv_mget_dev straight into GPU
memory); prefill is the usual estimate 2 x parameters x tokens / (GPU
FLOP/s x MFU) -- the GPU's dense BF16 peak from its name (nvidia-smi) or
--gpu-tflops, MFU --mfu (0.5) -- labelled an estimate in the report.

Cases (each: Llama-style key per block of all layers, and per block per layer):
  gpu-direct   the NIC writes the KV blocks into GPU memory (no host copy)
  gpu-bounce   the same through host memory + cudaMemcpy: storage without GPUDirect
  host         into host memory (no GPU on this host: the CPU-memory tier)
Report: kvcache_report.json (or --report).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# layers, KV heads, head dim, parameters
MODELS = {
    "llama3-8b": (32, 8, 128, 8.0e9),
    "llama3-70b": (80, 8, 128, 70.6e9),
    "qwen2.5-7b": (28, 4, 128, 7.6e9),
    "qwen2.5-72b": (80, 8, 128, 72.7e9),
    "mistral-7b": (32, 8, 128, 7.2e9),
}
# dense BF16/FP16 tensor TFLOP/s (no sparsity), by a substring of the nvidia-smi name
GPU_TFLOPS = [("H200", 989), ("H100", 989), ("H800", 989), ("GH200", 989), ("B200", 2250), ("B100", 1750),
              ("A100", 312), ("A800", 312), ("L40S", 362), ("L40", 181), ("A40", 150), ("A10", 125),
              ("L4", 121), ("RTX 6000 Ada", 364), ("4090", 165), ("3090", 71), ("V100", 125)]
DEFAULT_TFLOPS = 312.0


def gpu_tflops(name: str) -> float | None:
    for k, v in GPU_TFLOPS:
        if k in name:
            return float(v)
    return None


def prefill_ms(params: float, tokens: int, tflops: float, mfu: float) -> float:
    """dense prefill compute (the linear layers; attention's quadratic part
    is left out, which favours recomputation at long prompts)"""
    return 2.0 * params * tokens / (tflops * 1e12 * mfu) * 1e3


def ttft(row: dict, params: float, prompt: int, prefix: int, tflops: float, mfu: float) -> dict:
    full = prefill_ms(params, prompt, tflops, mfu)
    rest = prefill_ms(params, prompt - prefix, tflops, mfu)
    load = row["load_ms_p50"]
    cached = load + rest
    return {"ttft_recompute_ms": round(full, 2), "ttft_cached_ms": round(cached, 2),
            "ttft_cached_p99_ms": round(row["load_ms_p99"] + rest, 2),
            "ttft_speedup": round(full / cached, 2) if cached > 0 else None,
            "load_share_of_cached_ttft": round(load / cached, 3) if cached > 0 else None}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--model", default="llama3-8b", choices=sorted(MODELS))
    ap.add_argument("--prompt", type=int, default=4096, help="prompt tokens")
    ap.add_argument("--prefix", type=int, default=3072, help="of which a cached prefix (multi-turn history, system prompt, RAG context)")
    ap.add_argument("--block-tokens", type=int, default=16)
    ap.add_argument("--prefixes", type=int, default=4, help="distinct cached prefixes")
    ap.add_argument("--requests", type=int, default=32)
    ap.add_argument("--concurrency", type=int, default=4, help="requests loading at once (per GPU)")
    ap.add_argument("--osts", type=int, default=4)
    ap.add_argument("--gpu-tflops", type=float, help="for the prefill estimate (default: from the GPU's name)")
    ap.add_argument("--mfu", type=float, default=0.5)
    ap.add_argument("--emulate", action="store_true", help="no GPU: the emulated device")
    ap.add_argument("--report", default=os.path.join(ROOT, "kvcache_report.json"))
    args = ap.parse_args()
    from tools.bench_gds import gpu_inventory
    from tools.bench_ior import start_dist_servers
    from tools.topology import Topology
    layers, heads, hdim, params = MODELS[args.model]
    inv = gpu_inventory(fix=os.geteuid() == 0)
    gpu = bool(inv["gpus"]) or args.emulate
    tflops = args.gpu_tflops or (gpu_tflops(inv["gpus"][0]["name"]) if inv["gpus"] else None)
    tflops_src = "--gpu-tflops" if args.gpu_tflops else (f"{inv['gpus'][0]['name']} dense BF16 peak" if tflops else
                                                         f"assumed {DEFAULT_TFLOPS:g} (A100 class): no GPU recognised")
    tflops = tflops or DEFAULT_TFLOPS
    topo = Topology.current()
    if topo.real:
        bench = os.path.join(os.path.dirname(os.path.abspath(topo.lib)), "rocepool-kvcache-bench")
    else:
        from roce.native.build_lib import ensure_kvcache_bench
        bench = ensure_kvcache_bench()
    servers, _, osts, env = start_dist_servers(n_mdts=1, n_osts=args.osts, topo=topo)
    base = [bench, ",".join(a for _, a in osts), f"layers={layers}", f"kv_heads={heads}", f"head_dim={hdim}",
            f"block_tokens={args.block_tokens}", f"prompt_tokens={args.prompt}", f"prefix_tokens={args.prefix}",
            f"prefixes={args.prefixes}", f"requests={args.requests}", f"concurrency={args.concurrency}"]
    cases = ([("gpu-direct", ["mode=gpu"], {}), ("gpu-bounce", ["mode=gpu"], {"ROCEPOOL_GDS_BOUNCE": "1"})]
             if gpu else []) + [("host", ["mode=host"], {})]
    rows = []
    try:
        for gran in ("block", "layer"):
            for name, extra, xenv in cases:
                e = dict(env, **xenv)
                if args.emulate:
                    e["ROCEPOOL_DEV_EMU"] = "1"
                r = subprocess.run(topo.launch["A"].cmd(base + extra + [f"granularity={gran}"]), env=e,
                                   capture_output=True, text=True, timeout=3600)
                line = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None)
                row = {"case": name, **(json.loads(line[7:]) if line else {})}
                if not line or r.returncode:
                    row["error"] = (r.stdout + r.stderr)[-800:]
                else:
                    row.update(ttft(row, params, args.prompt, args.prefix, tflops, args.mfu))
                rows.append(row)
                print(json.dumps({k: row.get(k) for k in ("case", "granularity", "value_bytes", "load_ms_p50",
                                                         "load_ms_p99", "load_gbps", "lookup_ms_p50",
                                                         "ttft_recompute_ms", "ttft_cached_ms", "ttft_speedup",
                                                         "direct_bytes", "bounced_bytes", "error")
                                  if k in row}), flush=True)
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
    rep = {"when": time.strftime("%Y-%m-%d %H:%M:%S"), "topology": topo.describe, "model": args.model,
           "kv_bytes_per_token": 2 * layers * heads * hdim * 2, "prompt_tokens": args.prompt,
           "prefix_tokens": args.prefix, "gpu": inv["gpus"][0]["name"] if inv["gpus"] else
           ("emulated" if args.emulate else None), "prefill_estimate": {"tflops": tflops, "source": tflops_src,
                                                                         "mfu": args.mfu}, "results": rows}
    rep["status"], rep["summary"] = summarize(rep)
    with open(args.report, "w") as f:
        json.dump(rep, f, indent=2)
    print(f"[{rep['status']}] kvcache {rep['summary']}")
    print(f"report: {args.report}")
    return 1 if rep["status"] == "FAIL" else 0


def summarize(rep: dict) -> tuple[str, str]:
    rows = rep["results"]
    bad = [r for r in rows if "error" in r or r.get("errors") or r.get("store_errors")]
    if bad:
        return "FAIL", f"{bad[0]['case']}/{bad[0].get('granularity', '?')}: {bad[0].get('error', 'data check failed')[-300:]}"
    best = {}
    for r in rows:
        if r["case"] not in best or r["load_ms_p50"] < best[r["case"]]["load_ms_p50"]:
            best[r["case"]] = r
    main_case = "gpu-direct" if "gpu-direct" in best else "host"
    m = best[main_case]
    line = (f"{rep['model']}, {rep['prefix_tokens']} of {rep['prompt_tokens']} tokens cached "
            f"({m['cached_bytes_per_request'] / 2**20:.0f} MiB KV): {main_case} load p50 {m['load_ms_p50']:.1f} ms "
            f"({m['load_gbps']:.1f} GB/s, {m['granularity']} keys) -> TTFT {m['ttft_cached_ms']:.1f} ms vs "
            f"{m['ttft_recompute_ms']:.1f} ms recomputed (x{m['ttft_speedup']}, prefill estimated)")
    if "gpu-bounce" in best:
        line += f"; through host memory: load {best['gpu-bounce']['load_ms_p50']:.1f} ms"
    if rep.get("gpu") == "emulated" or "crossover" in rep.get("topology", ""):
        line += " [software RDMA emulator: functional check, not a hardware number]"
    status = "PASS"
    if main_case == "gpu-direct" and (m["bounced_bytes"] or not m["direct_bytes"]):
        status = "WARN"
        line += "; GPU memory not reachable by the NIC: loads went through host memory"
    return status, line


if __name__ == "__main__":
    sys.exit(main())
