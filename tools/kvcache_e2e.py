"""End-to-end KV-cache offload on RoCEPool: vLLM + LMCache + RoCEPool OSTs on
two GPU hosts, four tests, one report. Unattended; the AWS run is

    python3 tools/aws_cluster.py run --mode gpu --type g6e.8xlarge --nodes 2 \\
        --command "python3 tools/kvcache_e2e.py"

(HEAD_IP / PEER_IPS come from aws_cluster.py). On hosts of your own:

    python3 tools/kvcache_e2e.py --hosts 10.0.0.1,10.0.0.2      # run on 10.0.0.1, root ssh to the other

Steps -- each recorded in kvcache_e2e_progress.json the moment it ends (plus a
heartbeat every 30 s), so a run cut short still says how far it got:

  prepare   tools/roce_onsite.py --bench none: RDMA preflight, build, the tree
            shipped to the peer and built there (onsite_cluster.json)
  install   per host, in parallel: a venv with LMCache (pinned) and vLLM, the
            model downloaded; checks torch sees the GPU
  osts      RoCEPool OSTs on every host (RAM)
  engines   per host: an LMCache server with RoCEPool as its L2
            (roce.lmcache_connector.RoCEPoolL2Adapter, L1 arena registered with
            the NIC) and vLLM connected to it (LMCacheMPConnector, vLLM's own
            prefix cache off, so every hit is LMCache's)
  T1 correctness   host A: cold -> wait until stored in RoCEPool -> clear A's L1
                   -> same prompt (must load from RoCEPool) -> again (L1). The
                   three completions must be identical, and the second must
                   have loaded bytes from RoCEPool.
  T2 cross-server  A computes a prompt; B (never saw it) must load it from
                   RoCEPool and answer identically.
  T3 TTFT          per prompt length: cold (recompute), L1 hit, L2 hit on the
                   same host (L1 cleared), L2 hit on the other host -- median
                   time to first token, and the bytes loaded.
  T5 one-sided     B loads prompts A stored: the OSTs themselves report the
                   object bytes they sent in replies (must stay ~0 -- the
                   client's NIC read them), the preflight confirms RDMA read
                   in hardware, and OST CPU per MiB loaded is set against
                   per MiB stored (idle busy-polling subtracted).
  T6 striping      a chunk is split into parts (LMCache's chunk size vs the
                   part size) and every OST holds a balanced share.
  T4 zero copy     every byte between RoCEPool and LMCache's L1 moved by RDMA:
                   the connectors' "bytes copied" counters stay 0.

Report: kvcache_e2e_report.json (verdict PASS when every test passes; T3's
timings are measurements, its pass condition is that the cached answers agree). Logs of every process: kvcache_e2e_logs.tar.gz.

--engine fake: the same steps and tests against tests/fakes/fake_inference_server.py
(the same HTTP APIs; KV chunks stored and loaded through the real RoCEPool
connector) on local OSTs over the RDMA emulator -- the rehearsal that runs
without GPUs (tests/test_kvcache_e2e.py).
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import http.client
import json
import os
import random
import shlex
import statistics
import subprocess
import sys
import tarfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

LMCACHE_VERSION = "0.5.7"
DEFAULT_MODEL = "Qwen/Qwen2.5-7B-Instruct"
VENV = "/opt/kvenv"
VENV_IN_USE = [VENV]   # --venv: what the clean-up's patterns name
VLLM_PORT, LMC_PORT, LMC_HTTP_PORT, OST_PORT = 8000, 6555, 6556, 19950
WORDS = ("time year people way day man thing woman life child world school state family student group country "
         "problem hand part place case week company system program question work government number night point "
         "home water room mother area money story fact month lot right study book eye job word business issue "
         "side kind head house service friend father power hour game line end member law car city community "
         "name president team minute idea kid body information back parent face others level office door health "
         "person art war history party result change morning reason research girl guy moment air teacher force "
         "education").split()


# ---------------------------------------------------------------- report
class Report:
    """The report and the progress file: written at every step's end and by a
    30 s heartbeat, so what has been measured survives the run stopping."""

    def __init__(self, path: str, progress: str, meta: dict):
        self.path, self.progress = path, progress
        self.data = {"started": _now(), "meta": meta, "stage": "start", "steps": {}, "tests": {}}
        self.t0 = time.monotonic()
        self.lock = threading.Lock()
        self._stop = threading.Event()
        threading.Thread(target=self._beat, daemon=True).start()
        self.write()

    def _beat(self):
        while not self._stop.wait(30):
            self.write()

    def stage(self, name: str):
        with self.lock:
            self.data["stage"] = name
        print(f"== {name}", flush=True)
        self.write()

    def step(self, name: str, status: str, detail: str = "", **extra):
        with self.lock:
            self.data["steps"][name] = {"status": status, "detail": detail, "at_s": round(self.elapsed(), 1), **extra}
        print(f"[{status}] {name}  {detail}", flush=True)
        self.write()

    def test(self, name: str, result: dict):
        with self.lock:
            self.data["tests"][name] = result
        print(f"[{result.get('status', '?')}] {name}  {result.get('summary', '')}", flush=True)
        self.write()

    def elapsed(self) -> float:
        return time.monotonic() - self.t0

    def write(self, final: bool = False):
        with self.lock:
            self.data["updated"] = _now()
            self.data["elapsed_s"] = round(self.elapsed(), 1)
            blob = json.dumps(self.data, indent=2)
        for p in ([self.progress] + ([self.path] if final else [])):
            tmp = f"{p}.{threading.get_ident()}.tmp"   # the heartbeat and the steps write concurrently
            with open(tmp, "w") as f:
                f.write(blob)
            os.replace(tmp, p)

    def finish(self, verdict: str):
        self._stop.set()
        with self.lock:
            self.data["verdict"] = verdict
            self.data["stage"] = "done"
        self.write(final=True)
        print(f"\nverdict: {verdict}   report: {self.path}", flush=True)


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + "Z"


# ---------------------------------------------------------------- hosts
class Host:
    def __init__(self, index: int, ip: str, cluster, ports: dict):
        self.index, self.ip, self.cluster = index, ip, cluster
        self.vllm_port, self.lmc_port, self.lmc_http_port = ports["vllm"], ports["lmc"], ports["lmc_http"]
        self.ost_ports = ports["osts"]

    @property
    def name(self) -> str:
        return "A" if self.index == 0 else chr(ord("A") + self.index)

    def run(self, cmd: str, timeout: float) -> subprocess.CompletedProcess:
        return self.cluster.run(self.index, ["bash", "-c", cmd], self.cluster.env(), capture_output=True, text=True,
                                timeout=timeout)

    def spawn(self, argv: list[str], log: str, env_extra: dict | None = None, stdin=subprocess.DEVNULL):
        f = open(log, "ab")
        return self.cluster.popen(self.index, argv, self.cluster.env(env_extra), stdin=stdin, stdout=f,
                                  stderr=subprocess.STDOUT)


# ---------------------------------------------------------------- HTTP
def http_json(host: str, port: int, method: str, path: str, body: dict | None = None, timeout: float = 30):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request(method, path, json.dumps(body) if body is not None else None,
                     {"Content-Type": "application/json"} if body is not None else {})
        r = conn.getresponse()
        data = r.read()
        return r.status, (json.loads(data) if data else None)
    finally:
        conn.close()


def wait_http(host: str, port: int, path: str, timeout: float, proc=None, what: str = "") -> None:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc is not None and proc.poll() is not None:
            raise RuntimeError(f"{what} exited ({proc.returncode}) before answering")
        try:
            if http_json(host, port, "GET", path, timeout=5)[0] == 200:
                # the answer counts only if the process we started is still
                # there (something else may hold the port)
                time.sleep(1.5)
                if proc is not None and proc.poll() is not None:
                    raise RuntimeError(f"{what} exited ({proc.returncode}) although {path} answered")
                return
        except (OSError, http.client.HTTPException, ValueError):
            pass
        time.sleep(2)
    raise RuntimeError(f"{what} did not answer {path} within {timeout:.0f} s")


def complete(h: Host, model: str, prompt: str, max_tokens: int = 32) -> dict:
    """A streamed, greedy completion: time to first token, text, prompt tokens."""
    conn = http.client.HTTPConnection(h.ip, h.vllm_port, timeout=900)
    body = {"model": model, "prompt": prompt, "max_tokens": max_tokens, "temperature": 0, "stream": True,
            "stream_options": {"include_usage": True}}
    t0 = time.perf_counter()
    conn.request("POST", "/v1/completions", json.dumps(body), {"Content-Type": "application/json"})
    r = conn.getresponse()
    if r.status != 200:
        raise RuntimeError(f"host {h.name}: completion HTTP {r.status}: {r.read()[:300]!r}")
    ttft, parts, usage = None, [], {}
    while True:
        line = r.readline()
        if not line:
            break
        line = line.decode(errors="replace").strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        obj = json.loads(data)
        ch = obj.get("choices") or []
        if ch:
            if ttft is None:
                ttft = time.perf_counter() - t0
            parts.append(ch[0].get("text") or "")
        if obj.get("usage"):
            usage = obj["usage"]
    total = time.perf_counter() - t0
    conn.close()
    return {"ttft_s": ttft, "total_s": total, "text": "".join(parts), "prompt_tokens": usage.get("prompt_tokens")}


def find_key(obj, key: str):
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], dict):
            return obj
        for v in obj.values():
            got = find_key(v, key)
            if got is not None:
                return got
    elif isinstance(obj, list):
        for v in obj:
            got = find_key(v, key)
            if got is not None:
                return got
    return None


def rp_status(h: Host) -> dict:
    """The RoCEPool adapter's status inside LMCache's GET /status."""
    code, st = http_json(h.ip, h.lmc_http_port, "GET", "/status", timeout=30)
    if code != 200:
        raise RuntimeError(f"host {h.name}: LMCache /status HTTP {code}")
    ad = find_key(st, "rocepool")
    if ad is None:
        raise RuntimeError(f"host {h.name}: no RoCEPool L2 adapter in LMCache's status")
    return {"counters": ad["rocepool"], "zero_copy_l1": ad.get("zero_copy_l1"), "healthy": ad.get("is_healthy")}


def counters(h: Host) -> dict:
    return rp_status(h)["counters"]


def wait_stored(h: Host, before: dict, timeout: float = 120) -> dict:
    """Until host h's stores to RoCEPool have settled (LMCache writes L2
    asynchronously, after the request)."""
    end, last, still = time.monotonic() + timeout, None, 0
    while time.monotonic() < end:
        c = counters(h)
        if c["bytes_stored"] > before.get("bytes_stored", 0) and last is not None and \
                c["bytes_stored"] == last["bytes_stored"] and c["ops"] == last["ops"]:
            still += 1
            if still >= 3:
                return c
        else:
            still = 0
        last = c
        time.sleep(0.5)
    raise RuntimeError(f"host {h.name}: nothing (more) stored in RoCEPool within {timeout:.0f} s")


def clear_l1(h: Host) -> None:
    code, body = http_json(h.ip, h.lmc_http_port, "POST", "/cache/clear", {"tier": "l1", "force": True}, timeout=60)
    if code != 200:
        raise RuntimeError(f"host {h.name}: clearing L1 failed: HTTP {code} {body}")


def tokens(h: Host, model: str, text: str) -> int:
    code, body = http_json(h.ip, h.vllm_port, "POST", "/tokenize", {"model": model, "prompt": text}, timeout=120)
    if code != 200:
        raise RuntimeError(f"tokenize HTTP {code}")
    return int(body.get("count") or len(body.get("tokens") or []))


def make_prompt(h: Host, model: str, tag: str, n_tokens: int, seed: int) -> tuple[str, int]:
    """A prompt of about n_tokens tokens that shares no prefix with any other
    (it starts with its tag)."""
    rng = random.Random(seed)
    head = f"Document {tag}. Summarise it in one sentence.\n\n"
    words = [rng.choice(WORDS) for _ in range(int(n_tokens * 1.2) + 64)]
    k = max(16, int(n_tokens * 0.8))
    for _ in range(4):  # two or three rounds of proportional correction
        n = tokens(h, model, head + " ".join(words[:k]))
        if abs(n - n_tokens) <= max(8, n_tokens // 100):
            break
        k = max(16, min(len(words), int(k * n_tokens / max(n, 1))))
    text = head + " ".join(words[:k])
    return text, tokens(h, model, text)


# ---------------------------------------------------------------- stages
def prepare(args, rep: Report, ips: list[str]):
    """RDMA preflight, build, peer shipping -> a tools/cluster.py Cluster."""
    from tools.cluster import Cluster, Node
    if args.local:
        from roce.native.build_lib import ensure_kv_lib, ensure_softverbs_lib
        lib = ensure_softverbs_lib()
        cl = Cluster([Node(i, "127.0.0.1") for i in range(len(ips))], real=True, lib=lib,
                     env={"ROCEPOOL_KV_LIB": ensure_kv_lib()[0]}, describe="local rehearsal (RDMA emulator)")
        rep.step("prepare", "PASS", "local hosts: RDMA emulator library, KV library built")
        return cl
    if not args.skip_prepare:
        peers = ",".join(f"root@{ip}={ip}" for ip in ips[1:])
        cmd = [sys.executable, os.path.join(ROOT, "tools", "roce_onsite.py"), "--ip", ips[0], "--bench", "none",
               "--soft-roce", "--report", os.path.join(ROOT, "onsite_report.json")] + (["--peers", peers] if peers else [])
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=5400, cwd=ROOT)
        with open(os.path.join(args.logs, "prepare.log"), "w") as f:
            f.write(r.stdout + r.stderr)
        try:
            onsite = json.load(open(os.path.join(ROOT, "onsite_report.json")))
        except (OSError, ValueError):
            onsite = {}
        bad = {k: v.get("detail", "")[:200] for k, v in onsite.get("steps", {}).items() if v.get("status") == "FAIL"}
        if bad or not os.path.exists(os.path.join(ROOT, "onsite_cluster.json")):
            rep.step("prepare", "FAIL", f"roce_onsite: {bad or (r.stdout + r.stderr)[-400:]}")
            raise RuntimeError("prepare failed")
    cl = Cluster.from_file(os.path.join(ROOT, "onsite_cluster.json"))
    cl.extra_env.setdefault("ROCEPOOL_KV_LIB", os.path.join(ROOT, "roce", "native", "librocepool_kv.so"))
    transport = "EFA (libfabric)" if os.path.isdir("/sys/class/infiniband") and \
        any(d.startswith("efa") for d in os.listdir("/sys/class/infiniband")) else "verbs"
    rep.step("prepare", "PASS", f"{len(cl.nodes)} hosts, transport {transport}", transport=transport)
    return cl


INSTALL = r"""
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive PIP_DISABLE_PIP_VERSION_CHECK=1
nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader
python3 -m venv --help >/dev/null 2>&1 && python3 -c 'import ensurepip' 2>/dev/null || \
  (apt-get update -q >/dev/null && apt-get install -y -q python3-venv >/dev/null)
[ -x {venv}/bin/python ] || python3 -m venv {venv}
{venv}/bin/pip install -q -U pip uv
# LMCache's documented order: LMCache, then vLLM (which brings the torch build it needs)
{venv}/bin/uv pip install -q --python {venv}/bin/python {lmcache}
{venv}/bin/uv pip install -q --python {venv}/bin/python {vllm}
{venv}/bin/python - <<'PY'
import importlib.metadata as md, json, torch
from lmcache.integration.vllm import lmcache_mp_connector  # noqa: F401  the connector vLLM loads
from lmcache.v1.multiprocess import http_server  # noqa: F401            the server
print("KVENV " + json.dumps({{"torch": torch.__version__, "torch_cuda": torch.version.cuda,
      "cuda_available": torch.cuda.is_available(),
      "gpu": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
      "vllm": md.version("vllm"), "lmcache": md.version("lmcache")}}))
PY
{venv}/bin/python -c "from huggingface_hub import snapshot_download; snapshot_download('{model}')" >/dev/null
echo KVENV_OK
"""


def install(args, rep: Report, hosts: list[Host]) -> dict:
    if args.engine == "fake":
        rep.step("install", "PASS", "fake engine: nothing to install")
        return {}
    script = INSTALL.format(venv=args.venv, lmcache=shlex.quote(f"lmcache=={LMCACHE_VERSION}"),
                            vllm=shlex.quote(args.vllm_spec), model=args.model)

    def one(h: Host):
        r = h.run(script, timeout=3600)
        with open(os.path.join(args.logs, f"install_{h.name}.log"), "w") as f:
            f.write(r.stdout + r.stderr)
        info = next((json.loads(l[6:]) for l in r.stdout.splitlines() if l.startswith("KVENV ")), {})
        gpu = (r.stdout.strip().splitlines() or [""])[0]
        return h.name, {"ok": "KVENV_OK" in r.stdout and info.get("cuda_available"), "versions": info,
                        "nvidia_smi": gpu, "error": "" if "KVENV_OK" in r.stdout else (r.stdout + r.stderr)[-600:]}
    with cf.ThreadPoolExecutor(len(hosts)) as ex:
        res = dict(ex.map(one, hosts))
    ok = all(v["ok"] for v in res.values())
    v0 = next(iter(res.values()))["versions"]
    rep.step("install", "PASS" if ok else "FAIL",
             f"vLLM {v0.get('vllm')}, LMCache {v0.get('lmcache')}, torch {v0.get('torch')} "
             f"(CUDA {v0.get('torch_cuda')}), GPU {v0.get('gpu')}" if ok else
             "; ".join(f"{k}: {v['error'] or 'torch does not see the GPU (' + v['nvidia_smi'] + ')'}"
                       for k, v in res.items() if not v["ok"]), hosts=res)
    if not ok:
        raise RuntimeError("install failed")
    return res


def start_osts(args, rep: Report, hosts: list[Host]) -> tuple[list, str]:
    procs, addrs = [], []
    for h in hosts:
        for port in h.ost_ports:
            me = f"{h.ip}:{port}"
            p = h.cluster.popen(h.index, [sys.executable if h.index == 0 else "python3",
                                          os.path.join(ROOT, "tools", "crossover_node.py"), "ost-native", me],
                                h.cluster.env(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=open(os.path.join(args.logs, f"ost_{h.name}_{port}.log"), "ab"), text=True)
            procs.append(p)
            addrs.append((p, me))
    for p, me in addrs:
        line = _readline(p, 180)
        if not line.startswith("READY"):
            rep.step("osts", "FAIL", f"OST {me} did not start: {line!r}")
            raise RuntimeError("OSTs failed")
    OSTS.clear()
    for (p, me), h in zip(addrs, [h for h in hosts for _ in h.ost_ports]):
        o = {"addr": me, "host": h, "proc": p, "probes": [], "cv": threading.Condition()}
        threading.Thread(target=_collect_probes, args=(o,), daemon=True).start()
        OSTS.append(o)
    osts = ",".join(me for _, me in addrs)
    rep.step("osts", "PASS", f"{len(addrs)} OSTs (RAM): {osts}", osts=osts)
    return procs, osts


OSTS: list[dict] = []   # every OST: address, host, process, the MEMPROBE lines it printed


def _collect_probes(o: dict) -> None:
    for line in o["proc"].stdout:
        if line.startswith("MEMPROBE "):
            with o["cv"]:
                o["probes"].append(json.loads(line[9:]))
                o["cv"].notify_all()


def probe_osts(timeout: float = 20) -> dict:
    """Every OST's counters now (kill -USR1: objects, bytes, requests served,
    object bytes sent in replies, CPU seconds), by address."""
    seen = {o["addr"]: len(o["probes"]) for o in OSTS}
    for o in OSTS:
        pat = own(os.path.join(ROOT, "tools", "crossover_node.py")) + " ost-native " + o["addr"].replace(".", "\\.")
        o["host"].run(f"pkill -USR1 -f {shlex.quote(pat)}", timeout=60)
    out, end = {}, time.monotonic() + timeout
    for o in OSTS:
        with o["cv"]:
            while len(o["probes"]) <= seen[o["addr"]] and time.monotonic() < end:
                o["cv"].wait(0.2)
            if len(o["probes"]) <= seen[o["addr"]]:
                raise RuntimeError(f"OST {o['addr']} did not answer the probe")
            out[o["addr"]] = dict(o["probes"][-1], at=time.monotonic())
    return out


def _readline(p, timeout: float) -> str:
    out = []
    t = threading.Thread(target=lambda: out.append(p.stdout.readline()), daemon=True)
    t.start()
    t.join(timeout)
    return (out[0] if out else "").strip()


def l2_adapter(osts: str, namespace: str) -> str:
    return json.dumps({"type": "plugin", "module_path": "roce.lmcache_connector", "class_name": "RoCEPoolL2Adapter",
                       "adapter_params": {"osts": osts, "namespace": namespace, "part_mb": 4}})


def start_engines(args, rep: Report, hosts: list[Host], osts: str) -> list:
    procs = []
    ns = f"e2e-{args.run_tag}/"
    for h in hosts:
        if args.engine == "fake":
            p = h.spawn([sys.executable, os.path.join(ROOT, "tests", "fakes", "fake_inference_server.py"),
                         "--port", str(h.vllm_port), "--lmcache-http-port", str(h.lmc_http_port),
                         "--osts", osts, "--namespace", ns, *args.fake_args.split()],
                        os.path.join(args.logs, f"engine_{h.name}.log"))
            procs.append((h, "fake engine", p, h.vllm_port, "/health"))
            continue
        env = ["env", "PYTHONHASHSEED=0", f"PYTHONPATH={ROOT}", "LMCACHE_LOG_LEVEL=INFO"]
        lmc = h.spawn(env + [f"{args.venv}/bin/lmcache", "server", "--host", "127.0.0.1", "--port", str(h.lmc_port),
                             "--http-host", "0.0.0.0", "--http-port", str(h.lmc_http_port),
                             "--l1-size-gb", str(args.l1_gb), "--no-l1-use-lazy", "--eviction-policy", "LRU",
                             "--hash-algorithm", "blake3", "--l2-adapter", l2_adapter(osts, ns)],
                      os.path.join(args.logs, f"lmcache_{h.name}.log"))
        procs.append((h, "LMCache server", lmc, h.lmc_http_port, "/healthcheck"))
    for h, what, p, port, path in procs:
        wait_http(h.ip, port, path, 600, p, f"host {h.name} {what}")
    if args.engine == "fake":
        rep.step("engines", "PASS", "fake engines up")
        return [p for *_, p, _, _ in procs]
    def launch(batch_invariant: bool) -> list:
        vprocs = []
        for h in hosts:
            kvt = json.dumps({"kv_connector": "LMCacheMPConnector",
                              "kv_connector_module_path": "lmcache.integration.vllm.lmcache_mp_connector",
                              "kv_role": "kv_both", "kv_load_failure_policy": "recompute",
                              "kv_connector_extra_config": {"lmcache.mp.host": "127.0.0.1",
                                                            "lmcache.mp.port": h.lmc_port,
                                                            "lmcache.mp.autostart": False}})
            env = ["env", "PYTHONHASHSEED=0", f"PYTHONPATH={ROOT}"] + \
                (["VLLM_BATCH_INVARIANT=1"] if batch_invariant else [])
            p = h.spawn(env + [f"{args.venv}/bin/vllm", "serve", args.model, "--host", "0.0.0.0",
                               "--port", str(h.vllm_port), "--max-model-len", str(args.max_model_len),
                               "--gpu-memory-utilization", "0.88", "--no-enable-prefix-caching",
                               "--kv-transfer-config", kvt],
                        os.path.join(args.logs, f"vllm_{h.name}{'' if batch_invariant else '_retry'}.log"))
            vprocs.append((h, p))
        try:
            for h, p in vprocs:
                wait_http(h.ip, h.vllm_port, "/health", 1500, p, f"host {h.name} vLLM")
        except RuntimeError:
            stop_all(hosts, [p for _, p in vprocs], [own_patterns(args.venv)["vllm"]])
            raise
        return vprocs

    # batch-invariant kernels make a cold prefill and a cached one agree token
    # for token (LMCache's own CI runs this way); a GPU or vLLM without them
    # refuses to start: then without, and the report says so
    try:
        vprocs, bi = launch(True), True
    except RuntimeError as exc:
        if "exited" not in str(exc):
            raise
        vprocs, bi = launch(False), False
    st = {h.name: rp_status(h) for h in hosts}
    rep.step("engines", "PASS", "LMCache servers (RoCEPool L2) and vLLM up on " + ", ".join(st) +
             ("" if bi else "; vLLM without batch-invariant kernels (refused on this GPU/version)"),
             zero_copy_l1={k: v["zero_copy_l1"] for k, v in st.items()}, batch_invariant=bi)
    return [p for *_, p, _, _ in procs] + [p for _, p in vprocs]


# ---------------------------------------------------------------- tests
def delta(a: dict, b: dict, k: str) -> int:
    return int(b.get(k, 0)) - int(a.get(k, 0))


def first_diff(a: str, b: str) -> int | None:
    return next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), None if len(a) == len(b) else min(len(a), len(b)))


def t1_correctness(args, rep: Report, A: Host) -> dict:
    """The reference is an L1 hit: the same cached-KV computation as a reload
    from RoCEPool, so the two answers can only differ if RoCEPool returned
    different bytes. (A cold prefill runs other kernel shapes; its answer is
    reported, not required to match, unless vLLM runs batch-invariant.)"""
    cases, ok = [], True
    for i in range(args.t1_prompts):
        text, n = make_prompt(A, args.model, f"T1-{args.run_tag}-{i}", args.t1_tokens, 1000 + i)
        c0 = counters(A)
        cold = complete(A, args.model, text)
        c1 = wait_stored(A, c0)
        ref = complete(A, args.model, text)
        c2 = counters(A)
        clear_l1(A)
        l2 = complete(A, args.model, text)
        c3 = counters(A)
        loaded = delta(c2, c3, "bytes_loaded")
        good = l2["text"] == ref["text"] and loaded > 0 and delta(c1, c2, "bytes_loaded") == 0
        ok &= good
        cases.append({"prompt_tokens": n, "identical": l2["text"] == ref["text"],
                      "matches_cold_recompute": cold["text"] == l2["text"],
                      "first_difference_at": first_diff(ref["text"], l2["text"]),
                      "stored_bytes": delta(c0, c1, "bytes_stored"), "loaded_from_rocepool_bytes": loaded,
                      "loaded_on_l1_hit_bytes": delta(c1, c2, "bytes_loaded"),
                      "ttft_s": {"cold": cold["ttft_s"], "l1": ref["ttft_s"], "l2": l2["ttft_s"]},
                      "texts": {"cold": cold["text"][:200], "l1": ref["text"][:200], "l2": l2["text"][:200]}})
    return {"status": "PASS" if ok else "FAIL", "cases": cases,
            "summary": f"{sum(c['identical'] for c in cases)}/{len(cases)} answers identical after a reload from "
                       f"RoCEPool (reference: an L1 hit), {sum(c['matches_cold_recompute'] for c in cases)}/"
                       f"{len(cases)} also equal to the cold recompute, "
                       f"{sum(c['loaded_from_rocepool_bytes'] for c in cases) / 2**20:.1f} MiB loaded"}


def t2_cross_server(args, rep: Report, A: Host, B: Host) -> dict:
    cases, ok = [], True
    for i in range(args.t2_prompts):
        text, n = make_prompt(A, args.model, f"T2-{args.run_tag}-{i}", args.t1_tokens, 2000 + i)
        a0 = counters(A)
        cold = complete(A, args.model, text)
        wait_stored(A, a0)
        ref = complete(A, args.model, text)     # A's L1 hit: the reference
        b0 = counters(B)
        rb = complete(B, args.model, text)
        b1 = counters(B)
        loaded = delta(b0, b1, "bytes_loaded")
        good = ref["text"] == rb["text"] and loaded > 0
        ok &= good
        cases.append({"prompt_tokens": n, "identical": ref["text"] == rb["text"], "loaded_on_B_bytes": loaded,
                      "first_difference_at": first_diff(ref["text"], rb["text"]),
                      "matches_cold_recompute": cold["text"] == rb["text"],
                      "ttft_s": {"A_cold": cold["ttft_s"], "A_l1": ref["ttft_s"], "B_from_rocepool": rb["ttft_s"]}})
    return {"status": "PASS" if ok else "FAIL", "cases": cases,
            "summary": f"{sum(c['identical'] and c['loaded_on_B_bytes'] > 0 for c in cases)}/{len(cases)} prompts "
                       f"computed on A were loaded on B from RoCEPool and answered identically"}


def t3_ttft(args, rep: Report, A: Host, B: Host) -> dict:
    rows, consistent = [], True
    for L in args.lengths:
        samples = {"cold": [], "l1": [], "l2_local": [], "l2_remote": []}
        loaded = {"l2_local": [], "l2_remote": []}
        n_tok = None
        for r in range(args.reps):
            text, n_tok = make_prompt(A, args.model, f"T3-{args.run_tag}-{L}-{r}", L, 3000 + L + r)
            a0 = counters(A)
            cold = complete(A, args.model, text)
            wait_stored(A, a0)
            l1 = complete(A, args.model, text)
            clear_l1(A)
            a2 = counters(A)
            l2 = complete(A, args.model, text)
            a3 = counters(A)
            b0 = counters(B)
            rem = complete(B, args.model, text)
            b1 = counters(B)
            consistent &= l1["text"] == l2["text"] == rem["text"]
            for k, v in (("cold", cold), ("l1", l1), ("l2_local", l2), ("l2_remote", rem)):
                samples[k].append(v["ttft_s"])
            loaded["l2_local"].append(delta(a2, a3, "bytes_loaded"))
            loaded["l2_remote"].append(delta(b0, b1, "bytes_loaded"))
        med = {k: statistics.median(v) for k, v in samples.items()}
        row = {"target_tokens": L, "prompt_tokens": n_tok, "ttft_median_s": med, "ttft_samples_s": samples,
               "loaded_mib_median": {k: statistics.median(v) / 2**20 for k, v in loaded.items()},
               "speedup_vs_cold": {k: (med["cold"] / med[k]) if med[k] else None
                                   for k in ("l1", "l2_local", "l2_remote")}}
        rows.append(row)
        rep.test("T3_ttft_partial", {"status": "RUNNING", "rows": rows})
    summary = "; ".join(f"{r['prompt_tokens']} tok: cold {r['ttft_median_s']['cold'] * 1e3:.0f} ms, "
                        f"L2 local {r['ttft_median_s']['l2_local'] * 1e3:.0f} ms, "
                        f"L2 other host {r['ttft_median_s']['l2_remote'] * 1e3:.0f} ms" for r in rows)
    return {"status": "PASS" if consistent else "FAIL", "rows": rows, "identical_cached_answers": consistent,
            "summary": summary}


def preflight_rdma_read(args) -> dict:
    """The RDMA preflight's verdict on hardware RDMA read (EFA: by instance
    family, from AWS's documentation)."""
    if args.local:
        return {"status": "n/a", "detail": "local rehearsal: RDMA emulator"}
    try:
        pf = json.load(open(os.path.join(ROOT, "onsite_report.json")))["steps"]["preflight"]["preflight"]
        c = next(c for c in pf["checks"] if c["check"] == "rdma_read")
        return {"status": c["status"], "detail": c["detail"]}
    except (OSError, ValueError, KeyError, StopIteration):
        return {"status": "unknown", "detail": "no rdma_read check in onsite_report.json"}


def _ost_delta(p0: dict, p1: dict, idle_cpu_rate: dict) -> dict:
    dt = sum(p1[a]["at"] - p0[a]["at"] for a in p0) / len(p0)
    cpu = sum(max(0.0, p1[a]["cpu_s"] - p0[a]["cpu_s"] - idle_cpu_rate.get(a, 0) * (p1[a]["at"] - p0[a]["at"]))
              for a in p0)
    return {"seconds": dt, "net_cpu_s": cpu, "requests": sum(p1[a]["ops"] - p0[a]["ops"] for a in p0),
            "data_out_bytes": int(sum(p1[a]["data_out_mb"] - p0[a]["data_out_mb"] for a in p0) * 2**20)}


def t5_one_sided(args, rep: Report, A: Host, B: Host) -> dict:
    """Loads from RoCEPool are one-sided: the client's NIC reads the OSTs'
    memory (RDMA READ of leased parts) and no OST sends the data. Measured on
    the OSTs themselves: object bytes they sent in replies while B loaded
    (must stay ~0), and their CPU per MiB loaded vs per MiB stored."""
    p0 = probe_osts()
    time.sleep(args.idle_s)
    p1 = probe_osts()
    idle = {a: (p1[a]["cpu_s"] - p0[a]["cpu_s"]) / max(1e-6, p1[a]["at"] - p0[a]["at"]) for a in p0}
    prompts = [make_prompt(A, args.model, f"T5-{args.run_tag}-{i}", args.t5_tokens, 5000 + i)[0]
               for i in range(args.t5_prompts)]
    a0 = counters(A)
    ps0 = probe_osts()
    for t in prompts:
        complete(A, args.model, t)
    a1 = wait_stored(A, a0)
    ps1 = probe_osts()
    clear_l1(B)
    b0 = counters(B)
    pl0 = probe_osts()
    for t in prompts:
        complete(B, args.model, t)
    pl1 = probe_osts()
    b1 = counters(B)
    store, load = _ost_delta(ps0, ps1, idle), _ost_delta(pl0, pl1, idle)
    stored, loaded = delta(a0, a1, "bytes_stored"), delta(b0, b1, "bytes_loaded")
    copied, zc = delta(b0, b1, "bytes_copied"), delta(b0, b1, "bytes_zero_copy")
    us_store = store["net_cpu_s"] * 1e6 / max(1, stored / 2**20)
    us_load = load["net_cpu_s"] * 1e6 / max(1, loaded / 2**20)
    hw = preflight_rdma_read(args)
    checks = {
        "loaded_from_rocepool": loaded > 0,
        "client_zero_copy": copied == 0 and zc >= loaded,
        "osts_sent_no_data": load["data_out_bytes"] <= loaded // 100,
        "hardware_rdma_read": hw["status"] in ("PASS", "n/a"),
    }
    ok = all(checks.values())
    cpu_note = ("" if args.local or us_load <= 0.5 * us_store else
                "OST CPU per MiB loaded above half of per MiB stored: check the idle baseline")
    return {"status": "PASS" if ok else "FAIL", "checks": checks, "rdma_read_preflight": hw,
            "loaded_bytes": loaded, "stored_bytes": stored,
            "osts_during_loads": load, "osts_during_stores": store,
            "ost_cpu_us_per_mib": {"store": round(us_store, 1), "load": round(us_load, 1)},
            "ost_idle_cpu_per_s": round(sum(idle.values()), 4), "note": cpu_note,
            "summary": f"{loaded / 2**20:.0f} MiB loaded on B, OSTs sent {load['data_out_bytes'] / 2**20:.2f} MiB "
                       f"of it; OST CPU {us_load:.0f} us/MiB loaded vs {us_store:.0f} us/MiB stored; "
                       f"RDMA read in hardware: {hw['status']}"}


def lmcache_chunk_bytes(h: Host) -> int | None:
    """A full chunk's KV bytes as LMCache lays it out (chunk_size x
    cache_size_per_token from its status)."""
    code, st = http_json(h.ip, h.lmc_http_port, "GET", "/status", timeout=30)
    per_tok = find_value(st, "cache_size_per_token")
    chunk = find_value(st, "chunk_size")
    return int(per_tok * chunk) if per_tok and chunk else None


def find_value(obj, key: str):
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], (int, float)):
            return obj[key]
        for v in obj.values():
            got = find_value(v, key)
            if got is not None:
                return got
    elif isinstance(obj, list):
        for v in obj:
            got = find_value(v, key)
            if got is not None:
                return got
    return None


def t6_striping(args, rep: Report, hosts: list[Host]) -> dict:
    """Large chunks are split into parts spread over every OST."""
    c = counters(hosts[0])
    part = int(c.get("part_bytes") or 0)
    chunk_layout = lmcache_chunk_bytes(hosts[0])
    chunk_seen = c["bytes_stored"] / c["chunks_stored"] if c.get("chunks_stored") else None
    chunk = chunk_seen or chunk_layout or 0
    parts = -(-int(chunk) // part) if part and chunk else 0
    pr = probe_osts()
    per = {a: {"objects": v["engine_objects"], "mib": v["engine_bytes_mb"]} for a, v in pr.items()}
    mib = [v["mib"] for v in per.values()]
    objects = sum(v["objects"] for v in per.values())
    balance = max(mib) / max(min(mib), 1e-9) if mib else None
    # hashing spreads parts evenly only on average: with few objects a 2:1
    # spread is ordinary; with enough, every OST must hold half its fair share
    judged = objects >= 400
    min_share = min(mib) / max(sum(mib), 1e-9) * len(mib) if mib else 0
    checks = {"chunk_split_into_parts": parts >= 2, "every_ost_holds_data": all(m > 0 for m in mib),
              "balanced": (min_share >= 0.5) if judged else True}
    return {"status": "PASS" if all(checks.values()) else "FAIL", "checks": checks,
            "chunk_bytes": {"stored_mean": chunk_seen, "lmcache_layout": chunk_layout},
            "part_bytes": part, "parts_per_chunk": parts, "osts": per,
            "max_over_min_bytes": round(balance, 2) if balance else None, "objects": objects,
            "smallest_share_of_fair": round(min_share, 2),
            "balance_judged": judged if judged else f"no: {objects} objects, too few to judge balance",
            "summary": f"chunks of {chunk / 2**20:.1f} MiB in {parts} parts of {part / 2**20:.1f} MiB, "
                       f"over {len(per)} OSTs ({', '.join(f'{m:.0f}' for m in mib)} MiB each)"}


def t4_zero_copy(args, rep: Report, hosts: list[Host]) -> dict:
    per = {}
    for h in hosts:
        s = rp_status(h)
        c = s["counters"]
        per[h.name] = {"zero_copy_l1": s["zero_copy_l1"], "bytes_zero_copy": c["bytes_zero_copy"],
                       "bytes_copied": c["bytes_copied"], "bytes_stored": c["bytes_stored"],
                       "bytes_loaded": c["bytes_loaded"], "parts_failed": c["parts_failed"]}
    ok = all(v["zero_copy_l1"] and v["bytes_copied"] == 0 and v["bytes_zero_copy"] > 0 and v["parts_failed"] == 0
             for v in per.values())
    tot = sum(v["bytes_zero_copy"] for v in per.values())
    cop = sum(v["bytes_copied"] for v in per.values())
    return {"status": "PASS" if ok else "FAIL", "hosts": per,
            "summary": f"{tot / 2**30:.2f} GiB moved by RDMA, {cop} bytes copied by the CPU"}


# ---------------------------------------------------------------- main
def own(path: str) -> str:
    """A pkill -f pattern for exactly this path (this tree's script, this
    venv's binary): never someone else's vLLM or LMCache on the host. The
    bracket keeps the pattern from matching the shell that runs pkill."""
    import re
    return f"[{path[0]}]{re.escape(path[1:])}"


def own_patterns(venv: str) -> dict:
    return {"lmcache": own(f"{venv}/bin/lmcache") + " server", "vllm": own(f"{venv}/bin/vllm") + " serve",
            "fake": own(os.path.join(ROOT, "tests", "fakes", "fake_inference_server.py")),
            "ost": own(os.path.join(ROOT, "tools", "crossover_node.py")) + " ost-native"}


def stop_all(hosts: list[Host], procs: list, pats: list[str] | None = None) -> None:
    for p in procs:
        try:
            if p.poll() is None:
                p.terminate()
        except OSError:
            pass
    pats = pats or list(own_patterns(VENV_IN_USE[0]).values())
    for h in hosts:
        try:
            h.run(" ; ".join(f"pkill -f {shlex.quote(x)}" for x in pats) + " ; sleep 2 ; " +
                  " ; ".join(f"pkill -9 -f {shlex.quote(x)}" for x in pats) + " ; true", timeout=60)
        except (subprocess.SubprocessError, OSError):
            pass
    for p in procs:
        try:
            p.wait(timeout=30)
        except (subprocess.TimeoutExpired, OSError):
            pass


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--hosts", help="IPs, this host first (default: HEAD_IP and PEER_IPS from aws_cluster.py)")
    ap.add_argument("--engine", choices=("vllm", "fake"), default="vllm")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--vllm-spec", default="vllm", help="pip requirement for vLLM (default: what LMCache's "
                                                       "install guide uses, the current release)")
    ap.add_argument("--max-model-len", type=int, default=20480)
    ap.add_argument("--l1-gb", type=float, default=16)
    ap.add_argument("--lengths", default="1024,4096,8192,16384")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--t1-tokens", type=int, default=4096)
    ap.add_argument("--t1-prompts", type=int, default=2)
    ap.add_argument("--t2-prompts", type=int, default=2)
    ap.add_argument("--osts-per-host", type=int, default=2)
    ap.add_argument("--t5-prompts", type=int, default=3)
    ap.add_argument("--t5-tokens", type=int, default=8192)
    ap.add_argument("--idle-s", type=float, default=10, help="OST idle-CPU baseline for T5")
    ap.add_argument("--skip-prepare", action="store_true", help="hosts already prepared (onsite_cluster.json)")
    ap.add_argument("--venv", default=VENV, help="where LMCache and vLLM are installed")
    ap.add_argument("--local", action="store_true", help=argparse.SUPPRESS)   # rehearsal: local hosts, RDMA emulator
    ap.add_argument("--report", default=os.path.join(ROOT, "kvcache_e2e_report.json"))
    ap.add_argument("--progress", default=os.path.join(ROOT, "kvcache_e2e_progress.json"))
    ap.add_argument("--logs", default=os.path.join(ROOT, ".bench_cache", "kvcache_e2e"))
    ap.add_argument("--fake-args", default="", help=argparse.SUPPRESS)   # tests: fault injection in the fake
    ap.add_argument("--port-base", type=int, default=0, help=argparse.SUPPRESS)   # tests: free ports
    args = ap.parse_args()
    args.lengths = [int(x) for x in args.lengths.split(",") if x]
    args.run_tag = time.strftime("%Y%m%d%H%M%S") + f"-{os.getpid()}"
    args.local = args.local or args.engine == "fake"
    VENV_IN_USE[0] = os.path.abspath(args.venv)
    args.venv = VENV_IN_USE[0]
    if args.hosts:
        ips = [x.strip() for x in args.hosts.split(",") if x.strip()]
    else:
        ips = [os.environ.get("HEAD_IP", "")] + os.environ.get("PEER_IPS", "").split()
        ips = [x for x in ips if x]
    if len(ips) < 2:
        ap.error("two hosts are needed (--hosts or HEAD_IP / PEER_IPS)")
    os.makedirs(args.logs, exist_ok=True)
    rep = Report(args.report, args.progress, {"engine": args.engine, "model": args.model, "hosts": ips,
                                              "lmcache": LMCACHE_VERSION, "vllm_spec": args.vllm_spec,
                                              "lengths": args.lengths, "reps": args.reps,
                                              "instance_type": os.environ.get("ROCEPOOL_INSTANCE_TYPE")})
    hosts: list[Host] = []
    procs: list = []
    verdict = "FAIL"
    try:
        rep.stage("prepare")
        cl = prepare(args, rep, ips)
        local = args.local
        for i, ip in enumerate(ips[:2]):
            base = args.port_base + (i * 100 if local else 0)
            hosts.append(Host(i, ip if not local else "127.0.0.1", cl, {
                "vllm": VLLM_PORT + base, "lmc": LMC_PORT + base, "lmc_http": LMC_HTTP_PORT + base,
                "osts": [OST_PORT + base + k for k in range(args.osts_per_host)]}))
        A, B = hosts
        rep.stage("install")
        install(args, rep, hosts)
        rep.stage("osts")
        ost_procs, osts = start_osts(args, rep, hosts)
        procs += ost_procs
        rep.stage("engines")
        procs += start_engines(args, rep, hosts, osts)
        results = {}
        for name, fn in (("T1_correctness", lambda: t1_correctness(args, rep, A)),
                         ("T2_cross_server", lambda: t2_cross_server(args, rep, A, B)),
                         ("T3_ttft", lambda: t3_ttft(args, rep, A, B)),
                         ("T5_one_sided", lambda: t5_one_sided(args, rep, A, B)),
                         ("T6_striping", lambda: t6_striping(args, rep, hosts)),
                         ("T4_zero_copy", lambda: t4_zero_copy(args, rep, hosts))):
            rep.stage(name)
            try:
                results[name] = fn()
            except Exception as exc:  # noqa: BLE001 -- the test fails; the others still run
                results[name] = {"status": "FAIL", "summary": f"error: {exc}"}
            rep.test(name, results[name])
        rep.data["tests"].pop("T3_ttft_partial", None)
        verdict = "PASS" if all(results[t]["status"] == "PASS" for t in results) else "FAIL"
    except Exception as exc:  # noqa: BLE001
        rep.step("error", "FAIL", str(exc)[:600])
    finally:
        rep.stage("cleanup")
        stop_all(hosts, procs)
        try:
            with tarfile.open(os.path.join(os.path.dirname(os.path.abspath(args.report)), "kvcache_e2e_logs.tar.gz"),
                              "w:gz") as t:
                t.add(args.logs, arcname="kvcache_e2e_logs")
        except OSError:
            pass
        rep.finish(verdict)
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
