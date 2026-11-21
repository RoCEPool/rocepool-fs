#!/usr/bin/env python3
"""Stand-ins for the tools tools/kvcache_e2e.py drives on a GPU host -- pip,
uv, the venv's python, lmcache, vllm, nvidia-smi, apt-get -- chosen by the
name it is called as. Every call is appended to $KVENV_STUB_LOG (JSON lines).

  lmcache server ...  the arguments are checked by LMCache's own parser
                      (lmcache.cli.commands.server + parse_args_to_config)
                      when LMCache is importable, else by a strict parser of
                      the flags the harness uses; then it becomes
                      tests/fakes/fake_inference_server.py (which also serves
                      vLLM's port: lmcache's HTTP port - 6556 + 8000)
  vllm serve ...      strict check of the flags and of --kv-transfer-config;
                      then idles (the fake engine answers vLLM's port)
"""
import argparse
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
prog = os.path.basename(sys.argv[0])
VLLM_VERSION = os.environ.get("KVENV_STUB_VLLM", "0.33.0")   # what the stub vLLM reports and parses as
# KVENV_STUB_SITE: a directory whose sitecustomize.py makes LMCache importable
# (a source tree) -- for checking lmcache's arguments with LMCache's own parser
if os.environ.get("KVENV_STUB_SITE"):
    import importlib.util
    _spec = importlib.util.spec_from_file_location("_kvenv_site", os.path.join(os.environ["KVENV_STUB_SITE"],
                                                                                "sitecustomize.py"))
    _spec.loader.exec_module(importlib.util.module_from_spec(_spec))
argv = sys.argv[1:]


def log(**kw):
    with open(os.environ["KVENV_STUB_LOG"], "a") as f:
        f.write(json.dumps({"prog": prog, "argv": argv, "ppid": os.getppid(), **kw}) + "\n")


def fail(msg: str):
    log(error=msg)
    print(f"{prog} stub: {msg}", file=sys.stderr)
    sys.exit(2)


if prog == "nvidia-smi":
    log()
    print("NVIDIA L40S, 580.95.05, 46068 MiB")
elif prog in ("pip", "uv", "apt-get"):
    log()
elif prog == "python":
    if argv[:1] == ["-"]:
        code = sys.stdin.read()
        if "lmcache_mp_connector" not in code or "http_server" not in code:
            fail("the install check does not import the connector and the server")
        log(check=True)
        print("KVENV " + json.dumps({"torch": "2.9.0+cu130", "torch_cuda": "13.0", "cuda_available": True,
                                     "gpu": "NVIDIA L40S", "vllm": VLLM_VERSION, "lmcache": "0.5.7.post1"}))
    elif argv[:1] == ["-c"]:
        log(code=argv[1])
    else:
        fail(f"unexpected python call {argv}")
elif prog == "lmcache":
    if argv[:1] != ["server"]:
        fail("only 'lmcache server' is expected")
    try:  # LMCache's own parser, when LMCache is importable
        from lmcache.cli.commands.server import ServerCommand
        from lmcache.v1.distributed.config import parse_args_to_config
        p = argparse.ArgumentParser()
        ServerCommand().add_arguments(p)
        a = p.parse_args(argv[1:])
        cfg = parse_args_to_config(a)
        validated = "LMCache " + os.path.dirname(sys.modules["lmcache"].__file__)
        del cfg   # parse_args_to_config accepted them (it raises on an invalid combination)
        lazy = getattr(a, "l1_use_lazy", None)
        adapters = [json.loads(x) for x in a.l2_adapter]
        http_port = a.http_port
    except ImportError:
        p = argparse.ArgumentParser()
        for f in ("--host", "--port", "--http-host", "--http-port", "--l1-size-gb", "--eviction-policy",
                  "--hash-algorithm"):
            p.add_argument(f, required=True)
        p.add_argument("--no-l1-use-lazy", action="store_true", required=True)
        p.add_argument("--l2-adapter", action="append", required=True)
        a = p.parse_args(argv[1:])
        validated, lazy = "fallback parser", not a.no_l1_use_lazy
        adapters = [json.loads(x) for x in a.l2_adapter]
        http_port = int(a.http_port)
    except SystemExit:
        fail("LMCache's parser rejected the arguments")
    ad = adapters[0]
    if (ad.get("type"), ad.get("module_path"), ad.get("class_name")) != \
            ("plugin", "roce.lmcache_connector", "RoCEPoolL2Adapter"):
        fail(f"unexpected L2 adapter {ad}")
    log(validated=validated, l1_lazy=lazy, adapter=ad)
    params = ad["adapter_params"]
    os.execv(sys.executable, [sys.executable, os.path.join(ROOT, "tests", "fakes", "fake_inference_server.py"),
                              "--port", str(int(http_port) - 6556 + 8000), "--lmcache-http-port", str(http_port),
                              "--osts", params["osts"], "--namespace", params["namespace"]])
elif prog == "vllm":
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["serve"])
    p.add_argument("model")
    for f in ("--host", "--port", "--max-model-len", "--gpu-memory-utilization", "--kv-transfer-config"):
        p.add_argument(f, required=True)
    p.add_argument("--no-enable-prefix-caching", action="store_true", required=True)
    # vLLM 0.34's extensible KV cache cannot be shared by CUDA IPC, which
    # LMCache's MP connector uses: the harness must turn it off there, and
    # must not pass the flag to an older vLLM, which does not know it
    if tuple(int(x) for x in VLLM_VERSION.split(".")[:2]) >= (0, 34):
        p.add_argument("--no-enable-extensible-kv-cache", action="store_true", required=True)
    try:
        a = p.parse_args(argv)
    except SystemExit:
        fail("unexpected vllm arguments")
    if os.environ.get("VLLM_USE_FLASHINFER_SAMPLER") != "0":
        # vLLM 0.33 on a host without ninja: the FlashInfer sampler's JIT build fails
        fail("vLLM started without VLLM_USE_FLASHINFER_SAMPLER=0")
    if os.environ.get("KVENV_STUB_REJECT_BI") and os.environ.get("VLLM_BATCH_INVARIANT") == "1":
        log(rejected_batch_invariant=True)   # as a GPU without batch-invariant kernels would
        sys.exit(1)
    kvt = json.loads(a.kv_transfer_config)
    want = {"kv_connector": "LMCacheMPConnector", "kv_role": "kv_both",
            "kv_connector_module_path": "lmcache.integration.vllm.lmcache_mp_connector"}
    if any(kvt.get(k) != v for k, v in want.items()):
        fail(f"kv-transfer-config {kvt}")
    extra = kvt.get("kv_connector_extra_config", {})
    if extra.get("lmcache.mp.autostart") is not False or not extra.get("lmcache.mp.port"):
        fail(f"kv_connector_extra_config {extra}")
    mod = None
    try:  # the connector module exists in the LMCache that is installed
        import lmcache
        mod = os.path.join(os.path.dirname(lmcache.__file__), "integration", "vllm", "lmcache_mp_connector.py")
        if not os.path.exists(mod):
            fail(f"no {mod}")
    except ImportError:
        pass
    log(kvt=kvt, model=a.model, port=a.port, connector_module=mod,
        env={k: os.environ.get(k) for k in ("PYTHONHASHSEED", "VLLM_BATCH_INVARIANT", "VLLM_USE_FLASHINFER_SAMPLER")})
    while True:
        time.sleep(3600)
else:
    fail("unknown stub name")
