# RoCEPool as a KV-cache backend for inference engines (LMCache)

vLLM and SGLang offload KV cache through [LMCache](https://github.com/LMCache/LMCache).
RoCEPool plugs into LMCache's storage tier (L2), so an inference cluster
can keep KV cache that no longer fits in GPU or host memory on RoCEPool and
share it between servers. A prefix computed on one server can then be
loaded by another instead of being recomputed.

The integration is software only. It needs no particular NIC or DPU: it
runs on any RDMA transport RoCEPool supports (RoCE, InfiniBand, EFA,
Soft-RoCE).

## How it fits

```
vLLM / SGLang ──(LMCacheMPConnector)──> lmcache server ──(L2 adapter)──> RoCEPool OSTs
                                          L1: pinned host memory             RAM (+ NVMe tier)
```

LMCache keeps its L1 cache in pinned host memory and loads and stores L2
chunks between that memory and an L2 adapter. RoCEPool's adapter is
`roce.lmcache_connector`:

| | |
|---|---|
| Python | `roce/lmcache_connector.py`: `RoCEPoolL2Adapter` (adapter type `plugin`) and `RoCEPoolConnector` (adapter type `native_plugin`) |
| C | `roce/native/rocepool_lmc.c` (`rocepool_lmc.h`), built into `librocepool_kv.so` |

The data path is in C. Python only passes buffer addresses and collects
completions.

## Configuration

Start the LMCache server with RoCEPool as an L2 adapter, then point vLLM at
it as usual:

```bash
export PYTHONPATH=/path/to/rocepool-fs:$PYTHONPATH
export ROCEPOOL_KV_LIB=/path/to/rocepool-fs/roce/native/librocepool_kv.so   # roce/native/build.sh

lmcache server --l1-size-gb 60 --eviction-policy LRU --no-l1-use-lazy \
  --l2-adapter '{
    "type": "plugin",
    "module_path": "roce.lmcache_connector",
    "class_name": "RoCEPoolL2Adapter",
    "adapter_params": {"osts": "10.0.0.1:19951,10.0.0.2:19952,10.0.0.3:19953"}
  }'

vllm serve <model> --kv-transfer-config \
  '{"kv_connector":"LMCacheMPConnector", "kv_role":"kv_both"}'
```

Use type `plugin` with `RoCEPoolL2Adapter`. LMCache passes this adapter the
address of its L1 arena, which is registered with the NIC once, so every
transfer is zero-copy. Type `native_plugin` with `RoCEPoolConnector` also
works, but LMCache does not give that type the arena, so its transfers go
through a copy.

Start the server with `--no-l1-use-lazy`. With LMCache's default lazy L1
allocator, only the part of the arena pinned at start-up is exposed for
registration, and chunks outside that part are transferred through a
copy.

`adapter_params`:

| Key | Default | Meaning |
|---|---|---|
| `osts` | env `ROCEPOOL_OSTS` | OST list, `host:port,...`, the same on every server |
| `namespace` | `lmcache/` | key prefix; keeps KV cache apart from file data on the same OSTs, or separates deployments |
| `workers` | 4 | LMCache operations executed at once |
| `part_mb` | 4 | largest part a chunk is split into (capped below half a frame) |
| `max_capacity_gb` | 0 | capacity for LMCache's L2 usage accounting and eviction (0: unlimited) |

## How chunks are stored

- **Parts.** A chunk (one LMCache key, often tens of MB for a large model)
  is stored as parts of at most `part_mb`: `<key>#0`, `<key>#1`, ... Each
  part goes to the OST its hash selects, so one chunk is striped over all
  OSTs and moved by all of them in parallel.
- **Commit record.** After every part is stored, a 24-byte commit record
  is written under `<key>` itself. It holds the chunk size and the part
  size.
- **Lookups** check commit records only. A chunk whose store was cut short
  is never reported as present.
- **Loads** read the parts straight into LMCache's buffer. The buffer size
  determines the part count, so no extra round trip is needed. A part of
  the wrong size fails the load of that key.
- **Deletes** (LMCache eviction) remove the commit record first, so the
  chunk stops existing, and then its parts.

## Zero copy

| | Load (L2 to L1) | Store (L1 to L2) |
|---|---|---|
| `plugin`, L1 arena registered | one-sided RDMA READ by this host's NIC into the arena | RDMA READ by the OST out of the arena |
| buffer outside a registered region | read into a buffer, then copied | sent from the buffer (copied into the frame) |

On a NIC with hardware RDMA READ, neither direction involves a server CPU
or a host memcpy. `RoCEPoolConnector.stats()` reports the bytes moved
zero-copy and the bytes copied.

## Semantics LMCache relies on

- **Completion.** Every submitted operation completes exactly once,
  signalled through the connector's event fd. On close, operations
  already queued are finished first.
- **Store results** are reported per task. A store succeeds when every
  chunk in it was committed.
- **Lookups and loads** report per key. A missing chunk fails only its own
  key.
- **Locking** is client-side in LMCache's adapter. RoCEPool does not evict
  chunks on its own, so a chunk LMCache found stays until LMCache deletes
  it.

## Tests

| Test | What it covers |
|---|---|
| `tests/test_lmcache_connector.py` | The connector against native OSTs over the RDMA emulator: round trips of chunk sizes 1 B to 9 MiB over many parts; zero copy from and to registered regions (no byte copied by the CPU); copies for other buffers; only committed chunks exist; delete removes the commit and every part; missing or resized chunks fail per key; 40 concurrent operations from 4 threads each complete once; close finishes queued operations. |
| `tests/test_lmcache_adapter_integration.py` | LMCache's own code drives the connector: its config parser and factory build the adapter (`plugin` and `native_plugin`), and its `NativeConnectorL2Adapter` runs store, lookup-and-lock, load, unlock, delete and usage accounting. Skipped where LMCache is not installed. |

### Validation so far

- **LMCache code.** The integration test ran against LMCache's main branch
  (2026-11-21), with LMCache's C++ extension built from source. That
  environment had no PyTorch, so only the imports of torch and numba were
  satisfied by an inert placeholder. The adapter logic under test does not
  use torch.
- **End to end on GPUs.** Passed on two AWS g6.8xlarge hosts on
  2026-11-21. See [Results on AWS g6.8xlarge](#results-on-aws-g68xlarge-2026-11-21).

## End-to-end test on GPU hosts

`tools/kvcache_e2e.py` runs vLLM + LMCache + RoCEPool on two GPU hosts and
writes `kvcache_e2e_report.json`. Its steps:

1. **prepare:** `tools/roce_onsite.py --bench none` runs the RDMA preflight,
   builds the tree and ships it to the peer.
2. **install:** LMCache 0.5.7 (PyPI release 0.5.7.post1) and vLLM 0.33.0 (the vLLM release LMCache's CI validated 0.5.7 with) go into a venv, and the model is
   downloaded.
3. **osts:** RoCEPool OSTs start on both hosts.
4. **engines:** on each host, an LMCache server starts with RoCEPool as L2
   and its L1 arena registered with the NIC, then vLLM starts with its own
   prefix cache off.
5. **tests:**

| Test | Pass when |
|---|---|
| T1 correctness | After host A's L1 is cleared, the same prompt loads its KV cache from RoCEPool and the answer equals an L1 hit's. Both answers use the same cached-KV computation, so only different bytes could change it. |
| T2 cross-server | Host B, which never saw the prompt, loads host A's KV cache from RoCEPool and answers like A's L1 hit. |
| T3 TTFT | Not pass/fail on time. For each prompt length, median time to first token is recorded for four cases: cold (recompute), L1 hit, L2 hit on the same host, L2 hit on the other host. The cached answers must agree. |
| T4 zero copy | The connectors' "bytes copied" counters stay at 0 on both hosts, so every byte moved by RDMA. |
| T5 one-sided | Host B loads prompts host A stored. The OSTs' own `data_out` counter (object bytes sent in replies) stays at about 0 while B loads, so B's NIC read the data itself. The RDMA preflight confirms hardware RDMA read. OST CPU per MiB loaded is reported next to OST CPU per MiB stored, with idle busy-polling subtracted. |
| T6 striping | A chunk (LMCache's chunk size, about 14 MiB for Qwen2.5-7B) is stored as at least 2 parts, and every OST holds data. When there are enough objects to judge (400 or more), every OST must also hold at least half its fair share. |

**Progress write-back.** Every step and test result is written to
`kvcache_e2e_progress.json` as soon as it is known, and again every 30 s.
On AWS, `tools/aws_cluster.py` brings that file and the run's output back
every 150 s and publishes them to branch `aws-results/<run id>`, so a run
cut short keeps its partial results.

On AWS, one command creates two GPU hosts with EFA, runs everything,
publishes the reports and deletes the hosts:

```bash
gh api -X POST repos/RoCEPool/rocepool-fs/actions/workflows/aws-test.yml/dispatches -f ref=main \
  -f "inputs[mode]=gpu" -f "inputs[type]=g6.8xlarge" -f "inputs[nodes]=2" \
  -f "inputs[max_hours]=2.5" -f "inputs[budget_usd]=15" \
  -f "inputs[disk_gib]=200" -f "inputs[disk_mbps]=250" -f "inputs[disk_iops]=3000" \
  -f "inputs[command]=python3 tools/kvcache_e2e.py"
```

**Instance choice.** g6.8xlarge (1 × NVIDIA L4) is the smallest GPU type
whose EFA supports hardware RDMA read and write (Nitro v4). g5 types have
EFA without RDMA read or write.

**Hosts of your own:** run `python3 tools/kvcache_e2e.py --hosts <this
host>,<other host>` on the first host, with root ssh to the other.

**Rehearsal without GPUs:** `tests/test_kvcache_e2e.py` runs the same steps
and tests locally, in two ways:

- against a stand-in engine (`tests/fakes/fake_inference_server.py`) that
  serves vLLM's and LMCache's HTTP APIs and moves KV chunks through the
  real connector;
- against stand-in `pip`, `uv`, `lmcache` and `vllm` executables, which
  check the harness's real install script and command lines. The
  `lmcache server` arguments are checked by LMCache's own parser when
  LMCache is importable.

## Results on AWS g6.8xlarge (2026-11-21)

| | |
|---|---|
| Hosts | 2 × g6.8xlarge (1 × NVIDIA L4 24 GB, 32 vCPU), EFA with hardware RDMA read |
| Software | vLLM 0.33.0, LMCache 0.5.7.post1, torch 2.14.0 (CUDA 13.0), RoCEPool `7c853db` |
| Model | Qwen/Qwen2.5-7B-Instruct, bf16 |
| RoCEPool | 4 OSTs in RAM, 2 per host, over EFA (libfabric) |
| Run | GitHub Actions run 44913323539; report and logs on branch `aws-results/gh-44913323539` |

Every test passed:

| Test | Result |
|---|---|
| T1 correctness | 2/2 answers identical after a reload from RoCEPool (reference: an L1 hit), and equal to the cold recompute; 420 MiB loaded |
| T2 cross-server | 2/2 prompts computed on host A were loaded on host B from RoCEPool and answered identically |
| T4 zero copy | 17.96 GiB moved by RDMA over both hosts, 0 bytes copied by the CPU |
| T5 one-sided | Host B loaded 1302 MiB stored by A. The OSTs sent 0 bytes of it, so B's NIC read the data itself. Hardware RDMA read confirmed by the preflight. |
| T6 striping | LMCache chunks of 14.0 MiB stored as 4 parts of 4.0 MiB over 4 OSTs (1672 to 1748 MiB each; 2445 objects; largest/smallest 1.05) |

**T5 in detail.** During B's loads, the OSTs still answered 493 small
requests: the lookups of the 24-byte commit records that say a chunk is
complete. These took 30 ms of OST CPU in 8.6 s, which is 24 µs per MiB
loaded. Stores took 48 µs of OST CPU per MiB. Both figures are after
subtracting the OSTs' idle busy-polling. The data itself does not pass
through the OST's CPU; the commit lookups do.

**T3, time to first token** (median of 3):

| Prompt tokens | KV loaded | Cold (recompute) | L1 hit | L2, same host | L2, other host |
|---|---|---|---|---|---|
| 1016 | 42 MiB | 268 ms | 95 ms | 139 ms | 107 ms |
| 4086 | 210 MiB | 1150 ms | 123 ms | 224 ms | 157 ms |
| 8182 | 434 MiB | 2452 ms | 159 ms | 340 ms | 240 ms |
| 16374 | 882 MiB | 5441 ms | 231 ms | 562 ms | 395 ms |

At 16K tokens, loading the KV cache from RoCEPool cuts the time to first
token from 5.4 s to 0.4–0.6 s.

Loads on the other host were faster than on the host that stored the chunks.
In this run, host A always read freshly stored chunks first and host B
second. The logs place the difference in LMCache's lookup, which fetches
chunks from RoCEPool into L1. That step took about 330 ms on A and about
150 ms on B at 16K tokens. Moving data from L1 to the GPU took 33 ms on
both hosts. The gap grows with the amount of data, by about 0.17 ms per MiB.

The likely cause was in the OST. It registered each 64 MiB arena chunk with
the NIC when an object in that chunk was first leased, not when the chunk
was made. So the first reader of new data paid for pinning the chunks, while
holding the OST's store lock. Chunks are now registered by the write that
makes them, and `tests/test_ost_first_read.py` checks that reads register
none. T3 now alternates which host reads first, and has each host load the
chunks a second time. Its `order_study` reports whether slow loads go with
the first read or with host A. The next GPU run confirms whether the gap is
gone.

## Not covered yet

- **NVIDIA Dynamo.** Dynamo's KV block manager offloads through NIXL. A
  NIXL backend plugin for RoCEPool would be a separate piece of work.
- **GPU memory as the L1.** LMCache's L1 is host memory, which is what this
  adapter registers. The KV API's GPU calls (`rpkv_*_dev`,
  [GPUDIRECT_STORAGE.md](GPUDIRECT_STORAGE.md)) already move blocks between
  the OSTs and GPU memory, for engines that drive the store directly.
