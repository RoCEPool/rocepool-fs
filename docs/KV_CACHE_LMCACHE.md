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
- **Not yet run end to end.** No end-to-end run with vLLM on GPUs has been
  done. That run, and throughput and time-to-first-token figures on real
  RDMA NICs, are the next step.

## Not covered yet

- **NVIDIA Dynamo.** Dynamo's KV block manager offloads through NIXL. A
  NIXL backend plugin for RoCEPool would be a separate piece of work.
- **GPU memory as the L1.** LMCache's L1 is host memory, which is what this
  adapter registers. The KV API's GPU calls (`rpkv_*_dev`,
  [GPUDIRECT_STORAGE.md](GPUDIRECT_STORAGE.md)) already move blocks between
  the OSTs and GPU memory, for engines that drive the store directly.
