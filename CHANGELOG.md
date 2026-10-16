# Changelog

Every version that reaches `main` and passes the build and the full test
suite is published automatically:

- a GitHub Release `v<version>`, with the offline package
  `rocepool-full-<version>.tar.gz` (every third-party source included);
- a container image `ghcr.io/rocepool/rocepool-fs:<version>` (and `:latest`).

Each section below becomes that release's notes.

## 20261016a

- **Public repository** `RoCEPool/rocepool-fs`.
- **Container image** on the GitHub Container Registry,
  `ghcr.io/rocepool/rocepool-fs`. It holds the tree built for RoCE (verbs)
  with the RDMA emulator ready, for clients and servers in containers and
  for trying rocepool without RDMA hardware. See
  [docs/CONTAINER.md](docs/CONTAINER.md).
- **Release notes come from this changelog.** Earlier versions are released
  too, each built from the commit that introduced it.

## 20261014b

- **KV API reads and writes GPU memory directly**, for LLM KV-cache blocks
  (prefix caching, prefill/decode disaggregation):
  - Calls: `rpkv_dev_register`, `rpkv_get_dev`/`rpkv_put_dev`,
    `rpkv_mget_dev`/`rpkv_mput_dev` and `rpkv_mcontains`. In Python:
    `kv.register_device`, `kv.cached_prefix` and `buf.mget`/`buf.mput`.
  - A get is a one-sided RDMA READ into GPU memory. A put is RDMA-read by
    the storage server out of GPU memory. There is no host copy and no
    metadata server.
  - Leases are cached and checked against the object's seqlock on every
    read. A prefix lookup takes one round trip per storage server.
- **KV-cache TTFT benchmark** (`tools/bench_kvcache.py`, on-site step
  `kvcache`):
  - Uses the KV-cache geometry of real models: Llama-3 8B/70B, Qwen2.5 and
    Mistral.
  - Checks every loaded block.
  - Reports load p50/p99, GB/s, and TTFT with the cache versus recomputing
    the prompt. The prefill time is an estimate.

## 20261014a

- **GPUDirect Storage**: a cuFile-compatible `libcufile.so.0`.
  - Reads go from the storage servers' memory straight into GPU memory by
    RDMA, through a dma-buf or nvidia-peermem registration. Writes are
    RDMA-read by the servers out of GPU memory.
  - Fallbacks are automatic: small inline files, memory the NIC cannot
    register, and files on other file systems.
- **On-site step `gds`**:
  - Inventory of each host's GPUs, driver, dma-buf / peermem support, and
    PCIe distance between GPU and NIC.
  - Streaming benchmark: direct, through host memory, and all GPUs at once.

## 20261012b

- **The storage server's RAM cache is bounded on one-sided reads.** A lease
  now evicts like a write does. Before this fix, the site run's server was
  OOM-killed with 117 GiB resident against a 29 GiB budget.
- **The client probe works across hosts.** The MPI working directory is now
  created on every host.
- **The results bundle** keeps the previous run's outputs, and a failed probe
  reports its first error lines.

## 20261012a

- **Initial import:**
  - native C metadata (MDT) and object (OST) servers;
  - a FUSE client, an `LD_PRELOAD` interception client, and a KV API;
  - verbs and libfabric (EFA) transports, and a software RDMA emulator;
  - an NVMe tier;
  - unattended IO500 runs on N hosts from `cluster.conf`;
  - an offline build, with every third-party source in `deps/`.
