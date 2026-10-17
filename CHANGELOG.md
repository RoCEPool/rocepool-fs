# Changelog

Every version that reaches `main` and passes the build and the full test
suite is published automatically:

- a GitHub Release `v<version>`, with the offline package
  `rocepool-full-<version>.tar.gz` (every third-party source included);
- a container image `ghcr.io/rocepool/rocepool-fs:<version>` (and `:latest`).

Each section below becomes that release's notes. Versions from before this
repository (20261014a and older) have no release page; each one's
commit and full source archive (`deps/` included, so it builds offline) are
linked under its heading.

## 20261017a

From the site run of 2026-10-16 (2 hosts, 20261012b). Everything passed apart
from Boray02's slow disk, but the official IO500 result was INVALID for two
software reasons, and the faster client was never measured:

- **IO500 find saw none of mdtest-hard's files.** That phase writes 5.4 M
  files into one directory, and listing one MDT's share of it needed about
  75 MB in a single reply, more than a 16 MiB frame. The listing failed and
  `find` silently skipped the directory. **Fixed:** directory listings are
  now paged (half a frame per page, resumed after the last name), and older
  servers and clients still work together.
- **Every client-mode probe exited at once** with "block size must be a
  multiple of transfer size": the probe used the run's ior-easy block / 8
  (1545 MiB). The run therefore fell back to FUSE without measuring the
  interception library. **Fixed:** blockSize is always a multiple of the
  2 MiB transfer.
- **ior-easy-write ran out of data at 290 s** of the 300 s stonewall: 75% of
  the capacity budget went to ior-easy and 20% to ior-hard, which only used
  a third of its share. **Fixed:** the official run now sizes both from the
  quick run's measured write speeds. ior-hard gets what it writes in 300 s
  (+15%) and ior-easy the rest. If even that is too little, the run says so
  before it starts.
- **The run explains its own validity.** IO500's INVALID reasons (beyond those
  a short run has by design), what `find` matched, and the cause when it is
  known now go into the report. The on-site step shows WARN instead of PASS
  when the result is invalid or every client probe failed.
- **Host clock offsets are measured.** IO500's find compares times set on
  different hosts, so the offsets are recorded and a warning is printed above
  2 s.

## 20261016a

- **Public repository** `RoCEPool/rocepool-fs`.
- **Container image** on the GitHub Container Registry,
  `ghcr.io/rocepool/rocepool-fs`. It holds the tree built for RoCE (verbs)
  with the RDMA emulator ready, for clients and servers in containers and
  for trying rocepool without RDMA hardware. See
  [docs/CONTAINER.md](docs/CONTAINER.md).
- **Release notes come from this changelog.**

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

Source: [commit 9c35e33](https://github.com/RoCEPool/rocepool-fs/commit/9c35e33fef66381b002201b714ac001d9263d527), [tar.gz](https://github.com/RoCEPool/rocepool-fs/archive/9c35e33fef66381b002201b714ac001d9263d527.tar.gz)

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

Source: [commit baed826](https://github.com/RoCEPool/rocepool-fs/commit/baed8264e90f78d6003c8ece52e8e45fa8bee6eb), [tar.gz](https://github.com/RoCEPool/rocepool-fs/archive/baed8264e90f78d6003c8ece52e8e45fa8bee6eb.tar.gz)

- **The storage server's RAM cache is bounded on one-sided reads.** A lease
  now evicts like a write does. Before this fix, the site run's server was
  OOM-killed with 117 GiB resident against a 29 GiB budget.
- **The client probe works across hosts.** The MPI working directory is now
  created on every host.
- **The results bundle** keeps the previous run's outputs, and a failed probe
  reports its first error lines.

## 20261012a

Source: [commit 4e71958](https://github.com/RoCEPool/rocepool-fs/commit/4e71958aa597c06c77c822b9caca39b7a715dc5c), [tar.gz](https://github.com/RoCEPool/rocepool-fs/archive/4e71958aa597c06c77c822b9caca39b7a715dc5c.tar.gz)

- **Initial import:**
  - native C metadata (MDT) and object (OST) servers;
  - a FUSE client, an `LD_PRELOAD` interception client, and a KV API;
  - verbs and libfabric (EFA) transports, and a software RDMA emulator;
  - an NVMe tier;
  - unattended IO500 runs on N hosts from `cluster.conf`;
  - an offline build, with every third-party source in `deps/`.
