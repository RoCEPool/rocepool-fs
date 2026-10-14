# GPUDirect Storage: streaming file data into GPU memory

Rendering, training and simulation jobs on many GPUs spend a large part of
their time waiting for data. The bytes usually take this path: the NIC writes
them into host memory, the CPU copies them into a pinned buffer, and the GPU
copies them over PCIe again. With GPUDirect Storage (GDS) the NIC writes
straight into GPU memory. That removes the host bounce buffer, the CPU copy
and half of the PCIe traffic, and it is what WEKA's and DDN's GDS support
provide.

rocepool now offers the same through NVIDIA's cuFile API, which GDS-enabled
software already calls. That software includes PyTorch/KvikIO, RAPIDS
cuDF, DALI, and renderers and USD pipelines that load geometry and textures
with cuFile.

## How it works

`roce/native/rocepool_cufile.c` builds into `librocepool_cufile.so`. A copy
is placed at `roce/native/cufile/libcufile.so.0`, with a `libcufile.so`
symlink next to it for linking.

- **Reads (`cuFileRead`)**:
  1. The library finds the GPU allocation the buffer belongs to.
  2. It registers that allocation with the RDMA NIC, either as a dma-buf
     (`cuMemGetHandleForAddressRange` then `ibv_reg_dmabuf_mr`) or by the
     device pointer through nvidia-peermem.
  3. It reads every stripe of the range one-sided (OST lease, then RDMA
     READ) from all OSTs in parallel. The NIC places each stripe at its
     offset in GPU memory, and no CPU touches the data.
- **Writes (`cuFileWrite`)**: each OST RDMA-reads its stripes out of GPU
  memory. The request carries the GPU buffer's registration, so the bytes
  are never copied into host memory.
- **Small files** (stored inline with the metadata on the MDT) and **holes**
  are handled differently. Small files come with the metadata reply and are
  copied into the GPU with one `cuMemcpy`. Holes are zero-filled on the GPU
  (`cuMemsetD8`).
- **Fallbacks, chosen automatically**:
  - If GPU memory cannot be registered with the NIC (proprietary NVIDIA
    kernel modules and no nvidia-peermem), transfers go through host memory
    and the library counts them as "bounced".
  - Files that are not on rocepool are passed to NVIDIA's real libcufile if
    one is installed, or else read with `pread` plus a copy.
- **API coverage**:
  - Driver open/close and properties.
  - Handle and buffer registration (a buffer that was never registered is
    registered on first use).
  - `cuFileRead`/`Write`.
  - The stream API (`cuFileReadAsync`/`WriteAsync`, ordered after
    `cuStreamSynchronize`).
  - The batch API (`cuFileBatchIOSetUp`/`Submit`/`GetStatus`/`Destroy`),
    where entries run in parallel.
- **No CUDA is needed to build.** `libcuda.so.1` is opened at run time.

## Using it

```bash
# on the GPU node, after roce/native/build.sh
export LD_LIBRARY_PATH=/path/to/rocepool/roce/native/cufile:$LD_LIBRARY_PATH
export ROCEPOOL_MDTS=192.168.10.19:19901 ROCEPOOL_OSTS=192.168.10.19:19951,192.168.10.150:19951,...
export ROCEPOOL_PREFIX=/mnt/rocepool          # the FUSE mount point (or the interception library's prefix)
python3 train.py / your renderer              # its cuFile calls now go to rocepool
```

The library serves two kinds of file descriptors:

- Descriptors of files under `ROCEPOOL_PREFIX` opened through the **FUSE
  mount**. The data path goes around the kernel; the mount is still used for
  `open`, `stat` and so on.
- Descriptors of the **LD_PRELOAD interception library**
  (`librocepool_intercept.so`) in the same process.

After a direct write, the library rewrites the last byte through the
descriptor itself. This updates the size seen by the mount, so other
processes see the new size at once.

| variable | |
|---|---|
| `ROCEPOOL_IO_THREADS` | parallel stripe transfers per process (default 16) |
| `ROCEPOOL_STRIPE_SIZE`, `ROCEPOOL_STRIPE_COUNT` | layout of files created through cuFileWrite |
| `ROCEPOOL_GDS_BOUNCE=1` | force the host-memory path (for comparison) |
| `ROCEPOOL_CUFILE_REAL` | NVIDIA's libcufile for files that are not on rocepool |
| `ROCE_OFI_HMEM=1` | EFA (libfabric backend): open endpoints with `FI_HMEM` so GPU memory can be registered |

`rp_cufile_stats(&direct, &bounced, &posix)`, exported by the library,
reports how many bytes went each way.

## On site (automatic)

`run.sh` / `tools/roce_onsite.py` has a **gds** step, written to
`onsite_gds.json` and included in the results tarball. It runs
`tools/bench_gds.py`:

1. **Inventory** of every host:
   - GPUs (`nvidia-smi`).
   - Driver.
   - Open or proprietary kernel modules, which decides dma-buf support.
   - nvidia-peermem, loaded with `modprobe` if it is missing.
   - The PCIe relation of each GPU to each RDMA NIC: PIX/PXB means behind
     the same PCIe switch, which is best; PHB, NODE and SYS mean the path
     crosses the root complex or the socket link. A note is added when they
     are on different sockets.
2. **Benchmark** on a host with GPUs:
   1. Writes a 1 GiB file (4 GiB with `--bench full`) through the
      interception library.
   2. Reads it into GPU memory with a cuFile program (`tests/fakes/gds_probe.c`)
      three ways: **direct**, **via host memory** (what storage without GDS
      gets), and **all GPUs at once**. Each result is reported in GB/s.

A host without GPUs reports "no NVIDIA GPU" and the step passes.

## What has been verified, and where

The development machine has no GPU. The tests use an **emulated device**
(`ROCEPOOL_DEV_EMU=1`):

- Its memory is a memfd whose "device pointer" range is mapped `PROT_NONE`,
  so any CPU access to it crashes the process.
- The RDMA emulator reaches it through the dma-buf registration
  (`ibv_reg_dmabuf_mr`), the same way a NIC reaches a GPU.

`tests/test_gds.py` checks the following:

- Striped files of 10 stripes over 4 OSTs arrive in device memory
  byte-exact, with **every byte by RDMA and zero bytes through host
  memory**.
- Unaligned ranges, reads past end of file, sparse files and inline small
  files are correct.
- Writes from device memory work, including a write that spills an inline
  file. Each OST RDMA-reads its stripes out of the device.
- The batch and async APIs work.
- When no NIC registration is possible, the fallback is correct.
- Files on other file systems go through the POSIX path.
- FUSE-mount descriptors work, and the new size is visible on the mount at
  once.
- The libfabric backend is correct (its test provider has no `FI_HMEM`, so
  there the data goes through host memory).
- The inventory works on a fake sysfs.
- `tools/bench_gds.py` runs end to end.

**Still to be confirmed on real GPUs:** the CUDA side and the NIC side.

- The CUDA side is `cuPointerGetAttribute`, the dma-buf export, and the
  context handling for `cuMemcpy`/`cuMemset`.
- The NIC side is `ibv_reg_dmabuf_mr` on a real mlx5 with a GPU dma-buf, or
  `ibv_reg_mr` with nvidia-peermem.

The on-site gds step does exactly this check on the first site with GPUs. It
reports FAIL with the error if something is wrong, and WARN if the transfers
bounce instead of going direct.

## KV cache: blocks straight into GPU memory (prefix caching, prefill/decode)

### Why files are the wrong unit for the KV cache

LLM serving reuses KV cache in two places:

- **Prefix caching.** A system prompt, a multi-turn history or RAG context
  that an earlier request already prefilled.
- **Prefill/decode disaggregation.** The decode node needs what the prefill
  node computed.

In both cases the time to first token (TTFT) is the lookup and load of the
cached prefix plus the prefill of the rest of the prompt. KV blocks are
small (64 KiB per layer, or a few MiB for all layers of a block), there are
many of them, and the transfer is on the critical path. Files would add a
metadata lookup to every block.

### The GPU calls of the KV API

The KV API (`rocepool_kv.h`, Python `roce/kv.py`) has calls that move values
between the OSTs' RAM and GPU memory with no host copy and no metadata
server. A key maps to its OST by hash.

| C | Python | |
|---|---|---|
| `rpkv_dev_register(kv, ptr, len)` | `kv.register_device(tensor.data_ptr(), nbytes)` | registers the GPU allocation with the NIC once (dma-buf, or nvidia-peermem) |
| `rpkv_mcontains` | `kv.contains(keys)` / `kv.cached_prefix(block_keys)` | which blocks exist, in one BATCH round trip per OST; it also caches their leases |
| `rpkv_mget_dev` / `rpkv_get_dev` | `buf.mget(keys, offsets, caps)` / `buf.get` | each block arrives by a one-sided RDMA READ from this host's NIC into the GPU, many in flight across all OSTs |
| `rpkv_mput_dev` / `rpkv_put_dev` | `buf.mput(keys, offsets, lengths)` / `buf.put` | each OST RDMA-reads its blocks out of the GPU |

How the cached leases behave:

- A **lease** says where a value sits in an OST's RAM, and a handle keeps
  the leases it has taken.
- A **block read again** costs one RDMA READ and no RPC.
- **Every read checks the value's seqlock**, so an overwritten, deleted or
  evicted value is never returned stale. In those cases the client takes a
  new lease, or falls back to an RPC.
- **When the GPU cannot be reached by the NIC**, the same calls work through
  host memory, and `buf.direct` is False.
- **Value size**: one value can be up to one frame, 16 MiB by default
  (`ROCEPOOL_FRAME_BYTES`). That fits a 16-token block of all layers of a
  70B model (5.2 MB).
- **Concurrency**: the number of transfers in flight per call is set by
  `ROCEPOOL_KV_INFLIGHT` (default 16).

### The TTFT benchmark (`tools/bench_kvcache.py`, on-site step `kvcache`)

For a model's KV geometry (Llama-3 8B/70B, Qwen2.5 7B/72B, Mistral 7B), the
benchmark runs these steps:

1. Store the KV blocks of a few distinct prefixes from GPU memory.
2. Run R requests, C at a time. Each one looks up how many leading blocks
   of its prompt are cached, then loads them into GPU memory.
3. Check every loaded block against its pattern.
4. Report:
   - load p50/p99 and GB/s;
   - lookup time;
   - TTFT with the cache, which is load + the prefill of the uncached rest;
   - TTFT recomputing everything.

The **prefill time is an estimate**: 2 × parameters × tokens / (the GPU's
dense BF16 peak × MFU 0.5). The peak is taken from the GPU's name. It
leaves out attention's quadratic part, which understates the benefit for
long prompts.

Each run covers three cases, each with a key per block of all layers and
with a key per block per layer:

- **GPU direct**
- **GPU through host memory** — what storage without GPUDirect gets
- **host memory only** — on hosts without a GPU

The on-site run uses Llama-3-8B with 3072 of 4096 tokens cached (`--bench
quick`), or Llama-3-70B with 6144 of 8192 cached (`--bench full`). The
results are in `onsite_kvcache.json`.

### What has been verified

`tests/test_kv_gpu.py` runs on the emulated device, so a stray CPU access
crashes the test. It checks:

- round trips from 1 byte to 15 MiB;
- interoperability with the host-memory API;
- missing keys, too-small buffers, and ranges outside the buffer;
- that a cached lease detects another client's overwrite and delete;
- a prefix lookup of 400 keys in at most one round trip per OST, and the
  load that follows using no lease RPCs;
- the fallback without NIC registration;
- refusal of host memory;
- the benchmark tool end to end.

The numbers from the emulator are software RDMA on one machine. They show
that the path works, not what the hardware does: on a real GPU host the
`kvcache` step measures that.
