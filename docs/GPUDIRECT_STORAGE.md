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
