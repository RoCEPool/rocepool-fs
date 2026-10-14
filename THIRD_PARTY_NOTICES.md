# Third-party software

`deps/` holds unmodified-or-trimmed source archives of the tools rocepool
builds for an offline site. They are separate programs, built and run as
separate executables or libraries; each keeps its own license, whose full
text is inside its archive. `tools/make_deps.sh` rebuilds every archive from
its upstream source and documents what was trimmed.

| archive | upstream | license (file inside the archive) |
|---|---|---|
| `mpich-4.2.3-ch3.tar.xz` | https://www.mpich.org (v4.2.3; the yaksa, ucx and libfabric submodules removed) | BSD-style MPICH license (`mpich-4.2.3/COPYRIGHT`) |
| `io500-eb76386.tar.xz` | https://github.com/IO500/io500 (commit eb76386, with its pinned IOR, pfind and lz4; `prepare.sh` patched to build offline) | IO500: MIT (`io500/LICENSE`); bundled IOR: GPL-2.0 (`io500/build/ior/COPYRIGHT`); pfind: `io500/build/pfind/LICENSE`; lz4: BSD-2-Clause / GPL-2.0 (`io500/build/pfind/lz4/LICENSE`) |
| `ior-49b11cc.tar.xz` | https://github.com/hpc/ior (commit 49b11cc) | GPL-2.0 (`ior/COPYRIGHT`) |
| `libfabric-2.1.0.tar.xz` | https://github.com/ofiwg/libfabric (v2.1.0) | BSD-2-Clause or GPL-2.0, at your choice (`libfabric-2.1.0/COPYING`) |
| `rdma-core-59.2.tar.xz` | https://github.com/linux-rdma/rdma-core (v59.2) | GPL-2.0 or BSD (per component, `rdma-core-59.2/COPYING.md`) |

## RapidsSE: reference sources (`third_party/rapidsse/`)

`wlp/` (rocepool's client for RapidsSE's WLP v5 wire protocol) was written
against RapidsSE's own sources. The upstream files it refers to are kept,
byte for byte, in `third_party/rapidsse/` with RapidsSE's `LICENSE`,
`COPYRIGHT` and `README.md`; `third_party/rapidsse/UPSTREAM.md` gives the
version and which rocepool file uses which upstream file, and
`tests/test_wlp_against_rapidsse.py` checks the two agree.

| | |
|---|---|
| project | RapidsSE, the Rapids Storage Engine -- Copyright (C) 2026 Boray Data |
| license | Apache License 2.0 (`third_party/rapidsse/LICENSE`) |
| upstream | https://bitbucket.org/rapidsdataproject/rapidsse/ |
| version | commit `90012081f5a9` |
| files | `src/wlp/thrift/messageDefinitions/**`, `src/wlp/thrift/supports/WlpHeader.hpp`, `src/wlp/thrift/supports/BufferTransport.hpp`, `src/store/AdaptorWL.cpp` |

## Interfaces implemented against (not included)

| interface | where rocepool uses it | its license |
|---|---|---|
| libibverbs / librdmacm API (rdma-core; source in `deps/`) | `roce/native/roce_transport.c`; `softverbs/` and `test_stubs/` re-declare the subset used, written against the public API | GPL-2.0 or BSD |
| libfabric API (source in `deps/`) | `roce/native/ofi_transport.c` | BSD-2-Clause or GPL-2.0 |
| Linux UAPI `<linux/fuse.h>`, `<linux/io_uring.h>` (from the system's kernel headers at build time) | `roce/native/rocepool_fuse.c`; a few constants of newer kernels are spelled out there | (GPL-2.0 WITH Linux-syscall-note) OR BSD-2-Clause / OR MIT |
| NVIDIA cuFile API (GPUDirect Storage API reference) | `roce/native/rocepool_cufile.c` implements the API: the types, constants and function signatures are re-declared from the public documentation; no NVIDIA header or code is included | the API documentation's terms |
| CUDA driver API (`libcuda.so.1`, opened at run time, not linked or shipped) | `roce/native/rocepool_cufile.c`, `tests/fakes/gds_probe.c` | NVIDIA driver license (the user's installed driver) |
| Apache Thrift Compact Protocol (specification) | `wlp/compact_protocol.py`, a re-implementation | Apache-2.0 |

## rocepool

rocepool's own code (everything outside `deps/` and `third_party/`) is under the Apache License
2.0 (`LICENSE`). It links against libibverbs/librdmacm or libfabric through
their BSD-licensed options and does not include or link IOR, IO500 or MPICH:
those are built from `deps/` only to benchmark the file system.
