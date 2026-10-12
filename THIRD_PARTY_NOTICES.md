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

rocepool's own code (everything outside `deps/`) is under the Apache License
2.0 (`LICENSE`). It links against libibverbs/librdmacm or libfabric through
their BSD-licensed options and does not include or link IOR, IO500 or MPICH:
those are built from `deps/` only to benchmark the file system.
