# rocepool in a container: the whole tree, built for RoCE NICs (verbs
# backend: roce/native/libroce_transport.so, the FUSE client, the
# interception, KV and cuFile libraries) plus the software RDMA emulator
# build, so the test suite and the examples run without RDMA hardware.
# Published as ghcr.io/rocepool/rocepool-fs:<version> by .github/workflows/ci.yml.
# Usage: docs/CONTAINER.md
FROM ubuntu:24.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc g++ make cmake pkg-config python3 ca-certificates \
      libibverbs-dev librdmacm-dev rdma-core ibverbs-providers ibverbs-utils \
      fuse3 iproute2 kmod pciutils procps openssh-client \
    && rm -rf /var/lib/apt/lists/*
COPY . /opt/rocepool
WORKDIR /opt/rocepool
# the real RoCE build, then the emulator build (tests, examples without a NIC)
RUN ROCE_BACKEND=verbs bash roce/native/build.sh \
    && python3 roce/native/build_lib.py \
    && rm -rf /root/.cache
ENV ROCE_TRANSPORT_LIB=/opt/rocepool/roce/native/libroce_transport.so \
    ROCEPOOL_KV_LIB=/opt/rocepool/roce/native/librocepool_kv.so \
    PYTHONPATH=/opt/rocepool
LABEL org.opencontainers.image.source="https://github.com/RoCEPool/rocepool-fs" \
      org.opencontainers.image.description="rocepool: RDMA/RoCE memory-pool distributed file system (servers, FUSE / LD_PRELOAD / KV / cuFile clients, IO500 tools)" \
      org.opencontainers.image.licenses="Apache-2.0"
CMD ["sed", "-n", "/^## Quick reference/,$p", "docs/CONTAINER.md"]
