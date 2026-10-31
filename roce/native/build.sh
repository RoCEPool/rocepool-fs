#!/usr/bin/env bash
# Build the REAL transport library (roce_transport.c + rocepool_servers.c,
# linked against rdma-core's libibverbs/librdmacm) -> libroce_transport.so.
#
# No manual steps: it finds rdma-core's development files by itself, in
# this order, and says which one it used:
#   1. already installed (system headers, or ROCE_RDMA_CORE=<dir> pointing
#      at an rdma-core build/ or install prefix);
#   2. install the distro packages (needs root; apt-get or dnf/yum):
#        libibverbs-dev librdmacm-dev ibverbs-providers  /  rdma-core-devel
#   3. build rdma-core from source (github.com/linux-rdma/rdma-core, tag
#      ROCE_RDMA_CORE_TAG, default v59.2) into ~/.cache/rocepool -- needs
#      git, cmake and ninja or make; libnl is optional.
# Validated here: the sources build with -Wall -Wextra -Werror against
# rdma-core v59.2 built by path 3 (this environment has no RDMA device, so
# the result could not be RUN here -- tools/roce_onsite.py runs it on site).
#
# Two transports (ROCE_BACKEND, default auto):
#   verbs  roce_transport.c: RC queue pairs over rdma_cm -- RoCE NICs, and
#          Soft-RoCE (rxe) on any Ethernet NIC, AWS instances included
#   ofi    ofi_transport.c: libfabric reliable datagrams -- AWS EFA (SRD);
#          libfabric from ROCE_LIBFABRIC, the EFA installer's /opt/amazon/efa,
#          the distro (libfabric-dev) or built from source
#   auto   ofi when an EFA device exists (/sys/class/infiniband/*: PCI driver efa),
#          else verbs
# Same output names either way.
#
# Output: roce/native/libroce_transport.so (point ROCE_TRANSPORT_LIB at it;
# the native C FUSE client roce/native/rocepool-fuse is built alongside;
# tools/roce_onsite.py and tools/run_crossover_rdma.py --real do).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
CC="${CC:-gcc}"
OUT="${OUT:-$SCRIPT_DIR/libroce_transport.so}"
TAG="${ROCE_RDMA_CORE_TAG:-v59.2}"
CACHE="${ROCEPOOL_CACHE:-$HOME/.cache/rocepool}"
CFLAGS_EXTRA=()
LDFLAGS_EXTRA=()
DEPS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)/deps"   # bundled sources (tools/deps.py)
# unpack a bundled source tarball deps/<pattern> as directory $2; 1 when not bundled
unpack_dep() {
  local tb; tb=$(ls "$DEPS_DIR"/$1 2>/dev/null | sort | tail -1)
  [ -n "$tb" ] || return 1
  local tmp; tmp=$(mktemp -d "$(dirname "$2")/.deps-XXXX")
  tar xJf "$tb" -C "$tmp" && rm -rf "$2" && mv "$tmp"/* "$2"; local rc=$?
  rm -rf "$tmp"; [ $rc = 0 ] && echo "[build] using the bundled $(basename "$tb")"
  return $rc
}
no_download() {  # ROCEPOOL_OFFLINE=1: say what is missing instead of trying the network
  if [ "${ROCEPOOL_OFFLINE:-0}" = 1 ]; then echo "ERROR: ROCEPOOL_OFFLINE=1 and $1 is not bundled in $DEPS_DIR" >&2; exit 1; fi
}
BACKEND="${ROCE_BACKEND:-auto}"
if [ "$BACKEND" = auto ]; then
  # an EFA device: named efa* on older kernels, rdmap<bus>s<slot> on newer
  # ones -- its PCI driver is efa either way
  BACKEND=verbs
  for d in /sys/class/infiniband/*; do
    [ -e "$d" ] || continue
    case "$(basename "$d")" in efa*) BACKEND=ofi;; esac
    [ "$(basename "$(readlink -f "$d/device/driver" 2>/dev/null)")" = efa ] && BACKEND=ofi
  done
fi
case "$BACKEND" in verbs|ofi) ;; *) echo "ERROR: ROCE_BACKEND=$BACKEND (auto, verbs or ofi)" >&2; exit 1;; esac
echo "[build] transport backend: $BACKEND"

if [ "$BACKEND" = verbs ]; then
have_headers() {
  printf '#include <rdma/rdma_cma.h>\n#include <infiniband/verbs.h>\nint main(void){return 0;}\n' \
    | "$CC" -x c - -fsyntax-only "${CFLAGS_EXTRA[@]}" >/dev/null 2>&1
}

use_prefix() {  # an rdma-core build/ dir or install prefix
  local d="$1"
  [ -f "$d/include/infiniband/verbs.h" ] || return 1
  local lib="$d/lib"; [ -d "$d/lib64" ] && [ ! -e "$d/lib/libibverbs.so" ] && lib="$d/lib64"
  CFLAGS_EXTRA=(-I "$d/include")
  LDFLAGS_EXTRA=(-L "$lib" "-Wl,-rpath,$lib")
}

source_used=""
if [ -n "${ROCE_RDMA_CORE:-}" ] && use_prefix "$ROCE_RDMA_CORE" && have_headers; then
  source_used="ROCE_RDMA_CORE=$ROCE_RDMA_CORE"
elif have_headers; then
  source_used="system rdma-core headers"
fi

if [ -z "$source_used" ] && [ "$(id -u)" = 0 ]; then
  echo "[build] rdma-core development files missing: trying the distro packages"
  if command -v apt-get >/dev/null; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libibverbs-dev librdmacm-dev ibverbs-providers rdma-core >/dev/null 2>&1 || true
  elif command -v dnf >/dev/null; then
    dnf install -y rdma-core-devel libibverbs-utils librdmacm-utils >/dev/null 2>&1 || true
  elif command -v yum >/dev/null; then
    yum install -y rdma-core-devel libibverbs-utils librdmacm-utils >/dev/null 2>&1 || true
  fi
  have_headers && source_used="distro packages (installed now)"
fi

if [ -z "$source_used" ]; then
  src="$CACHE/rdma-core-$TAG"
  if [ ! -f "$src/build/include/infiniband/verbs.h" ]; then
    echo "[build] building rdma-core $TAG from source into $src (one-time)"
    command -v cmake >/dev/null || { echo "ERROR: need cmake to build rdma-core" >&2; exit 1; }
    mkdir -p "$CACHE"
    if [ ! -f "$src/CMakeLists.txt" ] && ! unpack_dep "rdma-core-*.tar.xz" "$src"; then
      no_download rdma-core
      git clone -q --depth 1 --branch "$TAG" https://github.com/linux-rdma/rdma-core.git "$src"
    fi
    gen=(); command -v ninja >/dev/null && gen=(-GNinja)
    nl=(); pkg-config --exists libnl-3.0 libnl-route-3.0 2>/dev/null || nl=(-DENABLE_RESOLVE_NEIGH=OFF)
    mkdir -p "$src/build"
    (cd "$src/build" && cmake "${gen[@]}" -DNO_MAN_PAGES=1 -DNO_PYVERBS=1 "${nl[@]}" \
       -DCMAKE_BUILD_TYPE=Release .. >/dev/null && cmake --build . -j"$(nproc)" >/dev/null)
  fi
  use_prefix "$src/build"
  have_headers || { echo "ERROR: rdma-core source build did not produce usable headers" >&2; exit 1; }
  source_used="rdma-core $TAG built from source ($src/build)"
fi

else
have_fabric() {
  printf '#include <rdma/fabric.h>\nint main(void){return 0;}\n' \
    | "$CC" -x c - -fsyntax-only "${CFLAGS_EXTRA[@]}" >/dev/null 2>&1
}
use_fabric_prefix() {
  local d="$1" lib
  [ -f "$d/include/rdma/fabric.h" ] || return 1
  for lib in "$d/lib64" "$d/lib" "$d/lib/x86_64-linux-gnu" "$d/lib/aarch64-linux-gnu"; do
    ls "$lib"/libfabric.so* >/dev/null 2>&1 && break
  done
  CFLAGS_EXTRA=(-I "$d/include")
  LDFLAGS_EXTRA=(-L "$lib" "-Wl,-rpath,$lib")
}
source_used=""
for d in "${ROCE_LIBFABRIC:-}" /opt/amazon/efa $(ls -d "$CACHE"/libfabric-* 2>/dev/null | sort -r); do
  [ -n "$d" ] || continue
  if use_fabric_prefix "$d" && have_fabric; then source_used="libfabric at $d"; break; fi
  CFLAGS_EXTRA=(); LDFLAGS_EXTRA=()
done
if [ -z "$source_used" ] && have_fabric && echo 'int main(void){return 0;}' | "$CC" -x c - -lfabric -o /dev/null 2>/dev/null; then
  source_used="system libfabric"
fi
if [ -z "$source_used" ] && [ "$(id -u)" = 0 ]; then
  echo "[build] libfabric missing: trying the distro packages"
  if command -v apt-get >/dev/null; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends libfabric-dev >/dev/null 2>&1 || true
  elif command -v dnf >/dev/null; then dnf install -y libfabric-devel >/dev/null 2>&1 || true
  elif command -v yum >/dev/null; then yum install -y libfabric-devel >/dev/null 2>&1 || true
  fi
  have_fabric && source_used="distro libfabric (installed now)"
fi
if [ -z "$source_used" ]; then
  FTAG="${ROCE_LIBFABRIC_TAG:-v2.1.0}"
  prefix="$CACHE/libfabric-$FTAG"; src="$prefix-src"
  echo "[build] building libfabric $FTAG from source into $prefix (one-time)"
  mkdir -p "$CACHE"
  if [ ! -f "$src/configure.ac" ] && ! unpack_dep "libfabric-*.tar.xz" "$src"; then
    no_download libfabric
    for t in git autoconf automake make; do command -v $t >/dev/null || { echo "ERROR: need $t to build libfabric" >&2; exit 1; }; done
    git clone -q --depth 1 --branch "$FTAG" https://github.com/ofiwg/libfabric.git "$src"
  fi
  (cd "$src" && { [ -x configure ] || ./autogen.sh >/dev/null; } && ./configure --prefix="$prefix" --disable-static \
     --enable-psm2=no --enable-psm3=no --enable-opx=no --enable-usnic=no --enable-ucx=no --enable-lpp=no >/dev/null \
   && make -j"$(nproc)" >/dev/null && make install >/dev/null)
  use_fabric_prefix "$prefix" && have_fabric || { echo "ERROR: libfabric build did not produce usable headers" >&2; exit 1; }
  source_used="libfabric $FTAG built from source ($prefix)"
fi
fi
echo "[build] using $source_used"
# extra link flags, e.g. -Wl,-rpath-link,<dir> for a libfabric whose own
# dependencies (libibverbs, libefa) are not on the default path
[ -n "${ROCE_LDFLAGS:-}" ] && LDFLAGS_EXTRA+=(${ROCE_LDFLAGS})
if [ "$BACKEND" = verbs ]; then
  TSRC=roce_transport.c; TLIBS=(-lrdmacm -libverbs -ldl)
else
  TSRC=ofi_transport.c; TLIBS=(-lfabric)
fi

# The OST's capacity tier (ost_tier.c) always has the kernel (file / block
# device, O_DIRECT) backend. Its SPDK backend (userspace NVMe driver) is
# built in when SPDK_DIR points at a built SPDK tree (./configure && make;
# build/lib/pkgconfig must exist) -- the OST then accepts
# ROCEPOOL_OST_TIER=spdk:<pci traddr>. Nothing here could be RUN without an
# NVMe bound to vfio-pci; the sources compile against SPDK v25.05's headers.
TIER_CFLAGS=()
TIER_LIBS=()
if [ -n "${SPDK_DIR:-}" ]; then
  if [ -d "$SPDK_DIR/build/lib/pkgconfig" ] && [ -f "$SPDK_DIR/include/spdk/nvme.h" ]; then
    export PKG_CONFIG_PATH="$SPDK_DIR/build/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
    TIER_CFLAGS=(-DROCEPOOL_SPDK -I "$SPDK_DIR/include")
    # static SPDK + DPDK: whole-archive, as SPDK's own examples link
    TIER_LIBS=(-Wl,--whole-archive $(pkg-config --libs spdk_nvme spdk_env_dpdk) -Wl,--no-whole-archive
               $(pkg-config --libs spdk_syslibs 2>/dev/null || echo "-lnuma -ldl -luuid"))
    echo "[build] OST tier: SPDK backend from $SPDK_DIR"
  else
    echo "ERROR: SPDK_DIR=$SPDK_DIR has no build/lib/pkgconfig (run ./configure && make there first)" >&2
    exit 1
  fi
else
  echo "[build] OST tier: kernel backend only (set SPDK_DIR=<built spdk tree> for the SPDK backend)"
fi
"$CC" -O2 -g -Wall -Wextra -pthread -fPIC -shared "${CFLAGS_EXTRA[@]}" "${TIER_CFLAGS[@]}" \
  "$TSRC" rocepool_servers.c ost_tier.c wal.c -o "$OUT" "${LDFLAGS_EXTRA[@]}" "${TIER_LIBS[@]}" "${TLIBS[@]}" -lpthread
echo "[build] built $OUT"
FUSE_OUT="${FUSE_OUT:-$SCRIPT_DIR/rocepool-fuse}"
"$CC" -O2 -g -Wall -Wextra -pthread "${CFLAGS_EXTRA[@]}" \
  rocepool_fuse.c rocepool_fs.c rocepool_client.c "$TSRC" -o "$FUSE_OUT" "${LDFLAGS_EXTRA[@]}" "${TLIBS[@]}" -lpthread
echo "[build] built $FUSE_OUT (native FUSE client)"
KV_OUT="${KV_OUT:-$SCRIPT_DIR/librocepool_kv.so}"
"$CC" -O2 -g -Wall -Wextra -pthread -fPIC -shared "${CFLAGS_EXTRA[@]}" \
  rocepool_kv.c rocepool_client.c rocepool_devmem.c "$TSRC" -o "$KV_OUT" "${LDFLAGS_EXTRA[@]}" "${TLIBS[@]}" -ldl -lpthread
"$CC" -O2 -g -Wall -Wextra -pthread -DRPKV_BENCH "${CFLAGS_EXTRA[@]}" \
  rocepool_kv.c rocepool_client.c rocepool_devmem.c "$TSRC" -o "$SCRIPT_DIR/rocepool-kv-bench" "${LDFLAGS_EXTRA[@]}" "${TLIBS[@]}" -ldl -lpthread
# KV-cache reuse (TTFT) benchmark over the KV API's GPU calls (tools/bench_kvcache.py)
"$CC" -O2 -g -Wall -Wextra -pthread "${CFLAGS_EXTRA[@]}" rocepool_kvcache_bench.c \
  rocepool_kv.c rocepool_client.c rocepool_devmem.c "$TSRC" -o "$SCRIPT_DIR/rocepool-kvcache-bench" "${LDFLAGS_EXTRA[@]}" "${TLIBS[@]}" -ldl -lpthread
echo "[build] built $KV_OUT (native KV client API) and rocepool-kv-bench"
# The LD_PRELOAD interception client: the client core with hidden visibility
# (only the libc entry points leave the .so), linked against rdma-core.
ICP_OUT="${ICP_OUT:-$SCRIPT_DIR/librocepool_intercept.so}"
ICP_TMP="$(mktemp -d)"
for src in rocepool_fs.c rocepool_client.c "$TSRC"; do
  "$CC" -O2 -g -Wall -Wextra -pthread -fPIC -fvisibility=hidden "${CFLAGS_EXTRA[@]}" -c "$src" -o "$ICP_TMP/$src.o"
done
"$CC" -O2 -g -Wall -Wextra -pthread -fPIC "${CFLAGS_EXTRA[@]}" -c rocepool_intercept.c -o "$ICP_TMP/intercept.o"
"$CC" -shared -pthread "$ICP_TMP"/*.o -o "$ICP_OUT" "${LDFLAGS_EXTRA[@]}" "${TLIBS[@]}" -ldl -lpthread
rm -rf "$ICP_TMP"
echo "[build] built $ICP_OUT (LD_PRELOAD interception client)"
# GPUDirect Storage: the cuFile API (libcufile.so.0) served by the client
# core, hidden like the interception library's; cufile/libcufile.so.0 is the
# copy GDS programs pick up through LD_LIBRARY_PATH. No CUDA needed to build.
CUF_OUT="${CUF_OUT:-$SCRIPT_DIR/librocepool_cufile.so}"
CUF_TMP="$(mktemp -d)"
for src in rocepool_fs.c rocepool_client.c "$TSRC" rocepool_devmem.c rocepool_cufile.c; do
  "$CC" -O2 -g -Wall -Wextra -pthread -fPIC -fvisibility=hidden "${CFLAGS_EXTRA[@]}" -c "$src" -o "$CUF_TMP/$(basename "$src").o"
done
"$CC" -shared -pthread -Wl,-soname,libcufile.so.0 "$CUF_TMP"/*.o -o "$CUF_OUT" "${LDFLAGS_EXTRA[@]}" "${TLIBS[@]}" -ldl -lpthread
rm -rf "$CUF_TMP"
mkdir -p "$SCRIPT_DIR/cufile"
cp -f "$CUF_OUT" "$SCRIPT_DIR/cufile/libcufile.so.0"
ln -sf libcufile.so.0 "$SCRIPT_DIR/cufile/libcufile.so"   # for -lcufile at link time
echo "[build] built $CUF_OUT and cufile/libcufile.so.0 (GPUDirect Storage, cuFile API)"
echo "$BACKEND" > "$SCRIPT_DIR/.backend"   # which transport the built artifacts use
