#!/usr/bin/env bash
# Rebuild deps/ (the bundled third-party sources, tools/deps.py) from upstream.
# Needs network, git, curl, autotools. The package ships the result, so
# sites never run this.
#   MPICH 4.2.3     release tarball minus modules/{yaksa,ucx,libfabric}
#                   (built as ch3:sock + dataloop: tools/bench_ior.py)
#   IO500           HEAD of github.com/IO500/io500 + its pinned IOR / pfind (+lz4)
#                   / schema tools under build/, IOR bootstrapped, prepare.sh
#                   patched to skip ./bootstrap when configure exists and to find
#                   the bundled IOR / pfind under build/ (upstream re-fetches them)
#   IOR             HEAD of github.com/hpc/ior, bootstrapped
#   libfabric       v2.1.0, autogen'ed
#   rdma-core       v59.2
set -euo pipefail
OUT="$(cd "$(dirname "$0")/.." && pwd)/deps"
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
cd "$W"
mkdir -p "$OUT"
curl -sSL -o mpich.tar.gz https://github.com/pmodels/mpich/releases/download/v4.2.3/mpich-4.2.3.tar.gz
tar xzf mpich.tar.gz && rm -rf mpich-4.2.3/modules/{yaksa,ucx,libfabric}
XZ_OPT=-9 tar cJf "$OUT/mpich-4.2.3-ch3.tar.xz" mpich-4.2.3

git clone -q https://github.com/IO500/io500.git io500
IOR_HASH=$(sed -n 's/^IOR_HASH=//p' io500/prepare.sh); PFIND_HASH=$(sed -n 's/^PFIND_HASH=//p' io500/prepare.sh)
mkdir -p io500/build
git clone -q https://github.com/hpc/ior.git io500/build/ior && git -C io500/build/ior checkout -q "$IOR_HASH"
git clone -q https://github.com/VI4IO/pfind.git io500/build/pfind && git -C io500/build/pfind checkout -q "$PFIND_HASH"
git clone -q --depth 1 https://github.com/lz4/lz4.git io500/build/pfind/lz4
git clone -q --depth 1 https://github.com/VI4IO/cdcl-schema-tools.git io500/build/cdcl-schema-tools
(cd io500 && ln -sfn build/cdcl-schema-tools schema-tools \
  && sed -i 's|^  \./bootstrap$|  [ -x configure ] \|\| ./bootstrap   # rocepool deps: shipped bootstrapped (no autotools needed)|' prepare.sh \
  && sed -i 's|  if \[ -d "\$ior_dir" \]; then|  if [ -d "$BUILD/$ior_dir" ]; then   # rocepool deps: bundled under build/|; s|  if \[ -d "\$pfind_dir" \]; then|  if [ -d "$BUILD/$pfind_dir" ]; then   # rocepool deps: bundled under build/|' prepare.sh \
  && cd build/ior && ./bootstrap)
H=$(git -C io500 rev-parse --short HEAD)
find io500 -name .git -prune -exec rm -rf {} + ; find io500 -name autom4te.cache -prune -exec rm -rf {} +
tar cJf "$OUT/io500-$H.tar.xz" io500

git clone -q --depth 1 https://github.com/hpc/ior.git ior && H=$(git -C ior rev-parse --short HEAD) && (cd ior && ./bootstrap)
rm -rf ior/.git ior/autom4te.cache; tar cJf "$OUT/ior-$H.tar.xz" ior

git clone -q --depth 1 --branch v2.1.0 https://github.com/ofiwg/libfabric.git libfabric-2.1.0 && (cd libfabric-2.1.0 && ./autogen.sh)
rm -rf libfabric-2.1.0/.git libfabric-2.1.0/autom4te.cache; XZ_OPT=-9 tar cJf "$OUT/libfabric-2.1.0.tar.xz" libfabric-2.1.0

git clone -q --depth 1 --branch v59.2 https://github.com/linux-rdma/rdma-core.git rdma-core-59.2
rm -rf rdma-core-59.2/.git; XZ_OPT=-9 tar cJf "$OUT/rdma-core-59.2.tar.xz" rdma-core-59.2
ls -la "$OUT"
