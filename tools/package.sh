#!/usr/bin/env bash
# Build the distributable package: sources + deps/, and NO binaries -- a
# library built here (another glibc / compiler) does not load on the site
# hosts; everything is compiled there (run.sh, build_lib.py, build.sh).
#   tools/package.sh [OUT.tar.gz]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$PWD/rocepool-full-$(date +%Y%m%d).tar.gz}"
cd "$(dirname "$ROOT")"
N="$(basename "$ROOT")"
tar czf "$OUT" \
  --exclude="$N/.bench_cache" --exclude="$N/aws_runs" --exclude="$N/mnt" --exclude="$N/.git" \
  --exclude="$N/roce/native/ofi" --exclude="$N/roce/native/.backend" \
  --exclude='*.so' --exclude='*.so.*' --exclude='*.o' --exclude='*.a' --exclude='*.pyc' --exclude='__pycache__' \
  --exclude='*.buildinfo' --exclude="$N/roce/native/rocepool-fuse" --exclude="$N/roce/native/rocepool-kv-bench" \
  --exclude="$N/roce/native/softverbs/rocepool-fuse" --exclude="$N/roce/native/softverbs/rocepool-kv-bench" \
  --exclude='rocepool-results-*' --exclude='onsite_*.json' --exclude='run.log' --exclude='selftest.log' \
  --exclude='roce_env.sh' --exclude="$N/cluster.conf" "$N"
# no ELF may ship
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
tar xzf "$OUT" -C "$T"
ELF=$(find "$T" -type f -exec sh -c 'head -c4 "$1" | grep -q "ELF" && echo "$1"' _ {} \; | sed "s|$T/||")
if [ -n "$ELF" ]; then echo "ERROR: binaries in the package:"; echo "$ELF"; rm -f "$OUT"; exit 1; fi
echo "$OUT ($(du -h "$OUT" | cut -f1))"
