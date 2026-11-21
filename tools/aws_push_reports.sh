#!/usr/bin/env bash
# Publish an AWS run's directory to branch aws-results/<run id> (force-pushed,
# so it can run again and again: mid-run by tools/aws_cluster.py's
# --progress-hook, then once more at the end).
#
#   tools/aws_push_reports.sh RUN_ID [RUN_DIR] [EXTRA_FILE...]
#
# Never published: the run's SSH key, *.pem, id_*, cluster.conf, the shipped
# tree (rocepool.tar.gz), files of 20 MB or more.
# Needs GH_TOKEN and GITHUB_REPOSITORY (or REMOTE_URL, e.g. a local bare
# repository in tests).
set -euo pipefail
run_id="$1"; shift
d="${1:-aws_runs/$run_id}"; [ $# -gt 0 ] && shift
[ -d "$d" ] || { echo "aws_push_reports: no $d"; exit 0; }
remote="${REMOTE_URL:-https://x-access-token:${GH_TOKEN:?GH_TOKEN not set}@github.com/${GITHUB_REPOSITORY:?GITHUB_REPOSITORY not set}}"
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
mkdir -p "$out/$run_id"
(cd "$d" && find . -type f ! -name ssh_key ! -name '*.pem' ! -name 'id_*' ! -name 'cluster.conf' \
   ! -name 'rocepool.tar.gz' -size -20M -print0 | xargs -0 -r -I{} cp --parents {} "$out/$run_id/")
for f in "$@"; do [ -f "$f" ] && cp "$f" "$out/$run_id/"; done
cd "$out"
git init -q
git checkout -q -b "aws-results/$run_id"
git add -A
git -c user.name=github-actions -c user.email=41898282+github-actions[bot]@users.noreply.github.com \
  commit -qm "aws-test reports $run_id ($(date -u +%H:%M:%SZ))" --allow-empty
git push -q -f "$remote" "aws-results/$run_id"
echo "aws_push_reports: branch aws-results/$run_id ($(find "$run_id" -type f | wc -l) files)"
