"""Release notes for version V: its section of CHANGELOG.md, plus how to get
it (used by .github/workflows/ci.yml for every release, backfilled ones too).

    python3 tools/release_notes.py 20261016a --sha <commit> [--repo owner/name]
"""
from __future__ import annotations

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def section(changelog: str, version: str) -> str:
    """the text under '## <version>' up to the next '## ' heading ('' if none)"""
    m = re.search(rf"^## {re.escape(version)}\s*$(.*?)(?=^## |\Z)", changelog, re.M | re.S)
    return m.group(1).strip() if m else ""


def notes(version: str, sha: str, repo: str, changelog: str, image: bool) -> str:
    body = section(changelog, version) or f"rocepool {version}."
    img = f"ghcr.io/{repo.lower()}"
    out = [body, "", "---", "",
           f"**Offline package**: `rocepool-full-{version}.tar.gz` below -- the whole tree with every "
           "third-party source in `deps/` (builds and runs with no network).",
           "", "```bash",
           f"tar xzf rocepool-full-{version}.tar.gz && cd rocepool",
           "cp cluster.conf.example cluster.conf     # list your hosts",
           "sudo ./run.sh", "```"]
    if image:
        out += ["", f"**Container image**: `docker pull {img}:{version}` "
                "([docs/CONTAINER.md](https://github.com/" + repo + "/blob/main/docs/CONTAINER.md))"]
    out += ["", f"Built from {sha} after the build and the full test suite passed."]
    return "\n".join(out) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("version")
    ap.add_argument("--sha", required=True)
    ap.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "RoCEPool/rocepool-fs"))
    ap.add_argument("--no-image", action="store_true", help="a backfilled release: no container image for it")
    args = ap.parse_args()
    with open(os.path.join(ROOT, "CHANGELOG.md")) as f:
        sys.stdout.write(notes(args.version, args.sha, args.repo, f.read(), not args.no_image))
    return 0


if __name__ == "__main__":
    sys.exit(main())
