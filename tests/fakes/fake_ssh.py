#!/usr/bin/env python3
"""A fake ssh / scp for tests/test_aws_cluster.py (argv[0] decides which).
Every call goes to $FAKE_SSH_LOG; the remote side behaves like a host that
sets up fine and whose run.sh finishes at once with a valid IO500 result."""
import json
import os
import sys

prog = os.path.basename(sys.argv[0])
args = sys.argv[1:]
with open(os.environ["FAKE_SSH_LOG"], "a") as f:
    f.write(json.dumps([prog] + args) + "\n")
if prog.startswith("fake_scp") or prog == "scp":
    src, dst = args[-2], args[-1]
    if "@" in src and ":" in src:  # fetching from the host
        name = os.path.basename(src.split(":", 1)[1])
        if name in ("onsite_report.json", "onsite_io500_valid.json", "run.log"):
            with open(dst, "w") as f:
                if name == "onsite_io500_valid.json":
                    json.dump({"score": {"score": 123.4, "bw_gib_s": 56.7, "md_kiops": 268.5}}, f)
                else:
                    f.write("{}\n")
            sys.exit(0)
        sys.exit(1)
    sys.exit(0)
cmd = args[-1]
if "cloud-init status" in cmd:
    print("up")
elif "ROCEPOOL_SETUP" in cmd:
    print("SETUP_OK")
elif "ssh-keygen" in cmd:
    print("ssh-ed25519 AAAAfake rocepool-cluster")
elif "wc -l < aws_run.out" in cmd:
    print("3\n0")
elif cmd.startswith("sed -n"):
    print("== 3/4 运行\n[PASS] io500  score 123.4\nverdict: PASS")
sys.exit(0)
