"""One command: rent an AWS cluster, run rocepool's on-site suite + the
official IO500 on it, bring the reports home, give the machines back.

    python3 tools/aws_cluster.py run --mode efa --nodes 10            # EFA (libfabric backend)
    python3 tools/aws_cluster.py run --mode rxe --nodes 3 --quick     # Soft-RoCE, the verbs backend as is
    python3 tools/aws_cluster.py down --run-id rp-20261003-1201       # clean up a kept / interrupted run
    python3 tools/aws_cluster.py list                                 # runs this tool still has resources for

Two ways to run on AWS (there is no RoCE NIC on EC2):

  rxe  Soft-RoCE: the kernel's rdma_rxe turns the ENA NIC into a RoCE v2
       device. rocepool's verbs transport runs unchanged; the CPU does what a
       RoCE NIC would, so it measures the software, not RDMA hardware.
  efa  Elastic Fabric Adapter: AWS's RDMA NIC (SRD, reliable datagrams,
       hardware RDMA read on Nitro v4+). rocepool's ofi transport backend
       (roce/native/ofi_transport.c over libfabric's efa provider) -- build.sh
       picks it by itself on a host with an EFA device.

What it does, all automatically:
  1. checks the aws CLI and credentials; region from --region / the CLI config
  2. Ubuntu 24.04 AMI (SSM public parameter, x86_64 or arm64 per type),
     an AZ that offers the type, the default VPC's subnet there
  3. a key pair, a security group (all traffic between members -- EFA needs
     it -- and SSH from this machine's public IP only), a cluster placement
     group; N instances, an EFA interface each in efa mode, 200 GiB gp3
  4. safety net: every instance powers itself off (= terminates) after
     --max-hours even if this tool dies
  5. per host: packages; rxe: rdma_rxe + `rdma link add rxe0`, MTU 9001;
     efa: the AWS EFA installer (kernel module, rdma-core, libfabric);
     root SSH between the hosts with a key made for this run
  6. ships this tree to the first host, runs `./run.sh --peers ...` there as
     root (preflight, build, transport checks, functional, bench, IO500 on
     all hosts), follows its progress, then copies the reports home to
     aws_runs/<run-id>/
  7. terminates everything and deletes what it created (--keep: leave it up)

State is written to aws_runs/<run-id>/state.json as resources are created,
so `down` can clean up after an interrupted run.
--aws / --ssh / --scp exist so the flow is tested with fakes
(tests/test_aws_cluster.py).
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import shlex
import subprocess
import sys
import tarfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNS = os.path.join(ROOT, "aws_runs")
UBUNTU_SSM = "/aws/service/canonical/ubuntu/server/24.04/stable/current/{arch}/hvm/ebs-gp3/ami-id"
DEFAULT_TYPE = {"efa": "c6in.32xlarge", "rxe": "c6in.16xlarge"}
EFA_INSTALLER = "https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz"
# what goes to the head node: the tree minus caches, results, VCS
EXCLUDE = {".git", ".bench_cache", "aws_runs", "__pycache__", "stress_reports", "mnt"}


def log(msg: str) -> None:
    print(f"[aws {time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Aws:
    def __init__(self, binary: str, region: str | None):
        self.bin = binary
        self.region = region

    def __call__(self, *args: str, check: bool = True, parse: bool = True):
        argv = [self.bin, *args, "--output", "json"] + (["--region", self.region] if self.region else [])
        r = subprocess.run(argv, capture_output=True, text=True, timeout=600)
        if check and r.returncode:
            raise RuntimeError(f"aws {' '.join(args[:2])}: {(r.stderr or r.stdout).strip()[-800:]}")
        if not parse:
            return r
        out = r.stdout.strip()
        return json.loads(out) if out else {}


class Run:
    def __init__(self, args):
        self.args = args
        self.run_id = args.run_id or time.strftime("rp-%Y%m%d-%H%M%S")
        self.dir = os.path.join(RUNS, self.run_id)
        os.makedirs(self.dir, exist_ok=True)
        self.state_file = os.path.join(self.dir, "state.json")
        self.state = json.load(open(self.state_file)) if os.path.exists(self.state_file) else {"run_id": self.run_id}
        region = args.region or self.state.get("region") or self._cli_region()
        self.aws = Aws(args.aws, region)
        self.state["region"] = region
        self.key_file = os.path.join(self.dir, "ssh_key")
        self.save()

    def _cli_region(self) -> str | None:
        if os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"):
            return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        try:
            r = subprocess.run([self.args.aws, "configure", "get", "region"], capture_output=True, text=True, timeout=30)
            return r.stdout.strip() or None
        except OSError:
            return None

    def save(self) -> None:
        with open(self.state_file + ".tmp", "w") as f:
            json.dump(self.state, f, indent=2)
        os.replace(self.state_file + ".tmp", self.state_file)

    # ---- ssh -----------------------------------------------------------
    def ssh_argv(self, ip: str, user: str = "root") -> list[str]:
        return [self.args.ssh, "-i", self.key_file, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                "-o", "LogLevel=ERROR", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=30", f"{user}@{ip}"]

    def ssh(self, ip: str, cmd: str, user: str = "root", timeout: int = 3600, check: bool = True):
        r = subprocess.run(self.ssh_argv(ip, user) + [cmd], capture_output=True, text=True, timeout=timeout)
        if check and r.returncode:
            raise RuntimeError(f"ssh {user}@{ip} `{cmd[:80]}`: rc={r.returncode} {(r.stdout + r.stderr)[-1500:]}")
        return r

    def scp(self, src: str, dst: str) -> None:
        r = subprocess.run([self.args.scp, "-q", "-i", self.key_file, "-o", "StrictHostKeyChecking=no",
                            "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR", src, dst],
                           capture_output=True, text=True, timeout=3600)
        if r.returncode:
            raise RuntimeError(f"scp {src} {dst}: {(r.stdout + r.stderr)[-800:]}")

    # ---- 1-3: resources -------------------------------------------------
    def preflight(self) -> None:
        ident = self.aws("sts", "get-caller-identity")
        log(f"account {ident.get('Account')} as {ident.get('Arn')}, region {self.aws.region}")
        if not self.aws.region:
            raise RuntimeError("no region: pass --region or `aws configure set region ...`")

    def instance_facts(self, itype: str) -> dict:
        r = self.aws("ec2", "describe-instance-types", "--instance-types", itype)
        t = r["InstanceTypes"][0]
        archs = t.get("ProcessorInfo", {}).get("SupportedArchitectures", ["x86_64"])
        return {"arch": "arm64" if "arm64" in archs and "x86_64" not in archs else "amd64",
                "efa": bool(t.get("NetworkInfo", {}).get("EfaSupported")),
                "vcpus": t.get("VCpuInfo", {}).get("DefaultVCpus"),
                "mem_mib": t.get("MemoryInfo", {}).get("SizeInMiB"),
                "network": t.get("NetworkInfo", {}).get("NetworkPerformance"),
                "nvme": t.get("InstanceStorageInfo", {}).get("TotalSizeInGB", 0)}

    def check_quota(self, itype: str, facts: dict, nodes: int) -> None:
        """The account's vCPU quota for on-demand standard instances (A, C, D,
        H, I, M, R, T, Z families; new accounts often have 32-64) must cover
        the run -- checked before anything is created."""
        fam = itype.split(".")[0]
        if not fam or fam[0] not in "acdhimrtz" or fam.startswith("hpc"):
            return
        need = (facts.get("vcpus") or 0) * nodes
        r = self.aws("service-quotas", "get-service-quota", "--service-code", "ec2", "--quota-code", "L-1216C47A",
                     check=False, parse=False)
        try:
            quota = float(json.loads(r.stdout)["Quota"]["Value"])
        except (ValueError, KeyError, TypeError):
            log("could not read the vCPU quota (service-quotas permission?): continuing")
            return
        if quota < need:
            raise RuntimeError(
                f"vCPU quota for on-demand standard instances is {quota:.0f}, this run needs {need} "
                f"({nodes} x {facts.get('vcpus')}). Ask for more: aws service-quotas request-service-quota-increase "
                f"--service-code ec2 --quota-code L-1216C47A --desired-value {need} --region {self.aws.region} "
                f"(or pick a smaller --type / fewer --nodes)")
        log(f"vCPU quota {quota:.0f} >= {need} needed (instances already running count against it too)")

    def my_ip(self) -> str:
        if self.args.ssh_cidr:
            return self.args.ssh_cidr
        with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as r:
            return r.read().decode().strip() + "/32"

    def create(self) -> None:
        a, st, itype = self.args, self.state, self.args.type or DEFAULT_TYPE[self.args.mode]
        facts = self.instance_facts(itype)
        st.update(mode=a.mode, type=itype, nodes=a.nodes, facts=facts)
        log(f"{a.nodes} x {itype}: {facts['vcpus']} vCPU, {facts['mem_mib'] // 1024 if facts['mem_mib'] else '?'} GiB, "
            f"{facts['network']}, NVMe {facts['nvme']} GB, EFA {'yes' if facts['efa'] else 'no'}")
        if a.mode == "efa" and not facts["efa"]:
            raise RuntimeError(f"{itype} has no EFA: pick an EFA type (c6in.32xlarge, i4i.32xlarge, hpc7a.96xlarge, ...) "
                               "or --mode rxe")
        self.check_quota(itype, facts, a.nodes)
        ami = self.aws("ssm", "get-parameters", "--names", UBUNTU_SSM.format(arch=facts["arch"]))
        st["ami"] = ami["Parameters"][0]["Value"]
        offers = self.aws("ec2", "describe-instance-type-offerings", "--location-type", "availability-zone",
                          "--filters", f"Name=instance-type,Values={itype}")
        zones = sorted(o["Location"] for o in offers.get("InstanceTypeOfferings", []))
        if a.az:
            zones = [z for z in zones if z == a.az]
        if not zones:
            raise RuntimeError(f"{itype} is not offered in {self.aws.region}{' ' + a.az if a.az else ''}")
        vpcs = self.aws("ec2", "describe-vpcs", "--filters", "Name=isDefault,Values=true")["Vpcs"]
        if not vpcs:
            raise RuntimeError("no default VPC in this region (create one: `aws ec2 create-default-vpc`)")
        st["vpc"] = vpcs[0]["VpcId"]
        subnets = self.aws("ec2", "describe-subnets", "--filters", f"Name=vpc-id,Values={st['vpc']}",
                           "Name=default-for-az,Values=true")["Subnets"]
        by_az = {s["AvailabilityZone"]: s["SubnetId"] for s in subnets}
        cand = [z for z in zones if z in by_az]
        if not cand:
            raise RuntimeError(f"the default VPC has no subnet in {zones}")
        st.update(az=cand[0], subnet=by_az[cand[0]])
        self.save()
        name = self.run_id
        # key pair
        if not st.get("key"):
            k = self.aws("ec2", "create-key-pair", "--key-name", name, "--key-type", "ed25519")
            with open(os.open(self.key_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
                f.write(k["KeyMaterial"].rstrip("\n") + "\n")
            st["key"] = name
            self.save()
        # security group: all traffic among members (EFA requires the
        # self-referencing in- and outbound rules), SSH from here only
        if not st.get("sg"):
            sg = self.aws("ec2", "create-security-group", "--group-name", name, "--vpc-id", st["vpc"],
                          "--description", f"rocepool run {name}")["GroupId"]
            st["sg"] = sg
            self.save()
            perms = [{"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": sg}]}]
            self.aws("ec2", "authorize-security-group-ingress", "--group-id", sg, "--ip-permissions", json.dumps(perms))
            self.aws("ec2", "authorize-security-group-egress", "--group-id", sg, "--ip-permissions", json.dumps(perms),
                     check=False)
            self.aws("ec2", "authorize-security-group-ingress", "--group-id", sg, "--protocol", "tcp", "--port", "22",
                     "--cidr", self.my_ip())
        if not st.get("pg"):
            self.aws("ec2", "create-placement-group", "--group-name", name, "--strategy", "cluster")
            st["pg"] = name
            self.save()
        # instances
        if not st.get("instances"):
            shutdown_min = int(a.max_hours * 60)
            user_data = ("#!/bin/bash\n"
                         f"# rocepool {name}: power off (= terminate) after {a.max_hours} h whatever happens\n"
                         f"shutdown -h +{shutdown_min} 'rocepool safety limit'\n")
            # an AZ out of capacity for the type: try the next one that offers it
            # (all instances in one AZ: the cluster placement group needs that);
            # rxe needs no placement group, so as a last resort it goes without one
            tries = [(z, True) for z in cand] + ([(z, False) for z in cand] if a.mode == "rxe" else [])
            r, why = None, []
            for zone, pg in tries:
                nic = {"DeviceIndex": 0, "SubnetId": by_az[zone], "Groups": [st["sg"]], "AssociatePublicIpAddress": True,
                       "DeleteOnTermination": True}
                if a.mode == "efa":
                    nic["InterfaceType"] = "efa"
                placement = {"AvailabilityZone": zone, **({"GroupName": st["pg"]} if pg else {})}
                try:
                    r = self.aws("ec2", "run-instances", "--image-id", st["ami"], "--instance-type", itype,
                                 "--count", str(a.nodes), "--key-name", st["key"],
                                 "--network-interfaces", json.dumps([nic]),
                                 "--placement", json.dumps(placement),
                                 "--instance-initiated-shutdown-behavior", "terminate",
                                 "--block-device-mappings", json.dumps([{"DeviceName": "/dev/sda1", "Ebs": {
                                     "VolumeSize": a.disk_gib, "VolumeType": "gp3", "DeleteOnTermination": True,
                                     "Throughput": a.disk_mbps, "Iops": a.disk_iops}}]),
                                 "--tag-specifications", json.dumps([{"ResourceType": "instance", "Tags": [
                                     {"Key": "Name", "Value": name}, {"Key": "rocepool-run", "Value": name}]}]),
                                 "--user-data", user_data)
                except RuntimeError as e:
                    if not any(k in str(e) for k in ("InsufficientInstanceCapacity", "Unsupported",
                                                      "InsufficientCapacity")):
                        raise
                    why.append(f"{zone}{'' if pg else ' (no placement group)'}")
                    log(f"no {itype} capacity in {zone}{'' if pg else ' without a placement group'}: trying the next")
                    continue
                st.update(az=zone, subnet=by_az[zone], placement_group_used=pg)
                break
            if r is None:
                raise RuntimeError(f"no capacity for {a.nodes} x {itype} in {self.aws.region} (tried {', '.join(why)}): "
                                   "try later, another --type or --region")
            st["instances"] = [i["InstanceId"] for i in r["Instances"]]
            self.save()
            log(f"launched {', '.join(st['instances'])}")
        self.aws("ec2", "wait", "instance-running", "--instance-ids", *st["instances"], parse=False)
        d = self.aws("ec2", "describe-instances", "--instance-ids", *st["instances"])
        hosts = {}
        for res in d["Reservations"]:
            for i in res["Instances"]:
                hosts[i["InstanceId"]] = {"public": i.get("PublicIpAddress"), "private": i.get("PrivateIpAddress")}
        st["hosts"] = [hosts[i] for i in st["instances"]]
        self.save()
        log("running: " + ", ".join(f"{h['public']} ({h['private']})" for h in st["hosts"]))

    # ---- 5: hosts --------------------------------------------------------
    def wait_ssh(self, ip: str) -> None:
        end = time.monotonic() + 600
        while True:
            r = subprocess.run(self.ssh_argv(ip, "ubuntu") + ["cloud-init status --wait >/dev/null 2>&1; echo up"],
                               capture_output=True, text=True, timeout=900)
            if r.returncode == 0 and "up" in r.stdout:
                return
            if time.monotonic() > end:
                raise RuntimeError(f"{ip}: no SSH after 10 min: {(r.stdout + r.stderr)[-300:]}")
            time.sleep(self.args.poll)

    def setup_script(self) -> str:
        mode = self.state["mode"]
        lines = [
            "set -euo pipefail",
            "export DEBIAN_FRONTEND=noninteractive",
            # root login with the run's key (host-to-host keys come after)
            "sudo install -d -m 700 /root/.ssh",
            "sudo cp ~/.ssh/authorized_keys /root/.ssh/authorized_keys",
            "printf 'Host *\\n  StrictHostKeyChecking no\\n  UserKnownHostsFile /dev/null\\n  LogLevel ERROR\\n' "
            "| sudo tee /root/.ssh/config >/dev/null",
            "sudo chmod 600 /root/.ssh/authorized_keys /root/.ssh/config",
            "sudo apt-get update -q >/dev/null",
            "sudo apt-get install -y -q python3 gcc g++ make git cmake curl pkg-config autoconf automake libtool "
            "fuse3 iproute2 rdma-core ibverbs-utils libibverbs-dev librdmacm-dev nvme-cli >/dev/null",
        ]
        if mode == "rxe":
            lines += [
                "sudo apt-get install -y -q linux-modules-extra-$(uname -r) >/dev/null || true",
                "sudo modprobe rdma_rxe",
                "dev=$(ip -4 -o route get 1.1.1.1 | sed -n 's/.* dev \\([^ ]*\\).*/\\1/p')",
                "sudo ip link set dev $dev mtu 9001",
                "sudo rdma link show | grep -q rxe0 || sudo rdma link add rxe0 type rxe netdev $dev",
                "ls /sys/class/infiniband/ | grep -q rxe0",
            ]
        else:
            lines += [
                "cd /tmp && curl -sSfO " + EFA_INSTALLER,
                "tar xzf aws-efa-installer-latest.tar.gz && cd aws-efa-installer",
                "sudo ./efa_installer.sh -y >/tmp/efa_install.log 2>&1 || (tail -40 /tmp/efa_install.log; exit 1)",
                "ls -d /sys/class/infiniband/efa* >/dev/null 2>&1 || sudo modprobe efa",
                "/opt/amazon/efa/bin/fi_info -p efa -t FI_EP_RDM | grep -q 'provider: efa'",
            ]
        lines.append("echo SETUP_OK")
        return "\n".join(lines)

    def setup_hosts(self) -> None:
        st = self.state
        script = self.setup_script()

        def one(h):
            self.wait_ssh(h["public"])
            r = self.ssh(h["public"], "bash -s <<'ROCEPOOL_SETUP'\n" + script + "\nROCEPOOL_SETUP", user="ubuntu",
                         timeout=2400, check=False)
            if "SETUP_OK" not in r.stdout:
                raise RuntimeError(f"{h['public']}: setup failed: {(r.stdout + r.stderr)[-1500:]}")
            return h["public"]
        with cf.ThreadPoolExecutor(len(st["hosts"])) as ex:
            for ip in ex.map(one, st["hosts"]):
                log(f"{ip}: ready ({st['mode']})")
        # host-to-host root SSH (the head drives the peers): a key made on the
        # head node, its public half appended on every host
        head = st["hosts"][0]["public"]
        r = self.ssh(head, "test -f /root/.ssh/id_ed25519 || ssh-keygen -q -t ed25519 -N '' -C rocepool-cluster "
                           "-f /root/.ssh/id_ed25519; cat /root/.ssh/id_ed25519.pub")
        pub = r.stdout.strip().splitlines()[-1]
        add = f"grep -qF {shlex.quote(pub)} /root/.ssh/authorized_keys || echo {shlex.quote(pub)} >> /root/.ssh/authorized_keys"
        with cf.ThreadPoolExecutor(len(st["hosts"])) as ex:
            list(ex.map(lambda h: self.ssh(h["public"], add), st["hosts"]))
        st["setup"] = True
        self.save()

    # ---- 6: run -----------------------------------------------------------
    def pack(self) -> str:
        out = os.path.join(self.dir, "rocepool.tar.gz")

        def keep(ti):
            parts = ti.name.split("/")
            return None if any(p in EXCLUDE for p in parts[1:]) or ti.name.endswith((".pyc", ".log")) else ti
        with tarfile.open(out, "w:gz") as t:
            t.add(ROOT, arcname="rocepool", filter=keep)
        return out

    def run_bench(self) -> int:
        st, a = self.state, self.args
        head = st["hosts"][0]["public"]
        self.scp(self.pack(), f"root@{head}:/root/rocepool.tar.gz")
        self.ssh(head, "rm -rf /root/rocepool && tar xzf /root/rocepool.tar.gz -C /root && rm /root/rocepool.tar.gz")
        peers = ",".join(f"root@{h['private']}={h['private']}" for h in st["hosts"][1:])
        flags = (["--peers", peers] if peers else []) + (["--quick"] if a.quick else []) + \
            ([] if a.self_test else ["--skip-tests"]) + ["--ip", st["hosts"][0]["private"]]
        if a.command:  # a diagnostic command instead of run.sh: HEAD_IP / PEER_IPS tell it the hosts
            ips = " ".join(h["private"] for h in st["hosts"][1:])
            cmd = (f"cd /root/rocepool && export HEAD_IP={st['hosts'][0]['private']} PEER_IPS={shlex.quote(ips)}; "
                   f"( {a.command} ) > aws_run.out 2>&1; echo $? > aws_run.rc")
        else:
            cmd = "cd /root/rocepool && nohup ./run.sh " + " ".join(shlex.quote(f) for f in flags) + \
                " > aws_run.out 2>&1; echo $? > aws_run.rc"
        self.ssh(head, f"setsid bash -c {shlex.quote(cmd)} >/dev/null 2>&1 < /dev/null &")
        log(f"{'command' if a.command else 'run.sh'} started on {head} ({len(st['hosts'])} hosts); following its progress")
        seen, end = 0, time.monotonic() + a.max_hours * 3600
        rc = None
        while time.monotonic() < end:
            r = self.ssh(head, "cd /root/rocepool; wc -l < aws_run.out 2>/dev/null || echo 0; "
                               "cat aws_run.rc 2>/dev/null || echo running", check=False, timeout=120)
            lines = r.stdout.split()
            n = int(lines[0]) if lines and lines[0].isdigit() else 0
            if n > seen:
                new = self.ssh(head, f"sed -n '{seen + 1},{n}p' /root/rocepool/aws_run.out", check=False, timeout=120)
                for l in new.stdout.splitlines():
                    if l.startswith(("==", "[", "verdict", "   ")):
                        print("  | " + l, flush=True)
                seen = n
            if len(lines) > 1 and lines[-1] != "running":
                rc = int(lines[-1]) if lines[-1].lstrip("-").isdigit() else 1
                break
            time.sleep(a.poll)
        self.fetch()
        return 1 if rc is None else rc

    def fetch(self) -> None:
        head = self.state["hosts"][0]["public"]
        files = ["onsite_report.json", "onsite_io500.json", "onsite_io500_valid.json", "run.log", "aws_run.out",
                 "onsite_crossover.json", "onsite_stress.json", "onsite_ior.json", "onsite_ior_hard.json",
                 "onsite_kv.json", "onsite_gds.json", "onsite_kvcache.json", "selftest.log"]
        got = []
        for f in files:
            try:
                self.scp(f"root@{head}:/root/rocepool/{f}", os.path.join(self.dir, f))
                got.append(f)
            except RuntimeError:
                pass
        # every host's kernel log tail (OOM kills, NIC errors) and memory
        for i, h in enumerate(self.state.get("hosts", [])):
            try:
                r = self.ssh(h["public"], "dmesg -T 2>/dev/null | tail -200; echo; free -m; echo; "
                                          "ps -eo pid,rss,vsz,stat,etime,comm --sort=-rss | head -15",
                             check=False, timeout=120)
                with open(os.path.join(self.dir, f"host{i}_kernel.txt"), "w") as f:
                    f.write(r.stdout)
                got.append(f"host{i}_kernel.txt")
            except (RuntimeError, OSError, subprocess.SubprocessError):
                pass
        # the servers' and clients' logs of the last IO500 (the head's, and the
        # peers' copied there), for diagnosis: the reports name them
        try:
            self.ssh(head, "cd /root/rocepool/.bench_cache/io500_run 2>/dev/null && "
                           "find . -maxdepth 2 \\( -name '*.log' -o -name '*.out' -o -name '*.err' \\) -size -50M -print0 | "
                           "tar czf /root/rocepool/io500_logs.tar.gz --null -T - 2>/dev/null; true", check=False)
            self.scp(f"root@{head}:/root/rocepool/io500_logs.tar.gz", os.path.join(self.dir, "io500_logs.tar.gz"))
            got.append("io500_logs.tar.gz")
        except RuntimeError:
            pass
        log(f"reports in {os.path.relpath(self.dir, ROOT)}/: {', '.join(got) or 'none'}")
        valid = os.path.join(self.dir, "onsite_io500_valid.json")
        if os.path.exists(valid):
            s = json.load(open(valid)).get("score", {})
            log(f"IO500 score {s.get('score')}  bw {s.get('bw_gib_s')} GiB/s  md {s.get('md_kiops')} kIOPS "
                f"({self.state['nodes']} x {self.state['type']}, {self.state['mode']})")

    # ---- 7: teardown ---------------------------------------------------------
    def down(self) -> None:
        st = self.state
        if st.get("instances"):
            self.aws("ec2", "terminate-instances", "--instance-ids", *st["instances"], check=False)
            log(f"terminating {', '.join(st['instances'])}")
            self.aws("ec2", "wait", "instance-terminated", "--instance-ids", *st["instances"], parse=False, check=False)
            st["instances"] = []
            self.save()
        if st.get("pg"):
            self.aws("ec2", "delete-placement-group", "--group-name", st["pg"], check=False)
            st["pg"] = None
        if st.get("sg"):
            for _ in range(10):  # ENIs of terminated instances linger a little
                r = self.aws("ec2", "delete-security-group", "--group-id", st["sg"], check=False, parse=False)
                if r.returncode == 0:
                    break
                time.sleep(self.args.poll)
            st["sg"] = None
        if st.get("key"):
            self.aws("ec2", "delete-key-pair", "--key-name", st["key"], check=False)
            st["key"] = None
        st["down"] = True
        self.save()
        log("all resources released")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="create, set up, run, fetch, tear down")
    r.add_argument("--mode", choices=("efa", "rxe"), default="efa")
    r.add_argument("--nodes", type=int, default=10)
    r.add_argument("--type", help=f"instance type (default {DEFAULT_TYPE['efa']} for efa, {DEFAULT_TYPE['rxe']} for rxe)")
    r.add_argument("--quick", action="store_true", help="30 s IO500 instead of the 300 s valid run")
    r.add_argument("--self-test", action="store_true", help="also run the unit suite on the head node (~3 min)")
    r.add_argument("--keep", action="store_true", help="leave the cluster up afterwards (`down` releases it)")
    r.add_argument("--command", help="run this shell command on the head (in the tree; HEAD_IP, PEER_IPS set) instead of "
                                     "run.sh -- for diagnosis; its output comes back as aws_run.out")
    r.add_argument("--max-hours", type=float, default=6.0, help="instances power off (terminate) after this")
    r.add_argument("--disk-gib", type=int, default=200)
    # the OSTs' capacity tier lives on the system disk when the type has no
    # local NVMe: gp3's default 125 MB/s bounded IO500's bandwidth at ~0.25
    # GiB/s on 2 x c5n.9xlarge (2026-10-24); its maximum costs cents an hour
    r.add_argument("--disk-mbps", type=int, default=1000, help="gp3 throughput, MB/s (125..1000)")
    r.add_argument("--disk-iops", type=int, default=16000, help="gp3 IOPS (3000..16000)")
    r.add_argument("--az", help="availability zone (default: the first that offers the type)")
    r.add_argument("--ssh-cidr", help="allow SSH from this CIDR (default: this machine's public IP /32)")
    d = sub.add_parser("down", help="release everything a run created")
    d.add_argument("--run-id", required=True)
    sub.add_parser("list", help="runs with resources still recorded")
    for p in (r, d):
        p.add_argument("--region")
        p.add_argument("--aws", default="aws", help=argparse.SUPPRESS)
        p.add_argument("--ssh", default="ssh", help=argparse.SUPPRESS)
        p.add_argument("--scp", default="scp", help=argparse.SUPPRESS)
        p.add_argument("--poll", type=float, default=15.0, help=argparse.SUPPRESS)
    r.add_argument("--run-id", help=argparse.SUPPRESS)
    args = ap.parse_args()

    if args.cmd == "list":
        for rid in sorted(os.listdir(RUNS)) if os.path.isdir(RUNS) else []:
            try:
                st = json.load(open(os.path.join(RUNS, rid, "state.json")))
            except (OSError, ValueError):
                continue
            if not st.get("down"):
                print(f"{rid}: {st.get('nodes')} x {st.get('type')} {st.get('mode')} in {st.get('region')} "
                      f"instances={st.get('instances')}")
        return 0
    if args.cmd == "down":
        if not os.path.exists(os.path.join(RUNS, args.run_id, "state.json")):
            print(f"no such run: {args.run_id}", file=sys.stderr)
            return 2
        Run(args).down()
        return 0

    run = Run(args)
    rc = 1
    try:
        run.preflight()
        run.create()
        run.setup_hosts()
        rc = run.run_bench()
        log(f"run.sh exit code {rc}")
    except KeyboardInterrupt:
        log("interrupted")
    except Exception as exc:  # noqa: BLE001 -- report, then release the machines
        log(f"FAILED: {exc}")
        if run.state.get("hosts"):
            try:
                run.fetch()
            except Exception:  # noqa: BLE001
                pass
    finally:
        if args.keep:
            log(f"--keep: cluster left up; release it with: python3 tools/aws_cluster.py down --run-id {run.run_id}")
            for h in run.state.get("hosts", []):
                log(f"  ssh -i {os.path.relpath(run.key_file, ROOT)} root@{h['public']}")
        else:
            run.down()
    return rc


if __name__ == "__main__":
    sys.exit(main())
