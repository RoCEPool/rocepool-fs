"""One command: rent an AWS cluster, run rocepool's on-site suite + the
official IO500 on it, bring the reports home, give the machines back.

    python3 tools/aws_cluster.py run --mode efa --nodes 10            # EFA (libfabric backend)
    python3 tools/aws_cluster.py run --mode rxe --nodes 3 --quick     # Soft-RoCE, the verbs backend as is
    python3 tools/aws_cluster.py run --mode gpu --nodes 1 --type g6e.2xlarge --command ...   # one GPU host
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
import io
import json
import os
import re
import shlex
import subprocess
import sys
import tarfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNS = os.path.join(ROOT, "aws_runs")
UBUNTU_SSM = "/aws/service/canonical/ubuntu/server/24.04/stable/current/{arch}/hvm/ebs-gp3/ami-id"
# gpu mode: AWS's Deep Learning Base AMI -- NVIDIA driver, Docker and the NVIDIA
# container toolkit already in it -- newest Ubuntu first
DLAMI_SSM = ["/aws/service/deeplearning/ami/{arch}/base-oss-nvidia-driver-gpu-ubuntu-24.04/latest/ami-id",
             "/aws/service/deeplearning/ami/{arch}/base-oss-nvidia-driver-gpu-ubuntu-22.04/latest/ami-id"]
DEFAULT_TYPE = {"efa": "c6in.32xlarge", "rxe": "c6in.16xlarge", "gpu": "g6e.2xlarge"}
EFA_INSTALLER = "https://efa-installer.amazonaws.com/aws-efa-installer-latest.tar.gz"
# what goes to the head node: the tree minus caches, results, VCS
EXCLUDE = {".git", ".bench_cache", "aws_runs", "__pycache__", "stress_reports", "mnt"}


def log(msg: str) -> None:
    print(f"[aws {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def quota_for(itype: str) -> tuple:
    """(Service Quotas code, what it covers) of the on-demand vCPU quota an
    instance type counts against; (None, None) for families not checked"""
    fam = itype.split(".")[0]
    if fam.startswith("hpc"):
        return "L-F7808C92", "on-demand HPC instances"
    if fam.startswith(("g", "vt")):  # g4dn, g5, g6, g6e, g7e, gr6, vt1: GPU / video
        return "L-DB2E81BA", "on-demand G and VT instances"
    if fam.startswith("p"):  # p4d, p5, ...
        return "L-417A185B", "on-demand P instances"
    if fam and fam[0] in "acdhimrtz":
        return "L-1216C47A", "on-demand standard instances"
    return None, None


def network_interfaces(mode: str, facts: dict, subnet: str, sg: str) -> list:
    """The instance's interfaces. Card 0: the primary (public IP, SSH; an EFA
    in efa mode). efa mode on a type with several network cards and EFA
    interfaces (hpc6id.32xlarge: 2): an EFA-only interface (RDMA, no IP) on
    each further card, so the transport gets the whole bandwidth -- one rail
    per card (ROCE_OFI_RAILS). AWS assigns no public IP at launch to an
    instance with several interfaces ("associatePublicIPAddress cannot be
    specified when launching with multiple network interfaces", found by
    the probe's dry run): such instances get an Elastic IP on the primary
    after launch (Run.attach_public_ips)."""
    nics = [{"NetworkCardIndex": 0, "DeviceIndex": 0, "SubnetId": subnet, "Groups": [sg],
             "AssociatePublicIpAddress": True, "DeleteOnTermination": True}]
    if mode != "efa":
        return nics
    nics[0]["InterfaceType"] = "efa"
    rails = max(1, min(int(facts.get("network_cards") or 1), int(facts.get("efa_max") or 1)))
    for card in range(1, rails):
        nics.append({"NetworkCardIndex": card, "DeviceIndex": 0, "SubnetId": subnet, "Groups": [sg],
                     "InterfaceType": "efa-only", "DeleteOnTermination": True})
    if len(nics) > 1:
        del nics[0]["AssociatePublicIpAddress"]
    return nics


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
                "nvme": t.get("InstanceStorageInfo", {}).get("TotalSizeInGB", 0),
                # network cards, and how many EFA interfaces the type takes:
                # hpc6id.32xlarge has 2 cards and reaches 200 Gb/s only with
                # an interface on each (one card: up to 170 Gb/s)
                "network_cards": t.get("NetworkInfo", {}).get("MaximumNetworkCards", 1),
                "efa_max": (t.get("NetworkInfo", {}).get("EfaInfo") or {}).get("MaximumEfaInterfaces", 1)}

    def check_quota(self, itype: str, facts: dict, nodes: int) -> None:
        """The account's vCPU quota for the type's family must cover the run
        -- checked before anything is created: on-demand standard instances
        (A, C, D, H, I, M, R, T, Z; new accounts often have 32-64) or HPC
        instances (hpc6id, hpc7a, ...: a quota of its own, 0 until asked)."""
        code, what = quota_for(itype)
        if not code:
            return
        need = (facts.get("vcpus") or 0) * nodes
        r = self.aws("service-quotas", "get-service-quota", "--service-code", "ec2", "--quota-code", code,
                     check=False, parse=False)
        try:
            quota = float(json.loads(r.stdout)["Quota"]["Value"])
        except (ValueError, KeyError, TypeError):
            log("could not read the vCPU quota (service-quotas permission?): continuing")
            return
        if quota < need:
            raise RuntimeError(
                f"vCPU quota for {what} is {quota:.0f}, this run needs {need} "
                f"({nodes} x {facts.get('vcpus')}). Ask for more: aws service-quotas request-service-quota-increase "
                f"--service-code ec2 --quota-code {code} --desired-value {need} --region {self.aws.region} "
                f"(or pick a smaller --type / fewer --nodes)")
        log(f"vCPU quota {quota:.0f} >= {need} needed (instances already running count against it too)")

    def check_eips(self, nodes: int) -> None:
        """Instances with several interfaces need an Elastic IP each: the
        region's Elastic IP quota (L-0263D0A3, 5 by default) less the
        addresses already allocated must cover them -- before anything is
        created."""
        r = self.aws("service-quotas", "get-service-quota", "--service-code", "ec2", "--quota-code", "L-0263D0A3",
                     check=False, parse=False)
        try:
            quota = float(json.loads(r.stdout)["Quota"]["Value"])
        except (ValueError, KeyError, TypeError):
            log("could not read the Elastic IP quota: continuing")
            return
        used = len(self.aws("ec2", "describe-addresses").get("Addresses", []))
        if quota - used < nodes:
            raise RuntimeError(
                f"{nodes} instances with several network interfaces need {nodes} Elastic IPs; the quota is "
                f"{quota:.0f} with {used} allocated. Ask for more: aws service-quotas request-service-quota-increase "
                f"--service-code ec2 --quota-code L-0263D0A3 --desired-value {used + nodes} --region {self.aws.region}")
        log(f"Elastic IPs: quota {quota:.0f}, {used} allocated, {nodes} needed")

    def attach_public_ips(self) -> None:
        """An Elastic IP on each instance's primary interface (card 0, device
        0) -- recorded before it is associated, so down() releases it
        whatever happens after."""
        st = self.state
        d = self.aws("ec2", "describe-instances", "--instance-ids", *st["instances"])
        eni = {}
        for res in d["Reservations"]:
            for i in res["Instances"]:
                for n in i.get("NetworkInterfaces", []):
                    att = n.get("Attachment", {})
                    if att.get("DeviceIndex", 0) == 0 and att.get("NetworkCardIndex", 0) == 0:
                        eni[i["InstanceId"]] = n["NetworkInterfaceId"]
        st.setdefault("eips", {})
        for iid in st["instances"]:
            if iid in st["eips"]:
                continue
            a = self.aws("ec2", "allocate-address", "--domain", "vpc", "--tag-specifications", json.dumps([{
                "ResourceType": "elastic-ip", "Tags": [{"Key": "rocepool-run", "Value": self.run_id}]}]))
            st["eips"][iid] = a["AllocationId"]
            self.save()
            if iid not in eni:
                raise RuntimeError(f"{iid}: no primary network interface found to give a public IP")
            self.aws("ec2", "associate-address", "--allocation-id", a["AllocationId"], "--network-interface-id", eni[iid])
        log(f"Elastic IPs on {len(st['eips'])} instances")

    def my_ip(self) -> str:
        if self.args.ssh_cidr:
            return self.args.ssh_cidr
        with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10) as r:
            return r.read().decode().strip() + "/32"

    def dlami(self, facts: dict) -> tuple:
        """(AMI id, SSM name) of the Deep Learning Base AMI for the type's arch."""
        # Canonical's parameters say amd64, AWS's Deep Learning ones x86_64
        names = [n.format(arch={"amd64": "x86_64"}.get(facts["arch"], facts["arch"])) for n in DLAMI_SSM]
        got = {p["Name"]: p["Value"] for p in self.aws("ssm", "get-parameters", "--names", *names).get("Parameters", [])}
        name = next((n for n in names if n in got), None)
        if not name:
            raise RuntimeError(f"no Deep Learning Base AMI in {self.aws.region} ({', '.join(names)})")
        log(f"AMI: {name.split('/')[-3]} ({got[name]})")
        return got[name], name

    def launch(self, itype: str, count: int, ami: str, nics: list, placement: dict, user_data: str, role: str):
        """run-instances for one group; None when the zone has no capacity for it."""
        a, name = self.args, self.run_id
        try:
            return self.aws("ec2", "run-instances", "--image-id", ami, "--instance-type", itype,
                            "--count", str(count), "--key-name", self.state["key"],
                            "--network-interfaces", json.dumps(nics),
                            "--placement", json.dumps(placement),
                            "--instance-initiated-shutdown-behavior", "terminate",
                            "--block-device-mappings", json.dumps([{"DeviceName": "/dev/sda1", "Ebs": {
                                "VolumeSize": a.disk_gib, "VolumeType": "gp3", "DeleteOnTermination": True,
                                "Throughput": a.disk_mbps, "Iops": a.disk_iops}}]),
                            "--tag-specifications", json.dumps([{"ResourceType": "instance", "Tags": [
                                {"Key": "Name", "Value": name}, {"Key": "rocepool-run", "Value": name},
                                {"Key": "rocepool-role", "Value": role}]}]),
                            "--user-data", user_data)
        except RuntimeError as e:
            if not any(k in str(e) for k in ("InsufficientInstanceCapacity", "Unsupported", "InsufficientCapacity")):
                raise
            return None

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
        rails = len(network_interfaces(a.mode, facts, "", ""))
        if rails > 1:
            log(f"{itype}: {facts['network_cards']} network cards -> {rails} EFA interfaces per instance "
                f"(one transport rail each)")
            self.check_eips(a.nodes)
        if a.mode == "gpu":
            st["ami"], st["ami_name"] = self.dlami(facts)
        else:
            ami = self.aws("ssm", "get-parameters", "--names", UBUNTU_SSM.format(arch=facts["arch"]))
            st["ami"] = ami["Parameters"][0]["Value"]
        gpu = None
        if a.gpu_nodes:
            # GPU hosts joined to the EFA cluster (the storage hosts): same AZ,
            # same placement group, the Deep Learning AMI, an EFA interface
            if a.mode != "efa":
                raise RuntimeError("--gpu-nodes joins GPU hosts to an EFA cluster: use --mode efa")
            if not a.gpu_type:
                raise RuntimeError("--gpu-nodes needs --gpu-type")
            gfacts = self.instance_facts(a.gpu_type)
            log(f"{a.gpu_nodes} x {a.gpu_type} (GPU hosts): {gfacts['vcpus']} vCPU, "
                f"{gfacts['mem_mib'] // 1024 if gfacts['mem_mib'] else '?'} GiB, {gfacts['network']}, "
                f"NVMe {gfacts['nvme']} GB, EFA {'yes' if gfacts['efa'] else 'no'}")
            if not gfacts["efa"]:
                raise RuntimeError(f"{a.gpu_type} has no EFA: GPU hosts of an EFA cluster need one "
                                   "(g6e.8xlarge and larger, p4d, p5, ...)")
            self.check_quota(a.gpu_type, gfacts, a.gpu_nodes)
            gami, gname = self.dlami(gfacts)
            gpu = {"type": a.gpu_type, "nodes": a.gpu_nodes, "facts": gfacts, "ami": gami, "ami_name": gname}
            st["gpu"] = gpu
        offers = self.aws("ec2", "describe-instance-type-offerings", "--location-type", "availability-zone",
                          "--filters", f"Name=instance-type,Values={itype}")
        zones = sorted(o["Location"] for o in offers.get("InstanceTypeOfferings", []))
        if gpu:
            g_offers = self.aws("ec2", "describe-instance-type-offerings", "--location-type", "availability-zone",
                                "--filters", f"Name=instance-type,Values={gpu['type']}")
            g_zones = {o["Location"] for o in g_offers.get("InstanceTypeOfferings", [])}
            zones = [z for z in zones if z in g_zones]
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
            # rxe needs no placement group, so as a last resort it goes without one.
            # With GPU hosts: the GPU group first (its capacity is the scarcer),
            # then the storage hosts in the same zone and placement group; a
            # zone that cannot take both is rolled back before the next is tried.
            groups = ([("gpu", gpu["type"], gpu["nodes"], gpu["ami"], gpu["facts"])] if gpu else []) + \
                [("storage", itype, a.nodes, st["ami"], facts)]
            tries = [(z, True) for z in cand] + ([(z, False) for z in cand] if a.mode in ("rxe", "gpu") else [])
            why, done = [], False
            for zone, pg in tries:
                placement = {"AvailabilityZone": zone, **({"GroupName": st["pg"]} if pg else {})}
                got = {}
                for role, t, count, ami, f in groups:
                    nics = network_interfaces(a.mode, f, by_az[zone], st["sg"])
                    r = self.launch(t, count, ami, nics, placement, user_data, role)
                    if r is None:
                        why.append(f"{t} in {zone}{'' if pg else ' (no placement group)'}")
                        log(f"no {t} capacity in {zone}{'' if pg else ' without a placement group'}: trying the next")
                        break
                    got[role] = [i["InstanceId"] for i in r["Instances"]]
                    # recorded at once: whatever happens next, down() releases them
                    st.setdefault("instances", [])
                    st["instances"] = st["instances"] + got[role]
                    self.save()
                else:
                    st.update(az=zone, subnet=by_az[zone], placement_group_used=pg,
                              roles={iid: role for role, ids in got.items() for iid in ids})
                    done = True
                    break
                rollback = [iid for ids in got.values() for iid in ids]
                if rollback:
                    log(f"releasing {', '.join(rollback)}: the rest does not fit in {zone}")
                    self.aws("ec2", "terminate-instances", "--instance-ids", *rollback, check=False)
                    self.aws("ec2", "wait", "instance-terminated", "--instance-ids", *rollback, parse=False, check=False)
                    st["instances"] = [i for i in st["instances"] if i not in rollback]
                    self.save()
            if not done:
                what = " + ".join(f"{count} x {t}" for _, t, count, _, _ in groups)
                raise RuntimeError(f"no capacity for {what} in {self.aws.region} (tried {', '.join(why)}): "
                                   "try later, another --type or --region")
            log(f"launched {', '.join(st['instances'])}")
        self.aws("ec2", "wait", "instance-running", "--instance-ids", *st["instances"], parse=False)
        if rails > 1:
            self.attach_public_ips()
        d = self.aws("ec2", "describe-instances", "--instance-ids", *st["instances"])
        hosts = {}
        for res in d["Reservations"]:
            for i in res["Instances"]:
                hosts[i["InstanceId"]] = {"public": i.get("PublicIpAddress"), "private": i.get("PrivateIpAddress")}
        roles = st.get("roles") or {}
        st["hosts"] = [hosts[i] for i in st["instances"] if roles.get(i, "storage") == "storage"]
        st["gpu_hosts"] = [hosts[i] for i in st["instances"] if roles.get(i) == "gpu"]
        self.save()
        log("running: " + ", ".join(f"{h['public']} ({h['private']})" for h in st["hosts"]) +
            ("; GPU: " + ", ".join(f"{h['public']} ({h['private']})" for h in st["gpu_hosts"]) if st["gpu_hosts"] else ""))

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

    def setup_script(self, mode: str | None = None, facts: dict | None = None, efa: bool = False) -> str:
        mode = mode or self.state["mode"]
        facts = self.state.get("facts") if facts is None else facts
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
        elif mode == "gpu":
            # the AMI has the driver, Docker and the NVIDIA container toolkit; no
            # RDMA set up here. The AMI may already have the instance store at
            # /opt/dlami/nvme: it is used as /nvmedata, never formatted again.
            lines += [
                "nvidia-smi -L",
                "sudo docker info --format '{{json .Runtimes}}' | grep -q nvidia || "
                "(sudo nvidia-ctk runtime configure --runtime=docker >/dev/null && sudo systemctl restart docker)",
                "sudo docker info --format '{{json .Runtimes}}' | grep -q nvidia",
                "if mountpoint -q /opt/dlami/nvme && ! mountpoint -q /nvmedata; then "
                "sudo mkdir -p /nvmedata && sudo mount --bind /opt/dlami/nvme /nvmedata; fi",
            ]
            if efa:
                # a GPU host of an EFA cluster: the Deep Learning AMI ships the EFA
                # software; installed here only when it does not
                lines += [
                    "if ! /opt/amazon/efa/bin/fi_info -p efa -t FI_EP_RDM 2>/dev/null | grep -q 'provider: efa'; then "
                    "cd /tmp && curl -sSfO " + EFA_INSTALLER + " && tar xzf aws-efa-installer-latest.tar.gz && "
                    "cd aws-efa-installer && (sudo ./efa_installer.sh -y >/tmp/efa_install.log 2>&1 || "
                    "(tail -40 /tmp/efa_install.log; exit 1)); fi",
                    "/opt/amazon/efa/bin/fi_info -p efa -t FI_EP_RDM | grep -q 'provider: efa'",
                ]
        else:
            lines += [
                "cd /tmp && curl -sSfO " + EFA_INSTALLER,
                "tar xzf aws-efa-installer-latest.tar.gz && cd aws-efa-installer",
                "sudo ./efa_installer.sh -y >/tmp/efa_install.log 2>&1 || (tail -40 /tmp/efa_install.log; exit 1)",
                "ls -d /sys/class/infiniband/efa* >/dev/null 2>&1 || sudo modprobe efa",
                "/opt/amazon/efa/bin/fi_info -p efa -t FI_EP_RDM | grep -q 'provider: efa'",
            ]
        if (facts or {}).get("nvme"):
            # instance-store NVMe (i3en, i4i, ...) comes blank: one RAID 0 of
            # all of them, XFS, at /nvmedata -- tools/nvme_preflight.py then
            # puts the OSTs' tier there (an NVMe-backed file system). Only the
            # instance store, never the EBS system disk (its model differs).
            lines += [
                "sudo apt-get install -y -q mdadm xfsprogs >/dev/null",
                "devs=$(for d in /dev/nvme*n1; do m=$(cat /sys/block/$(basename $d)/device/model 2>/dev/null); "
                "case \"$m\" in *'Instance Storage'*) findmnt -rn -S $d >/dev/null || echo $d;; esac; done)",
                "n=$(echo $devs | wc -w)",
                "if [ $n -gt 0 ] && ! mountpoint -q /nvmedata; then "
                "if [ $n -gt 1 ]; then sudo mdadm --create /dev/md0 --run --level=0 --raid-devices=$n $devs >/dev/null "
                "&& dev=/dev/md0; else dev=$devs; fi; "
                "sudo mkfs.xfs -f -q $dev && sudo mkdir -p /nvmedata && sudo mount -o noatime $dev /nvmedata; fi",
                "if [ $n -gt 0 ]; then mountpoint -q /nvmedata && echo \"local NVMe: $n device(s) at /nvmedata\"; fi",
            ]
        lines.append("echo SETUP_OK")
        return "\n".join(lines)

    def setup_hosts(self) -> None:
        st = self.state
        script = self.setup_script()

        jobs = [(h, script, st["mode"]) for h in st["hosts"]]
        if st.get("gpu_hosts"):
            g = st["gpu"]
            gscript = self.setup_script("gpu", g["facts"], efa=True)
            jobs += [(h, gscript, f"gpu {g['type']}, EFA") for h in st["gpu_hosts"]]

        def one(job):
            h, text, what = job
            self.wait_ssh(h["public"])
            r = self.ssh(h["public"], "bash -s <<'ROCEPOOL_SETUP'\n" + text + "\nROCEPOOL_SETUP", user="ubuntu",
                         timeout=2400, check=False)
            if "SETUP_OK" not in r.stdout:
                raise RuntimeError(f"{h['public']}: setup failed: {(r.stdout + r.stderr)[-1500:]}")
            return h["public"], what
        with cf.ThreadPoolExecutor(len(jobs)) as ex:
            for ip, what in ex.map(one, jobs):
                log(f"{ip}: ready ({what})")
        # host-to-host root SSH (the head drives the peers): a key made on the
        # head node, its public half appended on every host
        head = st["hosts"][0]["public"]
        r = self.ssh(head, "test -f /root/.ssh/id_ed25519 || ssh-keygen -q -t ed25519 -N '' -C rocepool-cluster "
                           "-f /root/.ssh/id_ed25519; cat /root/.ssh/id_ed25519.pub")
        pub = r.stdout.strip().splitlines()[-1]
        add = f"grep -qF {shlex.quote(pub)} /root/.ssh/authorized_keys || echo {shlex.quote(pub)} >> /root/.ssh/authorized_keys"
        every = st["hosts"] + (st.get("gpu_hosts") or [])
        with cf.ThreadPoolExecutor(len(every)) as ex:
            list(ex.map(lambda h: self.ssh(h["public"], add), every))
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
            # the commit the hosts run (no .git there): the IO500 submission
            # names it (io500_run.submission_bundle)
            r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True)
            dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT,
                                   capture_output=True, text=True).stdout.strip()
            if r.returncode == 0:
                data = (r.stdout.strip() + ("-dirty" if dirty else "") + "\n").encode()
                ti = tarfile.TarInfo("rocepool/COMMIT")
                ti.size, ti.mtime = len(data), int(time.time())
                t.addfile(ti, io.BytesIO(data))
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
            gips = " ".join(h["private"] for h in st.get("gpu_hosts") or [])
            cmd = (f"cd /root/rocepool && export HEAD_IP={st['hosts'][0]['private']} PEER_IPS={shlex.quote(ips)} "
                   f"GPU_IPS={shlex.quote(gips)}; "
                   f"( {a.command} ) > aws_run.out 2>&1; echo $? > aws_run.rc")
        else:
            # the NIC speed as AWS states it ("50 Gigabit"): ENA reports no
            # link speed (/sys/class/net/*/speed = -1), and the submission's
            # hardware record wants one (io500_run job/hardware)
            net = (st.get("facts") or {}).get("network") or ""
            m = re.match(r"(?:Up to )?(\d+(?:\.\d+)?) Gigabit", net)
            pre = f"ROCEPOOL_NIC_GBPS={m.group(1)} ROCEPOOL_NIC_GBPS_SOURCE={shlex.quote('AWS: ' + net)} " if m else ""
            if st.get("type"):
                pre += f"ROCEPOOL_INSTANCE_TYPE={shlex.quote(st['type'])} "
            cmd = f"cd /root/rocepool && {pre}nohup ./run.sh " + " ".join(shlex.quote(f) for f in flags) + \
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
            if n > seen and a.quiet_command:      # a private command: its output stays on the cluster
                seen = n
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
                 "onsite_kv.json", "onsite_gds.json", "onsite_kvcache.json", "selftest.log",
                 "rapidsse_tpch_report.json"]  # tools/rapidsse_tpch.py (RapidsSE's TPC-H on rocepool)
        # targeted measurements (roce_onsite.py --bench writes): onsite_writes_spin<N>.json
        # and the probes before the valid run: ranks per node, OFI endpoints per rank, the rates it was sized by
        r = self.ssh(head, "cd /root/rocepool && ls onsite_writes_*.json onsite_io500_ranks.json onsite_io500_eps*.json "
                           "onsite_rates.json 2>/dev/null", check=False, timeout=60)
        files += [f for f in r.stdout.split() if f.startswith("onsite_") and f.endswith(".json")]
        # what the run asked for by name (--fetch), relative to the tree on the head
        for f in getattr(self.args, "fetch", None) or []:
            if f and not f.startswith("/") and ".." not in f.split("/") and f not in files:
                files.append(f)
        got = []
        for f in files:
            try:
                self.scp(f"root@{head}:/root/rocepool/{f}", os.path.join(self.dir, os.path.basename(f)))
                got.append(f)
            except RuntimeError:
                pass
        # every host's kernel log tail (OOM kills, NIC errors) and memory
        tagged = [(f"host{i}", h) for i, h in enumerate(self.state.get("hosts", []))] + \
            [(f"gpu{i}", h) for i, h in enumerate(self.state.get("gpu_hosts") or [])]
        for label, h in tagged:
            try:
                r = self.ssh(h["public"], "dmesg -T 2>/dev/null | tail -200; echo; free -m; echo; "
                                          "ps -eo pid,rss,vsz,stat,etime,comm --sort=-rss | head -15",
                             check=False, timeout=120)
                with open(os.path.join(self.dir, f"{label}_kernel.txt"), "w") as f:
                    f.write(r.stdout)
                got.append(f"{label}_kernel.txt")
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
        # what an IO500 submission uploads (io500_run.submission_bundle): the
        # result directory, config, io500-verify, system description -- the
        # official run's, and the quick run's next to it when there is one
        for name, src in (("io500_submission.tgz", "io500_run"), ("io500_submission_quick.tgz", "io500_run_prev")):
            try:
                self.scp(f"root@{head}:/root/rocepool/.bench_cache/{src}/submission.tgz", os.path.join(self.dir, name))
                got.append(name)
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
        for iid, alloc in list((st.get("eips") or {}).items()):
            # terminating disassociates it; an allocated, unassociated address costs money
            for _ in range(10):
                r = self.aws("ec2", "release-address", "--allocation-id", alloc, check=False, parse=False)
                if r.returncode == 0 or "InvalidAllocationID.NotFound" in (r.stderr or ""):
                    del st["eips"][iid]
                    break
                time.sleep(self.args.poll)
            else:
                log(f"could not release Elastic IP {alloc} ({iid}): release it by hand")
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
    r.add_argument("--mode", choices=("efa", "rxe", "gpu"), default="efa")
    r.add_argument("--nodes", type=int, default=10)
    r.add_argument("--type", help=f"instance type (default {DEFAULT_TYPE['efa']} for efa, {DEFAULT_TYPE['rxe']} for rxe)")
    r.add_argument("--quick", action="store_true", help="30 s IO500 instead of the 300 s valid run")
    r.add_argument("--self-test", action="store_true", help="also run the unit suite on the head node (~3 min)")
    r.add_argument("--keep", action="store_true", help="leave the cluster up afterwards (`down` releases it)")
    r.add_argument("--command", help="run this shell command on the head (in the tree; HEAD_IP, PEER_IPS set) instead of "
                                     "run.sh -- for diagnosis; its output comes back as aws_run.out")
    r.add_argument("--quiet-command", action="store_true",
                   help="do not echo the --command's output here (a private bundle's run: it comes back encrypted)")
    r.add_argument("--fetch", action="append", default=[],
                   help="also bring back this file from the tree on the head (relative path; repeatable)")
    r.add_argument("--gpu-type", help="with --gpu-nodes: GPU hosts joined to the EFA cluster (e.g. g6e.12xlarge)")
    r.add_argument("--gpu-nodes", type=int, default=0,
                   help="that many GPU hosts in the same AZ and placement group as the --nodes storage hosts "
                        "(--mode efa); the command sees their addresses in GPU_IPS")
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
