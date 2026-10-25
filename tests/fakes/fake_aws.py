#!/usr/bin/env python3
"""A fake `aws` CLI for tests/test_aws_cluster.py: canned JSON per call,
every call appended to $FAKE_AWS_LOG (one JSON argv per line).
$FAKE_AWS_NO_EFA=1: the instance type has no EFA.
$FAKE_AWS_NO_CAPACITY=az[,az...]: run-instances fails there with InsufficientInstanceCapacity."""
import json
import os
import sys

argv = sys.argv[1:]
with open(os.environ["FAKE_AWS_LOG"], "a") as f:
    f.write(json.dumps(argv) + "\n")
if "--region" in argv:
    i = argv.index("--region")
    del argv[i:i + 2]
if "--output" in argv:
    i = argv.index("--output")
    del argv[i:i + 2]
cmd = tuple(argv[:2])
n = 3
if "--count" in argv:
    n = int(argv[argv.index("--count") + 1])
ids = [f"i-{k:04d}" for k in range(n)]


def out(o):
    print(json.dumps(o))


if cmd == ("configure", "get"):
    print("us-east-1")
elif cmd == ("sts", "get-caller-identity"):
    out({"Account": "123456789012", "Arn": "arn:aws:iam::123456789012:user/test"})
elif cmd == ("ec2", "describe-instance-types"):
    out({"InstanceTypes": [{"ProcessorInfo": {"SupportedArchitectures": ["x86_64"]},
                            "NetworkInfo": {"EfaSupported": os.environ.get("FAKE_AWS_NO_EFA") != "1",
                                            "NetworkPerformance": "200 Gigabit"},
                            "VCpuInfo": {"DefaultVCpus": 128}, "MemoryInfo": {"SizeInMiB": 262144},
                            **({"InstanceStorageInfo": {"TotalSizeInGB": int(os.environ["FAKE_AWS_NVME_GB"])}}
                               if os.environ.get("FAKE_AWS_NVME_GB") else {})}]})
elif cmd == ("ssm", "get-parameters"):
    out({"Parameters": [{"Value": "ami-0123456789abcdef0"}]})
elif cmd == ("ec2", "describe-instance-type-offerings"):
    out({"InstanceTypeOfferings": [{"Location": "us-east-1b"}, {"Location": "us-east-1a"}, {"Location": "us-east-1d"}]})
elif cmd == ("ec2", "describe-vpcs"):
    out({"Vpcs": [{"VpcId": "vpc-1"}]})
elif cmd == ("ec2", "describe-subnets"):
    out({"Subnets": [{"AvailabilityZone": "us-east-1a", "SubnetId": "subnet-a"},
                     {"AvailabilityZone": "us-east-1c", "SubnetId": "subnet-c"},
                     {"AvailabilityZone": "us-east-1d", "SubnetId": "subnet-d"}]})
elif cmd == ("ec2", "create-key-pair"):
    out({"KeyMaterial": "-----BEGIN OPENSSH PRIVATE KEY-----\nfake\n-----END OPENSSH PRIVATE KEY-----\n"})
elif cmd == ("ec2", "create-security-group"):
    out({"GroupId": "sg-1"})
elif cmd == ("ec2", "run-instances"):
    az = json.loads(argv[argv.index("--placement") + 1])["AvailabilityZone"]
    if az in os.environ.get("FAKE_AWS_NO_CAPACITY", "").split(","):
        sys.stderr.write("An error occurred (InsufficientInstanceCapacity) when calling the RunInstances operation: "
                         f"We currently do not have sufficient capacity in the Availability Zone you requested ({az}).\n")
        sys.exit(254)
    out({"Instances": [{"InstanceId": i} for i in ids]})
elif cmd == ("ec2", "describe-instances"):
    req = argv[argv.index("--instance-ids") + 1:]
    out({"Reservations": [{"Instances": [{"InstanceId": i, "PublicIpAddress": f"54.0.0.{k + 1}",
                                          "PrivateIpAddress": f"172.31.0.{k + 1}"} for k, i in enumerate(req)]}]})
elif cmd == ("service-quotas", "get-service-quota"):
    out({"Quota": {"Value": float(os.environ.get("FAKE_AWS_VCPU_QUOTA", "5000"))}})
else:
    out({})
