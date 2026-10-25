"""tools/aws_cluster.py end to end against a fake aws CLI and fake ssh/scp:
the resources it creates (and with what), the host setup per mode, the
run on the head node, the reports fetched, and that everything is released
-- also when a step fails."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "tools", "aws_cluster.py")


class AwsClusterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bin = os.path.join(self.tmp, "bin")
        os.makedirs(self.bin)
        for name, src in (("aws", "fake_aws.py"), ("ssh", "fake_ssh.py"), ("scp", "fake_ssh.py")):
            os.symlink(os.path.join(ROOT, "tests", "fakes", src), os.path.join(self.bin, name))
        self.aws_log = os.path.join(self.tmp, "aws.log")
        self.ssh_log = os.path.join(self.tmp, "ssh.log")
        self.run_ids = []

    def tearDown(self):
        for rid in self.run_ids:
            shutil.rmtree(os.path.join(ROOT, "aws_runs", rid), ignore_errors=True)
        try:
            os.rmdir(os.path.join(ROOT, "aws_runs"))
        except OSError:
            pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_tool(self, *args, env_extra=None, run_id="rp-test"):
        self.run_ids.append(run_id)
        env = dict(os.environ, FAKE_AWS_LOG=self.aws_log, FAKE_SSH_LOG=self.ssh_log, **(env_extra or {}))
        return subprocess.run([sys.executable, TOOL, "run", "--run-id", run_id, "--aws", os.path.join(self.bin, "aws"),
                               "--ssh", os.path.join(self.bin, "ssh"), "--scp", os.path.join(self.bin, "scp"),
                               "--poll", "0", "--ssh-cidr", "203.0.113.7/32", *args],
                              capture_output=True, text=True, env=env, timeout=300)

    def calls(self, log):
        with open(log) as f:
            return [json.loads(l) for l in f]

    def aws_calls(self, *prefix):
        return [c for c in self.calls(self.aws_log) if c[:len(prefix)] == list(prefix)]

    def test_efa_run_creates_runs_fetches_and_releases(self):
        r = self.run_tool("--mode", "efa", "--nodes", "3", "--quick")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        # instances: EFA interface in the subnet of an AZ that offers the type, placement group, safety shutdown
        run = self.aws_calls("ec2", "run-instances")[0]
        nic = json.loads(run[run.index("--network-interfaces") + 1])[0]
        self.assertEqual(nic["InterfaceType"], "efa")
        self.assertEqual(nic["SubnetId"], "subnet-a")
        self.assertEqual(nic["Groups"], ["sg-1"])
        self.assertEqual(run[run.index("--count") + 1], "3")
        self.assertEqual(run[run.index("--instance-type") + 1], "c6in.32xlarge")
        self.assertEqual(run[run.index("--instance-initiated-shutdown-behavior") + 1], "terminate")
        self.assertIn("shutdown -h +360", run[run.index("--user-data") + 1])
        self.assertEqual(json.loads(run[run.index("--placement") + 1])["GroupName"], "rp-test")
        ebs = json.loads(run[run.index("--block-device-mappings") + 1])[0]["Ebs"]
        self.assertEqual((ebs["VolumeType"], ebs["Throughput"], ebs["Iops"]), ("gp3", 1000, 16000))
        # security group: all traffic among members, SSH only from here
        ing = self.aws_calls("ec2", "authorize-security-group-ingress")
        self.assertEqual(json.loads(ing[0][ing[0].index("--ip-permissions") + 1])[0]["UserIdGroupPairs"],
                         [{"GroupId": "sg-1"}])
        self.assertIn("203.0.113.7/32", ing[1])
        self.assertTrue(self.aws_calls("ec2", "authorize-security-group-egress"))
        # hosts: EFA installer, root SSH between hosts
        ssh = self.calls(self.ssh_log)
        setup = [c for c in ssh if "ROCEPOOL_SETUP" in c[-1]]
        self.assertEqual(len(setup), 3)
        self.assertIn("efa_installer.sh -y", setup[0][-1])
        self.assertNotIn("rdma link add", setup[0][-1])
        self.assertEqual(sum("authorized_keys" in c[-1] and "ssh-ed25519 AAAAfake" in c[-1] for c in ssh), 3)
        # run.sh on the head with the other two as peers, by private IP
        start = next(c for c in ssh if "run.sh" in c[-1])
        self.assertIn("root@54.0.0.1", start)
        self.assertIn("--peers root@172.31.0.2=172.31.0.2,root@172.31.0.3=172.31.0.3", start[-1])
        self.assertIn("--quick", start[-1])
        self.assertIn("--ip 172.31.0.1", start[-1])
        self.assertTrue(any(c[0] == "scp" and c[-1] == "root@54.0.0.1:/root/rocepool.tar.gz" for c in ssh))
        # reports home, score shown, everything released
        d = os.path.join(ROOT, "aws_runs", "rp-test")
        self.assertTrue(os.path.exists(os.path.join(d, "onsite_io500_valid.json")))
        self.assertIn("IO500 score 123.4", r.stdout)
        for c in (("ec2", "terminate-instances"), ("ec2", "delete-placement-group"), ("ec2", "delete-security-group"),
                  ("ec2", "delete-key-pair")):
            self.assertTrue(self.aws_calls(*c), c)
        st = json.load(open(os.path.join(d, "state.json")))
        self.assertTrue(st["down"])
        self.assertEqual(st["instances"], [])
        self.assertEqual(oct(os.stat(os.path.join(d, "ssh_key")).st_mode & 0o777), "0o600")
        # the shipped tree has run.sh and no caches
        import tarfile
        names = tarfile.open(os.path.join(d, "rocepool.tar.gz")).getnames()
        self.assertIn("rocepool/run.sh", names)
        self.assertFalse([n for n in names if "/.bench_cache" in n or "/aws_runs" in n])

    def test_rxe_mode_sets_up_soft_roce_without_efa(self):
        r = self.run_tool("--mode", "rxe", "--nodes", "2")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        run = self.aws_calls("ec2", "run-instances")[0]
        self.assertNotIn("InterfaceType", run[run.index("--network-interfaces") + 1])
        self.assertEqual(run[run.index("--instance-type") + 1], "c6in.16xlarge")
        setup = [c for c in self.calls(self.ssh_log) if "ROCEPOOL_SETUP" in c[-1]][0][-1]
        self.assertIn("modprobe rdma_rxe", setup)
        self.assertIn("rdma link add rxe0 type rxe netdev $dev", setup)
        self.assertIn("mtu 9001", setup)
        self.assertNotIn("efa_installer", setup)
        start = next(c for c in self.calls(self.ssh_log) if "run.sh" in c[-1])[-1]
        self.assertNotIn("--quick", start)  # the valid 300 s run

    def test_local_nvme_is_assembled_and_mounted(self):
        r = self.run_tool("--mode", "efa", "--nodes", "2", "--quick", env_extra={"FAKE_AWS_NVME_GB": "30000"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        setup = [c for c in self.calls(self.ssh_log) if "ROCEPOOL_SETUP" in c[-1]][0][-1]
        self.assertIn("mdadm --create /dev/md0 --run --level=0", setup)
        self.assertIn("Instance Storage", setup)       # the instance store only, never the EBS disk
        self.assertIn("mount -o noatime $dev /nvmedata", setup)
        # and without local NVMe, none of it
        r = self.run_tool("--mode", "efa", "--nodes", "2", "--quick", run_id="rp-test2")
        setup = [c for c in self.calls(self.ssh_log) if "ROCEPOOL_SETUP" in c[-1]][-1][-1]
        self.assertNotIn("mdadm", setup)

    def test_type_without_efa_is_refused_and_nothing_left_behind(self):
        r = self.run_tool("--mode", "efa", "--nodes", "2", "--type", "m5.large", env_extra={"FAKE_AWS_NO_EFA": "1"})
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("has no EFA", r.stdout)
        self.assertFalse(self.aws_calls("ec2", "run-instances"))
        self.assertFalse(self.aws_calls("ec2", "create-security-group"))

    def test_low_vcpu_quota_stops_before_creating_anything(self):
        r = self.run_tool("--mode", "rxe", "--nodes", "3", env_extra={"FAKE_AWS_VCPU_QUOTA": "64"})
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("vCPU quota", r.stdout)
        self.assertIn("--desired-value 384", r.stdout)
        self.assertFalse(self.aws_calls("ec2", "create-key-pair"))
        self.assertFalse(self.aws_calls("ec2", "run-instances"))

    def test_az_without_capacity_moves_to_the_next(self):
        r = self.run_tool("--mode", "efa", "--nodes", "2", env_extra={"FAKE_AWS_NO_CAPACITY": "us-east-1a"})
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        runs = self.aws_calls("ec2", "run-instances")
        self.assertEqual([json.loads(c[c.index("--placement") + 1])["AvailabilityZone"] for c in runs],
                         ["us-east-1a", "us-east-1d"])
        nic = json.loads(runs[1][runs[1].index("--network-interfaces") + 1])[0]
        self.assertEqual(nic["SubnetId"], "subnet-d")
        self.assertIn("no c6in.32xlarge capacity in us-east-1a", r.stdout)

    def test_no_capacity_anywhere_fails_clearly_and_releases(self):
        r = self.run_tool("--mode", "rxe", "--nodes", "2", env_extra={"FAKE_AWS_NO_CAPACITY": "us-east-1a,us-east-1d"})
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("no capacity for 2 x c6in.16xlarge", r.stdout)
        # rxe: each AZ with the placement group, then each without
        runs = self.aws_calls("ec2", "run-instances")
        self.assertEqual([("GroupName" in json.loads(c[c.index("--placement") + 1])) for c in runs],
                         [True, True, False, False])
        for c in (("ec2", "delete-placement-group"), ("ec2", "delete-security-group"), ("ec2", "delete-key-pair")):
            self.assertTrue(self.aws_calls(*c), c)

    def test_failure_after_launch_still_releases(self):
        # a setup that never says SETUP_OK: instances exist, so they must be terminated
        bad = os.path.join(self.bin, "ssh")
        os.unlink(bad)
        with open(bad, "w") as f:
            f.write(f"#!/bin/sh\necho \"$@\" >> {self.ssh_log}.raw\ncase \"$*\" in *cloud-init*) echo up;; esac\n"
                    "exit 0\n")
        os.chmod(bad, 0o755)
        r = self.run_tool("--mode", "efa", "--nodes", "2")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("setup failed", r.stdout)
        self.assertTrue(self.aws_calls("ec2", "terminate-instances"))
        self.assertTrue(self.aws_calls("ec2", "delete-security-group"))

    def test_keep_then_down(self):
        r = self.run_tool("--mode", "rxe", "--nodes", "2", "--keep", run_id="rp-keep")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertFalse(self.aws_calls("ec2", "terminate-instances"))
        self.assertIn("down --run-id rp-keep", r.stdout)
        env = dict(os.environ, FAKE_AWS_LOG=self.aws_log, FAKE_SSH_LOG=self.ssh_log)
        lst = subprocess.run([sys.executable, TOOL, "list"], capture_output=True, text=True, env=env)
        self.assertIn("rp-keep", lst.stdout)
        d = subprocess.run([sys.executable, TOOL, "down", "--run-id", "rp-keep", "--aws", os.path.join(self.bin, "aws"),
                            "--poll", "0"], capture_output=True, text=True, env=env)
        self.assertEqual(d.returncode, 0, d.stdout + d.stderr)
        term = self.aws_calls("ec2", "terminate-instances")[0]
        self.assertEqual(term[term.index("--instance-ids") + 1:term.index("--instance-ids") + 3], ["i-0000", "i-0001"])


if __name__ == "__main__":
    unittest.main()
