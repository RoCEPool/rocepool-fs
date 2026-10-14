"""GPUDirect Storage (roce/native/rocepool_cufile.c): rocepool's cuFile
library, the libcufile.so.0 that GDS-enabled programs (PyTorch/kvikio, RAPIDS,
DALI, renderers) call, moves file data between rocepool's OSTs and GPU memory.

No GPU here: ROCEPOOL_DEV_EMU=1 gives the library an emulated device whose
memory is mapped PROT_NONE in the process -- the CPU cannot touch it
without a fault -- and the RDMA emulator reaches it through its dma-buf
registration, as a NIC reaches a GPU's memory. A C program linked against
libcufile.so.0 (tests/fakes/gds_probe.c) reads and writes through it:
files of the LD_PRELOAD interception library in the same process, files
of other file systems (through host memory), every size class (inline,
one stripe, many stripes, holes, unaligned), the batch and async API, and
the fallback when the device memory cannot be registered with the NIC."""
import hashlib
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(ROOT)


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class GDSTests(unittest.TestCase):
    port = 23420
    # the ofi backend's test provider (libfabric tcp) has no FI_HMEM: there
    # the library must still be right, through host memory
    direct = os.environ.get("ROCEPOOL_BACKEND", "softverbs") == "softverbs"

    def assertMoved(self, st, n, direct=None):
        """n bytes between the OSTs and the device: by RDMA into/out of it, or (no device registration) bounced"""
        if self.direct if direct is None else direct:
            self.assertEqual((st["direct"], st["bounced"]), (n, 0))
        else:
            self.assertEqual((st["direct"], st["bounced"]), (0, n))

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_cufile_lib, ensure_intercept_lib, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.icp = ensure_intercept_lib()
        cls.cufile = ensure_cufile_lib()
        cls.cudir = os.path.join(os.path.dirname(cls.cufile), "cufile")
        cls.tmp = tempfile.mkdtemp()
        cls.probe = os.path.join(cls.tmp, "gds_probe")
        subprocess.run(["gcc", "-O2", "-Wall", "-Werror", os.path.join(ROOT, "tests", "fakes", "gds_probe.c"),
                        "-o", cls.probe, "-L", cls.cudir, "-l:libcufile.so.0", "-ldl"], check=True)
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=cls.lib, PYTHONPATH=ROOT)
        cls.srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                    ROOT, str(cls.port), "2", "4"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   text=True, env=cls.env)
        _, cls.mdts, cls.osts = cls.srv.stdout.readline().split()
        cls.prefix = os.path.join(cls.tmp, "rp")
        cls.penv = dict(cls.env, LD_PRELOAD=cls.icp, LD_LIBRARY_PATH=cls.cudir, ROCEPOOL_DEV_EMU="1",
                        ROCEPOOL_PREFIX=cls.prefix, ROCEPOOL_MDTS=cls.mdts, ROCEPOOL_OSTS=cls.osts,
                        ROCEPOOL_ATTR_TTL="3", ROCEPOOL_STRIPE_SIZE=str(1 << 20))

    @classmethod
    def tearDownClass(cls):
        cls.srv.stdin.close()
        cls.srv.wait(timeout=10)
        shutil.rmtree(cls.tmp, ignore_errors=True)

    # ---- helpers
    def run_probe(self, *args, env=None, timeout=120):
        out = subprocess.run([self.probe, *map(str, args)], capture_output=True, text=True,
                             env=dict(self.penv, **(env or {})), timeout=timeout)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        st = next(l for l in out.stdout.splitlines() if l.startswith("STATS "))
        d, b, p, rt = st.split()[1:]
        return out.stdout, {"direct": int(d), "bounced": int(b), "posix": int(p), "runtime": rt}

    def put(self, name: str, data: bytes, at: int = 0):
        """writes through the interception library, from an ordinary program"""
        src = os.path.join(self.tmp, "src.bin")
        with open(src, "wb") as f:
            f.write(data)
        code = (f"import os\nP={self.prefix!r}+'/'+{name!r}\nos.makedirs(os.path.dirname(P), exist_ok=True)\n"
                f"fd=os.open(P, os.O_RDWR|os.O_CREAT, 0o644)\nos.pwrite(fd, open({src!r},'rb').read(), {at})\n"
                f"os.close(fd)\n")
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                             env=dict(self.penv, ROCEPOOL_DEV_EMU=""), timeout=120)
        self.assertEqual(out.returncode, 0, out.stderr)

    def get(self, name: str) -> bytes:
        code = f"import sys\nsys.stdout.buffer.write(open({self.prefix!r}+'/'+{name!r},'rb').read())\n"
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, env=self.penv, timeout=120)
        self.assertEqual(out.returncode, 0, out.stderr)
        return out.stdout

    def gpu_read(self, name, off, length, **kw):
        out = os.path.join(self.tmp, "out.bin")
        if os.path.exists(out):
            os.unlink(out)
        extra = ["noreg"] if kw.pop("noreg", False) else []
        txt, st = self.run_probe(kw.pop("cmd", "read"), f"{self.prefix}/{name}", off, length, out, *extra, **kw)
        with open(out, "rb") as f:
            return f.read(), st

    # ---- tests
    def test_striped_file_lands_in_device_memory_directly(self):
        data = random.Random(1).randbytes(9 * (1 << 20) + 12345)  # 10 stripes over 4 OSTs, a short last one
        self.put("big/a.bin", data)
        got, st = self.gpu_read("big/a.bin", 0, len(data))
        self.assertEqual(hashlib.sha256(got).hexdigest(), hashlib.sha256(data).hexdigest())
        self.assertEqual(st["runtime"], "emulated")
        # every byte by RDMA into the device, none through host memory
        self.assertMoved(st, len(data))
        self.assertEqual(st["posix"], 0)

    def test_unaligned_ranges_past_eof_and_holes(self):
        data = random.Random(2).randbytes(3 * (1 << 20) + 777)
        self.put("big/b.bin", data)
        for off, n in ((0, 1), (4095, 1 << 20), ((1 << 20) - 3, 7), (2 * (1 << 20) + 100, 5 << 20), (len(data), 10)):
            got, _ = self.gpu_read("big/b.bin", off, n)
            self.assertEqual(got, data[off:off + n], (off, n))
        # a sparse file: the unwritten stripes read as zeros
        tail = random.Random(3).randbytes(1000)
        self.put("big/sparse.bin", tail, at=5 * (1 << 20))
        got, _ = self.gpu_read("big/sparse.bin", 0, 6 << 20)
        self.assertEqual(got, bytes(5 << 20) + tail)

    def test_small_inline_file(self):
        data = b"tiny scene description\n" * 20
        self.put("small/s.txt", data)
        got, st = self.gpu_read("small/s.txt", 0, 1 << 16)
        self.assertEqual(got, data)
        self.assertEqual(st["bounced"], len(data))  # Data-on-MDT comes with the entry: one copy into the device

    def test_without_nic_registration_falls_back_to_host_copies(self):
        data = random.Random(4).randbytes(2 * (1 << 20) + 5)
        self.put("big/c.bin", data)
        got, st = self.gpu_read("big/c.bin", 0, len(data), env={"ROCEPOOL_DEV_EMU_NO_DMABUF": "1"})
        self.assertEqual(got, data)
        self.assertMoved(st, len(data), direct=False)

    def test_implicit_buffer_registration(self):
        data = random.Random(5).randbytes(3 << 20)
        self.put("big/d.bin", data)
        got, st = self.gpu_read("big/d.bin", 0, len(data), noreg=True)  # no cuFileBufRegister: done on first use
        self.assertEqual(got, data)
        self.assertMoved(st, len(data))

    def test_write_from_device_memory(self):
        self.put("w/.keep", b"")
        for name, size, off in (("w/big.bin", 5 * (1 << 20) + 99, 0), ("w/small.bin", 3000, 0),
                                ("w/at.bin", 2 << 20, 3 << 20)):
            data = random.Random(size).randbytes(size)
            src = os.path.join(self.tmp, "in.bin")
            with open(src, "wb") as f:
                f.write(data)
            txt, st = self.run_probe("write", f"{self.prefix}/{name}", off, src)
            self.assertIn(f"WROTE {size}", txt)
            self.assertEqual(self.get(name), bytes(off) + data, name)
            if size > (1 << 20):
                self.assertMoved(st, size)  # each OST RDMA-read its stripes out of the device
            got, _ = self.gpu_read(name, off, size)
            self.assertEqual(got, data)

    def test_write_over_an_inline_file_spills_it(self):
        self.put("w/.keep", b"")
        self.put("w/grow.bin", b"x" * 500)
        data = random.Random(6).randbytes(2 << 20)
        src = os.path.join(self.tmp, "in.bin")
        with open(src, "wb") as f:
            f.write(data)
        _, st = self.run_probe("write", f"{self.prefix}/w/grow.bin", 100, src)
        self.assertEqual(self.get("w/grow.bin"), b"x" * 100 + data)
        self.assertMoved(st, len(data))

    def test_batch_and_async(self):
        data = random.Random(7).randbytes(12 << 20)
        self.put("big/e.bin", data)
        out = os.path.join(self.tmp, "batch.bin")
        txt, st = self.run_probe("batch", f"{self.prefix}/big/e.bin", 1 << 20, 12, out)
        self.assertIn(f"BATCH {12 << 20}", txt)
        with open(out, "rb") as f:
            self.assertEqual(f.read(), data)
        self.assertMoved(st, len(data))
        got, _ = self.gpu_read("big/e.bin", 1000, 3 << 20, cmd="async")
        self.assertEqual(got, data[1000:1000 + (3 << 20)])

    def test_files_of_other_file_systems_go_through_host_memory(self):
        local = os.path.join(self.tmp, "local.bin")
        data = random.Random(8).randbytes(1 << 20)
        with open(local, "wb") as f:
            f.write(data)
        out = os.path.join(self.tmp, "out.bin")
        _, st = self.run_probe("read", local, 0, len(data), out, env={"ROCEPOOL_CUFILE_REAL": "/nonexistent"})
        with open(out, "rb") as f:
            self.assertEqual(f.read(), data)
        self.assertEqual(st["posix"], len(data))
        self.assertEqual(st["direct"], 0)

    def test_benchmark_mode(self):
        data = random.Random(9).randbytes(8 << 20)
        self.put("big/bench.bin", data)
        txt, st = self.run_probe("bench", f"{self.prefix}/big/bench.bin", len(data), 3)
        line = next(l for l in txt.splitlines() if l.startswith("BENCH "))
        total, _, gbps = line.split()[1:]
        self.assertEqual(int(total), 3 * len(data))
        self.assertGreater(float(gbps), 0)
        self.assertMoved(st, 3 * len(data))


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(os.path.exists("/dev/fuse") and os.geteuid() == 0 and shutil.which("gcc"),
                     "needs /dev/fuse, root and gcc")
class GDSFuseTests(unittest.TestCase):
    """The same library on descriptors of the FUSE mount: the program opens
    files on the mount as usual; their reads and writes to device memory go
    around the kernel, straight between the OSTs and the device."""
    port = 23440

    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, ROOT)
        from roce.native.build_lib import ensure_cufile_lib, ensure_fuse_client, ensure_softverbs_lib
        cls.lib = ensure_softverbs_lib()
        cls.bin = ensure_fuse_client()
        cls.cufile = ensure_cufile_lib()
        cls.cudir = os.path.join(os.path.dirname(cls.cufile), "cufile")
        cls.tmp = tempfile.mkdtemp()
        cls.probe = os.path.join(cls.tmp, "gds_probe")
        subprocess.run(["gcc", "-O2", "-Wall", "-Werror", os.path.join(ROOT, "tests", "fakes", "gds_probe.c"),
                        "-o", cls.probe, "-L", cls.cudir, "-l:libcufile.so.0", "-ldl"], check=True)
        cls.env = dict(os.environ, ROCE_TRANSPORT_LIB=cls.lib, PYTHONPATH=ROOT)
        cls.srv = subprocess.Popen([sys.executable, "-u", os.path.join(ROOT, "tests", "fakes", "native_cluster.py"),
                                    ROOT, str(cls.port), "2", "4"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   text=True, env=cls.env)
        _, cls.mdts, cls.osts = cls.srv.stdout.readline().split()
        cls.mnt = os.path.join(cls.tmp, "mnt")
        cls.fuse = subprocess.Popen([cls.bin, "--mount", cls.mnt, "--mdts", cls.mdts, "--osts", cls.osts,
                                     "--attr-ttl", "3", "--uring", "0"],
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=cls.env)
        line = cls.fuse.stdout.readline()
        assert line.startswith("READY"), line + cls.fuse.stdout.read()
        cls.penv = dict(cls.env, LD_LIBRARY_PATH=cls.cudir, ROCEPOOL_DEV_EMU="1", ROCEPOOL_PREFIX=cls.mnt,
                        ROCEPOOL_MDTS=cls.mdts, ROCEPOOL_OSTS=cls.osts)

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["umount", "-l", cls.mnt], capture_output=True)
        cls.fuse.terminate()
        try:
            cls.fuse.wait(timeout=10)
        except subprocess.TimeoutExpired:
            cls.fuse.kill()
        cls.srv.stdin.close()
        cls.srv.wait(timeout=10)
        shutil.rmtree(cls.tmp, ignore_errors=True)

    run_probe = GDSTests.run_probe
    assertMoved = GDSTests.assertMoved
    direct = GDSTests.direct

    def test_read_files_written_through_the_mount(self):
        os.makedirs(os.path.join(self.mnt, "f"), exist_ok=True)
        data = random.Random(11).randbytes(6 * (1 << 20) + 3)
        with open(os.path.join(self.mnt, "f", "a.bin"), "wb") as f:
            f.write(data)
        out = os.path.join(self.tmp, "out.bin")
        _, st = self.run_probe("read", os.path.join(self.mnt, "f", "a.bin"), 0, len(data), out)
        with open(out, "rb") as f:
            self.assertEqual(f.read(), data)
        self.assertMoved(st, len(data))
        self.assertEqual(st["posix"], 0)

    def test_write_from_the_device_is_visible_on_the_mount(self):
        os.makedirs(os.path.join(self.mnt, "f"), exist_ok=True)
        data = random.Random(12).randbytes(3 * (1 << 20) + 17)
        src = os.path.join(self.tmp, "in.bin")
        with open(src, "wb") as f:
            f.write(data)
        path = os.path.join(self.mnt, "f", "w.bin")
        _, st = self.run_probe("write", path, 0, src)
        self.assertMoved(st, len(data))
        self.assertEqual(os.stat(path).st_size, len(data))  # no wait for the attribute TTL
        with open(path, "rb") as f:
            self.assertEqual(f.read(), data)


class GPUInventoryTests(unittest.TestCase):
    """tools/bench_gds.gpu_inventory on a fake sysfs and fake nvidia-smi /
    modinfo: what the on-site report says about a GPU host."""

    def setUp(self):
        sys.path.insert(0, ROOT)
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)

    def dev(self, chain: str, numa: int) -> str:
        """a PCI device at sys/devices/<chain>, linked from sys/bus/pci/devices"""
        d = os.path.join(self.root, "sys", "devices", chain)
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "numa_node"), "w") as f:
            f.write(str(numa))
        bdf = os.path.basename(chain)
        os.makedirs(os.path.join(self.root, "sys", "bus", "pci", "devices"), exist_ok=True)
        os.symlink(d, os.path.join(self.root, "sys", "bus", "pci", "devices", bdf))
        return d

    def nic(self, name: str, chain: str, numa: int):
        d = self.dev(chain, numa)
        ib = os.path.join(self.root, "sys", "class", "infiniband", name)
        os.makedirs(ib)
        os.symlink(d, os.path.join(ib, "device"))

    def fake_run(self, smi_lines: list[str], license_: str = "Dual MIT/GPL"):
        def run(argv, **kw):
            if argv[0] == "nvidia-smi":
                return subprocess.CompletedProcess(argv, 0, "\n".join(smi_lines) + "\n", "")
            if argv[0] == "modinfo":
                return subprocess.CompletedProcess(argv, 0, license_ + "\n", "")
            if argv[0] == "modprobe":
                os.makedirs(os.path.join(self.root, "sys", "module", "nvidia_peermem"), exist_ok=True)
                return subprocess.CompletedProcess(argv, 0, "", "")
            raise AssertionError(argv)
        return run

    def test_gpus_dmabuf_and_pcie_relations(self):
        from tools.bench_gds import gpu_inventory
        sw = "pci0000:00/0000:00:01.0/0000:01:00.0"
        self.dev(f"{sw}/0000:02:00.0/0000:03:00.0", 0)                    # GPU0, behind the NIC's switch
        self.dev("pci0000:00/0000:00:02.0/0000:04:00.0", 0)              # GPU1, another root port
        self.dev("pci0000:80/0000:80:01.0/0000:81:00.0", 1)              # GPU2, the other socket
        self.nic("mlx5_0", f"{sw}/0000:02:01.0/0000:05:00.0", 0)
        smi = ["0, NVIDIA H100, 00000000:03:00.0, 81559, 560.35.03",
               "1, NVIDIA H100, 00000000:04:00.0, 81559, 560.35.03",
               "2, NVIDIA H100, 00000000:81:00.0, 81559, 560.35.03"]
        inv = gpu_inventory(fix=True, sysfs=self.root, run=self.fake_run(smi))
        self.assertEqual([g["bdf"] for g in inv["gpus"]], ["0000:03:00.0", "0000:04:00.0", "0000:81:00.0"])
        self.assertTrue(inv["open_kernel_modules"])
        self.assertTrue(inv["peermem"])  # loaded by the check (modprobe)
        self.assertEqual(inv["nics"]["mlx5_0"]["gpus"], {0: "PXB", 1: "PHB", 2: "SYS"})
        self.assertTrue(any("different CPU sockets" in n for n in inv["notes"]))

    def test_proprietary_modules_without_peermem_bounce(self):
        from tools.bench_gds import gpu_inventory
        self.dev("pci0000:00/0000:00:01.0/0000:01:00.0", 0)
        inv = gpu_inventory(fix=False, sysfs=self.root,
                            run=self.fake_run(["0, NVIDIA A100, 00000000:01:00.0, 40960, 535.0"], license_="NVIDIA"))
        self.assertFalse(inv["dmabuf"])
        self.assertFalse(inv["peermem"])
        self.assertTrue(any("bounce through host memory" in n for n in inv["notes"]))

    def test_no_gpu(self):
        from tools.bench_gds import gpu_inventory, summarize

        def run(argv, **kw):
            raise FileNotFoundError(argv[0])
        inv = gpu_inventory(sysfs=self.root, run=run)
        self.assertEqual(inv["gpus"], [])
        st, line = summarize({"inventory": inv})
        self.assertEqual(st, "PASS")
        self.assertIn("no NVIDIA GPU", line)


@unittest.skipUnless(os.geteuid() == 0 and shutil.which("gcc"), "needs root (the crossover rig) and gcc")
class BenchGDSToolTests(unittest.TestCase):
    """tools/bench_gds.py end to end on the crossover rig, emulated device:
    the on-site GPU step's own code path."""

    def test_emulated_benchmark(self):
        with tempfile.TemporaryDirectory() as td:
            rep = os.path.join(td, "gds.json")
            r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "bench_gds.py"), "--emulate",
                                "--size-mib", "32", "--iters", "2", "--report", rep],
                               capture_output=True, text=True, timeout=900, env=dict(os.environ, ROCEPOOL_TOPOLOGY=""))
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            import json
            with open(rep) as f:
                d = json.load(f)
        res = {x["case"]: {"direct": x["direct_bytes"], "bounced": x["bounced_bytes"], "gbps": x["gbps"]}
               for x in d["bench"]["results"]}
        GDSTests.assertMoved(self, res["direct"], 2 * (32 << 20), direct=GDSTests.direct)
        GDSTests.assertMoved(self, res["bounced"], 2 * (32 << 20), direct=False)
        self.assertGreater(res["direct"]["gbps"], 0)
        self.assertIn(d["status"], ("PASS", "WARN"))
