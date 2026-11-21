"""tools/kvcache_e2e.py rehearsed without GPUs: the same steps and tests
against tests/fakes/fake_inference_server.py (vLLM's and LMCache's HTTP
APIs, KV chunks stored and loaded through the real RoCEPool connector) on
two local "hosts" over the RDMA emulator.

Checked: a sound setup passes all four tests; KV that comes back corrupted
fails the correctness and cross-server tests; an unregistered L1 arena fails
the zero-copy test; an engine that cannot start fails the run cleanly; a run
killed midway leaves its progress (finished steps and test results) behind;
nothing is left running afterwards.
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "tools", "kvcache_e2e.py")


def leftovers() -> str:
    """This tree's engines and OSTs still running (absolute paths, as the
    harness starts them -- not a shell that merely mentions the names)."""
    import re
    pat = "|".join(re.escape(os.path.join(ROOT, x)) for x in ("tests/fakes/fake_inference_server.py",
                                                              "tools/crossover_node.py"))
    r = subprocess.run(["pgrep", "-af", pat], capture_output=True, text=True)
    return "\n".join(l for l in r.stdout.splitlines() if "pgrep" not in l and " -c " not in l)


class PinCheck(unittest.TestCase):
    """--check-pins: the exact versions the install step pins are on PyPI,
    checked before any host is paid for (the first g6 install asked for
    lmcache==0.5.7, which PyPI has only as 0.5.7.post1)."""

    @staticmethod
    def harness():
        import importlib.util
        spec = importlib.util.spec_from_file_location("kvcache_e2e_pins", TOOL)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    @staticmethod
    def opener(published: dict):
        import io
        import urllib.error

        def op(url, timeout=0):
            name, ver = url.split("/")[-3:-1]
            if (name, ver) not in published:
                raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
            info = published[(name, ver)]
            return io.BytesIO(json.dumps(info).encode())
        return op

    def test_published_pins_pass(self):
        m = self.harness()
        pub = {("lmcache", m.LMCACHE_VERSION): {"info": {"yanked": False}, "urls": [{"filename": "x.whl"}]},
               ("vllm", "0.19.1"): {"info": {"yanked": False}, "urls": [{"filename": "y.whl"}]}}
        self.assertEqual(m.check_pins("vllm==0.19.1", opener=self.opener(pub)), [])
        self.assertEqual(m.check_pins("vllm", opener=self.opener(pub)), [])   # unpinned: not checked

    def test_missing_and_yanked_pins_fail(self):
        m = self.harness()
        pub = {("lmcache", m.LMCACHE_VERSION): {"info": {"yanked": True}, "urls": [{"filename": "x.whl"}]}}
        bad = m.check_pins("vllm==9.9.9", opener=self.opener(pub))
        self.assertEqual(len(bad), 2, bad)
        self.assertIn("yanked", bad[0])
        self.assertIn("vllm==9.9.9 is not on PyPI (HTTP 404)", bad[1])

    def test_the_pin_is_the_post_release(self):
        self.assertEqual(self.harness().LMCACHE_VERSION, "0.5.7.post1")


class StripingVerdict(unittest.TestCase):
    """T6's verdict from synthetic OST counters: parts per chunk, every OST
    holding data, balance judged only with enough objects."""

    def verdict(self, mib, objects_each, chunk=14 << 20, part=4 << 20):
        sys.path.insert(0, ROOT)
        from unittest import mock
        import tools.kvcache_e2e as k
        osts = {f"10.0.0.{i}:1995{i}": {"engine_objects": objects_each, "engine_bytes_mb": m}
                for i, m in enumerate(mib)}
        with mock.patch.object(k, "counters", return_value={"part_bytes": part, "bytes_stored": chunk * 10,
                                                              "chunks_stored": 10}), \
                mock.patch.object(k, "lmcache_chunk_bytes", return_value=chunk), \
                mock.patch.object(k, "probe_osts", return_value=osts):
            return k.t6_striping(None, None, [None])

    def test_large_chunks_over_balanced_osts_pass(self):
        r = self.verdict([500, 520, 480, 510], 500)
        self.assertEqual((r["status"], r["parts_per_chunk"]), ("PASS", 4))

    def test_an_ost_far_below_its_share_fails_when_there_is_enough_to_judge(self):
        r = self.verdict([700, 700, 700, 150], 500)
        self.assertEqual(r["status"], "FAIL")
        self.assertFalse(r["checks"]["balanced"])

    def test_few_objects_are_not_judged_for_balance(self):
        r = self.verdict([10, 10, 14, 20], 15)
        self.assertEqual(r["status"], "PASS")
        self.assertIn("too few", r["balance_judged"])

    def test_chunks_that_fit_one_part_or_an_empty_ost_fail(self):
        self.assertFalse(self.verdict([500] * 4, 500, chunk=2 << 20)["checks"]["chunk_split_into_parts"])
        self.assertFalse(self.verdict([500, 500, 500, 0], 500)["checks"]["every_ost_holds_data"])


@unittest.skipUnless(shutil.which("gcc"), "needs gcc")
class KVCacheE2ERehearsal(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def args(self, port_base, fake_args="", lengths="1024,2048"):
        return [sys.executable, TOOL, "--engine", "fake", "--hosts", "127.0.0.1,127.0.0.1", "--lengths", lengths,
                "--reps", "2", "--t1-tokens", "1536", "--port-base", str(port_base), "--idle-s", "1",
                "--t5-prompts", "2", "--t5-tokens", "2048",
                "--report", os.path.join(self.tmp, "report.json"),
                "--progress", os.path.join(self.tmp, "progress.json"), "--logs", os.path.join(self.tmp, "logs"),
                *(["--fake-args=" + fake_args] if fake_args else [])]

    def run_e2e(self, port_base, fake_args=""):
        r = subprocess.run(self.args(port_base, fake_args), capture_output=True, text=True, timeout=600)
        with open(os.path.join(self.tmp, "report.json")) as f:
            return r, json.load(f)

    def test_a_sound_setup_passes_every_test(self):
        r, rep = self.run_e2e(4000)
        self.assertEqual(r.returncode, 0, r.stdout[-3000:] + r.stderr[-2000:])
        self.assertEqual(rep["verdict"], "PASS")
        t = rep["tests"]
        self.assertEqual([t[k]["status"] for k in ("T1_correctness", "T2_cross_server", "T3_ttft", "T5_one_sided",
                                                    "T6_striping", "T4_zero_copy")], ["PASS"] * 6)
        t5 = t["T5_one_sided"]
        self.assertGreater(t5["loaded_bytes"], 0)
        self.assertEqual(t5["osts_during_loads"]["data_out_bytes"], 0, "no OST sent the loaded data")
        self.assertGreater(t5["osts_during_loads"]["requests"], 0, "leases are still asked for")
        t6 = t["T6_striping"]
        self.assertEqual(t6["parts_per_chunk"], 2)          # 1 MiB chunks, 0.5 MiB parts in the fake
        self.assertEqual(t6["chunk_bytes"]["stored_mean"], 1 << 20)
        self.assertEqual(t6["chunk_bytes"]["lmcache_layout"], 1 << 20)
        self.assertEqual(len(t6["osts"]), 4)
        self.assertTrue(all(v["mib"] > 0 for v in t6["osts"].values()))
        for c in t["T1_correctness"]["cases"]:
            self.assertTrue(c["identical"])
            self.assertGreater(c["loaded_from_rocepool_bytes"], 0, "the reload really came from RoCEPool")
            self.assertEqual(c["loaded_on_l1_hit_bytes"], 0, "and the next request hit L1")
        self.assertTrue(all(c["loaded_on_B_bytes"] > 0 for c in t["T2_cross_server"]["cases"]))
        rows = t["T3_ttft"]["rows"]
        self.assertEqual([r_["target_tokens"] for r_ in rows], [1024, 2048])
        for row in rows:
            self.assertEqual(set(row["ttft_median_s"]), {"cold", "l1", "l2_local", "l2_remote"})
            self.assertGreater(row["loaded_mib_median"]["l2_remote"], 0)
        z = t["T4_zero_copy"]["hosts"]
        self.assertTrue(all(h["bytes_copied"] == 0 and h["bytes_zero_copy"] > 0 for h in z.values()))
        self.assertNotIn("T3_ttft_partial", t)
        self.assertTrue(os.path.exists(os.path.join(self.tmp, "kvcache_e2e_logs.tar.gz")))
        self.assertEqual(leftovers(), "")

    def test_corrupted_kv_from_rocepool_fails_correctness(self):
        r, rep = self.run_e2e(4200, "--corrupt-loads")
        self.assertEqual(r.returncode, 1)
        self.assertEqual(rep["verdict"], "FAIL")
        t = rep["tests"]
        self.assertEqual(t["T1_correctness"]["status"], "FAIL")
        self.assertFalse(any(c["identical"] for c in t["T1_correctness"]["cases"]))
        self.assertEqual(t["T2_cross_server"]["status"], "FAIL")
        self.assertEqual(leftovers(), "")

    def test_an_unregistered_arena_fails_zero_copy_only(self):
        r, rep = self.run_e2e(4400, "--no-register")
        self.assertEqual(rep["verdict"], "FAIL")
        t = rep["tests"]
        self.assertEqual((t["T1_correctness"]["status"], t["T2_cross_server"]["status"], t["T4_zero_copy"]["status"],
                          t["T5_one_sided"]["status"]), ("PASS", "PASS", "FAIL", "FAIL"))
        t5 = t["T5_one_sided"]
        self.assertFalse(t5["checks"]["osts_sent_no_data"], "loads into unregistered memory go through the OSTs")
        self.assertGreaterEqual(t5["osts_during_loads"]["data_out_bytes"], t5["loaded_bytes"])
        self.assertTrue(all(h["bytes_copied"] > 0 for h in t["T4_zero_copy"]["hosts"].values()))

    def test_an_engine_that_cannot_start_fails_cleanly(self):
        r, rep = self.run_e2e(4600, "--no-such-flag")
        self.assertEqual(r.returncode, 1)
        self.assertEqual(rep["steps"]["error"]["status"], "FAIL")
        self.assertIn("exited", rep["steps"]["error"]["detail"])
        self.assertEqual(rep["tests"], {})
        self.assertEqual(leftovers(), "")

    def test_the_vllm_path_with_stubbed_tools(self):
        # the real engine path -- install script, lmcache server and vllm serve
        # command lines, the install check's output -- against stand-ins:
        # lmcache's arguments go through LMCache's own parser when LMCache is
        # importable; vllm's flags and kv-transfer-config are checked strictly
        venv, pathbin = os.path.join(self.tmp, "venv"), os.path.join(self.tmp, "pathbin")
        os.makedirs(os.path.join(venv, "bin"))
        os.makedirs(pathbin)
        stub = os.path.join(ROOT, "tests", "fakes", "kvenv_stub.py")
        for n in ("pip", "uv", "python", "lmcache", "vllm"):
            os.symlink(stub, os.path.join(venv, "bin", n))
        for n in ("nvidia-smi", "apt-get"):
            os.symlink(stub, os.path.join(pathbin, n))
        stub_log = os.path.join(self.tmp, "stub.jsonl")
        env = dict(os.environ, KVENV_STUB_LOG=stub_log, PATH=pathbin + os.pathsep + os.environ["PATH"])
        argv = self.args(5000) + ["--venv", venv, "--local"]
        argv[argv.index("fake")] = "vllm"
        r = subprocess.run(argv, capture_output=True, text=True, timeout=600, env=env)
        rep = json.load(open(os.path.join(self.tmp, "report.json")))
        calls = [json.loads(l) for l in open(stub_log)]
        self.assertFalse([c for c in calls if c.get("error")], [c for c in calls if c.get("error")])
        self.assertEqual(r.returncode, 0, r.stdout[-3000:] + r.stderr[-2000:])
        self.assertEqual(rep["verdict"], "PASS")
        inst = rep["steps"]["install"]
        self.assertEqual(inst["status"], "PASS")
        self.assertIn("LMCache 0.5.7.post1", inst["detail"])
        self.assertEqual(set(inst["hosts"]), {"A", "B"})
        # install order: LMCache pinned, then vLLM; the model fetched; the check run
        per_host: dict = {}
        for c in calls:
            if c["prog"] == "uv":   # one install shell per host
                per_host.setdefault(c["ppid"], []).append(c["argv"][-1])
        self.assertEqual(sorted(per_host.values()), [["lmcache==0.5.7.post1", "vllm"]] * 2)
        self.assertEqual(sum(1 for c in calls if c["prog"] == "python" and c.get("check")), 2)
        self.assertTrue(any("snapshot_download('Qwen/Qwen2.5-7B-Instruct')" in c.get("code", "") for c in calls))
        lm = [c for c in calls if c["prog"] == "lmcache"]
        self.assertEqual(len(lm), 2)
        self.assertTrue(all(c["l1_lazy"] is False for c in lm), "the whole L1 arena is pinned up front")
        vl = [c for c in calls if c["prog"] == "vllm"]
        self.assertEqual(sorted(c["kvt"]["kv_connector_extra_config"]["lmcache.mp.port"] for c in vl),
                         [6555 + 5000, 6555 + 5100], "each vLLM talks to its own host's LMCache")
        self.assertTrue(all(c["env"] == {"PYTHONHASHSEED": "0", "VLLM_BATCH_INVARIANT": "1"} for c in vl))
        print("lmcache arguments validated by:", lm[0]["validated"])
        self.assertEqual(leftovers(), "")

    def test_vllm_without_batch_invariant_kernels_is_started_again_without(self):
        venv, pathbin = os.path.join(self.tmp, "venv"), os.path.join(self.tmp, "pathbin")
        os.makedirs(os.path.join(venv, "bin"))
        os.makedirs(pathbin)
        stub = os.path.join(ROOT, "tests", "fakes", "kvenv_stub.py")
        for n in ("pip", "uv", "python", "lmcache", "vllm"):
            os.symlink(stub, os.path.join(venv, "bin", n))
        for n in ("nvidia-smi", "apt-get"):
            os.symlink(stub, os.path.join(pathbin, n))
        stub_log = os.path.join(self.tmp, "stub.jsonl")
        env = dict(os.environ, KVENV_STUB_LOG=stub_log, KVENV_STUB_REJECT_BI="1",
                   PATH=pathbin + os.pathsep + os.environ["PATH"])
        argv = self.args(5400, lengths="1024") + ["--venv", venv, "--local"]
        argv[argv.index("fake")] = "vllm"
        r = subprocess.run(argv, capture_output=True, text=True, timeout=600, env=env)
        rep = json.load(open(os.path.join(self.tmp, "report.json")))
        self.assertEqual(r.returncode, 0, r.stdout[-3000:] + r.stderr[-2000:])
        self.assertFalse(rep["steps"]["engines"]["batch_invariant"])
        self.assertIn("without batch-invariant", rep["steps"]["engines"]["detail"])
        calls = [json.loads(l) for l in open(stub_log) if '"vllm"' in l]
        self.assertEqual(sum(1 for c in calls if c.get("rejected_batch_invariant")), 2)
        self.assertEqual(sum(1 for c in calls if c.get("kvt")), 2, "started again on both hosts, without")
        self.assertEqual(leftovers(), "")

    def test_clean_up_never_touches_processes_it_did_not_start(self):
        # a bystander whose command line names the same programs, by other paths
        decoy = subprocess.Popen(["bash", "-c", "exec -a 'vllm serve other-model' sleep 300"])
        decoy2 = subprocess.Popen(["bash", "-c",
                                   "exec -a 'python3 tests/fakes/fake_inference_server.py --port 1 "
                                   "lmcache server crossover_node.py ost-native' sleep 300"])
        try:
            r, rep = self.run_e2e(5800)
            self.assertEqual(rep["verdict"], "PASS", r.stdout[-2000:])
            self.assertIsNone(decoy.poll(), "someone else's vllm survives")
            self.assertIsNone(decoy2.poll(), "a command line that merely mentions the names survives")
        finally:
            decoy.kill()
            decoy2.kill()

    def test_a_run_killed_midway_leaves_its_progress(self):
        p = subprocess.Popen(self.args(4800, lengths="1024,2048,4096"), stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
        prog = os.path.join(self.tmp, "progress.json")
        end, seen = time.time() + 300, None
        while time.time() < end and p.poll() is None:
            try:
                seen = json.load(open(prog))
            except (OSError, ValueError):
                seen = None
            if seen and seen.get("stage") == "T3_ttft":
                break
            time.sleep(0.2)
        self.assertEqual((seen or {}).get("stage"), "T3_ttft", "reached the TTFT test")
        os.killpg(p.pid, signal.SIGKILL)   # the whole run dies, as when the host goes away
        p.wait()
        left = json.load(open(prog))
        self.assertEqual(left["tests"]["T1_correctness"]["status"], "PASS", "finished tests survive")
        self.assertEqual(left["tests"]["T2_cross_server"]["status"], "PASS")
        self.assertEqual({s["status"] for s in left["steps"].values()}, {"PASS"})
        self.assertNotIn("verdict", left, "an unfinished run has no verdict")
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "report.json")))
        # the process group took the run's servers with it (ports 8000+4800, 19950+4800 and +100 for host B)
        subprocess.run(["pkill", "-9", "-f", "[f]ake_inference_server.py --port 1(28|29)00"], check=False)
        subprocess.run(["pkill", "-9", "-f", "[c]rossover_node.py ost-native 127.0.0.1:24(75|85)"], check=False)
        time.sleep(1)
        self.assertEqual(leftovers(), "")


if __name__ == "__main__":
    unittest.main()
