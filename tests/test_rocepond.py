"""rocepond (DuckDB tasks scheduled over a cluster, data and shuffle on a
shared file system).

- the session's task generation: a recording executor, nothing needed
- the schedulers (thousands of tasks, retries, dead workers, time limits) and
  the pipelines end to end, on this host with a temporary directory as the
  shared file system:
    ProcessExecutor  needs DuckDB: the `duckdb` Python package, or a CLI in
                     ROCEPOND_DUCKDB / on PATH
    RayExecutor      needs `pip install ray duckdb`
- the 11 TPC-H queries against the plan's SQL: also RAPIDSSE_TREE (for dbgen)
"""
import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import rocepond  # noqa: E402
from rocepond import sort, tpch  # noqa: E402
from rocepond.duck import DuckError, have_module  # noqa: E402
from rocepond.executor import Executor  # noqa: E402
from rocepond.task import check_sorted, execute  # noqa: E402

DUCKDB = os.environ.get("ROCEPOND_DUCKDB") or shutil.which("duckdb") or ""
RAPIDSSE = os.environ.get("RAPIDSSE_TREE", "")
HAVE_DUCK = bool(DUCKDB) or have_module()
try:
    import ray  # noqa: F401
    HAVE_RAY = have_module()
except ImportError:
    HAVE_RAY = False


class RecordingExecutor(Executor):
    """Records the tasks, answers them empty."""
    parallelism = 4

    def __init__(self, rows=None):
        super().__init__()
        self.stages, self.rows = [], rows or [["1"]]

    def run_all(self, tasks, stage=""):
        self.stages.append((stage, tasks))
        return [{"ok": True, "duck_s": 0.0, "wall_s": 0.0, "rows": self.rows, "error": ""} for _ in tasks]


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.ex = RecordingExecutor()
        self.sp = rocepond.Session(self.ex, "/shared/pond")
        self.cols = [("k", "BIGINT"), ("v", "VARCHAR")]

    def last(self):
        return self.ex.stages[-1][1]

    def test_one_task_per_partition_and_tasks_name_their_files(self):
        df = self.sp.read_parquet(["/in/a/*.parquet", ["/in/b/1.parquet", "/in/b/2.parquet"], "/in/c.parquet"])
        self.assertEqual(df.npartitions, 3)
        out = df.map_partitions("SELECT k, count(*) FROM {0} GROUP BY k", name="counts")
        tasks = self.last()
        self.assertEqual(len(tasks), 3)
        self.assertIn("read_parquet(['/in/b/1.parquet', '/in/b/2.parquet']", tasks[1]["sql"][-1])
        self.assertIn("TO '/shared/pond/data/counts/part-00001.parquet' (FORMAT parquet, USE_TMP_FILE false)",
                      tasks[1]["sql"][-1])
        self.assertEqual(out.partition_file(2), "/shared/pond/data/counts/part-00002.parquet")
        # no task depends on which worker runs it: no worker state, no worker names
        self.assertTrue(all(set(t) <= {"name", "mkdirs", "sql"} for t in tasks))

    def test_repartition_is_a_partitioned_write_per_input_partition(self):
        self.ex.rows = [["0"], ["2"]]  # the listing: partitions 0 and 2 got rows
        src = self.sp.read_csv("t", [["/in/t.1"], [], ["/in/t.2"]], self.cols)
        self.assertEqual(src.npartitions, 2)
        df = src.repartition(4, hash_by="k", name="t_by_k")
        (stage, shuffle), (_, listing) = self.ex.stages
        self.assertEqual(len(shuffle), 2)
        sql = shuffle[1]["sql"][0]
        self.assertIn("(hash(k) % 4)::INTEGER AS bucket", sql)
        self.assertIn("read_csv(['/in/t.2']", sql)
        self.assertIn("TO '/shared/pond/data/t_by_k' (FORMAT parquet, PARTITION_BY (bucket), OVERWRITE_OR_IGNORE, "
                      "FILENAME_PATTERN 'from-1-{i}'", sql)
        # a retry first removes what the failed attempt wrote
        self.assertEqual(shuffle[1]["cleanup_globs"], ["/shared/pond/data/t_by_k/bucket=*/from-1-*.parquet"])
        self.assertEqual(len(listing), 1)
        self.assertEqual([p.files for p in df.parts], [
            ["/shared/pond/data/t_by_k/bucket=0/*.parquet"], ["/shared/pond/data/t_by_k/_empty.parquet"],
            ["/shared/pond/data/t_by_k/bucket=2/*.parquet"], ["/shared/pond/data/t_by_k/_empty.parquet"]])

    def test_partition_sql_joins_partition_i_with_partition_i_and_broadcast_whole(self):
        self.ex.rows = [[str(i)] for i in range(8)]
        a = self.sp.read_parquet(["/a/0", "/a/1"]).repartition(8, hash_by="k")
        b = self.sp.read_parquet(["/b/0"]).repartition(8, hash_by="k")
        small = self.sp.read_parquet(["/s/0", "/s/1"]).broadcast().as_table("small")
        part = self.sp.partition_sql("SELECT * FROM {0} JOIN {1} USING (k) JOIN {2} USING (k) JOIN small s2 USING (k)",
                                     a, b, small)
        tasks = self.last()
        self.assertEqual(len(tasks), 8)
        q = tasks[5]["sql"]
        self.assertEqual(q[0], "CREATE OR REPLACE TEMP VIEW small AS SELECT * FROM "
                               "read_parquet(['/s/0', '/s/1'], hive_partitioning=false)")
        self.assertIn("/tmp/shuffle1/bucket=5/*.parquet", q[1])
        self.assertIn("/tmp/shuffle2/bucket=5/*.parquet", q[1])
        self.assertIn("read_parquet(['/s/0', '/s/1']", q[1])
        self.assertEqual(part.npartitions, 8)
        self.sp.sql("SELECT count(*) FROM {0}", part)
        self.assertEqual(len(self.last()), 1)
        self.assertIn("/tmp/t3/part-*.parquet", self.last()[0]["sql"][0])
        self.assertTrue(self.last()[0]["fetch"])

    def test_generate_and_lines(self):
        g = self.sp.generate("g", "SELECT {split} AS s, {nsplits} AS n", splits=3)
        self.assertEqual(len(self.last()), 3)
        self.assertIn("COPY (SELECT 2 AS s, 3 AS n) TO '/shared/pond/data/g/part-00002.parquet'",
                      self.last()[2]["sql"][0])
        out = g.map_partitions("SELECT s FROM {0}", name="o", fmt="lines")
        self.assertIn("TO '/shared/pond/data/o/part-00000.dat' (FORMAT csv", self.last()[0]["sql"][0])
        self.assertIn("columns={'line': 'VARCHAR'}", out.whole)

    def test_misuse_is_refused(self):
        a = self.sp.read_parquet(["/a/0", "/a/1"])
        b = self.sp.read_parquet(["/b/0", "/b/1", "/b/2"])
        with self.assertRaises(ValueError):
            self.sp.partition_sql("SELECT * FROM {0}, {1}", a, b)  # different partition counts
        with self.assertRaises(ValueError):
            a.repartition(4)
        with self.assertRaises(ValueError):
            a.repartition(4, hash_by="k", by="k % 4")
        with self.assertRaises(ValueError):
            a.map_partitions("SELECT 1", fmt="csv")
        self.ex.rows = [["7"]]
        with self.assertRaises(ValueError):
            a.repartition(4, by="k")  # a partition number outside 0..3


class FakeEngine:
    def __init__(self):
        self.seen = []

    def run(self, sql):
        self.seen.append(sql)
        if "boom" in sql:
            raise DuckError("boom")
        return 0.25, [["x"]]


class TaskTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_check_sorted(self):
        path = f"{self.tmp}/f"
        with open(path, "wb") as f:
            f.write(b"aaa1 x\naaa1 y\naab0 z\n")
        self.assertEqual(check_sorted(path, 4), [3, 21, "aaa1", "aab0", 0])
        with open(path, "ab") as g:
            g.write(b"aaa9 late\n")
        self.assertEqual(check_sorted(path, 4)[4], 1)

    def test_execute(self):
        r = execute({"mkdirs": [f"{self.tmp}/d/e"], "sql": ["a", "b"], "fetch": True}, FakeEngine())
        self.assertTrue(r["ok"] and os.path.isdir(f"{self.tmp}/d/e"))
        self.assertEqual((r["rows"], r["duck_s"]), ([["x"]], 0.5))
        self.assertEqual(execute({"sql": ["a"]}, FakeEngine())["rows"], [])
        r = execute({"sql": ["a", "boom", "c"]}, e := FakeEngine())
        self.assertFalse(r["ok"])
        self.assertIn("boom", r["error"])
        self.assertEqual(e.seen, ["a", "boom"])

    def test_a_retry_removes_the_failed_attempts_files(self):
        os.makedirs(f"{self.tmp}/bucket=1")
        stale, other = f"{self.tmp}/bucket=1/from-3-0.parquet", f"{self.tmp}/bucket=1/from-4-0.parquet"
        for p in (stale, other):
            open(p, "w").close()
        task = {"cleanup_globs": [f"{self.tmp}/bucket=*/from-3-*.parquet"], "sql": []}
        execute(task, FakeEngine())
        self.assertTrue(os.path.exists(stale))
        execute(dict(task, attempt=1), FakeEngine())
        self.assertFalse(os.path.exists(stale))
        self.assertTrue(os.path.exists(other))


class SchedulerCases:
    """The guarantees of rocepond/executor.py, for every executor. make(**kw) -> executor with 4 workers."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_five_thousand_tasks(self):
        ex = self.make()
        self.addCleanup(ex.close)
        n = 5000
        res = ex.run_all([{"sql": [f"SELECT {i}, {i} * 2"], "fetch": True} for i in range(n)], "many")
        self.assertEqual([r["rows"] for r in res], [[[str(i), str(2 * i)]] for i in range(n)])  # in the tasks' order
        workers = {r["slot"] for r in res}
        self.assertGreaterEqual(len(workers), 2, workers)  # spread over the workers
        st = ex.stats[-1]
        self.assertEqual((st["stage"], st["tasks"], st["retries"]), ("many", n, 0))
        print(f"\n    {type(ex).__name__}: {n} tasks in {st['wall_s']} s = {st['tasks_per_s']} tasks/s "
              f"on {ex.parallelism} workers", file=sys.stderr)

    def test_a_failed_task_and_a_dead_worker_are_retried(self):
        ex = self.make()
        self.addCleanup(ex.close)
        tasks = [{"sql": [f"SELECT {i}"], "fetch": True} for i in range(200)]
        for i in (3, 77, 150):
            tasks[i]["_fail_once"] = f"{self.tmp}/fail{i}"
        for i in (20, 120):
            tasks[i]["_die_once"] = f"{self.tmp}/die{i}"  # the worker process exits in the middle of the task
        res = ex.run_all(tasks)
        self.assertEqual([r["rows"][0][0] for r in res], [str(i) for i in range(200)])
        self.assertEqual({i: res[i]["attempts"] for i in (3, 20, 77, 120, 150)}, {3: 2, 20: 2, 77: 2, 120: 2, 150: 2})
        self.assertGreaterEqual(ex.stats[-1]["retries"], 5)
        # and the executor is whole again
        self.assertEqual(len(ex.run_all([{"sql": ["SELECT 1"], "fetch": True}] * 50)), 50)

    def test_a_task_that_always_fails_fails_the_call_with_every_attempt(self):
        ex = self.make(max_retries=2)
        self.addCleanup(ex.close)
        tasks = [{"sql": ["SELECT 1"], "_sleep": 0.01} for _ in range(300)]
        tasks[5] = {"sql": ["SELECT no_such_column"], "name": "the bad one"}
        t0 = time.monotonic()
        with self.assertRaises(rocepond.TaskError) as cm:
            ex.run_all(tasks)
        msg = str(cm.exception)
        self.assertIn("task failed 3 times: the bad one", msg)
        self.assertIn("attempt 3", msg)
        self.assertIn("no_such_column", msg)
        self.assertLess(time.monotonic() - t0, 60)
        self.assertEqual(ex.run({"sql": ["SELECT 41 + 1"], "fetch": True})["rows"], [["42"]])

    def test_a_task_over_its_time_limit_is_stopped_and_retried(self):
        ex = self.make(max_retries=1, task_timeout=3)
        self.addCleanup(ex.close)
        with self.assertRaises(rocepond.TaskError) as cm:
            ex.run_all([{"sql": ["SELECT 1"]}, {"sql": ["SELECT 1"], "_sleep": 600, "name": "stuck"}])
        self.assertIn("task failed 2 times: stuck", str(cm.exception))
        self.assertIn("time limit", str(cm.exception))
        self.assertEqual(ex.run({"sql": ["SELECT 7"], "fetch": True})["rows"], [["7"]])


class PipelineCases:
    """The DataFrame operations end to end. make(**kw) as above."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.ex = self.make()
        self.addCleanup(self.ex.close)
        self.sp = rocepond.Session(self.ex, f"{self.tmp}/pond")

    def test_shuffle_join_merge(self):
        sp = self.sp
        orders = sp.generate("orders", "SELECT i AS id, i % 97 AS cust, i % 5 AS kind FROM range("
                             "{split} * 2000, ({split} + 1) * 2000) t(i)", splits=5)
        items = sp.generate("items", "SELECT i % 10000 AS id, i AS qty FROM range("
                            "{split} * 10000, ({split} + 1) * 10000) t(i)", splits=3)
        kinds = sp.generate("kinds", "SELECT i AS kind, 'kind-' || i AS label FROM range(5) t(i)", splits=1).broadcast().as_table("kinds")
        self.assertEqual((orders.count(), items.count()), (10000, 30000))
        o, i = orders.repartition(64, hash_by="id"), items.repartition(64, hash_by="id")
        self.assertEqual((o.npartitions, o.count(), i.count()), (64, 10000, 30000))
        # co-partitioned: an order's items are in the order's partition
        stray = sp.partition_sql("SELECT count(*) AS n FROM {0} WHERE id NOT IN (SELECT id FROM {1})", i, o)
        self.assertEqual(sp.sql("SELECT sum(n) FROM {0}", stray), [["0"]])
        part = sp.partition_sql("SELECT label, count(*) AS n, sum(qty)::BIGINT AS q FROM {0} JOIN {1} USING (id) "
                                "JOIN kinds USING (kind) GROUP BY label", o, i)
        got = sp.sql("SELECT label, sum(n)::BIGINT, sum(q)::BIGINT FROM {0} GROUP BY label ORDER BY label", part)
        want = sp.sql("SELECT label, count(*), sum(qty)::BIGINT FROM {0} JOIN {1} USING (id) JOIN {2} USING (kind) "
                      "GROUP BY label ORDER BY label", orders, items, kinds)
        self.assertEqual(got, want)
        self.assertEqual(len(got), 5)
        self.assertEqual(len(part.take(3)), 3)
        sp.drop_tmp()
        self.assertFalse(os.path.exists(f"{self.tmp}/pond/tmp"))

    def test_partitions_without_rows(self):
        df = self.sp.generate("few", "SELECT i AS k, 'v' AS v FROM range(3) t(i)", splits=1)
        parts = df.repartition(50, by="k * 10")
        self.assertEqual(parts.count(), 3)
        counts = self.sp.sql("SELECT sum(n) FROM {0}", parts.map_partitions("SELECT count(*) AS n, min(v) AS v FROM {0}"))
        self.assertEqual(counts, [["3"]])
        none = df.map_partitions("SELECT * FROM {0} WHERE k > 100").repartition(4, hash_by="k")
        self.assertEqual((none.count(), self.sp.sql("SELECT count(*) FROM {0}", none)), (0, [["0"]]))

    def test_sort_with_a_thousand_partitions_and_failures_on_the_way(self):
        res = sort.run(self.sp, 20_000_000, partitions=1024, splits=6)
        self.assertTrue(res["valid"], res)
        self.assertEqual((res["records"], res["tasks"]["partition_sql -> sort_output"]), (200_000, 1024))
        # independently: the files in order are one sorted sequence of 100-byte records
        keys, size = [], 0
        files = sorted(glob.glob(f"{self.tmp}/pond/data/sort_output/part-*.dat"))
        self.assertEqual(len(files), 1024)
        for path in files:
            with open(path, "rb") as f:
                for line in f:
                    self.assertEqual(len(line), 100)
                    keys.append(line[:10])
                    size += len(line)
        self.assertEqual(size, 20_000_000)
        self.assertEqual(keys, sorted(keys))

    def test_two_pass_shuffle_gives_the_same_partitions(self):
        df = self.sp.generate("n", "SELECT i AS k, i * 3 AS v FROM range({split} * 3000, ({split} + 1) * 3000) t(i)",
                              splits=3)
        one = df.repartition(200, hash_by="k")
        two = df.repartition(200, hash_by="k", max_fanout=16)  # 13 groups, then 200 partitions
        self.assertEqual([s["tasks"] for s in self.sp.stats[-5:]], [3, 1, 13, 1, 1])
        self.assertEqual(two.count(), 9000)
        diff = self.sp.partition_sql("SELECT count(*) AS n FROM (SELECT * FROM {0} EXCEPT SELECT * FROM {1})", one, two)
        self.assertEqual(self.sp.sql("SELECT sum(n), count(*) FROM {0}", diff), [["0", "200"]])
        self.assertFalse(glob.glob(f"{self.tmp}/pond/tmp/*/_pass1"))

    def test_a_shuffle_task_that_dies_after_writing_leaves_no_duplicates(self):
        sp = self.sp
        df = sp.generate("d", "SELECT i AS k FROM range({split} * 5000, ({split} + 1) * 5000) t(i)", splits=4)
        real = self.ex.run_all

        def failing_after_the_write(tasks, stage=""):
            if stage.startswith("repartition d ->"):
                # the task writes its files, then fails: the retry must not add them a second time
                tasks[2]["sql"].append("SELECT no_such_column")
                self.ex.run_all = real
                with self.assertRaises(rocepond.TaskError):
                    real(tasks, stage)
                tasks[2]["sql"].pop()
                return real([dict(t, attempt=1) if n == 2 else t for n, t in enumerate(tasks)], stage)
            return real(tasks, stage)
        self.ex.run_all = failing_after_the_write
        parts = df.repartition(16, hash_by="k")
        self.assertEqual(parts.count(), 20000)
        self.assertEqual(sp.sql("SELECT count(DISTINCT k), count(*) FROM {0}", parts), [["20000", "20000"]])


def local_process(**kw):
    return rocepond.local_executor(4, duckdb=DUCKDB, **kw)


@unittest.skipUnless(HAVE_DUCK, "needs DuckDB: pip install duckdb, or ROCEPOND_DUCKDB=<CLI>")
class ProcessSchedulerTests(SchedulerCases, unittest.TestCase):
    make = staticmethod(local_process)


@unittest.skipUnless(HAVE_DUCK, "needs DuckDB: pip install duckdb, or ROCEPOND_DUCKDB=<CLI>")
class ProcessPipelineTests(PipelineCases, unittest.TestCase):
    make = staticmethod(local_process)


@unittest.skipUnless(DUCKDB and have_module(), "needs both the duckdb package and a CLI (ROCEPOND_DUCKDB)")
class CliEngineTests(unittest.TestCase):
    """Where both engines exist: the CLI engine gives the package's answers."""

    def test_same_rows(self):
        from rocepond.duck import Duck, DuckModule
        sql = ("SELECT 1, 'a b', NULL, 1.50::DECIMAL(9,2), DATE '2024-02-29', true, 1e-3::DOUBLE "
               "UNION ALL SELECT 2, '', NULL, -0.25, DATE '1999-12-31', false, 2.5")
        a, b = Duck(DUCKDB, 1), DuckModule(1)
        try:
            self.assertEqual(a.run(sql)[1], b.run(sql)[1])
        finally:
            a.close()
            b.close()


@unittest.skipUnless(HAVE_RAY, "needs pip install ray duckdb")
class RayTests:
    @classmethod
    def setUpClass(cls):
        import ray
        if not ray.is_initialized():
            ray.init(num_cpus=4, include_dashboard=False,
                     runtime_env={"py_modules": [os.path.join(ROOT, "rocepond")]})

    @staticmethod
    def make(**kw):
        return rocepond.RayExecutor(**kw)


class RaySchedulerTests(RayTests, SchedulerCases, unittest.TestCase):
    def test_ray_options_reach_the_tasks(self):
        ex = rocepond.RayExecutor(threads=2, memory=64 << 20, resources=None, scheduling_strategy="SPREAD")
        self.assertEqual(ex.parallelism, 2)  # 4 CPUs, 2 per task
        res = ex.run_all([{"sql": ["SELECT current_setting('threads')"], "fetch": True}] * 8)
        self.assertEqual({r["rows"][0][0] for r in res}, {"2"})
        self.assertEqual(ex.info["scheduler"], "rocepond.RayExecutor")


class RayPipelineTests(RayTests, PipelineCases, unittest.TestCase):
    pass


@unittest.skipUnless(os.path.isdir(os.path.join(RAPIDSSE, "bamboo")), "set RAPIDSSE_TREE to a RapidsSE tree")
class ReferenceSqlTests(unittest.TestCase):
    def test_reference_sql_is_the_plans_text(self):
        from tools.rapidsse_jmx import parse
        plan = parse(os.path.join(RAPIDSSE, "bamboo/perftest/configs/tests/tpch-sf1-2D2F.jmx"))
        text = {s.name: s.sql for s in plan.steps if re.match(r"^q\d+$", s.name)}

        def norm(sql):
            return re.sub(r"\s+", "", sql.strip().rstrip(";")).lower()
        self.assertEqual(sorted(text), sorted(tpch.REFERENCE_SQL))
        self.assertEqual(sorted(text), sorted(tpch.QUERIES))
        for q in text:
            self.assertEqual(norm(tpch.REFERENCE_SQL[q]), norm(text[q]), q)


@unittest.skipUnless(HAVE_DUCK and os.path.isdir(os.path.join(RAPIDSSE, "dbgen")),
                     "needs DuckDB and RAPIDSSE_TREE (for dbgen)")
class TpchEndToEndTests(unittest.TestCase):
    """TPC-H at SF 0.01 in 8 partitions: all 11 queries against one DuckDB task running the plan's SQL."""

    def test_all_queries_equal_the_plans_sql(self):
        from tools.rapidsse_tpch import answers_match, ensure_dbgen
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        dbgen = ensure_dbgen(RAPIDSSE)
        gen = os.path.join(ROOT, "tools", "rapidsse_gen.py")
        for n in (1, 2):
            subprocess.run([sys.executable, gen, "--dbgen", dbgen, "--sf", "0.01", "--total", "2", "--first", str(n),
                            "--jobs", "2", "--scratch", f"{tmp}/s{n}", "--paths", f"{tmp}/raw/NODE{n}"],
                           check=True, capture_output=True)
        ex = local_process()
        self.addCleanup(ex.close)
        sp = rocepond.Session(ex, f"{tmp}/pond")
        files = {t: [sorted(glob.glob(f"{tmp}/raw/NODE{k}/{t}.tbl")) for k in (1, 2)] for t in tpch.SCHEMA}
        tables = tpch.prepare(sp, files, partitions=8)
        self.assertEqual(tables.lineitem.npartitions, 8)
        for q, fn in tpch.QUERIES.items():
            rows = fn(sp, tables)
            ref = sp.sql(tpch.REFERENCE_SQL[q])
            self.assertTrue(rows, q)
            ok, why = answers_match(rows, ref)
            self.assertTrue(ok, f"{q}: {why}")
        with self.assertRaises(rocepond.TaskError):
            sp.sql("select no_such_column from nation")
        self.assertEqual(sp.sql("select count(*) from nation"), [["25"]])


if __name__ == "__main__":
    unittest.main()
