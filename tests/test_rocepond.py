"""rocepond (DuckDB per node, data and shuffle on a shared file system).

The session's task generation is tested with a recording executor (no
DuckDB needed). The end-to-end test -- two workers, TPC-H at SF 0.01, all 11
queries against a single DuckDB running the plan's SQL -- runs where a
DuckDB CLI and a RapidsSE tree (for dbgen) are given:
    ROCEPOND_DUCKDB=/path/to/duckdb RAPIDSSE_TREE=/path/to/RapidsSE
"""
import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import rocepond  # noqa: E402
from rocepond import tpch  # noqa: E402

DUCKDB = os.environ.get("ROCEPOND_DUCKDB") or shutil.which("duckdb") or ""
RAPIDSSE = os.environ.get("RAPIDSSE_TREE", "")


class RecordingExecutor:
    """Stands in for ProcessExecutor: records the tasks, answers them empty."""

    def __init__(self, n):
        self.partition_workers = [f"w{k}" for k in range(n)]
        self.tasks = []

    def run(self, worker, task):
        self.tasks.append((worker, task))
        return {"ok": True, "duck_s": 0.0, "rows": [["1"]], "error": ""}

    def run_all(self, tasks):
        return {w: self.run(w, t) for w, t in tasks.items()}


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.ex = RecordingExecutor(2)
        self.sp = rocepond.Session(self.ex, "/shared/pond")
        self.cols = [("k", "BIGINT"), ("v", "VARCHAR")]

    def sql_of(self, worker):
        return [s for w, t in self.ex.tasks if w == worker for s in t.get("sql", [])]

    def test_repartition_shuffles_through_the_file_system(self):
        df = self.sp.read_csv("t", [["/raw/n1/t.tbl"], ["/raw/n2/t.tbl"]], self.cols).repartition(hash_by="k")
        self.assertEqual(df.kind, "partitioned")
        w0, w1 = self.sql_of("w0"), self.sql_of("w1")
        # each worker reads its own share and writes one file per partition
        self.assertIn("/raw/n1/t.tbl", w0[0])
        self.assertIn("/raw/n2/t.tbl", w1[0])
        for k, sqls in enumerate((w0, w1)):
            copies = [s for s in sqls if s.startswith("COPY")]
            self.assertEqual(len(copies), 2)
            for p, c in enumerate(copies):
                self.assertIn(f"k % 2 = {p}", c)
                self.assertIn(f"'/shared/pond/data/t/p={p}/from-{k}.parquet'", c)
                self.assertIn("USE_TMP_FILE false", c)  # no rename on the target file system
        # worker p then sees partition p under the table's name
        self.assertIn("CREATE VIEW t AS SELECT * FROM read_parquet('/shared/pond/data/t/p=0/*.parquet')", w0)
        self.assertIn("CREATE VIEW t AS SELECT * FROM read_parquet('/shared/pond/data/t/p=1/*.parquet')", w1)
        mk = [d for w, t in self.ex.tasks for d in t.get("mkdirs", [])]
        self.assertIn("/shared/pond/data/t/p=1", mk)

    def test_broadcast_is_one_copy_read_by_all(self):
        self.sp.read_csv("d", [["/raw/n1/d.tbl"], []], self.cols).broadcast()
        copies = [(w, s) for w, t in self.ex.tasks for s in t.get("sql", []) if s.startswith("COPY")]
        self.assertEqual([w for w, _ in copies], ["w0"])  # the worker with no share writes nothing
        for w in ("w0", "w1"):
            self.assertIn("CREATE VIEW d AS SELECT * FROM read_parquet('/shared/pond/data/d/*.parquet')",
                          self.sql_of(w))

    def test_partial_sql_then_merge(self):
        t = self.sp.read_csv("t", [["a"], ["b"]], self.cols).repartition(hash_by="k")
        self.ex.tasks.clear()
        part = self.sp.partial_sql("select v, count(*) as n from {0} group by v", t)
        self.assertEqual(part.kind, "partial")
        self.assertEqual(self.sql_of("w0"), ["COPY (select v, count(*) as n from t group by v) TO "
                                             "'/shared/pond/tmp/t1/part-0.parquet' (FORMAT parquet, "
                                             "USE_TMP_FILE false)"])
        self.assertIn("part-1.parquet", self.sql_of("w1")[0])
        self.ex.tasks.clear()
        rows = self.sp.sql("select v, sum(n) from {0} group by v", part)
        self.assertEqual(rows, [["1"]])
        (w, task), = self.ex.tasks
        self.assertEqual(w, "w0")
        self.assertEqual(task["sql"], ["select v, sum(n) from read_parquet('/shared/pond/tmp/t1/*.parquet') group by v"])
        self.assertTrue(task["fetch"])

    def test_misuse_is_refused(self):
        src = self.sp.read_csv("t", [["a"], ["b"]], self.cols)
        with self.assertRaises(ValueError):  # not placed yet
            self.sp.partial_sql("select * from {0}", src)
        t = src.repartition(hash_by="k")
        with self.assertRaises(ValueError):  # one worker holds one partition only
            self.sp.sql("select count(*) from {0}", t)
        with self.assertRaises(ValueError):
            self.sp.read_csv("u", [["a"]], self.cols)  # a share for every worker

    def test_memory_cache_loads_tables(self):
        self.sp.read_csv("t", [["a"], ["b"]], self.cols).repartition(hash_by="k")
        self.ex.tasks.clear()
        self.sp.set_cache("memory")
        self.assertIn("CREATE TABLE t AS SELECT * FROM read_parquet('/shared/pond/data/t/p=1/*.parquet')",
                      self.sql_of("w1"))
        self.assertEqual(self.sp.full_views(in_memory=True, replacing="VIEW"),
                         ["DROP VIEW t", "CREATE TABLE t AS SELECT * FROM read_parquet('/shared/pond/data/t/*/*.parquet')"])
        # and back: what is there is a table now (DuckDB refuses DROP VIEW on a table)
        self.ex.tasks.clear()
        self.sp.set_cache("none")
        self.assertEqual(self.sql_of("w0"), ["DROP TABLE t", "CREATE VIEW t AS SELECT * FROM read_parquet("
                                             "'/shared/pond/data/t/p=0/*.parquet')"])


class SortPipelineTaskTests(unittest.TestCase):
    """generate -> repartition(P, by=) -> map_partitions: the tasks they send."""

    def setUp(self):
        self.ex = RecordingExecutor(2)
        self.sp = rocepond.Session(self.ex, "/shared/pond")

    def sql_of(self, worker):
        return [s for w, t in self.ex.tasks if w == worker for s in t.get("sql", [])]

    def test_generate_shuffle_map(self):
        g = self.sp.generate("g", "select i as k from range({worker}, 10, {nworkers}) t(i)")
        self.assertEqual(self.sql_of("w1"), ["COPY (select i as k from range(1, 10, 2) t(i)) TO "
                                             "'/shared/pond/data/g/from-1.parquet' (FORMAT parquet, "
                                             "USE_TMP_FILE false, COMPRESSION uncompressed)"])
        self.ex.tasks.clear()
        b = g.repartition(4, by="k % 4", name="b")
        self.assertEqual((b.kind, b.npartitions), ("bucketed", 4))
        # each worker reads its own file once and writes it hive-partitioned (smallpond's shuffle)
        self.assertEqual(self.sql_of("w0"), [
            "COPY (SELECT *, (k % 4) AS bucket FROM read_parquet('/shared/pond/data/g/from-0.parquet')) TO "
            "'/shared/pond/data/b' (FORMAT parquet, PARTITION_BY (bucket), OVERWRITE_OR_IGNORE, "
            "FILENAME_PATTERN 'from-0-{i}', COMPRESSION uncompressed)"])
        self.ex.tasks.clear()
        out = b.map_partitions("SELECT k FROM {0} ORDER BY k", name="o", fmt="lines")
        self.assertEqual((out.kind, out.npartitions), ("files", 4))
        w0, w1 = self.sql_of("w0"), self.sql_of("w1")
        self.assertEqual(len(w0), 2)  # partitions 0 and 2 on worker 0, 1 and 3 on worker 1
        self.assertIn("read_parquet('/shared/pond/data/b/bucket=2/*.parquet')", w0[1])
        self.assertIn("TO '/shared/pond/data/o/part-00003.dat'", w1[1])
        self.assertEqual(self.sp.partition_files(out, 3), "/shared/pond/data/o/part-00003.dat")

    def test_misuse_is_refused(self):
        g = self.sp.generate("g", "select 1 as k")
        with self.assertRaises(ValueError):
            g.repartition(hash_by="k")  # that form is for source tables
        with self.assertRaises(ValueError):
            g.map_partitions("select * from {0}", name="o")  # not partitioned yet
        with self.assertRaises(ValueError):
            g.repartition(4, by="k % 4").map_partitions("select * from {0}", name="o", fmt="csv")

    def test_check_sorted(self):
        from rocepond.worker import check_sorted
        with tempfile.NamedTemporaryFile("wb", delete=False) as f:
            f.write(b"aaa1 x\naaa1 y\naab0 z\n")
        try:
            self.assertEqual(check_sorted(f.name, 4), [3, 21, "aaa1", "aab0", 0])
            with open(f.name, "ab") as g:
                g.write(b"aaa9 late\n")
            self.assertEqual(check_sorted(f.name, 4)[4], 1)
        finally:
            os.unlink(f.name)


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


@unittest.skipUnless(DUCKDB and os.path.isdir(os.path.join(RAPIDSSE, "dbgen")),
                     "set ROCEPOND_DUCKDB (DuckDB CLI) and RAPIDSSE_TREE")
class EndToEndTests(unittest.TestCase):
    """Two workers on this host, a temporary directory as the shared file system."""

    @classmethod
    def setUpClass(cls):
        from tools.rapidsse_tpch import ensure_dbgen
        cls.tmp = tempfile.mkdtemp()
        dbgen = ensure_dbgen(RAPIDSSE)
        gen = os.path.join(ROOT, "tools", "rapidsse_gen.py")
        for n in (1, 2):
            subprocess.run([sys.executable, gen, "--dbgen", dbgen, "--sf", "0.01", "--total", "2", "--first", str(n),
                            "--jobs", "2", "--scratch", f"{cls.tmp}/s{n}", "--paths", f"{cls.tmp}/raw/NODE{n}"],
                           check=True, capture_output=True)

        def launch(node, argv):
            return subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
        cls.ex = rocepond.ProcessExecutor(launch, [0, 1], DUCKDB, threads=1)
        cls.sp = rocepond.Session(cls.ex, f"{cls.tmp}/pond")
        files = {t: [sorted(glob.glob(f"{cls.tmp}/raw/NODE{k}/{t}.tbl")) for k in (1, 2)] for t in tpch.SCHEMA}
        cls.tables = tpch.prepare(cls.sp, files)
        cls.ex.add_worker("ref", 0, 2)
        cls.ex.run("ref", {"sql": cls.sp.full_views()})

    @classmethod
    def tearDownClass(cls):
        cls.ex.close()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def check_all(self):
        from tools.rapidsse_tpch import answers_match
        for q, fn in tpch.QUERIES.items():
            rows = fn(self.sp, self.tables)
            ref = self.ex.run("ref", {"sql": [tpch.REFERENCE_SQL[q]], "fetch": True})["rows"]
            self.assertTrue(rows, q)
            ok, why = answers_match(rows, ref)
            self.assertTrue(ok, f"{q}: {why}")

    def test_the_partitions_split_the_data(self):
        n = [int(self.ex.run(w, {"sql": ["select count(*) from lineitem"], "fetch": True})["rows"][0][0])
             for w in ("w0", "w1")]
        total = int(self.ex.run("ref", {"sql": ["select count(*) from lineitem"], "fetch": True})["rows"][0][0])
        self.assertEqual(sum(n), total)
        self.assertTrue(all(0.3 * total < x < 0.7 * total for x in n), n)
        # co-partitioned: no order's lines are on the other worker
        for w in ("w0", "w1"):
            r = self.ex.run(w, {"sql": ["select count(*) from lineitem where l_orderkey not in "
                                        "(select o_orderkey from orders)"], "fetch": True})["rows"]
            self.assertEqual(r, [["0"]])

    def test_all_queries_equal_the_single_node_answers(self):
        self.check_all()

    def test_all_queries_with_partitions_in_memory(self):
        self.sp.set_cache("memory")
        try:
            self.check_all()
        finally:
            self.sp.set_cache("none")

    def test_a_failing_statement_is_reported_and_the_worker_lives(self):
        with self.assertRaises(rocepond.TaskError):
            self.sp.sql("select no_such_column from nation")
        self.assertEqual(self.sp.sql("select count(*) from nation"), [["25"]])



@unittest.skipUnless(DUCKDB, "set ROCEPOND_DUCKDB (DuckDB CLI)")
class SortEndToEndTests(unittest.TestCase):
    def test_sort_is_valid_and_globally_ordered(self):
        from rocepond import sort
        tmp = tempfile.mkdtemp()

        def launch(node, argv):
            return subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
        ex = rocepond.ProcessExecutor(launch, [0, 1], DUCKDB, threads=1)
        try:
            res = sort.run(rocepond.Session(ex, f"{tmp}/pond"), 20_000_000, 16)
            self.assertTrue(res["valid"], res)
            self.assertEqual(res["records"], 200_000)
            # independently: the files in order are one sorted sequence of 100-byte records
            keys, size = [], 0
            for path in sorted(glob.glob(f"{tmp}/pond/data/sort_output/part-*.dat")):
                for line in open(path, "rb"):
                    self.assertEqual(len(line), 100)
                    keys.append(line[:10])
                    size += len(line)
            self.assertEqual(size, 20_000_000)
            self.assertEqual(keys, sorted(keys))
        finally:
            ex.close()
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
