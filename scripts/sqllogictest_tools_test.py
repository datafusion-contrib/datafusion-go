import io
import json
from pathlib import Path
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import sql_coverage
import sqllogictest


class SQLLogicToolsTest(unittest.TestCase):
    def test_spill_diagnostic_reports_failures_without_changing_corpus_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            reports = Path(temporary)
            corpus_summary = reports / "summary.json"
            corpus_summary.write_text('{"complete": true}\n', encoding="utf-8")
            calls = []

            def execute(command, **kwargs):
                calls.append((command, kwargs["env"]))
                kwargs["stdout"].write("diagnostic output\n")
                return SimpleNamespace(returncode=101 if len(calls) == 2 else 0)

            with patch.object(sqllogictest, "check", return_value={"source": {"commit": "pinned"}}), \
                    patch.object(sqllogictest.subprocess, "check_output", return_value="head\n"), \
                    patch.object(sqllogictest.subprocess, "run", side_effect=execute), \
                    patch.dict(sqllogictest.os.environ, {"DFGO_SQLLOGICTEST_ASYNC_COLLECT": "1"}):
                result = sqllogictest.spill(SimpleNamespace(reports=reports, target_dir=Path("target")))

            self.assertEqual(result, 1)
            self.assertEqual(corpus_summary.read_text(encoding="utf-8"), '{"complete": true}\n')
            summary = json.loads((reports / "spill/summary.json").read_text(encoding="utf-8"))
            self.assertTrue(summary["complete"])
            self.assertEqual(summary["results"], [
                {"mode": "go", "exit_code": 0},
                {"mode": "native-blocking", "exit_code": 101},
                {"mode": "native-async", "exit_code": 0},
            ])
            self.assertIn("-count=25", calls[0][0])
            self.assertEqual(calls[0][1]["DFGO_SQLLOGICTEST_SPILL_DIAGNOSTIC"], "1")
            self.assertNotIn("DFGO_SQLLOGICTEST_ASYNC_COLLECT", calls[1][1])
            self.assertEqual(calls[2][1]["DFGO_SQLLOGICTEST_ASYNC_COLLECT"], "1")
            for mode in ["go", "native-blocking", "native-async"]:
                self.assertEqual((reports / "spill" / (mode + ".log")).read_text(encoding="utf-8"),
                                 "diagnostic output\n")

    def test_native_comparison_rejects_stale_or_unknown_inputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            reports = Path(temporary)
            lock = sqllogictest.check()
            summary = {"upstream_commit": lock["source"]["commit"], "failed": ["arrow_typeof.slt"],
                       "run": {"driver_manifest_sha256": sqllogictest.digest(sqllogictest.DRIVER_LOCK)}}
            cases = [
                (dict(summary, upstream_commit="stale"), "pinned DataFusion version"),
                (dict(summary, run={"driver_manifest_sha256": "stale"}), "same driver SQL fixtures"),
                (dict(summary, failed=["../../unknown.slt"]), "unknown SQL files"),
            ]
            with patch.object(sqllogictest, "prepare") as prepare:
                for report, error in cases:
                    (reports / "summary.json").write_text(json.dumps(report), encoding="utf-8")
                    with self.assertRaisesRegex(ValueError, error):
                        sqllogictest.oracle(SimpleNamespace(reports=reports))
                prepare.assert_not_called()

    def test_archive_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "input.tar"
            with tarfile.open(archive, "w") as output:
                entry = tarfile.TarInfo("repo/../../escape")
                entry.size = 1
                output.addfile(entry, io.BytesIO(b"x"))
            with self.assertRaises(tarfile.TarError):
                sqllogictest.extract(archive, root / "destination")
            self.assertFalse((root / "escape").exists())

    def test_inventory_ignores_code_headings_but_keeps_function_aliases(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "scalar_functions.md").write_text(
                "# Functions\n### `abs`\n#### Example\n```sql\n### `not_a_function`\n```\n### `power`\n### `pow`\n", encoding="utf-8")
            self.assertEqual([e["name"] for e in sql_coverage.inventory(root)], ["abs", "power", "pow"])

    def test_missing_or_inconsistent_reports_cannot_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "coverage.json").write_text("[]", encoding="utf-8")
            (root / "coverage-map.json").write_text("{}", encoding="utf-8")
            reports = root / "reports"
            reports.mkdir()
            lock = {"files": {"a.slt": "hash", "b.slt": "hash"}, "source": {"commit": "test"},
                    "comment_only_files": [], "datafusion_version": "test"}
            original = sqllogictest.CORPUS
            self.addCleanup(setattr, sqllogictest, "CORPUS", original)
            sqllogictest.CORPUS = root
            original_driver = sqllogictest.DRIVER_LOCK
            self.addCleanup(setattr, sqllogictest, "DRIVER_LOCK", original_driver)
            sqllogictest.DRIVER_LOCK = root / "driver.json"
            sqllogictest.DRIVER_LOCK.write_text("{}", encoding="utf-8")
            report = {"file": "a.slt", "statements": 0, "queries": 1, "eligible": 1,
                      "executed": 1, "passed": 1, "skipped": [], "errors": []}
            (reports / "a.slt.json").write_text(json.dumps(report), encoding="utf-8")
            self.assertFalse(sqllogictest.report(reports, lock)["complete"])
            report.update(file="b.slt", passed=0)
            (reports / "b.slt.json").write_text(json.dumps(report), encoding="utf-8")
            self.assertFalse(sqllogictest.report(reports, lock)["complete"])
            report.update(passed=1)
            (reports / "b.slt.json").write_text(json.dumps(report), encoding="utf-8")
            self.assertTrue(sqllogictest.report(reports, lock)["complete"])

    def test_expected_errors_and_failed_queries_are_not_function_witnesses(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "coverage.json").write_text(json.dumps([
                {"id": "scalar_functions.md#abs", "kind": "function", "family": "scalar_functions", "name": "abs"}
            ]), encoding="utf-8")
            (root / "coverage-map.json").write_text("{}", encoding="utf-8")
            reports = root / "reports"
            reports.mkdir()
            record = {"location": "test.slt:1", "sql": "SELECT abs(1)", "passed": True,
                      "skipped": False, "expects_error": True, "kind": "query", "functions": ["abs"],
                      "query_statement": True, "returned_rows": 1}
            path = reports / "test.slt.json"
            for passed, expects_error, query_statement, rows, count in [
                (True, True, True, 1, 0), (False, False, True, 1, 0),
                (True, False, False, 1, 0), (True, False, True, 0, 0),
                (True, False, True, 1, 1),
            ]:
                record.update(passed=passed, expects_error=expects_error, query_statement=query_statement, returned_rows=rows)
                path.write_text(json.dumps({"records": [record]}), encoding="utf-8")
                self.assertEqual(sql_coverage.summarize(reports, root, "test")["functions_with_witnesses"], count)
            path.write_text(json.dumps({"file": "spark/example.slt", "records": [record]}), encoding="utf-8")
            self.assertEqual(sql_coverage.summarize(reports, root, "test")["functions_with_witnesses"], 0)
            record.update(expects_error=True, parse_error="invalid SQL")
            path.write_text(json.dumps({"records": [record]}), encoding="utf-8")
            self.assertEqual(sql_coverage.summarize(reports, root, "test")["unparsed_successful_queries"], [])
            # A reviewed section mapping must not bypass function evidence rules.
            (root / "coverage-map.json").write_text(json.dumps({"scalar_functions.md#abs": [
                {"location": record["location"], "sql_sha256": sql_coverage.sql_digest(record["sql"])}
            ]}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "only allowed for SQL sections"):
                sql_coverage.summarize(reports, root, "test")


if __name__ == "__main__":
    unittest.main()
