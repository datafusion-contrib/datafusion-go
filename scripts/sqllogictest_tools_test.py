import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

import sql_coverage
import sqllogictest


class SQLLogicToolsTest(unittest.TestCase):
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
                      "skipped": False, "expects_error": True, "kind": "query", "functions": ["abs"]}
            path = reports / "test.slt.json"
            for passed, expects_error, count in [(True, True, 0), (False, False, 0), (True, False, 1)]:
                record.update(passed=passed, expects_error=expects_error)
                path.write_text(json.dumps({"records": [record]}), encoding="utf-8")
                self.assertEqual(sql_coverage.summarize(reports, root, "test")["functions_with_witnesses"], count)


if __name__ == "__main__":
    unittest.main()
