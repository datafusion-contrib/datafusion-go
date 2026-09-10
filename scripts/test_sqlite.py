#!/usr/bin/env python3
"""Check the explicitly portable corpus subset with an independent SQL engine.

SQLite supplies an additional values/column-names oracle. Arrow schemas and
DataFusion-specific semantics are checked by the Go and native Rust runners.
"""

import json
from pathlib import Path
import sqlite3


def parameter(spec):
    if spec["type"] == "int64":
        return int(spec["value"])
    if spec["type"] == "string":
        return spec["value"]
    if spec["type"] == "null_int64":
        return None
    raise ValueError(f"unsupported SQLite parameter: {spec}")


def main():
    corpus = Path(__file__).resolve().parent.parent / "testdata" / "conformance"
    checked = 0
    for path in sorted(corpus.glob("*.json")):
        for case in json.loads(path.read_text()):
            if not case.get("sqlite", False):
                continue
            checked += 1
            with sqlite3.connect(":memory:") as conn:
                for sql in case.get("setup", []):
                    conn.execute(sql)
                params = case.get("parameters", [])
                if any(p.get("name") for p in params):
                    values = {p["name"]: parameter(p) for p in params}
                else:
                    values = [parameter(p) for p in params]
                try:
                    result = conn.execute(case["sql"], values)
                except sqlite3.Error as error:
                    assert case.get("error") and case["error"] in str(error), (case["name"], error)
                    continue
                assert not case.get("error"), (case["name"], "expected error")
                names = [column[0] for column in result.description]
                assert names == [column["name"] for column in case["columns"]], case["name"]
                rows = [[None if v is None else str(v) for v in row] for row in result.fetchall()]
                assert rows == case["rows"], (case["name"], rows, case["rows"])
    assert checked, "no portable SQLite cases"
    print(f"SQLite oracle: {checked} cases passed (SQLite {sqlite3.sqlite_version})")


if __name__ == "__main__":
    main()
