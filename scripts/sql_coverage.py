"""Inventory pinned SQL documentation and attach passing assertion witnesses.

Function witnesses are parsed call expressions in successful query assertions.
Section witnesses require reviewed mappings; text mentions never count. These
metrics measure examples, not every argument combination or execution path.
"""

import hashlib
import json
from pathlib import Path
import re


def headings(text):
    fenced = False
    for line, value in enumerate(text.splitlines(), 1):
        if value.lstrip().startswith("```"):
            fenced = not fenced
        if not fenced and (match := re.fullmatch(r"(#{1,6}) (.+)", value)):
            yield line, len(match[1]), match[2]


def inventory(directory):
    entries = []
    for path in sorted(directory.glob("*.md")):
        functions = path.stem in {"scalar_functions", "aggregate_functions", "window_functions"}
        seen = {}
        for line, level, title in headings(path.read_text(encoding="utf-8")):
            if functions:
                if level != 3 or not re.fullmatch(r"`[a-z_0-9]+`", title):
                    continue
                kind = "function"
                name = title.strip("`")
            else:
                if level > 3:
                    continue
                kind = "section"
                name = title.replace("`", "")
            anchor = re.sub(r"[^\w -]", "", name.lower()).replace(" ", "-")
            occurrence = seen.get(anchor, 0)
            seen[anchor] = occurrence + 1
            if occurrence:
                anchor += f"-{occurrence}"
            entries.append({"id": f"{path.name}#{anchor}", "kind": kind,
                            "family": path.stem, "name": name, "line": line})
    return entries


def sql_digest(sql):
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def summarize(directory, corpus, commit):
    entries = json.loads((corpus / "coverage.json").read_text(encoding="utf-8"))
    mappings = json.loads((corpus / "coverage-map.json").read_text(encoding="utf-8"))
    known = {entry["id"] for entry in entries}
    if mappings.keys() - known:
        raise ValueError("coverage mapping references an unknown documentation entry")
    records = {}
    functions = {}
    parse_errors = []
    for path in sorted(directory.rglob("*.slt.json")):
        result = json.loads(path.read_text(encoding="utf-8"))
        for record in result.get("records", []):
            location = record["location"].replace("\\", "/")
            witness = {"location": location, "sql_sha256": sql_digest(record["sql"])}
            if record["passed"] and not record["skipped"]:
                records[(location, witness["sql_sha256"])] = record
            # EXPLAIN verifies a plan, and expected errors verify rejection.
            # Neither is evidence of the function's returned value.
            if (not record["passed"] or record["skipped"] or record["expects_error"]
                    or record["kind"] != "query" or re.match(r"\s*EXPLAIN\b", record["sql"], re.I)):
                continue
            if record.get("parse_error"):
                parse_errors.append(witness)
            for name in record["functions"]:
                functions.setdefault(name, witness)
    result = []
    for entry in entries:
        witnesses = []
        if entry["kind"] == "function":
            name = ("window:" if entry["family"] == "window_functions" else "") + entry["name"]
            if name in functions:
                witnesses.append(functions[name])
        for witness in mappings.get(entry["id"], []):
            if (witness["location"], witness["sql_sha256"]) in records:
                witnesses.append(witness)
        result.append(dict(entry, witnesses=witnesses))
    summary = {
        "upstream_commit": commit,
        "metric": "documented function calls and reviewed SQL section witnesses",
        "functions": sum(e["kind"] == "function" for e in result),
        "functions_with_witnesses": sum(e["kind"] == "function" and bool(e["witnesses"]) for e in result),
        "sections": sum(e["kind"] == "section" for e in result),
        "sections_with_witnesses": sum(e["kind"] == "section" and bool(e["witnesses"]) for e in result),
        "missing": [e["id"] for e in result if not e["witnesses"]],
        "unparsed_successful_queries": parse_errors,
        "entries": result,
    }
    (directory / "coverage.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (directory / "coverage.md").write_text(
        f"Documented functions with query witnesses: {summary['functions_with_witnesses']}/{summary['functions']}.\n"
        f"SQL documentation sections with reviewed witnesses: {summary['sections_with_witnesses']}/{summary['sections']}.\n\n"
        "These are example-coverage metrics. A call inside a passing query does not prove all inputs, "
        "overloads, branches, or combinations correct. Sections without mappings remain visible gaps.\n\n"
        + "\n".join(f"- {name}" for name in summary["missing"]) + "\n", encoding="utf-8")
    return summary
