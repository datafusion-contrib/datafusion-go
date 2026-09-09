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
        parents = []
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
            if not re.search(r"\w", name):
                anchor = "operator-" + name.encode("utf-8").hex()
            occurrence = seen.get(anchor, 0)
            seen[anchor] = occurrence + 1
            if occurrence:
                anchor += f"-{occurrence}"
            while parents and parents[-1][0] >= level:
                parents.pop()
            identifier = f"{path.name}#{anchor}"
            entries.append({"id": identifier, "kind": kind,
                            "family": path.stem, "name": name, "line": line,
                            "parent": parents[-1][1] if parents else None})
            parents.append((level, identifier))
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
    operators = {}
    clauses = {}
    parse_errors = []
    for path in sorted(directory.rglob("*.slt.json")):
        result = json.loads(path.read_text(encoding="utf-8"))
        # Spark installs a different function registry. It belongs in corpus
        # execution totals, but cannot prove the default SQL guide's behavior.
        if result.get("file", "").startswith("spark/"):
            continue
        for record in result.get("records", []):
            location = record["location"].replace("\\", "/")
            witness = {"location": location, "sql_sha256": sql_digest(record["sql"])}
            if record["passed"] and not record["skipped"]:
                records[(location, witness["sql_sha256"])] = record
            # EXPLAIN verifies a plan, and expected errors verify rejection.
            # Neither is evidence of the function's returned value.
            if record["passed"] and record["kind"] == "query" and record.get("parse_error"):
                parse_errors.append(witness)
            if (not record["passed"] or record["skipped"] or record["expects_error"]
                    or record["kind"] != "query" or not record.get("query_statement")
                    or record.get("returned_rows", 0) == 0):
                continue
            for name in record["functions"]:
                functions.setdefault(name, witness)
            for name in record.get("operators", []):
                operators.setdefault(name, witness)
            for name in record.get("clauses", []):
                clauses.setdefault(name, witness)
    result = []
    for entry in entries:
        witnesses = []
        if entry["kind"] == "function":
            name = ("window:" if entry["family"] == "window_functions" else "") + entry["name"]
            if name in functions:
                witnesses.append(functions[name])
        elif entry["family"] == "operators" and entry["name"] in operators:
            witnesses.append(operators[entry["name"]])
        elif entry["id"] in clauses:
            witnesses.append(clauses[entry["id"]])
        for witness in mappings.get(entry["id"], []):
            if (witness["location"], witness["sql_sha256"]) in records:
                witnesses.append(witness)
        result.append(dict(entry, witnesses=witnesses))
    # A grouping heading can be accounted for by all of its documented child
    # sections. Preserve the child identifiers so this is visible in the report.
    children = {}
    for entry in result:
        if entry.get("parent"):
            children.setdefault(entry["parent"], []).append(entry)
    for entry in reversed(result):
        descendants = children.get(entry["id"], [])
        if (not entry["witnesses"] and descendants
                and all(child["witnesses"] for child in descendants)):
            entry["covered_by_children"] = [child["id"] for child in descendants]
            entry["witnesses"] = [w for child in descendants for w in child["witnesses"]]
    summary = {
        "upstream_commit": commit,
        "metric": "documented functions, operators, clauses, and reviewed SQL section witnesses",
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
        f"SQL documentation sections with witnesses: {summary['sections_with_witnesses']}/{summary['sections']}.\n\n"
        "These are example-coverage metrics. A call inside a passing query does not prove all inputs, "
        "overloads, branches, or combinations correct. Sections without mappings remain visible gaps.\n\n"
        + "\n".join(f"- {name}" for name in summary["missing"]) + "\n", encoding="utf-8")
    return summary
