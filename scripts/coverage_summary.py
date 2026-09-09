#!/usr/bin/env python3
"""Combine child-process Go coverage and report each language separately."""

from collections import defaultdict
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def go_coverage():
    # Test executables for installed consumers have a different coverage
    # metadata file. Merge their blocks explicitly, rather than omitting them
    # from the parent go test profile. Counts are reduced to covered/uncovered.
    blocks = {}
    files = [ROOT / "coverage/go.out", *sorted((ROOT / "coverage/install").glob("*.out"))]
    for path in files:
        for line in path.read_text().splitlines()[1:]:
            location, count, hits = line.split()
            key = (location, int(count))
            blocks[key] = max(blocks.get(key, 0), int(hits) > 0)
    with (ROOT / "coverage/go.out").open("w") as merged:
        merged.write("mode: set\n")
        for (location, count), hits in sorted(blocks.items()):
            merged.write(f"{location} {count} {int(hits)}\n")
    packages = defaultdict(lambda: [0, 0])
    prefix = "github.com/datafusion-contrib/datafusion-go"
    for (location, count), hits in blocks.items():
        path = location.split(":", 1)[0].removeprefix(prefix).lstrip("/")
        if path == "internal/native/test_coverage.go":
            continue  # Instrumentation hook; absent from production builds.
        package = str(Path(path).parent)
        packages[package][0] += count * bool(hits)
        packages[package][1] += count
    return {"Go " + name: values for name, values in packages.items() if not name.startswith("examples/")}


def lcov(path, prefix):
    files = defaultdict(dict)
    current = None
    for line in path.read_text().splitlines():
        if line.startswith("SF:"):
            current = Path(line[3:])
            if not current.is_absolute():
                current = ROOT / current
        elif line.startswith("DA:") and current is not None:
            number, hits, *_ = line[3:].split(",")
            files[current][number] = max(files[current].get(number, 0), int(hits) > 0)
    result = {}
    for path, lines in files.items():
        relative = path.relative_to(ROOT).as_posix()
        if relative.startswith("rust/tests/") or relative.endswith(("generated.rs", "_generated.h")):
            continue
        if lines:
            result[prefix + " " + relative] = [sum(lines.values()), len(lines)]
    return result


def main():
    measured = go_coverage()
    measured.update(lcov(ROOT / "coverage/rust.lcov", "Rust"))
    measured.update(lcov(ROOT / "coverage/c-coverage/c.lcov", "C"))
    floors = json.loads((ROOT / "testdata/coverage_minimums.json").read_text())
    rows = ["| Area | Covered | Total | Coverage | Minimum |", "|---|---:|---:|---:|---:|"]
    failed = []
    for name in sorted(measured):
        covered, total = measured[name]
        percent = covered * 100 / total
        minimum = floors.get(name, 0)
        rows.append(f"| {name} | {covered} | {total} | {percent:.1f}% | {minimum}% |")
        if percent < minimum:
            failed.append(f"{name}: {percent:.1f}% < {minimum}%")
    failed += [f"missing coverage: {name}" for name in floors if name not in measured]
    report = "\n".join(rows) + "\n\nGo measures statements; Rust and C measure executable lines.\n"
    (ROOT / "coverage/summary.md").write_text(report)
    print(report)
    if failed:
        raise SystemExit("\n".join(failed))


if __name__ == "__main__":
    main()
