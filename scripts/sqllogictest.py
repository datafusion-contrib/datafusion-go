#!/usr/bin/env python3
"""Pin, verify, prepare, and run the complete upstream DataFusion SQL corpus."""

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import urllib.request

import sql_coverage


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "testdata" / "sqllogictest"
CACHE = ROOT / ".cache" / "sqllogictest"
LOCK = CORPUS / "upstream.json"
DRIVER_LOCK = CORPUS / "driver.json"


def digest(path):
    with path.open("rb") as data:
        return hashlib.file_digest(data, "sha256").hexdigest()


def version():
    with (ROOT / "versions.toml").open("rb") as data:
        return tomllib.load(data)["datafusion"]["version"]


def inventory(directory):
    return {
        path.relative_to(directory).as_posix(): digest(path)
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    }


def comment_only_files(directory):
    return [
        path.relative_to(directory).as_posix()
        for path in sorted(directory.rglob("*.slt"))
        if all(not line.strip() or line.lstrip().startswith("#") for line in path.read_text(encoding="utf-8").splitlines())
    ]


def check():
    lock = json.loads(LOCK.read_text(encoding="utf-8"))
    if lock["datafusion_version"] != version():
        raise ValueError("SQL corpus version differs from versions.toml; run make sqllogic.sync")
    actual = inventory(CORPUS / "test_files")
    if actual != lock["files"]:
        missing = sorted(lock["files"].keys() - actual.keys())
        extra = sorted(actual.keys() - lock["files"].keys())
        changed = sorted(k for k in actual.keys() & lock["files"].keys() if actual[k] != lock["files"][k])
        raise ValueError(f"upstream corpus changed: missing={missing}, extra={extra}, modified={changed}")
    if not any(name.endswith(".slt") for name in actual):
        raise ValueError("upstream corpus is empty")
    if comment_only_files(CORPUS / "test_files") != lock["comment_only_files"]:
        raise ValueError("the inventory of upstream comment-only files changed")
    if inventory(CORPUS / "sql") != lock["documentation"]:
        raise ValueError("pinned SQL documentation changed; run make sqllogic.sync")
    if sql_coverage.inventory(CORPUS / "sql") != json.loads((CORPUS / "coverage.json").read_text(encoding="utf-8")):
        raise ValueError("SQL coverage inventory differs from the pinned documentation")
    if inventory(CORPUS / "driver") != json.loads(DRIVER_LOCK.read_text(encoding="utf-8")):
        raise ValueError("driver SQL fixtures changed; review them and run make sqllogic.driver.sync")
    return lock


def download(repository, commit, expected=None):
    directory = CACHE / "archives"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{repository.replace('/', '-')}-{commit}.tar.gz"
    if not path.exists() or (expected is not None and digest(path) != expected):
        url = f"https://codeload.github.com/{repository}/tar.gz/{commit}"
        print(f"Fetching {repository}@{commit}", file=sys.stderr)
        with tempfile.NamedTemporaryFile(dir=directory, delete=False) as output:
            temporary = Path(output.name)
            try:
                with urllib.request.urlopen(url, timeout=120) as response:
                    shutil.copyfileobj(response, output)
                output.close()
                if expected is not None and digest(temporary) != expected:
                    raise ValueError(f"archive checksum mismatch: {repository}@{commit}")
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)
    return path


def extract(archive, destination):
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive) as bundle:
        # GitHub snapshots have one containing directory. Fixtures need regular
        # files only; omit repository license symlinks and never follow links.
        for member in bundle.getmembers():
            if not member.isfile():
                continue
            parts = member.name.split("/", 1)
            if len(parts) != 2:
                raise ValueError(f"unexpected archive member: {member.name}")
            member.name = parts[1]
            bundle.extract(member, destination, filter="data")


def prepare():
    lock = check()
    root = CACHE / "source" / lock["source"]["commit"]
    sources = [dict(lock["source"], path="."), *lock["datasets"]]

    def materialize(source):
        archive = download(source["repository"], source["commit"], source["sha256"])
        extract(archive, root / source["path"])

    # Extract the parent first; it contains empty submodule directories.
    materialize(sources[0])
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(materialize, sources[1:]))
    tests = root / "datafusion" / "sqllogictest" / "test_files"
    # Each invocation starts from the reviewed fixtures. This removes scratch
    # output and prevents extra cached .slt files from changing the denominator.
    shutil.rmtree(tests)
    shutil.copytree(CORPUS / "test_files", tests)
    if inventory(tests) != lock["files"]:
        raise ValueError("prepared SQL corpus differs from the reviewed manifest")
    shutil.copytree(prepare_tpch(), tests / "tpch" / "data", dirs_exist_ok=True)
    shutil.copytree(CORPUS / "driver", tests / "_driver")
    return root


def prepare_tpch():
    specification = json.loads((CORPUS / "tpch.json").read_text(encoding="utf-8"))
    data = CACHE / "tpch-data"

    def valid():
        return all((data / name).is_file() and digest(data / name) == expected["sha256"]
                   for name, expected in specification["files"].items())

    if valid():
        return data
    archive = download(specification["repository"], specification["commit"], specification["sha256"])
    source = CACHE / "tpch-generator" / specification["commit"]
    extract(archive, source)
    generator = source / specification["directory"]
    windows = os.name == "nt"
    executable = "dbgen.exe" if windows else "dbgen"
    machine = "WIN32" if windows else "LINUX"
    # dbgen's alphanumeric RNG intentionally relies on signed 32-bit wrapping.
    # Make that behavior explicit so Clang and GCC produce the reference data.
    # Its old-style function declarations also require pre-C23 semantics.
    flags = (f"-std=gnu99 -O2 -fwrapv -D{machine} -DORACLE -DTPCH -DRNG_TEST -D_FILE_OFFSET_BITS=64 "
             "-D_POSIX_SOURCE -D_POSIX_C_SOURCE=200809L")
    subprocess.run(["make", "-B", executable, "CFLAGS=" + flags, "EXE=" + (".exe" if windows else "")],
                   cwd=generator, check=True)
    data.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(generator / executable), "-f", "-s", specification["scale"]], cwd=generator,
                   env=dict(os.environ, DSS_PATH=str(data)), check=True)
    if windows:
        # dbgen uses C text-mode output. Canonical LF files keep snapshots,
        # partition byte ranges and checksums identical on every platform.
        for name in specification["files"]:
            path = data / name
            temporary = path.with_suffix(".tmp")
            with path.open("r", encoding="ascii") as source_file, temporary.open("w", encoding="ascii", newline="\n") as output:
                shutil.copyfileobj(source_file, output)
            temporary.replace(path)
    if not valid():
        raise ValueError("generated TPC-H data does not match the pinned reference checksums")
    return data


def github(path):
    request = urllib.request.Request(
        "https://api.github.com/" + path,
        headers={"Accept": "application/vnd.github+json", "User-Agent": "datafusion-go-sqllogictest"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def sync():
    release = version()
    obj = github(f"repos/apache/datafusion/git/ref/tags/{release}")["object"]
    while obj["type"] == "tag":
        obj = github(f"repos/apache/datafusion/git/tags/{obj['sha']}")["object"]
    if obj["type"] != "commit":
        raise ValueError(f"unexpected release tag object: {obj}")
    commit = obj["sha"]
    archive = download("apache/datafusion", commit)
    tree = github(f"repos/apache/datafusion/git/trees/{commit}")["tree"]
    repositories = {
        "datafusion-testing": "apache/datafusion-testing",
        "parquet-testing": "apache/parquet-testing",
        "testing": "apache/arrow-testing",
    }
    datasets = []
    for path, repository in repositories.items():
        entry = next(e for e in tree if e["path"] == path and e["type"] == "commit")
        data = download(repository, entry["sha"])
        datasets.append({"path": path, "repository": repository, "commit": entry["sha"],
                         "sha256": digest(data), "bytes": data.stat().st_size})
    with tempfile.TemporaryDirectory(prefix="dfgo-slt-sync-") as directory:
        source = Path(directory)
        extract(archive, source)
        files = source / "datafusion" / "sqllogictest" / "test_files"
        manifest = inventory(files)
        comment_only = comment_only_files(files)
        if not any(name.endswith(".slt") for name in manifest):
            raise ValueError("release contains no SQLLogicTest files")
        CORPUS.mkdir(parents=True, exist_ok=True)
        if (CORPUS / "test_files").exists():
            shutil.rmtree(CORPUS / "test_files")
        shutil.copytree(files, CORPUS / "test_files")
        for name in ["LICENSE.txt", "NOTICE.txt"]:
            shutil.copyfile(source / name, CORPUS / name)
        docs = CORPUS / "sql"
        if docs.exists():
            shutil.rmtree(docs)
        shutil.copytree(source / "docs/source/user-guide/sql", docs)
        documentation = inventory(docs)
        (CORPUS / "coverage.json").write_text(json.dumps(sql_coverage.inventory(docs), indent=2) + "\n", encoding="utf-8")
    lock = {
        "generated": "Pinned upstream corpus; update with scripts/sqllogictest.py sync. Version is derived from versions.toml.",
        "datafusion_version": release,
        "source": {"repository": "apache/datafusion", "commit": commit, "sha256": digest(archive)},
        "datasets": datasets,
        "comment_only_files": comment_only,
        "files": manifest,
        "documentation": documentation,
    }
    LOCK.write_text(json.dumps(lock, indent=2) + "\n")
    print(f"Pinned {len(manifest)} upstream files for DataFusion {release}")


def report(directory, lock):
    upstream = {name for name in lock["files"] if name.endswith(".slt")}
    driver = {"_driver/" + name for name in json.loads(DRIVER_LOCK.read_text(encoding="utf-8")) if name.endswith(".slt")}
    expected = upstream | driver
    results = [json.loads(p.read_text(encoding="utf-8")) for p in directory.rglob("*.slt.json")]
    actual = {r["file"] for r in results}
    missing = sorted(expected - actual)
    failed = sorted(r["file"] for r in results if r["errors"] or r["passed"] != r["eligible"] or r["executed"] != r["eligible"])
    if len(actual) != len(results) or actual - expected:
        raise ValueError("duplicate or unexpected SQLLogicTest report files")
    coverage = sql_coverage.summarize(directory, CORPUS, lock["source"]["commit"])
    metadata_path = directory / "run.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else None
    summary = {
        "datafusion_version": lock["datafusion_version"],
        "upstream_commit": lock["source"]["commit"],
        "total_files": len(expected),
        "reported_files": len(results),
        "passing_files": sum(r["file"] not in failed and r["eligible"] > 0 for r in results),
        "upstream_files": len(upstream),
        "driver_files": len(driver),
        "executable_files": len(expected) - len(lock["comment_only_files"]),
        "comment_only_files": lock["comment_only_files"],
        "statements": sum(r["statements"] for r in results),
        "queries": sum(r["queries"] for r in results),
        "executed": sum(r["executed"] for r in results),
        "eligible": sum(r["eligible"] for r in results),
        "passed": sum(r["passed"] for r in results),
        "upstream_conditional_records": sum(len(r["skipped"]) for r in results),
        "missing": missing,
        "failed": failed,
        "complete": actual == expected and not failed,
        "documented_function_witnesses": coverage["functions_with_witnesses"],
        "documented_functions": coverage["functions"],
        "documentation_gaps": len(coverage["missing"]),
        "sql_surface_complete": actual == expected and not failed and not coverage["missing"],
        "run": metadata,
    }
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    text = (
        f"DataFusion {lock['datafusion_version']}: {summary['passing_files']}/{summary['executable_files']} executable SQL files passed "
        f"({len(expected)} files inventoried, {len(lock['comment_only_files'])} contain comments only); "
        f"{len(failed)} failed, {len(missing)} missing; {summary['executed']} SQL executions.\n"
    )
    (directory / "summary.md").write_text(text)
    print(text, end="")
    return summary


def run(arguments):
    source = prepare()
    lock = check()
    directory = arguments.reports.resolve()
    report_root = (CACHE / "reports").resolve()
    if directory == report_root or not directory.is_relative_to(report_root):
        raise ValueError("run reports must use a subdirectory of .cache/sqllogictest/reports")
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True)
    module = json.loads(subprocess.check_output(
        ["go", "list", "-m", "-json", "github.com/apache/arrow-go/v18"], cwd=ROOT, text=True))
    replacement = module.get("Replace")
    metadata = {
        "arrow_go_version": module["Version"],
        "arrow_go_replace": {k: replacement[k] for k in ["Path", "Version"] if k in replacement} if replacement else None,
        "tokio_worker_threads": os.environ.get("TOKIO_WORKER_THREADS", "runtime default"),
        "native_library_sha256": digest(Path(os.environ["DATAFUSION_GO_LIBRARY"])),
    }
    (directory / "run.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    environment = dict(os.environ, DFGO_SQLLOGICTEST_SOURCE=str(source),
                       DFGO_SQLLOGICTEST_REPORT=str(directory),
                       ARROW_TEST_DATA=str(source / "testing" / "data"),
                       PARQUET_TEST_DATA=str(source / "parquet-testing" / "data"),
                       DATAFUSION_TEST_DATA=str(source / "datafusion-testing" / "data"))
    command = ["go", "test", "-count=1", "-tags=datafusion_test_sqllogic", "-timeout=90m", "-v",
               "-run=" + arguments.run, "."]
    result = subprocess.run(command, cwd=ROOT, env=environment, check=False)
    summary = report(directory, lock)
    # Filtered runs are useful while fixing a case, but the report always shows
    # the full denominator and cannot claim complete coverage.
    return result.returncode or (0 if summary["complete"] or arguments.run != "^TestSQLLogic$" else 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["check", "sync", "sync-driver", "prepare", "run", "report"])
    parser.add_argument("--run", default="^TestSQLLogic$", help="Go subtest filter for local iteration")
    parser.add_argument("--reports", type=Path, default=CACHE / "reports" / "current")
    arguments = parser.parse_args()
    if arguments.command == "check":
        lock = check()
        print(f"Verified {len(lock['files'])} upstream files for DataFusion {version()}")
    elif arguments.command == "sync":
        sync()
    elif arguments.command == "sync-driver":
        DRIVER_LOCK.write_text(json.dumps(inventory(CORPUS / "driver"), indent=2) + "\n", encoding="utf-8")
    elif arguments.command == "prepare":
        print(prepare())
    elif arguments.command == "report":
        return 0 if report(arguments.reports, check())["complete"] else 1
    else:
        return run(arguments)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, tarfile.TarError) as error:
        sys.exit(str(error))
