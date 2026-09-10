#!/usr/bin/env python3
"""Run the C consumer with native instrumentation; never replaces release assets."""

import argparse
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess


def tool(name):
    override = os.environ.get("DFGO_" + name.upper().replace("-", "_"))
    if override:
        return shlex.split(override)
    if shutil.which(name):
        return [name]
    if platform.system() == "Darwin":
        return ["xcrun", name]
    raise SystemExit(f"{name} is required; install matching Clang/LLVM tools")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["asan", "coverage", "plain"])
    parser.add_argument("library", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    output = root / "coverage" / ("c-" + args.mode)
    output.mkdir(parents=True, exist_ok=True)
    exe = output / "abi-smoke"
    flags = ["-std=c11", "-g", "-O1", "-Wall", "-Wextra", "-Werror", "-Wno-unused-function"]
    if args.mode == "asan":
        flags += ["-fsanitize=address,undefined", "-fno-omit-frame-pointer"]
    elif args.mode == "coverage":
        flags += ["-fprofile-instr-generate", "-fcoverage-mapping"]
    if platform.system() == "Linux":
        flags += ["-ldl"]
    subprocess.run(tool("clang") + flags + [
        "-I" + str(root / "rust/include"), "-I" + str(root / "internal/native"),
        str(root / "rust/tests/abi_smoke.c"), "-o", str(exe),
    ], check=True)
    env = dict(os.environ)
    env["LLVM_PROFILE_FILE"] = str(output / "abi.profraw")
    # Apple's Clang runtime does not support LeakSanitizer. Linux CI runs the
    # leak detector; both platforms also run the explicit allocation checks.
    leaks = "0" if platform.system() == "Darwin" else "1"
    env.setdefault("ASAN_OPTIONS", f"detect_leaks={leaks}:halt_on_error=1")
    env.setdefault("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
    subprocess.run([str(exe), str(args.library.resolve(strict=True))], check=True, env=env)
    if args.mode == "coverage":
        profile = output / "abi.profdata"
        subprocess.run(tool("llvm-profdata") + ["merge", "-sparse", str(output / "abi.profraw"), "-o", str(profile)], check=True)
        common = [str(exe), "-instr-profile=" + str(profile)]
        subprocess.run(tool("llvm-cov") + ["report"] + common, check=True)
        with (output / "c.lcov").open("w") as report:
            subprocess.run(tool("llvm-cov") + ["export", "-format=lcov"] + common, check=True, stdout=report)
        subprocess.run(tool("llvm-cov") + ["show", "-format=html", "-output-dir=" + str(output / "html")] + common, check=True)


if __name__ == "__main__":
    main()
