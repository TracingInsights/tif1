#!/usr/bin/env python3
"""Verify that fenced code blocks in documentation files are unchanged.

Phase 3+ of DOCS_STE100_PLAN.md rewrites prose only. Every fenced code block
(``` or ~~~) must stay byte-identical to the git HEAD version. This tool
extracts the fences from both versions and compares them.

Usage:
    python3 tools/check_code_blocks.py docs/guides/ docs/concepts/data-flow.mdx
    python3 tools/check_code_blocks.py --ref HEAD~1 docs/
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

FENCE_PREFIXES = ("```", "~~~")


def extract_fences(text: str) -> list[str]:
    """Return the exact text of every fenced block, fences included."""
    blocks: list[str] = []
    current: list[str] | None = None
    for line in text.splitlines(keepends=True):
        stripped = line.lstrip()
        if any(stripped.startswith(prefix) for prefix in FENCE_PREFIXES):
            if current is None:
                current = [line]
            else:
                current.append(line)
                blocks.append("".join(current))
                current = None
        elif current is not None:
            current.append(line)
    if current is not None:
        blocks.append("".join(current))
    return blocks


def git_show(ref: str, path: str) -> str:
    result = subprocess.run(["git", "show", f"{ref}:{path}"], capture_output=True, check=True)
    return result.stdout.decode("utf-8")


def iter_files(paths: list[str]) -> list[Path]:
    files: list[Path] = []
    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            files.extend(sorted(p for p in path.rglob("*") if p.suffix in {".md", ".mdx"}))
        elif path.suffix in {".md", ".mdx"}:
            files.append(path)
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="+", help="files or directories to check")
    parser.add_argument("--ref", default="HEAD", help="git ref to compare against")
    args = parser.parse_args(argv)

    failures = 0
    for path in iter_files(args.paths):
        try:
            old = git_show(args.ref, path.as_posix())
        except subprocess.CalledProcessError:
            print(f"SKIP {path}: not present at {args.ref}")
            continue
        new = path.read_bytes().decode("utf-8")
        old_blocks, new_blocks = extract_fences(old), extract_fences(new)
        if old_blocks == new_blocks:
            print(f"OK   {path}: {len(new_blocks)} fenced blocks identical")
            continue
        failures += 1
        print(f"DIFF {path}: {len(old_blocks)} blocks at {args.ref} vs {len(new_blocks)} now")
        for index, (a, b) in enumerate(zip(old_blocks, new_blocks)):
            if a != b:
                print(f"  first differing block #{index}:")
                print(f"    {args.ref}: {a[:100]!r}")
                print(f"    now:     {b[:100]!r}")
                break
    verdict = "PASS" if not failures else f"FAIL: {failures} file(s) changed fenced blocks"
    print(verdict)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
