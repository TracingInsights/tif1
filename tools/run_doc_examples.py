#!/usr/bin/env python3
"""Run Python code blocks from the documentation headless and report results.

Executes every fenced Python block per page sequentially in one subprocess per
page (blocks in a page share state, matching the reading order). Blocks that
raise are reported with the exception; a block whose own text announces an
error ("Raises", "Error", "try:") is classified as an error demonstration.

Usage:
    uv run python tools/run_doc_examples.py [paths ...] [--json] [--timeout 300]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

RUNNER = """
import json
import matplotlib
matplotlib.use("Agg")
import sys

blocks = json.loads(sys.argv[1])
ns = {}
results = []
for i, block in enumerate(blocks):
    try:
        exec(compile(block, f"<block {i}>", "exec"), ns)
        results.append({"index": i, "status": "ok"})
    except Exception as exc:  # noqa: BLE001
        results.append({"index": i, "status": "error", "error": f"{type(exc).__name__}: {exc}"})
print(json.dumps(results))
"""


def extract_blocks(page: Path) -> list[str]:
    text = page.read_text(encoding="utf-8").replace("\r\n", "\n")
    blocks: list[str] = []
    current: list[str] = []
    in_fence = False
    lang = ""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(("```", "~~~")):
            if in_fence:
                blocks.append("\n".join(current))
                current = []
                in_fence = False
            else:
                in_fence = True
                lang = stripped.lstrip("`~").strip().lower()
            continue
        if in_fence and lang in {"python", "py"}:
            current.append(line)
    return [textwrap.dedent(b).strip() for b in blocks if b.strip()]


def is_error_demo(block: str) -> bool:
    lowered = block.lower()
    return "raises" in lowered or "try:" in lowered or "except" in lowered or "traceback" in lowered


def is_network_demo(block: str) -> bool:
    return bool(re.search(r"get_session|\.load\(|laps_async|get_fastest|fetch|Session\(", block))


def run_page(page: Path, timeout: int) -> dict[str, object]:
    blocks = extract_blocks(page)
    if not blocks:
        return {"page": page.as_posix(), "blocks": 0, "results": []}
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, encoding="utf-8"
    ) as handle:
        handle.write(RUNNER)
        runner_path = handle.name
    # Doc blocks may call config.set(), which persists to $HOME/.tif1rc and
    # would pollute later pages. Isolate HOME per page, keep the shared data
    # cache, and pin the default backend.
    env = dict(os.environ)
    home = tempfile.mkdtemp(prefix="tif1_docs_")
    env["HOME"] = home
    shared_cache = Path.home() / ".cache" / "tif1"
    env["TIF1_CACHE_DIR"] = str(shared_cache)
    env["TIF1_LIB"] = "pandas"
    try:
        proc = subprocess.run(  # noqa: PLW1510 -- exit code handled below
            [sys.executable, runner_path, json.dumps(blocks)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=REPO_ROOT,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return {"page": page.as_posix(), "blocks": len(blocks), "status": "timeout"}
    finally:
        Path(runner_path).unlink(missing_ok=True)
        shutil.rmtree(home, ignore_errors=True)
    if proc.returncode != 0:
        return {
            "page": page.as_posix(),
            "blocks": len(blocks),
            "status": "crashed",
            "error": proc.stderr[-2000:],
        }
    try:
        results = json.loads(proc.stdout.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        return {
            "page": page.as_posix(),
            "blocks": len(blocks),
            "status": "crashed",
            "error": proc.stdout[-2000:] + proc.stderr[-2000:],
        }
    for i, block in enumerate(blocks):
        for row in results:
            if row["index"] == i and row["status"] == "error":
                row["error_demo"] = is_error_demo(block)
                row["network"] = is_network_demo(block)
    return {"page": page.as_posix(), "blocks": len(blocks), "results": results}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="*", type=Path, help="mdx files or directories")
    parser.add_argument("--json", action="store_true", help="print JSON report")
    parser.add_argument("--timeout", type=int, default=420, help="seconds per page")
    args = parser.parse_args(argv)

    targets = args.paths or [
        REPO_ROOT / "docs/tutorials",
        REPO_ROOT / "docs/guides",
        REPO_ROOT / "docs/quickstart.mdx",
        REPO_ROOT / "docs/getting-started.mdx",
        REPO_ROOT / "docs/examples.mdx",
        REPO_ROOT / "docs/installation.mdx",
        REPO_ROOT / "docs/introduction.mdx",
    ]
    pages: list[Path] = []
    for target in targets:
        if target.is_dir():
            pages.extend(sorted(target.rglob("*.mdx")))
        elif target.is_file():
            pages.append(target)

    report = []
    for page in pages:
        row = run_page(page, args.timeout)
        report.append(row)
        failures = [
            r
            for r in row.get("results", [])
            if r.get("status") == "error" and not r.get("error_demo")
        ]
        status = row.get("status", "ok")
        print(
            f"{page.as_posix()}: blocks={row['blocks']} status={status} failures={len(failures)}",
            flush=True,
        )
        for f in failures:
            print(f"    block {f['index']}: {f['error']}", flush=True)

    if args.json:
        print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
