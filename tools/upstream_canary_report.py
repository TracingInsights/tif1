"""Markdown body of the ``upstream-drift`` issue the upstream canary opens or updates.

Reads the installed versions (``tools/upstream_prereleases.py --installed``
output), the JUnit XML reports of the canary's pytest steps and the step
outcomes, and writes one report naming the failing tests (the private-seam
tests carry the seam's name)::

    python tools/upstream_canary_report.py --versions canary-versions.txt \
        --junit seams=canary-seams.xml --junit unit=canary-unit.xml \
        --junit perf_gate=canary-perf.xml --outcome seams=failure ... --out report.md
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

#: Failing tests listed per step before the rest are counted only.
MAX_LISTED = 40


def _failures(junit: Path) -> list[tuple[str, str]]:
    """``(test id, first line of the failure message)`` per failed or errored test."""
    if not junit.is_file():
        return []
    root = ET.parse(junit).getroot()
    failed: list[tuple[str, str]] = []
    for case in root.iter("testcase"):
        for tag in ("failure", "error"):
            node = case.find(tag)
            if node is None:
                continue
            classname = case.get("classname", "")
            test_id = f"{classname.replace('.', '/')}.py::{case.get('name', '')}"
            message = (node.get("message") or node.text or "").strip().splitlines()
            failed.append((test_id, message[0][:200] if message else ""))
            break
    return failed


def _pairs(values: list[str]) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for value in values:
        key, _, rest = value.partition("=")
        pairs[key] = rest
    return pairs


def build_report(
    versions: str, junits: dict[str, Path], outcomes: dict[str, str], run_url: str
) -> str:
    """The issue body."""
    lines = [
        "The weekly upstream canary ran tif1's tests against the newest releases and",
        "pre-releases of the packages whose private API it relies on, and something failed.",
        "A failing `tests/unit/test_private_seams.py` test names the seam that drifted",
        "(its `SEAMS` entry says where it is used and what the fallback costs).",
        "",
    ]
    if run_url:
        lines += [f"Run: {run_url}", ""]
    lines += ["## Installed versions", "", "```", versions.strip(), "```", "", "## Steps", ""]
    lines += [f"- `{step}`: {outcome}" for step, outcome in outcomes.items()]
    for step, junit in junits.items():
        failed = _failures(junit)
        if not failed:
            continue
        lines += ["", f"## Failing tests: {step} ({len(failed)})", ""]
        lines += [f"- `{test_id}`: {message}" for test_id, message in failed[:MAX_LISTED]]
        if len(failed) > MAX_LISTED:
            lines.append(f"- ... and {len(failed) - MAX_LISTED} more")
    lines += [
        "",
        "Close this issue once the seams are re-verified (update `SEAMS` and the lock) or",
        "the upstream change is reverted; the next green canary run closes it too.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Write the report; the exit status is 0 either way."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--versions", type=Path, required=True)
    parser.add_argument("--junit", action="append", default=[], help="step=path.xml")
    parser.add_argument("--outcome", action="append", default=[], help="step=outcome")
    parser.add_argument("--run-url", default="")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    versions = args.versions.read_text(encoding="utf-8") if args.versions.is_file() else "(unknown)"
    junits = {step: Path(path) for step, path in _pairs(args.junit).items()}
    report = build_report(versions, junits, _pairs(args.outcome), args.run_url)
    args.out.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
