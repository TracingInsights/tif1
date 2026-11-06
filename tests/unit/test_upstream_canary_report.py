"""Offline checks of the upstream canary's issue report (``tools/upstream_canary_report.py``)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_TOOLS = Path(__file__).resolve().parents[2] / "tools"


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, _TOOLS / filename)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


report = _load("tif1_upstream_canary_report", "upstream_canary_report.py")
prereleases = _load("tif1_upstream_prereleases", "upstream_prereleases.py")

_JUNIT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" tests="3" failures="1" errors="1">
<testcase classname="tests.unit.test_private_seams" name="test_pandas_lap_rows" time="0.1">
<failure message="AssertionError: private row API missing (pandas 3.2.0rc0 installed)">trace</failure>
</testcase>
<testcase classname="tests.unit.test_private_seams" name="test_polars_pydf_wrap" time="0.1">
<error message="AttributeError: _from_pydf">trace</error>
</testcase>
<testcase classname="tests.unit.test_private_seams" name="test_qh3_hazmat_preload" time="0.1"/>
</testsuite></testsuites>
"""


def test_report_names_the_failing_seams(tmp_path):
    junit = tmp_path / "seams.xml"
    junit.write_text(_JUNIT, encoding="utf-8")
    versions = tmp_path / "versions.txt"
    versions.write_text("pandas               3.2.0rc0\n", encoding="utf-8")
    out = tmp_path / "report.md"
    assert (
        report.main(
            [
                "--versions",
                str(versions),
                "--junit",
                f"seams={junit}",
                "--junit",
                f"unit={tmp_path / 'missing.xml'}",
                "--outcome",
                "seams=failure",
                "--outcome",
                "unit=success",
                "--run-url",
                "https://example.invalid/run/1",
                "--out",
                str(out),
            ]
        )
        == 0
    )
    text = out.read_text(encoding="utf-8")
    assert "pandas               3.2.0rc0" in text
    assert "- `seams`: failure" in text
    assert "- `unit`: success" in text
    assert "## Failing tests: seams (2)" in text
    assert "tests/unit/test_private_seams.py::test_pandas_lap_rows" in text
    assert "private row API missing" in text
    assert "tests/unit/test_private_seams.py::test_polars_pydf_wrap" in text
    assert "test_qh3_hazmat_preload" not in text
    assert "https://example.invalid/run/1" in text


def test_report_caps_the_listed_failures(tmp_path):
    cases = "".join(
        f'<testcase classname="tests.unit.test_x" name="test_{i}"><failure message="m{i}"/>'
        "</testcase>"
        for i in range(report.MAX_LISTED + 5)
    )
    junit = tmp_path / "unit.xml"
    junit.write_text(f"<testsuites><testsuite>{cases}</testsuite></testsuites>", encoding="utf-8")
    text = report.build_report("v", {"unit": junit}, {"unit": "failure"}, "")
    assert f"## Failing tests: unit ({report.MAX_LISTED + 5})" in text
    assert "... and 5 more" in text
    assert "Run:" not in text


def test_prerelease_listing_covers_the_canary_packages():
    workflow = (_TOOLS.parent / ".github" / "workflows" / "upstream-canary.yml").read_text(
        encoding="utf-8"
    )
    line = next(row for row in workflow.splitlines() if "CANARY_PACKAGES:" in row)
    assert line.split(":", 1)[1].strip().strip('"').split() == list(prereleases.PACKAGES)
