"""Offline checks of the perf gate's comparison, digest diff and bundle format.

The gate itself (``tests/perf_gate``, marker ``perf_gate``) spawns fresh
interpreters and runs in its own CI job; these tests cover its pure logic on
every unit run.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_PERF_DIR = Path(__file__).resolve().parents[2] / "tools" / "perf"


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, _PERF_DIR / filename)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolve their module by name
    spec.loader.exec_module(module)
    return module


gate = _load("tif1_perf_gate", "perf_gate.py")
fixture = _load("tif1_perf_gate_fixture", "perf_gate_fixture.py")

BASE = {"pandas": {"laps": {"import": 10.0, "get_session": 300.0, "laps": 4.0, "total": 320.0}}}


def _measured(scale: float, **overrides: float) -> dict:
    phases = {k: v * scale for k, v in BASE["pandas"]["laps"].items()}
    phases.update(overrides)
    return {"pandas": {"laps": phases}}


def test_same_timings_pass_with_unit_factor() -> None:
    comparison = gate.compare(BASE, _measured(1.0))
    assert comparison.factor == pytest.approx(1.0)
    assert comparison.ok
    assert len(comparison.results) == 4


def test_slower_machine_scales_every_phase() -> None:
    comparison = gate.compare(BASE, _measured(1.8))
    assert comparison.factor == pytest.approx(1.8)
    assert comparison.ok


def test_one_slow_phase_regresses() -> None:
    comparison = gate.compare(BASE, _measured(1.0, laps=54.0))
    assert comparison.factor == pytest.approx(1.0)
    assert [(r.workload, r.phase) for r in comparison.regressions] == [("laps", "laps")]
    table = gate.format_table(comparison, only_regressions=True)
    assert "REGRESSED" in table
    assert "laps" in table


def test_small_phase_needs_the_absolute_floor() -> None:
    # 4 ms -> 8 ms is 2x but only 4 ms over: under the 5 ms floor.
    assert gate.compare(BASE, _measured(1.0, laps=8.0)).ok
    assert not gate.compare(BASE, _measured(1.0, laps=9.5)).ok
    assert gate.compare(BASE, _measured(1.0, laps=9.5), abs_floor_ms=10.0).ok


def test_relative_tolerance_on_long_phases() -> None:
    assert gate.compare(BASE, _measured(1.0, get_session=440.0)).ok
    assert not gate.compare(BASE, _measured(1.0, get_session=460.0)).ok
    assert gate.compare(BASE, _measured(1.0, get_session=460.0), tolerance=2.0).ok


def test_explicit_factor_overrides_the_median() -> None:
    comparison = gate.compare(BASE, _measured(2.0), factor=1.0)
    assert {r.phase for r in comparison.regressions} == {"import", "get_session", "total"}


def test_factor_ignores_sub_floor_phases_and_is_clamped() -> None:
    base = {"pandas": {"w": {"a": 10.0, "b": 10.0, "c": 10.0, "tiny": 0.1}}}
    measured = {"pandas": {"w": {"a": 12.0, "b": 12.0, "c": 12.0, "tiny": 5.0}}}
    assert gate.machine_factor(base, measured) == pytest.approx(1.2)
    huge = {"pandas": {"w": {"a": 100.0, "b": 100.0, "c": 100.0}}}
    assert gate.machine_factor(base, huge) == pytest.approx(gate._FACTOR_CLAMP)
    assert gate.machine_factor({}, measured) == 1.0


def test_limit_starts_from_the_baseline_median() -> None:
    # A wide phase (best 10 ms, median 18 ms) gets its spread; the factor stays min/min.
    base = {"polars": {"w": {"wide": {"min": 10.0, "median": 18.0}, "steady": 100.0}}}
    measured = {"polars": {"w": {"wide": 25.0, "steady": 100.0}}}
    comparison = gate.compare(base, measured)
    assert comparison.factor == pytest.approx(1.75)  # median of 2.5 and 1.0
    assert comparison.ok
    assert gate.compare(base, measured, factor=1.0).ok  # 25 < max(18 * 1.5, 18 + 5)
    measured["polars"]["w"]["wide"] = 28.0
    assert not gate.compare(base, measured, factor=1.0).ok


def test_baseline_document_holds_min_and_median() -> None:
    run = gate.GateRun()
    run.samples = {"pandas": {"laps": {"laps": [5.0, 3.0, 4.0, 9.0]}}}
    doc = gate.baseline_document(run, 4)
    assert doc["phases"] == {"pandas": {"laps": {"laps": {"min": 3.0, "median": 4.5}}}}
    assert doc["meta"]["reps"] == 4


def test_missing_phase_fails_and_new_phase_is_ignored() -> None:
    measured = _measured(1.0)
    del measured["pandas"]["laps"]["laps"]
    measured["pandas"]["laps"]["new_step"] = 99.0
    measured["pandas"]["unknown_workload"] = {"x": 1.0}
    comparison = gate.compare(BASE, measured)
    assert comparison.missing == ["pandas/laps/laps"]
    assert not comparison.ok
    assert "MISSING phase pandas/laps/laps" in gate.format_table(comparison)


def _frame(dtype: str = "float64", digest: str = "h1", cls: str = "Telemetry") -> dict:
    return {"shape": [2, 1], "index": "i", "columns": {"Speed": [dtype, digest]}, "class": cls}


def test_compare_digests_reports_each_difference() -> None:
    expected = {
        "laps": _frame(),
        "drivers": ["1", "81"],
        "split": [_frame(), _frame()],
        "all_tel": {"VER/1": _frame(), "VER/2": _frame()},
        "gone": "x",
    }
    assert gate.compare_digests(expected, dict(expected)) == []
    actual = {
        "laps": _frame(dtype="Float64"),
        "drivers": ["81", "1"],
        "split": [_frame(), _frame(digest="h2")],
        "all_tel": {"VER/1": _frame(cls="DataFrame"), "VER/2": _frame()},
        "new": "y",
    }
    lines = gate.compare_digests(expected, actual)
    assert "laps: column Speed: ['float64', 'h1'] -> ['Float64', 'h1']" in lines
    assert "drivers[0]: 1 -> 81" in lines
    assert "split[1]: column Speed: ['float64', 'h1'] -> ['float64', 'h2']" in lines
    assert "all_tel/VER/1: class Telemetry -> DataFrame" in lines
    assert "gone: only in the committed digest" in lines
    assert "new: only in this tree" in lines


def test_bundle_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "bundle.xz"
    json_rows = [("2025/X/Race/drivers.json", b'{"drivers":[]}')]
    tel_rows = [((2025, "X", "Race", "VER", 1), b'{"tel":{"time":[0.0]}}')]
    size = fixture.write_bundle(path, json_rows, tel_rows, ["2025/X/Race/LEC/1_tel.json"], {})
    assert size == path.stat().st_size
    manifest, area = gate.read_bundle(path)
    assert manifest["version"] == fixture.BUNDLE_VERSION
    assert manifest["missing"] == ["2025/X/Race/LEC/1_tel.json"]
    key, offset, length = manifest["json"][0]
    assert key == "2025/X/Race/drivers.json"
    assert bytes(area[offset : offset + length]) == b'{"drivers":[]}'
    *ref, offset, length = manifest["telemetry"][0]
    assert ref == [2025, "X", "Race", "VER", 1]
    assert bytes(area[offset : offset + length]) == b'{"tel":{"time":[0.0]}}'


def test_child_env_is_clean_and_offline(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TIF1_LIB", "polars")
    env = gate.child_env(tmp_path / "cache", tmp_path / "home", tmp_path / "src")
    assert "TIF1_LIB" not in env
    assert env["TIF1_OFFLINE_MODE"] == "true"
    assert env["TIF1_CACHE_DIR"] == str(tmp_path / "cache")
    assert env["HOME"] == str(tmp_path / "home")
    assert env["PYTHONPATH"] == str(tmp_path / "src")
    assert not any(k.startswith("PYTEST_") for k in env)


def test_committed_fixture_files_are_consistent() -> None:
    baseline = gate.load_baseline()
    assert set(baseline) == set(gate.LIBS)
    for lib in gate.LIBS:
        assert set(baseline[lib]) == set(gate.WORKLOADS)
        for phases in baseline[lib].values():
            for entry in phases.values():
                assert set(entry) == {"min", "median"}
                assert entry["min"] <= entry["median"]
        for kind in gate.DIGEST_KINDS:
            assert gate.expected_digest(lib, kind)
    assert gate.BUNDLE.stat().st_size < 10_000_000


def test_write_outputs_round_trip(tmp_path: Path, monkeypatch) -> None:
    import orjson

    monkeypatch.setattr(gate, "FIXTURE_DIR", tmp_path)
    monkeypatch.setattr(gate, "BASELINE", tmp_path / "baseline.json")
    gate.BASELINE.write_bytes(
        orjson.dumps({"phases": {"pandas": {"kept": {"x": {"min": 1.0, "median": 2.0}}}}})
    )
    warm = {"a": _frame(), "b": ["x"], "only_warm": 1}
    cold = {"a": _frame(cls="DataFrame"), "b": ["x"], "only_cold": 2}
    run = gate.GateRun(digests={"pandas": {"cold": cold, "warm": warm}})
    run.samples = {"pandas": {"laps": {"laps": [4.0, 3.0, 5.0]}}}
    gate.write_outputs(run, 3)
    assert gate.load_baseline() == {
        "pandas": {
            "kept": {"x": {"min": 1.0, "median": 2.0}},
            "laps": {"laps": {"min": 3.0, "median": 4.0}},
        }
    }
    assert gate.expected_digest("pandas", "warm") == warm
    assert gate.expected_digest("pandas", "cold") == cold
    assert set(orjson.loads(gate.cold_delta_path("pandas").read_bytes())) == {
        "a",
        "only_cold",
        "only_warm",
    }
    # One surface per line, so a changed surface is a one-line diff.
    assert gate.parity_path("pandas").read_text(encoding="utf-8").count("\n") == len(warm) + 2
