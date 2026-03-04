"""Benchmarks for notebook-style cold-start workflow."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

import tif1
from tif1.core import Session, clear_lap_cache

_DRIVER_CODES = [f"D{i:02d}" for i in range(20)]
_EXPECTED_FASTEST_DRIVER = "D07"
_EXPECTED_FASTEST_LAP = 23
_EXPECTED_FASTEST_TIME = 86.432


class _InMemoryCache:
    """Small in-memory cache stub used to control warm/cold benchmark state."""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._telemetry: dict[tuple[int, str, str, str, int], Any] = {}

    def get(self, key: str) -> Any | None:
        return self._data.get(key)

    def set(self, key: str, data: Any) -> None:
        self._data[key] = data

    def get_telemetry(self, year: int, gp: str, session: str, driver: str, lap: int) -> Any | None:
        return self._telemetry.get((year, gp, session, driver, lap))

    def set_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        self._telemetry[(year, gp, session, driver, lap)] = data

    def clear(self) -> None:
        self._data.clear()
        self._telemetry.clear()


def _build_laptime_payload(driver_code: str) -> dict[str, Any]:
    driver_index = int(driver_code[1:])
    laps = list(range(1, 61))
    times = [90.0 + (driver_index * 0.07) + ((lap % 11) * 0.003) for lap in laps]
    if driver_code == _EXPECTED_FASTEST_DRIVER:
        times[_EXPECTED_FASTEST_LAP - 1] = _EXPECTED_FASTEST_TIME

    return {
        "lap": laps,
        "time": times,
        "compound": ["SOFT"] * len(laps),
        "status": ["OK"] * len(laps),
    }


def _build_telemetry_payload(driver: str, lap_num: int) -> dict[str, Any]:
    speed_base = 200 + int(driver[1:])
    return {
        "tel": {
            "time": [0.0, 1.0, 2.0],
            "speed": [speed_base, speed_base + 3, speed_base + 6],
            "throttle": [100, 98, 95],
            "brake": [0, 0, 2],
            "lap": [lap_num, lap_num, lap_num],
        }
    }


def _parse_driver_and_lap(path: str) -> tuple[str, int]:
    driver, lap_resource = path.split("/", maxsplit=1)
    lap_num = int(lap_resource.split("_", maxsplit=1)[0])
    return driver, lap_num


async def _fake_fetch_multiple_async(
    requests: list[tuple[int, str, str, str]],
) -> list[dict | None]:
    results: list[dict | None] = []
    for _, _, _, path in requests:
        if path.endswith("/laptimes.json"):
            driver_code = path.split("/", maxsplit=1)[0]
            results.append(_build_laptime_payload(driver_code))
            continue
        if path.endswith("_tel.json"):
            driver_code, lap_num = _parse_driver_and_lap(path)
            results.append(_build_telemetry_payload(driver_code, lap_num))
            continue
        results.append(None)
    return results


def _fake_fetch_json(self: Session, path: str) -> dict[str, Any]:
    if path == "drivers.json":
        return {
            "drivers": [
                {"driver": driver_code, "team": f"Team {i:02d}"}
                for i, driver_code in enumerate(_DRIVER_CODES)
            ]
        }
    if path.endswith("/laptimes.json"):
        driver_code = path.split("/", maxsplit=1)[0]
        return _build_laptime_payload(driver_code)
    if path.endswith("_tel.json"):
        driver_code, lap_num = _parse_driver_and_lap(path)
        return _build_telemetry_payload(driver_code, lap_num)
    return {}


def _fake_fetch_from_cdn(self: Session, path: str) -> dict[str, Any]:
    return _fake_fetch_json(self, path)


def _setup_workflow_mocks(monkeypatch) -> _InMemoryCache:
    cache = _InMemoryCache()
    monkeypatch.setattr("tif1.core.get_cache", lambda: cache)
    monkeypatch.setattr("tif1.core.fetch_multiple_async", _fake_fetch_multiple_async)
    monkeypatch.setattr("tif1.core.Session._fetch_json", _fake_fetch_json)
    monkeypatch.setattr("tif1.core.Session._fetch_from_cdn", _fake_fetch_from_cdn)
    return cache


def _run_notebook_workflow(cache: _InMemoryCache, mode: str) -> pd.DataFrame:
    # Keep every benchmark iteration cold by clearing disk/memory equivalents up front.
    cache.clear()
    clear_lap_cache()

    session = tif1.get_session(
        2022, "Mexico City Grand Prix", "Race", enable_cache=True, lib="pandas"
    )
    if mode == "legacy":
        return session.get_fastest_laps_tels(by_driver=False)
    if mode == "optimized":
        return session.get_fastest_lap_tel()
    if mode == "ultra":
        return session.get_fastest_lap_tel(ultra_cold=True)
    raise ValueError(f"Unknown mode: {mode}")


def test_notebook_workflow_fast_path_matches_legacy(monkeypatch):
    """Optimized cold-start workflow should preserve legacy result semantics."""
    cache = _setup_workflow_mocks(monkeypatch)

    optimized = _run_notebook_workflow(cache, mode="optimized")
    legacy = _run_notebook_workflow(cache, mode="legacy")
    ultra = _run_notebook_workflow(cache, mode="ultra")

    assert not optimized.empty
    assert not legacy.empty
    assert not ultra.empty
    assert optimized["Driver"].iloc[0] == _EXPECTED_FASTEST_DRIVER
    assert legacy["Driver"].iloc[0] == _EXPECTED_FASTEST_DRIVER
    assert ultra["Driver"].iloc[0] == _EXPECTED_FASTEST_DRIVER
    assert int(optimized["LapNumber"].iloc[0]) == _EXPECTED_FASTEST_LAP
    assert int(legacy["LapNumber"].iloc[0]) == _EXPECTED_FASTEST_LAP
    assert int(ultra["LapNumber"].iloc[0]) == _EXPECTED_FASTEST_LAP
    assert optimized["Speed"].tolist() == legacy["Speed"].tolist()
    assert ultra["Speed"].tolist() == legacy["Speed"].tolist()


@pytest.mark.benchmark(group="core_notebook_workflow")
class TestCoreNotebookWorkflowBenchmark:
    """Benchmark notebook workflow before and after cold-start optimization."""

    def test_legacy_workflow_cold(self, benchmark, monkeypatch):
        cache = _setup_workflow_mocks(monkeypatch)
        result = benchmark(_run_notebook_workflow, cache, "legacy")
        assert not result.empty
        assert result["Driver"].iloc[0] == _EXPECTED_FASTEST_DRIVER
        assert int(result["LapNumber"].iloc[0]) == _EXPECTED_FASTEST_LAP

    def test_optimized_workflow_cold(self, benchmark, monkeypatch):
        cache = _setup_workflow_mocks(monkeypatch)
        result = benchmark(_run_notebook_workflow, cache, "optimized")
        assert not result.empty
        assert result["Driver"].iloc[0] == _EXPECTED_FASTEST_DRIVER
        assert int(result["LapNumber"].iloc[0]) == _EXPECTED_FASTEST_LAP

    def test_ultra_workflow_cold(self, benchmark, monkeypatch):
        cache = _setup_workflow_mocks(monkeypatch)
        result = benchmark(_run_notebook_workflow, cache, "ultra")
        assert not result.empty
        assert result["Driver"].iloc[0] == _EXPECTED_FASTEST_DRIVER
        assert int(result["LapNumber"].iloc[0]) == _EXPECTED_FASTEST_LAP
