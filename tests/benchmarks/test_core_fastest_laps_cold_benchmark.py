"""Benchmarks for cold-start fastest-lap calculation."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from tif1.core import Session

_DRIVER_CODES = [f"D{i:02d}" for i in range(20)]
_EXPECTED_FASTEST_DRIVER = "D07"
_EXPECTED_FASTEST_LAP = 23
_EXPECTED_FASTEST_TIME = 86.432


def _build_laptime_payload(driver_code: str) -> dict[str, Any]:
    driver_index = int(driver_code[1:])
    laps = list(range(1, 61))
    times = [90.0 + (driver_index * 0.05) + ((lap % 13) * 0.004) for lap in laps]
    if driver_code == _EXPECTED_FASTEST_DRIVER:
        times[_EXPECTED_FASTEST_LAP - 1] = _EXPECTED_FASTEST_TIME

    return {
        "lap": laps,
        "time": times,
        "compound": ["SOFT"] * len(laps),
        "status": ["OK"] * len(laps),
    }


def _build_drivers_payload() -> list[dict[str, Any]]:
    return [
        {"driver": driver_code, "team": f"Team {idx:02d}"}
        for idx, driver_code in enumerate(_DRIVER_CODES)
    ]


def _parse_driver(path: str) -> str:
    return path.split("/", maxsplit=1)[0]


async def _fake_fetch_multiple_async(
    requests: list[tuple[int, str, str, str]],
    **_kwargs,
) -> list[dict[str, Any] | None]:
    return [_build_laptime_payload(_parse_driver(path)) for _, _, _, path in requests]


def _fake_fetch_json(self: Session, path: str) -> dict[str, Any]:
    if path == "drivers.json":
        return {"drivers": _build_drivers_payload()}
    if path.endswith("/laptimes.json"):
        return _build_laptime_payload(_parse_driver(path))
    return {}


def _legacy_get_fastest_laps_by_driver() -> pd.DataFrame:
    rows = []
    for driver_info in _build_drivers_payload():
        driver_code = driver_info["driver"]
        payload = _build_laptime_payload(driver_code)
        lap_df = pd.DataFrame(payload)
        lap_df["Driver"] = driver_code
        lap_df["Team"] = driver_info["team"]
        rows.append(lap_df)

    laps = pd.concat(rows, ignore_index=True)
    laps = laps.rename(
        columns={
            "time": "LapTime",
            "lap": "LapNumber",
            "compound": "Compound",
            "status": "TrackStatus",
        }
    )

    lap_time_numeric = pd.to_numeric(laps["LapTime"], errors="coerce")
    valid_mask = lap_time_numeric.notna()
    valid = laps.loc[valid_mask].copy()
    valid["LapTimeSeconds"] = lap_time_numeric[valid_mask].to_numpy(copy=False)
    valid["LapTime"] = pd.to_timedelta(valid["LapTimeSeconds"], unit="s")

    result = valid.loc[valid.groupby("Driver", observed=True)["LapTimeSeconds"].idxmin()]
    return result.sort_values("LapTimeSeconds").reset_index(drop=True)


def _setup_fastest_mocks(monkeypatch) -> None:
    monkeypatch.setattr("tif1.core.fetch_multiple_async", _fake_fetch_multiple_async)
    monkeypatch.setattr("tif1.core.Session._fetch_json", _fake_fetch_json)
    monkeypatch.setattr("tif1.core.Session._fetch_json_unvalidated", _fake_fetch_json)


@pytest.mark.benchmark(group="core_fastest_laps_cold")
class TestCoreFastestLapsColdBenchmark:
    """Benchmark legacy vs optimized cold fastest-lap paths."""

    def test_legacy_cold_path(self, benchmark):
        result = benchmark(_legacy_get_fastest_laps_by_driver)
        assert not result.empty
        assert result["Driver"].iloc[0] == _EXPECTED_FASTEST_DRIVER
        assert int(result["LapNumber"].iloc[0]) == _EXPECTED_FASTEST_LAP
        assert float(result["LapTimeSeconds"].iloc[0]) == pytest.approx(_EXPECTED_FASTEST_TIME)

    def test_optimized_cold_path(self, benchmark, monkeypatch):
        _setup_fastest_mocks(monkeypatch)
        session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")

        def _run():
            session._drivers = None
            return session.get_fastest_laps(by_driver=True)

        result = benchmark(_run)
        assert not result.empty
        assert result["Driver"].iloc[0] == _EXPECTED_FASTEST_DRIVER
        assert int(result["LapNumber"].iloc[0]) == _EXPECTED_FASTEST_LAP
        assert float(result["LapTimeSeconds"].iloc[0]) == pytest.approx(_EXPECTED_FASTEST_TIME)
