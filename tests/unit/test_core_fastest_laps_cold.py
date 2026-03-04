"""Unit tests for cold-start fastest-laps semantics."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pandas as pd

import tif1.core as core_module
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


def _normalize_fastest(df: pd.DataFrame) -> list[tuple[str, int, float]]:
    lap_time_series = (
        df["LapTimeSeconds"]
        if "LapTimeSeconds" in df.columns
        else pd.to_timedelta(df["LapTime"]).dt.total_seconds()
    )
    return [
        (str(driver), int(lap), float(lap_time))
        for driver, lap, lap_time in zip(
            df["Driver"],
            df["LapNumber"],
            lap_time_series,
            strict=False,
        )
    ]


def _setup_fastest_mocks(monkeypatch) -> None:
    monkeypatch.setattr("tif1.core.fetch_multiple_async", _fake_fetch_multiple_async)
    monkeypatch.setattr("tif1.core.Session._fetch_json", _fake_fetch_json)
    monkeypatch.setattr("tif1.core.Session._fetch_json_unvalidated", _fake_fetch_json)


def test_get_fastest_laps_cold_path_matches_legacy(monkeypatch):
    """Cold-path fastest-lap selection should match legacy full-laps behavior."""
    _setup_fastest_mocks(monkeypatch)
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")

    optimized = session.get_fastest_laps(by_driver=True)
    legacy = _legacy_get_fastest_laps_by_driver()

    assert not optimized.empty
    assert _normalize_fastest(optimized) == _normalize_fastest(legacy)
    assert session._laps is None


def test_get_fastest_laps_cold_path_respects_driver_filter(monkeypatch):
    """Cold-path should preserve driver filtering semantics."""
    _setup_fastest_mocks(monkeypatch)
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")

    filtered = session.get_fastest_laps(by_driver=True, drivers=["D01", "D04", "MISSING"])
    assert set(filtered["Driver"]) == {"D01", "D04"}
    assert "MISSING" not in set(filtered["Driver"])


def test_get_fastest_laps_ultra_cold_disables_cache_io(monkeypatch):
    """Ultra-cold config should disable cache/validation I/O in cold fastest-lap fetch."""
    fetch_kwargs: dict[str, Any] = {}

    async def _capturing_fetch_multiple_async(
        requests: list[tuple[int, str, str, str]],
        **kwargs,
    ) -> list[dict[str, Any] | None]:
        fetch_kwargs.update(kwargs)
        return await _fake_fetch_multiple_async(requests, **kwargs)

    monkeypatch.setattr("tif1.core.fetch_multiple_async", _capturing_fetch_multiple_async)
    monkeypatch.setattr("tif1.core.Session._fetch_json", _fake_fetch_json)
    monkeypatch.setattr("tif1.core.Session._fetch_json_unvalidated", _fake_fetch_json)

    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")

    previous_ultra_cold = core_module.config.get("ultra_cold_start", False)
    previous_backfill = core_module.config.get("ultra_cold_background_cache_fill", False)
    core_module.config.set("ultra_cold_start", True)
    core_module.config.set("ultra_cold_background_cache_fill", True)
    try:
        with patch.object(session, "_schedule_background_cache_fill") as backfill_mock:
            fastest = session.get_fastest_laps(by_driver=True)
    finally:
        core_module.config.set("ultra_cold_start", previous_ultra_cold)
        core_module.config.set("ultra_cold_background_cache_fill", previous_backfill)

    assert not fastest.empty
    assert fetch_kwargs.get("use_cache") is False
    assert fetch_kwargs.get("write_cache") is False
    assert fetch_kwargs.get("validate_payload") is False
    backfill_mock.assert_called_once()
    payloads = backfill_mock.call_args.kwargs.get("json_payloads", [])
    assert len(payloads) == len(_DRIVER_CODES)
