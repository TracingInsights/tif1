"""Benchmarks for telemetry batch preparation in core Session."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from tif1.core import Session
from tif1.core_utils.constants import COL_DRIVER
from tif1.core_utils.helpers import _check_cached_telemetry, _get_lap_number


class _StubCache:
    """Simple in-memory telemetry cache stub."""

    def __init__(self, cached: dict[tuple[int, str, str, str, int], dict] | None = None):
        self._cached = cached or {}

    def get_telemetry(self, year: int, gp: str, session: str, driver: str, lap: int) -> Any | None:
        return self._cached.get((year, gp, session, driver, lap))

    def get_telemetry_batch(
        self, year: int, gp: str, session: str, driver_laps: list[tuple[str, int]]
    ) -> dict[tuple[str, int], Any]:
        results = {}
        for driver, lap in driver_laps:
            val = self.get_telemetry(year, gp, session, driver, lap)
            if val is not None:
                results[(driver, lap)] = val
        return results


def _build_fastest_laps(size: int = 5000) -> pd.DataFrame:
    """Build deterministic fastest-lap-like dataframe for benchmarking."""
    drivers = [f"DRV{i % 20:02d}" for i in range(size)]
    lap_numbers = [(i % 70) + 1 for i in range(size)]
    return pd.DataFrame({COL_DRIVER: drivers, "LapNumber": lap_numbers, "LapTime": [90.0] * size})


def _legacy_fetch_telemetry_batch(session: Session, fastest_laps: pd.DataFrame, cache: _StubCache):
    """Legacy implementation baseline for telemetry batch preparation."""
    requests, lap_info, tels = [], [], []

    for _, row_data in fastest_laps.iterrows():
        driver = row_data[COL_DRIVER]

        try:
            lap_num = _get_lap_number(row_data)
        except ValueError:
            continue

        cached_df = _check_cached_telemetry(
            cache, session.year, session.gp, session.session, driver, lap_num, session.lib
        )
        if cached_df is not None:
            tels.append(cached_df)
        else:
            requests.append(
                (session.year, session.gp, session.session, f"{driver}/{lap_num}_tel.json")
            )
            lap_info.append((driver, lap_num))

    return requests, lap_info, tels


def test_fetch_telemetry_batch_semantics_match_legacy(monkeypatch):
    """Optimized telemetry batch prep should match legacy behavior."""
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
    fastest_laps = pd.DataFrame(
        {
            COL_DRIVER: ["VER", "HAM", "LEC", "NOR"],
            "LapNumber": [1, "2", "bad", 4],
            "LapTime": [90.1, 90.2, 90.3, 90.4],
        }
    )

    cached_data = {
        (2025, session.gp, session.session, "HAM", 2): {"time": [0.0, 1.0], "speed": [100, 120]}
    }
    cache = _StubCache(cached=cached_data)
    monkeypatch.setattr("tif1.core.get_cache", lambda: cache)
    # Mark cache as available so the new implementation will use it
    session._cache_has_session_data = True

    legacy_requests, legacy_lap_info, legacy_tels = _legacy_fetch_telemetry_batch(
        session, fastest_laps, cache
    )
    requests, lap_info, tels = session._fetch_telemetry_batch(fastest_laps)

    assert requests == legacy_requests
    assert lap_info == legacy_lap_info
    assert len(tels) == len(legacy_tels)
    assert [df["Driver"].iloc[0] for df in tels] == [df["Driver"].iloc[0] for df in legacy_tels]
    assert [int(df["LapNumber"].iloc[0]) for df in tels] == [
        int(df["LapNumber"].iloc[0]) for df in legacy_tels
    ]


@pytest.mark.benchmark
class TestCoreTelemetryBatchBenchmarks:
    """Benchmark telemetry batch preparation performance."""

    def test_benchmark_fetch_telemetry_batch_legacy(self, benchmark):
        """Benchmark legacy telemetry batch prep."""
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        fastest_laps = _build_fastest_laps(5000)
        cache = _StubCache()

        requests, lap_info, tels = benchmark(
            _legacy_fetch_telemetry_batch, session, fastest_laps, cache
        )
        assert len(requests) == len(fastest_laps)
        assert len(lap_info) == len(fastest_laps)
        assert len(tels) == 0

    def test_benchmark_fetch_telemetry_batch_optimized(self, benchmark, monkeypatch):
        """Benchmark optimized telemetry batch prep."""
        session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
        fastest_laps = _build_fastest_laps(5000)
        cache = _StubCache()
        monkeypatch.setattr("tif1.core.get_cache", lambda: cache)

        requests, lap_info, tels = benchmark(session._fetch_telemetry_batch, fastest_laps)
        assert len(requests) == len(fastest_laps)
        assert len(lap_info) == len(fastest_laps)
        assert len(tels) == 0
