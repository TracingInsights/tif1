"""Benchmarks for pandas lap-time filtering implementations."""

from __future__ import annotations

import pandas as pd
import pytest

from tif1.core_utils.helpers import _filter_valid_laptimes


def _build_laps_df(size: int = 20000) -> pd.DataFrame:
    """Build deterministic laps dataframe with mixed lap-time values."""
    lap_time = []
    for idx in range(size):
        mod = idx % 10
        if mod == 0:
            lap_time.append(None)
        elif mod == 1:
            lap_time.append("None")
        elif mod == 2:
            lap_time.append("bad")
        elif mod == 3:
            lap_time.append("91.245")
        else:
            lap_time.append(90.0 + (idx % 100) * 0.01)

    return pd.DataFrame(
        {
            "Driver": [f"DRV{idx % 20:02d}" for idx in range(size)],
            "LapNumber": [(idx % 70) + 1 for idx in range(size)],
            "LapTime": lap_time,
        }
    )


def _legacy_filter_valid_laptimes_pandas(laps: pd.DataFrame) -> pd.DataFrame:
    """Legacy pandas implementation baseline."""
    mask = laps["LapTime"].notna() & (laps["LapTime"] != "None")
    valid = laps[mask].copy()
    if not valid.empty:
        valid["LapTimeSeconds"] = pd.to_numeric(valid["LapTime"], errors="coerce")
        valid = valid.dropna(subset=["LapTimeSeconds"])
        valid["LapTime"] = pd.to_timedelta(valid["LapTimeSeconds"], unit="s")
    return valid


def _candidate_filter_valid_laptimes_pandas(laps: pd.DataFrame) -> pd.DataFrame:
    """Candidate pandas implementation using one numeric-conversion pass."""
    lap_time_numeric = pd.to_numeric(laps["LapTime"], errors="coerce")
    valid_mask = lap_time_numeric.notna()

    valid = laps.loc[valid_mask].copy()
    if valid.empty:
        return valid

    valid["LapTimeSeconds"] = lap_time_numeric[valid_mask].to_numpy(copy=False)
    valid["LapTime"] = pd.to_timedelta(valid["LapTimeSeconds"], unit="s")
    return valid


def test_filter_valid_laptimes_candidate_matches_legacy():
    """Candidate output should match legacy implementation semantics."""
    laps = _build_laps_df(3000)
    legacy = _legacy_filter_valid_laptimes_pandas(laps)
    candidate = _candidate_filter_valid_laptimes_pandas(laps)
    production = _filter_valid_laptimes(laps, lib="pandas")

    pd.testing.assert_frame_equal(candidate, legacy)
    pd.testing.assert_frame_equal(production, legacy)


@pytest.mark.benchmark
class TestLapTimeFilterBenchmarks:
    """Benchmark lap-time filtering candidates."""

    def test_benchmark_filter_valid_laptimes_legacy(self, benchmark):
        """Benchmark legacy pandas lap-time filtering."""
        laps = _build_laps_df()
        result = benchmark(_legacy_filter_valid_laptimes_pandas, laps)
        assert not result.empty

    def test_benchmark_filter_valid_laptimes_candidate(self, benchmark):
        """Benchmark candidate pandas lap-time filtering."""
        laps = _build_laps_df()
        result = benchmark(_candidate_filter_valid_laptimes_pandas, laps)
        assert not result.empty

    def test_benchmark_filter_valid_laptimes_production(self, benchmark):
        """Benchmark current production pandas lap-time filtering."""
        laps = _build_laps_df()
        result = benchmark(_filter_valid_laptimes, laps, "pandas")
        assert not result.empty
