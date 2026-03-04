"""Benchmarks for validation hot paths."""

from __future__ import annotations

import pytest

from tif1.validation import Anomaly, AnomalyType, detect_lap_anomalies


def _build_laps_payload(size: int = 3000) -> list[dict]:
    """Build a deterministic lap payload with duplicates, gaps, and outliers."""
    laps: list[dict] = []
    for lap in range(1, size + 1):
        if lap % 17 == 0:
            continue  # introduce missing laps

        lap_number = lap - 1 if lap % 13 == 0 else lap
        lap_time = 90.0 + (lap % 40) * 0.08
        if lap % 233 == 0:
            lap_time = 500.0  # outlier

        laps.append({"lap": lap_number, "time": lap_time})
    return laps


def _legacy_detect_lap_anomalies(laps: list[dict]) -> list[Anomaly]:
    """Legacy anomaly detection implementation (pre-optimization baseline)."""
    anomalies: list[Anomaly] = []
    if not laps:
        return anomalies

    lap_numbers = []
    for lap in laps:
        lap_num = lap.get("lap") or lap.get("LapNumber")
        if lap_num is not None:
            lap_numbers.append(int(lap_num))

    if lap_numbers:
        expected = set(range(min(lap_numbers), max(lap_numbers) + 1))
        actual = set(lap_numbers)
        missing = sorted(expected - actual)
        if missing:
            anomalies.append(
                Anomaly(
                    type=AnomalyType.MISSING_LAPS,
                    severity="medium",
                    description=f"Missing {len(missing)} lap(s)",
                    details={"missing_laps": missing},
                )
            )

    if len(lap_numbers) != len(set(lap_numbers)):
        duplicates = [num for num in set(lap_numbers) if lap_numbers.count(num) > 1]
        anomalies.append(
            Anomaly(
                type=AnomalyType.DUPLICATE_LAPS,
                severity="high",
                description="Duplicate lap numbers detected",
                details={"duplicate_laps": sorted(duplicates)},
            )
        )

    lap_times = []
    for lap in laps:
        lap_time = lap.get("time") or lap.get("LapTime")
        if isinstance(lap_time, int | float) and lap_time > 0:
            lap_times.append(lap_time)

    if len(lap_times) >= 3:
        avg_time = sum(lap_times) / len(lap_times)
        outliers = [t for t in lap_times if t > avg_time * 3]
        if outliers:
            anomalies.append(
                Anomaly(
                    type=AnomalyType.OUTLIER_TIMES,
                    severity="low",
                    description=f"{len(outliers)} outlier lap time(s) detected",
                    details={"outlier_count": len(outliers), "average_time": round(avg_time, 3)},
                )
            )

    return anomalies


def test_anomaly_detection_semantics_match_legacy():
    """Optimized implementation should preserve legacy results."""
    laps = _build_laps_payload(1000)
    legacy = [a.model_dump() for a in _legacy_detect_lap_anomalies(laps)]
    optimized = [a.model_dump() for a in detect_lap_anomalies(laps)]
    assert optimized == legacy


@pytest.mark.benchmark
class TestValidationBenchmarks:
    """Benchmark validation performance."""

    def test_benchmark_anomaly_detection_legacy(self, benchmark):
        """Benchmark legacy anomaly detection complexity."""
        laps = _build_laps_payload(3000)
        result = benchmark(_legacy_detect_lap_anomalies, laps)
        assert result

    def test_benchmark_anomaly_detection_optimized(self, benchmark):
        """Benchmark optimized anomaly detection complexity."""
        laps = _build_laps_payload(3000)
        result = benchmark(detect_lap_anomalies, laps)
        assert result
