"""Micro-benchmarks for detect_lap_anomalies single-pass candidate."""

from __future__ import annotations

from collections import Counter

import pytest

from tif1.validation import Anomaly, AnomalyType, detect_lap_anomalies


def _build_laps_payload(size: int = 6000) -> list[dict]:
    laps: list[dict] = []
    for lap in range(1, size + 1):
        if lap % 19 == 0:
            continue
        lap_number = lap - 1 if lap % 17 == 0 else lap
        lap_time = 90.0 + (lap % 60) * 0.07
        if lap % 251 == 0:
            lap_time = 450.0
        laps.append({"lap": lap_number, "time": lap_time})
    return laps


def _candidate_detect_lap_anomalies(laps: list[dict]) -> list[Anomaly]:
    anomalies: list[Anomaly] = []
    if not laps:
        return anomalies

    lap_counts: Counter[int] = Counter()
    lap_times: list[float] = []
    min_lap: int | None = None
    max_lap: int | None = None

    for lap in laps:
        lap_num = lap.get("lap")
        if lap_num is None:
            lap_num = lap.get("LapNumber")

        if lap_num is not None:
            lap_int = int(lap_num)
            lap_counts[lap_int] += 1
            if min_lap is None or lap_int < min_lap:
                min_lap = lap_int
            if max_lap is None or lap_int > max_lap:
                max_lap = lap_int

        lap_time = lap.get("time")
        if lap_time is None:
            lap_time = lap.get("LapTime")
        if isinstance(lap_time, int | float) and lap_time > 0:
            lap_times.append(lap_time)

    if lap_counts and min_lap is not None and max_lap is not None:
        actual = set(lap_counts)
        missing = [lap_num for lap_num in range(min_lap, max_lap + 1) if lap_num not in actual]
        if missing:
            anomalies.append(
                Anomaly(
                    type=AnomalyType.MISSING_LAPS,
                    severity="medium",
                    description=f"Missing {len(missing)} lap(s)",
                    details={"missing_laps": missing},
                )
            )

        duplicates = sorted(num for num, count in lap_counts.items() if count > 1)
        if duplicates:
            anomalies.append(
                Anomaly(
                    type=AnomalyType.DUPLICATE_LAPS,
                    severity="high",
                    description="Duplicate lap numbers detected",
                    details={"duplicate_laps": duplicates},
                )
            )

    if len(lap_times) >= 3:
        avg_time = sum(lap_times) / len(lap_times)
        outliers = [lap_time for lap_time in lap_times if lap_time > avg_time * 3]
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


def test_anomaly_candidate_parity():
    laps = _build_laps_payload(2000)
    production = [a.model_dump() for a in detect_lap_anomalies(laps)]
    candidate = [a.model_dump() for a in _candidate_detect_lap_anomalies(laps)]
    assert candidate == production


@pytest.mark.benchmark(group="validation_anomaly_micro")
class TestValidationAnomalyMicroBenchmark:
    def test_production(self, benchmark):
        laps = _build_laps_payload()
        result = benchmark(detect_lap_anomalies, laps)
        assert result

    def test_candidate(self, benchmark):
        laps = _build_laps_payload()
        result = benchmark(_candidate_detect_lap_anomalies, laps)
        assert result
