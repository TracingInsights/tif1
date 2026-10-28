"""Benchmarks for the Ergast tier (offline, fixture-backed).

Budgets from the design: flattening a race result set stays in the low
milliseconds, a warm ``Session.results`` adds a few milliseconds over the
lap-derived view, and a past-season schedule needs no network.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tif1.ergast import Ergast, structure

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ergast" / "stats"


@pytest.fixture
def results_rows() -> list:
    payload = json.loads(
        (FIXTURES / "2025/chinese-grand-prix/results.json").read_text(encoding="utf-8")
    )
    return payload["MRData"]["RaceTable"]["Races"][0]["Results"]


@pytest.mark.benchmark
def test_flatten_race_results(benchmark, results_rows):
    rows = benchmark(structure.flatten_rows, results_rows, structure.RaceResults)
    assert len(rows["driverId"]) == 20


@pytest.mark.benchmark
def test_flatten_lap_timings(benchmark):
    payload = json.loads(
        (FIXTURES / "2025/chinese-grand-prix/laptimes.json").read_text(encoding="utf-8")
    )
    laps = payload["MRData"]["RaceTable"]["Races"][0]["Laps"]
    rows = benchmark(structure.flatten_rows, laps, structure.Laps)
    assert len(rows["driverId"]) == 1065


@pytest.mark.benchmark
@pytest.mark.usefixtures("ergast_offline")
def test_warm_race_results_query(benchmark):
    ergast = Ergast()
    ergast.get_race_results(2025, 2)  # warm the memo
    response = benchmark(ergast.get_race_results, 2025, 2)
    assert response.total_results == 20


@pytest.mark.benchmark
def test_past_season_schedule_is_offline(benchmark):
    import tif1

    schedule = benchmark(tif1.get_event_schedule, 1995)
    assert len(schedule) == 17
