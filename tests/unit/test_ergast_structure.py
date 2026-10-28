"""Flattening tables and casting helpers of ``tif1.ergast.structure``."""

from __future__ import annotations

import datetime
import json
import math
from pathlib import Path

import pandas as pd
import pytest

from tif1.ergast import structure

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ergast" / "stats"


def _payload(path: str) -> dict:
    return json.loads((FIXTURES / path).read_text(encoding="utf-8"))


def _races(path: str) -> list:
    return _payload(path)["MRData"]["RaceTable"]["Races"]


class TestCasting:
    def test_date_from_ergast(self):
        assert structure.date_from_ergast("2025-03-23") == datetime.datetime(2025, 3, 23)  # noqa: DTZ001
        assert structure.date_from_ergast("") is None
        assert structure.date_from_ergast(None) is None
        assert structure.date_from_ergast("23/03/2025") is None

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("07:00:00Z", datetime.time(7, 0, 0, tzinfo=datetime.UTC)),
            ("1:31.591", datetime.time(0, 1, 31, 591000)),
            ("31.5", datetime.time(0, 0, 31, 500000)),
            (
                "12:34:56+02:00",
                datetime.time(12, 34, 56, tzinfo=datetime.timezone(datetime.timedelta(hours=2))),
            ),
        ],
    )
    def test_time_from_ergast(self, text, expected):
        assert structure.time_from_ergast(text) == expected

    def test_time_from_ergast_rejects_garbage(self):
        assert structure.time_from_ergast("abc") is None
        assert structure.time_from_ergast(12) is None

    @pytest.mark.parametrize(
        ("text", "seconds"),
        [
            ("1:30:55.026", 5455.026),
            ("+9.748", 9.748),
            ("-1:00.000", -60.0),
            ("23.385", 23.385),
        ],
    )
    def test_timedelta_from_ergast(self, text, seconds):
        value = structure.timedelta_from_ergast(text)
        assert value is not None
        assert value.total_seconds() == pytest.approx(seconds)

    def test_timedelta_from_ergast_invalid(self):
        assert structure.timedelta_from_ergast("") is None
        assert structure.timedelta_from_ergast(None) is None

    def test_save_int_and_float(self):
        assert structure.save_int("12") == 12
        assert structure.save_int("+3") == 3
        assert structure.save_int("") == -1  # fastf1 #432: empty millis
        assert structure.save_int(None) == -1
        assert structure.save_float("206.018") == 206.018
        assert math.isnan(structure.save_float("fast"))
        assert math.isnan(structure.save_float(None))


class TestFlattening:
    def test_race_results_columns_match_fastf1_names(self):
        races = _races("2025/chinese-grand-prix/results.json")
        description = structure.flatten_rows(
            [{k: v for k, v in races[0].items() if k != "Results"}], structure.Races_RaceResults
        )
        assert list(description)[:6] == [
            "season",
            "round",
            "raceUrl",
            "raceName",
            "raceDate",
            "raceTime",
        ]
        assert description["circuitId"] == ["shanghai"]
        assert description["country"] == ["China"]

        rows = structure.flatten_rows(races[0]["Results"], structure.RaceResults)
        assert list(rows)[:7] == [
            "number",
            "position",
            "positionText",
            "points",
            "grid",
            "laps",
            "status",
        ]
        assert rows["driverId"][0] == "piastri"
        assert rows["constructorId"][0] == "mclaren"
        assert rows["points"][0] == 25.0
        assert rows["totalRaceTimeMillis"][0] == 5455026

    def test_total_race_time_is_true_total_and_gap(self):
        races = _races("2025/chinese-grand-prix/results.json")
        rows = structure.flatten_rows(races[0]["Results"], structure.RaceResults)
        winner_total = rows["totalRaceTime"][0]
        second_total = rows["totalRaceTime"][1]
        assert winner_total == datetime.timedelta(milliseconds=5455026)
        assert second_total == datetime.timedelta(milliseconds=5464774)
        assert rows["totalRaceTimeGap"][0] == datetime.timedelta(0)
        assert rows["totalRaceTimeGap"][1] == datetime.timedelta(seconds=9.748)
        # Drivers without a Time block (lapped, retired) get no total and no gap.
        without_time = [i for i, row in enumerate(races[0]["Results"]) if "Time" not in row]
        assert without_time
        assert pd.isna(rows["totalRaceTime"][without_time[0]])
        assert pd.isna(rows["totalRaceTimeGap"][without_time[0]])

    def test_total_race_time_without_cast_keeps_raw_strings(self):
        races = _races("2025/chinese-grand-prix/results.json")
        rows = structure.flatten_rows(races[0]["Results"], structure.RaceResults, cast=False)
        assert rows["totalRaceTime"][1] == "+9.748"
        assert "totalRaceTimeGap" not in rows

    def test_laps_flatten_merges_timings_per_lap(self):
        laps = _races("2025/chinese-grand-prix/laptimes.json")[0]["Laps"]
        rows = structure.flatten_rows(laps[:2], structure.Laps)
        assert len(rows["driverId"]) == 40
        assert rows["number"][:20] == [1] * 20
        assert rows["number"][20:] == [2] * 20
        assert rows["position"][0] == 1
        assert isinstance(rows["time"][0], datetime.timedelta)

    def test_flatten_fills_missing_columns_with_none(self):
        rows = structure.flatten_rows(
            [
                {"number": "1", "position": "1"},
                {"number": "2", "position": "2", "status": "Finished"},
            ],
            structure.RaceResults,
        )
        assert math.isnan(rows["status"][0])
        assert rows["status"][1] == "Finished"
        frame = pd.DataFrame(rows)
        assert frame.shape == (2, 3)

    def test_standings_inline_constructors(self):
        payload = _payload("2025/chinese-grand-prix/driverPoints.json")
        standings = payload["MRData"]["StandingsTable"]["StandingsLists"][0]["DriverStandings"]
        rows = structure.flatten_rows(standings, structure.DriverStandings)
        assert rows["constructorIds"][0] == ["mclaren"]
        assert rows["driverCode"][0] == "NOR"
        assert rows["points"][0] == 44.0

    def test_schedule_flattens_sprint_qualifying(self):
        races = _races("2025/events.json")
        rows = structure.flatten_rows(races, structure.Races_Schedule)
        china = rows["raceName"].index("Chinese Grand Prix")
        assert rows["sprintDate"][china] == datetime.datetime(2025, 3, 22)  # noqa: DTZ001
        assert rows["sprintQualifyingTime"][china] == datetime.time(7, 30, tzinfo=datetime.UTC)
        # fastf1 columns come first; the tif1 additions last.
        assert list(rows).index("sprintTime") < list(rows).index("sprintQualifyingDate")

    def test_flatten_does_not_mutate_input(self):
        races = _races("2025/chinese-grand-prix/laptimes.json")
        before = json.dumps(races[0]["Laps"][:2], sort_keys=True)
        structure.flatten_rows(races[0]["Laps"][:2], structure.Laps)
        assert json.dumps(races[0]["Laps"][:2], sort_keys=True) == before


class TestAutoCast:
    def test_auto_cast_casts_nested_values(self):
        races = _races("2025/chinese-grand-prix/results.json")
        data = json.loads(json.dumps(races))
        structure.auto_cast(data, structure.Races_RaceResults)
        assert data[0]["round"] == 2
        assert data[0]["Results"][0]["points"] == 25.0
        assert isinstance(data[0]["Results"][0]["Time"]["time"], datetime.timedelta)
        assert data[0]["Circuit"]["Location"]["lat"] == 31.3389
