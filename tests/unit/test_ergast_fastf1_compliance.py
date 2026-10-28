"""Flattened Ergast frames compared with fastf1's own flattening on the same payloads.

Runs only when fastf1 is installed (``pytest.importorskip``); the fixtures
are real Jolpica responses, so the frames must agree column by column except
for the documented ``totalRaceTime`` change (true total instead of the gap)
and the added columns.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from tif1.ergast import structure

fastf1_structure = pytest.importorskip("fastf1.ergast.structure")
fastf1_interface = pytest.importorskip("fastf1.ergast.interface")

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ergast"
TIF1_EXTRA_COLUMNS = {
    "totalRaceTimeGap",
    "sprintQualifyingDate",
    "sprintQualifyingTime",
    "sprintShootoutDate",
    "sprintShootoutTime",
}
#: Columns whose values differ by design.
VALUE_EXCEPTIONS = {"totalRaceTime"}


def _load(path: str) -> dict:
    return json.loads((FIXTURES / path).read_text(encoding="utf-8"))


def _fastf1_frame(response: list, category: dict) -> pd.DataFrame:
    return pd.DataFrame(fastf1_interface.ErgastResultFrame(response=response, category=category))


def _tif1_frame(response: list, category: dict) -> pd.DataFrame:
    return pd.DataFrame(structure.flatten_rows(response, category))


def _compare(response: list, tif1_category: dict, fastf1_category: dict) -> None:
    ours = _tif1_frame(response, tif1_category)
    theirs = _fastf1_frame(response, fastf1_category)
    shared = [column for column in theirs.columns if column in ours.columns]
    assert list(theirs.columns) == [
        column for column in ours.columns if column not in TIF1_EXTRA_COLUMNS
    ]
    assert len(ours) == len(theirs)
    for column in shared:
        if column in VALUE_EXCEPTIONS:
            continue
        left = ours[column].reset_index(drop=True)
        right = theirs[column].reset_index(drop=True)
        pd.testing.assert_series_equal(left, right, check_dtype=False, check_names=False)
    for column in ours.columns:
        if column in TIF1_EXTRA_COLUMNS:
            continue
        assert ours[column].dtype == theirs[column].dtype, column


@pytest.mark.compliance_offline
class TestFlatteningParity:
    def test_race_results(self):
        race = _load("stats/2025/chinese-grand-prix/results.json")["MRData"]["RaceTable"]["Races"][
            0
        ]
        _compare(race["Results"], structure.RaceResults, fastf1_structure.RaceResults)
        description = [{k: v for k, v in race.items() if k != "Results"}]
        _compare(description, structure.Races_RaceResults, fastf1_structure.Races_RaceResults)

    def test_total_race_time_differs_only_for_the_gap(self):
        race = _load("stats/2025/chinese-grand-prix/results.json")["MRData"]["RaceTable"]["Races"][
            0
        ]
        ours = _tif1_frame(race["Results"], structure.RaceResults)
        theirs = _fastf1_frame(race["Results"], fastf1_structure.RaceResults)
        # fastf1 parses the gap into totalRaceTime for every driver but the winner.
        assert ours["totalRaceTime"].iloc[0] == theirs["totalRaceTime"].iloc[0]
        assert ours["totalRaceTimeGap"].iloc[1] == theirs["totalRaceTime"].iloc[1]
        assert ours["totalRaceTime"].iloc[1] == pd.Timedelta(milliseconds=5464774)

    def test_qualifying_and_sprint(self):
        quali = _load("stats/2025/chinese-grand-prix/quali_results.json")["MRData"]["RaceTable"][
            "Races"
        ][0]
        _compare(
            quali["QualifyingResults"],
            structure.QualifyingResults,
            fastf1_structure.QualifyingResults,
        )
        sprint = _load("stats/2025/chinese-grand-prix/sprint_results.json")["MRData"]["RaceTable"][
            "Races"
        ][0]
        _compare(sprint["SprintResults"], structure.SprintResults, fastf1_structure.SprintResults)

    def test_laps_and_pitstops(self):
        laps = _load("stats/2025/chinese-grand-prix/laptimes.json")["MRData"]["RaceTable"]["Races"][
            0
        ]["Laps"]
        _compare(laps[:10], structure.Laps, fastf1_structure.Laps)
        stops = _load("stats/2025/chinese-grand-prix/pitstops.json")["MRData"]["RaceTable"][
            "Races"
        ][0]["PitStops"]
        _compare(stops, structure.PitStops, fastf1_structure.PitStops)

    def test_standings(self):
        drivers = _load("stats/2025/chinese-grand-prix/driverPoints.json")["MRData"][
            "StandingsTable"
        ]["StandingsLists"]
        _compare(
            drivers[0]["DriverStandings"],
            structure.DriverStandings,
            fastf1_structure.DriverStandings,
        )
        description = [{k: v for k, v in drivers[0].items() if k != "DriverStandings"}]
        _compare(
            description, structure.StandingsLists_Driver, fastf1_structure.StandingsLists_Driver
        )
        teams = _load("stats/2025/chinese-grand-prix/teamPoints.json")["MRData"]["StandingsTable"][
            "StandingsLists"
        ]
        _compare(
            teams[0]["ConstructorStandings"],
            structure.ConstructorStandings,
            fastf1_structure.ConstructorStandings,
        )

    def test_schedule_seasons_drivers_status(self):
        races = _load("stats/2025/events.json")["MRData"]["RaceTable"]["Races"]
        _compare(races, structure.Races_Schedule, fastf1_structure.Races_Schedule)
        seasons = _load("jolpica/seasons_limit5.json")["MRData"]["SeasonTable"]["Seasons"]
        _compare(seasons, structure.Seasons, fastf1_structure.Seasons)
        drivers = _load("jolpica/drivers_2024.json")["MRData"]["DriverTable"]["Drivers"]
        _compare(drivers, structure.Drivers, fastf1_structure.Drivers)
        status = _load("jolpica/status_2024.json")["MRData"]["StatusTable"]["Status"]
        _compare(status, structure.Status, fastf1_structure.Status)
        circuits = _load("jolpica/circuits_2024.json")["MRData"]["CircuitTable"]["Circuits"]
        _compare(circuits, structure.Circuits, fastf1_structure.Circuits)
        constructors = _load("jolpica/constructors_2024.json")["MRData"]["ConstructorTable"][
            "Constructors"
        ]
        _compare(constructors, structure.Constructors, fastf1_structure.Constructors)

    def test_raw_auto_cast_matches(self):
        race = _load("stats/2025/chinese-grand-prix/results.json")["MRData"]["RaceTable"]["Races"]
        ours = structure.auto_cast(json.loads(json.dumps(race)), structure.Races_RaceResults)
        theirs = fastf1_interface.ErgastRawResponse(
            query_result=race,
            category=fastf1_structure.Races_RaceResults,
            auto_cast=True,
            response_headers={},
            query_filters={},
            metadata={},
            selectors={},
        )
        assert ours[0]["Results"][0] == theirs[0]["Results"][0]
        assert ours[0]["Circuit"] == theirs[0]["Circuit"]


@pytest.mark.compliance_offline
def test_interface_signatures_match_fastf1():
    import inspect

    from tif1.ergast import Ergast

    fastf1_ergast = fastf1_interface.Ergast
    for name in (
        "get_seasons",
        "get_race_schedule",
        "get_driver_info",
        "get_constructor_info",
        "get_circuits",
        "get_finishing_status",
        "get_race_results",
        "get_qualifying_results",
        "get_sprint_results",
        "get_driver_standings",
        "get_constructor_standings",
        "get_lap_times",
        "get_pit_stops",
    ):
        ours = list(inspect.signature(getattr(Ergast, name)).parameters)
        theirs = list(inspect.signature(getattr(fastf1_ergast, name)).parameters)
        assert ours == theirs, name
    ours = inspect.signature(Ergast.__init__).parameters
    theirs = inspect.signature(fastf1_ergast.__init__).parameters
    assert [p for p in ours if p != "source"] == list(theirs)
