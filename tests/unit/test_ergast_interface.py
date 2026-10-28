"""The ``Ergast`` interface: response classes, paging, result types, legacy helpers."""

from __future__ import annotations

import datetime
from urllib.parse import urlsplit

import pandas as pd
import pytest

import tif1
from tif1.ergast import (
    Ergast,
    ErgastMultiResponse,
    ErgastRawResponse,
    ErgastResultFrame,
    ErgastSimpleResponse,
    fetch_day,
    fetch_results,
    fetch_season,
)
from tif1.ergast.interface import BASE_URL, HEADERS, TIMEOUT


class TestPackage:
    def test_lazy_exports(self):
        import tif1.ergast as package

        assert package.Ergast is Ergast
        assert tif1.Ergast is Ergast
        assert urlsplit(BASE_URL).hostname == "api.jolpi.ca"
        assert TIMEOUT == 5.0
        assert "User-Agent" in HEADERS
        with pytest.raises(AttributeError):
            _ = package.nope
        assert "structure" in dir(package)

    def test_constructor_validation(self):
        with pytest.raises(ValueError, match="result_type"):
            Ergast(result_type="json")
        with pytest.raises(ValueError, match="source"):
            Ergast(source="stats-only")

    def test_build_url_static(self):
        url = Ergast._build_url("results", season=2024, round=1, driver="alonso")
        assert url.endswith("/2024/1/drivers/alonso/results.json")


@pytest.mark.usefixtures("ergast_offline")
class TestResponses:
    def test_multi_response_shape(self):
        response = Ergast().get_race_results(2025, 2)
        assert isinstance(response, ErgastMultiResponse)
        assert response.source == "stats"
        assert isinstance(response.description, ErgastResultFrame)
        assert response.description.loc[0, "raceName"] == "Chinese Grand Prix"
        content = response.content[0]
        assert isinstance(content, ErgastResultFrame)
        assert list(content.columns[:7]) == [
            "number",
            "position",
            "positionText",
            "points",
            "grid",
            "laps",
            "status",
        ]
        assert content["totalRaceTime"].iloc[1] == pd.Timedelta(milliseconds=5464774)
        assert content["totalRaceTimeGap"].iloc[1] == pd.Timedelta(seconds=9.748)
        assert content["dateOfBirth"].dtype.kind == "M"
        assert response.is_complete
        assert response.total_results == 20
        with pytest.raises(ValueError, match="No more data"):
            response.get_next_result_page()
        with pytest.raises(ValueError, match="No more data"):
            response.get_prev_result_page()

    def test_simple_response_and_slicing_drops_mixin(self):
        schedule = Ergast().get_race_schedule(2025)
        assert isinstance(schedule, ErgastSimpleResponse)
        assert schedule.total_results == 24
        assert schedule.is_complete
        sliced = schedule[schedule["round"] == 2]
        assert isinstance(sliced, ErgastResultFrame)
        assert not isinstance(sliced, ErgastSimpleResponse)
        assert sliced["circuitId"].tolist() == ["shanghai"]
        row = schedule.iloc[1]
        assert row["raceName"] == "Chinese Grand Prix"

    def test_paging(self):
        page = Ergast(limit=5).get_race_results(2025, 2)
        assert len(page.content[0]) == 5
        assert not page.is_complete
        assert page.total_results == 20
        nxt = page.get_next_result_page()
        assert nxt.content[0]["position"].tolist() == [6, 7, 8, 9, 10]
        prev = nxt.get_prev_result_page()
        assert prev.content[0]["position"].tolist() == [1, 2, 3, 4, 5]
        last = Ergast().get_race_results(2025, 2, limit=5, offset=15)
        assert last.is_complete is False
        with pytest.raises(ValueError, match="No more data after"):
            last.get_next_result_page()

    def test_raw_response(self):
        raw = Ergast(result_type="raw").get_race_results(2025, 2)
        assert isinstance(raw, ErgastRawResponse)
        assert raw.source == "stats"
        assert raw[0]["Results"][0]["points"] == 25.0
        assert isinstance(raw[0]["Results"][0]["Time"]["time"], datetime.timedelta)
        uncast = Ergast(result_type="raw", auto_cast=False).get_race_results(2025, 2)
        assert uncast[0]["Results"][0]["points"] == "25"
        uncast[0]["Results"][0]["points"] = "mutated"
        again = Ergast(result_type="raw", auto_cast=False).get_race_results(2025, 2)
        assert again[0]["Results"][0]["points"] == "25"  # the memo was never handed out

    def test_polars_result_type(self):
        pl = pytest.importorskip("polars")
        response = Ergast(result_type="polars").get_race_results(2025, 2)
        assert isinstance(response.content[0], pl.DataFrame)
        assert isinstance(response.description, pl.DataFrame)
        assert response.content[0].height == 20
        schedule = Ergast(result_type="polars").get_race_schedule(2025)
        from tif1.ergast import ErgastPolarsResponse

        assert isinstance(schedule, ErgastPolarsResponse)
        assert isinstance(schedule, pl.DataFrame)
        assert schedule.height == 24
        # The polars twin of ErgastSimpleResponse: metadata and paging included.
        assert schedule.source == "stats"
        assert schedule.total_results == 24
        assert schedule.is_complete
        assert type(schedule.head(2)) is pl.DataFrame  # derived frames drop the metadata
        assert type(schedule.filter(pl.col("round") == 2)) is pl.DataFrame
        page = Ergast(result_type="polars", limit=5).get_race_schedule(2025)
        assert page.height == 5
        assert not page.is_complete
        nxt = page.get_next_result_page()
        assert isinstance(nxt, ErgastPolarsResponse)
        assert nxt["round"].to_list() == [6, 7, 8, 9, 10]
        assert nxt.get_prev_result_page()["round"].to_list() == [1, 2, 3, 4, 5]

    def test_every_endpoint_answers(self):
        ergast = Ergast()
        assert len(ergast.get_qualifying_results(2025, 2).content[0]) == 20
        assert len(ergast.get_sprint_results(2025, 2).content[0]) == 20
        laps = ergast.get_lap_times(2025, 2, lap_number=1)
        assert len(laps.content[0]) == 20
        assert laps.content[0]["number"].tolist() == [1] * 20
        stops = ergast.get_pit_stops(2025, 2, driver="norris")
        assert stops.content[0]["driverId"].tolist() == ["norris"]
        standings = ergast.get_driver_standings(2025, 2)
        assert standings.content[0]["driverCode"].iloc[0] == "NOR"
        assert standings.description["round"].tolist() == [2]
        constructors = ergast.get_constructor_standings(2025, 2, standings_position=1)
        assert constructors.content[0]["constructorId"].tolist() == ["mclaren"]
        circuits = ergast.get_circuits(season=2025)
        assert len(circuits) == 24
        assert circuits.source == "stats"
        info = ergast.get_constructor_info(season=2025, round=2)
        assert len(info) == 10
        seasons = ergast.get_seasons(limit=5)
        assert seasons.source == "jolpica"
        assert seasons["season"].tolist() == [1950, 1951, 1952, 1953, 1954]
        drivers = ergast.get_driver_info(season=2024, limit=30)
        assert drivers.source == "jolpica"
        assert "antonelli" in drivers["driverId"].tolist()
        status = ergast.get_finishing_status(season=2024)
        assert status.source == "jolpica"
        assert status["statusId"].tolist()[0] == 1

    def test_source_pinning(self):
        from tif1.exceptions import DataNotFoundError

        with pytest.raises(DataNotFoundError):
            Ergast(source="stats").get_seasons()
        assert Ergast(source="jolpica").get_driver_info(season=2024, limit=30).source == "jolpica"

    def test_legacy_helpers(self):
        races = fetch_season(2025)
        assert len(races) == 24
        assert races[1]["raceName"] == "Chinese Grand Prix"
        day = fetch_day(2025, 2, "qualifying")
        assert day[0]["QualifyingResults"][0]["Driver"]["driverId"] == "piastri"
        assert fetch_results(2025, 2, "Race")[0]["Driver"]["driverId"] == "piastri"
        assert fetch_results(2025, 2, "Sprint")[0]["Driver"]["driverId"] == "hamilton"
        with pytest.raises(ValueError, match="Unsupported"):
            fetch_day(2025, 2, "practice")


@pytest.mark.usefixtures("ergast_offline")
class TestStandingsHelpers:
    def test_top_level_standings(self):
        drivers = tif1.get_driver_standings(2025, 2)
        assert drivers.loc[0, "Abbreviation"] == "NOR"
        assert drivers.loc[0, "Points"] == 44.0
        assert drivers.loc[0, "TeamNames"] == ["McLaren"]
        teams = tif1.get_constructor_standings(2025, 2)
        assert teams.loc[0, "TeamName"] == "McLaren"
        assert teams.loc[0, "Points"] == 78.0

    def test_polars_standings(self):
        pl = pytest.importorskip("polars")
        frame = tif1.get_driver_standings(2025, 2, lib="polars")
        assert isinstance(frame, pl.DataFrame)
        assert frame.height == 20
