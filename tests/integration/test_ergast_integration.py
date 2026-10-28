"""Ergast integration smoke checks against the real Stats repository and Jolpica."""

from __future__ import annotations

import pandas as pd
import pytest

import tif1
from tif1.ergast import Ergast


@pytest.mark.integration
class TestErgastIntegration:
    def test_race_results_from_stats(self):
        response = Ergast().get_race_results(2024, 1)
        assert response.source == "stats"
        assert response.is_complete
        content = response.content[0]
        assert content.loc[0, "driverId"] == "max_verstappen"
        assert content.loc[0, "totalRaceTime"] > pd.Timedelta(hours=1)

    def test_season_assembly_and_paging(self):
        response = Ergast().get_race_results(2024)
        assert response.source == "stats"
        assert len(response.content) == 24
        assert response.total_results == sum(len(c) for c in response.content)
        page = Ergast(limit=30).get_race_results(2024)
        assert not page.is_complete
        assert sum(len(c) for c in page.content) == 30

    def test_jolpica_only_endpoint(self):
        seasons = Ergast().get_seasons(limit=3)
        assert seasons.source == "jolpica"
        assert seasons["season"].tolist() == [1950, 1951, 1952]

    def test_lap_times_merge_page_splits(self):
        laps = Ergast().get_lap_times(2024, 1)
        content = laps.content[0]
        assert content["number"].is_monotonic_increasing
        assert laps.total_results == len(content)

    def test_session_results_merge(self):
        session = tif1.get_session(2024, "Bahrain Grand Prix", "Race")
        results = session.results
        assert results.loc["1", "Points"] == 26.0
        assert results.loc["1", "DriverId"] == "max_verstappen"
        assert session.total_laps == 57
        assert session.drivers[0] == "1"
        assert len(session.pit_stops) > 0

    def test_pre_2018_session(self):
        session = tif1.get_session(2010, "Abu Dhabi Grand Prix", "Race")
        results = session.results
        assert results.iloc[0]["DriverId"] == "vettel"
        laps = session.laps
        assert len(laps) > 1000
        assert laps["Driver"].str.len().eq(3).all()

    def test_standings_helpers(self):
        drivers = tif1.get_driver_standings(2023)
        assert drivers.iloc[0]["Abbreviation"] == "VER"
        teams = tif1.get_constructor_standings(2023)
        assert teams.iloc[0]["TeamName"] == "Red Bull"
