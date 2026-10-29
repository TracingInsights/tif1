"""fastf1 parity on real sessions (network): session state, car data, drivers, circuit info.

2024 Bahrain (race and qualifying), the 2024 Canadian GP race (two safety
cars), the 2022 Bahrain race (old-schema ``corners.json``: marshal lights from
the MultiViewer backup), the 2025 Bahrain race (new-schema ``corners.json``)
and one Ergast-only race (2017 Australian GP). pandas backend throughout.
"""

from __future__ import annotations

import pandas as pd
import pytest

import tif1
from tif1.models import DriverResult, Telemetry

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def bahrain_2024_race():
    return tif1.get_session(2024, "Bahrain Grand Prix", "Race", lib="pandas")


@pytest.fixture(scope="module")
def bahrain_2024_qualifying():
    return tif1.get_session(2024, "Bahrain Grand Prix", "Qualifying", lib="pandas")


@pytest.fixture(scope="module")
def canada_2024_race():
    return tif1.get_session(2024, "Canadian Grand Prix", "Race", lib="pandas")


@pytest.fixture(scope="module")
def australia_2017_race():
    return tif1.get_session(2017, "Australian Grand Prix", "Race", lib="pandas")


# -- 2024 Bahrain race ----------------------------------------------------------------


def test_car_data_is_one_session_long_frame(bahrain_2024_race):
    tel = bahrain_2024_race.car_data["1"]
    assert isinstance(tel, Telemetry)
    assert not tel.empty
    assert tel["SessionTime"].notna().all()
    assert tel["SessionTime"].is_monotonic_increasing
    assert bahrain_2024_race.car_data["VER"] is tel


def test_lap_telemetry_session_clock(bahrain_2024_race):
    lap = bahrain_2024_race.laps.pick_drivers("VER").pick_laps(10).iloc[0]
    tel = lap.telemetry
    assert list(tel.columns[:2]) == ["Date", "SessionTime"]
    assert (tel["SessionTime"] - tel["Time"]).eq(lap["LapStartTime"]).all()
    assert (tel["Date"] - tel["Time"]).eq(lap["LapStartDate"]).all()


def test_t0_date_and_start_time(bahrain_2024_race):
    t0 = bahrain_2024_race.t0_date
    assert not pd.isna(t0)
    assert t0.value % 1_000_000 == 0
    assert bahrain_2024_race.session_start_time > pd.Timedelta(0)


def test_session_status_started_and_finished(bahrain_2024_race):
    status = bahrain_2024_race.session_status
    assert {"Started", "Finished"} <= set(status["Status"])
    assert status["Time"].is_monotonic_increasing


def test_get_driver_is_the_results_row(bahrain_2024_race):
    driver = bahrain_2024_race.get_driver("VER")
    assert isinstance(driver, DriverResult)
    assert driver["Position"] == 1.0
    assert driver.dnf is False


# -- 2024 Bahrain qualifying ------------------------------------------------------------


def test_qualifying_splits_into_three_segments(bahrain_2024_qualifying):
    q1, q2, q3 = bahrain_2024_qualifying.laps.split_qualifying_sessions()
    segments = [q1, q2, q3]
    assert all(segment is not None for segment in segments)
    assert all(len(segment) > 0 for segment in segments)
    assert q3["Driver"].nunique() <= 10


# -- 2024 Canadian GP race ------------------------------------------------------------------


def test_track_status_includes_the_safety_cars(canada_2024_race):
    track = canada_2024_race.track_status
    assert list(track.columns) == ["Time", "Status", "Message"]
    assert track["Time"].is_monotonic_increasing
    safety_cars = track[track["Status"] == "4"]
    assert len(safety_cars) >= 2
    assert safety_cars["Message"].eq("SCDeployed").all()


# -- circuit info ---------------------------------------------------------------------------


@pytest.mark.parametrize("year", [2022, 2025])
def test_marshal_frames_are_filled(year):
    session = tif1.get_session(year, "Bahrain Grand Prix", "Race", lib="pandas")
    info = session.get_circuit_info()
    assert not info.corners.empty
    assert not info.marshal_lights.empty
    assert not info.marshal_sectors.empty


def test_old_schema_corners_use_the_multiviewer_backup():
    info = tif1.get_session(2022, "Bahrain Grand Prix", "Race", lib="pandas").get_circuit_info()
    assert "multiviewer" in info.source


# -- Ergast-only race -----------------------------------------------------------------------


def test_ergast_only_race_has_results_but_no_timing_clock(australia_2017_race):
    session = australia_2017_race
    assert session.f1_api_support is False
    driver = session.get_driver("VET")
    assert driver["Position"] == 1.0
    assert driver.dnf is False
    assert session.t0_date is pd.NaT
    assert session.track_status["Status"].tolist() == ["1"]
    assert session.session_status.empty
