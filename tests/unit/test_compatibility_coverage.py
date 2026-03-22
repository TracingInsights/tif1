from unittest.mock import MagicMock

import pandas as pd
import pytest

from tif1.core import DriverResult, Lap, Laps, SessionResults, Telemetry, get_session
from tif1.events import Event, get_event, get_event_schedule
from tif1.exceptions import DataNotFoundError
from tif1.plotting import get_driver_color, get_team_color, setup_mpl
from tif1.utils import delta_time, recursive_dict_get, to_datetime, to_timedelta


def test_plotting_coverage():
    setup_mpl()
    with pytest.raises(ValueError, match="get_team_color requires a session"):
        get_team_color("Ferrari")
    with pytest.raises(ValueError, match="get_driver_color requires a session"):
        get_driver_color("VER")


def test_utils_coverage():
    td = to_timedelta("1:30.500")
    assert isinstance(td, pd.Timedelta)
    assert td.total_seconds() == 90.5

    td2 = to_timedelta(90.5)
    assert isinstance(td2, pd.Timedelta)
    assert td2.total_seconds() == 90.5

    dt = to_datetime("2025-01-01")
    assert isinstance(dt, pd.Timestamp)

    l1 = pd.Series(dtype=object)
    l1.get_telemetry = lambda: Telemetry()
    l2 = pd.Series(dtype=object)
    l2.get_telemetry = lambda: Telemetry()

    res, _, _ = delta_time(l1, l2)
    assert isinstance(res, pd.Series)

    d = {"a": {"b": 1}}
    assert recursive_dict_get(d, "a", "b") == 1
    assert recursive_dict_get(d, "a", "c") == {}
    assert recursive_dict_get(d, "a", "c", default_none=True) is None


def test_events_coverage():
    ev = get_event(2024, 1)
    assert isinstance(ev, Event)
    assert ev.year == 2024

    sched = get_event_schedule(2024)
    assert "EventName" in sched.columns

    ev2 = get_event(2024, "Monaco Grand Prix")
    assert ev2.EventName == "Monaco Grand Prix"

    ev2.get_session("Race")


def test_laps_methods_coverage():
    laps_df = pd.DataFrame(
        {
            "Driver": ["VER", "VER", "HAM"],
            "Team": ["Red Bull", "Red Bull", "Mercedes"],
            "LapTime": [
                pd.Timedelta(seconds=90),
                pd.Timedelta(seconds=91),
                pd.Timedelta(seconds=92),
            ],
            "Compound": ["SOFT", "SOFT", "MEDIUM"],
            "TrackStatus": ["1", "2", "1"],
            "IsAccurate": [True, True, False],
            "LapNumber": [1, 2, 1],
            "PitInTime": [pd.NaT, pd.NaT, pd.Timestamp("2025-01-01")],
            "PitOutTime": [pd.NaT, pd.Timestamp("2025-01-01"), pd.NaT],
            "Deleted": [False, False, True],
        }
    )
    laps = Laps(laps_df)

    assert len(laps.pick_driver("VER")) == 2
    assert len(laps.pick_drivers(["VER", "HAM"])) == 3
    assert len(laps.pick_team("Red Bull")) == 2
    assert len(laps.pick_teams(["Mercedes"])) == 1

    fastest = laps.pick_fastest()
    assert fastest["Driver"] == "VER"
    assert hasattr(fastest["LapTime"], "total_seconds")
    assert fastest["LapTime"].total_seconds() == 90

    # only_by_time branch
    fastest2 = laps.pick_fastest(only_by_time=True)
    assert fastest2["Driver"] == "VER"

    quick = laps.pick_quicklaps(threshold=1.01)
    assert len(quick) == 1

    assert len(laps.pick_tyre("SOFT")) == 2
    assert len(laps.pick_track_status("1")) == 2
    assert len(laps.pick_track_status("2", how="contains")) == 1

    assert len(laps.pick_accurate()) == 2
    assert len(laps.pick_wo_box()) == 1
    assert len(laps.pick_box_laps()) == 2
    assert len(laps.pick_not_deleted()) == 2

    ver_laps = laps.pick_driver("VER")
    try:
        ver_laps.get_telemetry()
    except Exception:
        pass

    for lap in laps.iterlaps():
        assert "Driver" in lap[1]
        assert lap["Driver"] in {"VER", "HAM"}
        break

    index, lap = next(laps.iterlaps(require=["Driver"]))
    assert index == 0
    assert lap["Driver"] == "VER"

    with pytest.raises(KeyError, match="required column 'Missing'"):
        next(laps.iterlaps(require=["Missing"]))


def test_driver_result_dnf():
    dr = DriverResult({"Status": "Finished"})
    assert not dr.dnf

    dr2 = DriverResult({"Status": "Collision"})
    assert dr2.dnf

    dr3 = DriverResult({"Status": "+1 Lap"})
    assert not dr3.dnf


def test_session_compatibility_properties():
    session = get_session(2024, "Monaco", "Race")
    session._drivers = [
        {
            "driver": "VER",
            "number": "1",
            "team": "Red Bull",
            "dn": "Max Verstappen",
            "fn": "Max",
            "ln": "Verstappen",
            "tc": "#0600ef",
        }
    ]

    assert session.session == "Race"
    assert hasattr(session.car_data, "__getitem__")
    assert hasattr(session.pos_data, "__getitem__")

    res = session.results
    assert isinstance(res, SessionResults)
    assert len(res) == 1
    assert res.iloc[0]["Abbreviation"] == "VER"
    assert res.iloc[0]["FullName"] == "Max Verstappen"

    ev = session.event
    assert ev.EventName == "Monaco Grand Prix"

    session.load()
    assert session.total_laps is None
    assert session.weather is session.weather


def test_telemetry_compatibility():
    tel = Telemetry({"Time": [0.1, 0.2], "Speed": [300, 301]})
    assert len(tel) == 2
    assert tel._constructor is Telemetry


def test_session_results_compatibility():
    sr = SessionResults({"A": [1, 2]})
    assert sr._constructor is SessionResults
    assert sr._constructor_sliced is DriverResult


def test_lap_properties():
    lap = Lap({"Driver": "VER", "LapNumber": 1})
    assert lap.driver == "VER"
    assert lap.lap_number == 1
    lap.session = None
    assert lap.telemetry.empty
    assert lap.get_telemetry().empty
    assert lap.get_weather_data().empty


def test_circuit_info():
    from tif1.core import CircuitInfo

    ci = CircuitInfo()
    assert ci.corners.empty
    assert list(ci.corners.columns) == ["X", "Y", "Number", "Letter", "Angle", "Distance"]
    assert ci.marshal_lights.empty
    assert ci.marshal_sectors.empty
    assert ci.rotation == 0.0


def test_session_data_not_found():
    session = get_session(2024, "Monaco", "Race")
    # Mock _load_session_table to raise DataNotFoundError
    session._load_session_table = MagicMock(
        side_effect=DataNotFoundError(year=2024, event="Monaco", session="Race")
    )
    # Clear any cached data
    session._race_control_messages = None
    session._weather = None

    assert session.race_control_messages.empty
    assert session.weather.empty


def test_laps_pick_compounds():
    laps = Laps(pd.DataFrame({"Compound": ["SOFT", "MEDIUM"], "Driver": ["VER", "HAM"]}))
    assert len(laps.pick_compounds(["SOFT"])) == 1
    assert len(laps.pick_compounds("MEDIUM")) == 1


def test_init_lazy_exports():
    import tif1

    assert tif1.plotting is not None
    assert tif1.utils is not None
    assert tif1.events is not None


def test_laps_pick_edge_cases():
    laps_df = pd.DataFrame({"Driver": ["VER"], "LapTime": [pd.NaT]})
    laps = Laps(laps_df)
    assert len(laps.pick_quicklaps()) == 1

    laps2 = Laps(pd.DataFrame({"A": [1]}))
    assert len(laps2.pick_wo_box()) == 1
    assert len(laps2.pick_box_laps()) == 1
    assert len(laps2.pick_not_deleted()) == 1
    assert len(laps2.pick_accurate()) == 1

    laps3 = Laps(pd.DataFrame({"Driver": ["VER", "HAM"]}))
    with pytest.raises(ValueError, match="Cannot retrieve telemetry for multiple drivers"):
        _ = laps3.telemetry


def test_laps_pick_driver_int():
    laps = Laps(pd.DataFrame({"Driver": ["1", "2"], "LapNumber": [1, 2]}))
    assert len(laps.pick_driver(1)) == 1


def test_laps_pick_track_status_other():
    laps = Laps(pd.DataFrame({"TrackStatus": ["1"], "Driver": ["VER"]}))
    assert len(laps.pick_track_status(1, how="invalid")) == 1


def test_session_get_fastest_laps_empty():
    session = get_session(2024, "Monaco", "Race")
    session._laps = Laps(pd.DataFrame(columns=["Driver", "LapTime", "IsAccurate"]))
    assert session.get_fastest_laps().empty


def test_lazy_telemetry_dict_key_error():
    session = get_session(2024, "Monaco", "Race")
    session._drivers = [{"driver": "VER", "number": "1"}]
    ltd = session.car_data
    with pytest.raises(KeyError):
        _ = ltd["XYZ"]
