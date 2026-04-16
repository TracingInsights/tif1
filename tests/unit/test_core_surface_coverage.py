"""Focused coverage tests for core dataframe-facing APIs."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from tif1 import core


def _make_laps_df() -> core.Laps:
    return core.Laps(
        pd.DataFrame(
            {
                "Driver": ["VER", "VER", "HAM"],
                "Team": ["Red Bull", "Red Bull", "Mercedes"],
                "LapNumber": [1, 2, 1],
                "LapTime": [90.1, 89.9, 91.0],
                "Compound": ["SOFT", "MEDIUM", "SOFT"],
                "TrackStatus": ["1", "14", "1"],
                "PitInTime": [pd.NaT, pd.NaT, pd.Timestamp("2025-01-01T00:00:01")],
                "PitOutTime": [pd.NaT, pd.Timestamp("2025-01-01T00:00:02"), pd.NaT],
                "Deleted": [False, True, False],
                "IsAccurate": [True, False, True],
            }
        ),
        session=SimpleNamespace(weather_data=pd.DataFrame({"AirTemp": [22.1]})),
    )


def test_laps_selection_and_compat_methods():
    laps = _make_laps_df()

    assert len(laps.pick_driver("VER")) == 2
    assert len(laps.pick_drivers([{"driver": "VER"}, 44])) == 2
    assert len(laps.pick_lap(2)) == 1
    assert len(laps.pick_laps(slice(1, 2))) == 3
    assert len(laps.pick_laps([2])) == 1
    assert len(laps.pick_team("Red Bull")) == 2
    assert len(laps.pick_teams(["Mercedes"])) == 1
    assert laps.pick_fastest() is not None
    assert len(laps.pick_quicklaps(1.0)) == 1
    assert len(laps.pick_tyre("SOFT")) == 2
    assert len(laps.pick_compounds(["MEDIUM"])) == 1
    assert len(laps.pick_track_status("1", how="equals")) == 2
    assert len(laps.pick_track_status("4", how="contains")) == 1
    assert len(laps.pick_wo_box()) == 1
    assert len(laps.pick_box_laps(which="in")) == 1
    assert len(laps.pick_box_laps(which="out")) == 1
    assert len(laps.pick_not_deleted()) == 2
    assert len(laps.pick_accurate()) == 2
    assert isinstance(laps.get_weather_data(), pd.DataFrame)
    q1, q2, q3 = laps.split_qualifying_sessions()
    assert len(q1) == len(q2) == len(q3) == len(laps)
    assert len(list(laps.iterlaps())) == len(laps)
    first_iter_lap = next(laps.iterlaps())
    assert first_iter_lap["Driver"] == "VER"

    _, first_lap = next(laps.iterlaps(require=["Driver"]))
    assert first_lap["Driver"] == "VER"


def test_laps_telemetry_and_reset_index_branches():
    laps = _make_laps_df().pick_driver("VER")
    laps["Level"] = [1, 2]
    telemetry_fetch = MagicMock(
        side_effect=lambda d, lap_num, ultra_cold=False, allow_prefetch=True: core.Telemetry(  # noqa: ARG005
            pd.DataFrame({"Time": ["0s"], "Speed": [300], "Driver": [d], "LapNumber": [lap_num]})
        )
    )

    session = SimpleNamespace(
        _resolve_telemetry_ultra_cold_mode=lambda _x: False,
        _get_telemetry_df_for_ref=telemetry_fetch,
        _record_telemetry_failure=lambda *_args, **_kwargs: None,
    )
    laps.session = session
    tel = laps.telemetry
    assert isinstance(tel, core.Telemetry)
    assert "Speed" in tel.columns
    assert telemetry_fetch.call_count == 2
    assert telemetry_fetch.call_args_list[0].kwargs["allow_prefetch"] is False
    assert telemetry_fetch.call_args_list[1].kwargs["allow_prefetch"] is False

    other = pd.DataFrame({"x": [1, 2]})
    assert "x" in laps.reset_index().join(other).columns
    assert "x" in laps.reset_index().merge(other, left_index=True, right_index=True).columns

    mixed = _make_laps_df()
    with pytest.raises(ValueError, match="Cannot retrieve telemetry"):
        _ = mixed.telemetry


def test_lap_paths_and_fetch_telemetry(monkeypatch):
    record_failure = MagicMock()
    telemetry_fetch = MagicMock(
        side_effect=lambda d, lap_num, ultra_cold=False, allow_prefetch=True: core.Telemetry(  # noqa: ARG005
            pd.DataFrame({"Time": ["0s"], "Speed": [100], "Driver": [d], "LapNumber": [lap_num]})
        )
    )
    session = SimpleNamespace(
        year=2025,
        gp="Abu Dhabi Grand Prix",
        session="Practice 1",
        enable_cache=True,
        _resolve_telemetry_ultra_cold_mode=lambda _x: False,
        _get_telemetry_df_for_ref=telemetry_fetch,
        _record_telemetry_failure=record_failure,
        _fetch_json=lambda _path: {"tel": {"speed": [100.0]}},
        _fetch_json_unvalidated=lambda _path: {"tel": {"speed": [101.0]}},
        _remember_telemetry_payload=MagicMock(),
        _should_backfill_ultra_cold_cache=lambda enabled: enabled,
        _schedule_background_cache_fill=MagicMock(),
        _mark_session_cache_populated=MagicMock(),
    )

    fake_cache = SimpleNamespace(set_telemetry=MagicMock())
    monkeypatch.setattr(core, "get_cache", lambda: fake_cache)

    lap = core.Lap({"Driver": "VER", "LapNumber": 5}, session=session)
    assert isinstance(lap.get_telemetry(), core.Telemetry)
    telemetry_fetch.assert_called_once_with("VER", 5, ultra_cold=False, allow_prefetch=False)
    assert isinstance(lap.get_car_data(), core.Telemetry)
    assert isinstance(lap.get_pos_data(), core.Telemetry)
    assert isinstance(lap.get_weather_data(), pd.Series)

    tel = lap._fetch_telemetry(ultra_cold=False)
    assert tel == {"speed": [100.0]}
    fake_cache.set_telemetry.assert_called_once()

    tel_uc = lap._fetch_telemetry(ultra_cold=True)
    assert tel_uc == {"speed": [101.0]}
    session._schedule_background_cache_fill.assert_called()

    bad_session = SimpleNamespace(
        _resolve_telemetry_ultra_cold_mode=lambda _x: False,
        _get_telemetry_df_for_ref=lambda _d, _lap_num, _ultra_cold=False, _allow_prefetch=True: (
            _ for _ in ()
        ).throw(core.NetworkError(url="x")),
        _record_telemetry_failure=record_failure,
    )
    bad_lap = core.Lap({"Driver": "VER", "LapNumber": 6}, session=bad_session)
    empty_tel = bad_lap.telemetry
    assert isinstance(empty_tel, core.Telemetry)
    assert empty_tel.empty
    assert record_failure.called


def test_telemetry_methods_cover_surface():
    tel = core.Telemetry(
        pd.DataFrame(
            {
                "Time": ["0s", "1s", "2s"],
                "Speed": [0.0, 180.0, 200.0],
                "LapNumber": [1, 1, 1],
                "TrackStatus": ["1", "1", "2"],
            }
        )
    )

    assert tel.get_first_non_zero_time_index() == 1
    assert isinstance(tel.fill_missing(), core.Telemetry)
    assert len(tel.integrate_distance()) == 3
    assert len(tel.calculate_differential_distance()) == 3
    assert "DifferentialDistance" in tel.add_differential_distance().columns
    assert "Distance" in tel.add_distance().columns
    assert "RelativeDistance" in tel.add_relative_distance().columns
    assert "DriverAhead" in tel.add_driver_ahead().columns
    assert "TrackStatus" in tel.add_track_status().columns
    assert len(tel.slice_by_mask(tel["Speed"] > 0)) == 2
    assert len(tel.slice_by_time("0.5s", "1.5s", pad=0.1)) >= 1
    assert len(tel.slice_by_lap(1)) == 3
    assert len(tel.slice_by_lap(pd.Series({"LapNumber": 1}))) == 3
    assert len(tel.slice_by_lap(pd.DataFrame({"LapNumber": [1]}))) == 3

    other = core.Telemetry(pd.DataFrame({"Time": ["0s", "1s", "2s"], "Throttle": [10, 20, 30]}))
    merged = tel.merge_channels(other)
    assert "Throttle" in merged.columns

    merged_no_time = core.Telemetry(pd.DataFrame({"A": [1, 2]})).merge_channels(
        core.Telemetry(pd.DataFrame({"B": [3, 4]}))
    )
    assert "B" in merged_no_time.columns

    resampled = tel.resample_channels("1s")
    assert isinstance(resampled, core.Telemetry)
    assert not resampled.empty
    assert isinstance(tel.base_class_view(), pd.DataFrame)
    assert isinstance(tel.join(pd.DataFrame({"x": [1, 2, 3]})), core.Telemetry)
    assert isinstance(
        tel.merge(pd.DataFrame({"Time": ["0s", "1s", "2s"]}), on="Time"), core.Telemetry
    )


def test_telemetry_slice_by_time_numeric_seconds_and_pad_side():
    tel = core.Telemetry(
        pd.DataFrame(
            {
                "Time": [0.0, 0.5, 1.0, 1.5, 2.0],
                "Speed": [100, 110, 120, 130, 140],
            }
        )
    )

    both = tel.slice_by_time("0.5s", "1.0s", pad=1, pad_side="both")
    before = tel.slice_by_time("0.5s", "1.0s", pad=1, pad_side="before")
    after = tel.slice_by_time("0.5s", "1.0s", pad=1, pad_side="after")

    assert len(both) == 4
    assert len(before) == 3
    assert len(after) == 3
    assert pd.to_timedelta(both["Time"].iloc[1]) == pd.Timedelta(0)


def test_telemetry_slice_by_lap_uses_lap_time_window():
    tel = core.Telemetry(
        pd.DataFrame(
            {
                "SessionTime": [0.0, 1.0, 2.0, 3.0, 4.0],
                "Time": [0.0, 1.0, 2.0, 3.0, 4.0],
                "Speed": [100, 110, 120, 130, 140],
                "LapNumber": [1, 1, 1, 2, 2],
            }
        )
    )
    ref_laps = core.Laps(
        pd.DataFrame(
            {
                "DriverNumber": ["1"],
                "LapStartTime": [pd.Timedelta(seconds=1)],
                "Time": [pd.Timedelta(seconds=3)],
                "LapNumber": [1],
            }
        )
    )

    sliced = tel.slice_by_lap(ref_laps)
    assert len(sliced) == 3
    assert pd.to_timedelta(sliced["Time"].iloc[0]) == pd.Timedelta(0)
    assert pd.to_timedelta(sliced["Time"].iloc[-1]) == pd.Timedelta(seconds=2)


def test_telemetry_slice_by_lap_multiple_drivers_raises():
    tel = core.Telemetry(
        pd.DataFrame(
            {
                "SessionTime": [0.0, 1.0, 2.0],
                "Time": [0.0, 1.0, 2.0],
                "Speed": [100, 110, 120],
                "LapNumber": [1, 1, 1],
            }
        )
    )
    ref_laps = core.Laps(
        pd.DataFrame(
            {
                "DriverNumber": ["1", "44"],
                "LapStartTime": [pd.Timedelta(seconds=0), pd.Timedelta(seconds=1)],
                "Time": [pd.Timedelta(seconds=1), pd.Timedelta(seconds=2)],
                "LapNumber": [1, 1],
            }
        )
    )

    with pytest.raises(ValueError, match="contains Laps of multiple drivers"):
        tel.slice_by_lap(ref_laps)


def test_results_circuit_and_name_resolution_helpers(monkeypatch):
    driver_result = core.DriverResult({"Status": "Finished"})
    assert driver_result.dnf is False
    assert core.DriverResult({"Status": "DNF"}).dnf is True

    circuit = core.CircuitInfo()
    assert circuit.corners.empty
    assert circuit.rotation == 0.0

    # add_marker_distance: modifies corners in-place using the telemetry of a
    # reference Lap.  We provide a Lap whose .telemetry returns a Telemetry
    # DataFrame with the three required columns.
    tel_df = core.Telemetry(
        pd.DataFrame(
            {
                "X": [0.0, 10.0, 20.0],
                "Y": [0.0, 5.0, 10.0],
                "Distance": [0.0, 15.0, 30.0],
            }
        )
    )
    mock_lap = MagicMock()
    type(mock_lap).telemetry = property(lambda _self: tel_df)

    # Populate corners so the algorithm has something to fit
    circuit.corners = pd.DataFrame(
        {
            "X": [1.0, 11.0],
            "Y": [1.0, 6.0],
            "Number": [1, 2],
            "Letter": ["", ""],
            "Angle": [0.0, 45.0],
            "Distance": [float("nan"), float("nan")],
        }
    )

    circuit.add_marker_distance(mock_lap)  # in-place, returns None
    assert not circuit.corners["Distance"].isna().any()
    assert circuit.corners["Distance"].iloc[0] == 0.0  # closest to (0,0) → Distance=0
    assert circuit.corners["Distance"].iloc[1] == 15.0  # closest to (10,5) → Distance=15

    assert core._normalize_event_key("Abu Dhabi Grand Prix!") == "abu dhabi"
    assert core._normalize_session_name("FP1") == "Practice 1"
    assert core._normalize_session_name("Unknown") == "Unknown"

    monkeypatch.setattr(core, "_resolve_gp_name", lambda _y, _g: "Abu Dhabi Grand Prix")
    monkeypatch.setattr(core, "_resolve_session_name", lambda _y, _gp, _s: "Practice 1")
    monkeypatch.setattr(
        "tif1.events.get_sessions",
        lambda _y, _gp: ["Practice 1", "Practice 2"],
    )
    monkeypatch.setattr(core, "Session", lambda *args: ("session", args))
    created = core.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
    assert created[0] == "session"

    monkeypatch.setattr(core, "_resolve_session_name", lambda _y, _gp, _s: "Race")
    with pytest.raises(ValueError, match="does not exist"):
        core.get_session(2025, "Abu Dhabi Grand Prix", "Race")


def test_resolve_helpers_with_event_module(monkeypatch):
    monkeypatch.setattr(
        "tif1.events.get_event", lambda _y, _r: SimpleNamespace(EventName="Test GP")
    )
    assert core._resolve_gp_name(2025, 1) == "Test GP"

    monkeypatch.setattr(
        "tif1.events.get_event_by_name",
        lambda y, n, exact_match=False: SimpleNamespace(EventName="Matched GP"),  # noqa: ARG005
    )
    assert core._resolve_gp_name(2025, "matched") == "Matched GP"

    monkeypatch.setattr(
        "tif1.events.get_event_by_name",
        lambda y, n, exact_match=False: (_ for _ in ()).throw(ValueError("bad")),  # noqa: ARG005
    )
    assert core._resolve_gp_name(2025, "raw name") == "raw name"

    monkeypatch.setattr("tif1.events.get_sessions", lambda _y, _gp: ["FP1", "Race"])
    assert core._resolve_session_name(2025, "Any", 2) == "Race"
    with pytest.raises(ValueError, match="out of range"):
        core._resolve_session_name(2025, "Any", 9)


def test_driver_and_lapinternal_paths(monkeypatch):
    session = SimpleNamespace(
        lib="pandas",
        _laps=pd.DataFrame(
            {
                "Driver": ["VER", "HAM", "VER"],
                "LapNumber": [1, 1, 2],
                "LapTime": [90.5, 91.1, 89.9],
                "Team": ["Red Bull", "Mercedes", "Red Bull"],
            }
        ),
        _get_driver_info=lambda _d: {
            "fn": "Max",
            "ln": "Verstappen",
            "dn": "1",
            "team": "Red Bull",
        },
        year=2025,
        gp="Test GP",
        session="Race",
        _lap_time_sort_column=lambda _df: "LapTime",
        _resolve_ultra_cold_mode=lambda _x: False,
        _get_local_payload=lambda _path: None,
        _get_or_derive_driver_laptime_payload=lambda _d: None,
        _extract_fastest_lap_candidate=lambda _d, _payload: None,
        get_fastest_laps_tels=lambda **_kwargs: pd.DataFrame({"Time": [0.1]}),
        _get_telemetry_df_for_ref=lambda d, lap_num, ultra_cold=False: pd.DataFrame(  # noqa: ARG005
            {"Time": [0.1], "Speed": [301], "Driver": [d], "LapNumber": [lap_num]}
        ),
    )

    driver = core.Driver(session, "VER")
    laps = driver.laps
    assert len(laps) == 2
    lap = driver.get_lap(1)
    assert isinstance(lap, core.Lap)
    assert int(lap["LapNumber"]) == 1
    with pytest.raises(core.LapNotFoundError):
        driver.get_lap(999)
    assert len(driver.get_fastest_lap()) == 1
    tel_df = driver.get_fastest_lap_tel()
    assert "Speed" in tel_df.columns

    # Exercise _LapInternal cache-first telemetry path
    rec = MagicMock()
    inner_session = SimpleNamespace(
        _resolve_telemetry_ultra_cold_mode=lambda _x: False,
        _get_telemetry_payload=lambda _d, _lap_num: None,
        enable_cache=True,
        _session_cache_available=lambda: True,
        year=2025,
        gp="Test GP",
        session="Race",
        _remember_telemetry_payload=MagicMock(),
        _should_skip_telemetry_fetch=lambda d: False,  # noqa: ARG005
        lib="pandas",
        _record_telemetry_failure=rec,
        _fetch_json=lambda _p: {"tel": {"speed": [300.0]}},
        _fetch_json_unvalidated=lambda _p: {"tel": {"speed": [301.0]}},
        _should_backfill_ultra_cold_cache=lambda enabled: enabled,
        _schedule_background_cache_fill=MagicMock(),
        _mark_session_cache_populated=MagicMock(),
    )
    fake_cache = SimpleNamespace(
        get_telemetry=lambda *_args: {"speed": [302.0]}, set_telemetry=MagicMock()
    )
    monkeypatch.setattr(core, "get_cache", lambda: fake_cache)

    li = core._LapInternal(inner_session, "VER", 10)
    tel = li.telemetry
    assert len(tel) == 1

    # Skip-fetch branch
    skip_session = SimpleNamespace(**inner_session.__dict__)
    skip_session._session_cache_available = lambda: False
    skip_session._should_skip_telemetry_fetch = lambda d: True  # noqa: ARG005
    li_skip = core._LapInternal(skip_session, "VER", 11)
    assert li_skip.telemetry.empty

    # Error branch records telemetry failure
    err_session = SimpleNamespace(**inner_session.__dict__)
    err_session._fetch_json = lambda p: (_ for _ in ()).throw(core.NetworkError(url="x"))  # noqa: ARG005
    err_session._session_cache_available = lambda: False
    err_session._should_skip_telemetry_fetch = lambda d: False  # noqa: ARG005
    li_err = core._LapInternal(err_session, "VER", 12)
    assert li_err.telemetry.empty
    assert rec.called


def test_telemetry_failure_throttling_paths(monkeypatch):
    monkeypatch.setattr(core, "_resolve_gp_name", lambda _y, _g: "Test GP")
    monkeypatch.setattr(core, "_resolve_session_name", lambda _y, _gp, _s: "Race")
    session = core.Session(2025, "Test GP", "Race", lib="pandas", enable_cache=True)

    err = core.NetworkError(url="x")
    session._record_telemetry_failure("VER", 1, err)
    session._record_telemetry_failure("VER", 2, err)
    session._record_telemetry_failure("VER", 3, err)
    session._record_telemetry_failure("VER", 4, err)
    session._record_telemetry_failure("VER", 5, err)

    assert session._telemetry_failure_counts["VER"] == 5
    assert "VER" in session._telemetry_unavailable_drivers
    assert "VER" in session._telemetry_failure_suppressed_drivers
    assert session._should_skip_telemetry_fetch("VER") is True
    assert session._should_skip_telemetry_fetch("HAM") is False


def test_ultra_cold_resolution_and_driver_load_warm_path(monkeypatch):
    monkeypatch.setattr(core, "_resolve_gp_name", lambda _y, _g: "Test GP")
    monkeypatch.setattr(core, "_resolve_session_name", lambda _y, _gp, _s: "Race")

    class SessionWithDrivers(core.Session):
        @property
        def drivers(self):
            self._drivers = [{"driver": "VER"}]
            return self._drivers

    session = SessionWithDrivers(2025, "Test GP", "Race", lib="pandas", enable_cache=True)
    session._session_cache_available = lambda: False

    assert session._resolve_telemetry_ultra_cold_mode(False) is True

    drivers, payloads = session._load_drivers_for_fastest_lap_reference(ultra_cold=False)
    assert drivers == [{"driver": "VER"}]
    assert payloads == []

    class SessionWithFallbackDrivers(core.Session):
        @property
        def drivers(self):
            self._drivers = [{"driver": "HAM"}]
            return self._drivers

    fallback = SessionWithFallbackDrivers(2025, "Test GP", "Race", lib="pandas", enable_cache=True)
    fallback._fetch_json_unvalidated = lambda path: (_ for _ in ()).throw(
        core.NetworkError(url=path)
    )
    fallback_drivers, fallback_payloads = fallback._load_drivers_for_fastest_lap_reference(
        ultra_cold=True
    )
    assert fallback_drivers == [{"driver": "HAM"}]
    assert fallback_payloads == []
