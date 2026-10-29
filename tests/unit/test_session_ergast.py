"""The Ergast classification merged into ``Session``: results, drivers, extras, history."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pandas as pd
import pytest

import tif1
from tif1.config import get_config
from tif1.core import Session
from tif1.core_utils.constants import FASTF1_RESULTS_COLUMNS, TIF1_RESULTS_EXTRA_COLUMNS
from tif1.core_utils.ergast_results import (
    broadcast_name,
    build_laps_payloads,
    finalize_results_frame,
    merge_ergast_rows,
    order_driver_rows,
    session_kind,
    unique_driver_labels,
)
from tif1.exceptions import DataNotFoundError, NetworkError
from tif1.models import DriverResult, Laps
from tif1.payload_loader import InMemoryTransport

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ergast" / "stats"


def _results_rows(path: str = "2025/chinese-grand-prix/results.json") -> list[dict]:
    payload = json.loads((FIXTURES / path).read_text(encoding="utf-8"))
    race = payload["MRData"]["RaceTable"]["Races"][0]
    return race.get("Results") or race.get("QualifyingResults") or race.get("SprintResults")


def _drivers_payload(skip: str = "BOR") -> dict:
    """A TracingInsights drivers.json, number-sorted, one driver missing."""
    rows = []
    for row in _results_rows():
        driver, constructor = row["Driver"], row["Constructor"]
        if driver["code"] == skip:
            continue
        rows.append(
            {
                "driver": driver["code"],
                "team": constructor["name"],
                "dn": row["number"],
                "fn": driver["givenName"],
                "ln": driver["familyName"],
                "tc": "FF8000" if constructor["constructorId"] == "mclaren" else "",
                "url": "",
            }
        )
    return {"drivers": sorted(rows, key=lambda d: int(d["dn"]))}


def _session(session_name: str = "Race", **kwargs) -> Session:
    session = tif1.get_session(
        2025, "Chinese Grand Prix", session_name, enable_cache=False, **kwargs
    )
    session._payload_loader.transport = InMemoryTransport({"drivers.json": _drivers_payload()})
    return session


@pytest.mark.usefixtures("ergast_offline")
class TestRaceMerge:
    def test_drivers_follow_official_classification(self):
        session = _session()
        assert session.drivers[:4] == ["81", "4", "63", "1"]
        assert len(session.drivers) == 20  # the did-not-appear driver is added back
        assert session.drivers_df["Driver"].tolist()[:2] == ["PIA", "NOR"]

    def test_results_columns_and_values(self):
        results = _session().results
        assert list(results.columns) == list(FASTF1_RESULTS_COLUMNS + TIF1_RESULTS_EXTRA_COLUMNS)
        assert results.index.tolist()[:3] == ["81", "4", "63"]
        first = results.iloc[0]
        assert first["Abbreviation"] == "PIA"
        assert first["Position"] == 1.0
        assert first["ClassifiedPosition"] == "1"
        assert first["GridPosition"] == 1.0
        assert first["Points"] == 25.0
        assert first["Status"] == "Finished"
        assert first["Laps"] == 56.0
        assert first["DriverId"] == "piastri"
        assert first["TeamId"] == "mclaren"
        assert first["BroadcastName"] == "O PIASTRI"
        assert first["Time"] == pd.Timedelta(milliseconds=5455026)
        assert first["TimeGap"] == pd.Timedelta(0)
        assert first["TeamColor"] == "FF8000"
        second = results.iloc[1]
        assert second["Time"] == pd.Timedelta(milliseconds=5464774)
        assert second["TimeGap"] == pd.Timedelta(seconds=9.748)
        assert second["FastestLapRank"] == 1.0
        assert second["FastestLapTime"] == pd.Timedelta("0 days 00:01:35.454")
        assert second["DriverNationality"] == "British"
        assert second["DateOfBirth"] == pd.Timestamp("1999-11-13")
        assert results["Position"].dtype == "float64"
        assert results["Time"].dtype == "timedelta64[ns]"
        assert results["Q1"].dtype == "timedelta64[ns]"
        assert results["Q1"].isna().all()
        assert results["CountryCode"].eq("").all()

    def test_did_not_appear_driver_is_synthesized(self):
        results = _session().results
        row = results.loc["5"]
        assert row["Abbreviation"] == "BOR"
        assert row["FullName"] == "Gabriel Bortoleto"
        assert row["TeamName"] == "Sauber"
        assert pd.isna(row["Position"])
        assert pd.isna(row["GridPosition"])
        assert row["Status"] == "Lapped"
        assert row["DriverId"] == "bortoleto"
        assert results.index.tolist()[-1] == "5"  # NaN positions sort last
        driver = _session().get_driver("BOR")
        assert driver["LastName"] == "Bortoleto"
        assert driver["DriverId"] == "bortoleto"
        assert driver.laps.empty  # no timing data for a driver only Ergast lists
        from tif1.models import Driver

        fresh = _session()
        assert Driver(fresh, "PIA")["DriverId"] == "piastri"  # identity loads on demand

    def test_total_laps_and_dnf(self):
        session = _session()
        assert session.total_laps == 56
        assert not session.results.loc["81"].dnf
        assert not session.results.loc["5"].dnf  # "Lapped" is a finish
        lapped_or_out = session.results[~session.results["Status"].isin(["Finished", "Lapped"])]
        assert all(row.dnf for _, row in lapped_or_out.iterrows())

    def test_one_stats_fetch_for_results_and_drivers(self, ergast_offline):
        session = _session()
        _ = session.drivers
        _ = session.results
        _ = session.drivers_df
        stats_calls = [c for c in ergast_offline.calls if "chinese-grand-prix" in c]
        assert stats_calls == [c for c in stats_calls if c.endswith("results.json")]
        assert len(stats_calls) == 1

    def test_pit_stops_and_standings(self):
        session = _session()
        stops = session.pit_stops
        assert list(stops.columns) == [
            "DriverNumber",
            "Driver",
            "DriverId",
            "Stop",
            "Lap",
            "TimeOfDay",
            "Duration",
        ]
        assert len(stops) == 25
        assert stops.iloc[0]["Driver"] == "BOR"
        assert stops.iloc[0]["Duration"] == pd.Timedelta(seconds=23.385)
        assert stops.iloc[0]["TimeOfDay"] == pd.Timedelta(hours=15, minutes=5, seconds=40)
        drivers = session.driver_standings
        assert drivers.iloc[0]["Abbreviation"] == "NOR"
        assert drivers.iloc[0]["Points"] == 44.0
        teams = session.constructor_standings
        assert teams.iloc[0]["TeamName"] == "McLaren"
        assert session.pit_stops is stops  # memoized

    def test_polars_session_extras(self):
        pl = pytest.importorskip("polars")
        session = _session(lib="polars")
        assert isinstance(session.pit_stops, pl.DataFrame)
        assert isinstance(session.driver_standings, pl.DataFrame)
        results = session.results
        assert type(results).__name__ == "PolarsSessionResults"
        assert results.session is session

    @pytest.mark.parametrize("name", ["Race", "Qualifying", "Sprint"])
    def test_polars_results_equal_the_pandas_merge(self, name):
        """The polars-native merge gives the pandas rows, columns and values (through Arrow)."""
        pl = pytest.importorskip("polars")
        expected = pl.from_pandas(_session(name).results.reset_index(drop=True))
        actual = _session(name, lib="polars").results
        assert actual.columns == expected.columns
        assert actual.schema == expected.schema
        assert actual.base_class_view.equals(expected)

    def test_qualifying_segments(self):
        results = _session("Qualifying").results
        first = results.iloc[0]
        assert first["Abbreviation"] == "PIA"
        assert first["Q3"] == pd.Timedelta("0 days 00:01:30.641")
        assert first["Position"] == 1.0
        assert first["Status"] == "Finished"
        assert pd.isna(first["Points"])
        assert pd.isna(first["Laps"])
        assert results["Q1"].dtype == "timedelta64[ns]"

    def test_sprint_classification(self):
        results = _session("Sprint").results
        assert results.iloc[0]["Abbreviation"] == "HAM"
        assert results.iloc[0]["Points"] == 8.0
        assert _session("Sprint").total_laps == 19

    def test_practice_keeps_lap_derived_order_but_gets_identity(self, monkeypatch):
        # The lap-derived path would load laps through the async pipeline
        # (the real CDN); the socket block makes that slow and order-dependent.
        monkeypatch.setattr(Session, "_lap_summary", lambda _self: {})
        session = _session("Practice 1")
        assert session.drivers[:3] == ["1", "4", "6"]  # number order (no laps in the test)
        results = session.results
        assert results["DriverId"].ne("").all()
        assert results.loc["4", "TeamId"] == "mclaren"
        assert results["Position"].isna().all()
        assert session.total_laps is None

    def test_opt_out_restores_lap_derived_results(self, ergast_offline, monkeypatch):
        monkeypatch.setattr(Session, "_lap_summary", lambda _self: {})
        session = _session(ergast=False)
        results = session.results
        assert results["Points"].isna().all()
        assert results["DriverId"].eq("").all()
        assert not any("chinese-grand-prix" in c for c in ergast_offline.calls)
        assert session.total_laps is None
        get_config().set("ergast_results", False)
        assert _session()._ergast_enabled is False
        assert _session(ergast=True)._ergast_enabled is True

    def test_missing_ergast_data_degrades_silently(self, monkeypatch):
        monkeypatch.setattr(Session, "_lap_summary", lambda _self: {})
        get_config().set("ergast_fallback", False)
        session = tif1.get_session(2025, "Australian Grand Prix", "Race", enable_cache=False)
        session._payload_loader.transport = InMemoryTransport({"drivers.json": _drivers_payload()})
        results = session.results
        assert results["Points"].isna().all()
        assert len(results) == 19
        assert session.pit_stops.empty
        assert session.driver_standings.empty


@pytest.mark.usefixtures("ergast_offline")
class TestHistory:
    def test_pre_2018_network_failure_raises(self, ergast_offline, monkeypatch):
        get_config().set("ergast_fallback", False)
        from tif1.exceptions import NetworkError

        def down(url: str, *, timeout=None):
            raise NetworkError(url=url, status_code=None)

        monkeypatch.setattr(ergast_offline, "get_json", down)
        session = tif1.get_session(1950, "British Grand Prix", "Race")
        with pytest.raises(NetworkError):
            _ = session.drivers

    def test_pit_stops_with_merge_off_keep_driver_identity(self):
        session = _session(ergast=False)
        stops = session.pit_stops
        assert len(stops) == 25
        assert stops.iloc[0]["Driver"] == "BOR"
        assert stops.iloc[0]["DriverNumber"] == "5"
        assert session.results["Points"].isna().all()  # the merge itself stays off

    def test_pre_2018_session_from_ergast(self):
        get_config().set("ergast_fallback", False)
        session = tif1.get_session(1950, "British Grand Prix", "Race")
        assert session._ergast_only
        assert session.drivers[:3] == ["2", "3", "4"]
        results = session.results
        assert results.iloc[0]["DriverId"] == "farina"
        assert results.iloc[0]["Abbreviation"] == "farina"  # no code before the 1990s
        assert results.iloc[0]["Position"] == 1.0
        assert results.iloc[0]["Points"] == 9.0
        assert results.iloc[0]["Time"] == pd.Timedelta(milliseconds=8003600)
        assert results.iloc[0]["TeamName"] == "Alfa Romeo"
        assert session.total_laps == 70
        assert session.laps.empty  # no lap timings before 1996
        assert session.weather.empty
        assert session.get_driver("farina")["FullName"] == "Nino Farina"
        with pytest.raises(DataNotFoundError) as info:
            session._fetch_json("rcm.json")
        assert "before 2018" in info.value.context["reason"]

    def test_pre_2018_schedule_lists_every_session_but_loads_the_ergast_ones(self):
        # fastf1 parity: Session{i} is session i for every season, so the
        # session list and the schedule columns agree; what can load is a
        # get_session decision (Ergast carries no practice data).
        assert tif1.get_sessions(1950, "British Grand Prix") == [
            "Practice 1",
            "Practice 2",
            "Practice 3",
            "Qualifying",
            "Race",
        ]
        event = tif1.get_event(1950, 1)
        assert [event.get_session_name(i) for i in range(1, 6)] == [
            event[f"Session{i}"] for i in range(1, 6)
        ]
        assert event.get_session_date(5, utc=True) == pd.Timestamp("1950-05-13")
        with pytest.raises(DataNotFoundError) as info:
            tif1.get_session(1950, "British Grand Prix", "Practice 1")
        assert "no Practice 1 data" in info.value.context["reason"]
        with pytest.raises(DataNotFoundError):
            event.get_session(2)
        assert event.get_session(5).session_name == "Race"

    def test_shared_car_rows_get_unique_labels(self):
        get_config().set("ergast_fallback", False)
        session = tif1.get_session(1950, "British Grand Prix", "Race")
        results = session.results
        assert results.index.is_unique
        numbers = results["DriverNumber"]
        number = numbers[numbers.duplicated(keep=False)].iloc[0]  # a car two drivers shared
        rows = results[numbers == number]
        assert len(rows) == 2
        first, second = rows.index
        assert first == number
        assert second == f"{number}/{rows['Abbreviation'].iloc[1]}"
        co_driver = results.loc[second]
        assert isinstance(co_driver, DriverResult)  # one row, not a two-row frame
        assert co_driver["DriverNumber"] == number
        assert co_driver["DriverId"] != results.loc[first]["DriverId"]
        drivers = session.drivers
        assert drivers.count(number) == 1
        assert second in drivers
        assert len(drivers) == len(results)
        # get_driver reaches each of them, with the right identity.
        assert session.get_driver(second)["DriverId"] == co_driver["DriverId"]
        assert session.get_driver(number)["DriverId"] == results.loc[first]["DriverId"]
        assert session.get_driver(co_driver["Abbreviation"])["DriverNumber"] == number

    def test_failed_ergast_only_load_is_memoized(self, ergast_offline, monkeypatch):
        get_config().set("ergast_fallback", False)
        calls: list[str] = []

        def down(url: str, *, timeout=None):
            calls.append(url)
            raise NetworkError(url=url, status_code=None)

        monkeypatch.setattr(ergast_offline, "get_json", down)
        session = tif1.get_session(1950, "British Grand Prix", "Race")
        with pytest.raises(NetworkError):
            _ = session.drivers
        walked = len(calls)
        assert walked > 0
        with pytest.raises(NetworkError):
            _ = session.results
        assert len(calls) == walked  # the stored error is re-raised; no second mirror walk

    def test_pre_2018_qualifying_without_data_is_empty(self):
        get_config().set("ergast_fallback", False)
        session = tif1.get_session(1950, "British Grand Prix", "Qualifying")
        assert session.results.empty
        assert session.drivers == []


class TestHelpers:
    def test_session_kind_and_broadcast_name(self):
        assert session_kind("Race") == "race"
        assert session_kind("Sprint") == "race"
        assert session_kind("Qualifying") == "qualifying"
        assert session_kind("Sprint Qualifying") == "identity"
        assert broadcast_name("Pierre", "Gasly") == "P GASLY"
        assert broadcast_name("", "Gasly") == "GASLY"
        assert broadcast_name(None, None) == ""

    def test_order_driver_rows_handles_duplicates_and_unknowns(self):
        rows = [{"driver": "A", "dn": "1"}, {"driver": "B", "dn": "2"}, {"driver": "X", "dn": "9"}]
        classification = [
            {"number": "2", "position": "1", "Driver": {"code": "B"}, "Constructor": {"name": "T"}},
            {"number": "1", "position": "2", "Driver": {"code": "A"}, "Constructor": {"name": "T"}},
            {
                "number": "7",
                "position": "3",
                "Driver": {"driverId": "ghost"},
                "Constructor": {"name": "T"},
            },
            {
                "number": "1",
                "position": "4",
                "Driver": {"code": "A2"},
                "Constructor": {"name": "T"},
            },
        ]
        ordered = order_driver_rows(rows, classification, {"T": "ABCDEF"})
        assert [r["driver"] for r in ordered] == ["B", "A", "ghost", "A2", "X"]
        assert ordered[2]["_ergast_only"]
        assert ordered[2]["tc"] == "ABCDEF"
        assert ordered[3]["_ergast_only"]  # shared car: second row synthesized

    def test_order_driver_rows_matches_numberless_rows_by_code(self):
        # 2024 Belgian GP: drivers.json carries no car numbers; matching on
        # the number alone synthesized every classified driver and appended
        # the session's rows again (40 results rows).
        rows = [{"driver": "HAM", "team": "Mercedes"}, {"driver": "RUS", "team": "Mercedes"}]
        classification = [
            {"number": "63", "position": "2", "Driver": {"code": "RUS"}, "Constructor": {}},
            {"number": "44", "position": "1", "Driver": {"code": "HAM"}, "Constructor": {}},
        ]
        ordered = order_driver_rows(rows, classification, {})
        assert [r["driver"] for r in ordered] == ["HAM", "RUS"]
        assert ordered[0] is rows[0]  # the session's dict, not a synthesized one
        assert [r["dn"] for r in ordered] == ["44", "63"]  # numbers taken from Ergast
        assert not any(r.get("_ergast_only") for r in ordered)

    def test_order_driver_rows_never_code_matches_a_numbered_row(self):
        rows = [{"driver": "AAA", "dn": "5"}]
        classification = [
            {"number": "6", "position": "1", "Driver": {"code": "AAA"}, "Constructor": {}}
        ]
        ordered = order_driver_rows(rows, classification, {})
        assert ordered[0]["_ergast_only"]
        assert ordered[1] is rows[0]
        assert rows[0]["dn"] == "5"

    def test_results_row_data_with_a_repeated_abbreviation(self):
        from types import SimpleNamespace

        from tif1.models import _results_row_data

        # pandas 3 hands out read-only arrays from Series.to_numpy(): the
        # shared-car narrowing must not combine masks in place.
        frame = finalize_results_frame(
            [
                {"DriverNumber": "10", "Abbreviation": "fry", "Position": 3.0},
                {"DriverNumber": "12", "Abbreviation": "fry", "Position": 7.0},
            ]
        )
        session = SimpleNamespace(_results=frame, results=frame)
        found = _results_row_data(session, "fry", "12")
        assert found is not None
        label, row = found
        assert row["Position"] == 7.0
        assert label == frame.index[1]

    def test_unique_driver_labels(self):
        assert unique_driver_labels([("1", "A"), ("2", "B")]) == ["1", "2"]
        assert unique_driver_labels(
            [("10", "fry"), ("10", "shawe-taylor"), ("9", "x"), ("10", "fry")]
        ) == ["10", "10/shawe-taylor", "9", "10/fry"]
        assert unique_driver_labels([("7", ""), ("7", "")]) == ["7", "7/2"]
        frame = finalize_results_frame(
            [
                {"DriverNumber": "10", "Abbreviation": "fry", "Position": 10.0},
                {"DriverNumber": "10", "Abbreviation": "shawe-taylor", "Position": 10.0},
            ]
        )
        assert frame.index.tolist() == ["10", "10/shawe-taylor"]
        assert frame["DriverNumber"].tolist() == ["10", "10"]

    def test_merge_and_finalize_without_ergast(self):
        rows = [
            {"DriverNumber": "1", "Abbreviation": "A", "Position": 2.0, "Status": None},
            {"DriverNumber": "2", "Abbreviation": "B", "Position": 1.0, "Status": None},
        ]
        frame = finalize_results_frame(merge_ergast_rows(rows, [], "race"))
        assert frame.index.tolist() == ["2", "1"]
        assert frame["Status"].isna().all()
        assert frame["Q1"].dtype == "timedelta64[ns]"
        assert frame["DateOfBirth"].dtype == "datetime64[ns]"
        assert frame["TeamName"].eq("").all()
        assert finalize_results_frame([]).shape == (
            0,
            len(FASTF1_RESULTS_COLUMNS) + len(TIF1_RESULTS_EXTRA_COLUMNS),
        )

    def test_build_laps_payloads_from_timings(self):
        laps_race = json.loads(
            (FIXTURES / "2025/chinese-grand-prix/laptimes.json").read_text(encoding="utf-8")
        )["MRData"]["RaceTable"]["Races"][0]
        stops_race = json.loads(
            (FIXTURES / "2025/chinese-grand-prix/pitstops.json").read_text(encoding="utf-8")
        )["MRData"]["RaceTable"]["Races"][0]
        from tif1.ergast.jolpica import merge_split_laps

        laps_race = {**laps_race, "Laps": merge_split_laps(laps_race["Laps"])}
        payloads = build_laps_payloads(laps_race, _results_rows(), stops_race)
        by_driver = {code: payload for payload, code, _team in payloads}
        norris = by_driver["NOR"]
        assert norris["lap"][:3] == [1, 2, 3]
        assert norris["time"][0] == pytest.approx(99.268)
        assert norris["sesT"][1] == pytest.approx(norris["time"][0] + norris["time"][1])
        assert norris["lST"][1] == pytest.approx(norris["time"][0])
        assert norris["dNum"][0] == "4"
        assert norris["stint"][0] == 1
        norris_stop_lap = next(
            int(s["lap"]) for s in stops_race["PitStops"] if s["driverId"] == "norris"
        )
        assert norris["stint"][norris_stop_lap] == 2  # the out-lap starts the next stint
        assert norris["stint"][norris_stop_lap - 1] == 1
        team = next(team for _p, code, team in payloads if code == "NOR")
        assert team == "McLaren"

    @pytest.mark.usefixtures("ergast_offline")
    def test_ergast_only_driver_laps_do_not_depend_on_access_order(self):
        get_config().set("ergast_fallback", False)
        session = _session()
        session._ergast_only = True  # every driver row is synthesized from Ergast
        _ = session.drivers
        _ = session.results
        laps = session.get_driver("NOR").laps
        assert isinstance(laps, Laps)
        assert len(laps) == 56
        assert laps.session is session
        fresh = _session()
        fresh._ergast_only = True
        assert len(fresh.get_driver("NOR").laps) == 56  # same answer before .drivers

    @pytest.mark.usefixtures("ergast_offline")
    def test_driver_without_timing_data_gets_an_empty_laps_frame_with_columns(self):
        session = _session()  # BOR is only in the classification (did not start)
        laps = session.get_driver("BOR").laps  # before the session laps table exists
        assert isinstance(laps, Laps)
        assert laps.empty
        assert laps.session is session
        assert {"LapNumber", "LapTime", "Driver", "Team", "Stint"} <= set(laps.columns)
        assert laps.pick_fastest() is None
        assert laps.pick_quicklaps().empty

    @pytest.mark.usefixtures("ergast_offline")
    def test_polars_driver_without_timing_data_gets_an_empty_frame_with_columns(self):
        pl = pytest.importorskip("polars")
        polars_session = _session(lib="polars")
        polars_laps = polars_session.get_driver("BOR").laps
        assert isinstance(polars_laps, pl.DataFrame)
        assert polars_laps.height == 0
        assert {"LapNumber", "LapTime", "Driver"} <= set(polars_laps.columns)
        assert polars_laps.filter(pl.col("Driver") == "BOR").height == 0

    @pytest.mark.usefixtures("ergast_offline")
    def test_prefetch_runs_on_a_daemon_thread(self, monkeypatch):
        started: list[dict] = []
        real_thread = threading.Thread

        class Recording(real_thread):
            def __init__(self, *args, **kwargs):
                started.append(dict(kwargs))
                super().__init__(*args, **kwargs)

        monkeypatch.setattr(threading, "Thread", Recording)
        session = _session()
        _ = session.drivers
        ergast_threads = [kw for kw in started if kw.get("name") == "tif1-ergast"]
        assert ergast_threads
        assert all(kw.get("daemon") for kw in ergast_threads)  # never delays interpreter exit

    @pytest.mark.usefixtures("ergast_offline")
    def test_session_laps_built_from_ergast_timings(self):
        get_config().set("ergast_fallback", False)
        session = _session()
        session._ergast_only = True  # a 2025 race with the pre-2018 code path
        laps = session.laps
        assert len(laps) == 1065
        assert laps["Driver"].nunique() == 20
        norris = laps[laps["Driver"] == "NOR"].sort_values("LapNumber")
        assert norris["LapTime"].iloc[0] == pd.Timedelta(seconds=99.268)
        assert norris["Position"].iloc[0] == 2
        assert norris["DriverNumber"].iloc[0] == "4"
        assert norris["Team"].iloc[0] == "McLaren"
        assert norris["Stint"].max() >= 2
        assert laps["Time"].dtype == "timedelta64[ns]"
        assert "Compound" in laps.columns
