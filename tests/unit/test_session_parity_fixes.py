"""Session-level fixes from the 0.11 audit: driver rows, results, circuit markers, cache.

Each test pins one confirmed defect: identity values a ``drivers.json`` lacks,
payload dicts shared across sessions, the car a ``get_driver`` identifier
names, pre-2018 personal bests, the qualifying classification from laps,
the practice best-lap ``Time``, marker distances on both backends, ``car_data``
keys for shared cars, the Ergast-only session state, and the results frame.
"""

from __future__ import annotations

import math
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from tif1 import core
from tif1.cache import Cache
from tif1.core_utils import ergast_results as er
from tif1.models import CircuitInfo, LazyTelemetryDict


def _erow(number: str, code: str, given: str, family: str, position: str) -> dict[str, Any]:
    return {
        "number": number,
        "position": position,
        "Driver": {
            "driverId": code.lower(),
            "code": code,
            "givenName": given,
            "familyName": family,
        },
        "Constructor": {"constructorId": "team", "name": "Team"},
    }


class TestDriverRows:
    def test_numberless_rows_take_number_and_names_from_ergast(self):
        rows = [{"driver": "HAM", "team": "Mercedes"}, {"driver": "PIA", "fn": "Oscar", "dn": ""}]
        classification = [
            _erow("81", "PIA", "Oscar", "Piastri", "2"),
            _erow("44", "HAM", "Lewis", "Hamilton", "1"),
        ]
        ordered = er.order_driver_rows(rows, classification, {})
        assert [row["driver"] for row in ordered] == ["HAM", "PIA"]
        assert ordered[0] is rows[0]  # the session's own dict, as its lookup indices hold
        assert (rows[0]["dn"], rows[0]["fn"], rows[0]["ln"]) == ("44", "Lewis", "Hamilton")
        assert (rows[1]["dn"], rows[1]["fn"], rows[1]["ln"]) == ("81", "Oscar", "Piastri")

    def test_values_the_row_has_are_kept(self):
        rows = [{"driver": "HAM", "dn": "44", "fn": "Sir Lewis", "ln": ""}]
        er.order_driver_rows(rows, [_erow("44", "HAM", "Lewis", "Hamilton", "1")], {})
        assert rows[0]["fn"] == "Sir Lewis"
        assert rows[0]["ln"] == "Hamilton"

    def test_every_session_gets_its_own_driver_dicts(self):
        payload = [{"driver": "HAM"}, "junk"]
        own = core._session_driver_rows(payload)
        own[0]["dn"] = "44"
        assert "dn" not in payload[0]
        assert own[1] == "junk"
        assert core._session_driver_rows(None) == []

    @pytest.mark.parametrize(
        ("identifier", "number"),
        [
            ("8", "8"),
            (8, "8"),
            ("8/FAG", "8"),
            (" 14/ascari ", "14"),
            ("VER", None),
            ("Max Verstappen", None),
            (True, None),
            (None, None),
        ],
    )
    def test_identifier_car_number(self, identifier, number):
        assert core._identifier_car_number(identifier) == number


class TestErgastLaps:
    def test_personal_best_marks_each_improvement(self):
        laps_race = {
            "Laps": [
                {"number": str(n), "Timings": [{"driverId": "ham", "time": t, "position": "1"}]}
                for n, t in ((1, "1:40.000"), (2, "1:35.000"), (3, "1:36.000"), (4, "1:34.500"))
            ]
        }
        ((payload, code, _team),) = er.build_laps_payloads(
            laps_race, [_erow("44", "HAM", "Lewis", "Hamilton", "1")], None
        )
        assert code == "HAM"
        assert payload["pb"] == [True, True, False, True]


_BESTS: dict[str, list[int | None]] = {
    "AAA": [90_000_000_000, 89_000_000_000, 88_000_000_000],
    "BBB": [90_500_000_000, 88_500_000_000, 87_900_000_000],
    "CCC": [91_000_000_000, 89_500_000_000, None],
    "DDD": [97_000_000_000, None, None],  # outside 107% of the best Q1
    "EEE": [92_000_000_000, None, None],
}


class TestQualifyingFromLaps:
    def test_order_is_q3_then_q2_then_q1_missing_times_last(self):
        rows = [{"driver": code} for code in ("DDD", "AAA", "EEE", "CCC", "ZZZ", "BBB")]
        ordered = core._quali_sort(rows, _BESTS)
        assert [row["driver"] for row in ordered] == ["BBB", "AAA", "CCC", "EEE", "DDD", "ZZZ"]

    def test_fields_and_the_107_percent_rule(self):
        floor = core._quali_q1_floor(_BESTS)
        assert floor == int(90_000_000_000 * 1.07)
        out = core._quali_row_fields(_BESTS["DDD"], floor)
        assert out["ClassifiedPosition"] == "N"
        assert out["Q1"] == pd.Timedelta(97, unit="s")
        assert out["Q2"] is pd.NaT
        assert "ClassifiedPosition" not in core._quali_row_fields(_BESTS["EEE"], floor)
        assert core._quali_row_fields(None, floor) == {}
        assert core._quali_q1_floor({}) is None


@pytest.mark.parametrize("seconds", [90.505, 61.0, 100.001, 89.999])
def test_practice_best_lap_time_is_the_exact_lap_time(seconds):
    lap_time = pd.Timedelta(seconds, unit="s")
    best = core._best_lap_timedelta({"best": lap_time.total_seconds()})
    assert best == lap_time
    assert core._best_lap_timedelta({"best": None}) is pd.NaT
    assert core._best_lap_timedelta(None) is pd.NaT


class _ReferenceLap:
    def __init__(self, telemetry):
        self.telemetry = telemetry


def _circuit(make, known: float) -> CircuitInfo:
    corners = make({"X": [0.0, 10.0], "Y": [0.0, 0.0], "Distance": [known, math.nan]})
    lights = make({"X": [5.0], "Y": [0.0], "Distance": [math.nan]})
    empty = make({"X": [], "Y": [], "Distance": []})
    return CircuitInfo(corners=corners, marshal_lights=lights, marshal_sectors=empty, rotation=0.0)


def _telemetry(make):
    return make({"X": [0.0, 5.0, 10.0], "Y": [0.0, 0.0, 0.0], "Distance": [1.0, 2.0, 3.0]})


@pytest.mark.parametrize("backend", ["pandas", "polars"])
def test_marker_fit_fills_only_missing_distances(backend):
    if backend == "polars":
        pl = pytest.importorskip("polars")
        make = pl.DataFrame
    else:
        make = pd.DataFrame
    info = _circuit(make, known=1508.78)
    info._fit_marker_distances(_ReferenceLap(_telemetry(make)), only_missing=True)
    corners = info.corners if backend == "pandas" else info.corners.to_pandas()
    lights = info.marshal_lights if backend == "pandas" else info.marshal_lights.to_pandas()
    assert corners["Distance"].tolist() == [1508.78, 3.0]
    assert lights["Distance"].tolist() == [2.0]
    info.add_marker_distance(_ReferenceLap(_telemetry(make)))  # fastf1: every marker
    corners = info.corners if backend == "pandas" else info.corners.to_pandas()
    assert corners["Distance"].tolist() == [1.0, 3.0]


class _FakeSession:
    _ergast_only = False

    def __init__(self, rows: list[dict[str, Any]], backfill: dict[str, str] | None = None):
        self._drivers_data = rows
        self._backfill = backfill or {}
        self.resolved = 0

    def _driver_rows_info(self):
        self.resolved += 1
        for row in self._drivers_data:
            row.setdefault("dn", self._backfill.get(row["driver"], ""))
        return self._drivers_data, True


def test_car_data_keys_resolve_numbers_first_and_label_shared_cars():
    session = _FakeSession(
        [{"driver": "fangio"}, {"driver": "fagioli", "dn": "8"}, {"driver": "ascari", "dn": "14"}],
        backfill={"fangio": "8"},
    )
    data = LazyTelemetryDict(session)  # type: ignore[arg-type]
    assert list(data) == ["8", "8/fagioli", "14"]
    assert session.resolved == 1
    assert data._driver_entry("8") == ("8", "fangio")
    assert data._driver_entry("8/fagioli") == ("8/fagioli", "fagioli")
    assert data._driver_entry("ascari") == ("14", "ascari")
    assert "99" not in data


def test_car_data_keys_never_resolve_when_numbers_are_present():
    session = _FakeSession([{"driver": "VER", "dn": "1"}, {"driver": "HAM", "dn": "44"}])
    assert LazyTelemetryDict(session).keys() == ["1", "44"]  # type: ignore[arg-type]
    assert session.resolved == 0


def test_ergast_only_session_state_reads_no_laps():
    session = SimpleNamespace(_ergast_only=True)
    session.laps = property(lambda _self: pytest.fail("the laps table was loaded"))
    assert core.Session._loaded_laps_or_none(session) is None  # type: ignore[arg-type]


def test_cache_reads_through_the_memory_map(tmp_path: Path):
    cache = Cache(tmp_path)
    assert cache.conn.execute("PRAGMA mmap_size").fetchone()[0] > 0


class TestResultsFrame:
    def test_mixed_value_types_take_the_pandas_conversion(self):
        rows = [
            {
                "DriverNumber": 1,
                "Abbreviation": "VER",
                "Position": 2,
                "Points": "18",
                "Time": "0 days 01:31:44.742",
                "Status": "Finished",
            },
            {
                "DriverNumber": "44",
                "Abbreviation": "HAM",
                "Position": 1.0,
                "Points": "x",
                "Time": None,
                "Status": None,
            },
            {"DriverNumber": "16", "Abbreviation": "LEC", "Position": math.nan},
        ]
        frame = er.finalize_results_frame(rows)
        assert list(frame.columns) == list(er.RESULTS_COLUMN_ORDER)
        assert list(frame.index) == ["44", "1", "16"]  # by Position, unknown last
        assert frame["Points"].tolist()[:2] == [pytest.approx(math.nan, nan_ok=True), 18.0]
        assert frame["Time"].dtype == "timedelta64[ns]"
        assert frame.loc["1", "Time"] == pd.Timedelta("01:31:44.742")
        assert frame["DateOfBirth"].dtype == "datetime64[ns]"
        assert frame["DriverNumber"].dtype == object
        assert frame.loc["1", "DriverNumber"] == 1  # values are kept as given
        assert frame["TeamName"].tolist() == ["", "", ""]
        assert frame["Q1"].isna().all()
        assert np.isnan(frame.loc["16", "Position"])

    def test_typed_rows_take_the_direct_path(self):
        rows = [
            {
                "DriverNumber": "1",
                "Abbreviation": "VER",
                "Position": 1.0,
                "Status": "Finished",
                "Time": pd.Timedelta(seconds=5000),
                "DateOfBirth": pd.Timestamp("1997-09-30"),
            },
            {
                "DriverNumber": "4",
                "Abbreviation": "NOR",
                "Position": 2.0,
                "Status": "+1 Lap",
                "Time": pd.NaT,
                "DateOfBirth": pd.Timestamp("1999-11-13"),
            },
        ]
        frame = er.finalize_results_frame(rows)
        assert frame.loc["1", "Time"] == pd.Timedelta(seconds=5000)
        assert pd.isna(frame.loc["4", "Time"])
        assert frame.loc["4", "DateOfBirth"] == pd.Timestamp("1999-11-13")
        assert frame["Status"].tolist() == ["Finished", "+1 Lap"]
        assert frame["Status"].dtype == object


class TestQualifyingKnockout:
    @staticmethod
    def _bests(
        q1: list[int], q2: dict[int, int], q3: dict[int, int]
    ) -> dict[str, list[int | None]]:
        return {f"D{i:02d}": [q1_time, q2.get(i), q3.get(i)] for i, q1_time in enumerate(q1)}

    def test_a_driver_out_in_q1_loses_later_times(self):
        # 20 cars: 15 go through. D15 is 16th in Q1 yet carries a Q2 time.
        q1 = [90_000 + i for i in range(20)]
        bests = self._bests(
            q1, {i: 89_000 + i for i in range(16)}, {i: 88_000 + i for i in range(11)}
        )
        core._apply_quali_knockout(bests, 20)
        assert bests["D15"][1:] == [None, None]
        assert bests["D14"][1] == 89_014
        assert bests["D10"][2] is None  # 11th in Q2: no Q3
        assert bests["D09"][2] == 88_009

    def test_the_q1_cut_follows_the_field_size(self):
        q1 = [90_000 + i for i in range(22)]
        bests = self._bests(q1, {i: 89_000 + i for i in range(17)}, {})
        core._apply_quali_knockout(bests, 22)
        assert bests["D15"][1] == 89_015  # 16 of 22 go through
        assert bests["D16"][1] is None

    def test_a_driver_without_a_q1_time_is_left_alone(self):
        bests = {
            "AAA": [None, 89_000, 88_000],
            **self._bests([90_000 + i for i in range(20)], {}, {}),
        }
        core._apply_quali_knockout(bests, 21)
        assert bests["AAA"] == [None, 89_000, 88_000]


def test_polars_fastest_laps_match_the_expression_path():
    pl = pytest.importorskip("polars")
    seconds = [91.0, 90.5, None, 90.5, 92.0, 90.5, 89.9, 95.0]
    laps = pl.DataFrame(
        {
            "Driver": ["VER", "VER", "HAM", "HAM", "LEC", "LEC", "LEC", "NOR"],
            "LapNumber": [1, 2, 1, 2, 1, 2, 3, 1],
            "LapTime": pl.Series(
                [None if s is None else int(s * 1e9) for s in seconds], dtype=pl.Int64
            ).cast(pl.Duration("ns")),
            "Deleted": [False, False, None, False, False, False, True, None],
        }
    )
    session = core.Session(2025, "Pre-Season Testing", "Race", enable_cache=False, lib="polars")
    for by_driver in (True, False):
        for drivers in (None, ["VER", "LEC"], ["ZZZ"]):
            fast = core._fastest_laps_polars(laps, by_driver=by_driver, drivers=drivers)
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(core, "_fastest_laps_polars", lambda *_a, **_k: None)
                expected = session._select_fastest_laps(laps, by_driver=by_driver, drivers=drivers)
            if not expected.height:
                assert fast.height == 0
                continue
            assert fast.columns == expected.columns
            assert fast.drop("LapTimeSeconds").equals(expected.drop("LapTimeSeconds"))
            assert fast["LapTimeSeconds"].to_list() == pytest.approx(
                expected["LapTimeSeconds"].to_list(), rel=1e-15
            )
    # Ties: VER and HAM both 90.5 s; VER appears first, LEC's 89.9 s is deleted.
    by_driver = core._fastest_laps_polars(laps, by_driver=True, drivers=None)
    assert by_driver["Driver"].to_list() == ["VER", "HAM", "LEC", "NOR"]
    assert by_driver["LapNumber"].to_list() == [2, 2, 2, 1]


def test_pandas_clock_columns_convert_any_resolution():
    mask = np.array([True, False, True])
    starts = pd.Series(pd.to_timedelta([1, 2, np.nan], unit="s").astype("timedelta64[ms]"))
    dates = pd.Series(pd.to_datetime(["2025-04-13 15:00", None, "2025-04-13 15:01"]))
    assert core._pandas_ns_values(starts, mask, timedelta=True).tolist() == [
        1_000_000_000,
        np.iinfo("int64").min,
    ]
    assert core._pandas_ns_values(dates, mask, timedelta=False).tolist() == [
        pd.Timestamp("2025-04-13 15:00").value,
        pd.Timestamp("2025-04-13 15:01").value,
    ]
