"""fastf1 parity on polars sessions, and the regressions found while wiring it.

A polars session hands out the polars model family (``PolarsLaps``,
``PolarsTelemetry``, ``PolarsSessionResults``, ``PolarsDriver``) built
natively, with the same rows and values as the pandas session. Every payload
is served from memory: nothing reaches the network.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import numpy as np
import pandas as pd
import pytest

import tif1
from tif1 import core
from tif1.config import get_config
from tif1.core_utils.ergast_results import FASTF1_RESULTS_COLUMNS
from tif1.core_utils.helpers import _filter_valid_laptimes, _polars_laps_contract
from tif1.exceptions import EventNotFoundError
from tif1.models import Telemetry, _timedelta_ns
from tif1.payload_loader import InMemoryTransport

pl = pytest.importorskip("polars")
models_polars = pytest.importorskip("tif1.models_polars")

T0 = pd.Timestamp("2025-04-13 14:55:00")
LAP_TIME = 95.0
LAP_STARTS = {"VER": [300.0, 395.0, 490.0], "HAM": [300.5, 396.0, 491.5]}
DRIVERS = [
    {"driver": "VER", "dn": "1", "team": "Red Bull Racing", "fn": "Max", "ln": "Verstappen"},
    {"driver": "HAM", "dn": "44", "team": "Ferrari", "fn": "Lewis", "ln": "Hamilton"},
]
TEL_SAMPLES = 9


def _laptimes(code: str, number: str, team: str) -> dict[str, Any]:
    """A three-lap ``{DRIVER}/laptimes.json`` payload (TracingInsights keys)."""
    starts = LAP_STARTS[code]
    n = len(starts)
    return {
        "time": [LAP_TIME, LAP_TIME - 1.0, LAP_TIME + 1.0],
        "lap": [1, 2, 3],
        "compound": ["SOFT"] * n,
        "stint": [1] * n,
        "s1": [30.0] * n,
        "s2": [30.0] * n,
        "s3": [LAP_TIME - 60.0] * n,
        "life": [1, 2, 3],
        "pos": [1.0 if code == "VER" else 2.0] * n,
        "status": ["1", "14", "1"],
        "pb": [False, None, True],
        "sesT": [start + LAP_TIME for start in starts],
        "drv": [code] * n,
        "dNum": [number] * n,
        "pout": [None] * n,
        "pin": [None] * n,
        "s1T": [start + 30.0 for start in starts],
        "s2T": [start + 60.0 for start in starts],
        "s3T": [start + LAP_TIME for start in starts],
        "vi1": [300.0] * n,
        "vi2": [300.0] * n,
        "vfl": [300.0] * n,
        "vst": [300.0] * n,
        "fresh": [True, None, False],
        "team": [team] * n,
        "lST": starts,
        "lSD": [(T0 + pd.Timedelta(seconds=start)).isoformat() for start in starts],
        "del": [False] * n,
        "delR": [None] * n,
        "ff1G": [False] * n,
        "iacc": [True] * n,
        "wT": [start + 10.0 for start in starts],
        "wAT": [27.0, 28.0, 29.0],
        "wH": [45.0] * n,
        "wP": [1017.0] * n,
        "wR": [False] * n,
        "wTT": [35.0] * n,
        "wWD": [200] * n,
        "wWS": [1.0] * n,
    }


def _telemetry(ahead: str) -> dict[str, Any]:
    n = TEL_SAMPLES
    return {
        "tel": {
            "time": [i * 0.25 for i in range(n)],
            "rpm": [10000.0 + 100 * i for i in range(n)],
            "speed": [200.0 + i for i in range(n)],
            "gear": [7, 7, 7, 8, 8, 8, 8, 8, 8],
            "throttle": [100.0] * n,
            "brake": [False] * n,
            "drs": [0] * n,
            "distance": [i * 14.0 for i in range(n)],
            "rel_distance": [i / (n - 1) for i in range(n)],
            "driver_ahead": [ahead] * n,
            "distance_to_driver_ahead": [12.0] * n,
            "x": [float(i) for i in range(n)],
            "y": [0.0] * n,
            "z": [0.0] * n,
        }
    }


def _payloads() -> dict[str, Any]:
    payloads: dict[str, Any] = {
        "drivers.json": {"drivers": [dict(d) for d in DRIVERS]},
        "VER/laptimes.json": _laptimes("VER", "1", "Red Bull Racing"),
        "HAM/laptimes.json": _laptimes("HAM", "44", "Ferrari"),
    }
    for code, ahead in (("VER", "44"), ("HAM", "1")):
        for lap in (1, 2, 3):
            payloads[f"{code}/{lap}_tel.json"] = _telemetry(ahead)
    return payloads


class _Cdn:
    """Both fetch seams served from memory, recording what is asked for."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.payloads = _payloads()
        self.transport = InMemoryTransport(self.payloads)
        self.batches: list[list[str]] = []

        async def fetch_multiple(requests, **_kwargs):
            self.batches.append([request[3] for request in requests])
            return [self.payloads.get(request[3]) for request in requests]

        monkeypatch.setattr(core, "fetch_multiple_async", fetch_multiple)
        monkeypatch.setitem(get_config()._config, "prefetch_all_telemetry_after_laps_load", False)

    def requested(self) -> list[str]:
        return [path for batch in self.batches for path in batch] + list(self.transport.calls)

    def session(self, lib: str) -> core.Session:
        session = core.Session(2025, "Bahrain Grand Prix", "Race", enable_cache=False, lib=lib)
        session._payload_loader.transport = self.transport
        return session


@pytest.fixture
def cdn(monkeypatch) -> _Cdn:
    return _Cdn(monkeypatch)


@pytest.fixture
def polars_session(cdn) -> core.Session:
    return cdn.session("polars")


@pytest.fixture
def pandas_session(cdn) -> core.Session:
    return cdn.session("pandas")


def _same_frame(pandas_frame: pd.DataFrame, polars_frame: Any) -> None:
    """Same columns and values (pandas' leaked ``index`` column aside)."""
    expected = pandas_frame.drop(columns="index", errors="ignore").reset_index(drop=True)
    actual = polars_frame.to_pandas()
    assert list(actual.columns) == list(expected.columns)
    for column in expected.columns:
        left, right = expected[column], actual[column]
        if pd.api.types.is_float_dtype(left) or pd.api.types.is_float_dtype(right):
            np.testing.assert_allclose(
                pd.to_numeric(left).to_numpy(dtype=float, na_value=np.nan),
                pd.to_numeric(right).to_numpy(dtype=float, na_value=np.nan),
                err_msg=column,
            )
        else:
            assert [None if pd.isna(v) else str(v) for v in left] == [
                None if pd.isna(v) else str(v) for v in right
            ], column


# -- session surfaces ----------------------------------------------------------------


class TestSurfaceClasses:
    def test_laps_are_bound_polars_laps(self, polars_session):
        laps = polars_session.laps
        assert isinstance(laps, models_polars.PolarsLaps)
        assert laps.session is polars_session

    def test_results_are_polars_results_with_fastf1_columns_first(self, polars_session):
        results = polars_session.results
        assert isinstance(results, models_polars.PolarsSessionResults)
        assert results.columns[: len(FASTF1_RESULTS_COLUMNS)] == list(FASTF1_RESULTS_COLUMNS)
        assert results.schema["Q1"] == pl.Duration("ns")
        assert results.schema["Position"] == pl.Float64

    def test_drivers_df_is_a_string_polars_frame(self, polars_session, pandas_session):
        frame = polars_session.drivers_df
        assert type(frame) is pl.DataFrame
        assert set(frame.schema.values()) == {pl.String}
        _same_frame(pandas_session.drivers_df, frame)

    def test_get_driver_is_a_polars_driver_with_result_columns(self, polars_session):
        driver = polars_session.get_driver("VER")
        assert isinstance(driver, models_polars.PolarsDriver)
        assert isinstance(driver, models_polars.PolarsDriverResult)
        assert driver.driver == "VER"
        assert driver["FullName"] == "Max Verstappen"
        # Practice-like (no classification in the fixture): fastf1's columns stay.
        assert set(FASTF1_RESULTS_COLUMNS) <= set(driver.keys())
        assert driver.dnf is False

    def test_get_fastest_laps_keeps_the_laps_order_and_seconds(
        self, polars_session, pandas_session
    ):
        fastest = polars_session.get_fastest_laps()
        assert isinstance(fastest, models_polars.PolarsLaps)
        assert fastest.columns == polars_session.laps.columns
        # LapTimeSeconds is seconds, not the Duration's nanoseconds.
        assert fastest["LapTimeSeconds"].to_list() == pytest.approx([94.0, 94.0])
        _same_frame(pandas_session.get_fastest_laps(), fastest)


class TestPolarsDriverLaps:
    def test_driver_laps_are_its_own(self, polars_session):
        laps = polars_session.get_driver("HAM").laps
        assert isinstance(laps, models_polars.PolarsLaps)
        assert laps["Driver"].unique().to_list() == ["HAM"]

    def test_driver_laps_load_only_its_own_payload_on_a_cold_session(self, polars_session, cdn):
        # What ``PolarsDriver.laps`` reads. ``get_driver`` itself builds the
        # ``results`` row, which a session without an official classification
        # derives from every driver's laps.
        laps = polars_session._driver_laps_frame("HAM")
        assert isinstance(laps, models_polars.PolarsLaps)
        assert laps["Driver"].unique().to_list() == ["HAM"]
        assert not [path for path in cdn.requested() if path.startswith("VER/")]
        assert polars_session._laps is None

    def test_matches_the_pandas_driver_laps(self, polars_session, pandas_session):
        _same_frame(pandas_session.get_driver("VER").laps, polars_session.get_driver("VER").laps)

    def test_full_name_and_number_identifiers(self, polars_session):
        assert polars_session.get_driver("max verstappen").driver == "VER"
        assert polars_session.get_driver(44).driver == "HAM"
        assert polars_session.get_driver(identifier="HAM").driver == "HAM"
        assert polars_session.get_driver(driver="HAM").driver == "HAM"


class TestCarData:
    def test_driver_timeline_is_one_batch_of_polars_telemetry(self, polars_session, cdn):
        _ = polars_session.laps
        tel = polars_session.car_data["1"]
        assert isinstance(tel, models_polars.PolarsTelemetry)
        assert tel.height == 3 * TEL_SAMPLES
        assert tel.columns[:2] == ["Date", "SessionTime"]
        assert tel["SessionTime"].is_sorted()
        telemetry_batches = [batch for batch in cdn.batches if any("_tel" in p for p in batch)]
        assert telemetry_batches == [["VER/1_tel.json", "VER/2_tel.json", "VER/3_tel.json"]]

    def test_matches_the_pandas_car_data(self, polars_session, pandas_session):
        _same_frame(pandas_session.car_data["44"], polars_session.car_data["HAM"])

    def test_pandas_cold_car_data_fetches_in_one_wave(self, pandas_session, cdn):
        _ = pandas_session.laps
        tel = pandas_session.car_data["1"]
        assert len(tel) == 3 * TEL_SAMPLES
        telemetry_batches = [batch for batch in cdn.batches if any("_tel" in p for p in batch)]
        assert telemetry_batches == [["VER/1_tel.json", "VER/2_tel.json", "VER/3_tel.json"]]

    def test_pad_slices_the_driver_timeline(self, polars_session, pandas_session):
        polars_lap = polars_session.laps.pick_drivers("VER").pick_laps(2)
        pandas_lap = pandas_session.laps.pick_drivers("VER").pick_laps(2)
        padded = polars_lap.get_car_data(pad=1, pad_side="both")
        window = polars_session.car_data["1"].slice_by_lap(polars_lap)
        assert padded.height == window.height + 2
        _same_frame(pandas_lap.get_car_data(pad=1, pad_side="both"), padded)


class TestTelemetryMemo:
    def test_tier_frames_keep_the_adopted_polars_frame(self, polars_session, monkeypatch):
        """``_memoized_telemetry`` stored the unadopted frame (no SessionTime) on polars."""
        _ = polars_session.laps
        raw = core._create_telemetry_df(_telemetry("44")["tel"], "VER", 1, "polars")

        class _Tier:
            def get_telemetry_frames_batch(self, *_args, **_kwargs):
                return {("VER", 1): raw}

        polars_session.enable_cache = True
        monkeypatch.setattr(polars_session, "_session_cache_available", lambda: True)
        monkeypatch.setattr(core, "get_cache", lambda: _Tier())
        frames, pending = polars_session._memoized_telemetry([("VER", 1)], False)
        assert pending == []
        stored = polars_session._memo.get("telemetry_df", ("VER", 1))
        assert stored is frames[("VER", 1)]
        assert isinstance(stored, models_polars.PolarsTelemetry)
        assert "SessionTime" in stored.columns

    def test_precompute_never_memoises_pandas_frames_on_polars(self, polars_session):
        polars_session._remember_telemetry_payload("VER", 1, _telemetry("44")["tel"])
        polars_session._precompute_telemetry_dfs()
        assert polars_session._memo.get("telemetry_df", ("VER", 1)) is None
        tel = polars_session._get_telemetry_df_for_ref("VER", 1, ultra_cold=False)
        assert isinstance(tel, models_polars.PolarsTelemetry)


# -- laps contract regressions -------------------------------------------------------


class TestPolarsLapsContract:
    def test_flags_are_boolean_with_missing_read_as_false(self, polars_session, pandas_session):
        laps = polars_session.laps
        for column in ("IsPersonalBest", "FreshTyre"):
            assert laps.schema[column] == pl.Boolean
            assert laps[column].null_count() == 0
            assert laps[column].to_list() == pandas_session.laps[column].tolist()

    def test_all_null_columns_get_their_session_dtype(self):
        frame = pl.DataFrame(
            {
                "DeletedReason": [None, None],
                "Compound": [None, None],
                "TyreLife": [None, None],
                "SpeedI1": [None, None],
                "IsAccurate": [None, None],
            }
        )
        typed = _polars_laps_contract(frame)
        assert typed.schema["DeletedReason"] == pl.String
        assert typed.schema["Compound"] == pl.String
        assert typed.schema["TyreLife"] == pl.Int64
        assert typed.schema["SpeedI1"] == pl.Float64
        assert typed["IsAccurate"].to_list() == [False, False]

    def test_valid_laptimes_seconds_from_a_duration(self):
        laps = pl.DataFrame(
            {
                "LapTime": pl.Series([dt.timedelta(seconds=95.14), None]),
                "LapTimeSeconds": [95.14, None],
            }
        )
        valid = _filter_valid_laptimes(laps, "polars")
        assert valid["LapTimeSeconds"].to_list() == pytest.approx([95.14])


# -- resampling: the same rows on both backends ----------------------------------------


def _clocked_frame() -> pd.DataFrame:
    """Two laps of one driver with the session clock (``Date``/``SessionTime``)."""
    n = 40
    time = np.r_[np.arange(n // 2), np.arange(n // 2)] * 0.13
    session_time = np.r_[300.0 + time[: n // 2], 360.0 + time[n // 2 :]]
    rng = np.random.default_rng(3)
    return pd.DataFrame(
        {
            "Date": T0 + pd.to_timedelta(session_time, unit="s"),
            "SessionTime": pd.to_timedelta(session_time, unit="s").astype("m8[ns]"),
            "Time": pd.to_timedelta(time, unit="s").astype("m8[ns]"),
            "Speed": rng.uniform(100, 300, n),
            "nGear": pd.array(rng.integers(1, 9, n), dtype="Int64"),
            "Brake": rng.integers(0, 2, n).astype(bool),
            "Driver": ["VER"] * n,
            "LapNumber": pd.array([1] * (n // 2) + [2] * (n // 2), dtype="Int64"),
        }
    )


class TestResampleParity:
    @pytest.mark.parametrize("rule", ["100ms", "1s", "250ms"])
    def test_rule_resampling_matches(self, rule):
        frame = _clocked_frame()
        expected = Telemetry(frame).resample_channels(rule)
        actual = models_polars.PolarsTelemetry(pl.from_pandas(frame)).resample_channels(rule)
        _same_frame(expected, actual)
        # Bins on the session clock: the two laps never fold onto each other.
        assert len(expected) > 60
        assert expected["Time"].iloc[0] == pd.Timedelta(0)
        assert set(expected["nGear"].dropna()) <= set(frame["nGear"])

    def test_new_date_ref_matches(self):
        frame = _clocked_frame()
        ref = frame["Date"].iloc[::3].reset_index(drop=True) + pd.Timedelta("7ms")
        expected = Telemetry(frame).resample_channels(new_date_ref=ref)
        actual = models_polars.PolarsTelemetry(pl.from_pandas(frame)).resample_channels(
            new_date_ref=pl.from_pandas(ref)
        )
        assert len(expected) == len(ref)
        _same_frame(expected, actual)

    def test_neither_argument_means_one_second(self):
        frame = _clocked_frame()
        assert Telemetry(frame).resample_channels().equals(Telemetry(frame).resample_channels("1s"))


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_timedelta_ns_whatever_the_resolution(unit):
    series = pd.Series(pd.to_timedelta([1.5, None], unit="s")).astype(f"m8[{unit}]")
    values = _timedelta_ns(series)
    assert values[0] == (1_000_000_000 if unit == "s" else 1_500_000_000)
    assert values[1] == np.iinfo("int64").min


# -- keywords --------------------------------------------------------------------------


class TestGetSessionKeywords:
    def test_identifier_is_fastf1s_name_of_session(self):
        session = tif1.get_session(2025, "Bahrain Grand Prix", identifier="Race")
        assert session.session_name == "Race"

    def test_session_and_identifier_conflict(self):
        with pytest.raises(ValueError, match="Cannot pass both"):
            tif1.get_session(2025, "Bahrain Grand Prix", "Race", identifier="Qualifying")
        with pytest.raises(TypeError, match="identifier"):
            tif1.get_session(2025, "Bahrain Grand Prix")

    def test_exact_match_refuses_an_alias(self):
        assert tif1.get_session(2025, "Sakhir", "Race").event_name == "Bahrain Grand Prix"
        with pytest.raises(EventNotFoundError):
            tif1.get_session(2025, "Sakhir", "Race", exact_match=True)
        exact = tif1.get_session(2025, "Bahrain Grand Prix", "Race", exact_match=True)
        assert exact.event_name == "Bahrain Grand Prix"


def test_get_driver_identifier_and_driver_conflict(pandas_session):
    with pytest.raises(ValueError, match="Cannot pass both"):
        pandas_session.get_driver("VER", driver="HAM")
    with pytest.raises(TypeError, match="identifier"):
        pandas_session.get_driver()


@pytest.mark.parametrize("lib", ["pandas", "polars"])
def test_legacy_inject_driver_ahead(cdn, lib):
    from tif1.legacy import inject_driver_ahead

    session = cdn.session(lib)
    result = inject_driver_ahead(session)
    assert list(result) == ["1", "44"]
    for telemetry in result.values():
        assert {"DriverAhead", "DistanceToDriverAhead"} <= set(telemetry.columns)
        assert len(telemetry) == 3 * TEL_SAMPLES


def test_ergast_frames_have_base_class_view():
    from tif1.ergast.interface import ErgastResultFrame

    frame = ErgastResultFrame({"a": [1]})
    assert type(frame.base_class_view) is pd.DataFrame
