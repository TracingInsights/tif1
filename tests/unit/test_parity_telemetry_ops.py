"""fastf1 parity of the telemetry operations, the pandas and polars classes alike.

Covers ``merge_channels`` (session-clock key, nearest sample, ties, missing
keys), multi-lap frames (``Time`` from the first lap's start, the lap-boundary
sample kept once), ``slice_by_time(interpolate_edges=True)``, the padded
``get_car_data`` index, ``add_distance``/``add_relative_distance``
``drop_existing``, ``fill_missing`` on discrete channels and
``register_new_channel``. Every operation is run on both backends and the
polars result compared with the pandas one value by value. Sessions are
served from memory: nothing reaches the network.
"""

from __future__ import annotations

import datetime as dt
import math
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pytest

from tif1 import core
from tif1.config import get_config
from tif1.models import Telemetry, _join_lap_telemetry
from tif1.models_polars import PolarsTelemetry, _join_lap_frames
from tif1.payload_loader import InMemoryTransport

T0 = pd.Timestamp("2025-04-13 14:55:00")


def assert_same(pandas_frame: pd.DataFrame, polars_frame: pl.DataFrame) -> None:
    """The polars frame equals the pandas one: columns, rows and every value (dtype families)."""
    other = polars_frame.to_pandas()
    expected = pd.DataFrame(pandas_frame).reset_index(drop=True)
    assert list(other.columns) == list(expected.columns)
    assert len(other) == len(expected)
    for column in expected.columns:
        left, right = expected[column], other[column]
        if left.dtype.kind in "mM" or right.dtype.kind in "mM":
            left_ns = left.astype("int64" if left.dtype.kind == "m" else "datetime64[ns]")
            right_ns = right.astype("int64" if right.dtype.kind == "m" else "datetime64[ns]")
            assert (left.isna().to_numpy() == right.isna().to_numpy()).all(), column
            assert left_ns[left.notna()].tolist() == right_ns[right.notna()].tolist(), column
        elif pd.api.types.is_float_dtype(left) or pd.api.types.is_float_dtype(right):
            a = pd.to_numeric(left).to_numpy(dtype="float64", na_value=np.nan)
            b = pd.to_numeric(right).to_numpy(dtype="float64", na_value=np.nan)
            assert np.array_equal(a, b, equal_nan=True), column
        else:
            assert left.isna().tolist() == right.isna().tolist(), column
            assert (
                left[left.notna()].astype(str).tolist() == right[right.notna()].astype(str).tolist()
            ), column


def frames(data: dict[str, Any]) -> tuple[Telemetry, PolarsTelemetry]:
    """The same synthetic telemetry as a pandas ``Telemetry`` and a ``PolarsTelemetry``."""
    pandas_frame = Telemetry(pd.DataFrame(data))
    return pandas_frame, PolarsTelemetry._from_frame(pl.from_pandas(pd.DataFrame(data)))


def _ms(values: list[float]) -> pd.Series:
    return pd.Series(pd.to_timedelta(values, unit="ms")).astype("timedelta64[ns]")


def lap_frame(lap: int, start_ms: float, n: int = 5, step_ms: float = 500.0) -> pd.DataFrame:
    """One lap's frame: ``n`` samples from ``start_ms`` session time, ``Time`` lap-relative."""
    times = [i * step_ms for i in range(n)]
    session_time = _ms([start_ms + t for t in times])
    return pd.DataFrame(
        {
            "Date": T0 + session_time,
            "SessionTime": session_time,
            "DriverAhead": pd.Series(["44"] * n, dtype=object),
            "Time": _ms(times),
            "Speed": [100.0 + 10 * i + lap for i in range(n)],
            "nGear": pd.array([lap + i % 2 for i in range(n)], dtype="Int64"),
            "Brake": [i % 2 == 0 for i in range(n)],
            "Source": pd.Series(["merged"] * n, dtype=object),
            "Distance": [25.0 * i for i in range(n)],
            "Driver": pd.Series(["VER"] * n, dtype=object),
            "LapNumber": pd.array([lap] * n, dtype="Int64"),
        }
    )


def three_laps() -> pd.DataFrame:
    """Laps 1-3 back to back (2 s each): each lap repeats the previous lap's last sample."""
    return pd.concat([lap_frame(lap, 2000.0 * lap) for lap in (1, 2, 3)], ignore_index=True)


# -- merge_channels -----------------------------------------------------------------------


class TestMergeChannels:
    def test_multi_lap_frames_are_matched_on_the_session_clock(self):
        left_pd, left_pl = frames(three_laps())
        # One sample per lap, 100 ms into it: lap-relative Time would match
        # every lap's first samples to lap 1's sample.
        other = pd.DataFrame(
            {
                "Date": T0 + _ms([2100.0, 4100.0, 6100.0]),
                "Time": _ms([100.0, 100.0, 100.0]),
                "OtherLap": [1, 2, 3],
            }
        )
        merged_pd = left_pd.merge_channels(other)
        merged_pl = left_pl.merge_channels(pl.from_pandas(other))
        # Samples every 500 ms from 2 s; the other samples sit at 2.1, 4.1 and 6.1 s.
        nearest = [1, 1, 1, 2, 2] + [2, 2, 2, 3, 3] + [3] * 5
        assert merged_pd["OtherLap"].tolist() == nearest
        assert merged_pd["Time_other"].notna().all()
        assert_same(merged_pd, merged_pl)

    @pytest.mark.parametrize(
        ("keep", "key"),
        [(("Date", "SessionTime", "Time"), "Date"), (("SessionTime", "Time"), "SessionTime")],
    )
    def test_key_is_the_first_shared_clock(self, keep, key):
        data = three_laps()
        left_pd, left_pl = frames(data)
        other = data[list(keep)].iloc[::4].reset_index(drop=True)
        other["Marker"] = np.arange(len(other), dtype=float)
        merged_pd = left_pd.merge_channels(other)
        assert merged_pd[key].is_monotonic_increasing
        assert_same(merged_pd, left_pl.merge_channels(pl.from_pandas(other)))

    def test_ties_go_to_the_earlier_sample_and_exact_matches_to_the_last(self):
        left_pd, left_pl = frames({"Time": _ms([0.0, 50.0, 100.0]), "Speed": [1.0, 2.0, 3.0]})
        other = pd.DataFrame({"Time": _ms([0.0, 0.0, 100.0]), "Extra": [10.0, 11.0, 12.0]})
        merged_pd = left_pd.merge_channels(other)
        # 0 ms: the last of the two 0 ms samples; 50 ms: tie between 0 and 100 -> earlier.
        assert merged_pd["Extra"].tolist() == [11.0, 11.0, 12.0]
        assert_same(merged_pd, left_pl.merge_channels(pl.from_pandas(other)))

    def test_rows_with_a_missing_clock_are_kept_last_with_nulls(self):
        data = {
            "Date": [T0 + pd.Timedelta(seconds=2), pd.NaT, T0, T0 + pd.Timedelta(seconds=1)],
            "Speed": [3.0, 9.0, 1.0, 2.0],
        }
        left_pd, left_pl = frames(data)
        other = pd.DataFrame(
            {
                "Date": [T0 + pd.Timedelta(seconds=1), pd.NaT],
                "Flag": [True, False],
                "Count": [5, 6],
            }
        )
        merged_pd = left_pd.merge_channels(other)
        assert merged_pd["Speed"].tolist() == [1.0, 2.0, 3.0, 9.0]
        assert merged_pd["Count"].iloc[:3].tolist() == [5, 5, 5]
        assert pd.isna(merged_pd["Count"].iloc[3])
        assert list(merged_pd.index) == [0, 1, 2, 3]
        merged_pl = left_pl.merge_channels(pl.from_pandas(other))
        assert merged_pl.height == 4
        assert merged_pl["Count"].to_list() == [5, 5, 5, None]
        assert_same(merged_pd, merged_pl)

    def test_empty_other_gives_null_columns(self):
        left_pd, left_pl = frames({"Time": _ms([0.0, 1.0]), "Speed": [1.0, 2.0]})
        other = pd.DataFrame({"Time": pd.Series([], dtype="timedelta64[ns]"), "X": []})
        merged_pd = left_pd.merge_channels(other)
        assert merged_pd["X"].isna().all()
        assert_same(merged_pd, left_pl.merge_channels(pl.from_pandas(other)))


# -- multi-lap frames ------------------------------------------------------------------------


class TestJoinLapFrames:
    def test_boundary_repeat_dropped_and_time_from_the_first_lap_start(self):
        data = three_laps()
        joined_pd = _join_lap_telemetry(Telemetry(data), 2_000_000_000)
        assert len(joined_pd) == 13
        assert not joined_pd["Date"].duplicated().any()
        # The repeat is kept as the earlier lap's last sample.
        assert joined_pd["LapNumber"].tolist() == [1] * 5 + [2] * 4 + [3] * 4
        assert (joined_pd["Time"] == joined_pd["SessionTime"] - pd.Timedelta(seconds=2)).all()
        assert joined_pd["Time"].is_monotonic_increasing
        assert list(joined_pd.index) == list(range(13))
        joined_pl = _join_lap_frames(pl.from_pandas(data), 2_000_000_000)
        assert_same(joined_pd, joined_pl)

    def test_origin_defaults_to_the_earliest_lap_start_in_the_frame(self):
        data = three_laps().iloc[5:]  # lap 1 missing
        joined_pd = _join_lap_telemetry(Telemetry(data.reset_index(drop=True)))
        assert joined_pd["Time"].iloc[0] == pd.Timedelta(0)
        assert_same(joined_pd, _join_lap_frames(pl.from_pandas(data), None))

    def test_out_of_order_laps_are_put_in_session_order(self):
        data = pd.concat([lap_frame(lap, 2000.0 * lap) for lap in (3, 1, 2)], ignore_index=True)
        joined_pd = _join_lap_telemetry(Telemetry(data), 2_000_000_000)
        assert joined_pd["SessionTime"].is_monotonic_increasing
        assert joined_pd["LapNumber"].tolist() == [1] * 5 + [2] * 4 + [3] * 4
        assert_same(joined_pd, _join_lap_frames(pl.from_pandas(data), 2_000_000_000))

    def test_one_lap_is_unchanged(self):
        data = lap_frame(2, 4000.0)
        tel = Telemetry(data)
        assert _join_lap_telemetry(tel, 4_000_000_000) is tel
        frame = pl.from_pandas(data)
        assert _join_lap_frames(frame, 4_000_000_000) is frame

    def test_without_a_session_clock_nothing_changes(self):
        data = three_laps().drop(columns=["Date", "SessionTime"])
        tel = Telemetry(data)
        assert _join_lap_telemetry(tel, 0) is tel
        assert _join_lap_frames(pl.from_pandas(data), 0).height == 15

    def test_multi_driver_frames_keep_each_drivers_block_and_origin(self):
        ver = three_laps()
        ham = pd.concat([lap_frame(lap, 2000.0 * lap + 300.0) for lap in (1, 2)])
        ham["Driver"] = "HAM"
        data = pd.concat([ver, ham], ignore_index=True)
        joined_pd = _join_lap_telemetry(Telemetry(data), multi_driver=True)
        assert joined_pd["Driver"].tolist() == ["VER"] * 13 + ["HAM"] * 9
        ham_rows = joined_pd[joined_pd["Driver"] == "HAM"]
        assert ham_rows["Time"].iloc[0] == pd.Timedelta(0)
        assert_same(joined_pd, _join_lap_frames(pl.from_pandas(data), None, multi_driver=True))


# -- slice_by_time(interpolate_edges=True) ---------------------------------------------------


def _edge_frame() -> pd.DataFrame:
    data = lap_frame(1, 1000.0, n=6)
    data["DriverAhead"] = pd.Series(["44", None, "44", "16", None, "16"], dtype=object)
    data["Int"] = pd.array([10, 20, 30, 40, 50, 60], dtype="Int64")
    return data


class TestInterpolateEdges:
    def test_one_sample_at_each_edge_and_nothing_else_changes(self):
        data = _edge_frame()
        tel_pd, tel_pl = frames(data)
        start, end = pd.Timedelta(milliseconds=1250), pd.Timedelta(milliseconds=2600)
        edged = tel_pd.slice_by_time(start, end, interpolate_edges=True)
        plain = tel_pd.slice_by_time(start, end)
        assert len(edged) == len(plain) + 2
        assert edged["SessionTime"].iloc[0] == start
        assert edged["SessionTime"].iloc[-1] == end
        assert edged["Time"].iloc[0] == pd.Timedelta(0)
        assert edged["Date"].iloc[0] == T0 + start
        # Continuous: linear between the neighbours (1000 ms -> 101, 1500 ms -> 111).
        assert edged["Speed"].iloc[0] == pytest.approx(106.0)
        assert edged["Distance"].iloc[0] == pytest.approx(12.5)
        assert edged["Int"].iloc[0] == 15  # integer continuous: rounded
        # Discrete and text: the value in force; Source marks the sample.
        assert edged["nGear"].iloc[0] == 1
        assert edged["Brake"].iloc[0]
        assert edged["DriverAhead"].iloc[-1] == "16"  # the 2.5 s sample's
        assert edged["Source"].iloc[0] == "interpolation"
        assert edged["nGear"].dtype == "Int64"
        # The samples in between are the plain slice, genuine gaps included.
        inner = pd.DataFrame(edged).iloc[1:-1].reset_index(drop=True)
        assert inner.equals(pd.DataFrame(plain).reset_index(drop=True))
        assert inner["DriverAhead"].isna().sum() == 1
        assert_same(edged, tel_pl.slice_by_time(start, end, interpolate_edges=True))

    def test_an_edge_on_a_sample_is_not_duplicated(self):
        tel_pd, tel_pl = frames(_edge_frame())
        start, end = pd.Timedelta(milliseconds=1500), pd.Timedelta(milliseconds=3000)
        edged = tel_pd.slice_by_time(start, end, interpolate_edges=True)
        assert len(edged) == len(tel_pd.slice_by_time(start, end))
        assert_same(edged, tel_pl.slice_by_time(start, end, interpolate_edges=True))

    @pytest.mark.parametrize("pad_side", ["both", "before", "after"])
    def test_padding_counts_the_samples_around_the_window(self, pad_side):
        tel_pd, tel_pl = frames(_edge_frame())
        start, end = pd.Timedelta(milliseconds=2250), pd.Timedelta(milliseconds=2750)
        edged = tel_pd.slice_by_time(start, end, pad=1, pad_side=pad_side, interpolate_edges=True)
        before = pad_side in ("both", "before")
        after = pad_side in ("both", "after")
        assert len(edged) == 3 + before + after  # 2 edges + 1 sample + padding
        assert_same(
            edged,
            tel_pl.slice_by_time(start, end, pad=1, pad_side=pad_side, interpolate_edges=True),
        )

    def test_edges_beyond_the_data_hold_the_nearest_sample(self):
        tel_pd, tel_pl = frames(_edge_frame())
        start, end = pd.Timedelta(milliseconds=0), pd.Timedelta(milliseconds=9000)
        edged = tel_pd.slice_by_time(start, end, interpolate_edges=True)
        assert len(edged) == 8
        assert edged["Speed"].iloc[0] == edged["Speed"].iloc[1]
        assert edged["Speed"].iloc[-1] == edged["Speed"].iloc[-2]
        assert_same(edged, tel_pl.slice_by_time(start, end, interpolate_edges=True))

    def test_unsorted_frames_are_read_in_clock_order(self):
        data = _edge_frame().iloc[::-1].reset_index(drop=True)
        tel_pd, tel_pl = frames(data)
        start, end = pd.Timedelta(milliseconds=1250), pd.Timedelta(milliseconds=2600)
        edged = tel_pd.slice_by_time(start, end, interpolate_edges=True)
        assert edged["SessionTime"].is_monotonic_increasing
        assert_same(edged, tel_pl.slice_by_time(start, end, interpolate_edges=True))

    def test_reversed_window_is_empty(self):
        tel_pd, tel_pl = frames(_edge_frame())
        start, end = pd.Timedelta(milliseconds=2600), pd.Timedelta(milliseconds=1250)
        assert tel_pd.slice_by_time(start, end, interpolate_edges=True).empty
        assert tel_pl.slice_by_time(start, end, interpolate_edges=True).height == 0


# -- add_distance / add_relative_distance ------------------------------------------------


class TestDistanceDropExisting:
    def test_add_distance_replaces_or_keeps_the_column(self):
        data = three_laps()
        data["Time"] = data["SessionTime"] - data["SessionTime"].iloc[0]
        tel_pd, tel_pl = frames(data)
        assert tel_pd.add_distance(drop_existing=False) is tel_pd
        assert tel_pl.add_distance(drop_existing=False) is tel_pl
        replaced = tel_pd.add_distance()
        assert list(replaced.columns) == list(data.columns)
        assert np.diff(replaced["Distance"].to_numpy()).min() >= 0  # no per-lap resets
        assert replaced["Distance"].tolist() == tel_pd.integrate_distance().tolist()
        assert_same(replaced, tel_pl.add_distance())

    def test_add_relative_distance_follows_fastf1(self):
        data = three_laps()
        tel_pd, tel_pl = frames(data)
        added = tel_pd.add_relative_distance(drop_existing=False)  # no column yet: added
        expected = data["Distance"] / data["Distance"].iloc[-1]
        assert added["RelativeDistance"].tolist() == expected.tolist()
        assert_same(added, tel_pl.add_relative_distance())
        again = added.add_relative_distance(drop_existing=False)
        assert again is added
        no_distance_pd = Telemetry(data.drop(columns=["Distance"]))
        integrated = no_distance_pd.add_relative_distance()
        assert "Distance" not in integrated.columns
        assert integrated["RelativeDistance"].iloc[-1] == 1.0
        no_distance_pl = PolarsTelemetry._from_frame(pl.from_pandas(data.drop(columns="Distance")))
        assert_same(integrated, no_distance_pl.add_relative_distance())

    def test_without_speed_the_column_is_missing_values(self):
        data = three_laps().drop(columns=["Speed", "Distance"])
        tel_pd, tel_pl = frames(data)
        assert tel_pd.add_distance()["Distance"].isna().all()
        assert tel_pl.add_distance()["Distance"].null_count() == len(data)


# -- fill_missing ---------------------------------------------------------------------------


class TestFillMissing:
    def test_discrete_channels_are_held_and_keep_their_dtype(self):
        data = lap_frame(3, 0.0, n=6)
        data["nGear"] = pd.array([None, 2, None, None, 5, None], dtype="Int64")
        data["Speed"] = [np.nan, 1.0, np.nan, 3.0, np.nan, np.nan]
        data["DriverAhead"] = pd.Series([None, "1", None, "16", None, None], dtype=object)
        data["Source"] = pd.Series(["merged", None, "merged", None, "merged", "merged"])
        data["LapNumber"] = pd.array([3, None, 3, 3, None, 3], dtype="Int64")
        tel_pd, tel_pl = frames(data)
        filled = tel_pd.fill_missing()
        assert filled["nGear"].dtype == "Int64"
        assert filled["nGear"].tolist() == [2, 2, 2, 2, 5, 5]
        assert filled["Speed"].tolist() == [1.0, 1.0, 2.0, 3.0, 3.0, 3.0]
        assert filled["DriverAhead"].tolist() == ["1", "1", "1", "16", "16", "16"]
        assert filled["Source"].tolist()[:2] == ["merged", "interpolation"]
        assert filled["LapNumber"].tolist() == [3] * 6
        assert filled["SessionTime"].equals(tel_pd["SessionTime"])
        assert_same(filled, tel_pl.fill_missing())

    def test_columns_without_gaps_are_untouched(self):
        tel_pd, tel_pl = frames(lap_frame(1, 0.0))
        assert pd.DataFrame(tel_pd.fill_missing()).equals(pd.DataFrame(tel_pd))
        assert tel_pl.fill_missing().equals(tel_pl)


# -- register_new_channel ----------------------------------------------------------------


@pytest.fixture
def channels(monkeypatch):
    """Both registries restored after the test."""
    monkeypatch.setattr(Telemetry, "_CHANNELS", dict(Telemetry._CHANNELS))
    monkeypatch.setattr(PolarsTelemetry, "_CHANNELS", dict(PolarsTelemetry._CHANNELS))


@pytest.mark.usefixtures("channels")
class TestRegisterNewChannel:
    @pytest.mark.parametrize("cls", [Telemetry, PolarsTelemetry])
    def test_fastf1_signature_and_storage(self, cls):
        cls.register_new_channel("Ignored", "excluded")
        cls.register_new_channel("Gear2", "discrete")
        cls.register_new_channel("Boost", "continuous", "linear")
        assert cls._CHANNELS["Ignored"] == {"type": "excluded", "method": None}
        assert cls._CHANNELS["Boost"] == {"type": "continuous", "method": "linear"}
        assert cls._CHANNELS["RPM"] == {"type": "continuous", "method": "index"}
        assert cls._CHANNELS["Date"] == {"type": "excluded"}
        with pytest.raises(ValueError, match="requires interpolation_method"):
            cls.register_new_channel("NoMethod", "continuous")
        with pytest.raises(ValueError, match="Unknown signal type"):
            cls.register_new_channel("Bad", "fuzzy")

    def test_registered_types_drive_fill_missing(self):
        for cls in (Telemetry, PolarsTelemetry):
            cls.register_new_channel("Mode", "discrete")
            cls.register_new_channel("Raw", "excluded")
        data = {
            "Mode": [1.0, np.nan, 3.0],
            "Raw": [1.0, np.nan, 3.0],
            "Free": [1.0, np.nan, 3.0],
        }
        tel_pd, tel_pl = frames(data)
        filled = tel_pd.fill_missing()
        assert filled["Mode"].tolist() == [1.0, 1.0, 3.0]
        assert math.isnan(filled["Raw"].iloc[1])
        assert filled["Free"].tolist() == [1.0, 2.0, 3.0]
        assert_same(filled, tel_pl.fill_missing())


# -- sessions: Laps.telemetry, get_car_data, car_data -----------------------------------------

LAP_TIME = 2.0
LAP_STARTS = [300.0, 302.0, 304.0]
TEL_SAMPLES = 9  # 0.25 s apart: the last sample of a lap is the first of the next


def _iso(seconds: float) -> str:
    return (T0 + pd.Timedelta(seconds=seconds)).isoformat()


def _laptimes(code: str, number: str) -> dict[str, Any]:
    n = len(LAP_STARTS)
    ends = [start + LAP_TIME for start in LAP_STARTS]
    return {
        "time": [LAP_TIME] * n,
        "lap": list(range(1, n + 1)),
        "compound": ["SOFT"] * n,
        "stint": [1] * n,
        "s1": [0.5] * n,
        "s2": [0.5] * n,
        "s3": [1.0] * n,
        "life": list(range(1, n + 1)),
        "pos": [1.0] * n,
        "status": ["1"] * n,
        "pb": [False] * n,
        "sesT": ends,
        "drv": [code] * n,
        "dNum": [number] * n,
        "pout": [None] * n,
        "pin": [None] * n,
        "s1T": [start + 0.5 for start in LAP_STARTS],
        "s2T": [start + 1.0 for start in LAP_STARTS],
        "s3T": ends,
        "vi1": [300.0] * n,
        "vi2": [300.0] * n,
        "vfl": [300.0] * n,
        "vst": [300.0] * n,
        "fresh": [True] * n,
        "team": ["Red Bull Racing"] * n,
        "lST": LAP_STARTS,
        "lSD": [_iso(start) for start in LAP_STARTS],
        "del": [False] * n,
        "delR": [None] * n,
        "ff1G": [False] * n,
        "iacc": [True] * n,
        "wT": [start + 0.1 for start in LAP_STARTS],
        "wAT": [27.0] * n,
        "wH": [45.0] * n,
        "wP": [1017.0] * n,
        "wR": [False] * n,
        "wTT": [35.0] * n,
        "wWD": [200] * n,
        "wWS": [1.0] * n,
    }


def _tel(lap: int) -> dict[str, Any]:
    n = TEL_SAMPLES
    return {
        "tel": {
            "time": [i * 0.25 for i in range(n)],
            "rpm": [10000.0 + 100 * i for i in range(n)],
            "speed": [200.0 + i + 10 * lap for i in range(n)],
            "gear": [7] * n,
            "throttle": [100.0] * n,
            "brake": [False] * n,
            "drs": [0] * n,
            "distance": [i * 14.0 for i in range(n)],
            "rel_distance": [i / (n - 1) for i in range(n)],
            "driver_ahead": ["44"] * n,
            "distance_to_driver_ahead": [12.0] * n,
            "x": [float(i) for i in range(n)],
            "y": [0.0] * n,
            "z": [0.0] * n,
        }
    }


def _payloads() -> dict[str, Any]:
    payloads: dict[str, Any] = {
        "drivers.json": {
            "drivers": [
                {"driver": "VER", "dn": "1", "team": "Red Bull Racing", "fn": "Max", "ln": "V"}
            ]
        },
        "rcm.json": {
            key: []
            for key in ("time", "cat", "msg", "status", "flag", "scope", "sector", "dNum", "lap")
        },
        "weather.json": {
            "wT": [250.0],
            "wAT": [26.0],
            "wH": [40.0],
            "wP": [1010.0],
            "wR": [False],
            "wTT": [30.0],
            "wWD": [100],
            "wWS": [1.0],
        },
        "VER/laptimes.json": _laptimes("VER", "1"),
    }
    for lap in (1, 2, 3):
        payloads[f"VER/{lap}_tel.json"] = _tel(lap)
    return payloads


@pytest.fixture
def sessions(monkeypatch) -> dict[str, core.Session]:
    """A pandas and a polars 2025 Bahrain Race served from memory (laps back to back)."""
    payloads = _payloads()

    async def fetch_multiple(requests, **_kwargs):
        return [payloads.get(request[3]) for request in requests]

    monkeypatch.setattr(core, "fetch_multiple_async", fetch_multiple)
    monkeypatch.setitem(get_config()._config, "prefetch_all_telemetry_after_laps_load", False)
    built = {}
    for lib in ("pandas", "polars"):
        session = core.Session(2025, "Bahrain Grand Prix", "Race", enable_cache=False, lib=lib)
        session._payload_loader.transport = InMemoryTransport(payloads)
        built[lib] = session
    return built


class TestSessionTelemetry:
    def test_laps_telemetry_is_one_continuous_slice(self, sessions):
        tel_pd = sessions["pandas"].laps.pick_drivers("VER").telemetry
        tel_pl = sessions["polars"].laps.pick_drivers("VER").telemetry
        assert len(tel_pd) == 3 * TEL_SAMPLES - 2
        assert not tel_pd["Date"].duplicated().any()
        first_start = pd.Timedelta(seconds=LAP_STARTS[0])
        assert (tel_pd["Time"] == tel_pd["SessionTime"] - first_start).all()
        assert tel_pd["Time"].is_monotonic_increasing
        assert_same(tel_pd, tel_pl)

    def test_one_lap_keeps_its_lap_relative_time(self, sessions):
        lap = sessions["pandas"].laps.pick_drivers("VER").pick_laps(2)
        tel = lap.telemetry
        assert len(tel) == TEL_SAMPLES
        assert tel["Time"].iloc[0] == pd.Timedelta(0)
        assert (tel["SessionTime"] - tel["Time"] == pd.Timedelta(seconds=LAP_STARTS[1])).all()
        assert_same(tel, sessions["polars"].laps.pick_drivers("VER").pick_laps(2).telemetry)

    def test_get_car_data_with_and_without_pad(self, sessions):
        laps_pd = sessions["pandas"].laps.pick_drivers("VER").pick_laps([2, 3])
        laps_pl = sessions["polars"].laps.pick_drivers("VER").pick_laps([2, 3])
        plain = laps_pd.get_car_data()
        assert plain["Time"].iloc[0] == pd.Timedelta(0)
        assert plain["Time"].is_monotonic_increasing
        assert_same(plain, laps_pl.get_car_data())
        padded = laps_pd.get_car_data(pad=1)
        assert list(padded.index) == list(range(len(padded)))  # fastf1's reset_index
        assert padded.session is sessions["pandas"]
        assert len(padded) == len(plain) + 1  # one sample before; none after lap 3
        assert padded["Time"].iloc[0] < pd.Timedelta(0)
        assert padded["Time"].is_monotonic_increasing
        assert_same(padded, laps_pl.get_car_data(pad=1))
        lap = laps_pd.iloc[0]
        lap_padded = lap.get_car_data(pad=2, pad_side="before")
        assert list(lap_padded.index) == list(range(len(lap_padded)))

    def test_car_data_and_its_session_long_clock(self, sessions):
        car_pd = sessions["pandas"].car_data["1"]
        car_pl = sessions["polars"].car_data["1"]
        assert not car_pd["Date"].duplicated().any()
        assert_same(car_pd, car_pl)
        # fastf1's session-long clock: car_data's ``Time`` is ``SessionTime``.
        assert (car_pd["Time"] == car_pd["SessionTime"]).all()
        laps_tel = sessions["pandas"].laps.pick_drivers("VER").telemetry
        assert not laps_tel["Time"].equals(car_pd["Time"])  # the laps keep their slice clock
        pd.testing.assert_frame_equal(
            pd.DataFrame(laps_tel._session_time_as_time()), pd.DataFrame(car_pd)
        )

    def test_slice_by_lap_with_interpolated_edges(self, sessions):
        laps_pd = sessions["pandas"].laps.pick_drivers("VER")
        laps_pl = sessions["polars"].laps.pick_drivers("VER")
        car_pd = sessions["pandas"].car_data["1"]
        window = laps_pd.pick_laps(2)
        edged = car_pd.slice_by_lap(window, interpolate_edges=True)
        # Both lap edges are samples: nothing is interpolated.
        assert len(edged) == TEL_SAMPLES
        assert_same(
            edged,
            sessions["polars"]
            .car_data["1"]
            .slice_by_lap(laps_pl.pick_laps(2), interpolate_edges=True),
        )


def test_polars_time_column_keeps_its_duration_unit():
    data = pl.from_pandas(three_laps()).with_columns(pl.col("Time").cast(pl.Duration("ms")))
    joined = _join_lap_frames(data, 2_000_000_000)
    assert joined.schema["Time"] == pl.Duration("ms")
    assert joined["Time"][0] == dt.timedelta(0)
