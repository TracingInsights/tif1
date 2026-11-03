"""Elis E6: the polars telemetry reads without numpy, and the eager IPC row read.

Each numpy-free path is pinned to the numpy path it replaces (same frame:
values, dtypes, nulls, column order), and every input outside its clean
cases is shown to fall back to the numpy path unchanged. "numpy not
imported" is simulated with ``sys.modules["numpy"] = None`` (the import
blocker marker); numpy itself stays loaded in this process, so the
reference builds still run.
"""

from __future__ import annotations

import datetime as dt
import io
import math
import sys

import numpy as np
import orjson
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from tif1 import cache as cache_mod
from tif1 import core
from tif1 import models_polars as mp
from tif1.core import _fastest_laps_polars, _polars_ns_list
from tif1.core_utils import helpers
from tif1.core_utils import session_state as ss
from tif1.core_utils.json_utils import decode_one_telemetry_payload, decode_telemetry_payload

NS = pl.Duration("ns")
_NS_MIN = -(2**63)
_T0 = dt.datetime(2025, 4, 13, 15)  # noqa: DTZ001 - naive, as the tier clocks


@pytest.fixture(autouse=True)
def _polars_bound():
    helpers._ensure_polars_available()
    core._ensure_polars_available()


@pytest.fixture
def numpy_hidden(monkeypatch):
    """Make the process look as if numpy had not been imported."""
    monkeypatch.setitem(sys.modules, "numpy", None)


# --------------------------------------------------------------------------- #
# IPC row reader
# --------------------------------------------------------------------------- #


def _tier_frame() -> pl.DataFrame:
    n = 5
    return pl.DataFrame(
        {
            "Date": pl.Series([_T0 + dt.timedelta(seconds=i) for i in range(n)]),
            "SessionTime": pl.Series([i * 250_000_000 for i in range(n)]).cast(NS),
            "DriverAhead": ["81", None, "81", "4", None],
            "Speed": [200.0, None, 201.5, 202.0, 203.0],
            "nGear": [7, 7, 8, 8, 8],
            "Brake": [False, True, False, None, False],
            "Source": pl.Series(["car"] * n, dtype=pl.Categorical),
            "Driver": ["VER"] * n,
            "LapNumber": [1, 1, 2, 2, 2],
        }
    )


class TestIpcRowReader:
    def test_stream_reader_matches_the_file_reader(self):
        frame = _tier_frame()
        blob = cache_mod._write_polars_ipc(frame)
        assert blob.startswith(b"ARROW1")
        expected = pl.read_ipc(blob)
        got = cache_mod._read_polars_ipc_row(blob)
        assert_frame_equal(got, expected, check_exact=True)
        assert got.schema == expected.schema
        assert got.n_chunks("all") == [1] * got.width
        assert_frame_equal(got, frame, check_exact=True)

    def test_polars_written_file_reads_too(self):
        frame = _tier_frame()
        buffer = io.BytesIO()
        frame.write_ipc(buffer, compression="lz4")
        assert_frame_equal(cache_mod._read_polars_ipc_row(buffer.getvalue()), frame)

    def test_without_the_stream_reader_falls_back_to_read_ipc(self, monkeypatch):
        frame = _tier_frame()
        blob = cache_mod._write_polars_ipc(frame)
        monkeypatch.setattr(cache_mod, "_IPC_STREAM_READER", False)
        calls = []
        real = pl.read_ipc
        monkeypatch.setattr(pl, "read_ipc", lambda data: calls.append(1) or real(data))
        assert_frame_equal(cache_mod._read_polars_ipc_row(blob), frame)
        assert calls == [1]

    def test_non_file_bytes_go_through_read_ipc(self, monkeypatch):
        calls = []
        monkeypatch.setattr(pl, "read_ipc", lambda data: calls.append(data) or "sentinel")
        assert cache_mod._read_polars_ipc_row(b"not an arrow file") == "sentinel"
        assert calls == [b"not an arrow file"]

    def test_a_truncated_row_raises(self):
        blob = cache_mod._write_polars_ipc(_tier_frame())
        with pytest.raises(pl.exceptions.PolarsError):
            cache_mod._read_polars_ipc_row(blob[:200])


# --------------------------------------------------------------------------- #
# Lone payload decode
# --------------------------------------------------------------------------- #

_PAYLOAD = orjson.dumps(
    {
        "time": [0.0, 0.25, 0.5],
        "speed": [200, 201.5, "None"],
        "gear": [7, 7, 8],
        "brake": [False, True, False],
        "DriverAhead": ["81", "None", None],
    }
)


class TestLonePayloadDecode:
    @pytest.mark.usefixtures("numpy_hidden")
    def test_orjson_lists_while_numpy_is_not_imported(self):
        decoded = decode_one_telemetry_payload(_PAYLOAD)
        assert decoded == orjson.loads(_PAYLOAD)
        assert all(type(values) is list for values in decoded.values())

    def test_native_decoder_once_numpy_is_resident(self):
        decoded = decode_one_telemetry_payload(_PAYLOAD)
        reference = decode_telemetry_payload(_PAYLOAD)
        assert decoded.keys() == reference.keys()
        assert {k: type(v) for k, v in decoded.items()} == {
            k: type(v) for k, v in reference.items()
        }

    @pytest.mark.usefixtures("numpy_hidden")
    def test_channel_types_without_numpy(self, monkeypatch):
        monkeypatch.setattr(helpers, "_TELEMETRY_CHANNEL_TYPES", None)
        assert helpers._telemetry_channel_types() == (list, helpers.DictionaryChannel)
        assert helpers._TELEMETRY_CHANNEL_TYPES is None  # not memoized before numpy
        assert helpers._channel_list([1, 2]) == [1, 2]
        assert helpers._channel_list("dataKey") == "dataKey"


# --------------------------------------------------------------------------- #
# List telemetry builder
# --------------------------------------------------------------------------- #


def _channels(**overrides):
    n = 4
    channels = {
        "Time": [0.0, 0.25, 0.5, 0.75],
        "RPM": [10000, 10100, 10200, 10300],
        "Speed": [200, 201.5, 202, 203.25],
        "nGear": [7, 7, 8, 8],
        "Throttle": [100.0, 99.5, "None", 100.0],
        "Brake": [False, True, 0, 1],
        "DRS": [0, 0, 12, 12],
        "Distance": [0.0, 14.0, 28.1, 42.2],
        "RelativeDistance": [0.0, 0.1, 0.2, 0.3],
        "DriverAhead": ["81", None, "81", None],
        "DistanceToDriverAhead": [None] * n,
        "X": [1.0, 2.0, 3.0, 4.0],
        "Y": [0.0] * n,
        "Z": [0, 0, 0, 0],
        "AccelerationX": [0.1, -0.2, "None", "None"],
    }
    channels.update(overrides)
    return {name: values for name, values in channels.items() if values is not None}


_CLOCKS = (None, (3_600_000_000_000, 1_744_556_400_000_000_000), (3_600_000_000_000, _NS_MIN))


class TestListTelemetryBuilder:
    @pytest.mark.parametrize("clock", _CLOCKS)
    @pytest.mark.parametrize(
        "overrides",
        [
            {},
            {"Time": [0, 1, 2.5, 3]},  # ints in the clock
            {"Time": [0.1234567895, 0.5, 1.0000000005, 2.0]},  # half-even rounding
            {"Speed": [1, 2, 3, 4]},  # an all-int numeric channel stays Int64
            {"Brake": [True, False, True, True]},
            {"DriverAhead": ["None", "None", "None", "None"]},
            {"X": None, "Y": None, "Z": None, "nGear": None},  # missing channels
            {"DistanceToDriverAhead": ["None", 1.5, "None", 2]},
        ],
    )
    def test_equals_the_numpy_builder(self, overrides, clock):
        channels = _channels(**overrides)
        channels["DriverAhead"] = helpers._without_null_tokens(channels["DriverAhead"])
        twin = helpers._polars_list_telemetry_frame(dict(channels), "VER", 3, clock)
        reference = helpers._polars_typed_telemetry_frame(dict(channels), "VER", 3, clock)
        assert twin is not helpers._UNHANDLED
        assert reference is not None
        assert_frame_equal(twin, reference, check_exact=True)
        assert twin.columns == reference.columns

    def test_equals_the_native_decoded_build(self):
        raw = orjson.dumps(
            {
                "time": [0.0, 0.25, 0.5],
                "rpm": [10000, 10100, 10200],
                "speed": [200, 201.5, "None"],
                "gear": [7, 7, 8],
                "brake": [False, True, False],
                "drs": [0, 0, 12],
                "DriverAhead": ["81", "None", None],
                "DistanceToDriverAhead": ["None", 4.5, 5.0],
                "x": [1.0, 2.0, 3.0],
                "y": [0.0, 0.0, 0.0],
                "z": [0.0, 0.0, 0.0],
                "dataKey": "abc",
            }
        )
        clock = _CLOCKS[1]
        native = helpers._create_telemetry_df(
            decode_telemetry_payload(raw), "VER", 2, "polars", clock=clock
        )
        sys.modules["numpy"] = None
        try:
            lists = helpers._create_telemetry_df(orjson.loads(raw), "VER", 2, "polars", clock=clock)
        finally:
            sys.modules["numpy"] = np
        assert_frame_equal(lists, native, check_exact=True)

    @pytest.mark.parametrize(
        "overrides",
        [
            {"Speed": [200.0, None, 201.0, 202.0]},  # a None sample
            {"Speed": [True, 1.0, 2.0, 3.0]},  # a bool in a numeric channel
            {"Speed": [1, 2, 3, 2**63]},  # beyond int64
            {"Speed": ["fast", 1.0, 2.0, 3.0]},  # an unexpected string
            {"nGear": [7, 7.0, 8, 8]},  # a float gear
            {"nGear": [7, "None", 8, 8]},
            {"Brake": [False, None, True, True]},
            {"Brake": [0.0, 1.0, 0.0, 1.0]},
            {"Time": [0.0, math.inf, 1.0, 2.0]},
            {"Time": [0.0, 1e10, 1.0, 2.0]},
            {"Time": [0.0, None, 1.0, 2.0]},
            {"Time": [0.0, True, 1.0, 2.0]},
            {"Mystery": [1, 2, 3, 4]},  # an unknown channel
            {"DriverAhead": ["81", 5, None, None]},
            {"X": [1.0, 2.0, 3.0]},  # ragged
        ],
    )
    def test_other_inputs_go_to_the_numpy_builder(self, overrides):
        channels = _channels(**overrides)
        assert helpers._polars_list_telemetry_frame(channels, "VER", 3, None) is helpers._UNHANDLED

    @pytest.mark.usefixtures("numpy_hidden")
    def test_create_telemetry_df_falls_back_when_unhandled(self):
        # ``sys.modules["numpy"] is None`` here; the fallback is the numpy
        # builder, which still works because ``helpers.np`` is already bound.
        channels = _channels(nGear=[7, 7.0, 8, 8])
        frame = helpers._create_telemetry_df(
            {k.lower(): v for k, v in channels.items()} | {"nGear": channels["nGear"]},
            "VER",
            3,
            "polars",
        )
        assert frame is not None
        assert frame.schema["nGear"] == pl.Int64

    def test_ints_in_int64(self):
        assert helpers._ints_in_int64([1, 2, 3], {int})
        assert helpers._ints_in_int64([1.5, 2.5], {float})
        assert helpers._ints_in_int64([1.5, 2**62], {float, int})
        assert not helpers._ints_in_int64([1.5, -(2**63) - 1], {float, int})


# --------------------------------------------------------------------------- #
# Lap clock lists
# --------------------------------------------------------------------------- #


class TestLapClockLists:
    def test_timedelta_list_matches_the_array_helper(self):
        values = [95.123456789, 0, None, float("nan"), True, 1.0000000005, -3.25]
        assert ss._timedelta_ns_list(values) == ss._timedelta_ns(values).tolist()

    @pytest.mark.parametrize("values", [[1.0, dt.timedelta(seconds=1)], [1.0, "1"], [math.inf]])
    def test_timedelta_list_defers(self, values):
        assert ss._timedelta_ns_list(values) is None

    def test_datetime_list_matches_the_array_helper(self):
        values = [
            "2025-04-13T15:03:01",
            None,
            "2025-04-13T15:04:36.123",
            "2025-04-13T15:04:36.123456789",
        ]
        assert ss._datetime_ns_list(values) == ss._datetime_ns(values).tolist()

    @pytest.mark.parametrize(
        "values",
        [["2025-04-13 15:03:01"], ["2025-02-30T15:03:01"], [dt.datetime(2025, 1, 1)]],  # noqa: DTZ001
    )
    def test_datetime_list_defers(self, values):
        assert ss._datetime_ns_list(values) is None

    def test_empty_lists_defer(self):
        assert ss._timedelta_ns_list([]) is None
        assert ss._datetime_ns_list(None) is None

    def test_polars_ns_list(self):
        durations = pl.Series([1_000, None, 3_000]).cast(NS)
        assert _polars_ns_list(durations, timedelta=True) == [1_000, _NS_MIN, 3_000]
        dates = pl.Series([dt.datetime(2025, 1, 1), None])  # noqa: DTZ001
        assert _polars_ns_list(dates.cast(pl.Datetime("ns")), timedelta=False) == [
            1_735_689_600_000_000_000,
            _NS_MIN,
        ]
        micros = pl.Series([1.5, None])  # float seconds: the value-by-value helper
        assert _polars_ns_list(micros, timedelta=True) == [1_500_000_000, _NS_MIN]


# --------------------------------------------------------------------------- #
# Selection kernels
# --------------------------------------------------------------------------- #


class TestSeriesMasks:
    def test_filter_rows_with_a_series_mask_equals_the_numpy_mask(self):
        frame = pl.DataFrame({"a": [1, 2, 3, 4], "b": ["w", "x", "y", "z"]})
        mask = pl.Series([True, None, False, True])
        expected = mp._filter_rows(frame, np.array([True, False, False, True]))
        assert_frame_equal(mp._filter_rows(frame, mask), expected)

    def test_all_true_with_a_null_is_not_keep_all(self):
        frame = pl.DataFrame({"a": [1, 2]})
        assert mp._filter_rows(frame, pl.Series([True, None])).height == 1
        assert mp._filter_rows(frame, pl.Series([True, True])).height == 2

    def test_chunked_mask_is_rechunked(self):
        frame = pl.concat([pl.DataFrame({"a": [1, 2]}), pl.DataFrame({"a": [3]})], rechunk=False)
        mask = pl.Series([True]).append(pl.Series([False, True]))
        assert mask.n_chunks() == 2
        assert mp._filter_rows(frame, mask)["a"].to_list() == [1, 3]

    def test_pick_driver_ignores_null_codes(self):
        frame = pl.DataFrame({"Driver": ["VER", None, "VER", "HAM"], "LapNumber": [1, 2, 3, 1]})
        laps = mp.PolarsLaps._from_frame(frame)
        assert laps.pick_driver("VER")["LapNumber"].to_list() == [1, 3]


def _lap_frames(rows_per_lap: int = 4, duplicate: bool = True) -> pl.DataFrame:
    """Three laps of one driver, each repeating its last sample as the next lap's first."""
    frames = []
    for lap in (1, 2, 3):
        base = (lap - 1) * (rows_per_lap - (1 if duplicate else 0))
        session_ns = [(3600 + base + i) * 1_000_000_000 for i in range(rows_per_lap)]
        frames.append(
            pl.DataFrame(
                {
                    "SessionTime": pl.Series(session_ns).cast(NS),
                    "Time": pl.Series([i * 1_000_000_000 for i in range(rows_per_lap)]).cast(NS),
                    "Speed": [float(lap * 10 + i) for i in range(rows_per_lap)],
                    "Driver": ["VER"] * rows_per_lap,
                    "LapNumber": [lap] * rows_per_lap,
                }
            )
        )
    return pl.concat(frames, rechunk=False)


class TestJoinOneDriverLaps:
    @pytest.mark.parametrize("duplicate", [True, False])
    @pytest.mark.parametrize("lap_start_ns", [None, 3_599_000_000_000])
    def test_equals_the_numpy_path(self, monkeypatch, duplicate, lap_start_ns):
        frame = _lap_frames(duplicate=duplicate)
        fast = mp._join_lap_frames(frame, lap_start_ns)
        monkeypatch.setattr(mp, "_join_one_driver_laps", lambda *_args: None)
        reference = mp._join_lap_frames(frame, lap_start_ns)
        assert_frame_equal(fast, reference, check_exact=True)
        assert fast.height == (10 if duplicate else 12)

    def test_unsorted_or_null_clock_takes_the_numpy_path(self):
        frame = _lap_frames()
        reversed_clock = frame.with_columns(pl.col("SessionTime").reverse())
        assert mp._join_one_driver_laps(reversed_clock, "SessionTime", None) is None
        with_null = frame.with_columns(
            pl.when(pl.int_range(pl.len()) == 2)
            .then(None)
            .otherwise(pl.col("LapNumber"))
            .alias("LapNumber")
        )
        assert mp._join_one_driver_laps(with_null, "SessionTime", None) is None

    def test_single_lap_is_unchanged(self):
        frame = _lap_frames().filter(pl.col("LapNumber") == 1)
        assert mp._join_one_driver_laps(frame, "SessionTime", None) is frame


# --------------------------------------------------------------------------- #
# Fastest laps
# --------------------------------------------------------------------------- #


def _reference_fastest(laps: pl.DataFrame, *, by_driver: bool, drivers):
    """The numpy implementation ``_fastest_laps_polars`` had before E6."""
    raw = laps["LapTime"].to_numpy().astype("timedelta64[ns]").view("int64")
    keep = raw != _NS_MIN
    keep &= ~(laps["Deleted"].to_numpy() == True)  # noqa: E712
    codes = laps["Driver"].to_numpy()
    if drivers:
        keep &= np.isin(codes, list(drivers))
    rows = np.flatnonzero(keep)
    seconds = raw.astype("float64") / 1_000_000_000
    if not len(rows):
        picked: list[int] = []
    elif not by_driver:
        picked = [int(rows[np.argmin(seconds[rows])])]
    else:
        first_seen: dict = {}
        for position, code in enumerate(codes[rows].tolist()):
            first_seen.setdefault(code, position)
        best: dict = {}
        for row in rows[np.argsort(seconds[rows], kind="stable")].tolist():
            best.setdefault(codes[row], row)
        picked = sorted(best.values(), key=lambda row: (seconds[row], first_seen[codes[row]]))
    result = laps[np.asarray(picked, dtype="int64")]
    return result.with_columns(pl.Series("LapTimeSeconds", seconds[picked], dtype=pl.Float64))


class TestFastestLapsLists:
    @pytest.fixture
    def laps(self):
        ms = 1_000_000
        return pl.DataFrame(
            {
                "Driver": ["HAM", "VER", "HAM", "VER", "LEC", "LEC", "NOR"],
                "LapNumber": [1, 1, 2, 2, 1, 2, 1],
                "LapTime": pl.Series(
                    [92_000 * ms, 92_000 * ms, 91_500 * ms, None, 91_500 * ms, 90_000 * ms, None]
                ).cast(NS),
                "Deleted": [False, None, False, False, False, True, False],
            }
        )

    @pytest.mark.parametrize("by_driver", [True, False])
    @pytest.mark.parametrize("drivers", [None, ["VER", "LEC"], ["XXX"]])
    def test_equals_the_numpy_selection(self, laps, by_driver, drivers):
        got = _fastest_laps_polars(laps, by_driver=by_driver, drivers=drivers)
        expected = _reference_fastest(laps, by_driver=by_driver, drivers=drivers)
        assert_frame_equal(got, expected, check_exact=True)

    def test_non_duration_or_null_driver_defers(self, laps):
        assert (
            _fastest_laps_polars(
                laps.with_columns(pl.col("LapTime").cast(pl.Int64)), by_driver=True, drivers=None
            )
            is None
        )
        nulled = laps.with_columns(
            pl.Series("Driver", ["HAM", None, "HAM", "VER", "LEC", "LEC", "NOR"])
        )
        assert _fastest_laps_polars(nulled, by_driver=True, drivers=None) is None


# --------------------------------------------------------------------------- #
# Qualifying split on session status
# --------------------------------------------------------------------------- #


class _StatusSession:
    name = "Qualifying"

    def __init__(self, times_s, states):
        self.session_status = pl.DataFrame(
            {
                "Time": pl.Series([round(t * 1e9) for t in times_s]).cast(NS),
                "Status": states,
            }
        )


class TestQualifyingSplitLists:
    def test_status_split_with_a_red_flag_nulls_and_an_early_out_lap(self):
        sec = 1_000_000_000
        starts = [100, 200, None, 1150, 1300, 2100, 2200, 2400]
        ends = [190, 290, 400, 1240, 1390, 2190, 2290, 2490]
        ends[1] = 1250  # pit-out lap started in Q1 and crossed the Q2 start
        pit_out = [None, 150, None, None, None, None, None, None]
        frame = pl.DataFrame(
            {
                "Driver": ["VER"] * 8,
                "LapNumber": list(range(1, 9)),
                "LapStartTime": pl.Series([None if s is None else s * sec for s in starts]).cast(
                    NS
                ),
                "Time": pl.Series([e * sec for e in ends]).cast(NS),
                "PitOutTime": pl.Series([None if p is None else p * sec for p in pit_out]).cast(NS),
            }
        )
        session = _StatusSession(
            [0, 1000, 1200, 1250, 2000, 3000],
            ["Started", "Finished", "Started", "Aborted", "Started", "Finished"],
        )
        # Bounds: Started at 0, 1000 is Finished (no split), Started at 1200,
        # Aborted at 1250 and resumed at 2000 (no split), then the end at 3000.
        q1, q2, q3 = mp.PolarsLaps._from_frame(frame, session).split_qualifying_sessions()
        # Lap 2 is an out-lap across the Q2 start (Q2), lap 3 has no start
        # (no segment), lap 4 ends after the split but is no out-lap (Q1).
        assert q1["LapNumber"].to_list() == [1, 4]
        assert q2["LapNumber"].to_list() == [2, 5, 6, 7, 8]
        assert q3 is None
