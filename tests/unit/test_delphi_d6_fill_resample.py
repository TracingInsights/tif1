"""Delphi D6: ``Telemetry.fill_missing`` / ``resample_channels`` numpy fill paths.

The pandas class fills the held (non-continuous) columns of a resampling and
the gaps of ``fill_missing`` through numpy source-index gathers and
``np.interp`` instead of one pandas ``ffill``/``bfill``/``interpolate``
chain per column. Every test compares the result with the pandas chains the
methods replaced (``_reference_*`` below, the previous implementation),
value for value and dtype for dtype, on gaps at the start, in the middle
and at the end, all-NA columns, masked ``Int64``, object and ``str``
strings, booleans and a non-default index. Nothing reaches the network.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from tif1.models import (
    _RESAMPLE_AXIS_COLUMNS,
    Telemetry,
    _ffill_bfill_source,
    _fill_rule,
    _modern_offset_alias,
)

T0 = pd.Timestamp("2025-04-13 15:03:00")
NAT_NS = np.iinfo("int64").min


# -- the pandas chains the methods replaced (the previous implementation) -----------


def _reference_fill_missing(tel: Telemetry) -> pd.DataFrame:
    filled = pd.DataFrame(tel).copy()
    for i, (col, dtype) in enumerate(zip(tel.columns, tel.dtypes, strict=True)):
        rule = _fill_rule(tel._CHANNELS, col, dtype)
        if rule is None:
            continue
        values = pd.DataFrame(tel)._ixs(i, axis=1)
        if not values.hasnans:
            continue
        if rule == "interpolate":
            values = values.interpolate(limit_direction="both")
        elif rule == "hold":
            values = values.ffill().bfill()
        else:
            values = values.fillna("interpolation")
        filled.isetitem(i, values)
    return filled


def _reference_resample(tel: Telemetry, rule: str) -> pd.DataFrame:
    frame = pd.DataFrame(tel)
    step = int(pd.Timedelta(_modern_offset_alias(rule)).value)
    axis = next(col for col in _RESAMPLE_AXIS_COLUMNS if col in frame.columns)
    axis_ns = tel._clock_ns(axis)
    positions = np.flatnonzero(axis_ns != NAT_NS)
    positions = positions[np.argsort(axis_ns[positions], kind="stable")]
    x = axis_ns[positions]
    bins = (x - x[0]) // step
    n_bins = int(bins[-1]) + 1
    offsets = np.arange(n_bins, dtype="int64") * step
    held = np.searchsorted(x, x[0] + offsets, side="right") - 1
    out: dict[str, Any] = {}
    for col, dtype in zip(frame.columns, frame.dtypes, strict=True):
        if col in _RESAMPLE_AXIS_COLUMNS:
            is_date = pd.api.types.is_datetime64_any_dtype(dtype)
            if col == "Time":
                values = offsets
            else:
                first = int(tel._clock_ns(col)[positions[0]])
                values = np.full(len(offsets), NAT_NS) if first == NAT_NS else first + offsets
            out[col] = values.view("M8[ns]" if is_date else "m8[ns]")
        elif tel._is_continuous_column(col, dtype):
            values = tel._float_values(col)[positions]
            valid = ~np.isnan(values)
            counts = np.bincount(bins[valid], minlength=n_bins)
            sums = np.bincount(bins[valid], weights=values[valid], minlength=n_bins)
            filled = counts > 0
            means = np.full(n_bins, np.nan)
            if filled.any():
                means[filled] = sums[filled] / counts[filled]
                means = np.interp(np.arange(n_bins), np.flatnonzero(filled), means[filled])
            out[col] = means
        else:
            in_force = frame[col].iloc[positions].reset_index(drop=True).ffill()
            out[col] = in_force.iloc[held].reset_index(drop=True).bfill()
    return pd.DataFrame(out, columns=frame.columns)


# -- frames ---------------------------------------------------------------------------


def _gapped(values: list[Any], gaps: str, rng: np.random.Generator) -> list[Any]:
    """``values`` with NA (None) at the start, the end, the middle or everywhere."""
    out = list(values)
    n = len(out)
    if "all" in gaps:
        return [None] * n
    if "start" in gaps:
        out[0] = None
        out[1] = None
    if "end" in gaps:
        out[-1] = None
    if "middle" in gaps:
        for pos in rng.choice(np.arange(2, n - 1), size=max(1, n // 4), replace=False):
            out[int(pos)] = None
    return out


def build_frame(
    n: int = 40,
    *,
    seed: int = 0,
    gaps: str = "start middle end",
    all_na: tuple[str, ...] = (),
    index: pd.Index | None = None,
    unsorted: bool = False,
    nat: bool = False,
) -> Telemetry:
    rng = np.random.default_rng(seed)
    seconds = np.cumsum(rng.uniform(0.05, 0.4, n))
    if unsorted:
        rng.shuffle(seconds)
    date = T0 + pd.to_timedelta(seconds, unit="s")
    data: dict[str, Any] = {
        "Date": pd.Series(date).astype("datetime64[ns]"),
        "SessionTime": pd.to_timedelta(seconds + 3600.0, unit="s").astype("timedelta64[ns]"),
        "Time": pd.to_timedelta(seconds, unit="s").astype("timedelta64[ns]"),
        "DriverAhead": pd.Series(
            _gapped([str(int(v)) for v in rng.integers(1, 99, n)], gaps, rng), dtype=object
        ),
        "DistanceToDriverAhead": pd.Series(
            _gapped(list(rng.uniform(0, 500, n)), gaps, rng), dtype="float64"
        ),
        "RPM": pd.Series(_gapped(list(rng.uniform(5000, 12000, n)), gaps, rng), dtype="float64"),
        "Speed": pd.Series(_gapped(list(rng.uniform(80, 330, n)), gaps, rng), dtype="float64"),
        "nGear": pd.array(_gapped(list(rng.integers(1, 9, n)), gaps, rng), dtype="Int64"),
        "Throttle": pd.Series(rng.uniform(0, 100, n), dtype="float64"),
        "Brake": pd.Series(rng.integers(0, 2, n).astype(bool)),
        "DRS": pd.array(_gapped(list(rng.integers(0, 15, n)), gaps, rng), dtype="Int64"),
        "Source": pd.Series(_gapped(["car"] * n, gaps, rng), dtype=object),
        "Status": pd.Series(_gapped(["OnTrack"] * n, gaps, rng), dtype=object),
        "Driver": pd.Series(["VER"] * n, dtype=object),
        "LapNumber": pd.array([7] * n, dtype="Int64"),
        "X": pd.Series(_gapped(list(rng.uniform(-9000, 9000, n)), gaps, rng), dtype="float64"),
        "Team": pd.Series(_gapped(["Red Bull"] * n, gaps, rng), dtype="str"),
        "Flag": pd.array(
            _gapped(list(rng.integers(0, 2, n).astype(bool)), gaps, rng), dtype="boolean"
        ),
        "Small": pd.Series(_gapped(list(rng.uniform(0, 1, n)), gaps, rng), dtype="float64").astype(
            "float32"
        ),
        "Masked": pd.array(_gapped(list(rng.uniform(0, 1, n)), gaps, rng), dtype="Float64"),
        "Pit": pd.to_timedelta(_gapped(list(rng.uniform(0, 1, n)), gaps, rng), unit="s"),
        "Stint": pd.Series(rng.integers(1, 4, n), dtype="int64"),
    }
    for col in all_na:
        kind = data[col].dtype
        data[col] = pd.Series([None] * n, dtype=kind)
    if nat:
        dates = data["Date"].copy()
        dates.iloc[3] = pd.NaT
        dates.iloc[-2] = pd.NaT
        data["Date"] = dates
    frame = pd.DataFrame(data)
    if index is not None:
        frame.index = index
    tel = Telemetry(frame, session=object(), driver="VER")
    return tel


def assert_frames_identical(result: pd.DataFrame, reference: pd.DataFrame) -> None:
    assert list(result.columns) == list(reference.columns)
    assert result.dtypes.tolist() == reference.dtypes.tolist()
    pd.testing.assert_frame_equal(
        pd.DataFrame(result), pd.DataFrame(reference), check_exact=True, check_dtype=True
    )


# -- the source-index map ---------------------------------------------------------------


class TestFfillBfillSource:
    def test_leading_middle_and_trailing_gaps(self):
        na = np.array([True, True, False, True, False, True, True])
        assert _ffill_bfill_source(na).tolist() == [2, 2, 2, 2, 4, 4, 4]

    def test_all_missing_is_none(self):
        assert _ffill_bfill_source(np.array([True, True])) is None

    def test_gathered_at_held_takes_the_first_value_found(self):
        # ffill -> [NA, a, b, b]; gathered at [0, 2, 3] -> [NA, b, b]; bfill -> [b, b, b]
        na = np.array([True, False, False, True])
        held = np.array([0, 2, 3])
        assert _ffill_bfill_source(na, held).tolist() == [2, 2, 2]

    def test_held_before_any_value_is_none(self):
        na = np.array([True, False])
        assert _ffill_bfill_source(na, np.array([0, 0])) is None

    @pytest.mark.parametrize("seed", range(20))
    def test_matches_pandas_ffill_bfill(self, seed):
        rng = np.random.default_rng(seed)
        n = int(rng.integers(1, 60))
        values = rng.standard_normal(n)
        values[rng.random(n) < rng.choice([0.2, 0.5, 0.9])] = np.nan
        na = np.isnan(values)
        source = _ffill_bfill_source(na)
        expected = pd.Series(values).ffill().bfill().to_numpy()
        if source is None:
            assert np.isnan(expected).all()
        else:
            assert np.array_equal(values[source], expected)
        held = np.sort(rng.integers(0, n, size=int(rng.integers(1, 30))))
        source = _ffill_bfill_source(na, held)
        chain = pd.Series(values).ffill().iloc[held].reset_index(drop=True).bfill().to_numpy()
        if source is None:
            assert np.isnan(chain).all()
        else:
            assert np.array_equal(values[source], chain)


# -- fill_missing ---------------------------------------------------------------------


class TestFillMissing:
    @pytest.mark.parametrize("gaps", ["start", "middle", "end", "start middle end"])
    def test_matches_the_pandas_chains(self, gaps):
        tel = build_frame(gaps=gaps)
        result = tel.fill_missing()
        assert type(result) is Telemetry
        assert result.session is tel.session
        assert result.driver == "VER"
        assert_frames_identical(result, _reference_fill_missing(tel))
        # Gaps are gone from the filled columns, the clocks are untouched.
        assert not result["Speed"].isna().any()
        assert not result["nGear"].isna().any()
        assert not result["DriverAhead"].isna().any()
        assert (result["Source"] == "interpolation").sum() == tel["Source"].isna().sum()
        assert result["Pit"].isna().sum() == tel["Pit"].isna().sum()
        assert result["Date"].equals(tel["Date"])

    def test_all_na_columns_stay_all_na(self):
        tel = build_frame(all_na=("DriverAhead", "Speed", "nGear", "Source", "Team", "Masked"))
        result = tel.fill_missing()
        assert_frames_identical(result, _reference_fill_missing(tel))
        assert result["DriverAhead"].isna().all()
        assert result["Speed"].isna().all()
        assert result["nGear"].isna().all()
        assert (result["Source"] == "interpolation").all()
        assert result["Team"].isna().all()  # an unregistered string column: no rule
        assert result["Masked"].isna().all()

    def test_non_default_index_is_kept(self):
        tel = build_frame(index=pd.RangeIndex(100, 140))
        result = tel.fill_missing()
        assert result.index.equals(tel.index)
        assert_frames_identical(result, _reference_fill_missing(tel))

    def test_non_uniform_index_interpolates_on_positions(self):
        index = pd.Index(np.sort(np.random.default_rng(1).choice(np.arange(500), 40, False)))
        tel = build_frame(index=index)
        result = tel.fill_missing()
        assert result.index.equals(index)
        assert_frames_identical(result, _reference_fill_missing(tel))

    def test_result_does_not_share_memory_with_the_source(self):
        tel = build_frame()
        result = tel.fill_missing()
        result.loc[result.index[0], "Throttle"] = -1.0
        assert tel["Throttle"].iloc[0] != -1.0
        result["Driver"] = "LEC"
        assert (tel["Driver"] == "VER").all()

    @pytest.mark.parametrize("seed", range(8))
    def test_random_gap_patterns(self, seed):
        tel = build_frame(n=int(np.random.default_rng(seed).integers(3, 80)), seed=seed)
        assert_frames_identical(tel.fill_missing(), _reference_fill_missing(tel))


# -- resample_channels ---------------------------------------------------------------


class TestResampleChannels:
    @pytest.mark.parametrize("rule", ["0.1s", "1s", "250ms", "3s"])
    @pytest.mark.parametrize("gaps", ["start", "middle", "end", "start middle end"])
    def test_matches_the_pandas_chains(self, rule, gaps):
        tel = build_frame(gaps=gaps)
        result = tel.resample_channels(rule=rule)
        assert type(result) is Telemetry
        assert result.session is tel.session
        assert result.driver == "VER"
        assert result.columns is tel.columns
        assert isinstance(result.index, pd.RangeIndex)
        assert_frames_identical(result, _reference_resample(tel, rule))

    def test_all_na_held_and_continuous_columns(self):
        tel = build_frame(all_na=("DriverAhead", "Speed", "nGear", "Source", "Team", "Pit"))
        result = tel.resample_channels(rule="0.1s")
        assert_frames_identical(result, _reference_resample(tel, "0.1s"))
        assert result["DriverAhead"].isna().all()
        assert result["Speed"].isna().all()
        assert result["nGear"].dtype == "Int64"
        assert result["nGear"].isna().all()
        assert result["Pit"].isna().all()

    def test_unsorted_dates_and_nat_rows(self):
        tel = build_frame(unsorted=True, nat=True)
        assert_frames_identical(
            tel.resample_channels(rule="0.1s"), _reference_resample(tel, "0.1s")
        )

    def test_non_default_index(self):
        tel = build_frame(index=pd.RangeIndex(100, 140))
        result = tel.resample_channels(rule="0.1s")
        assert isinstance(result.index, pd.RangeIndex)
        assert result.index[0] == 0
        assert_frames_identical(result, _reference_resample(tel, "0.1s"))

    def test_session_time_axis_when_date_is_missing(self):
        tel = build_frame()
        tel = Telemetry(pd.DataFrame(tel).drop(columns=["Date"]), session=tel.session)
        result = tel.resample_channels(rule="0.5s")
        assert_frames_identical(result, _reference_resample(tel, "0.5s"))

    def test_other_clock_resolutions_go_through_the_general_path(self):
        tel = build_frame()
        frame = pd.DataFrame(tel)
        frame["Date"] = frame["Date"].astype("datetime64[us]")
        frame["SessionTime"] = frame["SessionTime"].astype("timedelta64[ms]")
        tel = Telemetry(frame, session=tel.session)
        result = tel.resample_channels(rule="0.1s")
        assert_frames_identical(result, _reference_resample(tel, "0.1s"))

    @pytest.mark.parametrize("seed", range(8))
    def test_random_gap_patterns(self, seed):
        rng = np.random.default_rng(seed)
        tel = build_frame(n=int(rng.integers(3, 80)), seed=seed, unsorted=bool(seed % 2))
        rule = str(rng.choice(["0.1s", "0.2s", "1s"]))
        assert_frames_identical(tel.resample_channels(rule=rule), _reference_resample(tel, rule))

    def test_plain_ratio_shortcut_matches_the_masked_arithmetic(self):
        # With no NaN sample and no empty bin the means are plain ratios; they
        # must be bit-identical to the masked bincount / np.interp path.
        rng = np.random.default_rng(7)
        checked = 0
        while checked < 30:
            n = int(rng.integers(2, 100))
            step = int(rng.choice([50, 100, 250]))
            x = np.sort(rng.integers(0, 2000, n)) * 1_000_000
            values = rng.standard_normal(n) * rng.choice([1.0, 1e-4, 1e5])
            bins = (x - x[0]) // (step * 1_000_000)
            n_bins = int(bins[-1]) + 1
            counts = np.bincount(bins, minlength=n_bins)
            if not counts.all():
                continue
            checked += 1
            fast = np.bincount(bins, weights=values, minlength=n_bins) / counts
            filled = counts > 0
            means = np.full(n_bins, np.nan)
            sums = np.bincount(bins, weights=values, minlength=n_bins)
            means[filled] = sums[filled] / counts[filled]
            means = np.interp(np.arange(n_bins), np.flatnonzero(filled), means[filled])
            assert fast.tobytes() == means.tobytes()

    def test_empty_bins_are_interpolated_across(self):
        # Samples 0.0 s, 0.1 s, 0.5 s with rule 0.1 s: bins 2-4 are empty and
        # take the linear interpolation, not a division by zero.
        frame = pd.DataFrame(
            {
                "Date": pd.to_datetime([T0, T0 + pd.Timedelta("0.1s"), T0 + pd.Timedelta("0.5s")]),
                "Speed": [100.0, 110.0, 150.0],
                "nGear": pd.array([3, 4, 5], dtype="Int64"),
            }
        )
        result = Telemetry(frame).resample_channels(rule="0.1s")
        assert result["Speed"].tolist() == [100.0, 110.0, 120.0, 130.0, 140.0, 150.0]
        assert result["nGear"].tolist() == [3, 4, 4, 4, 4, 5]
        assert_frames_identical(result, _reference_resample(Telemetry(frame), "0.1s"))
