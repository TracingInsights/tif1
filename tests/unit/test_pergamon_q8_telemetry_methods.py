"""Contract tests for Pergamon Q8: Telemetry analysis methods without avoidable pandas work.

- ``Telemetry`` results of pandas ops adopt the fresh manager (``_from_mgr``)
  instead of ``Telemetry(df)`` (a shallow manager copy); column access returns
  the plain Series without re-wrapping; ``_wrap`` re-tags fresh frames.
- ``integrate_distance``/``calculate_differential_distance`` run the same
  arithmetic in numpy for float64 ``Speed`` + numpy timedelta ``Time``.
- ``fill_missing``/``merge_channels``/``resample_channels``/
  ``add_differential_distance``/``add_track_status`` skip deep copies,
  re-normalizing already-timedelta columns and interpolating NaN-free floats.

Every method must match the pre-Q8 implementation (kept below as the
reference): values, dtypes, columns, index, block count and metadata.
"""

from __future__ import annotations

import pickle

import numpy as np
import pandas as pd
import pytest

from tif1.models import Telemetry, _modern_offset_alias

N = 240
_SESSION = object()


def _data(seed: int = 3, **over) -> dict:
    rng = np.random.default_rng(seed)
    data = {
        "Time": pd.to_timedelta(np.cumsum(rng.uniform(0.05, 0.3, N)), unit="s"),
        "RPM": rng.uniform(5000, 12000, N),
        "Speed": rng.uniform(50, 320, N),
        "nGear": pd.array(rng.integers(1, 9, N), dtype="Int64"),
        "Throttle": rng.uniform(0, 100, N),
        "Brake": rng.random(N) > 0.8,
        "X": np.where(rng.random(N) > 0.7, rng.normal(size=N), np.nan),
        "DriverAhead": pd.array([None if i % 5 == 0 else "16" for i in range(N)], dtype="str"),
        "DistanceToDriverAhead": rng.uniform(0, 100, N),
        "Driver": np.array(["VER"] * N, dtype=object),
        "LapNumber": pd.array([3] * N, dtype="Int64"),
    }
    data.update(over)
    return data


def _tel(frame: pd.DataFrame) -> Telemetry:
    return Telemetry(frame, session=_SESSION, driver="VER")  # ty: ignore[invalid-argument-type]


def _frames() -> dict[str, Telemetry]:
    frames = {"plain": _tel(pd.DataFrame(_data()))}
    speed = _data()["Speed"].copy()
    speed[[3, 50, 120]] = np.nan
    frames["speed_nan"] = _tel(pd.DataFrame(_data(Speed=speed)))
    time = _data()["Time"].to_numpy().copy()
    time[[0, 10, 11]] = np.timedelta64("NaT", "ns")
    frames["time_nat"] = _tel(pd.DataFrame(_data(Time=time)))
    frames["time_float"] = _tel(pd.DataFrame(_data(Time=np.arange(N) * 0.25)))
    frames["time_ms"] = _tel(pd.DataFrame(_data(Time=_data()["Time"].astype("m8[ms]"))))
    frames["speed_int"] = _tel(pd.DataFrame(_data(Speed=np.arange(N) % 300)))
    frames["speed_inf"] = _tel(pd.DataFrame(_data(Speed=np.where(np.arange(N) == 7, np.inf, 1.0))))
    back = pd.to_timedelta(np.r_[np.arange(N // 2), np.arange(N // 2)] * 0.1, unit="s")
    frames["time_backwards"] = _tel(pd.DataFrame(_data(Time=back)))
    unconsolidated = _tel(pd.DataFrame(_data()))
    unconsolidated["Extra"] = 1.5
    frames["unconsolidated"] = unconsolidated
    with_session_time = _data()
    with_session_time["SessionTime"] = with_session_time["Time"] + pd.Timedelta(hours=1)
    frames["session_time"] = _tel(pd.DataFrame(with_session_time))
    attrs = _tel(pd.DataFrame(_data()))
    attrs.attrs["source"] = "x"
    attrs.flags.allows_duplicate_labels = False
    frames["attrs_flags"] = attrs
    dup = pd.DataFrame(_data())
    frames["duplicate_columns"] = _tel(pd.concat([dup, dup[["X"]]], axis=1))
    return frames


# -- the pre-Q8 implementation, kept as the parity reference -----------------


def _ref_wrap(self: Telemetry, frame: pd.DataFrame) -> Telemetry:
    wrapped = Telemetry(frame)
    wrapped.session = self.session
    wrapped.driver = self.driver
    return wrapped


def _ref_fill_missing(self: Telemetry) -> Telemetry:
    """fastf1's ``fill_missing`` rule spelled out (the contract since the parity fixes).

    Discrete channels, ``Driver``/``LapNumber`` and integer/boolean columns
    are held (forward then backward fill, dtype kept), other floats
    interpolated linearly, ``Source`` gaps marked ``"interpolation"``,
    excluded channels (the clocks) and anything else left alone.
    """
    filled = self.copy()
    for i, col in enumerate(self.columns):
        values = self.iloc[:, i]
        if not values.isna().any():
            continue
        kind = Telemetry._CHANNELS.get(col, {}).get("type")
        dtype = values.dtype
        if col == "Source":
            new = values.fillna("interpolation")
        elif kind == "excluded":
            continue
        elif (
            kind == "discrete"
            or col in ("Driver", "LapNumber")
            or pd.api.types.is_bool_dtype(dtype)
            or pd.api.types.is_integer_dtype(dtype)
        ):
            new = values.ffill().bfill()
        elif pd.api.types.is_float_dtype(dtype):
            new = values.interpolate(limit_direction="both")
        else:
            continue
        filled.isetitem(i, new)
    return _ref_wrap(self, filled)


def _ref_integrate_distance(self: Telemetry) -> pd.Series:
    if self.empty or "Speed" not in self.columns:
        return pd.Series(dtype=float)
    speed_ms = pd.to_numeric(self["Speed"], errors="coerce").fillna(0.0) / 3.6
    t = pd.to_timedelta(self["Time"], errors="coerce").dt.total_seconds().fillna(0.0)
    dt = t.diff().fillna(0.0).clip(lower=0.0)
    return (speed_ms * dt).cumsum()


def _ref_differential_distance(self: Telemetry) -> pd.Series:
    speed_ms = pd.to_numeric(self["Speed"], errors="coerce").fillna(0.0) / 3.6
    t = pd.to_timedelta(self["Time"], errors="coerce").dt.total_seconds().fillna(0.0)
    dt = t.diff().fillna(0.0).clip(lower=0.0)
    return speed_ms * dt


def _ref_add_differential_distance(self: Telemetry) -> Telemetry:
    tel = self.copy()
    tel["DifferentialDistance"] = _ref_differential_distance(self)
    return _ref_wrap(self, tel)


def _ref_add_track_status(self: Telemetry) -> Telemetry:
    tel = self.copy()
    if "TrackStatus" not in tel.columns:
        tel["TrackStatus"] = "1"
    return _ref_wrap(self, tel)


def _clock_keys(series: pd.Series) -> list[int | None]:
    """A clock column as integer nanoseconds (None when missing; numbers are seconds)."""
    if pd.api.types.is_datetime64_any_dtype(series.dtype):
        values = series.astype("datetime64[ns]")
    else:
        values = Telemetry._coerce_timedelta_series(series).astype("timedelta64[ns]")
    return [None if pd.isna(v) else int(v.value) for v in values]


def _ref_merge_channels(self: Telemetry, other) -> Telemetry:
    """The nearest-sample merge spelled out (the contract since the parity fixes).

    Keyed on the first of ``Date``/``SessionTime``/``Time`` both frames
    carry; this frame sorted on it (stable, missing last); each row takes
    the ``other`` row nearest in time, the earlier one on a tie and the last
    of equal timestamps on an exact match, nothing for a missing key.
    """
    right = pd.DataFrame(other)
    key = next(c for c in ("Date", "SessionTime", "Time") if c in self.columns and c in right)
    left_keys = _clock_keys(self[key])
    order = sorted(range(len(self)), key=lambda i: (left_keys[i] is None, left_keys[i] or 0))
    candidates = sorted((k, j) for j, k in enumerate(_clock_keys(right[key])) if k is not None)
    matches = []
    for i in order:
        x = left_keys[i]
        best: tuple[int, int, int] | None = None
        if x is not None:
            for k, j in candidates:
                gap = abs(k - x)
                if best is None or gap < best[0] or (gap == best[0] and k == best[1]):
                    best = (gap, k, j)
        matches.append(-1 if best is None else best[2])
    columns = [col for col in right.columns if col != key]
    part = right[columns].reset_index(drop=True)
    part.columns = [f"{col}_other" if col in self.columns else col for col in columns]
    taken = part.reindex(matches).reset_index(drop=True)
    left = self.iloc[order].reset_index(drop=True)
    return _ref_wrap(self, pd.concat([left, taken], axis=1))


def _ref_resample_channels(self: Telemetry, rule: str) -> Telemetry:
    """The fastf1-parity contract spelled out plainly, one bin at a time.

    Bins every ``rule`` from the first sample on the session clock (``Date``,
    else ``SessionTime``, else ``Time``); continuous channels are the bin mean
    interpolated across empty bins, everything else the value in force at
    each bin start; ``Date``/``SessionTime`` advance with the bins and
    ``Time`` restarts at zero.
    """
    rule = _modern_offset_alias(rule)
    if not self.columns.is_unique:
        frame = self.copy()
        frame["Time"] = pd.to_timedelta(frame["Time"], errors="coerce")
        frame = frame.dropna(subset=["Time"]).set_index("Time").sort_index()
        numeric_cols = [c for c in frame.columns if pd.api.types.is_numeric_dtype(frame[c])]
        resampled = frame[numeric_cols].resample(rule).mean().interpolate(limit_direction="both")
        return _ref_wrap(self, resampled.reset_index())
    axis = next(c for c in ("Date", "SessionTime", "Time") if c in self.columns)
    # Numeric clocks are seconds (tif1's convention, as everywhere in Telemetry).
    if axis == "Date":
        clock = pd.to_datetime(self[axis], errors="coerce").astype("datetime64[ns]")
    else:
        clock = Telemetry._coerce_timedelta_series(self[axis]).astype("timedelta64[ns]")
    kept = np.flatnonzero(clock.notna().to_numpy())
    positions = kept[np.argsort(clock.iloc[kept].to_numpy(), kind="stable")]
    t = clock.iloc[positions].reset_index(drop=True)
    step = pd.Timedelta(rule)
    n_bins = int((t.iloc[-1] - t.iloc[0]) // step) + 1
    labels = [t.iloc[0] + k * step for k in range(n_bins)]
    offsets = np.arange(n_bins, dtype="int64") * step.value
    out = {}
    for col in self.columns:
        series = self[col].iloc[positions].reset_index(drop=True)
        dtype = self[col].dtype
        if col in ("Date", "SessionTime", "Time"):
            if col == "Time":
                values = offsets
            else:
                if col == "Date":
                    first = pd.to_datetime(series).iloc[0]
                else:
                    first = Telemetry._coerce_timedelta_series(series).iloc[0]
                values = pd.Timedelta(first).value if col != "Date" else first.value
                values = values + offsets
            out[col] = values.view("M8[ns]" if col == "Date" else "m8[ns]")
        elif (
            pd.api.types.is_numeric_dtype(dtype)
            and not pd.api.types.is_bool_dtype(dtype)
            and Telemetry._CHANNELS.get(col, {}).get("type") != "discrete"
            and col not in ("Driver", "LapNumber")
        ):
            values = pd.to_numeric(series, errors="coerce")
            means = []
            for label in labels:
                # Summed left to right in time order, then divided by the count
                # (a loop: Python's sum() of floats is compensated since 3.12).
                in_bin = values[(t >= label) & (t < label + step)].dropna().tolist()
                total = 0.0
                for value in in_bin:
                    total += value
                means.append(total / len(in_bin) if in_bin else np.nan)
            out[col] = pd.Series(means, dtype=float).interpolate(limit_direction="both").to_numpy()
        else:
            in_force = series.ffill()
            held = [int((t <= label).sum()) - 1 for label in labels]
            out[col] = in_force.iloc[held].reset_index(drop=True).bfill()
    return _ref_wrap(self, pd.DataFrame(out, columns=self.columns))


def _outcome(fn):
    try:
        return fn(), None
    except Exception as exc:
        return None, (type(exc), str(exc))


def _assert_same(actual, expected) -> None:
    assert type(actual) is type(expected)
    if isinstance(expected, pd.Series):
        assert actual.name == expected.name
        assert actual.dtype == expected.dtype
        assert actual.index.equals(expected.index)
        assert actual.equals(expected)
        return
    assert list(actual.columns) == list(expected.columns)
    assert list(actual.dtypes) == list(expected.dtypes)
    assert type(actual.index) is type(expected.index)
    assert actual.index.equals(expected.index)
    assert actual._mgr.nblocks == expected._mgr.nblocks
    assert pd.DataFrame(actual).equals(pd.DataFrame(expected))
    assert actual.session is expected.session
    assert actual.driver == expected.driver
    assert actual.attrs == expected.attrs
    assert actual.flags.allows_duplicate_labels == expected.flags.allows_duplicate_labels


CASES = {
    "fill_missing": (lambda t: t.fill_missing(), _ref_fill_missing),
    "integrate_distance": (lambda t: t.integrate_distance(), _ref_integrate_distance),
    "calculate_differential_distance": (
        lambda t: t.calculate_differential_distance(),
        _ref_differential_distance,
    ),
    "add_differential_distance": (
        lambda t: t.add_differential_distance(),
        _ref_add_differential_distance,
    ),
    "add_track_status": (lambda t: t.add_track_status(), _ref_add_track_status),
    "merge_channels": (
        lambda t: t.merge_channels(_tel(pd.DataFrame(_data(seed=9)))),
        lambda t: _ref_merge_channels(t, _tel(pd.DataFrame(_data(seed=9)))),
    ),
    "merge_channels_plain_other": (
        lambda t: t.merge_channels(pd.DataFrame(_data(seed=9))[["Time", "X"]]),
        lambda t: _ref_merge_channels(t, pd.DataFrame(_data(seed=9))[["Time", "X"]]),
    ),
    "resample_channels": (
        lambda t: t.resample_channels("0.5s"),
        lambda t: _ref_resample_channels(t, "0.5s"),
    ),
    "resample_channels_legacy_alias": (
        lambda t: t.resample_channels("1S"),
        lambda t: _ref_resample_channels(t, "1S"),
    ),
}


@pytest.mark.parametrize("frame_name", list(_frames()))
@pytest.mark.parametrize("case", list(CASES))
def test_q8_methods_match_the_reference_implementation(frame_name, case):
    frame = _frames()[frame_name]
    method, reference = CASES[case]
    actual, actual_error = _outcome(lambda: method(frame))
    expected, expected_error = _outcome(lambda: reference(frame))
    assert actual_error == expected_error
    if expected_error is None:
        _assert_same(actual, expected)


@pytest.mark.parametrize("case", ["fill_missing", "add_track_status", "add_differential_distance"])
def test_q8_results_are_mutation_isolated_from_the_source(case):
    frame = _frames()["plain"]
    before = pd.DataFrame(frame).copy()
    result = CASES[case][0](frame)
    result.loc[result.index[0], "RPM"] = -1.0
    result["Speed"] = result["Speed"] * 2
    assert pd.DataFrame(frame).equals(before)


def test_q8_pandas_ops_keep_telemetry_metadata_without_init():
    frame = _frames()["plain"]
    calls = []
    original_init = Telemetry.__init__

    def counting_init(self, *args, **kwargs):
        calls.append(1)
        original_init(self, *args, **kwargs)

    Telemetry.__init__ = counting_init  # ty: ignore[invalid-assignment]
    try:
        results = [frame.copy(), frame.iloc[5:50], frame[frame["Speed"] > 100], frame.head()]
    finally:
        Telemetry.__init__ = original_init  # ty: ignore[invalid-assignment]
    assert calls == []
    for result in results:
        assert type(result) is Telemetry
        assert result.session is _SESSION
        assert result.driver == "VER"
    column = frame["Speed"]
    assert type(column) is pd.Series
    assert column.name == "Speed"
    restored = pickle.loads(pickle.dumps(frame.copy()))
    assert restored.driver == "VER"


def test_q8_wrap_retags_fresh_frames_and_copies_self():
    frame = _frames()["attrs_flags"]
    assert frame._wrap(frame) is not frame
    fresh = frame.iloc[2:20]
    wrapped = frame._wrap(fresh)
    assert wrapped is fresh
    assert wrapped.attrs == {}
    assert wrapped.flags.allows_duplicate_labels
    assert wrapped.session is _SESSION
    assert wrapped.driver == "VER"
