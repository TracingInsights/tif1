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
    filled = self.copy()
    for col in filled.columns:
        if pd.api.types.is_numeric_dtype(filled[col]):
            filled[col] = filled[col].interpolate(limit_direction="both")
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


def _ref_merge_channels(self: Telemetry, other) -> Telemetry:
    left = self.copy()
    right = pd.DataFrame(other).copy()
    left["Time"] = pd.to_timedelta(left["Time"], errors="coerce")
    right["Time"] = pd.to_timedelta(right["Time"], errors="coerce")
    merged = pd.merge_asof(
        left.sort_values("Time"),
        right.sort_values("Time"),
        on="Time",
        suffixes=("", "_other"),
        direction="nearest",
    )
    return _ref_wrap(self, merged)


def _ref_resample_channels(self: Telemetry, rule: str) -> Telemetry:
    rule = _modern_offset_alias(rule)
    frame = self.copy()
    frame["Time"] = pd.to_timedelta(frame["Time"], errors="coerce")
    frame = frame.dropna(subset=["Time"]).set_index("Time").sort_index()
    numeric_cols = [c for c in frame.columns if pd.api.types.is_numeric_dtype(frame[c])]
    resampled = frame[numeric_cols].resample(rule).mean().interpolate(limit_direction="both")
    return _ref_wrap(self, resampled.reset_index())


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
