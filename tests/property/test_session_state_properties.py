"""Property-based tests for the derived session state and the per-lap weather rule.

The track-status and session-status machines run on arbitrary race-control
sequences; ``_weather_rows_for_laps`` is checked against a brute-force
statement of fastf1's rule; ``resample_channels`` never invents values of a
discrete channel.
"""

from __future__ import annotations

from itertools import pairwise
from typing import Any

import numpy as np
import pandas as pd
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tif1.core_utils import session_state as ss
from tif1.models import Telemetry, _weather_rows_for_laps

T0 = pd.Timestamp("2024-03-02 14:55:00")
T0_NS = int(T0.value)
SEC = 1_000_000_000
TRACK_CODES = frozenset(ss.TRACK_STATUS_MESSAGES)

#: Race-control rows the CDN publishes: (Category, Message, Status, Flag, Scope, Sector).
_MESSAGE_KINDS: list[tuple[str, str, Any, Any, Any, Any]] = [
    ("Flag", "GREEN LIGHT - PIT EXIT OPEN", None, "GREEN", "Track", None),
    ("Flag", "TRACK CLEAR", None, "CLEAR", "Track", None),
    ("Flag", "YELLOW FLAG", None, "YELLOW", "Track", None),
    ("Flag", "DOUBLE YELLOW FLAG", None, "DOUBLE YELLOW", "Track", None),
    ("Flag", "RED FLAG", None, "RED", "Track", None),
    ("Flag", "CHEQUERED FLAG", None, "CHEQUERED", "Track", None),
    ("Flag", "WAVED BLUE FLAG FOR CAR 1", None, "BLUE", "Driver", None),
    ("Flag", "BLACK AND WHITE FLAG FOR CAR 44", None, "BLACK AND WHITE", "Driver", None),
    ("SafetyCar", "SAFETY CAR DEPLOYED", "DEPLOYED", None, None, None),
    ("SafetyCar", "SAFETY CAR IN THIS LAP", "IN THIS LAP", None, None, None),
    ("SafetyCar", "VIRTUAL SAFETY CAR DEPLOYED", "DEPLOYED", None, None, None),
    ("SafetyCar", "VIRTUAL SAFETY CAR ENDING", "ENDING", None, None, None),
    ("Other", "DRS ENABLED", None, None, None, None),
]

_sector_flag = st.builds(
    lambda flag_name, sector: (
        "Flag",
        f"{flag_name} IN TRACK SECTOR {sector}",
        None,
        flag_name,
        "Sector",
        float(sector),
    ),
    st.sampled_from(["YELLOW", "DOUBLE YELLOW", "CLEAR"]),
    st.integers(min_value=1, max_value=20),
)
_message = st.one_of(st.sampled_from(_MESSAGE_KINDS), _sector_flag)
#: Messages at arbitrary (unsorted, possibly pre-t0) session times, in seconds.
_messages = st.lists(
    st.tuples(st.integers(min_value=-120, max_value=7200), _message), min_size=0, max_size=40
)


def _rcm(messages: list[tuple[int, tuple[Any, ...]]]) -> pd.DataFrame:
    """A ``race_control_messages``-shaped frame of ``(seconds, kind)`` rows."""
    return pd.DataFrame(
        {
            "Time": pd.Series(
                [T0 + pd.Timedelta(seconds=seconds) for seconds, _ in messages],
                dtype="datetime64[ns]",
            ),
            "Category": pd.Series([kind[0] for _, kind in messages], dtype=object),
            "Message": pd.Series([kind[1] for _, kind in messages], dtype=object),
            "Status": pd.Series([kind[2] for _, kind in messages], dtype=object),
            "Flag": pd.Series([kind[3] for _, kind in messages], dtype=object),
            "Scope": pd.Series([kind[4] for _, kind in messages], dtype=object),
            "Sector": pd.Series([kind[5] for _, kind in messages], dtype="float64"),
        }
    )


# -- track status ---------------------------------------------------------------------------


@given(messages=_messages)
@settings(max_examples=300, deadline=None)
def test_track_status_times_are_monotonic_and_codes_in_vocabulary(messages):
    rows = ss.track_status_rows(_rcm(messages), T0_NS)
    times = [time_ns for time_ns, _ in rows]
    assert rows[0] == (0, "1")
    assert times == sorted(times)
    assert all(time_ns >= 0 for time_ns in times)
    assert {code for _, code in rows} <= TRACK_CODES


@given(messages=_messages)
@settings(max_examples=300, deadline=None)
def test_track_status_never_repeats_a_status(messages):
    codes = [code for _, code in ss.track_status_rows(_rcm(messages), T0_NS)]
    assert all(left != right for left, right in pairwise(codes))


@given(messages=_messages)
@settings(max_examples=100, deadline=None)
def test_track_status_frame_messages_match_codes(messages):
    rows = ss.track_status_rows(_rcm(messages), T0_NS)
    frame = ss.status_frame(rows, "pandas", with_message=True)
    assert frame["Time"].is_monotonic_increasing
    assert frame["Message"].tolist() == [ss.TRACK_STATUS_MESSAGES[c] for c in frame["Status"]]


@given(messages=_messages)
@settings(max_examples=100, deadline=None)
def test_track_status_is_independent_of_input_row_order(messages):
    shuffled = list(reversed(messages))
    # Equal timestamps keep their input order (stable sort), so compare only
    # sequences whose timestamps are distinct.
    if len({seconds for seconds, _ in messages}) != len(messages):
        return
    assert ss.track_status_rows(_rcm(messages), T0_NS) == ss.track_status_rows(
        _rcm(shuffled), T0_NS
    )


_lap_codes = st.text(alphabet="1234567", min_size=0, max_size=4)


@given(
    laps=st.lists(
        st.tuples(st.integers(min_value=0, max_value=10_000), _lap_codes, st.integers(1, 80)),
        max_size=60,
    )
)
@settings(max_examples=200, deadline=None)
def test_laps_fallback_is_monotonic_without_repeats(laps):
    frame = pd.DataFrame(
        {
            "LapStartTime": pd.to_timedelta([start for start, _, _ in laps], unit="s").astype(
                "timedelta64[ns]"
            ),
            "TrackStatus": [codes for _, codes, _ in laps],
            "LapNumber": [float(number) for _, _, number in laps],
        }
    )
    rows = ss.track_status_rows_from_laps(frame)
    times = [time_ns for time_ns, _ in rows]
    codes = [code for _, code in rows]
    assert rows[0] == (0, "1")
    assert times == sorted(times)
    assert set(codes) <= TRACK_CODES
    assert all(left != right for left, right in pairwise(codes))


# -- session status -------------------------------------------------------------------------


@given(messages=_messages, start=st.one_of(st.none(), st.integers(min_value=0, max_value=600)))
@settings(max_examples=200, deadline=None)
def test_session_status_is_time_ordered_in_its_vocabulary(messages, start):
    start_ns = None if start is None else start * SEC
    rows = ss.session_status_rows(_rcm(messages), T0_NS, start_ns)
    times = [time_ns for time_ns, _ in rows]
    assert times == sorted(times)
    assert {status for _, status in rows} <= {"Started", "Aborted", "Finished"}
    if start_ns is not None:
        assert (start_ns, "Started") in rows


# -- per-lap weather rule -----------------------------------------------------------------------


def _expected_weather_time(times: list[int], start: int, end: int) -> int | None:
    """fastf1's rule stated directly: first sample in [start, end], else last before end."""
    within = [t for t in times if start <= t <= end]
    if within:
        return min(within)
    before = [t for t in times if t <= end]
    return max(before) if before else None


_ns = st.integers(min_value=0, max_value=10_000)


@given(
    weather=st.lists(_ns, max_size=30),
    laps=st.lists(st.tuples(_ns, st.integers(min_value=0, max_value=500)), min_size=1, max_size=30),
)
@settings(max_examples=400, deadline=None)
def test_weather_rows_follow_fastf1s_rule(weather, laps):
    weather_times = np.asarray(weather, dtype="int64")
    starts = np.asarray([start for start, _ in laps], dtype="int64")
    ends = np.asarray([start + length for start, length in laps], dtype="int64")
    rows = _weather_rows_for_laps(weather_times, starts, ends)
    assert rows.shape == (len(laps),)
    for position, (start, end) in enumerate(zip(starts, ends, strict=True)):
        expected = _expected_weather_time(weather, int(start), int(end))
        if expected is None:
            assert rows[position] == -1
        else:
            assert 0 <= rows[position] < len(weather)
            assert weather_times[rows[position]] == expected


@given(weather=st.lists(_ns, min_size=1, max_size=30), end=_ns)
@settings(max_examples=200, deadline=None)
def test_weather_rows_without_a_lap_start_take_the_last_sample_before_the_end(weather, end):
    # fastf1: ``Time >= NaT`` is False for every sample, so only the
    # "last sample before the lap ended" fallback can match.
    nat = np.iinfo("int64").min
    rows = _weather_rows_for_laps(
        np.asarray(weather, dtype="int64"),
        np.asarray([nat], dtype="int64"),
        np.asarray([end], dtype="int64"),
    )
    candidates = [t for t in weather if t <= end]
    if candidates:
        assert weather[rows[0]] == max(candidates)
    else:
        assert rows[0] == -1


# -- resampling -------------------------------------------------------------------------------


@st.composite
def _telemetry(draw: Any) -> Telemetry:
    n = draw(st.integers(min_value=2, max_value=60))
    steps = draw(st.lists(st.integers(min_value=1, max_value=700), min_size=n - 1, max_size=n - 1))
    times_ms = np.concatenate([[0], np.cumsum(steps)]).astype("int64")
    time = pd.Series(pd.to_timedelta(times_ms, unit="ms")).astype("timedelta64[ns]")
    lap_start = pd.Timedelta(seconds=draw(st.integers(min_value=0, max_value=7200)))
    gears = draw(st.lists(st.integers(min_value=1, max_value=8), min_size=n, max_size=n))
    drs = draw(st.lists(st.sampled_from([0, 1, 8, 10, 12, 14]), min_size=n, max_size=n))
    brake = draw(st.lists(st.booleans(), min_size=n, max_size=n))
    speed = draw(
        st.lists(st.floats(min_value=0, max_value=360, allow_nan=False), min_size=n, max_size=n)
    )
    return Telemetry(
        {
            "Date": T0 + lap_start + time,
            "SessionTime": lap_start + time,
            "Time": time,
            "Speed": speed,
            "nGear": pd.array(gears, dtype="Int64"),
            "DRS": pd.array(drs, dtype="Int64"),
            "Brake": brake,
            "LapNumber": pd.array([1] * n, dtype="Int64"),
        }
    )


@given(tel=_telemetry(), rule=st.sampled_from(["50ms", "100ms", "250ms", "1s"]))
@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_resample_keeps_discrete_channels_in_their_value_set(tel, rule):
    out = tel.resample_channels(rule=rule)
    assert len(out) > 0
    for column in ("nGear", "DRS", "Brake", "LapNumber"):
        assert set(out[column].dropna().tolist()) <= set(tel[column].dropna().tolist()), column


@given(tel=_telemetry(), rule=st.sampled_from(["50ms", "100ms", "250ms", "1s"]))
@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_resample_keeps_continuous_channels_within_their_range(tel, rule):
    out = tel.resample_channels(rule=rule)
    low, high = tel["Speed"].min(), tel["Speed"].max()
    assert out["Speed"].between(low - 1e-9, high + 1e-9).all()


@given(tel=_telemetry(), rule=st.sampled_from(["50ms", "100ms", "250ms", "1s"]))
@settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_resample_clock_columns_follow_the_new_axis(tel, rule):
    out = tel.resample_channels(rule=rule)
    lap_start = tel["SessionTime"].iloc[0] - tel["Time"].iloc[0]
    assert out["Time"].is_monotonic_increasing
    assert (out["SessionTime"] - out["Time"]).eq(lap_start).all()
    assert (out["Date"] - out["SessionTime"]).eq(T0).all()
