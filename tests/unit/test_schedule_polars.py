"""``lib="polars"`` schedules and the Series-free pandas schedule build (Corinth C9).

The polars schedule holds the pandas ``EventSchedule``'s rows and columns
value for value under the documented mapping (local session dates as naive
wall-clock times, missing values as nulls) and is built without pandas,
numpy or pyarrow (``test_polars_without_pandas.py`` checks that in a fresh
interpreter). The pandas schedule is built from plain row dicts instead
of one ``Event`` Series per event; it must equal the per-event build.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime

import pandas as pd
import pytest

from tif1 import events as events_mod
from tif1.event_frames import Event, EventSchedule
from tif1.events import (
    get_event,
    get_event_by_round,
    get_event_schedule,
    get_events_remaining,
)

pl = pytest.importorskip("polars")


def _naive(*args: int) -> datetime:
    """A naive datetime (the schedule's times are naive UTC or local)."""
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


@pytest.fixture(autouse=True)
def _clear_memo():
    events_mod._clear_schedule_memo()
    yield
    events_mod._clear_schedule_memo()


def _expected(value, column):
    """A pandas schedule value as the polars schedule holds it."""
    if value is None or value is pd.NaT or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)  # local track time, offset in GmtOffset
    if hasattr(value, "item"):
        return value.item()
    return value


def _assert_same_rows(pandas_frame, polars_frame):
    assert isinstance(polars_frame, pl.DataFrame)
    assert polars_frame.columns == list(pandas_frame.columns)
    assert polars_frame.height == len(pandas_frame)
    for column in pandas_frame.columns:
        expected = [_expected(v, column) for v in pandas_frame[column].tolist()]
        assert polars_frame[column].to_list() == expected, column


def _per_event_schedule(year, backend=None):
    """The schedule as the per-event ``Event`` Series build made it."""
    payload = events_mod._resolve_year_payload(year, backend)
    rows = [
        events_mod._create_event(year, name, payload["metadata"].get(name), backend).to_dict()
        for name in events_mod._get_events_cached(year, backend)
    ]
    return EventSchedule(rows)


@pytest.mark.parametrize(("year", "backend"), [(2025, None), (2024, None), (1998, None)])
def test_pandas_schedule_equals_per_event_build(year, backend):
    schedule = get_event_schedule(year, backend=backend)
    expected = _per_event_schedule(year, backend)
    pd.testing.assert_frame_equal(schedule, expected)
    assert list(schedule.columns) == list(expected.columns)
    assert schedule.dtypes.astype(str).tolist() == expected.dtypes.astype(str).tolist()
    for column in schedule.columns:
        assert [type(v) for v in schedule[column].tolist()] == [
            type(v) for v in expected[column].tolist()
        ], column
    assert schedule.year == year


def test_pandas_schedule_keeps_testing_columns_last_order():
    # The testing event (three sessions) is the last row of 2025: Session4/5
    # come from the race rows, GmtOffset and the circuit columns after them.
    columns = list(get_event_schedule(2025).columns)
    assert columns[:7] == [
        "RoundNumber",
        "Country",
        "Location",
        "OfficialEventName",
        "EventDate",
        "EventName",
        "EventFormat",
    ]
    assert columns[columns.index("F1ApiSupport") + 1] == "GmtOffset"
    assert columns[-1] == "CircuitUrl"


@pytest.mark.parametrize(
    ("year", "backend", "include_testing"),
    [
        (2025, None, True),
        (2025, None, False),
        (2026, None, True),
        (2021, None, True),
        (2025, "ergast", True),
        (1998, None, True),
        (1955, None, False),
    ],
)
def test_polars_schedule_matches_pandas(year, backend, include_testing):
    pandas_frame = get_event_schedule(year, include_testing=include_testing, backend=backend)
    polars_frame = get_event_schedule(
        year, include_testing=include_testing, backend=backend, lib="polars"
    )
    _assert_same_rows(pandas_frame, polars_frame)


def test_polars_schedule_dtypes():
    frame = get_event_schedule(2025, lib="polars")
    schema = frame.schema
    assert schema["RoundNumber"] == pl.Int64
    assert schema["F1ApiSupport"] == pl.Boolean
    assert schema["CircuitLatitude"] == pl.Float64
    assert schema["EventDate"] == pl.Datetime("us")
    assert schema["Session5DateUtc"] == pl.Datetime("us")
    assert schema["Session1Date"] == pl.Datetime("us")
    assert schema["EventName"] == pl.String
    assert schema["Session4"] == pl.String
    first = frame.row(0, named=True)
    assert first["EventName"] == "Australian Grand Prix"
    assert first["Session1Date"] == _naive(2025, 3, 14, 12, 30)  # local (+11:00)
    assert first["Session1DateUtc"] == _naive(2025, 3, 14, 1, 30)
    assert first["EventDate"] == _naive(2025, 3, 16)
    testing = frame.filter(pl.col("EventFormat") == "testing").row(0, named=True)
    assert testing["RoundNumber"] == 0
    assert testing["Session4"] is None
    assert testing["CircuitLatitude"] is None


def test_polars_schedule_without_testing_has_race_rounds_only():
    frame = get_event_schedule(2025, include_testing=False, lib="polars")
    assert frame["RoundNumber"].to_list() == list(range(1, 25))


@pytest.mark.parametrize("include_testing", [True, False])
@pytest.mark.parametrize(
    "dt",
    [_naive(2025, 1, 1), _naive(2025, 2, 27, 12), _naive(2025, 7, 1), _naive(2026, 1, 1)],
)
def test_polars_events_remaining_matches_pandas(dt, include_testing):
    pandas_frame = get_events_remaining(dt, include_testing=include_testing)
    polars_frame = get_events_remaining(dt, include_testing=include_testing, lib="polars")
    _assert_same_rows(pandas_frame, polars_frame)


def test_events_remaining_keeps_index_labels():
    remaining = get_events_remaining(_naive(2025, 7, 1))
    assert isinstance(remaining, EventSchedule)
    assert remaining.year == 2025
    # Rows 0-10 (rounds 1-11) and the February test are over; labels are kept.
    assert remaining.index.tolist() == list(range(11, 24))
    assert remaining["RoundNumber"].tolist() == list(range(12, 25))


def test_unknown_lib_raises():
    with pytest.raises(ValueError, match="Unknown schedule lib"):
        get_event_schedule(2025, lib="arrow")  # ty: ignore[no-matching-overload]


def test_polars_missing_falls_back_to_pandas(monkeypatch, caplog):
    from tif1.core_utils import runtime

    monkeypatch.setattr(runtime, "import_supported_polars", lambda: None)
    schedule = get_event_schedule(2025, lib="polars")
    assert isinstance(schedule, EventSchedule)
    assert "falling back to pandas" in caplog.text


@pytest.mark.parametrize(
    "gp", ["Monza", "monza", "Monacco", "british", "Spielberg", 8, 8.0, "3", True]
)
def test_get_event_matches_schedule_lookup(gp):
    schedule = get_event_schedule(2025)
    if isinstance(gp, str):
        expected = schedule.get_event_by_name(gp)
        if expected is None:
            expected = schedule.get_event_by_round(int(gp))
    else:
        expected = schedule.get_event_by_round(gp)
    event = get_event(2025, gp)
    assert isinstance(event, Event)
    pd.testing.assert_series_equal(event, expected)
    assert event.year == 2025


@pytest.mark.parametrize(
    ("gp", "message"),
    [
        (0, "Cannot get testing event by round number!"),
        (99, "Invalid round: 99"),
        (None, "missing 1 required argument: 'round_number'"),
    ],
)
def test_get_event_round_errors_match_schedule(gp, message):
    with pytest.raises(ValueError, match=message):
        get_event(2025, gp)
    with pytest.raises(ValueError, match=message):
        get_event_schedule(2025).get_event_by_round(gp)


def test_get_event_unmatched_name_returns_none():
    assert get_event(2025, "Moon GP") is None
    assert get_event(2025, "Monacco", exact_match=True) is None
    assert get_event(2030, "Monza") is None


def test_get_event_by_round():
    event = get_event_by_round(2025, 24)
    pd.testing.assert_series_equal(event, get_event_schedule(2025).get_event_by_round(24))
