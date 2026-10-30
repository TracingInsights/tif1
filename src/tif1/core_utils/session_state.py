"""Derived session state: fastf1's ``track_status``, ``session_status``, ``session_info``,
``t0_date`` and ``session_start_time`` built from the files the CDN serves.

fastf1 reads these from the live-timing streams (``TrackStatus``,
``SessionStatus``, ``SessionInfo``), which need an F1 account and are outside
tif1's historical scope. The derivations below use the laps table (its
``LapStartDate - LapStartTime`` is the session's ``t0_date`` to the
millisecond), the race-control messages (exact timestamps of every flag and
safety-car message) and the schedule row. Shapes, status codes and message
strings are fastf1's exactly; the data is an approximation of the streams and
is documented as such.

Every function takes plain Python sequences or either backend's frame and
returns the frame of the requested backend; the state machines run once on
Python lists (a few hundred race-control rows) so there is one
implementation per derivation. Clock columns are read as whole numpy arrays
(``to_numpy()`` on either backend, never a polars query plan).

The typed weather and race-control tables (:func:`weather_frame`,
:func:`race_control_frame`) are built here straight from the payload lists
for the common payload shapes; the session falls back to its generic build
for anything else.
"""

from __future__ import annotations

import logging
import math
import re
from datetime import timedelta
from typing import Any

import numpy as np

from .runtime import import_supported_polars

logger = logging.getLogger(__name__)

#: fastf1's track-status vocabulary (``fastf1.api.track_status_data``).
TRACK_STATUS_MESSAGES: dict[str, str] = {
    "1": "AllClear",
    "2": "Yellow",
    "4": "SCDeployed",
    "5": "Red",
    "6": "VSCDeployed",
    "7": "VSCEnding",
}

#: Nominal session durations for ``session_info["EndDate"]`` (no end time is
#: published in the files tif1 reads).
_SESSION_DURATIONS: dict[str, timedelta] = {
    "Practice 1": timedelta(hours=1, minutes=30),
    "Practice 2": timedelta(hours=1, minutes=30),
    "Practice 3": timedelta(hours=1),
    "Qualifying": timedelta(hours=1),
    "Sprint Qualifying": timedelta(minutes=44),
    "Sprint Shootout": timedelta(minutes=44),
    "Sprint": timedelta(hours=1),
    "Race": timedelta(hours=2),
}

_NS_MIN = np.iinfo("int64").min


def _is_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


# -- column access on either backend -------------------------------------------


def _column_list(frame: Any, name: str) -> list[Any] | None:
    """A column of a pandas or polars frame as a Python list (``None`` if absent)."""
    if frame is None:
        return None
    columns = getattr(frame, "columns", None)
    if columns is None or name not in list(columns):
        return None
    column = frame[name]
    if hasattr(column, "to_list"):
        return list(column.to_list())
    return list(column.tolist())


def _datetime_ns(values: list[Any] | None) -> np.ndarray:
    """Datetime-like values as int64 nanoseconds (``NaT``/``None`` -> int64 min)."""
    if not values:
        return np.empty(0, dtype="int64")
    out = np.full(len(values), _NS_MIN, dtype="int64")
    for i, value in enumerate(values):
        if value is None:
            continue
        try:
            ns = np.datetime64(value, "ns")
        except (TypeError, ValueError):
            try:
                ns = np.datetime64(str(value), "ns")
            except (TypeError, ValueError):
                continue
        if not np.isnat(ns):
            out[i] = ns.astype("int64")
    return out


def _timedelta_ns(values: list[Any] | None) -> np.ndarray:
    """Timedelta-like values (timedelta, numpy, pandas or float seconds) as int64 ns."""
    if not values:
        return np.empty(0, dtype="int64")
    out = np.full(len(values), _NS_MIN, dtype="int64")
    for i, value in enumerate(values):
        if value is None:
            continue
        if isinstance(value, bool):
            continue
        if isinstance(value, int | float | np.integer | np.floating):
            if math.isnan(float(value)):
                continue
            out[i] = round(float(value) * 1_000_000_000)
            continue
        try:
            ns = np.timedelta64(value, "ns")
        except (TypeError, ValueError):
            continue
        if not np.isnat(ns):
            out[i] = ns.astype("int64")
    return out


def _column_ns(frame: Any, name: str, *, timedelta: bool) -> np.ndarray:
    """A clock column of either backend as int64 ns (int64 min where unknown).

    Native columns convert as one array: pandas ``datetime64``/``timedelta64``
    of any unit (naive), polars ``Datetime`` (naive)/``Duration``. polars
    values are cut to microseconds as its ``to_list()`` objects are
    (datetimes floor, durations truncate toward zero), so both paths give
    the same numbers. Any other dtype goes value by value.
    """
    native = _native_column_ns(frame, name, timedelta=timedelta)
    if native is not None:
        return native
    values = _column_list(frame, name)
    return _timedelta_ns(values) if timedelta else _datetime_ns(values)


def _native_column_ns(frame: Any, name: str, *, timedelta: bool) -> np.ndarray | None:
    """:func:`_column_ns` for a natively typed column; None for any other column."""
    if frame is None:
        return None
    columns = getattr(frame, "columns", None)
    if columns is None or name not in columns:
        return None
    column = frame[name]
    dtype = column.dtype
    target = "timedelta64[ns]" if timedelta else "datetime64[ns]"
    if isinstance(dtype, np.dtype):  # pandas (numpy-backed, so never tz-aware)
        if dtype.kind != ("m" if timedelta else "M"):
            return None
        return column.to_numpy().astype(target).view("int64")
    pl = import_supported_polars()
    if pl is None or not isinstance(column, pl.Series):
        return None
    if timedelta:
        if not isinstance(dtype, pl.Duration):
            return None
    elif not isinstance(dtype, pl.Datetime) or dtype.time_zone is not None:
        return None
    ns = column.to_numpy().astype(target).view("int64")
    valid = ns != _NS_MIN
    if valid.any():
        values = ns[valid]
        # The microseconds a datetime/timedelta object of to_list() holds.
        ns[valid] = values - np.fmod(values, 1000) if timedelta else values // 1000 * 1000
    return ns


def _text(values: list[Any] | None, n: int) -> list[str]:
    if not values:
        return [""] * n
    return ["" if value is None or _is_nan(value) else str(value) for value in values]


# -- t0_date / session_start_time ---------------------------------------------


def t0_date_ns(laps: Any) -> int | None:
    """``median(LapStartDate - LapStartTime)`` of the laps table as int64 ns, or None.

    Both columns come from fastf1's session clock in the extraction, so the
    difference is constant to the millisecond across every lap.
    """
    dates = _column_ns(laps, "LapStartDate", timedelta=False)
    starts = _column_ns(laps, "LapStartTime", timedelta=True)
    if not len(dates) or len(dates) != len(starts):
        return None
    valid = (dates != _NS_MIN) & (starts != _NS_MIN)
    if not valid.any():
        return None
    offsets = dates[valid] - starts[valid]
    # fastf1 rounds t0_date to the millisecond; the float lap starts carry
    # nanosecond noise below that.
    return round(float(np.median(offsets)) / 1_000_000) * 1_000_000


def session_start_time_ns(
    rcm: Any, t0_ns: int | None, scheduled_utc_ns: int | None, *, race_like: bool
) -> int | None:
    """SessionTime of the session start as int64 ns, or None.

    Races and sprints start on the scheduled time (the formation lap); every
    other session starts with the first ``GREEN LIGHT - PIT EXIT OPEN``
    race-control message. Each falls back to the other.
    """
    if t0_ns is None:
        return None
    green = _first_green_light_ns(rcm)
    candidates = (scheduled_utc_ns, green) if race_like else (green, scheduled_utc_ns)
    for candidate in candidates:
        if candidate is not None and candidate != _NS_MIN:
            return int(candidate - t0_ns)
    return None


def _timed_positions(times: np.ndarray) -> tuple[list[int], list[int]]:
    """Row positions with a timestamp and their times, in stable time order."""
    order = np.argsort(times, kind="stable")
    ordered = times[order]
    keep = ordered != _NS_MIN
    return order[keep].tolist(), ordered[keep].tolist()


def _first_green_light_ns(rcm: Any) -> int | None:
    times = _column_ns(rcm, "Time", timedelta=False)
    if not len(times):
        return None
    messages = _text(_column_list(rcm, "Message"), len(times))
    flags = _text(_column_list(rcm, "Flag"), len(times))
    for position, time_ns in zip(*_timed_positions(times), strict=True):
        if flags[position].upper() == "GREEN" and messages[position].upper().startswith(
            "GREEN LIGHT"
        ):
            return time_ns
    return None


# -- track status ---------------------------------------------------------------


def _rcm_rows(rcm: Any, t0_ns: int) -> list[tuple[int, str, str, str, str, int | None, str]]:
    """Race-control rows as ``(session_time_ns, category, flag, scope, status, sector, message)``.

    Time-sorted; rows without a timestamp are dropped.
    """
    times = _column_ns(rcm, "Time", timedelta=False)
    n = len(times)
    if not n:
        return []
    categories = _text(_column_list(rcm, "Category"), n)
    flags = _text(_column_list(rcm, "Flag"), n)
    scopes = _text(_column_list(rcm, "Scope"), n)
    statuses = _text(_column_list(rcm, "Status"), n)
    messages = _text(_column_list(rcm, "Message"), n)
    sectors_raw = _column_list(rcm, "Sector") or [None] * n
    rows = []
    for position, time_ns in zip(*_timed_positions(times), strict=True):
        sector_value = sectors_raw[position]
        sector: int | None
        try:
            sector = (
                None if sector_value is None or _is_nan(sector_value) else int(float(sector_value))
            )
        except (TypeError, ValueError):
            sector = None
        rows.append(
            (
                time_ns - t0_ns,
                categories[position],
                flags[position].upper(),
                scopes[position].upper(),
                statuses[position].upper(),
                sector,
                messages[position].upper(),
            )
        )
    return rows


def track_status_rows(rcm: Any, t0_ns: int) -> list[tuple[int, str]]:
    """``(session_time_ns, status_code)`` transitions derived from race control.

    One pass over the messages: track-scope flags, sector yellows (a set, so
    the status returns to ``1`` only when every sector is clear), safety car
    and virtual safety car deployments. Starts with ``AllClear`` at time 0 as
    the live stream does; consecutive identical statuses collapse.
    """
    rows: list[tuple[int, str]] = [(0, "1")]
    current = "1"
    yellow_sectors: set[int] = set()
    sc_active = False
    vsc_active = False

    def emit(time_ns: int, status: str) -> None:
        nonlocal current
        if status != current:
            rows.append((max(time_ns, 0), status))
            current = status

    for time_ns, category, flag, scope, status, sector, message in _rcm_rows(rcm, t0_ns):
        if category == "Flag":
            if scope in ("TRACK", ""):
                if flag in ("GREEN", "CLEAR"):
                    yellow_sectors.clear()
                    sc_active = vsc_active = False
                    emit(time_ns, "1")
                elif flag in ("YELLOW", "DOUBLE YELLOW"):
                    if not sc_active and not vsc_active and current != "5":
                        emit(time_ns, "2")
                elif flag == "RED":
                    yellow_sectors.clear()
                    sc_active = vsc_active = False
                    emit(time_ns, "5")
            elif scope == "SECTOR":
                if flag in ("YELLOW", "DOUBLE YELLOW"):
                    if sector is not None:
                        yellow_sectors.add(sector)
                    if not sc_active and not vsc_active and current != "5":
                        emit(time_ns, "2")
                elif flag == "CLEAR":
                    if sector is not None:
                        yellow_sectors.discard(sector)
                    if not yellow_sectors and not sc_active and not vsc_active and current == "2":
                        emit(time_ns, "1")
        elif category == "SafetyCar":
            virtual = "VIRTUAL" in message
            if status == "DEPLOYED":
                if virtual:
                    vsc_active, sc_active = True, False
                    emit(time_ns, "6")
                else:
                    sc_active, vsc_active = True, False
                    emit(time_ns, "4")
            elif status == "ENDING" and virtual:
                emit(time_ns, "7")
    return rows


def track_status_rows_from_laps(laps: Any) -> list[tuple[int, str]]:
    """Fallback transitions from the laps' ``TrackStatus`` codes at ``LapStartTime``.

    Lap granularity: a multi-code lap (``"124"``) emits its codes in order at
    the lap start; for each lap number the earliest lap start is used.
    """
    starts = _column_ns(laps, "LapStartTime", timedelta=True)
    codes = _text(_column_list(laps, "TrackStatus"), len(starts))
    numbers = _column_list(laps, "LapNumber") or [None] * len(starts)
    rows: list[tuple[int, str]] = [(0, "1")]
    if not len(starts):
        return rows
    first_by_lap: dict[Any, tuple[int, str]] = {}
    for position, start_ns in zip(*_timed_positions(starts), strict=True):
        key = numbers[position]
        if key not in first_by_lap:
            first_by_lap[key] = (start_ns, codes[position])
    current = "1"
    for start_ns, code in sorted(first_by_lap.values()):
        for char in code:
            if char in TRACK_STATUS_MESSAGES and char != current:
                rows.append((start_ns, char))
                current = char
    return rows


# -- session status -------------------------------------------------------------


def session_status_rows(rcm: Any, t0_ns: int, start_ns: int | None) -> list[tuple[int, str]]:
    """``(session_time_ns, status)`` rows: Started, Aborted (red flag), Started, Finished.

    ``Finalised`` and ``Ends`` are not derivable from the files and are not
    emitted.
    """
    rows: list[tuple[int, str]] = []
    if start_ns is not None:
        rows.append((start_ns, "Started"))
    # A green light restarts the session after a red flag and starts the
    # next segment after a chequered flag (Q2/Q3, SQ2/SQ3), as the stream's
    # ``Started`` rows do.
    last = "Started" if start_ns is not None else ""
    for time_ns, category, flag, scope, _status, _sector, message in _rcm_rows(rcm, t0_ns):
        if category != "Flag" or scope not in ("TRACK", ""):
            continue
        if flag == "RED":
            rows.append((time_ns, "Aborted"))
            last = "Aborted"
        elif flag == "GREEN" and message.startswith("GREEN LIGHT") and last != "Started":
            if last or start_ns is None:
                rows.append((time_ns, "Started"))
                last = "Started"
        elif flag == "CHEQUERED":
            rows.append((time_ns, "Finished"))
            last = "Finished"
    rows.sort(key=lambda row: row[0])
    return rows


# -- session info ---------------------------------------------------------------


def _session_type(session_name: str) -> str:
    if session_name.startswith("Practice"):
        return "Practice"
    if "Qualifying" in session_name or "Shootout" in session_name:
        return "Qualifying"
    return "Race"


def _event_value(event: Any, key: str) -> Any:
    try:
        value = event[key] if key in event.index else None
    except (AttributeError, KeyError, TypeError):
        value = getattr(event, key, None)
    if value is None or _is_nan(value):
        return None
    return value


def session_info_dict(
    event: Any,
    session_name: str,
    *,
    start_date: Any,
    gmt_offset: Any,
    circuit_key: int | None = None,
    country_key: int | None = None,
    country_code: str = "",
    circuit_short_name: str = "",
) -> dict[str, Any]:
    """fastf1's ``session_info`` dict from the schedule row (keys tif1 cannot know are None)."""
    country_name = _event_value(event, "Country") or ""
    duration = _SESSION_DURATIONS.get(session_name, timedelta(hours=1))
    end_date = None if start_date is None else start_date + duration
    return {
        "Meeting": {
            "Key": None,
            "Name": _event_value(event, "EventName") or "",
            "OfficialName": _event_value(event, "OfficialEventName") or "",
            "Location": _event_value(event, "Location") or "",
            "Country": {"Key": country_key, "Code": country_code, "Name": country_name},
            "Circuit": {
                "Key": circuit_key,
                "ShortName": circuit_short_name or _event_value(event, "CircuitName") or "",
            },
        },
        "ArchiveStatus": None,
        "Key": None,
        "Type": _session_type(session_name),
        "Name": session_name,
        "StartDate": start_date,
        "EndDate": end_date,
        "GmtOffset": gmt_offset,
        "Path": None,
    }


# -- frame construction ----------------------------------------------------------


def status_frame(rows: list[tuple[int, str]], lib: str, *, with_message: bool) -> Any:
    """``Time``/``Status``(/``Message``) frame of ``rows`` on the requested backend."""
    times = np.asarray([row[0] for row in rows], dtype="int64")
    statuses = [row[1] for row in rows]
    messages = [TRACK_STATUS_MESSAGES.get(status, "") for status in statuses]
    if lib == "polars":
        pl = import_supported_polars()
        if pl is None:  # pragma: no cover - a polars session always has polars
            raise ImportError("polars is required for lib='polars'")
        data: dict[str, Any] = {
            "Time": pl.Series("Time", times, dtype=pl.Int64).cast(pl.Duration("ns")),
            "Status": pl.Series("Status", statuses, dtype=pl.String),
        }
        if with_message:
            data["Message"] = pl.Series("Message", messages, dtype=pl.String)
        return pl.DataFrame(data)
    import pandas as pd

    data_pd: dict[str, Any] = {
        "Time": pd.Series(times.view("timedelta64[ns]")),
        "Status": pd.Series(statuses, dtype=object),
    }
    if with_message:
        data_pd["Message"] = pd.Series(messages, dtype=object)
    return pd.DataFrame(data_pd)


# -- typed weather / race-control tables ------------------------------------------

_WEATHER_FLOAT_COLUMNS = frozenset({"AirTemp", "Humidity", "Pressure", "TrackTemp", "WindSpeed"})
_WEATHER_COLUMNS = _WEATHER_FLOAT_COLUMNS | {"Time", "WindDirection", "Rainfall"}
_RCM_TEXT_COLUMNS = frozenset({"Category", "Message", "Status", "Flag", "Scope", "RacingNumber"})
_RCM_COLUMNS = _RCM_TEXT_COLUMNS | {"Time", "Sector", "Lap"}
# Timestamps numpy, pandas' ISO8601 parser and polars' ``%Y-%m-%dT%H:%M:%S%.f`` agree on.
_ISO_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?")
# Integers a float64 holds exactly: the generic builds pass some columns through float64.
_FLOAT_EXACT_INT = 2**53


def _table_columns(
    data: dict[str, Any], rename_map: dict[str, str], known: frozenset[str]
) -> list[tuple[str, str]] | None:
    """``(payload key, column)`` pairs when every key renames to its own known column.

    Only then does the generic build rename every key in payload order; a
    payload with other keys, clashing keys or empty or ragged lists is left
    to it.
    """
    pairs: list[tuple[str, str]] = []
    taken: set[str] = set()
    length = -1
    for key, values in data.items():
        column = rename_map.get(key)
        if column is None or column not in known or column in taken:
            return None
        if type(values) is not list or not values or length not in (-1, len(values)):
            return None
        length = len(values)
        taken.add(column)
        pairs.append((key, column))
    return pairs or None


def weather_frame(data: dict[str, Any], rename_map: dict[str, str], lib: str) -> Any | None:
    """The typed weather table of ``data`` built from its lists, or None.

    Same frame as the generic build (renamed payload frame, then the
    per-column casts of ``Session.weather``) for a payload whose columns are
    all known and hold plain JSON numbers/booleans; None for anything else
    (sentinels, nulls, strings), which the generic build handles.
    """
    pairs = _table_columns(data, rename_map, _WEATHER_COLUMNS)
    if pairs is None:
        return None
    from .helpers import _seconds_to_td64ns, _td_cast_is_safe

    arrays: dict[str, np.ndarray] = {}
    try:
        for key, column in pairs:
            values = data[key]
            types = set(map(type, values))
            if column == "Rainfall":
                if types != {bool}:
                    return None
                arrays[column] = np.array(values, dtype=bool)
            elif column == "WindDirection":
                # polars rounds through float64 before its Int64 cast.
                if types != {int}:
                    return None
                directions = np.array(values, dtype=np.int64)
                if np.abs(directions).max() >= _FLOAT_EXACT_INT:
                    return None
                arrays[column] = directions
            else:
                if not types <= {float, int}:
                    return None
                floats = np.array(values, dtype=np.float64)
                if column == "Time":
                    if not _td_cast_is_safe(floats):
                        return None
                    floats = _seconds_to_td64ns(floats)
                arrays[column] = floats
    except (OverflowError, TypeError, ValueError):
        return None
    if lib == "polars":
        pl = import_supported_polars()
        if pl is None:  # pragma: no cover - a polars session always has polars
            return None
        return pl.DataFrame([pl.Series(column, array) for column, array in arrays.items()])
    import pandas as pd

    columns: dict[str, Any] = dict(arrays)
    if "WindDirection" in columns:
        columns["WindDirection"] = pd.array(columns["WindDirection"], dtype="Int64")
    if "Rainfall" in columns:
        columns["Rainfall"] = pd.array(columns["Rainfall"], dtype="boolean")
    return pd.DataFrame(columns)


def _iso_timestamps_ns(values: list[Any]) -> np.ndarray | None:
    """Plain ISO timestamps (no zone) as int64 ns, or None when any value is not one."""
    for value in values:
        if type(value) is not str or _ISO_TIMESTAMP.fullmatch(value) is None:
            return None
    try:
        return np.array(values, dtype="datetime64[ns]").view("int64")
    except (OverflowError, ValueError):
        return None


def _sector_floats(values: list[Any], missing: Any) -> list[Any] | None:
    """Sector numbers as floats (``missing`` for a gap), or None for any other value.

    Gaps are ``None`` or ``"None"``; the numbers are either all digit strings
    or all JSON numbers (never both), which the generic builds parse to the
    same floats.
    """
    out: list[Any] = []
    digits = numbers = False
    for value in values:
        kind = type(value)
        if value is None or value == "None":
            out.append(missing)
        elif kind is str:
            if not (value.isascii() and value.isdigit() and len(value) < 16):
                return None
            digits = True
            out.append(float(value))
        elif kind is float or (kind is int and abs(value) < _FLOAT_EXACT_INT):
            numbers = True
            out.append(float(value))
        else:
            return None
    return None if digits and numbers else out


def race_control_frame(data: dict[str, Any], rename_map: dict[str, str], lib: str) -> Any | None:
    """The typed race-control table of ``data`` built from its lists, or None.

    Same frame as the generic build (renamed payload frame, then the
    per-column parsing of ``Session.race_control_messages``) for a payload
    whose columns are all known, with plain ISO timestamps, text columns,
    numeric sectors and integer laps (``"None"`` gaps allowed); None for
    anything else, which the generic build handles.
    """
    pairs = _table_columns(data, rename_map, _RCM_COLUMNS)
    if pairs is None:
        return None
    if lib == "polars":
        return _race_control_polars(data, pairs)
    import pandas as pd

    columns: dict[str, Any] = {}
    for key, column in pairs:
        values = data[key]
        if column == "Time":
            times = _iso_timestamps_ns(values)
            if times is None:
                return None
            columns[column] = times.view("datetime64[ns]")
        elif column == "Sector":
            if any(value is None for value in values):
                return None
            sectors = _sector_floats(values, math.nan)
            if sectors is None:
                return None
            columns[column] = np.array(sectors, dtype=np.float64)
        elif column == "Lap":
            laps: list[int] = []
            for value in values:
                if type(value) is int and abs(value) < _FLOAT_EXACT_INT:
                    laps.append(value)
                elif value == "None":
                    laps.append(0)
                else:
                    return None
            columns[column] = np.array(laps, dtype=np.int64)
        else:
            if any(type(value) is not str for value in values):
                return None
            columns[column] = pd.Series(
                [None if value == "None" else value for value in values], dtype=object
            )
    return pd.DataFrame(columns)


def _race_control_polars(data: dict[str, Any], pairs: list[tuple[str, str]]) -> Any | None:
    """:func:`race_control_frame` for polars: the generic build's null coercion, then typed lists."""
    pl = import_supported_polars()
    if pl is None:  # pragma: no cover - a polars session always has polars
        return None
    from .helpers import _coerce_null_like_payload

    data = _coerce_null_like_payload(data)
    series = []
    try:
        for key, column in pairs:
            values = data[key]
            if column == "Time":
                times = _iso_timestamps_ns(values)
                # polars parses these timestamps to microseconds.
                if times is None or (times % 1000).any():
                    return None
                series.append(pl.Series(column, times.view("datetime64[ns]")))
            elif column == "Sector":
                sectors = _sector_floats(values, None)
                if sectors is None:
                    return None
                series.append(pl.Series(column, sectors, dtype=pl.Float64))
            elif column == "Lap":
                if any(
                    value is not None and (type(value) is not int or abs(value) >= 2**63)
                    for value in values
                ):
                    return None
                series.append(pl.Series(column, values, dtype=pl.Int64))
            else:
                if any(value is not None and type(value) is not str for value in values):
                    return None
                series.append(pl.Series(column, values, dtype=pl.String))
    except (OverflowError, TypeError, ValueError):
        return None
    return pl.DataFrame(series)


__all__ = [
    "TRACK_STATUS_MESSAGES",
    "race_control_frame",
    "session_info_dict",
    "session_start_time_ns",
    "session_status_rows",
    "status_frame",
    "t0_date_ns",
    "track_status_rows",
    "track_status_rows_from_laps",
    "weather_frame",
]
