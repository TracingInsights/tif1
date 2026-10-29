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
implementation per derivation.
"""

from __future__ import annotations

import logging
import math
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
    dates = _datetime_ns(_column_list(laps, "LapStartDate"))
    starts = _timedelta_ns(_column_list(laps, "LapStartTime"))
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


def _first_green_light_ns(rcm: Any) -> int | None:
    times = _datetime_ns(_column_list(rcm, "Time"))
    if not len(times):
        return None
    messages = _text(_column_list(rcm, "Message"), len(times))
    flags = _text(_column_list(rcm, "Flag"), len(times))
    for position in np.argsort(times, kind="stable"):
        if times[position] == _NS_MIN:
            continue
        if flags[position].upper() == "GREEN" and messages[position].upper().startswith(
            "GREEN LIGHT"
        ):
            return int(times[position])
    return None


# -- track status ---------------------------------------------------------------


def _rcm_rows(rcm: Any, t0_ns: int) -> list[tuple[int, str, str, str, str, int | None, str]]:
    """Race-control rows as ``(session_time_ns, category, flag, scope, status, sector, message)``.

    Time-sorted; rows without a timestamp are dropped.
    """
    times = _datetime_ns(_column_list(rcm, "Time"))
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
    for position in np.argsort(times, kind="stable"):
        if times[position] == _NS_MIN:
            continue
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
                int(times[position]) - t0_ns,
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
    starts = _timedelta_ns(_column_list(laps, "LapStartTime"))
    codes = _text(_column_list(laps, "TrackStatus"), len(starts))
    numbers = _column_list(laps, "LapNumber") or [None] * len(starts)
    rows: list[tuple[int, str]] = [(0, "1")]
    if not len(starts):
        return rows
    first_by_lap: dict[Any, tuple[int, str]] = {}
    for position in np.argsort(starts, kind="stable"):
        if starts[position] == _NS_MIN:
            continue
        key = numbers[position]
        if key not in first_by_lap:
            first_by_lap[key] = (int(starts[position]), codes[position])
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


__all__ = [
    "TRACK_STATUS_MESSAGES",
    "session_info_dict",
    "session_start_time_ns",
    "session_status_rows",
    "status_frame",
    "t0_date_ns",
    "track_status_rows",
    "track_status_rows_from_laps",
]
