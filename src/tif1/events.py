"""Event and session information for F1 seasons."""

from __future__ import annotations

import json
import logging
import math
import re
import threading
import time
import unicodedata
import warnings
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from importlib.resources import files
from typing import TYPE_CHECKING, Any, cast

from .cache import get_cache
from .config import get_config
from .core_utils.constants import EVENT_NAME_ALIASES, F1_API_FIRST_YEAR, MIN_YEAR
from .core_utils.runtime import THREADS_AVAILABLE
from .exceptions import (
    CacheError,
    DataNotFoundError,
    EventNotFoundError,
    EventNotInSeasonError,
    InvalidDataError,
    NetworkError,
)
from .payload_loader import get_url_loader
from .schedule_schema import validate_schedule_payload

if TYPE_CHECKING:
    from .event_frames import Event, EventSchedule
    from .types import ScheduleBackend


def __getattr__(name: str) -> Any:
    # ``Event``/``EventSchedule`` subclass pandas objects; they are defined in
    # ``event_frames`` and imported on first use so the schedule (event and
    # session names) loads without pandas.
    if name in ("Event", "EventSchedule"):
        from . import event_frames

        return getattr(event_frames, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


_F1SCHEDULE_YEAR_PATTERN = re.compile(r"^schedule_(\d{4})\.json$")
# Same source FastF1 reads. raw.githubusercontent.com serves a branch file with
# a 5-minute edge cache, so a calendar change lands within minutes; jsDelivr
# pins branch references for 12 hours (and the CDN chain in ``tif1.cdn`` is for
# the large per-year data repositories, not this one small file per season).
_F1SCHEDULE_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/theOehrly/f1schedule/master/schedule_{year}.json"
)
_F1SCHEDULE_CACHE_KEY_TEMPLATE = "f1schedule/schedule_{year}.json"
_F1SCHEDULE_FETCH_TIMEOUT = 15
# A failed refresh keeps the fallback (stale cache row or vendored file) for
# this long before the next lookup tries the network again.
_F1SCHEDULE_RETRY_AFTER_SECONDS = 300.0
# A stale cached calendar is served while a background refresh runs; the
# memo re-reads the cache row after this many seconds (or as soon as the
# refresh lands, whichever comes first).
_F1SCHEDULE_STALE_GRACE_SECONDS = 60.0
_DEFAULT_SCHEDULE_TTL_HOURS = 6.0

logger = logging.getLogger(__name__)

_SESSION_TYPES = (
    "Practice 1",
    "Practice 2",
    "Practice 3",
    "Qualifying",
    "Sprint",
    "Sprint Shootout",
    "Sprint Qualifying",
    "Race",
)
_SESSION_TYPE_ABBREVIATIONS = {
    "FP1": "Practice 1",
    "FP2": "Practice 2",
    "FP3": "Practice 3",
    "Q": "Qualifying",
    "S": "Sprint",
    "SS": "Sprint Shootout",
    "SQ": "Sprint Qualifying",
    "R": "Race",
}
_SESSION_TYPES_BY_CASEFOLD = {session.casefold(): session for session in _SESSION_TYPES}


def _sorted_index_keys(column_data: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for key in column_data:
        if isinstance(key, str):
            keys.append(key)
        elif isinstance(key, int):
            keys.append(str(key))
    return sorted(keys, key=lambda value: int(value))


def _extract_sessions_for_index(raw_year: dict[str, Any], index: str) -> list[str]:
    sessions: list[str] = []
    for session_number in range(1, 6):
        column_name = f"session{session_number}"
        session_column = raw_year.get(column_name, {})
        if not isinstance(session_column, dict):
            continue
        session_name = session_column.get(index)
        if isinstance(session_name, str) and session_name.strip():
            sessions.append(session_name)
    return sessions


def _parse_gmt_offset(value: Any) -> timedelta | None:
    """Parse an f1schedule ``gmt_offset`` such as ``"+02:00"`` or ``"-04:30"``."""
    if not isinstance(value, str) or ":" not in value:
        return None
    sign = -1 if value.startswith("-") else 1
    hours, minutes = value.lstrip("+-").split(":")[:2]
    try:
        return sign * timedelta(hours=int(hours), minutes=int(minutes))
    except ValueError:
        return None


def _convert_f1schedule_year(raw_year: dict[str, Any], year: int) -> dict[str, Any]:
    event_name_col = raw_year.get("event_name")
    round_col = raw_year.get("round_number")
    event_date_col = raw_year.get("event_date", {})
    location_col = raw_year.get("location", {})
    country_col = raw_year.get("country", {})
    official_name_col = raw_year.get("official_event_name", {})
    event_format_col = raw_year.get("event_format", {})
    gmt_offset_col = raw_year.get("gmt_offset", {})
    f1_api_support_col = raw_year.get("f1_api_support", {})
    if not isinstance(event_name_col, dict) or not isinstance(round_col, dict):
        raise InvalidDataError(reason=f"Invalid f1schedule data for year={year}")

    race_events: list[tuple[int, str]] = []
    testing_events: list[tuple[str, str]] = []
    sessions: dict[str, list[str]] = {}
    event_metadata: dict[str, dict[str, Any]] = {}

    for index in _sorted_index_keys(event_name_col):
        event_name = event_name_col.get(index)
        if not isinstance(event_name, str) or not event_name.strip():
            continue
        event_name = event_name.strip()
        event_sessions = _extract_sessions_for_index(raw_year, index)
        if event_sessions:
            sessions[event_name] = event_sessions

        # f1schedule session dates are local track time; UTC is local minus
        # the event's GMT offset (left unset when the offset is unknown).
        gmt_offset = _parse_gmt_offset(gmt_offset_col.get(index))
        session_dates = {}
        session_dates_utc = {}
        for i in range(1, 6):
            session_date_col = raw_year.get(f"session{i}_date", {})
            session_date = session_date_col.get(index)
            if session_date:
                session_dates[f"Session{i}Date"] = session_date
                if gmt_offset is not None:
                    session_dates_utc[f"Session{i}DateUtc"] = (
                        datetime.fromisoformat(session_date) - gmt_offset
                    ).isoformat()

        event_metadata[event_name] = {
            "RoundNumber": round_col.get(index),
            "EventDate": event_date_col.get(index),
            "Location": location_col.get(index),
            "Country": country_col.get(index),
            "OfficialEventName": official_name_col.get(index),
            "EventFormat": event_format_col.get(index, "conventional"),
            "GmtOffset": gmt_offset_col.get(index),
            "F1ApiSupport": f1_api_support_col.get(index, True),
            **session_dates,
            **session_dates_utc,
        }

        round_value = round_col.get(index)
        if isinstance(round_value, int) and round_value > 0:
            race_events.append((round_value, event_name))
            continue

        event_date_value = event_date_col.get(index)
        date_key = event_date_value if isinstance(event_date_value, str) else ""
        testing_events.append((date_key, event_name))

    race_events_sorted = [
        event_name for _, event_name in sorted(race_events, key=lambda item: item[0])
    ]
    testing_sorted = [
        event_name for _, event_name in sorted(testing_events, key=lambda item: item[0])
    ]
    return {
        "events": race_events_sorted + testing_sorted,
        "sessions": sessions,
        "metadata": event_metadata,
    }


def _load_vendored_f1schedule_years() -> dict[str, Any]:
    schedules_dir = files("tif1").joinpath("data/schedules/f1schedule")
    years: dict[str, Any] = {}
    for item in schedules_dir.iterdir():
        match = _F1SCHEDULE_YEAR_PATTERN.match(item.name)
        if match is None:
            continue
        year_str = match.group(1)
        with item.open("r", encoding="utf-8") as handle:
            raw_year = json.load(handle)
        if not isinstance(raw_year, dict):
            raise InvalidDataError(reason=f"Invalid schedule payload file: {item.name}")
        years[year_str] = _convert_f1schedule_year(raw_year, int(year_str))
    return years


@dataclass(slots=True)
class _YearSchedule:
    """Per-process memo of one season's converted schedule.

    ``expires_at`` is the wall-clock time after which the live source is
    consulted again; ``math.inf`` for seasons served from the vendored files.
    ``sessions`` memoizes resolved session lists per queried event name (the
    fuzzy event resolution is the expensive part of ``get_sessions``).
    """

    payload: dict[str, Any]
    events: tuple[str, ...]
    expires_at: float
    sessions: dict[str, tuple[str, ...]] = field(default_factory=dict)
    # Refresh generation the record was built from; a background refresh
    # that lands bumps the season's generation so the memo re-reads the row.
    generation: int = 0


_year_schedules: dict[int, _YearSchedule] = {}
_year_schedules_lock = threading.Lock()


def _schedule_ttl_seconds() -> float:
    ttl_hours = get_config().get("schedule_ttl_hours", _DEFAULT_SCHEDULE_TTL_HOURS)
    return float(ttl_hours) * 3600.0


def _is_current_or_future_season(year: int) -> bool:
    return year >= datetime.now(UTC).year


def _read_cached_f1schedule_year(year: int) -> tuple[float, dict[str, Any]] | None:
    """Return ``(fetched_at, raw_year)`` from the SQLite cache, or None."""
    key = _F1SCHEDULE_CACHE_KEY_TEMPLATE.format(year=year)
    try:
        entry = get_cache().get_entry("json", key)
    except (CacheError, OSError) as exc:
        logger.debug("Schedule cache read failed for %s: %s", key, exc)
        return None
    if not isinstance(entry, dict):
        return None
    fetched_at = entry.get("fetched_at")
    raw_year = entry.get("schedule")
    if not isinstance(fetched_at, int | float) or not isinstance(raw_year, dict):
        return None
    return float(fetched_at), raw_year


def _write_cached_f1schedule_year(year: int, raw_year: dict[str, Any], fetched_at: float) -> None:
    key = _F1SCHEDULE_CACHE_KEY_TEMPLATE.format(year=year)
    try:
        get_cache().set_entry("json", key, {"fetched_at": fetched_at, "schedule": raw_year})
    except (CacheError, OSError) as exc:
        logger.debug("Schedule cache write failed for %s: %s", key, exc)


def _fetch_f1schedule_year(year: int) -> dict[str, Any] | None:
    """Fetch ``schedule_{year}.json`` from the f1schedule repository.

    Returns the converted year payload, ``{}`` when the file does not exist
    upstream (a season without a published calendar) and ``None`` when the
    fetch failed, so the caller can fall back to cached or vendored data.
    """
    url = _F1SCHEDULE_URL_TEMPLATE.format(year=year)
    try:
        raw_year = get_url_loader().get_url(url, timeout=_F1SCHEDULE_FETCH_TIMEOUT)
    except DataNotFoundError:
        return {}
    except (NetworkError, InvalidDataError) as exc:
        logger.warning("Failed to fetch schedule_%s.json from %s: %s", year, url, exc)
        return None
    try:
        payload = _convert_f1schedule_year(raw_year, year)
    except InvalidDataError as exc:
        logger.warning("Ignoring malformed schedule_%s.json from %s: %s", year, url, exc)
        return None
    _write_cached_f1schedule_year(year, raw_year, time.time())
    return payload


_refreshing_years: set[int] = set()
_refresh_threads: dict[int, threading.Thread] = {}
_schedule_generation: dict[int, int] = {}


def _join_schedule_refreshes(timeout: float | None = None) -> None:
    """Wait for in-flight background calendar refreshes (tests and shutdown hooks)."""
    with _year_schedules_lock:
        threads = list(_refresh_threads.values())
    for thread in threads:
        thread.join(timeout)


def _refresh_schedule_in_background(year: int) -> None:
    """Re-fetch a stale cached calendar in a daemon thread (one per season at a time).

    Thebes TH7: the first ``get_session`` after the TTL expired used to block
    on the GitHub fetch (hundreds of milliseconds to seconds); the stale row
    is served immediately instead and the memo expires shortly after the
    refresh lands.
    """
    with _year_schedules_lock:
        if year in _refreshing_years:
            return
        _refreshing_years.add(year)

    def _run() -> None:
        try:
            if _fetch_f1schedule_year(year) is not None:
                with _year_schedules_lock:
                    # Next lookup re-reads the fresh SQLite row, whether the
                    # stale record is already memoized or still being built.
                    _schedule_generation[year] = _schedule_generation.get(year, 0) + 1
        finally:
            with _year_schedules_lock:
                _refreshing_years.discard(year)
                _refresh_threads.pop(year, None)

    if not THREADS_AVAILABLE:
        # Pyodide: no background thread; one small fetch refreshes the row now.
        _run()
        return
    thread = threading.Thread(target=_run, name=f"tif1-schedule-refresh-{year}", daemon=True)
    with _year_schedules_lock:
        _refresh_threads[year] = thread
    thread.start()


def _load_live_year_payload(year: int, now: float) -> tuple[dict[str, Any], float]:
    """Resolve a season that may still change: cached row first, live source, fallbacks.

    Order: SQLite row younger than ``schedule_ttl_hours`` -> stale SQLite row
    served immediately while a background thread refreshes it -> live fetch
    (no usable row) -> vendored file -> empty. Returns the payload and the
    memo expiry.
    """
    ttl = _schedule_ttl_seconds()
    offline = bool(get_config().get("offline_mode", False))
    cached = _read_cached_f1schedule_year(year)
    cached_payload: dict[str, Any] | None = None
    fetched_at = 0.0
    if cached is not None:
        fetched_at = cached[0]
        try:
            cached_payload = _convert_f1schedule_year(cached[1], year)
        except InvalidDataError as exc:
            logger.debug("Discarding malformed cached schedule_%s.json: %s", year, exc)
    if cached_payload is not None:
        if now - fetched_at < ttl:
            return cached_payload, fetched_at + ttl
        if offline:
            return cached_payload, now + _F1SCHEDULE_RETRY_AFTER_SECONDS
        # Stale-while-revalidate: answer from the stale row now, refresh it
        # off the critical path, and re-check the row again soon.
        _refresh_schedule_in_background(year)
        return cached_payload, now + _F1SCHEDULE_STALE_GRACE_SECONDS

    if not offline:
        fetched = _fetch_f1schedule_year(year)
        if fetched is not None:
            return fetched, now + ttl

    vendored = _load_schedule_payload()["years"].get(str(year), {})
    return vendored, now + _F1SCHEDULE_RETRY_AFTER_SECONDS


def _build_year_schedule(year: int, now: float) -> _YearSchedule:
    if not _is_current_or_future_season(year):
        vendored = _load_schedule_payload()["years"].get(str(year))
        if isinstance(vendored, dict) and vendored:
            return _YearSchedule(vendored, tuple(vendored.get("events", ())), float("inf"))
    with _year_schedules_lock:
        generation = _schedule_generation.get(year, 0)
    payload, expires_at = _load_live_year_payload(year, now)
    events = payload.get("events", [])
    return _YearSchedule(
        payload,
        tuple(events) if isinstance(events, list) else (),
        expires_at,
        generation=generation,
    )


def _get_f1schedule_year_schedule(year: int) -> _YearSchedule:
    """Return the memoized f1schedule season, refreshing a live season past its TTL."""
    now = time.time()
    record = _year_schedules.get(year)
    if record is not None and record.expires_at > now and _is_current_generation(record, year):
        return record
    fresh = _build_year_schedule(year, now)
    with _year_schedules_lock:
        current = _year_schedules.get(year)
        if (
            current is not None
            and current.expires_at > now
            and current.generation == _schedule_generation.get(year, 0)
        ):
            # Another thread refreshed while this one fetched.
            return current
        if current is not None and current.payload == fresh.payload:
            # Unchanged upstream: keep the memoized session lookups.
            current.expires_at = fresh.expires_at
            current.generation = fresh.generation
            return current
        _year_schedules[year] = fresh
    return fresh


def _is_current_generation(record: _YearSchedule, year: int) -> bool:
    with _year_schedules_lock:
        return record.generation == _schedule_generation.get(year, 0)


def _resolve_year_payload(year: int, backend: ScheduleBackend | None = None) -> dict[str, Any]:
    return _get_year_schedule(year, backend).payload


# -- schedule backends ---------------------------------------------------------

_ERGAST_SESSION_KEYS = {
    "Practice 1": "FirstPractice",
    "Practice 2": "SecondPractice",
    "Practice 3": "ThirdPractice",
    "Qualifying": "Qualifying",
    "Sprint": "Sprint",
    "Sprint Qualifying": "SprintQualifying",
    "Sprint Shootout": "SprintShootout",
}
_ERGAST_CIRCUIT_KEYS = (
    "CircuitId",
    "CircuitName",
    "CircuitLatitude",
    "CircuitLongitude",
    "EventUrl",
    "CircuitUrl",
)
_EMPTY_CIRCUIT_EXTRAS: dict[str, Any] = {
    "CircuitId": "",
    "CircuitName": "",
    "CircuitLatitude": None,
    "CircuitLongitude": None,
    "EventUrl": "",
    "CircuitUrl": "",
}
_ergast_year_schedules: dict[int, _YearSchedule] = {}
_ergast_vendored_years: dict[int, list[dict[str, Any]] | None] = {}


def _effective_backend(year: int, backend: ScheduleBackend | None) -> str:
    """``"f1schedule"`` or ``"ergast"`` for a fastf1-style backend name.

    ``None`` means the f1schedule calendar from :data:`F1_API_FIRST_YEAR` (with
    Ergast as fallback when it has no season) and Ergast before it;
    ``"fastf1"`` and ``"f1timing"`` both name the f1schedule calendar (tif1 has
    no live-timing backend), ``"ergast"`` the Ergast race table.
    """
    if backend is None:
        return "f1schedule" if year >= F1_API_FIRST_YEAR else "ergast"
    if backend in ("fastf1", "f1timing"):
        return "f1schedule"
    if backend == "ergast":
        return "ergast"
    raise ValueError(f"Unknown schedule backend {backend!r}; use 'fastf1', 'f1timing' or 'ergast'")


def _normalize_backend(
    backend: ScheduleBackend | None, force_ergast: bool
) -> ScheduleBackend | None:
    if force_ergast:
        warnings.warn(
            "Option ``force_ergast`` has been deprecated, use ``backend='ergast'`` instead",
            DeprecationWarning,
            stacklevel=3,
        )
        return "ergast"
    return backend


def _get_year_schedule(year: int, backend: ScheduleBackend | None = None) -> _YearSchedule:
    """The memoized season schedule of ``backend`` (see :func:`_effective_backend`)."""
    effective = _effective_backend(year, backend)
    if effective == "ergast":
        return _get_ergast_year_schedule(year)
    record = _get_f1schedule_year_schedule(year)
    if backend is None and not record.events:
        # No f1schedule calendar for the season (not published yet): Ergast
        # knows the race table.
        ergast_record = _get_ergast_year_schedule(year)
        if ergast_record.events:
            return ergast_record
    return record


def _load_vendored_ergast_year(year: int) -> list[dict[str, Any]] | None:
    """The packaged compact Ergast race list of ``year`` (None when not vendored)."""
    if year in _ergast_vendored_years:
        return _ergast_vendored_years[year]
    path = files("tif1").joinpath(f"data/schedules/ergast/schedule_{year}.json")
    races: list[dict[str, Any]] | None = None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        candidate = payload.get("races") if isinstance(payload, dict) else None
        if isinstance(candidate, list):
            races = candidate
    except (FileNotFoundError, OSError, ValueError):
        races = None
    _ergast_vendored_years[year] = races
    return races


def _fetch_live_ergast_year(year: int) -> list[dict[str, Any]] | None:
    """The season's ``events.json`` from the Stats repository (None when unavailable)."""
    from .ergast.stats import get_stats_source

    if year < MIN_YEAR:
        return None
    try:
        return get_stats_source().races(year, fast=True)
    except (CacheError, DataNotFoundError, InvalidDataError, NetworkError) as exc:
        logger.warning("Failed to fetch the Ergast schedule for %s: %s", year, exc)
        return None


def _build_ergast_year_schedule(year: int, now: float) -> _YearSchedule:
    races: list[dict[str, Any]] | None
    offline = bool(get_config().get("offline_mode", False))
    if _is_current_or_future_season(year):
        races = None if offline else _fetch_live_ergast_year(year)
        expires_at = now + _schedule_ttl_seconds()
        if races is None:
            races = _load_vendored_ergast_year(year)
            expires_at = now + _F1SCHEDULE_RETRY_AFTER_SECONDS
    else:
        races = _load_vendored_ergast_year(year)
        expires_at = math.inf
        if races is None and not offline:
            races = _fetch_live_ergast_year(year)
            expires_at = now + _schedule_ttl_seconds()
    payload = _convert_ergast_year(races or [], year)
    return _YearSchedule(payload, tuple(payload["events"]), expires_at)


def _get_ergast_year_schedule(year: int) -> _YearSchedule:
    now = time.time()
    record = _ergast_year_schedules.get(year)
    if record is not None and record.expires_at > now:
        return record
    fresh = _build_ergast_year_schedule(year, now)
    with _year_schedules_lock:
        _ergast_year_schedules[year] = fresh
    return fresh


def _ergast_iso(block: Any) -> str | None:
    """``YYYY-MM-DDTHH:MM:SS`` (UTC, no suffix) from an Ergast ``{date, time}`` block."""
    if not isinstance(block, dict):
        return None
    date = block.get("date")
    if not isinstance(date, str) or not date:
        return None
    time_part = block.get("time")
    if isinstance(time_part, str) and time_part:
        return f"{date}T{time_part.replace('Z', '')}"
    return f"{date}T00:00:00"


def _ergast_session_layout(race: dict[str, Any], year: int) -> tuple[str, list[str]]:
    """``(EventFormat, session names)`` of a race as fastf1's Ergast backend names them."""
    if "Sprint" in race:
        if year in (2021, 2022):
            return "sprint", ["Practice 1", "Qualifying", "Practice 2", "Sprint", "Race"]
        if year == 2023:
            return "sprint_shootout", [
                "Practice 1",
                "Qualifying",
                "Sprint Shootout",
                "Sprint",
                "Race",
            ]
        return "sprint_qualifying", [
            "Practice 1",
            "Sprint Qualifying",
            "Sprint",
            "Qualifying",
            "Race",
        ]
    return "conventional", ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _ergast_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _convert_ergast_year(races: list[dict[str, Any]], year: int) -> dict[str, Any]:
    """The schedule payload (events, sessions, metadata) of an Ergast race list.

    Session names follow fastf1's Ergast backend; session dates come from the
    real Jolpica session keys when present and fastf1's day-offset estimates
    otherwise (two days before the race for sessions 1-2, one day for 3-4).
    The session list and the ``Session1..5`` columns always agree, so
    ``Event.get_session_name(i)`` is ``Session{i}`` for every season; which
    of them a season before :data:`F1_API_FIRST_YEAR` can load is decided
    by :func:`tif1.get_session` (:data:`ERGAST_LOADABLE_SESSIONS`).
    A race whose ``round`` is not a number is skipped, never the season.
    """
    numbered: list[tuple[int, dict[str, Any]]] = []
    for race in races:
        if not isinstance(race, dict):
            continue
        try:
            round_number = int(race.get("round", 0))
        except (TypeError, ValueError):
            continue
        numbered.append((round_number, race))
    numbered.sort(key=lambda item: item[0])
    events: list[str] = []
    sessions: dict[str, list[str]] = {}
    metadata: dict[str, dict[str, Any]] = {}
    for round_number, race in numbered:
        name = str(race.get("raceName", "")).strip()
        if not name:
            continue
        event_format, names = _ergast_session_layout(race, year)
        race_iso = _ergast_iso(race)
        race_date = datetime.fromisoformat(race_iso) if race_iso else None
        circuit = _as_dict(race.get("Circuit"))
        location = _as_dict(circuit.get("Location"))
        row: dict[str, Any] = {
            "RoundNumber": round_number,
            "EventDate": race.get("date"),
            "Location": location.get("locality", ""),
            "Country": location.get("country", ""),
            "OfficialEventName": "",
            "EventFormat": event_format,
            "GmtOffset": None,
            "F1ApiSupport": year >= F1_API_FIRST_YEAR,
        }
        for index, session_name in enumerate(names, 1):
            row[f"Session{index}"] = session_name
            row[f"Session{index}Date"] = None
            key = _ERGAST_SESSION_KEYS.get(session_name)
            if session_name == "Race":
                iso = race_iso
            else:
                iso = _ergast_iso(race.get(key)) if key else None
                if iso is None and race_date is not None:
                    days = 2 if index <= 2 else 1
                    midnight = race_date.replace(hour=0, minute=0, second=0)
                    iso = (midnight - timedelta(days=days)).isoformat()
            row[f"Session{index}DateUtc"] = iso
        row.update(
            {
                "CircuitId": circuit.get("circuitId", ""),
                "CircuitName": circuit.get("circuitName", ""),
                "CircuitLatitude": _ergast_float(location.get("lat")),
                "CircuitLongitude": _ergast_float(location.get("long")),
                "EventUrl": race.get("url", ""),
                "CircuitUrl": circuit.get("url", ""),
            }
        )
        events.append(name)
        metadata[name] = row
        sessions[name] = list(names)
    return {"events": events, "sessions": sessions, "metadata": metadata}


@lru_cache(maxsize=128)
def _ergast_year_metadata(year: int) -> dict[str, dict[str, Any]]:
    """Per-event metadata of the packaged Ergast schedule (offline only)."""
    races = _load_vendored_ergast_year(year) or []
    return _convert_ergast_year(races, year)["metadata"]


def _ergast_circuit_extras(year: int, round_number: Any) -> dict[str, Any]:
    """Circuit columns of a round from the packaged Ergast schedule (never a request)."""
    try:
        number = int(round_number)
    except (TypeError, ValueError):
        return {}
    if number <= 0:
        return {}
    for row in _ergast_year_metadata(year).values():
        if row.get("RoundNumber") == number:
            return {key: row[key] for key in _ERGAST_CIRCUIT_KEYS}
    return {}


def _clear_schedule_memo() -> None:
    """Drop the per-process schedule memo (tests and explicit refreshes).

    In-flight background refreshes are joined first so a refresh started by
    an earlier lookup cannot land in (or block) the next one.
    """
    _join_schedule_refreshes(timeout=5.0)
    with _year_schedules_lock:
        _year_schedules.clear()
        _ergast_year_schedules.clear()
        _refreshing_years.clear()
        _refresh_threads.clear()
        _schedule_generation.clear()


@lru_cache(maxsize=1)
def _load_schedule_payload() -> dict[str, Any]:
    vendored_years = _load_vendored_f1schedule_years()
    payload = {"schema_version": 1, "years": vendored_years}
    return validate_schedule_payload(payload)


def _build_events_for_year(year: int, backend: ScheduleBackend | None = None) -> list[str]:
    """Get list of events for a given year."""
    return list(_get_year_schedule(year, backend).events)


def _build_sessions_for_event(
    year: int, event: str, backend: ScheduleBackend | None = None
) -> list[str]:
    """Get list of sessions for a given year and event.

    ``event`` is resolved like ``get_event_by_name`` (exact name, then alias,
    then fuzzy match), so ``get_sessions(2025, "Spielberg")`` answers for the
    Austrian Grand Prix instead of the generic five-session weekend. A name
    that matches no event of the season raises ``EventNotFoundError``
    (``EventNotInSeasonError`` for an alias of an event not held that year).
    """
    standard = ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]
    year_payload = _resolve_year_payload(year, backend)
    sessions = year_payload.get("sessions", {})
    if not isinstance(sessions, dict):
        return standard
    event_sessions = sessions.get(event)
    if event_sessions is None and event and _get_events_cached(year, backend):
        # Raises for a name that matches no event, as get_session does: the
        # generic five sessions would describe a race that does not exist.
        event_sessions = sessions.get(_resolve_event_name(year, event, backend=backend))
    if not isinstance(event_sessions, list) or not event_sessions:
        return standard
    return list(event_sessions)


def _get_events_cached(year: int, backend: ScheduleBackend | None = None) -> tuple[str, ...]:
    """Get the season's event names as an immutable tuple (memoized per season)."""
    return _get_year_schedule(year, backend).events


def get_events(year: int, *, backend: ScheduleBackend | None = None) -> EventSchedule:
    """Get event schedule for a given year."""
    return get_event_schedule(year, backend=backend)


def _get_sessions_cached(
    year: int, event: str, backend: ScheduleBackend | None = None
) -> tuple[str, ...]:
    """Get the resolved session list as an immutable tuple (memoized per season)."""
    record = _get_year_schedule(year, backend)
    sessions = record.sessions.get(event)
    if sessions is None:
        sessions = tuple(_build_sessions_for_event(year, event, backend))
        record.sessions[event] = sessions
    return sessions


def get_sessions(year: int, event: str, *, backend: ScheduleBackend | None = None) -> list[str]:
    """Get list of sessions for a given year and event.

    Args:
        year: Championship year.
        event: Event name (resolved like :func:`get_event_by_name`).
        backend: Schedule backend (see :func:`get_event_schedule`).
    """
    return list(_get_sessions_cached(year, event, backend))


def _get_event_for_year(
    year: int, event_name: str, backend: ScheduleBackend | None = None
) -> Event:
    year_payload = _resolve_year_payload(year, backend)
    metadata_dict = year_payload.get("metadata", {})
    metadata = metadata_dict.get(event_name) if isinstance(metadata_dict, dict) else None
    return _create_event(year, event_name, metadata, backend)


def _create_event(
    year: int, event_name: str, metadata: Any, backend: ScheduleBackend | None = None
) -> Event:
    from .event_frames import Event

    event_factory = cast(Any, Event)
    event_metadata = dict(metadata) if isinstance(metadata, dict) else None
    if event_metadata is not None and _effective_backend(year, backend) == "f1schedule":
        # Circuit columns (id, name, coordinates, URLs) from the packaged
        # Ergast schedule, joined by round; never a network request. Rows
        # without a round (testing) keep empty values so the columns exist.
        extras = dict(_EMPTY_CIRCUIT_EXTRAS)
        extras.update(_ergast_circuit_extras(year, event_metadata.get("RoundNumber")))
        for key, value in extras.items():
            event_metadata.setdefault(key, value)
    return cast(Event, event_factory(year, event_name, event_metadata, backend=backend))


# Lowest fuzzy score (rapidfuzz ratio, 0-100) that still counts as a match.
# Typos of real events score 75-97 ("Sao Paolo" 75, "Monacco" 92); names unlike
# every event score 67 or less ("Moon GP", "Paris", "Invalid GP" 57).
_FUZZY_MIN_SCORE = 70.0

_ALIAS_NOISE_RE = re.compile(r"\b(?:grand prix|gp|formula 1|f1)\b")


def _event_alias_key(name: str) -> str:
    """Normalise an event query for :data:`EVENT_NAME_ALIASES` lookup.

    Folds accents ("Montréal" -> "montreal"), lower-cases, turns punctuation
    into spaces and drops "Grand Prix"/"GP"/"Formula 1" so "Spa GP" and
    "Spa-Francorchamps" find their keys.
    """
    folded = "".join(
        ch for ch in unicodedata.normalize("NFKD", name) if not unicodedata.combining(ch)
    ).casefold()
    words = re.sub(r"[^0-9a-z]+", " ", folded)
    return " ".join(_ALIAS_NOISE_RE.sub(" ", words).split())


def _event_alias_targets(name: str) -> tuple[str, ...]:
    """Event names a circuit/city/country alias refers to (empty if not an alias)."""
    return EVENT_NAME_ALIASES.get(_event_alias_key(name), ())


def _find_event_by_name(
    year: int,
    event_names: list[str],
    name: str,
    exact_match: bool = False,
    backend: ScheduleBackend | None = None,
) -> Event | None:
    matched = _match_event_name(year, event_names, name, exact_match=exact_match)
    return None if matched is None else _get_event_for_year(year, matched, backend)


def _match_event_name(
    year: int, event_names: list[str], name: str, exact_match: bool = False
) -> str | None:
    """Name of the event ``name`` refers to among ``event_names`` (``None`` if none).

    Resolution only: no :class:`Event` (a pandas Series) is built, so session
    lookups by alias or near-miss name stay pandas-free.
    """
    year_payload = _resolve_year_payload(year)
    metadata_dict = year_payload.get("metadata", {})

    if exact_match:
        query = name.lower()
        for event_name in event_names:
            if event_name.lower() == query:
                return event_name
        return None

    if not event_names:
        return None

    # Exact (case-insensitive) names are the common call pattern; returning
    # them directly skips the fuzzy machinery *and* its rapidfuzz import
    # (~14 ms on the first resolve of a process). The fuzzy path returns the
    # same event for exact names (ratio 100 -> exact=True, no warning), so
    # behavior is identical; near-miss names still take the fuzzy route.
    folded = name.casefold()
    for event_name in event_names:
        if event_name.casefold() == folded:
            return event_name

    # Circuit/city/country aliases ("Spa", "Interlagos", "USA") resolve
    # directly: fuzzy matching sends several of them to the wrong race. An
    # alias whose event is not on this season's calendar returns None rather
    # than silently loading a different race.
    alias_targets = _event_alias_targets(name)
    if alias_targets:
        for target in alias_targets:
            if target in event_names:
                return target
        logger.info(
            "'%s' refers to the %s, which is not on the %s calendar", name, alias_targets[0], year
        )
        return None

    from .fuzzy import _fuzzy_match

    def _remove_common_words(event_name: str) -> str:
        common_words = ["formula 1", str(year), "grand prix", "gp"]
        event_name = event_name.casefold()
        for word in common_words:
            event_name = event_name.replace(word, "")
        return event_name.strip()

    def _matcher_strings(event_name: str, metadata: dict[str, Any]) -> list[str]:
        strings = []
        if metadata.get("Location"):
            strings.append(metadata["Location"].casefold())
        if metadata.get("Country"):
            strings.append(metadata["Country"].casefold())
        # The schedule metadata carries no EventName key, so the event's own
        # name ("british") was never a feature: "Britain" then scored
        # "Bahrain" highest. Always match against the name itself.
        strings.append(_remove_common_words(str(metadata.get("EventName") or event_name)))
        if metadata.get("OfficialEventName"):
            strings.append(_remove_common_words(metadata["OfficialEventName"]))
        return strings

    user_input = name
    name = _remove_common_words(name)
    reference = [
        _matcher_strings(
            event_name,
            metadata_dict.get(event_name) or {} if isinstance(metadata_dict, dict) else {},
        )
        for event_name in event_names
    ]
    index, exact, score = _fuzzy_match(name, reference)
    matched_event_name = event_names[index]

    if score < _FUZZY_MIN_SCORE:
        # Resembles no event: loading the least-bad match would be a wrong race.
        logger.info(
            "'%s' matches no %s event (closest: '%s', score %.0f)",
            user_input,
            year,
            matched_event_name,
            score,
        )
        return None

    if not exact:
        logger.warning(f"Correcting user input '{user_input}' to '{matched_event_name}'")

    return matched_event_name


def get_event(
    year: int,
    gp: int | str,
    exact_match: bool = False,
    *,
    backend: ScheduleBackend | None = None,
    force_ergast: bool = False,
    strict_search: bool = False,
) -> Event | None:
    """Get event by round number or name.

    Args:
        year: Championship year.
        gp: Round number or event name (fuzzy-matched unless ``exact_match``).
        exact_match: Require an exact name match (``strict_search`` is
            fastf1's name for the same switch).
        backend: Schedule backend (see :func:`get_event_schedule`).
        force_ergast: Deprecated alias of ``backend="ergast"``.
        strict_search: Same as ``exact_match``.
    """
    backend = _normalize_backend(backend, force_ergast)
    exact_match = exact_match or strict_search
    schedule = get_event_schedule(year, backend=backend)
    if isinstance(gp, int):
        return schedule.get_event_by_round(gp)

    event = schedule.get_event_by_name(gp, strict_search=exact_match)
    if event is not None:
        return event

    if not exact_match:
        try:
            return schedule.get_event_by_round(int(gp))
        except (TypeError, ValueError):
            return None
    return None


def get_event_by_round(
    year: int, round_number: int, *, backend: ScheduleBackend | None = None
) -> Event:
    """Get event by round number."""
    return get_event_schedule(year, backend=backend).get_event_by_round(round_number)


def get_event_by_name(
    year: int, name: str, exact_match: bool = False, *, backend: ScheduleBackend | None = None
) -> Event:
    """Get event by name with optional fuzzy matching.

    A fuzzy match is performed to find the event that best matches the
    given name. Fuzzy matching is performed using the country, location,
    name and officialName of each event.

    Args:
        year: Championship year.
        name: The name of the event. For example,
            ``get_event_by_name(2024, "british")`` and
            ``get_event_by_name(2024, "silverstone")`` will both return the
            event for the British Grand Prix.
        exact_match: Search only for exact query matches instead of
            using fuzzy search.
        backend: Schedule backend (see :func:`get_event_schedule`).
    """
    return _get_event_for_year(
        year, _resolve_event_name(year, name, exact_match=exact_match, backend=backend), backend
    )


def _resolve_event_name(
    year: int, name: str, exact_match: bool = False, backend: ScheduleBackend | None = None
) -> str:
    """Season event name ``name`` resolves to; raises like :func:`get_event_by_name`."""
    events = list(_get_events_cached(year, backend))
    event = _match_event_name(year, events, name, exact_match=exact_match)
    if event is None:
        alias_targets = () if exact_match else _event_alias_targets(name)
        if alias_targets:
            raise EventNotInSeasonError(
                f"'{name}' refers to the {alias_targets[0]}, which is not on the {year} calendar"
            )
        if not events:
            raise ValueError(f"No events found for {year}")
        if exact_match:
            raise EventNotFoundError(f"No exact match found for event '{name}' in {year}")
        raise EventNotFoundError(f"No event matching '{name}' in {year}")
    return event


def get_event_schedule(
    year: int,
    include_testing: bool = True,
    *,
    backend: ScheduleBackend | None = None,
    force_ergast: bool = False,
) -> EventSchedule:
    """Return FastF1-compatible event schedule for a specific season.

    Args:
        year: Championship year (1950 onward).
        include_testing: Keep pre-season testing events (f1schedule only).
        backend: ``"fastf1"`` or ``"f1timing"`` read the f1schedule calendar
            (2018 onward, local session times); ``"ergast"`` builds the
            schedule from the Ergast race table (1950 onward, UTC times,
            estimated practice times for old seasons). ``None`` uses
            f1schedule from 2018 (Ergast when it has no season yet) and
            Ergast before.
        force_ergast: Deprecated alias of ``backend="ergast"``.
    """
    import pandas as pd

    from .event_frames import EventSchedule

    backend = _normalize_backend(backend, force_ergast)
    year_payload = _resolve_year_payload(year, backend)
    metadata_dict = year_payload.get("metadata", {})
    events = list(_get_events_cached(year, backend))

    rows: list[dict[str, Any]] = []
    for event_name in events:
        metadata = metadata_dict.get(event_name) if isinstance(metadata_dict, dict) else None
        event = _create_event(year, event_name, metadata, backend)
        rows.append(cast(dict[str, Any], event.to_dict()))

    schedule = EventSchedule(rows)
    schedule.year = year
    schedule.backend = backend
    if include_testing or "RoundNumber" not in schedule.columns:
        return schedule

    rounds = pd.to_numeric(schedule["RoundNumber"], errors="coerce")
    filtered = schedule.loc[rounds > 0].copy()
    filtered.year = year
    filtered.backend = backend
    return filtered


def get_events_remaining(
    dt: datetime | None = None,
    *,
    include_testing: bool = True,
    backend: ScheduleBackend | None = None,
    force_ergast: bool = False,
) -> EventSchedule:
    """Events of the season of ``dt`` (default: now, UTC) that have not finished yet.

    An event is kept while its last session (the third session for testing
    events) starts after ``dt``.

    Args:
        dt: Reference time (naive UTC). Defaults to the current UTC time.
        include_testing: Keep pre-season testing events.
        backend: Schedule backend (see :func:`get_event_schedule`).
        force_ergast: Deprecated alias of ``backend="ergast"``.
    """
    import pandas as pd

    if dt is None:
        dt = datetime.now(UTC).replace(tzinfo=None)
    events = get_event_schedule(
        dt.year, include_testing=include_testing, backend=backend, force_ergast=force_ergast
    )
    if events.empty:
        return events
    if not include_testing:
        # An event without a race time (no UTC offset parsed, or none
        # published yet) is not over: keep it, as the loop below does.
        race_dates = pd.to_datetime(events["Session5DateUtc"])
        result = events.loc[race_dates.isna() | (race_dates > dt)]
        result.year = events.year
        result.backend = events.backend
        return result

    indexes_to_drop = []
    for idx, event in events.iterrows():
        testing = str(event.get("EventFormat", "")) == "testing"
        column = "Session3DateUtc" if testing else "Session5DateUtc"
        session_date = pd.Timestamp(event[column]) if column in event.index else pd.NaT
        if not pd.isna(session_date) and session_date <= dt:
            indexes_to_drop.append(idx)
    result = events.drop(index=indexes_to_drop)
    result.year = events.year
    result.backend = events.backend
    return result


def get_testing_event(
    year: int, test_number: int, *, backend: ScheduleBackend | None = None
) -> Event:
    """The ``test_number``-th pre-season testing event of ``year`` (1-based).

    Raises:
        ValueError: ``backend="ergast"`` (Ergast has no testing events) or no such test.
    """
    if backend == "ergast":
        raise ValueError("The 'ergast' backend does not support testing events.")
    schedule = get_event_schedule(year, include_testing=True, backend=backend)
    if schedule.empty or "EventFormat" not in schedule.columns:
        raise ValueError(f"Testing event number {test_number} does not exist in {year}")
    testing = schedule.loc[schedule["EventFormat"] == "testing"]
    if "EventDate" in testing.columns:
        testing = testing.sort_values("EventDate", kind="stable")
    if test_number < 1 or test_number > len(testing):
        raise ValueError(f"Testing event number {test_number} does not exist in {year}")
    event_name = str(testing.iloc[test_number - 1]["EventName"])
    return _get_event_for_year(year, event_name, backend)


def get_testing_session(
    year: int, test_number: int, session_number: int, *, backend: ScheduleBackend | None = None
):
    """Session ``session_number`` (1-based) of a pre-season testing event."""
    event = get_testing_event(year, test_number, backend=backend)
    return event.get_session(session_number)
