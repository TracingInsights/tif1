"""Event and session information for F1 seasons."""

from __future__ import annotations

import json
import logging
import re
import threading
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from importlib.resources import files
from typing import Any, ClassVar, Self, cast

import pandas as pd

from .cache import get_cache
from .config import get_config
from .core_utils.constants import EVENT_NAME_ALIASES
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


def _get_year_schedule(year: int) -> _YearSchedule:
    """Return the memoized season schedule, refreshing a live season past its TTL."""
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


def _resolve_year_payload(year: int) -> dict[str, Any]:
    return _get_year_schedule(year).payload


def _clear_schedule_memo() -> None:
    """Drop the per-process schedule memo (tests and explicit refreshes).

    In-flight background refreshes are joined first so a refresh started by
    an earlier lookup cannot land in (or block) the next one.
    """
    _join_schedule_refreshes(timeout=5.0)
    with _year_schedules_lock:
        _year_schedules.clear()
        _refreshing_years.clear()
        _refresh_threads.clear()
        _schedule_generation.clear()


@lru_cache(maxsize=1)
def _load_schedule_payload() -> dict[str, Any]:
    vendored_years = _load_vendored_f1schedule_years()
    payload = {"schema_version": 1, "years": vendored_years}
    return validate_schedule_payload(payload)


def _build_events_for_year(year: int) -> list[str]:
    """Get list of events for a given year."""
    return list(_get_year_schedule(year).events)


def _build_sessions_for_event(year: int, event: str) -> list[str]:
    """Get list of sessions for a given year and event.

    ``event`` is resolved like ``get_event_by_name`` (exact name, then alias,
    then fuzzy match), so ``get_sessions(2025, "Spielberg")`` answers for the
    Austrian Grand Prix instead of the generic five-session weekend. A name
    that matches no event of the season raises ``EventNotFoundError``
    (``EventNotInSeasonError`` for an alias of an event not held that year).
    """
    standard = ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]
    year_payload = _resolve_year_payload(year)
    sessions = year_payload.get("sessions", {})
    if not isinstance(sessions, dict):
        return standard
    event_sessions = sessions.get(event)
    if event_sessions is None and event and _get_events_cached(year):
        # Raises for a name that matches no event, as get_session does: the
        # generic five sessions would describe a race that does not exist.
        resolved = get_event_by_name(year, event)
        event_sessions = sessions.get(str(resolved.EventName))
    if not isinstance(event_sessions, list) or not event_sessions:
        return standard
    return list(event_sessions)


def _get_events_cached(year: int) -> tuple[str, ...]:
    """Get the season's event names as an immutable tuple (memoized per season)."""
    return _get_year_schedule(year).events


def get_events(year: int) -> EventSchedule:
    """Get event schedule for a given year."""
    return get_event_schedule(year)


def _get_sessions_cached(year: int, event: str) -> tuple[str, ...]:
    """Get the resolved session list as an immutable tuple (memoized per season)."""
    record = _get_year_schedule(year)
    sessions = record.sessions.get(event)
    if sessions is None:
        sessions = tuple(_build_sessions_for_event(year, event))
        record.sessions[event] = sessions
    return sessions


def get_sessions(year: int, event: str) -> list[str]:
    """Get list of sessions for a given year and event."""
    return list(_get_sessions_cached(year, event))


class Event(pd.Series):
    _metadata: ClassVar[list[str]] = ["_name", "_year", "_event_name"]
    _year: int
    _event_name: str

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:  # noqa: ARG004
        """Allow Series-style subclass construction without re-implementing __new__."""
        return cast(Self, super().__new__(cls))

    def __init__(self, year: int, name: str, metadata: dict | None = None):
        from datetime import timezone as tz

        sessions = get_sessions(year, name)
        data = {
            "RoundNumber": 0,
            "Country": "",
            "Location": self._derive_location(name),
            "OfficialEventName": name,
            "EventDate": pd.NaT,
            "EventName": name,
            "EventFormat": "conventional",
        }

        for i, session in enumerate(sessions, 1):
            data[f"Session{i}"] = session
            data[f"Session{i}Date"] = None
            data[f"Session{i}DateUtc"] = pd.NaT

        data["F1ApiSupport"] = True

        if metadata:
            gmt_offset = _parse_gmt_offset(metadata.get("GmtOffset"))
            for key, value in metadata.items():
                if key == "EventDate" and value:
                    data[key] = pd.Timestamp(value.split("T")[0])
                elif key.endswith("DateUtc") and value:
                    data[key] = pd.Timestamp(value)
                elif key.endswith("Date") and value and gmt_offset is not None:
                    # Local track time, made tz-aware with the event's GMT offset
                    data[key] = datetime.fromisoformat(value).replace(tzinfo=tz(gmt_offset))
                else:
                    data[key] = value

        super().__init__(data=data, name=data.get("RoundNumber", 0))  # type: ignore[ty:unknown-argument]
        self._year = year
        self._event_name = name

    def _reconstruct(self, data=None, *args, **kwargs):
        """Reconstruct an Event Series without invoking schedule lookup."""
        result = object.__new__(type(self))
        cast(Any, pd.Series).__init__(result, data, *args, **kwargs)
        result._year = self._year
        result._event_name = self._event_name
        return result

    @property
    def _constructor(self):
        return self._reconstruct

    @staticmethod
    def _derive_location(name: str) -> str:
        if not isinstance(name, str):
            return ""
        stripped = name.replace("Grand Prix", "").strip()
        return stripped if stripped else name

    @property
    def year(self):
        return self._year

    def get_session_name(self, identifier: int | str) -> str:
        """Return a full session name for a session identifier.

        Args:
            identifier: Session identifier as number, abbreviation (for example
                ``"FP1"`` or ``"Q"``), or full/partial session name.

        Returns:
            Canonical session name for this event.

        Raises:
            ValueError: If the identifier is invalid or the resolved session
                does not exist for this event.
        """
        available_sessions = get_sessions(self._year, self._event_name)

        try:
            session_num = float(identifier)
        except (TypeError, ValueError):
            if not isinstance(identifier, str):
                raise ValueError(f"Invalid session type '{identifier}'") from None

            session_name = _SESSION_TYPES_BY_CASEFOLD.get(identifier.casefold())
            if session_name is None:
                try:
                    session_name = _SESSION_TYPE_ABBREVIATIONS[identifier.upper()]
                except KeyError:
                    raise ValueError(f"Invalid session type '{identifier}'") from None

            # Backward compatibility for older sprint weekends.
            if session_name == "Sprint Qualifying" and self.year in (2021, 2022):
                session_name = "Sprint"

            if session_name not in available_sessions:
                raise ValueError(f"Session type '{identifier}' does not exist for this event")

            return session_name

        if not session_num.is_integer() or int(session_num) not in (1, 2, 3, 4, 5):
            raise ValueError(f"Invalid session type '{session_num}'")

        session_index = int(session_num) - 1
        if session_index >= len(available_sessions):
            raise ValueError(f"Session number {int(session_num)} does not exist for this event")

        return available_sessions[session_index]

    def get_session_date(self, identifier: int | str, utc: bool = False) -> pd.Timestamp:
        """Return date and time of a specific session from this event.

        Args:
            identifier: Session name, abbreviation or number.
            utc: If True, return non-timezone-aware UTC timestamp.

        Raises:
            ValueError: If there is no matching session, identifier is invalid
                or local timestamp is unavailable.
        """
        session_name = self.get_session_name(identifier)
        relevant_columns = self.loc[["Session1", "Session2", "Session3", "Session4", "Session5"]]
        mask = relevant_columns == session_name

        if not mask.any():
            raise ValueError(f"Session type '{identifier}' does not exist for this event")

        name = mask.idxmax()
        date_utc = self[f"{name}DateUtc"]
        date = self[f"{name}Date"]

        if (not utc) and pd.isna(date) and (not pd.isna(date_utc)):
            raise ValueError("Local timestamp is not available")

        if utc:
            return pd.Timestamp(date_utc)
        return pd.Timestamp(date)

    def get_session(self, session_name: int | str):
        from .core import get_session

        resolved_session_name = self.get_session_name(session_name)
        return get_session(self._year, self._event_name, resolved_session_name)

    def get_race(self):
        """Return the race session."""
        return self.get_session("Race")

    def get_qualifying(self):
        """Return the qualifying session."""
        return self.get_session("Qualifying")

    def get_sprint(self):
        """Return the sprint session."""
        return self.get_session("Sprint")

    def get_sprint_shootout(self):
        """Return the sprint shootout session."""
        return self.get_session("Sprint Shootout")

    def get_sprint_qualifying(self):
        """Return the sprint qualifying session."""
        return self.get_session("Sprint Qualifying")

    def get_practice(self, number: int):
        """Return the specified practice session.

        Args:
            number: Practice session number (1, 2, or 3).
        """
        return self.get_session(f"Practice {number}")


class EventSchedule(pd.DataFrame):
    """FastF1-compatible event schedule container."""

    _metadata: ClassVar[list[str]] = ["year"]
    year: int | None

    def __init__(self, *args: Any, year: int | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.year = year

    @property
    def _constructor(self):
        return EventSchedule

    def get_event_by_round(self, round_number: int) -> Event:
        """Return event for a specific championship round."""
        if round_number == 0:
            raise ValueError("Cannot get testing event by round number!")
        if "RoundNumber" not in self.columns:
            raise ValueError(f"Invalid round: {round_number}")

        rounds = pd.to_numeric(self["RoundNumber"], errors="coerce")
        matches = self.loc[rounds == round_number]
        if len(matches) == 1:
            event_name = str(matches.iloc[0].get("EventName", ""))
            return _get_event_for_year(int(self.year or 0), event_name)
        if len(matches) == 0:
            raise ValueError(f"Invalid round: {round_number}")
        raise ValueError("Something went wrong, cannot determine a unique event.")

    def get_event_by_name(self, name: str, strict_search: bool = False) -> Event | None:
        """Return event by name; optionally require strict exact matching."""
        if self.year is None:
            return None
        event_names: list[str] = []
        if "EventName" in self.columns:
            event_names = [item for item in self["EventName"].dropna().astype(str).tolist() if item]
        return _find_event_by_name(int(self.year), event_names, name, exact_match=strict_search)

    def get_event(self, identifier: int | str, strict_search: bool = False) -> Event | None:
        """Return event by round number or name."""
        if isinstance(identifier, int):
            return self.get_event_by_round(identifier)
        event = self.get_event_by_name(identifier, strict_search=strict_search)
        if event is not None:
            return event
        if not strict_search:
            try:
                return self.get_event_by_round(int(identifier))
            except (TypeError, ValueError):
                return None
        return None


def _get_event_for_year(year: int, event_name: str) -> Event:
    year_payload = _resolve_year_payload(year)
    metadata_dict = year_payload.get("metadata", {})
    metadata = metadata_dict.get(event_name) if isinstance(metadata_dict, dict) else None
    return _create_event(year, event_name, metadata)


def _create_event(year: int, event_name: str, metadata: Any) -> Event:
    event_factory = cast(Any, Event)
    event_metadata = metadata if isinstance(metadata, dict) else None
    return cast(Event, event_factory(year, event_name, event_metadata))


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
    year: int, event_names: list[str], name: str, exact_match: bool = False
) -> Event | None:
    year_payload = _resolve_year_payload(year)
    metadata_dict = year_payload.get("metadata", {})

    if exact_match:
        query = name.lower()
        for event_name in event_names:
            if event_name.lower() == query:
                metadata = (
                    metadata_dict.get(event_name) if isinstance(metadata_dict, dict) else None
                )
                return _create_event(year, event_name, metadata)
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
            metadata = metadata_dict.get(event_name) if isinstance(metadata_dict, dict) else None
            return _create_event(year, event_name, metadata)

    # Circuit/city/country aliases ("Spa", "Interlagos", "USA") resolve
    # directly: fuzzy matching sends several of them to the wrong race. An
    # alias whose event is not on this season's calendar returns None rather
    # than silently loading a different race.
    alias_targets = _event_alias_targets(name)
    if alias_targets:
        for target in alias_targets:
            if target in event_names:
                metadata = metadata_dict.get(target) if isinstance(metadata_dict, dict) else None
                return _create_event(year, target, metadata)
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

    metadata = metadata_dict.get(matched_event_name) if isinstance(metadata_dict, dict) else None
    return _create_event(year, matched_event_name, metadata)


def get_event(year: int, gp: int | str, exact_match: bool = False) -> Event | None:
    """Get event by round number or name."""
    schedule = get_event_schedule(year)
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


def get_event_by_round(year: int, round_number: int) -> Event:
    """Get event by round number."""
    return get_event_schedule(year).get_event_by_round(round_number)


def get_event_by_name(year: int, name: str, exact_match: bool = False) -> Event:
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
    """
    events = list(_get_events_cached(year))
    event = _find_event_by_name(year, events, name, exact_match=exact_match)
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


def get_event_schedule(year: int, include_testing: bool = True) -> EventSchedule:
    """Return FastF1-compatible event schedule for a specific season."""
    year_payload = _resolve_year_payload(year)
    metadata_dict = year_payload.get("metadata", {})
    events = list(_get_events_cached(year))

    rows: list[dict[str, Any]] = []
    for event_name in events:
        metadata = metadata_dict.get(event_name) if isinstance(metadata_dict, dict) else None
        event = _create_event(year, event_name, metadata)
        rows.append(cast(dict[str, Any], event.to_dict()))

    schedule = EventSchedule(rows)
    schedule.year = year
    if include_testing or "RoundNumber" not in schedule.columns:
        return schedule

    rounds = pd.to_numeric(schedule["RoundNumber"], errors="coerce")
    filtered = schedule.loc[rounds > 0].copy()
    filtered.year = year
    return filtered
