"""Event and session information for F1 seasons."""

from __future__ import annotations

import json
import logging
import re
from functools import lru_cache
from importlib.resources import files
from typing import Any, ClassVar, cast

import niquests
import pandas as pd

from .exceptions import InvalidDataError
from .http_session import get_session as get_http_session
from .retry import retry_with_backoff
from .schedule_schema import validate_schedule_payload

_F1SCHEDULE_YEAR_PATTERN = re.compile(r"^schedule_(\d{4})\.json$")
_F1SCHEDULE_CDN_TEMPLATE = (
    "https://cdn.jsdelivr.net/gh/theOehrly/f1schedule@master/schedule_{year}.json"
)

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

        session_dates = {}
        session_dates_utc = {}
        for i in range(1, 6):
            session_date_col = raw_year.get(f"session{i}_date", {})
            session_date = session_date_col.get(index)
            if session_date:
                session_dates[f"Session{i}Date"] = session_date
                session_dates_utc[f"Session{i}DateUtc"] = session_date

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


@lru_cache(maxsize=16)
def _load_f1schedule_year_from_cdn(year: int) -> dict[str, Any] | None:
    @retry_with_backoff(
        max_retries=3,
        backoff_factor=1.5,
        jitter=True,
        exceptions=(niquests.RequestException, InvalidDataError, TypeError, ValueError),
    )
    def _fetch_payload() -> dict[str, Any] | None:
        from .http_session import _track_request

        url = _F1SCHEDULE_CDN_TEMPLATE.format(year=year)
        response = get_http_session().get(url, timeout=15)
        _track_request(reused=True)
        if response.status_code == 404:
            return None
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, dict):
            raise InvalidDataError(reason=f"Invalid CDN schedule payload for year={year}")
        return _convert_f1schedule_year(payload, year)

    try:
        return _fetch_payload()
    except Exception as exc:
        logger.warning("Failed to fetch schedule_%s.json from CDN: %s", year, exc)
        return None


def _resolve_year_payload(year: int) -> dict[str, Any]:
    payload = _load_schedule_payload()
    year_payload = payload["years"].get(str(year), {})
    if isinstance(year_payload, dict) and year_payload:
        return year_payload

    cdn_payload = _load_f1schedule_year_from_cdn(year)
    if isinstance(cdn_payload, dict):
        return cdn_payload
    return {}


@lru_cache(maxsize=1)
def _load_schedule_payload() -> dict[str, Any]:
    vendored_years = _load_vendored_f1schedule_years()
    payload = {"schema_version": 1, "years": vendored_years}
    return validate_schedule_payload(payload)


def _build_events_for_year(year: int) -> list[str]:
    """Get list of events for a given year."""
    year_payload = _resolve_year_payload(year)
    events = year_payload.get("events", [])
    return list(events) if isinstance(events, list) else []


def _build_sessions_for_event(year: int, event: str) -> list[str]:
    """Get list of sessions for a given year and event."""
    standard = ["Practice 1", "Practice 2", "Practice 3", "Qualifying", "Race"]
    year_payload = _resolve_year_payload(year)
    sessions = year_payload.get("sessions", {})
    if not isinstance(sessions, dict):
        return standard
    event_sessions = sessions.get(event)
    if not isinstance(event_sessions, list) or not event_sessions:
        return standard
    return list(event_sessions)


@lru_cache(maxsize=16)
def _get_events_cached(year: int) -> tuple[str, ...]:
    """Get cached events as immutable tuple."""
    return tuple(_build_events_for_year(year))


def get_events(year: int) -> EventSchedule:
    """Get event schedule for a given year."""
    return get_event_schedule(year)


@lru_cache(maxsize=128)
def _get_sessions_cached(year: int, event: str) -> tuple[str, ...]:
    """Get cached sessions as immutable tuple."""
    return tuple(_build_sessions_for_event(year, event))


def get_sessions(year: int, event: str) -> list[str]:
    """Get list of sessions for a given year and event."""
    return list(_get_sessions_cached(year, event))


class Event(pd.Series):
    _year: int
    _event_name: str

    def __init__(self, year: int, name: str, metadata: dict | None = None):
        from datetime import datetime, timedelta
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
            gmt_offset = metadata.get("GmtOffset")
            for key, value in metadata.items():
                if key == "EventDate" and value:
                    data[key] = pd.Timestamp(value.split("T")[0])
                elif key.endswith("DateUtc") and value:
                    data[key] = pd.Timestamp(value)
                elif key.endswith("Date") and value and gmt_offset:
                    # Parse as tz-aware datetime
                    dt = datetime.fromisoformat(value)
                    # Apply GMT offset
                    hours = int(gmt_offset.split(":")[0])
                    minutes = int(gmt_offset.split(":")[1])

                    offset = timedelta(hours=hours, minutes=minutes)
                    data[key] = dt.replace(tzinfo=tz(offset))
                else:
                    data[key] = value

        super().__init__(data=data, name=data.get("RoundNumber", 0))  # type: ignore[call-arg]
        self._year = year
        self._event_name = name

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

    from .fuzzy import fuzzy_matcher

    def _remove_common_words(event_name: str) -> str:
        common_words = ["formula 1", str(year), "grand prix", "gp"]
        event_name = event_name.casefold()
        for word in common_words:
            event_name = event_name.replace(word, "")
        return event_name.strip()

    def _matcher_strings(metadata: dict[str, Any]) -> list[str]:
        strings = []
        if metadata.get("Location"):
            strings.append(metadata["Location"].casefold())
        if metadata.get("Country"):
            strings.append(metadata["Country"].casefold())
        if metadata.get("EventName"):
            strings.append(_remove_common_words(metadata["EventName"]))
        if metadata.get("OfficialEventName"):
            strings.append(_remove_common_words(metadata["OfficialEventName"]))
        return strings

    user_input = name
    name = _remove_common_words(name)
    reference = [
        _matcher_strings(metadata_dict.get(event_name, {}))
        if isinstance(metadata_dict, dict)
        else []
        for event_name in event_names
    ]
    index, exact = fuzzy_matcher(name, reference)
    matched_event_name = event_names[index]

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
        raise ValueError(f"No exact match found for event '{name}' in {year}")
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
