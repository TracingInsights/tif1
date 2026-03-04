"""Schema validation for packaged F1 schedule data."""

from __future__ import annotations

from typing import Any

from .exceptions import InvalidDataError


def validate_schedule_payload(payload: Any) -> dict[str, Any]:
    """Validate packaged schedule payload shape.

    Args:
        payload: Decoded JSON payload.

    Returns:
        The validated payload.

    Raises:
        InvalidDataError: If payload shape is invalid.
    """
    if not isinstance(payload, dict):
        raise InvalidDataError(reason="Schedule payload must be an object")

    version = payload.get("schema_version")
    if version != 1:
        raise InvalidDataError(reason=f"Unsupported schedule schema version: {version}")

    years = payload.get("years")
    if not isinstance(years, dict):
        raise InvalidDataError(reason="Schedule payload missing 'years' object")

    for year, year_payload in years.items():
        if not isinstance(year, str) or not year.isdigit():
            raise InvalidDataError(reason=f"Invalid year key: {year!r}")
        if not isinstance(year_payload, dict):
            raise InvalidDataError(reason=f"Year payload must be object for year={year}")

        events = year_payload.get("events")
        sessions = year_payload.get("sessions")
        if not isinstance(events, list) or not all(isinstance(event, str) for event in events):
            raise InvalidDataError(reason=f"Invalid events list for year={year}")
        if not isinstance(sessions, dict):
            raise InvalidDataError(reason=f"Invalid sessions map for year={year}")

        for event_name in events:
            event_sessions = sessions.get(event_name)
            if not isinstance(event_sessions, list) or not all(
                isinstance(session, str) for session in event_sessions
            ):
                raise InvalidDataError(
                    reason=f"Invalid session list for year={year} event={event_name!r}"
                )

    return payload
