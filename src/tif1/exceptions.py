"""Custom exceptions for tif1."""

from typing import Any
from urllib.parse import unquote


class TIF1Error(Exception):
    """Base exception for tif1."""

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context = context


def _plain_names(context: dict[str, Any]) -> dict[str, Any]:
    """Decode URL-encoded ``event``/``session`` context values (fetch paths carry them)."""
    for key in ("event", "session"):
        value = context.get(key)
        if isinstance(value, str):
            context[key] = unquote(value)
    return context


class DataNotFoundError(TIF1Error):
    """Raised when requested data is not available."""

    def __init__(
        self,
        year: int | None = None,
        event: str | None = None,
        session: str | None = None,
        **context: Any,
    ) -> None:
        # Fetch paths carry the URL-encoded event/session; show the plain names.
        if isinstance(event, str):
            event = unquote(event)
        if isinstance(session, str):
            session = unquote(session)
        parts = ["Data not found"]
        if year:
            parts.append(f"year={year}")
        if event:
            parts.append(f"event='{event}'")
        if session:
            parts.append(f"session='{session}'")
        message = ": " + ", ".join(parts[1:]) if len(parts) > 1 else ""
        super().__init__(parts[0] + message, year=year, event=event, session=session, **context)


class NetworkError(TIF1Error):
    """Raised when network request fails."""

    def __init__(
        self, url: str | None = None, status_code: int | None = None, **context: Any
    ) -> None:
        parts = ["Network request failed"]
        if url:
            parts.append(f"url='{url}'")
        if status_code:
            parts.append(f"status={status_code}")
        message = ": " + ", ".join(parts[1:]) if len(parts) > 1 else ""
        super().__init__(parts[0] + message, url=url, status_code=status_code, **context)


class InvalidDataError(TIF1Error):
    """Raised when fetched data is invalid or corrupted."""

    def __init__(self, reason: str | None = None, **context: Any) -> None:
        message = f"Invalid data: {reason}" if reason else "Invalid or corrupted data"
        super().__init__(message, reason=reason, **context)


class CircuitBreakerOpenError(NetworkError):
    """Raised when the circuit breaker refuses a call because it is open.

    A ``NetworkError`` so callers that handle transport failures handle this
    one too; the message stays "Circuit breaker is open".
    """

    def __init__(self, **context: Any) -> None:
        TIF1Error.__init__(self, "Circuit breaker is open", **context)


class CacheError(TIF1Error):
    """Raised when cache operations fail."""


class SessionNotLoadedError(TIF1Error):
    """Raised when accessing session data before loading."""

    def __init__(self, attribute: str | None = None) -> None:
        message = (
            f"Session data not loaded. Access '{attribute}' requires loading data first."
            if attribute
            else "Session data not loaded"
        )
        super().__init__(message, attribute=attribute)


class DriverNotFoundError(DataNotFoundError):
    """Raised when driver is not found in session."""

    def __init__(self, driver: str, **context: Any) -> None:
        message = f"Driver '{driver}' not found in session"
        # Call TIF1Error.__init__ directly to avoid DataNotFoundError's message construction
        TIF1Error.__init__(self, message, driver=driver, **_plain_names(context))


class EventNotInSeasonError(ValueError):
    """Raised when an event alias names a Grand Prix that is not on that season's calendar.

    ``get_event_by_name(2021, "Suzuka")`` resolves the alias to the Japanese
    Grand Prix, which was not held in 2021. This is a ``ValueError`` (not a
    ``TIF1Error``) because ``get_event_by_name`` raises ``ValueError`` for every
    unresolved name; ``get_session`` re-raises it instead of loading a
    different race.
    """


class LapNotFoundError(DataNotFoundError):
    """Raised when lap is not found."""

    def __init__(
        self, lap_number: int | None = None, driver: str | None = None, **context: Any
    ) -> None:
        parts = ["Lap not found"]
        if lap_number:
            parts.append(f"lap={lap_number}")
        if driver:
            parts.append(f"driver='{driver}'")
        message = ": " + ", ".join(parts[1:]) if len(parts) > 1 else parts[0]
        # Call TIF1Error.__init__ directly to avoid DataNotFoundError's message construction
        TIF1Error.__init__(
            self, parts[0] + message, lap_number=lap_number, driver=driver, **_plain_names(context)
        )


# Null-like string sentinels shared by payload normalization
# (``helpers``) and payload validation (``validation``). Defined here so
# hot-path helpers stay importable without pulling in pydantic.
_NULL_LIKE_STRINGS = {"", "none", "null", "nan"}


def _coerce_null_like_string_list(values: list[Any]) -> list[Any]:
    """Convert null-like string tokens to None in any list field."""
    if not values:
        return values

    normalized: list[Any] = []
    changed = False
    for value in values:
        if isinstance(value, str) and value.strip().lower() in _NULL_LIKE_STRINGS:
            normalized.append(None)
            changed = True
        else:
            normalized.append(value)
    return normalized if changed else values
