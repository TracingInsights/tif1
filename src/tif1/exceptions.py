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


class AsyncRequiredError(TIF1Error):
    """Raised when a synchronous API cannot block on its network fetch.

    Inside Pyodide the event loop is the browser's own and synchronous code
    can only wait on it through JavaScript Promise Integration (JSPI). Without
    JSPI (Firefox and Safari by default, or code entered through
    ``runPython`` instead of ``runPythonAsync``) the synchronous entry points
    (``session.load()``, ``session.laps``) raise this error; the async
    counterpart named in ``alternative`` works everywhere.
    """

    def __init__(
        self, operation: str | None = None, alternative: str | None = None, **context: Any
    ) -> None:
        what = f"`{operation}`" if operation else "This operation"
        message = (
            f"{what} cannot block on network fetches in this runtime "
            "(no JavaScript Promise Integration)"
        )
        if alternative:
            message += f"; use `await {alternative}` instead"
        super().__init__(message, operation=operation, alternative=alternative, **context)


class SessionNotLoadedError(TIF1Error):
    """Raised when accessing session data before loading."""

    def __init__(self, attribute: str | None = None, **context: Any) -> None:
        message = (
            f"Session data not loaded. Access '{attribute}' requires loading data first."
            if attribute
            else "Session data not loaded"
        )
        super().__init__(message, attribute=attribute, **context)


DataNotLoadedError = SessionNotLoadedError
"""fastf1's name for :class:`SessionNotLoadedError` (the same class object)."""


class DriverNotFoundError(DataNotFoundError, ValueError):
    """Raised when driver is not found in session.

    Also a ``ValueError``: fastf1's ``Session.get_driver`` raises ``ValueError``
    for an unknown identifier, so code written for it keeps catching this.
    """

    def __init__(self, driver: str, **context: Any) -> None:
        message = f"Driver '{driver}' not found in session"
        # Call TIF1Error.__init__ directly to avoid DataNotFoundError's message construction
        TIF1Error.__init__(self, message, driver=driver, **_plain_names(context))


class EventNotFoundError(ValueError):
    """Raised when an event name matches no event of the season.

    ``get_event_by_name(2025, "Invalid GP")`` finds no event whose name,
    location or country is close enough to the query, so it raises instead of
    returning the least-bad match. This is a ``ValueError`` (not a
    ``TIF1Error``) because ``get_event_by_name`` raises ``ValueError`` for every
    unresolved name; ``get_session`` re-raises it instead of loading a
    different race.
    """


class EventNotInSeasonError(EventNotFoundError):
    """Raised when an event alias names a Grand Prix that is not on that season's calendar.

    ``get_event_by_name(2021, "Suzuka")`` resolves the alias to the Japanese
    Grand Prix, which was not held in 2021.
    """


class FuzzyMatchError(EventNotFoundError):
    """Raised when no fuzzy event-name match reaches the confidence threshold.

    fastf1's name for the failure ``get_event_by_name`` reports as
    :class:`EventNotFoundError`; it stays a ``ValueError`` like its parent.
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


class NoLapDataError(LapNotFoundError):
    """Raised when the session loaded but the lap payload held no usable laps.

    fastf1's exception for a successful API request that yields no data
    after processing; the message is fastf1's. A :class:`LapNotFoundError`,
    so handlers for missing laps catch it too.
    """

    def __init__(self, *_args: Any, **context: Any) -> None:
        TIF1Error.__init__(
            self,
            "Failed to load session because the API did not provide any usable data.",
            **_plain_names(context),
        )


class ErgastError(TIF1Error):
    """Base class for errors raised by the Ergast data layer (``tif1.ergast``)."""


class ErgastJsonError(ErgastError):
    """Raised when an Ergast/Jolpica response body cannot be parsed as JSON."""


class ErgastInvalidRequestError(ErgastError):
    """Raised when the Ergast/Jolpica API rejects a request (non-200 answer)."""


class FastF1CriticalError(TIF1Error):
    """Base class for errors that must break through catch-all error handling.

    fastf1 wraps its data-loading steps in :func:`tif1.logger.soft_exceptions`,
    which logs and swallows every exception except these: they are always
    raised to the user and loading stops (an exceeded API rate limit, for
    example). A ``TIF1Error`` like every other tif1 exception.
    """


class RateLimitExceededError(NetworkError, FastF1CriticalError):
    """Raised when the Jolpica API keeps answering 429 after the configured retries.

    A ``NetworkError`` so callers that handle transport failures handle this
    one too, and a :class:`FastF1CriticalError` so ``soft_exceptions`` never
    swallows it; the message names the limit that was hit.
    """

    def __init__(
        self, url: str | None = None, retry_after: float | None = None, **context: Any
    ) -> None:
        message = "Jolpica rate limit exceeded"
        if retry_after:
            message += f" (retry after {retry_after:.0f}s)"
        if url:
            message += f": url='{url}'"
        TIF1Error.__init__(
            self, message, url=url, status_code=429, retry_after=retry_after, **context
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
