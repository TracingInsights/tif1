"""Custom exceptions for tif1."""

from typing import Any


class TIF1Error(Exception):
    """Base exception for tif1."""

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context = context


class DataNotFoundError(TIF1Error):
    """Raised when requested data is not available."""

    def __init__(
        self,
        year: int | None = None,
        event: str | None = None,
        session: str | None = None,
        **context: Any,
    ) -> None:
        parts = ["Data not found"]
        if year:
            parts.append(f"year={year}")
        if event:
            parts.append(f"event='{event}'")
        if session:
            parts.append(f"session='{session}'")
        message = ": " + ", ".join(parts[1:]) if len(parts) > 1 else parts[0]
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
        message = ": " + ", ".join(parts[1:]) if len(parts) > 1 else parts[0]
        super().__init__(parts[0] + message, url=url, status_code=status_code, **context)


class InvalidDataError(TIF1Error):
    """Raised when fetched data is invalid or corrupted."""

    def __init__(self, reason: str | None = None, **context: Any) -> None:
        message = f"Invalid data: {reason}" if reason else "Invalid or corrupted data"
        super().__init__(message, reason=reason, **context)


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
        TIF1Error.__init__(self, message, driver=driver, **context)


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
            self, parts[0] + message, lap_number=lap_number, driver=driver, **context
        )
