"""Tests for exceptions module."""

import pytest

from tif1.exceptions import (
    CacheError,
    DataNotFoundError,
    DriverNotFoundError,
    InvalidDataError,
    LapNotFoundError,
    NetworkError,
    SessionNotLoadedError,
    TIF1Error,
)


class TestExceptions:
    """Test custom exceptions."""

    def test_tif1_error(self):
        """Test base TIF1Error with context."""
        error = TIF1Error("Base error", foo="bar", year=2025)
        assert error.message == "Base error"
        assert error.context == {"foo": "bar", "year": 2025}
        assert str(error) == "Base error"

    def test_data_not_found_error(self):
        """Test DataNotFoundError with context."""
        error = DataNotFoundError(year=2025, event="Monaco Grand Prix", session="Race")
        assert "2025" in str(error)
        assert "Monaco Grand Prix" in str(error)
        assert "Race" in str(error)
        assert error.context["year"] == 2025

        with pytest.raises(TIF1Error):
            raise DataNotFoundError(year=2025)

    def test_network_error(self):
        """Test NetworkError with URL and status."""
        error = NetworkError(url="https://example.com", status_code=404)
        assert "https://example.com" in str(error)
        assert "404" in str(error)
        assert error.context["url"] == "https://example.com"
        assert error.context["status_code"] == 404

        with pytest.raises(TIF1Error):
            raise NetworkError(url="https://example.com")

    def test_invalid_data_error(self):
        """Test InvalidDataError with reason."""
        error = InvalidDataError(reason="Missing required field")
        assert "Missing required field" in str(error)
        assert error.context["reason"] == "Missing required field"

        with pytest.raises(TIF1Error):
            raise InvalidDataError(reason="test")

    def test_cache_error(self):
        """Test CacheError."""
        with pytest.raises(CacheError):
            raise CacheError("Cache operation failed")

        with pytest.raises(TIF1Error):
            raise CacheError("Inherits from TIF1Error")

    def test_session_not_loaded_error(self):
        """Test SessionNotLoadedError."""
        error = SessionNotLoadedError(attribute="laps")
        assert "laps" in str(error)
        assert error.context["attribute"] == "laps"

        with pytest.raises(TIF1Error):
            raise SessionNotLoadedError

    def test_driver_not_found_error(self):
        """Test DriverNotFoundError."""
        error = DriverNotFoundError(driver="VER", year=2025, event="Monaco Grand Prix")
        assert "VER" in str(error)
        assert error.context["driver"] == "VER"

        with pytest.raises(DataNotFoundError):
            raise DriverNotFoundError(driver="HAM")

        with pytest.raises(TIF1Error):
            raise DriverNotFoundError(driver="HAM")

    def test_lap_not_found_error(self):
        """Test LapNotFoundError."""
        error = LapNotFoundError(lap_number=10, driver="VER")
        assert "10" in str(error)
        assert "VER" in str(error)
        assert error.context["lap_number"] == 10
        assert error.context["driver"] == "VER"

        with pytest.raises(DataNotFoundError):
            raise LapNotFoundError(lap_number=5)

        with pytest.raises(TIF1Error):
            raise LapNotFoundError(lap_number=5)
