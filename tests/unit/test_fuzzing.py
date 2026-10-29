"""Fuzzing tests for malformed and corrupted data."""

import json
from unittest.mock import patch

import pandas as pd
import pytest

from tif1.core import Lap, Session
from tif1.exceptions import InvalidDataError
from tif1.payload_loader import InMemoryTransport


def _delegate_fast_fetch(self, path):
    """Route ``_fetch_from_cdn_fast`` through the patched regular fetch seam.

    Tests patch only ``Session._fetch_from_cdn``; with the ultra-cold fast
    path being the default, the fast delegate is explicitly redirected to the
    same mock so both fetch seams hit the test double.
    """
    return self._fetch_from_cdn(path)


class TestFuzzing:
    """Fuzzing tests with malformed data."""

    @patch("tif1.core.Session._fetch_from_cdn_fast", new=_delegate_fast_fetch)
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_malformed_json(self, mock_fetch):
        """Test handling of malformed JSON."""
        mock_fetch.side_effect = json.JSONDecodeError("Invalid", "", 0)

        session = Session(2025, "Pre-Season Testing", "Race", enable_cache=False)
        with pytest.raises(json.JSONDecodeError):
            session._fetch_json("test.json")

    def test_non_dict_response(self):
        """Non-dict JSON responses are rejected by the payload pipeline."""
        session = Session(2025, "Pre-Season Testing", "Race", enable_cache=False)
        session._payload_loader.transport = InMemoryTransport(
            {"test.json": ["list", "not", "dict"]}
        )
        with pytest.raises(InvalidDataError, match="Expected dict"):
            session._fetch_json("test.json")

    def test_null_response(self):
        """Null responses are rejected by the payload pipeline."""
        session = Session(2025, "Pre-Season Testing", "Race", enable_cache=False)
        session._payload_loader.transport = InMemoryTransport({"test.json": None})
        with pytest.raises(InvalidDataError, match="Expected dict"):
            session._fetch_json("test.json")

    @patch("tif1.core.Session._fetch_from_cdn_fast", new=_delegate_fast_fetch)
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_empty_drivers_list(self, mock_fetch):
        """Test handling of empty drivers list."""
        mock_fetch.return_value = {"drivers": []}

        session = Session(2025, "Pre-Season Testing", "Race", enable_cache=False)
        drivers = session.drivers
        assert drivers == []

    @patch("tif1.core.Session._fetch_from_cdn_fast", new=_delegate_fast_fetch)
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_missing_drivers_key(self, mock_fetch):
        """Test handling of missing drivers key."""
        mock_fetch.return_value = {"data": []}

        session = Session(2025, "Pre-Season Testing", "Race", enable_cache=False)
        drivers = session.drivers
        assert drivers == []

    @patch("tif1.core.Session._fetch_from_cdn_fast", new=_delegate_fast_fetch)
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_corrupted_lap_data(self, mock_fetch):
        """Corrupted lap payloads degrade to NaN rows; nothing raises."""
        mock_fetch.return_value = {"drivers": [{"driver": "VER", "team": "Red Bull", "number": 1}]}

        async def corrupted(requests, **_kwargs):
            return [{"time": "invalid", "lap": "not_a_number"} for _ in requests]

        session = Session(2025, "Pre-Season Testing", "Race", enable_cache=False)
        with patch("tif1.core.fetch_multiple_async", new=corrupted):
            laps = session.laps
        assert isinstance(laps, pd.DataFrame)

    @patch("tif1.core.Session._fetch_from_cdn_fast", new=_delegate_fast_fetch)
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_empty_telemetry(self, mock_fetch):
        """Test handling of empty telemetry data."""
        mock_fetch.return_value = {"tel": {}}

        session = Session(2025, "Pre-Season Testing", "Race", enable_cache=False)
        lap = Lap({"Driver": "VER", "LapNumber": 1}, session=session)
        telemetry = lap.telemetry
        assert len(telemetry) == 0

    @patch("tif1.core.Session._fetch_from_cdn_fast", new=_delegate_fast_fetch)
    @patch("tif1.core.Session._fetch_from_cdn")
    def test_mismatched_array_lengths(self, mock_fetch):
        """Test handling of mismatched array lengths."""
        mock_fetch.return_value = {
            "tel": {"time": [0, 1, 2], "speed": [100, 200]}  # Mismatched lengths
        }

        session = Session(2025, "Pre-Season Testing", "Race", enable_cache=False)
        lap = Lap({"Driver": "VER", "LapNumber": 1}, session=session)
        telemetry = lap.telemetry
        assert len(telemetry) >= 0  # Should handle gracefully
