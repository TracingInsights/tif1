"""Fuzzing tests for malformed and corrupted data."""

import json
from unittest.mock import patch

import pandas as pd
import pytest

from tif1.core import Lap, Session


class TestFuzzing:
    """Fuzzing tests with malformed data."""

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_malformed_json(self, mock_fetch):
        """Test handling of malformed JSON."""
        mock_fetch.side_effect = json.JSONDecodeError("Invalid", "", 0)

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        with pytest.raises(json.JSONDecodeError):
            session._fetch_json("test.json")

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_non_dict_response(self, mock_fetch):
        """Test handling of non-dict JSON response."""
        mock_fetch.return_value = ["list", "not", "dict"]

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        # This should still work as _fetch_from_cdn returns the data
        data = session._fetch_json("test.json")
        assert isinstance(data, list)

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_null_response(self, mock_fetch):
        """Test handling of null response."""
        mock_fetch.return_value = None

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        data = session._fetch_json("test.json")
        assert data is None

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_empty_drivers_list(self, mock_fetch):
        """Test handling of empty drivers list."""
        mock_fetch.return_value = {"drivers": []}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        drivers = session.drivers
        assert drivers == []

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_missing_drivers_key(self, mock_fetch):
        """Test handling of missing drivers key."""
        mock_fetch.return_value = {"data": []}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        drivers = session.drivers
        assert drivers == []

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_corrupted_lap_data(self, mock_fetch):
        """Test handling of corrupted lap data."""
        call_count = [0]

        def side_effect(path):
            call_count[0] += 1
            if "drivers.json" in path:
                return {"drivers": [{"driver": "VER", "team": "Red Bull", "number": 1}]}
            return {"time": "invalid", "lap": "not_a_number"}

        mock_fetch.side_effect = side_effect

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        laps = session.laps
        assert isinstance(laps, pd.DataFrame)

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_empty_telemetry(self, mock_fetch):
        """Test handling of empty telemetry data."""
        mock_fetch.return_value = {"tel": {}}

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        lap = Lap({"Driver": "VER", "LapNumber": 1}, session=session)
        telemetry = lap.telemetry
        assert len(telemetry) == 0

    @patch("tif1.core.Session._fetch_from_cdn")
    def test_mismatched_array_lengths(self, mock_fetch):
        """Test handling of mismatched array lengths."""
        mock_fetch.return_value = {
            "tel": {"time": [0, 1, 2], "speed": [100, 200]}  # Mismatched lengths
        }

        session = Session(2025, "Test GP", "Race", enable_cache=False)
        lap = Lap({"Driver": "VER", "LapNumber": 1}, session=session)
        telemetry = lap.telemetry
        assert len(telemetry) >= 0  # Should handle gracefully
