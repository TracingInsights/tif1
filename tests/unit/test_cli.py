"""Tests for CLI module."""

from unittest.mock import Mock, patch

import pandas as pd
from typer.testing import CliRunner

import tif1
from tif1.cli import app

runner = CliRunner()


class TestCLI:
    """Test CLI commands."""

    @patch("tif1.get_events")
    def test_events_command(self, mock_get_events):
        """Test events command."""
        mock_get_events.return_value = pd.DataFrame(
            {"EventName": ["Australian Grand Prix", "Chinese Grand Prix"]}
        )
        result = runner.invoke(app, ["events", "2025"])
        assert result.exit_code == 0
        assert "Australian Grand Prix" in result.stdout

    @patch("tif1.get_sessions")
    def test_sessions_command(self, mock_get_sessions):
        """Test sessions command."""
        mock_get_sessions.return_value = ["Practice 1", "Qualifying", "Race"]
        result = runner.invoke(app, ["sessions", "2025", "Abu Dhabi Grand Prix"])
        assert result.exit_code == 0
        assert "Practice 1" in result.stdout

    @patch("tif1.get_session")
    def test_drivers_command(self, mock_get_session):
        """Test drivers command."""
        mock_session = Mock()
        mock_session.drivers = [
            {"driver": "VER", "team": "Red Bull Racing"},
            {"driver": "HAM", "team": "Mercedes"},
        ]
        mock_get_session.return_value = mock_session

        result = runner.invoke(app, ["drivers", "2025", "Abu Dhabi Grand Prix", "Race"])
        assert result.exit_code == 0
        assert "VER" in result.stdout

    @patch("tif1.get_session")
    def test_fastest_command_all_drivers(self, mock_get_session):
        """Test fastest command for all drivers."""
        mock_session = Mock()
        mock_session.lib = "pandas"
        mock_df = pd.DataFrame(
            {
                "Driver": ["VER"],
                "Team": ["Red Bull Racing"],
                "time": [90.123],
            }
        )
        mock_session.get_fastest_laps.return_value = mock_df
        mock_get_session.return_value = mock_session

        result = runner.invoke(app, ["fastest", "2025", "Abu Dhabi Grand Prix", "Race"])
        assert result.exit_code == 0

    @patch("tif1.get_session")
    def test_fastest_command_specific_driver(self, mock_get_session):
        """Test fastest command for specific driver."""
        mock_session = Mock()
        mock_driver = Mock()
        mock_lap = Mock()
        mock_lap.iloc = [{"time": 90.123}]
        mock_lap.__len__ = Mock(return_value=1)
        mock_driver.get_fastest_lap.return_value = mock_lap
        mock_session.get_driver.return_value = mock_driver
        mock_get_session.return_value = mock_session

        result = runner.invoke(
            app, ["fastest", "2025", "Abu Dhabi Grand Prix", "Race", "-d", "VER"]
        )
        assert result.exit_code == 0

    @patch("tif1.get_session")
    def test_fastest_command_no_laps(self, mock_get_session):
        """Test fastest command with no valid laps."""
        mock_session = Mock()
        mock_driver = Mock()
        mock_lap = Mock()
        mock_lap.__len__ = Mock(return_value=0)
        mock_driver.get_fastest_lap.return_value = mock_lap
        mock_session.get_driver.return_value = mock_driver
        mock_get_session.return_value = mock_session

        result = runner.invoke(
            app, ["fastest", "2025", "Abu Dhabi Grand Prix", "Race", "-d", "VER"]
        )
        assert result.exit_code == 0
        assert "No valid laps" in result.stdout

    @patch("tif1.get_session")
    def test_fastest_command_polars(self, mock_get_session):
        """Test fastest command with polars lib."""
        mock_session = Mock()
        mock_session.lib = "pandas"
        mock_df = pd.DataFrame(
            {
                "Driver": ["VER"],
                "Team": ["Red Bull Racing"],
                "time": [90.123],
            }
        )
        mock_session.get_fastest_laps.return_value = mock_df
        mock_get_session.return_value = mock_session

        result = runner.invoke(app, ["fastest", "2025", "Abu Dhabi Grand Prix", "Race"])
        assert result.exit_code == 0

    @patch("tif1.get_cache")
    def test_cache_info_command(self, mock_get_cache):
        """Test cache-info command."""
        mock_cache = Mock()
        mock_cache.cache_dir = Mock()
        mock_cache.cache_dir.glob.return_value = []
        mock_cache.cache_dir.iterdir.return_value = []
        mock_get_cache.return_value = mock_cache

        result = runner.invoke(app, ["cache-info"])
        assert result.exit_code == 0

    @patch("tif1.get_cache")
    def test_cache_clear_command_with_yes(self, mock_get_cache):
        """Test cache-clear command with --yes flag."""
        mock_cache = Mock()
        mock_get_cache.return_value = mock_cache

        result = runner.invoke(app, ["cache-clear", "--yes"])
        assert result.exit_code == 0
        mock_cache.clear.assert_called_once()

    @patch("tif1.get_cache")
    def test_cache_clear_command_cancelled(self, mock_get_cache):
        """Test cache-clear command cancelled."""
        mock_cache = Mock()
        mock_get_cache.return_value = mock_cache

        result = runner.invoke(app, ["cache-clear"], input="n\n")
        assert result.exit_code == 0
        assert "cancelled" in result.stdout

    def test_version_command(self):
        """Test version command."""
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert tif1.__version__ in result.stdout

    @patch("tif1.setup_logging")
    @patch("tif1.get_session")
    def test_debug_command(self, mock_get_session, mock_setup_logging):
        """Test debug command."""
        mock_session = Mock()
        mock_session.drivers = []
        mock_session.laps = []
        mock_get_session.return_value = mock_session

        result = runner.invoke(app, ["debug", "2025", "Abu Dhabi Grand Prix", "Race"])
        assert result.exit_code == 0
        mock_setup_logging.assert_called_once()
