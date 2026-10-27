"""Exercise public tif1 lap adaptation and cutoff handling without ML or network."""

import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import weekend_data as data


def canonical_laps() -> pd.DataFrame:
    """Build canonical lap rows with nullable flags and timezone-aware dates."""
    return pd.DataFrame(
        {
            "Driver": ["VER", "VER", "NOR"],
            "LapNumber": [1.0, 2.0, 1.0],
            "LapTime": pd.to_timedelta([90, 91, 92], unit="s"),
            "LapTimeSeconds": [90.0, 91.0, 92.0],
            "Time": pd.to_timedelta([100, 191, 102], unit="s"),
            "LapStartDate": [
                "2026-10-23T14:00:00-05:00",
                "2026-10-23T19:01:30Z",
                "2026-10-23T19:00:00Z",
            ],
            "TyreLife": [3.0, 4.0, 3.0],
            "Stint": [1.0, 1.0, 1.0],
            "Compound": ["MEDIUM", "MEDIUM", "SOFT"],
            "TrackStatus": [1.0, 1.0, 4.0],
            "PitInTime": pd.to_timedelta([None, 191, None], unit="s"),
            "PitOutTime": pd.to_timedelta([None, None, None], unit="s"),
            "Position": [2.0, 1.0, 1.0],
            "Deleted": pd.Series([False, True, None], dtype="boolean"),
            "IsAccurate": pd.Series([True, False, None], dtype="boolean"),
            "Rainfall": pd.Series([False, False, None], dtype="boolean"),
            "SpeedST": [320.0, 321.0, 322.0],
        }
    )


class Tif1AdapterTests(unittest.TestCase):
    """Keep the model input contract independent of raw telemetry file formats."""

    def test_maps_timedeltas_flags_weather_and_speed(self) -> None:
        """Preserve seconds and unknown flags instead of scaling or fabricating data."""
        actual = data.laps_to_model_data(canonical_laps())
        assert actual["time"] == [90.0, 91.0, 92.0]
        assert actual["sesT"] == [100.0, 191.0, 102.0]
        assert actual["pin"] == [None, 191.0, None]
        assert actual["status"] == ["1", "1", "4"]
        assert actual["lSD"][0] == "2026-10-23T19:00:00+00:00"
        assert actual["del"] == [False, True, None]
        assert actual["iacc"] == [True, False, None]
        assert actual["wR"] == [False, False, None]
        assert actual["vst"] == [320.0, 321.0, 322.0]

    def test_lap_time_fallback_and_optional_fields(self) -> None:
        """Use canonical LapTime when its numeric helper is absent."""
        laps = canonical_laps()[["Driver", "LapNumber", "LapTime"]]
        actual = data.laps_to_model_data(laps)
        assert actual["time"] == [90.0, 91.0, 92.0]
        assert "wR" not in actual

    def test_missing_essential_schema_raises(self) -> None:
        """Malformed data must remain distinguishable from unavailable optional data."""
        with pytest.raises(data.tif1.InvalidDataError):
            data.laps_to_model_data(pd.DataFrame({"Driver": ["VER"]}))

    def test_position_uses_lap_end_not_start_and_includes_boundary(self) -> None:
        """A later lap starting before the cutoff cannot leak its finishing position."""
        cutoff = dt.datetime(2026, 10, 23, 19, 1, 30, tzinfo=dt.UTC)
        positions = data.session_positions(data.laps_to_model_data(canonical_laps()), cutoff)
        assert positions == {"VER": 2.0}

    def test_naive_calendar_clocks_are_explicitly_utc(self) -> None:
        """Unzoned schedule strings must not depend on the execution host's timezone."""
        expected = dt.datetime(2026, 10, 23, 19, tzinfo=dt.UTC)
        assert data.timestamp("2026-10-23", "19:00:00") == expected
        assert data.timestamp("2026-10-23", "14:00:00-05:00") == expected

    def test_public_session_api_owns_shared_cache_and_pandas_choice(self) -> None:
        """Load canonical laps using tif1 caching instead of a private HTTP cache."""
        with (
            patch.object(
                data.tif1, "get_session", return_value=SimpleNamespace(laps=canonical_laps())
            ) as load,
            patch.object(
                data.requests, "get", side_effect=AssertionError("Unexpected private HTTP")
            ),
        ):
            actual, audit = data.tif1_session_data(2026, "United States Grand Prix", "Practice 1")
        load.assert_called_once_with(
            2026, "United States Grand Prix", "Practice 1", enable_cache=True, lib="pandas"
        )
        assert actual["drv"] == ["VER", "VER", "NOR"]
        assert audit["status"] == "ok"
        assert audit["rows"] == 3
        assert audit["cache"] == "tif1 shared cache"

    def test_offline_guard_prevents_any_session_or_http_request(self) -> None:
        """A caller cannot accidentally request the network with offline=True."""
        with (
            patch.object(data.tif1, "get_config", return_value=Mock(get=Mock(return_value=False))),
            patch.object(data.tif1, "get_session") as load,
            patch.object(data.requests, "get") as request,
            self.assertWarnsRegex(RuntimeWarning, "prevent network access"),
        ):
            actual, audit = data.tif1_session_data(2026, "Monaco", "Practice 1", offline=True)
        assert actual is None
        assert "offline disabled" in audit["status"]
        load.assert_not_called()
        request.assert_not_called()

    def test_offline_uses_shared_cached_session_when_enabled(self) -> None:
        """Let tif1 service a cache-only lookup when global offline mode is active."""
        with (
            patch.object(data.tif1, "get_config", return_value=Mock(get=Mock(return_value=True))),
            patch.object(
                data.tif1, "get_session", return_value=SimpleNamespace(laps=canonical_laps())
            ) as load,
        ):
            actual, audit = data.tif1_session_data(2026, "Monaco", "Practice 1", offline=True)
        assert len(actual["lap"]) == 3
        assert audit["status"] == "ok"
        assert load.call_args.kwargs["enable_cache"]

    def test_offline_cache_miss_and_online_failure_are_visible(self) -> None:
        """Report cache or network failures explicitly instead of inventing features."""
        for offline in (True, False):
            with (
                self.subTest(offline=offline),
                patch.object(
                    data.tif1, "get_config", return_value=Mock(get=Mock(return_value=True))
                ),
                patch.object(
                    data.tif1, "get_session", side_effect=data.tif1.NetworkError("cache miss")
                ),
                self.assertWarnsRegex(RuntimeWarning, "NetworkError"),
            ):
                actual, audit = data.tif1_session_data(
                    2026, "Monaco", "Practice 1", offline=offline
                )
            assert actual is None
            prefix = "offline cache unavailable" if offline else "session unavailable"
            assert prefix in audit["status"]

    def test_invalid_loaded_data_propagates(self) -> None:
        """Keep schema and validation errors actionable for the contributor."""
        with patch.object(
            data.tif1, "get_session", side_effect=data.tif1.InvalidDataError("bad schema")
        ):
            with pytest.raises(data.tif1.InvalidDataError):
                data.tif1_session_data(2026, "Monaco", "Practice 1")

    def test_unfinished_weekend_session_is_never_loaded(self) -> None:
        """Require the conservative session completion/publication buffer first."""
        race = {
            "season": "2026",
            "round": "19",
            "raceName": "United States Grand Prix",
            "date": "2026-10-25",
            "time": "19:00:00Z",
            "FirstPractice": {"date": "2026-10-23", "time": "19:00:00Z"},
        }
        cutoff = dt.datetime(2026, 10, 23, 21, tzinfo=dt.UTC)
        with patch.object(data, "tif1_session_data") as load:
            actual, audit = data.weekend_for(race, "practice", cutoff, weather=False)
        load.assert_not_called()
        assert actual["drivers"] == {}
        assert audit["sessions"] == []


if __name__ == "__main__":
    unittest.main()
