"""Tests that _process_lap_df enforces the _COLUMNS dtype contract on a pandas laps DataFrame."""

import pandas as pd
import pytest

from tif1.core import _process_lap_df

# ---------------------------------------------------------------------------
# Sample payload matching the user-provided laptimes.json (3 laps, "None" sentinels)
# Keys are the raw JSON keys that come out of the laptimes endpoint.
# The _normalize_payload_lists step converts "None" → None before validation;
# we replicate that here so the test is realistic.
# ---------------------------------------------------------------------------
_RAW_PAYLOAD = {
    "time": [None, 87.13, None],  # LapTime (seconds)
    "lap": [1, 2, 3],  # LapNumber
    "compound": ["HARD", "HARD", "HARD"],
    "stint": [1, 1, 1],
    "s1": [None, 17.582, 37.283],  # Sector1Time
    "s2": [42.784, 37.242, 79.416],  # Sector2Time
    "s3": [46.09, 32.306, 52.161],  # Sector3Time
    "life": [1, 2, 3],  # TyreLife
    "pos": [None, None, None],  # Position
    "status": ["1", "1", "12"],  # TrackStatus
    "pb": [False, True, False],  # IsPersonalBest
    "sesT": [1102.412, 1189.542, 1358.365],  # Time (session time)
    "drv": ["VER", "VER", "VER"],  # Driver
    "dNum": ["1", "1", "1"],  # DriverNumber
    "pout": [962.225, None, None],  # PitOutTime
    "pin": [None, None, None],  # PitInTime
    "s1T": [None, 1119.994, 1227.069],  # Sector1SessionTime
    "s2T": [1056.498, 1157.236, 1306.487],  # Sector2SessionTime
    "s3T": [1102.48, 1189.542, 1358.596],  # Sector3SessionTime
    "vi1": [262.0, 294.0, 148.0],  # SpeedI1
    "vi2": [153.0, 322.0, 138.0],  # SpeedI2
    "vfl": [219.0, 212.0, 218.0],  # SpeedFL
    "vst": [293.0, 329.0, 143.0],  # SpeedST
    "fresh": [True, True, True],  # FreshTyre
    "team": ["Red Bull Racing", "Red Bull Racing", "Red Bull Racing"],
    "lST": [962.225, 1102.412, 1189.542],  # LapStartTime
    "lSD": [  # LapStartDate
        "2025-12-05T09:31:03.592000000",
        "2025-12-05T09:33:23.779000000",
        "2025-12-05T09:34:50.909000000",
    ],
    "del": [False, False, False],  # Deleted
    "delR": [None, None, None],  # DeletedReason
    "ff1G": [False, False, False],  # FastF1Generated
    "iacc": [False, True, False],  # IsAccurate
    "wT": [1013.842, 1134.295, 1194.393],  # WeatherTime
    "wAT": [27.7, 27.9, 27.9],  # AirTemp
    "wH": [45.0, 46.0, 45.0],  # Humidity
    "wP": [1017.1, 1017.2, 1017.2],  # Pressure
    "wR": [False, False, False],  # Rainfall
    "wTT": [34.9, 34.0, 34.0],  # TrackTemp
    "wWD": [222, 231, 239],  # WindDirection
    "wWS": [1.5, 1.1, 1.1],  # WindSpeed
}


@pytest.fixture
def laps_df() -> pd.DataFrame:
    """Return a processed laps DataFrame from the sample payload."""
    raw = pd.DataFrame(_RAW_PAYLOAD)
    return _process_lap_df(raw, "pandas")


class TestLapsDtypes:
    """Verify that _process_lap_df enforces _COLUMNS dtypes for every column."""

    # ------------------------------------------------------------------
    # Timedelta columns
    # ------------------------------------------------------------------

    def test_laptime_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["LapTime"])

    def test_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["Time"])

    def test_weather_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["WeatherTime"])

    def test_sector1_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["Sector1Time"])

    def test_sector2_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["Sector2Time"])

    def test_sector3_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["Sector3Time"])

    def test_sector1_session_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["Sector1SessionTime"])

    def test_sector2_session_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["Sector2SessionTime"])

    def test_sector3_session_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["Sector3SessionTime"])

    def test_pit_out_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["PitOutTime"])

    def test_pit_in_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["PitInTime"])

    def test_lap_start_time_is_timedelta(self, laps_df):
        assert pd.api.types.is_timedelta64_ns_dtype(laps_df["LapStartTime"])

    # ------------------------------------------------------------------
    # Datetime column
    # ------------------------------------------------------------------

    def test_lap_start_date_is_datetime(self, laps_df):
        assert pd.api.types.is_datetime64_any_dtype(laps_df["LapStartDate"])

    def test_lap_start_date_values_correct(self, laps_df):
        expected = pd.Timestamp("2025-12-05T09:31:03.592")
        assert laps_df["LapStartDate"].iloc[0] == expected

    # ------------------------------------------------------------------
    # Float64 columns
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "col",
        [
            "LapNumber",
            "Stint",
            "TyreLife",
            "SpeedI1",
            "SpeedI2",
            "SpeedFL",
            "SpeedST",
            "AirTemp",
            "Humidity",
            "Pressure",
            "TrackTemp",
            "WindSpeed",
        ],
    )
    def test_float64_column(self, laps_df, col):
        assert laps_df[col].dtype == "float64", f"{col} should be float64, got {laps_df[col].dtype}"

    def test_position_is_float64(self, laps_df):
        # Position is all None — should still be float64
        assert laps_df["Position"].dtype == "float64"
        assert laps_df["Position"].isna().all()

    # ------------------------------------------------------------------
    # Int64 nullable column
    # ------------------------------------------------------------------

    def test_wind_direction_is_int64(self, laps_df):
        assert laps_df["WindDirection"].dtype.name == "Int64"

    def test_wind_direction_values(self, laps_df):
        assert list(laps_df["WindDirection"]) == [222, 231, 239]

    # ------------------------------------------------------------------
    # Bool columns
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "col",
        [
            "IsPersonalBest",
            "FreshTyre",
            "FastF1Generated",
            "IsAccurate",
            "Rainfall",
        ],
    )
    def test_bool_column(self, laps_df, col):
        assert laps_df[col].dtype == bool, f"{col} should be bool, got {laps_df[col].dtype}"

    def test_deleted_is_nullable_boolean(self, laps_df):
        assert laps_df["Deleted"].dtype.name == "boolean"

    # ------------------------------------------------------------------
    # None/"None" sentinel handling
    # ------------------------------------------------------------------

    def test_none_laptime_becomes_nat(self, laps_df):
        # First and third laps have None LapTime
        assert pd.isna(laps_df["LapTime"].iloc[0])
        assert pd.isna(laps_df["LapTime"].iloc[2])

    def test_valid_laptime_converted_correctly(self, laps_df):
        expected = pd.Timedelta(seconds=87.13)
        assert laps_df["LapTime"].iloc[1] == expected

    def test_none_pitout_becomes_nat(self, laps_df):
        assert pd.isna(laps_df["PitOutTime"].iloc[1])
        assert pd.isna(laps_df["PitOutTime"].iloc[2])

    def test_valid_pitout_converted(self, laps_df):
        expected = pd.Timedelta(seconds=962.225)
        assert laps_df["PitOutTime"].iloc[0] == expected

    def test_none_sector1_time_becomes_nat(self, laps_df):
        assert pd.isna(laps_df["Sector1Time"].iloc[0])

    def test_laptime_seconds_derived(self, laps_df):
        """LapTimeSeconds column should be the float total_seconds for valid laps."""
        assert "LapTimeSeconds" in laps_df.columns
        assert laps_df["LapTimeSeconds"].dtype == "float64"
        assert pytest.approx(laps_df["LapTimeSeconds"].iloc[1], abs=1e-3) == 87.13

    # ------------------------------------------------------------------
    # String columns
    # ------------------------------------------------------------------

    def test_driver_is_string(self, laps_df):
        assert laps_df["Driver"].iloc[0] == "VER"

    def test_driver_number_is_string(self, laps_df):
        assert laps_df["DriverNumber"].iloc[0] == "1"

    def test_track_status_is_string(self, laps_df):
        assert laps_df["TrackStatus"].iloc[0] == "1"
