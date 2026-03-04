"""Tests that _create_telemetry_df enforces DATA_DTYPES on a pandas telemetry DataFrame.

Uses the sample _tel.json payload from the user spec (with "None" sentinels
already normalised to Python None, as the validation layer does).
"""

import pandas as pd
import pytest

from tif1.core_utils.helpers import _create_telemetry_df

# ---------------------------------------------------------------------------
# Sample payload mirroring the _tel.json spec (3 samples per channel).
# "None" strings for DriverAhead replicate nulls that arrive from the JSON
# and are converted to Python None by _coerce_null_like_string_list.
# ---------------------------------------------------------------------------
_RAW_TEL = {
    "time": [0.0, 0.063, 0.124],
    "rpm": [11422.88, 11320.82, 11222.0],
    "speed": [222.14, 223.085, 224.0],
    "gear": [5, 5, 5],
    "throttle": [62.0, 30.5, 0.0],
    "brake": [0, 0, 1],
    "drs": [0, 0, 0],
    "distance": [0.11675, 4.03139, 7.77889],
    "rel_distance": [0.0000223, 0.000769, 0.001485],
    "DriverAhead": [None, None, None],  # "None" already normalised
    "DistanceToDriverAhead": [46.432, 46.432, 46.432],
    "acc_x": [2.7778, 0.7882, -4.0729],
    "acc_y": [-1.4228, -1.1979, -0.9454],
    "acc_z": [-0.2229, -0.2229, -0.4627],
    "x": [608.185, 647.0, 684.664],
    "y": [2085.047, 2090.0, 2094.749],
    "z": [-240.108, -240.0, -239.738],
}

_DRIVER = "VER"
_LAP_NUM = 3


@pytest.fixture
def tel_df() -> pd.DataFrame:
    """Return a processed telemetry DataFrame from the sample payload."""
    result = _create_telemetry_df(_RAW_TEL, _DRIVER, _LAP_NUM, "pandas")
    assert result is not None, "_create_telemetry_df returned None for valid payload"
    return result


class TestTelemetryDtypes:
    """Verify _create_telemetry_df enforces DATA_DTYPES for every column."""

    # ------------------------------------------------------------------
    # Time → timedelta64[ns]
    # ------------------------------------------------------------------

    def test_time_is_timedelta(self, tel_df):
        assert pd.api.types.is_timedelta64_ns_dtype(tel_df["Time"]), (
            f"Time should be timedelta64[ns], got {tel_df['Time'].dtype}"
        )

    def test_time_zero_value(self, tel_df):
        assert tel_df["Time"].iloc[0] == pd.Timedelta(0)

    def test_time_second_sample(self, tel_df):
        assert tel_df["Time"].iloc[1] == pytest.approx(
            pd.Timedelta(seconds=0.063).total_seconds() * 1e9, rel=1e-6
        ) or tel_df["Time"].iloc[1] == pd.Timedelta(seconds=0.063)

    # ------------------------------------------------------------------
    # Float64 channels
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "col",
        [
            "RPM",
            "Speed",
            "Throttle",
            "Distance",
            "RelativeDistance",
            "DistanceToDriverAhead",
            "AccelerationX",
            "AccelerationY",
            "AccelerationZ",
            "X",
            "Y",
            "Z",
        ],
    )
    def test_float64_column(self, tel_df, col):
        assert tel_df[col].dtype == "float64", f"{col} should be float64, got {tel_df[col].dtype}"

    # ------------------------------------------------------------------
    # Brake → bool  (JSON sends 0/1 integers)
    # ------------------------------------------------------------------

    def test_brake_is_bool(self, tel_df):
        assert tel_df["Brake"].dtype == bool, f"Brake should be bool, got {tel_df['Brake'].dtype}"

    def test_brake_values(self, tel_df):
        assert tel_df["Brake"].tolist() == [False, False, True]

    # ------------------------------------------------------------------
    # Nullable Int64 channels
    # ------------------------------------------------------------------

    def test_ngear_is_nullable_int64(self, tel_df):
        assert tel_df["nGear"].dtype.name == "Int64", (
            f"nGear should be Int64, got {tel_df['nGear'].dtype}"
        )

    def test_drs_is_nullable_int64(self, tel_df):
        assert tel_df["DRS"].dtype.name == "Int64", (
            f"DRS should be Int64, got {tel_df['DRS'].dtype}"
        )

    def test_lap_number_is_nullable_int64(self, tel_df):
        assert tel_df["LapNumber"].dtype.name == "Int64", (
            f"LapNumber should be Int64, got {tel_df['LapNumber'].dtype}"
        )

    def test_lap_number_value(self, tel_df):
        assert (tel_df["LapNumber"] == _LAP_NUM).all()

    # ------------------------------------------------------------------
    # Object (string) channels
    # ------------------------------------------------------------------

    def test_driver_ahead_is_object(self, tel_df):
        assert tel_df["DriverAhead"].dtype == object, (
            f"DriverAhead should be object, got {tel_df['DriverAhead'].dtype}"
        )

    def test_driver_ahead_none_values(self, tel_df):
        assert tel_df["DriverAhead"].isna().all()

    def test_driver_is_object(self, tel_df):
        assert tel_df["Driver"].dtype == object

    def test_driver_value(self, tel_df):
        assert (tel_df["Driver"] == _DRIVER).all()

    # ------------------------------------------------------------------
    # Concrete value spot-checks
    # ------------------------------------------------------------------

    def test_speed_first_value(self, tel_df):
        assert pytest.approx(tel_df["Speed"].iloc[0], rel=1e-4) == 222.14

    def test_rpm_fractional_preserved(self, tel_df):
        assert pytest.approx(tel_df["RPM"].iloc[0], rel=1e-4) == 11422.88

    def test_throttle_zero_value(self, tel_df):
        assert tel_df["Throttle"].iloc[2] == 0.0

    def test_ngear_value(self, tel_df):
        assert tel_df["nGear"].iloc[0] == 5


class TestTelemetryDtypesWithNoneValues:
    """Verify dtype contract still holds when numeric channels contain None."""

    def test_none_gear_becomes_nat_in_int64(self):
        payload = {
            "speed": [200.0, 210.0, 190.0],
            "gear": [5, None, 6],
            "drs": [0, None, 0],
        }
        result = _create_telemetry_df(payload, "VER", 1, "pandas")
        assert result is not None
        assert result["nGear"].dtype.name == "Int64"
        assert result["DRS"].dtype.name == "Int64"
        assert pd.isna(result["nGear"].iloc[1])
        assert pd.isna(result["DRS"].iloc[1])

    def test_none_time_becomes_nat(self):
        payload = {
            "time": [0.0, None, 0.124],
            "speed": [222.0, 223.0, 224.0],
        }
        result = _create_telemetry_df(payload, "VER", 1, "pandas")
        assert result is not None
        assert pd.api.types.is_timedelta64_ns_dtype(result["Time"])
        assert pd.isna(result["Time"].iloc[1])

    def test_none_float_columns_become_nan(self):
        payload = {
            "speed": [222.0, None, 224.0],
            "rpm": [11000.0, None, 10800.0],
        }
        result = _create_telemetry_df(payload, "VER", 1, "pandas")
        assert result is not None
        assert result["Speed"].dtype == "float64"
        assert result["RPM"].dtype == "float64"
        assert pd.isna(result["Speed"].iloc[1])
        assert pd.isna(result["RPM"].iloc[1])

    def test_driver_ahead_none_string_normalised(self):
        """None values (already normalised from "None") stay None in DriverAhead."""
        payload = {
            "speed": [200.0],
            "DriverAhead": [None],
            "DistanceToDriverAhead": [46.4],
        }
        result = _create_telemetry_df(payload, "VER", 1, "pandas")
        assert result is not None
        assert result["DriverAhead"].dtype == object
        assert pd.isna(result["DriverAhead"].iloc[0])
