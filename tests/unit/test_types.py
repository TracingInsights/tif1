"""Tests for types module."""

from tif1.types import (
    BackendType,
    CompoundType,
    DataFrame,
    DataFrameProtocol,
    DriverInfoDict,
    LapDataDict,
    RaceControlDataDict,
    SessionType,
    TelemetryDataDict,
    TrackStatusType,
    WeatherDataDict,
)


class TestTypeDefinitions:
    """Test type definitions are importable and usable."""

    def test_lap_data_dict(self):
        lap: LapDataDict = {"LapNumber": 1, "Driver": "VER", "Team": "Red Bull"}
        assert lap["LapNumber"] == 1
        assert lap["Driver"] == "VER"

    def test_lap_data_dict_optional_fields(self):
        lap: LapDataDict = {
            "LapNumber": 1,
            "Driver": "VER",
            "Team": "Red Bull",
            "LapTime": 90.5,
            "Sector1Time": 30.0,
            "Sector2Time": 30.0,
            "Sector3Time": 30.5,
            "Compound": "SOFT",
            "Stint": 1,
            "TyreLife": 5,
            "Position": 1,
            "TrackStatus": "1",
            "IsPersonalBest": True,
            "QualifyingSession": "Q3",
        }
        assert lap["LapTime"] == 90.5

    def test_telemetry_data_dict(self):
        tel: TelemetryDataDict = {"Time": 1.0}
        assert tel["Time"] == 1.0

    def test_telemetry_data_dict_optional_fields(self):
        tel: TelemetryDataDict = {
            "Time": 1.0,
            "RPM": 12000,
            "Speed": 300.0,
            "nGear": 8,
            "Throttle": 100.0,
            "Brake": 0,
            "DRS": 1,
            "Distance": 500.0,
            "RelativeDistance": 0.5,
            "X": 1.0,
            "Y": 2.0,
            "Z": 3.0,
            "AccelerationX": 0.1,
            "AccelerationY": 0.2,
            "AccelerationZ": 0.3,
            "DataKey": "key",
        }
        assert tel["Speed"] == 300.0

    def test_driver_info_dict(self):
        info: DriverInfoDict = {"driver": "VER", "team": "Red Bull"}
        assert info["driver"] == "VER"

    def test_driver_info_dict_with_number(self):
        info: DriverInfoDict = {"driver": "VER", "team": "Red Bull", "number": 1}
        assert info["number"] == 1

    def test_driver_info_dict_with_extended_fields(self):
        info: DriverInfoDict = {
            "driver": "VER",
            "team": "Red Bull",
            "dn": "1",
            "fn": "Max",
            "ln": "Verstappen",
            "tc": "#3671C6",
            "url": "https://example.com/ver.png",
        }
        assert info["dn"] == "1"
        assert info["fn"] == "Max"
        assert info["ln"] == "Verstappen"

    def test_race_control_data_dict(self):
        rcm: RaceControlDataDict = {"Time": 10.0, "Category": "Track", "Message": "Green flag"}
        assert rcm["Time"] == 10.0
        assert rcm["Category"] == "Track"

    def test_weather_data_dict(self):
        weather: WeatherDataDict = {"Time": 10.0, "AirTemp": 25.0, "Rainfall": False}
        assert weather["Time"] == 10.0
        assert weather["AirTemp"] == 25.0

    def test_session_type_values(self):
        valid_sessions: list[SessionType] = [
            "Practice 1",
            "Practice 2",
            "Practice 3",
            "Qualifying",
            "Sprint",
            "Sprint Qualifying",
            "Sprint Shootout",
            "Race",
        ]
        assert len(valid_sessions) == 8

    def test_backend_type_values(self):
        backends: list[BackendType] = ["pandas", "polars"]
        assert "pandas" in backends
        assert "polars" in backends

    def test_compound_type_values(self):
        compounds: list[CompoundType] = [
            "SOFT",
            "MEDIUM",
            "HARD",
            "INTERMEDIATE",
            "WET",
            "UNKNOWN",
            "TEST_UNKNOWN",
        ]
        assert len(compounds) == 7

    def test_track_status_type_values(self):
        statuses: list[TrackStatusType] = ["1", "2", "4", "5", "6", "7"]
        assert len(statuses) == 6

    def test_dataframe_type_exists(self):
        assert DataFrame is not None

    def test_dataframe_protocol_has_methods(self):
        assert hasattr(DataFrameProtocol, "shape")
        assert hasattr(DataFrameProtocol, "columns")
        assert hasattr(DataFrameProtocol, "head")
        assert hasattr(DataFrameProtocol, "__len__")
        assert hasattr(DataFrameProtocol, "__getitem__")

    def test_all_exports(self):
        from tif1 import types

        assert "BackendType" in types.__all__
        assert "CompoundType" in types.__all__
        assert "DataFrame" in types.__all__
        assert "DataFrameProtocol" in types.__all__
        assert "DriverInfoDict" in types.__all__
        assert "LapDataDict" in types.__all__
        assert "RaceControlDataDict" in types.__all__
        assert "SessionType" in types.__all__
        assert "TelemetryDataDict" in types.__all__
        assert "TrackStatusType" in types.__all__
        assert "WeatherDataDict" in types.__all__
