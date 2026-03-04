"""Tests for validation module."""

import pytest
from pydantic import ValidationError

from tif1.validation import (
    AnomalyType,
    DriverInfo,
    detect_lap_anomalies,
    validate_drivers,
    validate_lap_data,
    validate_laps,
    validate_race_control,
    validate_race_control_data,
    validate_telemetry,
    validate_weather,
    validate_weather_data,
)


class TestValidation:
    """Test data validation."""

    def test_driver_info_valid(self):
        """Test valid driver info."""
        driver = DriverInfo(
            driver="VER",
            team="Red Bull Racing",
            dn="1",
            fn="Max",
            ln="Verstappen",
            tc="3671C6",
            url="https://example.com/ver.png",
        )
        assert driver.driver == "VER"
        assert driver.team == "Red Bull Racing"
        assert driver.dn == "1"

    def test_driver_info_invalid_code(self):
        """Test invalid driver code."""
        with pytest.raises(ValidationError):
            DriverInfo(
                driver="VERSTAPPEN",
                team="Red Bull Racing",
                dn="1",
                fn="Max",
                ln="Verstappen",
                tc="3671C6",
                url="",
            )

        with pytest.raises(ValidationError):
            DriverInfo(
                driver="VE",
                team="Red Bull Racing",
                dn="1",
                fn="Max",
                ln="Verstappen",
                tc="3671C6",
                url="",
            )

        with pytest.raises(ValidationError):
            DriverInfo(
                driver="VERR",
                team="Red Bull Racing",
                dn="1",
                fn="Max",
                ln="Verstappen",
                tc="3671C6",
                url="",
            )

    def test_driver_info_invalid_team_empty(self):
        """Test invalid team (empty)."""
        with pytest.raises(ValidationError):
            DriverInfo(
                driver="VER",
                team="",
                dn="1",
                fn="Max",
                ln="Verstappen",
                tc="3671C6",
                url="",
            )

    def test_drivers_data_valid(self):
        """Test valid drivers data."""
        data = {
            "drivers": [
                {
                    "driver": "VER",
                    "team": "Red Bull Racing",
                    "dn": "1",
                    "fn": "Max",
                    "ln": "Verstappen",
                    "tc": "3671C6",
                    "url": "",
                },
                {
                    "driver": "HAM",
                    "team": "Mercedes",
                    "dn": "44",
                    "fn": "Lewis",
                    "ln": "Hamilton",
                    "tc": "00D2BE",
                    "url": "",
                },
            ]
        }
        drivers = validate_drivers(data)
        assert len(drivers.drivers) == 2

    def test_validate_drivers_preserves_fields(self):
        """Validates driver fields are preserved in model dump."""
        data = {
            "drivers": [
                {
                    "driver": "VER",
                    "team": "Red Bull Racing",
                    "dn": "1",
                    "fn": "Max",
                    "ln": "Verstappen",
                    "tc": "#3671C6",
                    "url": "https://example.com/ver.png",
                }
            ]
        }

        drivers = validate_drivers(data)
        dumped = drivers.model_dump()

        assert dumped["drivers"][0]["dn"] == "1"
        assert dumped["drivers"][0]["fn"] == "Max"
        assert dumped["drivers"][0]["ln"] == "Verstappen"
        assert dumped["drivers"][0]["tc"] == "#3671C6"
        assert dumped["drivers"][0]["url"] == "https://example.com/ver.png"

    def test_lap_data_valid(self):
        """Test valid lap data."""
        data = {
            "time": [90.123, 89.456],
            "lap": [1.0, 2.0],
            "compound": ["SOFT", "SOFT"],
            "stint": [1, 1],
            "s1": [30.1, 29.9],
            "s2": [35.2, 34.8],
            "s3": [24.8, 24.7],
            "life": [1, 2],
            "pos": [1, 1],
            "status": ["OK", "OK"],
            "pb": [True, True],
        }
        laps = validate_laps(data)
        assert len(laps.time) == 2

    def test_lap_data_with_none(self):
        """Test lap data with None values."""
        data = {
            "time": [90.123, None],
            "lap": [1.0, 2.0],
            "compound": ["SOFT", "SOFT"],
            "stint": [1, 1],
            "s1": [30.1, None],
            "s2": [35.2, None],
            "s3": [24.8, None],
            "life": [1, 2],
            "pos": [1, None],
            "status": ["OK", "PIT"],
            "pb": [True, False],
        }
        laps = validate_laps(data)
        assert laps.time[1] is None

    def test_validate_lap_data_coerces_none_strings_without_mutating_input(self):
        """Converts null-like strings in optional lap fields while preserving input."""
        data = {
            "time": [90.123, "None"],
            "lap": [1.0, 2.0],
            "compound": ["SOFT", "SOFT"],
            "stint": [1, 1],
            "s1": [30.1, "None"],
            "s2": [35.2, "null"],
            "s3": [24.8, "  nan "],
            "life": [1, 2],
            "pos": [1, "None"],
            "status": ["OK", "PIT"],
            "pb": [True, False],
        }
        original = {k: v[:] if isinstance(v, list) else v for k, v in data.items()}

        validated = validate_lap_data(data, strict=True)

        assert validated["time"][1] is None
        assert validated["s1"][1] is None
        assert validated["s2"][1] is None
        assert validated["s3"][1] is None
        assert validated["pos"][1] is None
        assert data == original

    def test_validate_lap_data_coerces_none_strings_for_all_fields(self):
        """Converts null-like strings for every lap list field."""
        data = {
            "time": ["None"],
            "lap": ["None"],
            "compound": ["None"],
            "stint": ["None"],
            "s1": ["None"],
            "s2": ["None"],
            "s3": ["None"],
            "life": ["None"],
            "pos": ["None"],
            "status": ["None"],
            "pb": ["None"],
        }

        validated = validate_lap_data(data, strict=True)

        for key in data:
            assert validated[key] == [None]

    def test_validate_lap_data_preserves_extended_alias_fields(self):
        """Accepts aliased CDN lap fields and normalizes them in validated output."""
        data = {
            "time": [90.123, 89.456],
            "lap": [1.0, 2.0],
            "compound": ["SOFT", "SOFT"],
            "stint": [1, 1],
            "s1": [30.1, 29.9],
            "s2": [35.2, 34.8],
            "s3": [24.8, 24.7],
            "life": [1, 2],
            "pos": [1, 1],
            "status": ["OK", "OK"],
            "pb": [True, True],
            "sesT": [100.0, 190.0],
            "drv": ["VER", "VER"],
            "dNum": ["1", "1"],
            "pout": [None, None],
            "pin": [None, None],
            "s1T": [30.0, 120.0],
            "s2T": [65.0, 155.0],
            "s3T": [89.8, 179.7],
            "vi1": [280.0, 281.0],
            "vi2": [290.0, 291.0],
            "vfl": [300.0, 301.0],
            "vst": [305.0, 306.0],
            "fresh": [True, False],
            "team": ["Red Bull", "Red Bull"],
            "lST": [0.0, 100.0],
            "lSD": ["2025-03-01T10:00:00", "2025-03-01T10:01:40"],
            "del": [False, False],
            "delR": [None, None],
            "ff1G": [False, False],
            "iacc": [True, True],
            "wT": [100.0, 190.0],
            "wAT": [25.1, 25.3],
            "wH": [40.0, 41.0],
            "wP": [1012.0, 1011.8],
            "wR": [False, False],
            "wTT": [32.5, 32.7],
            "wWD": [180.0, 181.0],
            "wWS": [2.2, 2.1],
        }

        validated = validate_lap_data(data, strict=True)

        assert validated["session_time"] == [100.0, 190.0]
        assert validated["source_driver"] == ["VER", "VER"]
        assert validated["driver_number"] == ["1", "1"]
        assert validated["source_team"] == ["Red Bull", "Red Bull"]
        assert validated["weather_time"] == [100.0, 190.0]
        assert validated["air_temp"] == [25.1, 25.3]
        assert validated["track_temp"] == [32.5, 32.7]
        assert validated["wind_direction"] == [180, 181]
        assert validated["wind_speed"] == [2.2, 2.1]
        assert "sesT" not in validated
        assert "drv" not in validated
        assert "team" not in validated
        assert "wAT" not in validated

    def test_validate_lap_data_accepts_normalized_extended_fields(self):
        """Accepts normalized snake_case extended lap fields."""
        data = {
            "time": [90.123],
            "lap": [1.0],
            "compound": ["SOFT"],
            "stint": [1],
            "s1": [30.1],
            "s2": [35.2],
            "s3": [24.8],
            "life": [1],
            "pos": [1],
            "status": ["OK"],
            "pb": [True],
            "session_time": [100.0],
            "source_driver": ["VER"],
            "driver_number": ["1"],
            "pit_out_time": [None],
            "pit_in_time": [None],
            "sector1_session_time": [30.0],
            "sector2_session_time": [65.0],
            "sector3_session_time": [89.8],
            "speed_i1": [280.0],
            "speed_i2": [290.0],
            "speed_fl": [300.0],
            "speed_st": [305.0],
            "fresh_tyre": [True],
            "source_team": ["Red Bull"],
            "lap_start_time": [0.0],
            "lap_start_date": ["2025-03-01T10:00:00"],
            "deleted": [False],
            "deleted_reason": [None],
            "fastf1_generated": [False],
            "is_accurate": [True],
            "weather_time": [100.0],
            "air_temp": [25.1],
            "humidity": [40.0],
            "pressure": [1012.0],
            "rainfall": [False],
            "track_temp": [32.5],
            "wind_direction": [180.0],
            "wind_speed": [2.2],
        }

        validated = validate_lap_data(data, strict=True)

        assert validated["session_time"] == [100.0]
        assert validated["source_driver"] == ["VER"]
        assert validated["driver_number"] == ["1"]
        assert validated["weather_time"] == [100.0]
        assert validated["air_temp"] == [25.1]

    def test_validate_laps_rejects_mismatched_extended_lap_lengths(self):
        """Length mismatches in extended lap fields fail model validation."""
        data = {
            "time": [90.123, 89.456],
            "lap": [1.0, 2.0],
            "compound": ["SOFT", "SOFT"],
            "stint": [1, 1],
            "s1": [30.1, 29.9],
            "s2": [35.2, 34.8],
            "s3": [24.8, 24.7],
            "life": [1, 2],
            "pos": [1, 1],
            "status": ["OK", "OK"],
            "pb": [True, True],
            "vi1": [280.0],  # Mismatched length
        }

        with pytest.raises(ValidationError):
            validate_laps(data)

    def test_telemetry_data_valid(self):
        """Test valid telemetry data."""
        data = {
            "time": [0.0, 0.1, 0.2],
            "speed": [100.0, 150.0, 200.0],
            "throttle": [50.0, 75.0, 100.0],
            "brake": [False, False, False],
        }
        telemetry = validate_telemetry(data)
        assert len(telemetry.time) == 3

    def test_telemetry_data_missing_required(self):
        """Test telemetry data missing required fields."""
        data = {
            "throttle": [50.0, 75.0, 100.0],
        }
        with pytest.raises(ValidationError):
            validate_telemetry(data)

    def test_validate_telemetry_converts_int_flags_without_mutating_input(self):
        """Converts int flags to bool while preserving caller input data."""
        data = {
            "time": [0.0, 0.1, 0.2],
            "speed": [100.0, 150.0, 200.0],
            "brake": [0, 1, None],
            "drs": [1, 0, None],
        }
        original = {k: v[:] if isinstance(v, list) else v for k, v in data.items()}

        telemetry = validate_telemetry(data)

        assert telemetry.brake == [False, True, None]
        assert telemetry.drs == [True, False, None]
        assert data == original

    def test_validate_telemetry_bool_flags_without_mutating_input(self):
        """Leaves bool flags unchanged while preserving caller input data."""
        data = {
            "time": [0.0, 0.1, 0.2],
            "speed": [100.0, 150.0, 200.0],
            "brake": [False, True, None],
            "drs": [True, False, None],
        }
        original = {k: v[:] if isinstance(v, list) else v for k, v in data.items()}

        telemetry = validate_telemetry(data)

        assert telemetry.brake == [False, True, None]
        assert telemetry.drs == [True, False, None]
        assert data == original

    def test_validate_telemetry_accepts_fractional_rpm_without_mutating_input(self):
        """Accepts fractional rpm values while preserving caller input data."""
        data = {
            "time": [0.0, 0.1, 0.2],
            "speed": [100.0, 150.0, 200.0],
            "rpm": [9512.5183296, 8881.606784, None],
        }
        original = {k: v[:] if isinstance(v, list) else v for k, v in data.items()}

        telemetry = validate_telemetry(data)

        assert telemetry.rpm == [9512.5183296, 8881.606784, None]
        assert data == original

    def test_validate_telemetry_coerces_none_strings_for_all_fields(self):
        """Converts null-like strings for telemetry fields."""
        data = {
            "time": ["None"],
            "speed": ["None"],
            "rpm": ["None"],
            "gear": ["None"],
            "throttle": ["None"],
            "brake": ["None"],
            "drs": ["None"],
            "distance": ["None"],
            "rel_distance": ["None"],
            "x": ["None"],
            "y": ["None"],
            "z": ["None"],
            "acc_x": ["None"],
            "acc_y": ["None"],
            "acc_z": ["None"],
        }
        original = {k: v[:] if isinstance(v, list) else v for k, v in data.items()}

        telemetry = validate_telemetry(data)

        assert telemetry.time == [None]
        assert telemetry.speed == [None]
        assert telemetry.rpm == [None]
        assert telemetry.gear == [None]
        assert telemetry.throttle == [None]
        assert telemetry.brake == [None]
        assert telemetry.drs == [None]
        assert telemetry.distance == [None]
        assert telemetry.rel_distance == [None]
        assert telemetry.x == [None]
        assert telemetry.y == [None]
        assert telemetry.z == [None]
        assert telemetry.acc_x == [None]
        assert telemetry.acc_y == [None]
        assert telemetry.acc_z == [None]
        assert data == original

    def test_validate_telemetry_preserves_driver_ahead_alias_fields(self):
        """Accepts aliased CDN fields and normalizes them in model dumps."""
        data = {
            "time": [0.0, 0.1, 0.2],
            "speed": [100.0, 150.0, 200.0],
            "DriverAhead": ["VER", "VER", "HAM"],
            "DistanceToDriverAhead": [10.0, 9.7, 8.4],
            "dataKey": ["k1", "k2", "k3"],
        }
        original = {k: v[:] if isinstance(v, list) else v for k, v in data.items()}

        telemetry = validate_telemetry(data)
        dumped = telemetry.model_dump()

        assert telemetry.driver_ahead == ["VER", "VER", "HAM"]
        assert telemetry.distance_to_driver_ahead == [10.0, 9.7, 8.4]
        assert telemetry.data_key == ["k1", "k2", "k3"]
        assert dumped["driver_ahead"] == ["VER", "VER", "HAM"]
        assert dumped["distance_to_driver_ahead"] == [10.0, 9.7, 8.4]
        assert dumped["data_key"] == ["k1", "k2", "k3"]
        assert "DriverAhead" not in dumped
        assert "DistanceToDriverAhead" not in dumped
        assert "dataKey" not in dumped
        assert data == original

    def test_validate_telemetry_accepts_normalized_driver_ahead_fields(self):
        """Accepts normalized snake_case fields when populating the model."""
        data = {
            "time": [0.0, 0.1],
            "speed": [100.0, 150.0],
            "driver_ahead": ["VER", "VER"],
            "distance_to_driver_ahead": [10.0, 9.8],
            "data_key": ["k1", "k2"],
        }

        telemetry = validate_telemetry(data)
        dumped = telemetry.model_dump()

        assert dumped["driver_ahead"] == ["VER", "VER"]
        assert dumped["distance_to_driver_ahead"] == [10.0, 9.8]
        assert dumped["data_key"] == ["k1", "k2"]

    def test_validate_telemetry_rejects_mismatched_driver_ahead_lengths(self):
        """Length mismatches in new telemetry arrays still fail validation."""
        data = {
            "time": [0.0, 0.1, 0.2],
            "speed": [100.0, 150.0, 200.0],
            "DriverAhead": ["VER", "HAM"],
            "DistanceToDriverAhead": [10.0, 9.8, 9.1],
            "dataKey": ["k1", "k2", "k3"],
        }

        with pytest.raises(ValidationError):
            validate_telemetry(data)

    def test_validate_race_control_preserves_alias_fields(self):
        """Accepts race-control aliases and normalizes output keys."""
        data = {
            "time": [10.0, 20.0],
            "cat": ["Other", "Track"],
            "msg": ["Incident", "Green flag"],
            "status": [None, "Clear"],
            "flag": [None, "GREEN"],
            "scope": [None, "Sector"],
            "sector": [None, 2],
            "dNum": [None, "1"],
            "lap": [5, 6],
        }

        rcm = validate_race_control(data)
        dumped = rcm.model_dump()

        assert dumped["category"] == ["Other", "Track"]
        assert dumped["message"] == ["Incident", "Green flag"]
        assert dumped["racing_number"] == [None, "1"]
        assert "cat" not in dumped
        assert "msg" not in dumped
        assert "dNum" not in dumped

    def test_validate_race_control_accepts_normalized_fields(self):
        """Accepts normalized snake_case race-control fields."""
        data = {
            "time": [10.0],
            "category": ["Other"],
            "message": ["Incident"],
            "status": [None],
            "flag": [None],
            "scope": [None],
            "sector": [None],
            "racing_number": [None],
            "lap": [5],
        }

        rcm = validate_race_control(data)
        assert rcm.model_dump()["category"] == ["Other"]

    def test_validate_race_control_rejects_mismatched_lengths(self):
        """Length mismatches in race-control arrays fail strict validation."""
        data = {
            "time": [10.0, 20.0],
            "cat": ["Other"],
            "msg": ["Incident", "Green flag"],
            "lap": [5, 6],
        }

        with pytest.raises(ValidationError):
            validate_race_control(data)

    def test_validate_weather_preserves_alias_fields(self):
        """Accepts weather aliases and normalizes output keys."""
        data = {
            "wT": [10.0, 20.0],
            "wAT": [25.0, 25.1],
            "wH": [40.0, 41.0],
            "wP": [1012.0, 1011.9],
            "wR": [False, False],
            "wTT": [30.0, 30.2],
            "wWD": [180.0, 181.0],
            "wWS": [2.2, 2.0],
        }

        weather = validate_weather(data)
        dumped = weather.model_dump()

        assert dumped["time"] == [10.0, 20.0]
        assert dumped["air_temp"] == [25.0, 25.1]
        assert dumped["rainfall"] == [False, False]
        assert "wT" not in dumped
        assert "wAT" not in dumped

    def test_validate_weather_accepts_normalized_fields(self):
        """Accepts normalized snake_case weather fields."""
        data = {
            "time": [10.0],
            "air_temp": [25.0],
            "humidity": [40.0],
            "pressure": [1012.0],
            "rainfall": [False],
            "track_temp": [30.0],
            "wind_direction": [180],
            "wind_speed": [2.2],
        }

        weather = validate_weather(data)
        assert weather.model_dump()["track_temp"] == [30.0]

    def test_validate_weather_rejects_mismatched_lengths(self):
        """Length mismatches in weather arrays fail strict validation."""
        data = {
            "wT": [10.0, 20.0],
            "wAT": [25.0],
            "wH": [40.0, 41.0],
        }

        with pytest.raises(ValidationError):
            validate_weather(data)

    def test_validate_race_control_data_non_strict_fallback(self):
        """Non-strict race-control validation returns original payload on failure."""
        data = {"time": [10.0, 20.0], "cat": ["Other"]}
        validated = validate_race_control_data(data, strict=False)
        assert validated == data

    def test_validate_weather_data_non_strict_fallback(self):
        """Non-strict weather validation returns original payload on failure."""
        data = {"wT": [10.0, 20.0], "wAT": [25.0]}
        validated = validate_weather_data(data, strict=False)
        assert validated == data

    def test_validate_drivers_invalid(self):
        """Test validate_drivers with invalid data."""
        with pytest.raises(ValidationError):
            validate_drivers({"drivers": [{"driver": "TOOLONG", "team": "Team", "number": 1}]})


class TestLapAnomalyDetection:
    """Test lap anomaly detection."""

    def test_detects_missing_duplicate_and_outlier_laps(self):
        """Detects all supported anomaly types."""
        laps = [
            {"lap": 1, "time": 1.0},
            {"lap": 2, "time": 1.0},
            {"lap": 2, "time": 1.0},
            {"lap": 4, "time": 500.0},
        ]

        anomalies = detect_lap_anomalies(laps)
        anomaly_by_type = {a.type: a for a in anomalies}

        assert AnomalyType.MISSING_LAPS in anomaly_by_type
        assert anomaly_by_type[AnomalyType.MISSING_LAPS].details["missing_laps"] == [3]

        assert AnomalyType.DUPLICATE_LAPS in anomaly_by_type
        assert anomaly_by_type[AnomalyType.DUPLICATE_LAPS].details["duplicate_laps"] == [2]

        assert AnomalyType.OUTLIER_TIMES in anomaly_by_type
        assert anomaly_by_type[AnomalyType.OUTLIER_TIMES].details["outlier_count"] == 1

    def test_detect_lap_anomalies_empty_input(self):
        """Returns an empty anomaly list for empty input."""
        assert detect_lap_anomalies([]) == []
