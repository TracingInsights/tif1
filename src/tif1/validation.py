"""Data validation with Pydantic models."""

import logging
from collections import Counter
from enum import Enum
from functools import lru_cache
from typing import Any, TypeVar

from pydantic import BaseModel, Field, field_validator, model_validator

from .exceptions import InvalidDataError

T = TypeVar("T", bound="ConsistentLengthsMixin")

logger = logging.getLogger(__name__)


class ConsistentLengthsMixin:
    """Mixin that validates all list fields have consistent lengths.

    Subclasses define ``_length_check_fields`` returning the field names to check.
    Empty lists are skipped so optional columns don't trigger false positives.
    """

    def _length_check_fields(self) -> tuple[str, ...]:
        raise NotImplementedError

    def _check_consistent_lengths(self: T, error_label: str) -> T:
        first_len: int | None = None
        for name in self._length_check_fields():
            values = getattr(self, name)
            if not values:
                continue
            current_len = len(values)
            if first_len is None:
                first_len = current_len
            elif current_len != first_len:
                raise ValueError(f"Inconsistent {error_label} lengths")
        return self


# Validation constants
MAX_RPM = 20000
MAX_SPEED = 400  # km/h
MAX_GEAR = 8
MAX_THROTTLE = 100
MAX_ACCELERATION = 500  # m/s²
MIN_YEAR = 2018
MAX_YEAR = 2100


class TireCompound(str, Enum):
    """Valid tire compounds."""

    SOFT = "SOFT"
    MEDIUM = "MEDIUM"
    HARD = "HARD"
    INTERMEDIATE = "INTERMEDIATE"
    WET = "WET"
    UNKNOWN = "UNKNOWN"
    TEST_UNKNOWN = "TEST-UNKNOWN"


class LapStatus(str, Enum):
    """Valid lap status values."""

    VALID = "VALID"
    INVALID = "INVALID"
    OUTLAP = "OUTLAP"
    INLAP = "INLAP"


class SessionType(str, Enum):
    """Valid session types."""

    PRACTICE_1 = "Practice 1"
    PRACTICE_2 = "Practice 2"
    PRACTICE_3 = "Practice 3"
    QUALIFYING = "Qualifying"
    SPRINT = "Sprint"
    SPRINT_QUALIFYING = "Sprint Qualifying"
    SPRINT_SHOOTOUT = "Sprint Shootout"
    RACE = "Race"


class AnomalyType(str, Enum):
    """Types of data anomalies."""

    MISSING_LAPS = "missing_laps"
    DUPLICATE_LAPS = "duplicate_laps"
    OUTLIER_TIMES = "outlier_times"


class Anomaly(BaseModel):
    """Structured anomaly result."""

    type: AnomalyType
    severity: str = Field(..., pattern=r"^(low|medium|high)$")
    description: str
    details: dict[str, Any] = Field(default_factory=dict)


class DriverInfo(BaseModel):
    """Driver information schema."""

    driver: str = Field(..., min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    team: str = Field(..., min_length=1, max_length=100)
    dn: str = Field(...)
    fn: str = Field(...)
    ln: str = Field(...)
    tc: str = Field(...)
    url: str = Field(...)


class DriversData(BaseModel):
    """Drivers data schema."""

    drivers: list[DriverInfo]


class LapData(ConsistentLengthsMixin, BaseModel):
    """Lap data schema."""

    time: list[float | None] = Field(..., min_length=1)
    lap: list[float | None] = Field(..., min_length=1)
    compound: list[str | None] = Field(..., min_length=1)
    stint: list[int | None] = Field(..., min_length=1)
    s1: list[float | None] = Field(..., min_length=1)
    s2: list[float | None] = Field(..., min_length=1)
    s3: list[float | None] = Field(..., min_length=1)
    life: list[int | None] = Field(..., min_length=1)
    pos: list[int | None] = Field(..., min_length=1)
    status: list[str | None] = Field(..., min_length=1)
    pb: list[bool | None] = Field(..., min_length=1)
    qualifying_session: list[str | None] = Field(default_factory=list, alias="qs")
    session_time: list[float | None] = Field(default_factory=list, alias="sesT")
    source_driver: list[str | None] = Field(default_factory=list, alias="drv")
    driver_number: list[str | None] = Field(default_factory=list, alias="dNum")
    pit_out_time: list[float | None] = Field(default_factory=list, alias="pout")
    pit_in_time: list[float | None] = Field(default_factory=list, alias="pin")
    sector1_session_time: list[float | None] = Field(default_factory=list, alias="s1T")
    sector2_session_time: list[float | None] = Field(default_factory=list, alias="s2T")
    sector3_session_time: list[float | None] = Field(default_factory=list, alias="s3T")
    speed_i1: list[float | None] = Field(default_factory=list, alias="vi1")
    speed_i2: list[float | None] = Field(default_factory=list, alias="vi2")
    speed_fl: list[float | None] = Field(default_factory=list, alias="vfl")
    speed_st: list[float | None] = Field(default_factory=list, alias="vst")
    fresh_tyre: list[bool | None] = Field(default_factory=list, alias="fresh")
    source_team: list[str | None] = Field(default_factory=list, alias="team")
    lap_start_time: list[float | None] = Field(default_factory=list, alias="lST")
    lap_start_date: list[str | None] = Field(default_factory=list, alias="lSD")
    deleted: list[bool | None] = Field(default_factory=list, alias="del")
    deleted_reason: list[str | None] = Field(default_factory=list, alias="delR")
    fastf1_generated: list[bool | None] = Field(default_factory=list, alias="ff1G")
    is_accurate: list[bool | None] = Field(default_factory=list, alias="iacc")
    weather_time: list[float | None] = Field(default_factory=list, alias="wT")
    air_temp: list[float | None] = Field(default_factory=list, alias="wAT")
    humidity: list[float | None] = Field(default_factory=list, alias="wH")
    pressure: list[float | None] = Field(default_factory=list, alias="wP")
    rainfall: list[bool | None] = Field(default_factory=list, alias="wR")
    track_temp: list[float | None] = Field(default_factory=list, alias="wTT")
    wind_direction: list[float | None] = Field(default_factory=list, alias="wWD")
    wind_speed: list[float | None] = Field(default_factory=list, alias="wWS")

    model_config = {"populate_by_name": True}

    def _length_check_fields(self) -> tuple[str, ...]:
        return (
            "time",
            "lap",
            "compound",
            "stint",
            "s1",
            "s2",
            "s3",
            "life",
            "pos",
            "status",
            "pb",
            "qualifying_session",
            "session_time",
            "source_driver",
            "driver_number",
            "pit_out_time",
            "pit_in_time",
            "sector1_session_time",
            "sector2_session_time",
            "sector3_session_time",
            "speed_i1",
            "speed_i2",
            "speed_fl",
            "speed_st",
            "fresh_tyre",
            "source_team",
            "lap_start_time",
            "lap_start_date",
            "deleted",
            "deleted_reason",
            "fastf1_generated",
            "is_accurate",
            "weather_time",
            "air_temp",
            "humidity",
            "pressure",
            "rainfall",
            "track_temp",
            "wind_direction",
            "wind_speed",
        )

    @model_validator(mode="after")
    def validate_consistent_lengths(self) -> "LapData":
        """Ensure all lists have the same length."""
        return self._check_consistent_lengths("lap data")

    @field_validator("stint")
    @classmethod
    def validate_stint(cls, v: list[int | None]) -> list[int | None]:
        """Validate stint numbers are positive."""
        for stint in v:
            if stint is None:
                continue
            if stint < 1:
                raise ValueError("Stint numbers must be >= 1")
        return v

    @field_validator("life")
    @classmethod
    def validate_life(cls, v: list[int | None]) -> list[int | None]:
        """Validate tire life is non-negative."""
        for life in v:
            if life is None:
                continue
            if life < 0:
                raise ValueError("Tire life must be >= 0")
        return v


class TelemetryData(ConsistentLengthsMixin, BaseModel):
    """Telemetry data schema for batch validation."""

    tel: dict[str, Any] | None = None

    time: list[float | None] = Field(..., min_length=1)
    speed: list[float | None] = Field(..., min_length=1)
    rpm: list[float | None] = Field(default_factory=list)
    gear: list[int | None] = Field(default_factory=list)
    throttle: list[float | None] = Field(default_factory=list)
    brake: list[bool | None] = Field(default_factory=list)
    drs: list[bool | None] = Field(default_factory=list)
    distance: list[float | None] = Field(default_factory=list)
    rel_distance: list[float | None] = Field(default_factory=list)
    driver_ahead: list[str | None] = Field(default_factory=list, alias="DriverAhead")
    distance_to_driver_ahead: list[float | None] = Field(
        default_factory=list, alias="DistanceToDriverAhead"
    )
    x: list[float | None] = Field(default_factory=list)
    y: list[float | None] = Field(default_factory=list)
    z: list[float | None] = Field(default_factory=list)
    acc_x: list[float | None] = Field(default_factory=list)
    acc_y: list[float | None] = Field(default_factory=list)
    acc_z: list[float | None] = Field(default_factory=list)
    data_key: list[str | None] = Field(default_factory=list, alias="dataKey")

    model_config = {"populate_by_name": True}

    def _length_check_fields(self) -> tuple[str, ...]:
        return (
            "time",
            "speed",
            "rpm",
            "gear",
            "throttle",
            "brake",
            "drs",
            "distance",
            "rel_distance",
            "driver_ahead",
            "distance_to_driver_ahead",
            "x",
            "y",
            "z",
            "acc_x",
            "acc_y",
            "acc_z",
            "data_key",
        )

    @model_validator(mode="before")
    @classmethod
    def _unwrap_tel(cls, data: Any) -> Any:
        if isinstance(data, dict) and "tel" in data and isinstance(data["tel"], dict):
            merged = dict(data)
            tel_dict = merged.pop("tel")
            merged.update(tel_dict)
            merged["tel"] = tel_dict
            return merged
        return data

    @model_validator(mode="after")
    def validate_consistent_lengths(self) -> "TelemetryData":
        """Ensure all non-empty lists have the same length."""
        return self._check_consistent_lengths("telemetry array")


class RaceControlData(ConsistentLengthsMixin, BaseModel):
    """Race control messages schema."""

    time: list[float | None] = Field(..., min_length=1)
    category: list[str | None] = Field(default_factory=list, alias="cat")
    message: list[str | None] = Field(default_factory=list, alias="msg")
    status: list[str | None] = Field(default_factory=list)
    flag: list[str | None] = Field(default_factory=list)
    scope: list[str | None] = Field(default_factory=list)
    sector: list[int | str | None] = Field(default_factory=list)
    racing_number: list[str | None] = Field(default_factory=list, alias="dNum")
    lap: list[int | None] = Field(default_factory=list)

    model_config = {"populate_by_name": True}

    def _length_check_fields(self) -> tuple[str, ...]:
        return (
            "time",
            "category",
            "message",
            "status",
            "flag",
            "scope",
            "sector",
            "racing_number",
            "lap",
        )

    @model_validator(mode="after")
    def validate_consistent_lengths(self) -> "RaceControlData":
        """Ensure all non-empty lists have the same length."""
        return self._check_consistent_lengths("race control data")


class WeatherData(ConsistentLengthsMixin, BaseModel):
    """Session weather schema."""

    time: list[float | None] = Field(..., min_length=1, alias="wT")
    air_temp: list[float | None] = Field(default_factory=list, alias="wAT")
    humidity: list[float | None] = Field(default_factory=list, alias="wH")
    pressure: list[float | None] = Field(default_factory=list, alias="wP")
    rainfall: list[bool | None] = Field(default_factory=list, alias="wR")
    track_temp: list[float | None] = Field(default_factory=list, alias="wTT")
    wind_direction: list[float | None] = Field(default_factory=list, alias="wWD")
    wind_speed: list[float | None] = Field(default_factory=list, alias="wWS")

    model_config = {"populate_by_name": True}

    @model_validator(mode="before")
    @classmethod
    def _normalize_pascalcase_keys(cls, data: Any) -> Any:
        """Convert PascalCase keys from CDN to snake_case for validation."""
        if not isinstance(data, dict):
            return data

        # Map PascalCase keys to snake_case
        pascalcase_map = {
            "Time": "time",
            "AirTemp": "air_temp",
            "Humidity": "humidity",
            "Pressure": "pressure",
            "Rainfall": "rainfall",
            "TrackTemp": "track_temp",
            "WindDirection": "wind_direction",
            "WindSpeed": "wind_speed",
        }

        normalized = {}
        for key, value in data.items():
            normalized_key = pascalcase_map.get(key, key)
            normalized[normalized_key] = value

        return normalized

    def _length_check_fields(self) -> tuple[str, ...]:
        return (
            "time",
            "air_temp",
            "humidity",
            "pressure",
            "rainfall",
            "track_temp",
            "wind_direction",
            "wind_speed",
        )

    @model_validator(mode="after")
    def validate_consistent_lengths(self) -> "WeatherData":
        """Ensure all non-empty lists have the same length."""
        return self._check_consistent_lengths("weather data")


class TelemetryPoint(BaseModel):
    """Telemetry data point validation (deprecated - use TelemetryData for batch)."""

    time: float = Field(..., ge=0, alias="Time")
    rpm: int | None = Field(None, ge=0, le=MAX_RPM, alias="RPM")
    speed: float | None = Field(None, ge=0, le=MAX_SPEED, alias="Speed")
    gear: int | None = Field(None, ge=0, le=MAX_GEAR, alias="nGear")
    throttle: float | None = Field(None, ge=0, le=MAX_THROTTLE, alias="Throttle")
    brake: bool | None = Field(None, alias="Brake")
    drs: bool | None = Field(None, alias="DRS")
    distance: float | None = Field(None, ge=0, alias="Distance")
    rel_distance: float | None = Field(None, ge=0, le=1, alias="RelativeDistance")
    x: float | None = Field(None, alias="X")
    y: float | None = Field(None, alias="Y")
    z: float | None = Field(None, alias="Z")
    acc_x: float | None = Field(
        None, ge=-MAX_ACCELERATION, le=MAX_ACCELERATION, alias="AccelerationX"
    )
    acc_y: float | None = Field(
        None, ge=-MAX_ACCELERATION, le=MAX_ACCELERATION, alias="AccelerationY"
    )
    acc_z: float | None = Field(
        None, ge=-MAX_ACCELERATION, le=MAX_ACCELERATION, alias="AccelerationZ"
    )

    model_config = {"populate_by_name": True}


class SessionData(BaseModel):
    """Session metadata validation."""

    year: int = Field(..., ge=MIN_YEAR, le=MAX_YEAR)
    gp: str = Field(..., min_length=1)
    session: str = Field(..., min_length=1)
    drivers: list[DriverInfo] = Field(default_factory=list)

    @field_validator("session")
    @classmethod
    def validate_session_type(cls, v: str) -> str:
        """Validate session type against known values."""
        valid_sessions = {st.value for st in SessionType}
        if v not in valid_sessions:
            logger.warning(f"Unknown session type: {v}")
        return v


@lru_cache(maxsize=128)
def _get_validation_cache_key(data_type: str, data_hash: int) -> str:
    """Generate cache key for validation results."""
    return f"{data_type}:{data_hash}"


def validate_drivers(data: dict) -> DriversData:
    """Validate drivers data.

    Raises:
        ValidationError: If validation fails
    """
    return DriversData.model_validate(data)


def validate_laps(data: dict) -> LapData:
    """Validate lap data.

    Raises:
        ValidationError: If validation fails
    """
    return LapData.model_validate(data)


def _coerce_optional_bool_list(values: list[Any]) -> list[Any]:
    """Coerce list values to optional bools only when non-bool entries are present."""
    if not values:
        return values

    for value in values:
        if value is not None and not isinstance(value, bool):
            return [bool(v) if v is not None else None for v in values]
    return values


_NULL_LIKE_STRINGS = {"", "none", "null", "nan"}


def _coerce_null_like_string_list(values: list[Any]) -> list[Any]:
    """Convert null-like string tokens to None in any list field."""
    if not values:
        return values

    normalized: list[Any] = []
    changed = False
    for value in values:
        if isinstance(value, str) and value.strip().lower() in _NULL_LIKE_STRINGS:
            normalized.append(None)
            changed = True
        else:
            normalized.append(value)
    return normalized if changed else values


def _normalize_payload_lists(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize payload list values by converting null-like strings to None."""
    normalized = data
    for key, values in data.items():
        if isinstance(values, list):
            coerced = _coerce_null_like_string_list(values)
            if coerced is not values:
                if normalized is data:
                    normalized = data.copy()
                normalized[key] = coerced
    return normalized


def _normalize_lap_data(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize lap payloads by converting null-like strings to None."""
    return _normalize_payload_lists(data)


def _normalize_telemetry_data(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize telemetry payloads by converting null-like strings to None."""
    return _normalize_payload_lists(data)


def validate_telemetry(data: dict) -> TelemetryData:
    """Validate telemetry data in batch (faster than point-by-point).

    Raises:
        ValidationError: If validation fails
    """
    tel_data = _normalize_telemetry_data(data)

    brake_values = tel_data.get("brake")
    if isinstance(brake_values, list):
        coerced_brake = _coerce_optional_bool_list(brake_values)
        if coerced_brake is not brake_values:
            tel_data = tel_data.copy()
            tel_data["brake"] = coerced_brake

    drs_values = tel_data.get("drs")
    if isinstance(drs_values, list):
        coerced_drs = _coerce_optional_bool_list(drs_values)
        if coerced_drs is not drs_values:
            tel_data = tel_data.copy()
            tel_data["drs"] = coerced_drs

    return TelemetryData.model_validate(tel_data)


def validate_race_control(data: dict) -> RaceControlData:
    """Validate race control payload."""
    rcm_data = _normalize_payload_lists(data)
    return RaceControlData.model_validate(rcm_data)


def validate_weather(data: dict) -> WeatherData:
    """Validate weather payload."""
    weather_data = _normalize_payload_lists(data)
    return WeatherData.model_validate(weather_data)


def validate_lap_data(data: dict, strict: bool = False) -> dict:
    """Validate lap data with quality checks.

    Args:
        data: Lap data dictionary
        strict: If True, raise on validation errors; if False, return original data

    Returns:
        Validated data dictionary

    Raises:
        InvalidDataError: If strict=True and validation fails
    """
    try:
        normalized_data = _normalize_lap_data(data)
        validated = LapData.model_validate(normalized_data)
        return validated.model_dump()
    except Exception as e:
        if strict:
            raise InvalidDataError(reason=f"Lap data validation failed: {e}")
        logger.debug(f"Lap validation failed (non-strict): {e}")
        return data


def validate_telemetry_data(data: dict, strict: bool = False) -> dict:
    """Validate telemetry data in batch (replaces point-by-point validation).

    Args:
        data: Telemetry data dictionary with arrays
        strict: If True, raise on validation errors; if False, return original data

    Returns:
        Validated data dictionary

    Raises:
        InvalidDataError: If strict=True and validation fails
    """
    try:
        validated = validate_telemetry(data)
        return validated.model_dump()
    except Exception as e:
        if strict:
            raise InvalidDataError(reason=f"Telemetry validation failed: {e}")
        logger.debug(f"Telemetry validation failed (non-strict): {e}")
        return data


def validate_race_control_data(data: dict, strict: bool = False) -> dict:
    """Validate race control data arrays."""
    try:
        validated = validate_race_control(data)
        return validated.model_dump()
    except Exception as e:
        if strict:
            raise InvalidDataError(reason=f"Race control validation failed: {e}")
        logger.debug(f"Race control validation failed (non-strict): {e}")
        return data


def validate_weather_data(data: dict, strict: bool = False) -> dict:
    """Validate weather data arrays."""
    try:
        validated = validate_weather(data)
        return validated.model_dump()
    except Exception as e:
        if strict:
            raise InvalidDataError(reason=f"Weather validation failed: {e}")
        logger.debug(f"Weather validation failed (non-strict): {e}")
        return data


def validate_driver_info(data: dict, strict: bool = False) -> dict:
    """Validate driver information.

    Args:
        data: Driver info dictionary
        strict: If True, raise on validation errors; if False, return original data

    Returns:
        Validated data dictionary

    Raises:
        InvalidDataError: If strict=True and validation fails
    """
    try:
        validated = DriverInfo.model_validate(data)
        return validated.model_dump()
    except Exception as e:
        if strict:
            raise InvalidDataError(reason=f"Driver info validation failed: {e}")
        logger.debug(f"Driver info validation failed (non-strict): {e}")
        return data


def detect_lap_anomalies(laps: list[dict]) -> list[Anomaly]:
    """Detect anomalies in lap data with structured results.

    Args:
        laps: List of lap dictionaries

    Returns:
        List of structured Anomaly objects
    """
    anomalies: list[Anomaly] = []

    if not laps:
        return anomalies

    lap_numbers = []
    lap_times = []
    for lap in laps:
        lap_num = lap.get("lap") or lap.get("LapNumber")
        if lap_num is not None:
            lap_numbers.append(int(lap_num))

        lap_time = lap.get("time") or lap.get("LapTime")
        if isinstance(lap_time, int | float) and lap_time > 0:
            lap_times.append(lap_time)

    # Check for missing laps
    if lap_numbers:
        expected = set(range(min(lap_numbers), max(lap_numbers) + 1))
        actual = set(lap_numbers)
        missing = sorted(expected - actual)
        if missing:
            anomalies.append(
                Anomaly(
                    type=AnomalyType.MISSING_LAPS,
                    severity="medium",
                    description=f"Missing {len(missing)} lap(s)",
                    details={"missing_laps": missing},
                )
            )

    # Check for duplicate laps
    if lap_numbers:
        lap_counts = Counter(lap_numbers)
        duplicates = sorted(num for num, count in lap_counts.items() if count > 1)
    else:
        duplicates = []

    if duplicates:
        anomalies.append(
            Anomaly(
                type=AnomalyType.DUPLICATE_LAPS,
                severity="high",
                description="Duplicate lap numbers detected",
                details={"duplicate_laps": duplicates},
            )
        )

    if len(lap_times) >= 3:  # Need at least 3 laps for meaningful outlier detection
        avg_time = sum(lap_times) / len(lap_times)
        # More lenient outlier detection (3x instead of 2x)
        outliers = [t for t in lap_times if t > avg_time * 3]
        if outliers:
            anomalies.append(
                Anomaly(
                    type=AnomalyType.OUTLIER_TIMES,
                    severity="low",
                    description=f"{len(outliers)} outlier lap time(s) detected",
                    details={"outlier_count": len(outliers), "average_time": round(avg_time, 3)},
                )
            )

    return anomalies
