"""Type stubs for tif1 with DataFrame column types."""

import datetime
from typing import Literal, Protocol, TypedDict, Union

import pandas as pd
from typing_extensions import NotRequired

try:
    import polars as pl

    DataFrame = Union[pd.DataFrame, pl.DataFrame]
except ImportError:
    DataFrame = pd.DataFrame


# Lap data column types
class LapDataDict(TypedDict):
    """Type definition for lap data dictionary (matches _COLUMNS dtype contract)."""

    # Core identity
    Driver: str
    DriverNumber: str
    Team: str

    # Session time (timedelta from session start)
    Time: datetime.timedelta

    # Lap timing (all timedelta, seconds → timedelta)
    LapTime: NotRequired[datetime.timedelta | None]
    LapNumber: NotRequired[float | None]
    Stint: NotRequired[float | None]
    PitOutTime: NotRequired[datetime.timedelta | None]
    PitInTime: NotRequired[datetime.timedelta | None]
    Sector1Time: NotRequired[datetime.timedelta | None]
    Sector2Time: NotRequired[datetime.timedelta | None]
    Sector3Time: NotRequired[datetime.timedelta | None]
    Sector1SessionTime: NotRequired[datetime.timedelta | None]
    Sector2SessionTime: NotRequired[datetime.timedelta | None]
    Sector3SessionTime: NotRequired[datetime.timedelta | None]
    LapStartTime: NotRequired[datetime.timedelta | None]
    LapStartDate: NotRequired[datetime.datetime | None]

    # Speed traps (float64)
    SpeedI1: NotRequired[float | None]
    SpeedI2: NotRequired[float | None]
    SpeedFL: NotRequired[float | None]
    SpeedST: NotRequired[float | None]

    # Tyre info
    Compound: NotRequired[str | None]
    TyreLife: NotRequired[float | None]
    FreshTyre: NotRequired[bool]

    # Session/lap metadata
    TrackStatus: NotRequired[str | None]
    Position: NotRequired[float | None]
    IsPersonalBest: NotRequired[bool]
    Deleted: NotRequired[bool | None]
    DeletedReason: NotRequired[str | None]
    FastF1Generated: NotRequired[bool]
    IsAccurate: NotRequired[bool]

    # Derived
    LapTimeSeconds: NotRequired[float | None]

    # Per-lap weather data
    WeatherTime: NotRequired[datetime.timedelta | None]
    AirTemp: NotRequired[float | None]
    Humidity: NotRequired[float | None]
    Pressure: NotRequired[float | None]
    Rainfall: NotRequired[bool]
    TrackTemp: NotRequired[float | None]
    WindDirection: NotRequired[int | None]
    WindSpeed: NotRequired[float | None]


# Telemetry data column types
class TelemetryDataDict(TypedDict):
    """Type definition for telemetry data dictionary."""

    Time: float
    RPM: NotRequired[int | None]
    Speed: NotRequired[float | None]
    nGear: NotRequired[int | None]
    Throttle: NotRequired[float | None]
    Brake: NotRequired[int | None]
    DRS: NotRequired[int | None]
    Distance: NotRequired[float | None]
    RelativeDistance: NotRequired[float | None]
    DriverAhead: NotRequired[str | None]
    DistanceToDriverAhead: NotRequired[float | None]
    X: NotRequired[float | None]
    Y: NotRequired[float | None]
    Z: NotRequired[float | None]
    AccelerationX: NotRequired[float | None]
    AccelerationY: NotRequired[float | None]
    AccelerationZ: NotRequired[float | None]
    DataKey: NotRequired[str | None]


# Driver info type
class DriverInfoDict(TypedDict):
    """Type definition for driver info dictionary."""

    driver: str
    team: str
    dn: str
    fn: str
    ln: str
    tc: str
    url: str


class RaceControlDataDict(TypedDict):
    """Type definition for race control data dictionary."""

    Time: float
    Category: NotRequired[str | None]
    Message: NotRequired[str | None]
    Status: NotRequired[str | None]
    Flag: NotRequired[str | None]
    Scope: NotRequired[str | None]
    Sector: NotRequired[int | str | None]
    RacingNumber: NotRequired[str | None]
    Lap: NotRequired[int | None]


class WeatherDataDict(TypedDict):
    """Type definition for weather data dictionary returned by session.weather."""

    Time: datetime.timedelta
    AirTemp: NotRequired[float | None]
    Humidity: NotRequired[float | None]
    Pressure: NotRequired[float | None]
    Rainfall: NotRequired[bool | None]
    TrackTemp: NotRequired[float | None]
    WindDirection: NotRequired[int | None]
    WindSpeed: NotRequired[float | None]


# Session types
SessionType = Literal[
    "Practice 1",
    "Practice 2",
    "Practice 3",
    "Qualifying",
    "Sprint",
    "Sprint Qualifying",
    "Sprint Shootout",
    "Race",
]

# Lib types
BackendType = Literal["pandas", "polars"]

# Compound types
CompoundType = Literal["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET", "UNKNOWN", "TEST_UNKNOWN"]

# Track status types
TrackStatusType = Literal["1", "2", "4", "5", "6", "7"]


class DataFrameProtocol(Protocol):
    """Protocol for DataFrame-like objects."""

    @property
    def shape(self) -> tuple[int, int]: ...

    @property
    def columns(self) -> list[str]: ...

    def head(self, n: int = 5) -> "DataFrameProtocol": ...

    def __len__(self) -> int: ...

    def __getitem__(self, key): ...


# Export all types
__all__ = [
    "BackendType",
    "CompoundType",
    "DataFrame",
    "DataFrameProtocol",
    "DriverInfoDict",
    "LapDataDict",
    "RaceControlDataDict",
    "SessionType",
    "TelemetryDataDict",
    "TrackStatusType",
    "WeatherDataDict",
]
