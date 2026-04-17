"""Constants and configuration for tif1 core."""

# Year range
MIN_YEAR = 2018
MAX_YEAR = 2100

# Cache configuration
MAX_CACHE_SIZE = 100

# Column name mappings
LAP_RENAME_MAP = {
    "time": "LapTime",
    "lap": "LapNumber",
    "compound": "Compound",
    "stint": "Stint",
    "s1": "Sector1Time",
    "s2": "Sector2Time",
    "s3": "Sector3Time",
    "life": "TyreLife",
    "pos": "Position",
    "status": "TrackStatus",
    "pb": "IsPersonalBest",
    "qs": "QualifyingSession",
    "qualifying_session": "QualifyingSession",
    "session_time": "Time",
    "source_driver": "Driver",
    "drv": "Driver",  # Alias from validation
    "driver_number": "DriverNumber",
    "pit_out_time": "PitOutTime",
    "pit_in_time": "PitInTime",
    "sector1_session_time": "Sector1SessionTime",
    "sector2_session_time": "Sector2SessionTime",
    "sector3_session_time": "Sector3SessionTime",
    "speed_i1": "SpeedI1",
    "speed_i2": "SpeedI2",
    "speed_fl": "SpeedFL",
    "speed_st": "SpeedST",
    "fresh_tyre": "FreshTyre",
    "source_team": "Team",
    "team": "Team",  # Alias from validation
    "lap_start_time": "LapStartTime",
    "lap_start_date": "LapStartDate",
    "deleted": "Deleted",
    "deleted_reason": "DeletedReason",
    "fastf1_generated": "FastF1Generated",
    "is_accurate": "IsAccurate",
    "weather_time": "WeatherTime",
    "air_temp": "AirTemp",
    "humidity": "Humidity",
    "pressure": "Pressure",
    "rainfall": "Rainfall",
    "track_temp": "TrackTemp",
    "wind_direction": "WindDirection",
    "wind_speed": "WindSpeed",
    # Handle unvalidated/raw payload keys as well.
    "sesT": "Time",
    "dNum": "DriverNumber",
    "pout": "PitOutTime",
    "pin": "PitInTime",
    "s1T": "Sector1SessionTime",
    "s2T": "Sector2SessionTime",
    "s3T": "Sector3SessionTime",
    "vi1": "SpeedI1",
    "vi2": "SpeedI2",
    "vfl": "SpeedFL",
    "vst": "SpeedST",
    "fresh": "FreshTyre",
    "lST": "LapStartTime",
    "lSD": "LapStartDate",
    "del": "Deleted",
    "delR": "DeletedReason",
    "ff1G": "FastF1Generated",
    "iacc": "IsAccurate",
    "wT": "WeatherTime",
    "wAT": "AirTemp",
    "wH": "Humidity",
    "wP": "Pressure",
    "wR": "Rainfall",
    "wTT": "TrackTemp",
    "wWD": "WindDirection",
    "wWS": "WindSpeed",
}

TELEMETRY_RENAME_MAP = {
    "time": "Time",
    "rpm": "RPM",
    "speed": "Speed",
    "gear": "nGear",
    "throttle": "Throttle",
    "brake": "Brake",
    "drs": "DRS",
    "distance": "Distance",
    "rel_distance": "RelativeDistance",
    "driver_ahead": "DriverAhead",
    "distance_to_driver_ahead": "DistanceToDriverAhead",
    "acc_x": "AccelerationX",
    "acc_y": "AccelerationY",
    "acc_z": "AccelerationZ",
    "x": "X",
    "y": "Y",
    "z": "Z",
    "data_key": "DataKey",
    "dataKey": "DataKey",
}

RACE_CONTROL_RENAME_MAP = {
    "time": "Time",
    "category": "Category",
    "cat": "Category",
    "message": "Message",
    "msg": "Message",
    "status": "Status",
    "flag": "Flag",
    "scope": "Scope",
    "sector": "Sector",
    "racing_number": "RacingNumber",
    "dNum": "RacingNumber",
    "lap": "Lap",
}

WEATHER_RENAME_MAP = {
    "time": "Time",
    "wT": "Time",
    "air_temp": "AirTemp",
    "wAT": "AirTemp",
    "humidity": "Humidity",
    "wH": "Humidity",
    "pressure": "Pressure",
    "wP": "Pressure",
    "rainfall": "Rainfall",
    "wR": "Rainfall",
    "track_temp": "TrackTemp",
    "wTT": "TrackTemp",
    "wind_direction": "WindDirection",
    "wWD": "WindDirection",
    "wind_speed": "WindSpeed",
    "wWS": "WindSpeed",
}

CATEGORICAL_COLS = ["Driver", "Team", "Compound", "TrackStatus"]

# Column names
COL_DRIVER = "Driver"
COL_TEAM = "Team"
COL_LAP_NUMBER = "LapNumber"
COL_LAP_TIME = "LapTime"
COL_LAP_TIME_SECONDS = "LapTimeSeconds"
COL_LAP_NUMBER_ALT = "lap"

# FastF1-compatible column order for Laps DataFrame
FASTF1_LAPS_COLUMN_ORDER = [
    # Core fastf1 columns in exact order
    "index",
    "Time",
    "Driver",
    "DriverNumber",
    "LapTime",
    "LapNumber",
    "Stint",
    "PitOutTime",
    "PitInTime",
    "Sector1Time",
    "Sector2Time",
    "Sector3Time",
    "Sector1SessionTime",
    "Sector2SessionTime",
    "Sector3SessionTime",
    "SpeedI1",
    "SpeedI2",
    "SpeedFL",
    "SpeedST",
    "IsPersonalBest",
    "Compound",
    "TyreLife",
    "FreshTyre",
    "Team",
    "LapStartTime",
    "LapStartDate",
    "TrackStatus",
    "Position",
    "Deleted",
    "DeletedReason",
    "FastF1Generated",
    "IsAccurate",
    # Weather columns (per-lap weather data)
    "WeatherTime",
    "AirTemp",
    "Humidity",
    "Pressure",
    "Rainfall",
    "TrackTemp",
    "WindDirection",
    "WindSpeed",
    # tif1-specific columns (added at the end)
    "LapTimeSeconds",
    "QualifyingSession",
]
