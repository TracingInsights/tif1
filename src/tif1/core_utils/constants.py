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
COL_DELETED = "Deleted"
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


def telemetry_ref_from_path(path: str) -> tuple[str, int] | None:
    """Parse a ``{driver}/{lap}_tel.json`` payload path into (driver, lap)."""
    if not path.endswith("_tel.json") or path.count("/") != 1:
        return None
    driver, file_name = path.split("/", 1)
    lap_part = file_name.removesuffix("_tel.json")
    if not driver or not lap_part.isdigit():
        return None
    return driver, int(lap_part)


# Circuit, city and country names users type for an event, mapped to the
# event names they mean, in priority order (the first one on that season's
# calendar wins). Keys are normalised by ``tif1.events._event_alias_key``
# (accents folded, punctuation and "Grand Prix"/"GP" removed, lower case).
# Fuzzy matching alone resolves several of these to the wrong race: "Spa" is
# a substring of "Spain", "USA" of "Lusail", and "Interlagos" or "Suzuka"
# resemble no event string at all. A name is listed only when it identifies
# one event; countries that hosted several (Germany: German and Eifel GP)
# are left to fuzzy matching.
EVENT_NAME_ALIASES: dict[str, tuple[str, ...]] = {
    **dict.fromkeys(
        ("spa", "spa francorchamps", "francorchamps", "belgium"), ("Belgian Grand Prix",)
    ),
    **dict.fromkeys(
        ("interlagos", "sao paulo", "brazil", "brasil"),
        ("São Paulo Grand Prix", "Brazilian Grand Prix"),
    ),
    **dict.fromkeys(
        ("silverstone", "britain", "great britain", "uk", "united kingdom", "england"),
        ("British Grand Prix",),
    ),
    **dict.fromkeys(("monza", "italy"), ("Italian Grand Prix",)),
    **dict.fromkeys(("imola", "emilia romagna"), ("Emilia Romagna Grand Prix",)),
    **dict.fromkeys(("mugello", "tuscany"), ("Tuscan Grand Prix",)),
    **dict.fromkeys(
        ("barcelona", "catalunya", "catalonia", "montmelo"),
        ("Barcelona Grand Prix", "Spanish Grand Prix"),
    ),
    **dict.fromkeys(("spain", "madrid", "madring"), ("Spanish Grand Prix",)),
    **dict.fromkeys(("hungary", "budapest", "hungaroring"), ("Hungarian Grand Prix",)),
    **dict.fromkeys(("baku", "azerbaijan"), ("Azerbaijan Grand Prix",)),
    **dict.fromkeys(("jeddah", "saudi", "saudi arabia"), ("Saudi Arabian Grand Prix",)),
    **dict.fromkeys(("suzuka", "japan"), ("Japanese Grand Prix",)),
    **dict.fromkeys(("melbourne", "albert park", "australia"), ("Australian Grand Prix",)),
    **dict.fromkeys(("shanghai", "china"), ("Chinese Grand Prix",)),
    **dict.fromkeys(("montreal", "canada"), ("Canadian Grand Prix",)),
    **dict.fromkeys(("spielberg", "red bull ring", "austria"), ("Austrian Grand Prix",)),
    **dict.fromkeys(("zandvoort", "netherlands", "holland"), ("Dutch Grand Prix",)),
    **dict.fromkeys(
        ("austin", "cota", "usa", "us", "united states", "america"),
        ("United States Grand Prix",),
    ),
    **dict.fromkeys(("miami", "miami gardens"), ("Miami Grand Prix",)),
    **dict.fromkeys(("vegas", "las vegas"), ("Las Vegas Grand Prix",)),
    **dict.fromkeys(("mexico", "mexico city"), ("Mexico City Grand Prix", "Mexican Grand Prix")),
    **dict.fromkeys(("lusail", "losail", "qatar"), ("Qatar Grand Prix",)),
    **dict.fromkeys(
        ("yas marina", "yas island", "abu dhabi", "uae", "united arab emirates"),
        ("Abu Dhabi Grand Prix",),
    ),
    **dict.fromkeys(("singapore", "marina bay"), ("Singapore Grand Prix",)),
    **dict.fromkeys(("monaco", "monte carlo"), ("Monaco Grand Prix",)),
    "bahrain": ("Bahrain Grand Prix",),
    "sakhir": ("Sakhir Grand Prix", "Bahrain Grand Prix"),
    **dict.fromkeys(("le castellet", "paul ricard", "france"), ("French Grand Prix",)),
    "hockenheim": ("German Grand Prix",),
    **dict.fromkeys(("nurburgring", "eifel"), ("Eifel Grand Prix",)),
    **dict.fromkeys(("sochi", "russia"), ("Russian Grand Prix",)),
    **dict.fromkeys(("portimao", "algarve", "portugal"), ("Portuguese Grand Prix",)),
    **dict.fromkeys(("istanbul", "istanbul park", "turkey"), ("Turkish Grand Prix",)),
}
