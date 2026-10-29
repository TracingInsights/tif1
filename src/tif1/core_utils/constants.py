"""Constants and configuration for tif1 core."""

# Year range. Sessions from 1950 are served from Ergast data (results, lap
# timings, pit stops, standings); the TracingInsights timing/telemetry
# repositories start with F1_API_FIRST_YEAR.
MIN_YEAR = 1950
MAX_YEAR = 2100
F1_API_FIRST_YEAR = 2018
#: Sessions a season before F1_API_FIRST_YEAR can load (Ergast carries no practice data).
ERGAST_LOADABLE_SESSIONS: tuple[str, ...] = ("Qualifying", "Sprint", "Race")

# Cache configuration
MAX_CACHE_SIZE = 100

#: Raw keys of one driver's laptime payload (TracingInsights names), in file order.
#: An empty payload built from them runs through the lap pipeline to give a
#: driver without laps a frame that still carries every laps column.
LAP_PAYLOAD_KEYS: tuple[str, ...] = (
    "lap",
    "time",
    "pos",
    "sesT",
    "lST",
    "stint",
    "dNum",
    "iacc",
    "ff1G",
    "del",
    "compound",
    "life",
    "fresh",
    "s1",
    "s2",
    "s3",
    "s1T",
    "s2T",
    "s3T",
    "vi1",
    "vi2",
    "vfl",
    "vst",
    "pin",
    "pout",
    "lSD",
    "status",
    "delR",
    "pb",
)

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
# are left to fuzzy matching. Event names that fuzzy matching sends to a
# neighbour in the seasons they were not held are listed too: "Australian"
# scores the Austrian GP in 2020/21 and "Styrian" the Austrian GP outside
# 2020/21; as aliases they raise ``EventNotInSeasonError`` there instead.
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
    **dict.fromkeys(
        ("melbourne", "albert park", "australia", "australian"), ("Australian Grand Prix",)
    ),
    **dict.fromkeys(("shanghai", "china"), ("Chinese Grand Prix",)),
    **dict.fromkeys(("montreal", "canada"), ("Canadian Grand Prix",)),
    **dict.fromkeys(("spielberg", "red bull ring", "austria"), ("Austrian Grand Prix",)),
    **dict.fromkeys(("styria", "styrian"), ("Styrian Grand Prix",)),
    **dict.fromkeys(("zandvoort", "netherlands", "holland"), ("Dutch Grand Prix",)),
    **dict.fromkeys(
        ("austin", "austin texas", "cota", "usa", "us", "united states", "america"),
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


# Telemetry channels that are numeric by contract (see
# ``core_utils.helpers._NUMERIC_TELEMETRY_CHANNELS``): upstream writes the
# literal string "None" for a missing sample, which the frame builders coerce
# to NaN/null instead of letting one sentinel stringify the whole column.
NUMERIC_TELEMETRY_CHANNELS = frozenset(
    {
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
    }
)
# fastf1's telemetry column order (``Date`` .. ``Z``) followed by tif1's extras;
# every telemetry frame is laid out in this order, unknown channels after it.
TELEMETRY_COLUMN_ORDER = (
    "Date",
    "SessionTime",
    "DriverAhead",
    "DistanceToDriverAhead",
    "Time",
    "RPM",
    "Speed",
    "nGear",
    "Throttle",
    "Brake",
    "DRS",
    "Source",
    "Distance",
    "RelativeDistance",
    "Status",
    "X",
    "Y",
    "Z",
    "AccelerationX",
    "AccelerationY",
    "AccelerationZ",
    "Driver",
    "LapNumber",
)
_TELEMETRY_COLUMN_RANK = {name: rank for rank, name in enumerate(TELEMETRY_COLUMN_ORDER)}
#: Constant channels fastf1 reads from the live streams: tif1's payload is the
#: car+pos merge (one ``Source``) and carries no off-track samples (``Status``).
TELEMETRY_SOURCE_VALUE = "merged"
TELEMETRY_STATUS_VALUE = "OnTrack"


def ordered_telemetry_columns(columns: list[str]) -> list[str]:
    """``columns`` in :data:`TELEMETRY_COLUMN_ORDER`, unknown names last in their order."""
    unknown_rank = len(TELEMETRY_COLUMN_ORDER)
    return (
        sorted(columns, key=lambda name: (_TELEMETRY_COLUMN_RANK.get(name, unknown_rank), 0))
        if any(name in _TELEMETRY_COLUMN_RANK for name in columns)
        else list(columns)
    )


# The same channels under the raw payload keys (before TELEMETRY_RENAME_MAP).
NUMERIC_TELEMETRY_PAYLOAD_KEYS = frozenset(
    {
        raw
        for raw, canonical in TELEMETRY_RENAME_MAP.items()
        if canonical in NUMERIC_TELEMETRY_CHANNELS
    }
    | NUMERIC_TELEMETRY_CHANNELS
)


# Session name -> Stats repository file holding its Ergast classification.
# Practice and the quali-like sprint formats borrow the race results file for
# driver identity (DriverId/TeamId) only, as fastf1 does for practice.
ERGAST_SESSION_FILES: dict[str, str] = {
    "Race": "results.json",
    "Qualifying": "quali_results.json",
    "Sprint": "sprint_results.json",
}

# fastf1's SessionResults column contract, in its order, followed by the tif1
# extras carried from Ergast. ``Session.results`` always has every column.
FASTF1_RESULTS_COLUMNS: tuple[str, ...] = (
    "DriverNumber",
    "BroadcastName",
    "Abbreviation",
    "DriverId",
    "TeamName",
    "TeamColor",
    "TeamId",
    "FirstName",
    "LastName",
    "FullName",
    "HeadshotUrl",
    "CountryCode",
    "Position",
    "ClassifiedPosition",
    "GridPosition",
    "Q1",
    "Q2",
    "Q3",
    "Time",
    "Status",
    "Points",
    "Laps",
)
TIF1_RESULTS_EXTRA_COLUMNS: tuple[str, ...] = (
    "TimeGap",
    "FastestLapRank",
    "FastestLapNumber",
    "FastestLapTime",
    "FastestLapAvgSpeed",
    "PermanentNumber",
    "DriverNationality",
    "TeamNationality",
    "DateOfBirth",
    "DriverUrl",
    "TeamUrl",
)
RESULTS_COLUMN_ORDER: tuple[str, ...] = FASTF1_RESULTS_COLUMNS + TIF1_RESULTS_EXTRA_COLUMNS
