"""Ergast response structure: flattening tables and value casting.

This is the tif1 port of ``fastf1.ergast.structure``. Each *category* dict
describes one level of an Ergast (Jolpica) response: the key it lives under
(``name``), whether it is a ``dict`` or a ``list`` (``type``), the function
that flattens it (``method``), the ``map`` of response keys to output column
names and cast functions, the nested categories to descend into (``sub``) and
an optional ``finalize`` step. The output column names and the casting rules
are fastf1's, so frames built from these tables are drop-in replacements for
fastf1's ``ErgastResultFrame``.

tif1 additions keep fastf1's columns first and unchanged:

- ``Races_Schedule`` also flattens Jolpica's ``SprintQualifying`` and
  ``SprintShootout`` session keys.
- ``TotalRaceTime`` builds ``totalRaceTime`` from ``Time.millis`` (the true
  total for every classified driver) and adds ``totalRaceTimeGap`` with the
  gap to the winner. fastf1 parses ``Time.time`` into ``totalRaceTime``,
  which is the gap for every driver but the winner.

Flattening builds columns directly (dict of lists) instead of a list of
per-row dicts, and never mutates the response it reads.
"""

from __future__ import annotations

import datetime
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

Category = dict[str, Any]
#: Filler for rows that lack a column (pandas' missing marker for object columns).
_MISSING = float("nan")

# ##############################################
# ### functions for date and time conversion ###

_time_string_matcher = re.compile(
    r"(\d{1,2}:)?(\d{1,2}:)?(\d{1,2})(\.\d{1,6})?(Z|[+-]\d{2}:\d{2})?"
)
# matches [hh:][mm:]ss[.micros][Z | +-hh:mm] timestring
_INT_MATCHER = re.compile(r"^[+-]?\d+$")
_FLOAT_MATCHER = re.compile(r"^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$")


def date_from_ergast(d_str: Any) -> datetime.datetime | None:
    """Create a ``datetime.datetime`` from a ``YYYY-MM-DD`` date stamp (None when invalid)."""
    if not isinstance(d_str, str) or not d_str:
        return None
    try:
        return datetime.datetime.strptime(d_str, "%Y-%m-%d")  # noqa: DTZ007
    except (ValueError, TypeError):
        logger.debug("Failed to parse date stamp %r in Ergast response", d_str)
        return None


def time_from_ergast(t_str: Any) -> datetime.time | None:
    """Create a ``datetime.time`` from ``[hh:][mm:]ss[.micros][Z|+-hh:mm]`` (None when invalid)."""
    if not isinstance(t_str, str) or not t_str:
        return None

    res = _time_string_matcher.match(t_str)
    if res is None:
        logger.debug("Failed to parse timestamp %r in Ergast response", t_str)
        return None

    if res[1] and res[2] and res[3]:
        hour, minute, second = int(res[1][:-1]), int(res[2][:-1]), int(res[3])
    elif res[1] and res[3]:
        hour, minute, second = 0, int(res[1][:-1]), int(res[3])
    else:
        hour, minute, second = 0, 0, int(res[3])

    if res[4]:
        digits = res[4][1:]
        microsecond = int(digits) * 10 ** (6 - len(digits))
    else:
        microsecond = 0

    if res[5] == "Z":
        tzinfo: datetime.tzinfo | None = datetime.UTC
    elif res[5]:
        tzinfo = datetime.datetime.strptime(res[5], "%z").tzinfo
    else:
        tzinfo = None

    try:
        return datetime.time(
            hour=hour, minute=minute, second=second, microsecond=microsecond, tzinfo=tzinfo
        )
    except ValueError:
        logger.debug("Failed to parse timestamp %r in Ergast response", t_str)
        return None


def timedelta_from_ergast(t_str: Any) -> datetime.timedelta | None:
    """Create a ``datetime.timedelta`` from ``[+/-][hh:][mm:]ss[.micros]`` (None when invalid)."""
    if not isinstance(t_str, str) or not t_str:
        return None

    if t_str.startswith("-"):
        sign = -1
        t_str = t_str.strip("-")
    else:
        sign = 1
        t_str = t_str.strip("+")

    pseudo_time = time_from_ergast(t_str)
    if pseudo_time is not None:
        return sign * datetime.timedelta(
            hours=pseudo_time.hour,
            minutes=pseudo_time.minute,
            seconds=pseudo_time.second,
            microseconds=pseudo_time.microsecond,
        )
    return None


def save_int(i_str: Any) -> int:
    """``int`` from an integer-formatted string; ``-1`` for anything else (fastf1 #432)."""
    try:
        if _INT_MATCHER.match(i_str):
            return int(i_str)
    except TypeError:
        return -1
    return -1


def save_float(f_str: Any) -> float:
    """``float`` from a float-formatted string; ``nan`` for anything else."""
    try:
        if _FLOAT_MATCHER.match(f_str):
            return float(f_str)
    except TypeError:
        return float("nan")
    return float("nan")


# ########################################################
# ### functions for flattening of ergast response data ###


def _flatten_by_rename(
    nested: dict, category: Category, flat: dict, *, cast: bool = True, rename: bool = True
) -> set[str] | None:
    """Rename (and cast) every mapped value of the current level into ``flat``.

    Values not defined by ``category['map']`` are skipped. This is the default
    operation for most Ergast responses. Returns the names of sub-categories
    it consumed itself (None here).
    """
    for name, mapping in category["map"].items():
        if name not in nested:
            continue
        value = nested[name]
        if cast:
            value = mapping["type"](value)
        flat[mapping["name"] if rename else name] = value
    return None


def _flatten_inline_list_of_dicts(
    nested: list, category: Category, flat: dict, *, cast: bool = True, rename: bool = True
) -> set[str] | None:
    """Turn a list of dicts into one list per mapped key.

    ``[{"constructorId": "mclaren", ...}, {"constructorId": "mercedes", ...}]``
    becomes ``{"constructorIds": ["mclaren", "mercedes", ...], ...}`` so a
    multi-valued element (driver standings with several constructors) fits one
    row.
    """
    for name, mapping in category["map"].items():
        joined = []
        for item in nested:
            if name not in item:
                continue
            value = item[name]
            if cast:
                value = mapping["type"](value)
            joined.append(value)
        if joined:
            flat[mapping["name"] if rename else name] = joined
    return None


def _lap_timings_flatten_by_rename(
    nested: dict, category: Category, flat: dict, *, cast: bool = True, rename: bool = True
) -> set[str] | None:
    """Flatten one lap: its ``Timings`` list inline, ``number`` replicated per timing."""
    _flatten_by_rename(nested, category, flat, cast=cast, rename=rename)
    timings = nested.get("Timings", [])
    Timings["method"](timings, Timings, flat, cast=cast, rename=rename)
    count = len(flat.get("driverId", flat.get("Timings", [])) or [])
    flat["number"] = [flat.get("number")] * count
    return {"Timings"}


def _flatten_total_race_time(
    nested: dict, category: Category, flat: dict, *, cast: bool = True, rename: bool = True
) -> set[str] | None:
    """Flatten ``Time`` into the true total (from millis) plus the gap to the winner.

    fastf1 keeps ``Time.time`` as ``totalRaceTime``; Ergast stores the gap
    (``"+9.748"``) there for every driver but the winner, while ``Time.millis``
    is the total for everyone. ``totalRaceTime`` is therefore built from
    ``millis`` whenever it is valid (the winner's parsed ``time`` otherwise),
    and ``totalRaceTimeGap`` holds the parsed gap (zero for the winner).
    Without ``cast``/``rename`` the raw values pass through as in fastf1.
    """
    _flatten_by_rename(nested, category, flat, cast=cast, rename=rename)
    if not (cast and rename):
        return None
    raw_time = nested.get("time")
    parsed = flat.get("totalRaceTime")
    is_gap = isinstance(raw_time, str) and raw_time.startswith("+")
    if is_gap:
        flat["totalRaceTimeGap"] = parsed
    else:
        flat["totalRaceTimeGap"] = datetime.timedelta(0) if parsed is not None else None
    millis = flat.get("totalRaceTimeMillis")
    if isinstance(millis, int) and millis >= 0:
        flat["totalRaceTime"] = datetime.timedelta(milliseconds=millis)
    elif is_gap:
        # The gap is known but the total is not (no millis).
        flat["totalRaceTime"] = None
    return None


# ################################
# ### root category finalizers ###


def _merge_lists_of_lists(columns: dict[str, list]) -> dict[str, list]:
    """Concatenate per-row lists into flat columns (laps: one row per timing).

    ``{"driverId": [["a", "b"], ["a", "b"]], ...}`` becomes
    ``{"driverId": ["a", "b", "a", "b"], ...}``. Rows without a value for a
    column contribute nothing for that column.
    """
    merged: dict[str, list] = {}
    for name, cells in columns.items():
        joined: list = []
        for cell in cells:
            if isinstance(cell, list):
                joined.extend(cell)
        merged[name] = joined
    return merged


# #####################################
# ### response subcategory elements ###

FirstPractice: Category = {
    "name": "FirstPractice",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "date": {"name": "fp1Date", "type": date_from_ergast},
        "time": {"name": "fp1Time", "type": time_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

SecondPractice: Category = {
    "name": "SecondPractice",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "date": {"name": "fp2Date", "type": date_from_ergast},
        "time": {"name": "fp2Time", "type": time_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

ThirdPractice: Category = {
    "name": "ThirdPractice",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "date": {"name": "fp3Date", "type": date_from_ergast},
        "time": {"name": "fp3Time", "type": time_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

Qualifying: Category = {
    "name": "Qualifying",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "date": {"name": "qualifyingDate", "type": date_from_ergast},
        "time": {"name": "qualifyingTime", "type": time_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

Sprint: Category = {
    "name": "Sprint",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "date": {"name": "sprintDate", "type": date_from_ergast},
        "time": {"name": "sprintTime", "type": time_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

# tif1: Jolpica names the quali-like sprint sessions (fastf1 has no table).
SprintQualifying: Category = {
    "name": "SprintQualifying",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "date": {"name": "sprintQualifyingDate", "type": date_from_ergast},
        "time": {"name": "sprintQualifyingTime", "type": time_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

SprintShootout: Category = {
    "name": "SprintShootout",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "date": {"name": "sprintShootoutDate", "type": date_from_ergast},
        "time": {"name": "sprintShootoutTime", "type": time_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

TotalRaceTime: Category = {
    "name": "Time",
    "type": dict,
    "method": _flatten_total_race_time,
    "map": {
        "millis": {"name": "totalRaceTimeMillis", "type": save_int},
        "time": {"name": "totalRaceTime", "type": timedelta_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

FastestLapTime: Category = {
    "name": "Time",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "millis": {"name": "fastestLapTimeMillis", "type": save_int},
        "time": {"name": "fastestLapTime", "type": timedelta_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

FastestLapAvgSpeed: Category = {
    "name": "AverageSpeed",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "units": {"name": "fastestLapAvgSpeedUnits", "type": str},
        "speed": {"name": "fastestLapAvgSpeed", "type": save_float},
    },
    "sub": [],
    "finalize": None,
}

FastestLap: Category = {
    "name": "FastestLap",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "rank": {"name": "fastestLapRank", "type": save_int},
        "lap": {"name": "fastestLapNumber", "type": save_int},
    },
    "sub": [FastestLapTime, FastestLapAvgSpeed],
    "finalize": None,
}

Driver: Category = {
    "name": "Driver",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "driverId": {"name": "driverId", "type": str},
        "permanentNumber": {"name": "driverNumber", "type": save_int},
        "code": {"name": "driverCode", "type": str},
        "url": {"name": "driverUrl", "type": str},
        "givenName": {"name": "givenName", "type": str},
        "familyName": {"name": "familyName", "type": str},
        "dateOfBirth": {"name": "dateOfBirth", "type": date_from_ergast},
        "nationality": {"name": "driverNationality", "type": str},
    },
    "sub": [],
    "finalize": None,
}

Constructor: Category = {
    "name": "Constructor",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "constructorId": {"name": "constructorId", "type": str},
        "url": {"name": "constructorUrl", "type": str},
        "name": {"name": "constructorName", "type": str},
        "nationality": {"name": "constructorNationality", "type": str},
    },
    "sub": [],
    "finalize": None,
}

ConstructorsInline: Category = {
    # A list of constructors for a single element (driver standings with
    # potentially multiple constructors per driver), reduced to lists.
    "name": "Constructors",
    "type": list,
    "method": _flatten_inline_list_of_dicts,
    "map": {
        "constructorId": {"name": "constructorIds", "type": str},
        "url": {"name": "constructorUrls", "type": str},
        "name": {"name": "constructorNames", "type": str},
        "nationality": {"name": "constructorNationalities", "type": str},
    },
    "sub": [],
    "finalize": None,
}

Location: Category = {
    "name": "Location",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "lat": {"name": "lat", "type": save_float},
        "long": {"name": "long", "type": save_float},
        "locality": {"name": "locality", "type": str},
        "country": {"name": "country", "type": str},
    },
    "sub": [],
    "finalize": None,
}

Circuit: Category = {
    "name": "Circuit",
    "type": dict,
    "method": _flatten_by_rename,
    "map": {
        "circuitId": {"name": "circuitId", "type": str},
        "url": {"name": "circuitUrl", "type": str},
        "circuitName": {"name": "circuitName", "type": str},
    },
    "sub": [Location],
    "finalize": None,
}

QualifyingResults: Category = {
    "name": "QualifyingResults",
    "type": list,
    "method": _flatten_by_rename,
    "map": {
        "number": {"name": "number", "type": save_int},
        "position": {"name": "position", "type": save_int},
        "Q1": {"name": "Q1", "type": timedelta_from_ergast},
        "Q2": {"name": "Q2", "type": timedelta_from_ergast},
        "Q3": {"name": "Q3", "type": timedelta_from_ergast},
    },
    "sub": [Driver, Constructor],
    "finalize": None,
}

RaceResults: Category = {
    "name": "Results",
    "type": list,
    "method": _flatten_by_rename,
    "map": {
        "number": {"name": "number", "type": save_int},
        "position": {"name": "position", "type": save_int},
        "positionText": {"name": "positionText", "type": str},
        "points": {"name": "points", "type": save_float},
        "grid": {"name": "grid", "type": save_int},
        "laps": {"name": "laps", "type": save_int},
        "status": {"name": "status", "type": str},
    },
    "sub": [Driver, Constructor, TotalRaceTime, FastestLap, FastestLapAvgSpeed],
    "finalize": None,
}

SprintResults: Category = {
    **RaceResults,  # generate from RaceResults
    "name": "SprintResults",
}

DriverStandings: Category = {
    "name": "DriverStandings",
    "type": list,
    "method": _flatten_by_rename,
    "map": {
        "position": {"name": "position", "type": save_int},
        "positionText": {"name": "positionText", "type": str},
        "points": {"name": "points", "type": save_float},
        "wins": {"name": "wins", "type": save_int},
    },
    "sub": [Driver, ConstructorsInline],
    "finalize": None,
}

ConstructorStandings: Category = {
    **DriverStandings,  # generate from DriverStandings
    "name": "ConstructorStandings",
    "sub": [Constructor],
}

Timings: Category = {
    "name": "Timings",
    "type": list,
    "method": _flatten_inline_list_of_dicts,
    "map": {
        "driverId": {"name": "driverId", "type": str},
        "position": {"name": "position", "type": save_int},
        "time": {"name": "time", "type": timedelta_from_ergast},
    },
    "sub": [],
    "finalize": None,
}

Laps: Category = {
    "name": "Laps",
    "type": list,
    "method": _lap_timings_flatten_by_rename,
    "map": {"number": {"name": "number", "type": save_int}},
    "sub": [Timings],
    "finalize": _merge_lists_of_lists,
}

PitStops: Category = {
    "name": "PitStops",
    "type": list,
    "method": _flatten_by_rename,
    "map": {
        "driverId": {"name": "driverId", "type": str},
        "stop": {"name": "stop", "type": save_int},
        "lap": {"name": "lap", "type": save_int},
        "time": {"name": "time", "type": time_from_ergast},
        "duration": {"name": "duration", "type": timedelta_from_ergast},
    },
    "sub": [Driver, ConstructorsInline],
    "finalize": None,
}

# ##############################
# ### response root elements ###

Seasons: Category = {
    "name": "Seasons",
    "type": list,
    "method": _flatten_by_rename,
    "map": {
        "season": {"name": "season", "type": save_int},
        "url": {"name": "seasonUrl", "type": str},
    },
    "sub": [],
    "finalize": None,
}

__StandingsLists: Category = {
    "name": "StandingsLists",
    "type": list,
    "method": _flatten_by_rename,
    "map": {
        "season": {"name": "season", "type": save_int},
        "round": {"name": "round", "type": save_int},
    },
    "finalize": None,
}

StandingsLists_Driver: Category = {**__StandingsLists, "sub": [DriverStandings]}

StandingsLists_Constructor: Category = {**__StandingsLists, "sub": [ConstructorStandings]}

__Races: Category = {
    # template for all 'Races' based categories
    "name": "Races",
    "type": list,
    "method": _flatten_by_rename,
    "map": {
        "season": {"name": "season", "type": save_int},
        "round": {"name": "round", "type": save_int},
        "url": {"name": "raceUrl", "type": str},
        "raceName": {"name": "raceName", "type": str},
        "date": {"name": "raceDate", "type": date_from_ergast},
        "time": {"name": "raceTime", "type": time_from_ergast},
    },
    "finalize": None,
}

Races_Schedule: Category = {
    **__Races,
    "sub": [
        Circuit,
        FirstPractice,
        SecondPractice,
        ThirdPractice,
        Qualifying,
        Sprint,
        SprintQualifying,
        SprintShootout,
    ],
}

Races_RaceResults: Category = {**__Races, "sub": [Circuit, RaceResults]}

Races_QualifyingResults: Category = {**__Races, "sub": [Circuit, QualifyingResults]}

Races_SprintResults: Category = {**__Races, "sub": [Circuit, SprintResults]}

Races_Laps: Category = {**__Races, "sub": [Circuit, Laps]}

Races_PitStops: Category = {**__Races, "sub": [Circuit, PitStops]}

Drivers: Category = {**Driver, "type": list, "name": "Drivers"}

Constructors: Category = {**Constructor, "type": list, "name": "Constructors"}

Circuits: Category = {**Circuit, "type": list, "name": "Circuits"}

Status: Category = {
    "name": "Status",
    "type": list,
    "method": _flatten_by_rename,
    "map": {
        "statusId": {"name": "statusId", "type": save_int},
        "count": {"name": "count", "type": save_int},
        "status": {"name": "status", "type": str},
    },
    "sub": [],
    "finalize": None,
}


# ###############################
# ### flattening entry points ###


def flatten_element(nested: Any, category: Category, cast: bool) -> dict[str, Any]:
    """Flatten one response element (and its sub-categories) into a flat dict.

    The response is only read, never mutated.
    """
    flat: dict[str, Any] = {}
    consumed = category["method"](nested, category, flat, cast=cast) or ()
    if isinstance(nested, dict):
        for subcategory in category["sub"]:
            subname = subcategory["name"]
            if subname in consumed or subname not in nested:
                continue
            flat.update(flatten_element(nested[subname], subcategory, cast))
    return flat


def flatten_rows(response: list, category: Category, cast: bool = True) -> dict[str, list]:
    """Flatten a list of response elements into columns (dict of equal-length lists).

    Columns appear in first-seen order; rows lacking a column get ``NaN`` so
    pandas builds the same frame it would from a list of row dicts. The
    category's ``finalize`` step (laps: concatenate per-lap lists) runs last.
    """
    columns: dict[str, list] = {}
    for index, element in enumerate(response):
        flat = flatten_element(element, category, cast)
        for name, value in flat.items():
            column = columns.get(name)
            if column is None:
                # NaN (not None) for rows without the column: what pandas
                # fills in when building from row dicts, as fastf1 does.
                column = [_MISSING] * index
                columns[name] = column
            column.append(value)
        for column in columns.values():
            if len(column) == index:
                column.append(_MISSING)
    finalize = category.get("finalize")
    if finalize is not None:
        columns = finalize(columns)
    return columns


def auto_cast(data: Any, category: Category) -> Any:
    """Cast every known value of a (copied) raw response in place and return it."""
    if category["type"] is list:
        for index in range(len(data)):
            data[index] = _auto_cast_item(data[index], category)
        return data
    return _auto_cast_item(data, category)


def _auto_cast_item(data: Any, category: Category) -> Any:
    if not isinstance(data, dict):
        return data
    for name, mapping in category["map"].items():
        if name in data:
            data[name] = mapping["type"](data[name])
    for subcategory in category["sub"]:
        subname = subcategory["name"]
        if subname in data:
            data[subname] = auto_cast(data[subname], subcategory)
    return data


__all__ = [
    "Category",
    "Circuit",
    "Circuits",
    "Constructor",
    "ConstructorStandings",
    "Constructors",
    "ConstructorsInline",
    "Driver",
    "DriverStandings",
    "Drivers",
    "FastestLap",
    "FastestLapAvgSpeed",
    "FastestLapTime",
    "FirstPractice",
    "Laps",
    "Location",
    "PitStops",
    "Qualifying",
    "QualifyingResults",
    "RaceResults",
    "Races_Laps",
    "Races_PitStops",
    "Races_QualifyingResults",
    "Races_RaceResults",
    "Races_Schedule",
    "Races_SprintResults",
    "Seasons",
    "SecondPractice",
    "Sprint",
    "SprintQualifying",
    "SprintResults",
    "SprintShootout",
    "StandingsLists_Constructor",
    "StandingsLists_Driver",
    "Status",
    "ThirdPractice",
    "Timings",
    "TotalRaceTime",
    "auto_cast",
    "date_from_ergast",
    "flatten_element",
    "flatten_rows",
    "save_float",
    "save_int",
    "time_from_ergast",
    "timedelta_from_ergast",
]
