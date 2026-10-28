"""Merge Ergast classification data into ``Session`` views (pure helpers).

``tif1.core.Session`` fetches one Ergast race record per session (the Stats
repository file of the round, Jolpica as fallback) and hands it to these
functions to:

- order the driver rows by the official classification
  (:func:`order_driver_rows`),
- fill fastf1's ``SessionResults`` columns and tif1's extras
  (:func:`merge_ergast_rows`, :func:`finalize_results_frame`),
- build the standings and pit-stop frames (:func:`build_standings_frame`,
  :func:`build_pit_stops_frame`),
- turn Ergast lap timings into the per-driver laptime payloads the laps
  pipeline already understands, for seasons without timing data
  (:func:`build_laps_payloads`).

Nothing here touches the network; everything is a function of the payloads.
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from typing import Any, Literal

import pandas as pd

from tif1.core_utils.constants import (
    FASTF1_RESULTS_COLUMNS,
    RESULTS_COLUMN_ORDER,
    TIF1_RESULTS_EXTRA_COLUMNS,
)
from tif1.ergast.structure import date_from_ergast, timedelta_from_ergast

SessionKind = Literal["race", "qualifying", "identity"]

#: Ergast endpoint per session name; practice and the quali-like sprint
#: formats borrow the race results for driver identity only.
_SESSION_ENDPOINTS = {"Race": "results", "Qualifying": "qualifying", "Sprint": "sprint"}
LEAF_KEYS = {"results": "Results", "qualifying": "QualifyingResults", "sprint": "SprintResults"}

_STR_COLUMNS = (
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
    "ClassifiedPosition",
    "Status",
    "PermanentNumber",
    "DriverNationality",
    "TeamNationality",
    "DriverUrl",
    "TeamUrl",
)
_FLOAT_COLUMNS = (
    "Position",
    "GridPosition",
    "Points",
    "Laps",
    "FastestLapRank",
    "FastestLapNumber",
    "FastestLapAvgSpeed",
)
_TIMEDELTA_COLUMNS = ("Q1", "Q2", "Q3", "Time", "TimeGap", "FastestLapTime")
_DATETIME_COLUMNS = ("DateOfBirth",)
_NO_FILL_STR = frozenset({"Status"})  # keeps NaN when nothing is known (no laps, no Ergast)


def session_kind(session_name: str) -> SessionKind:
    """``"race"`` (Race, Sprint), ``"qualifying"`` or ``"identity"`` (every other session)."""
    if session_name in ("Race", "Sprint"):
        return "race"
    if session_name == "Qualifying":
        return "qualifying"
    return "identity"


def ergast_endpoint(session_name: str) -> str:
    """The Ergast endpoint whose rows describe ``session_name`` (race results otherwise)."""
    return _SESSION_ENDPOINTS.get(session_name, "results")


def ergast_rows(race: dict[str, Any] | None, endpoint: str) -> list[dict[str, Any]]:
    """The result rows of an Ergast race record for ``endpoint`` (empty when absent)."""
    if not isinstance(race, dict):
        return []
    rows = race.get(LEAF_KEYS.get(endpoint, "Results"), [])
    return [row for row in rows if isinstance(row, dict)]


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _timedelta(value: Any) -> Any:
    """``pd.Timedelta`` from an Ergast time string, ``pd.NaT`` when missing/invalid."""
    parsed = timedelta_from_ergast(value)
    return pd.NaT if parsed is None else pd.Timedelta(parsed)


def _number(row: dict[str, Any]) -> str:
    return str(row.get("number", "")).strip()


def driver_code(row: dict[str, Any]) -> str:
    """The driver's three-letter code, or the Ergast driver id when there is none (pre-1990s)."""
    driver = row.get("Driver", {}) if isinstance(row.get("Driver"), dict) else {}
    return str(driver.get("code") or driver.get("driverId") or "")


def team_color_map(driver_rows: list[dict[str, Any]], year: int) -> dict[str, str]:
    """Team name -> hex colour (no ``#``) from the session's drivers, then the plotting defaults."""
    colors: dict[str, str] = {}
    try:
        from tif1.plotting_constants import TEAM_COLORS

        for team, color in TEAM_COLORS.get(year, {}).items():
            colors[team] = str(color).lstrip("#")
    except Exception:  # plotting constants are optional for this lookup
        pass
    for row in driver_rows:
        team = row.get("team")
        color = row.get("tc")
        if isinstance(team, str) and team and isinstance(color, str) and color:
            colors[team] = color
    return colors


def synthesize_driver_row(row: dict[str, Any], team_colors: dict[str, str]) -> dict[str, Any]:
    """A TracingInsights-style driver dict for a driver only Ergast knows (DNS, pre-2018)."""
    driver = row.get("Driver", {}) if isinstance(row.get("Driver"), dict) else {}
    constructor = row.get("Constructor", {}) if isinstance(row.get("Constructor"), dict) else {}
    team = str(constructor.get("name", ""))
    return {
        "driver": driver_code(row),
        "team": team,
        "dn": _number(row),
        "fn": str(driver.get("givenName", "")),
        "ln": str(driver.get("familyName", "")),
        "tc": team_colors.get(team, ""),
        "url": "",
        "_ergast_only": True,
    }


def _position_key(row: dict[str, Any]) -> tuple[int, int]:
    position = _as_int(row.get("position"))
    return (0, position) if position is not None else (1, 0)


def order_driver_rows(
    rows: list[dict[str, Any]],
    classification: list[dict[str, Any]],
    team_colors: dict[str, str],
) -> list[dict[str, Any]]:
    """Driver dicts in the official classification order.

    Each Ergast row (sorted by ``position``, ties in Ergast order) takes the
    session's driver dict with the same car number, or a synthesized dict
    when the session's drivers do not list that car (did not start). Drivers
    the classification does not mention keep their place at the end, in
    their original order.
    """
    by_number: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    for row in rows:
        by_number[str(row.get("dn", "")).strip()].append(row)
    ordered: list[dict[str, Any]] = []
    for erow in sorted(classification, key=_position_key):
        number = _number(erow)
        queue = by_number.get(number)
        if queue:
            ordered.append(queue.popleft())
        else:
            ordered.append(synthesize_driver_row(erow, team_colors))
    for queue in by_number.values():
        ordered.extend(queue)
    return ordered


def _identity_fields(row: dict[str, Any]) -> dict[str, Any]:
    driver = row.get("Driver", {}) if isinstance(row.get("Driver"), dict) else {}
    constructor = row.get("Constructor", {}) if isinstance(row.get("Constructor"), dict) else {}
    date_of_birth = date_from_ergast(driver.get("dateOfBirth"))
    return {
        "DriverId": str(driver.get("driverId", "")),
        "TeamId": str(constructor.get("constructorId", "")),
        "PermanentNumber": str(driver.get("permanentNumber", "")),
        "DriverNationality": str(driver.get("nationality", "")),
        "TeamNationality": str(constructor.get("nationality", "")),
        "DateOfBirth": pd.Timestamp(date_of_birth) if date_of_birth is not None else pd.NaT,
        "DriverUrl": str(driver.get("url", "")),
        "TeamUrl": str(constructor.get("url", "")),
    }


def _race_fields(row: dict[str, Any]) -> dict[str, Any]:
    position = _as_int(row.get("position"))
    time_block = row.get("Time", {}) if isinstance(row.get("Time"), dict) else {}
    millis = _as_int(time_block.get("millis"))
    raw_time = time_block.get("time")
    if millis is not None and millis >= 0:
        total = pd.Timedelta(milliseconds=millis)
    elif isinstance(raw_time, str) and not raw_time.startswith("+"):
        total = _timedelta(raw_time)
    else:
        total = pd.NaT
    if isinstance(raw_time, str) and raw_time.startswith("+"):
        gap = _timedelta(raw_time)
    elif isinstance(raw_time, str) and raw_time:
        gap = pd.Timedelta(0)
    else:
        gap = pd.NaT
    fastest = row.get("FastestLap", {}) if isinstance(row.get("FastestLap"), dict) else {}
    fastest_time = fastest.get("Time", {}) if isinstance(fastest.get("Time"), dict) else {}
    speed = fastest.get("AverageSpeed", {}) if isinstance(fastest.get("AverageSpeed"), dict) else {}
    return {
        "Position": float(position) if position is not None else math.nan,
        "ClassifiedPosition": str(row.get("positionText", "")),
        "GridPosition": _as_float(row.get("grid")),
        "Status": str(row.get("status", "")),
        "Points": _as_float(row.get("points")),
        "Time": total,
        "TimeGap": gap,
        "Laps": _as_float(row.get("laps")),
        "FastestLapRank": _as_float(fastest.get("rank")),
        "FastestLapNumber": _as_float(fastest.get("lap")),
        "FastestLapTime": _timedelta(fastest_time.get("time")),
        "FastestLapAvgSpeed": _as_float(speed.get("speed")),
    }


def _qualifying_fields(row: dict[str, Any]) -> dict[str, Any]:
    position = _as_int(row.get("position"))
    return {
        "Position": float(position) if position is not None else math.nan,
        "ClassifiedPosition": str(position) if position is not None else "",
        "Q1": _timedelta(row.get("Q1")),
        "Q2": _timedelta(row.get("Q2")),
        "Q3": _timedelta(row.get("Q3")),
    }


def broadcast_name(first_name: Any, last_name: Any) -> str:
    """``P GASLY`` from the first and last name (fastf1's BroadcastName)."""
    first = str(first_name or "").strip()
    last = str(last_name or "").strip()
    if not first and not last:
        return ""
    initial = f"{first[0]} " if first else ""
    return f"{initial}{last.upper()}".strip()


def merge_ergast_rows(
    rows: list[dict[str, Any]],
    classification: list[dict[str, Any]],
    kind: SessionKind,
) -> list[dict[str, Any]]:
    """Fill the Ergast columns of result rows (matched by ``DriverNumber``).

    ``rows`` are the session's result rows (one per driver dict, same order).
    ``kind`` selects which Ergast columns apply: race/sprint classification,
    qualifying segments, or identity only (practice and the quali-like sprint
    formats, which borrow the race results for ``DriverId``/``TeamId``).
    Drivers only Ergast knows keep NaN ``Position``/``GridPosition`` (fastf1).
    """
    by_number: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    for erow in classification:
        by_number[_number(erow)].append(erow)
    merged: list[dict[str, Any]] = []
    for row in rows:
        queue = by_number.get(str(row.get("DriverNumber", "")).strip())
        erow = queue.popleft() if queue else None
        out = dict(row)
        if erow is not None:
            out.update(_identity_fields(erow))
            if kind == "race":
                out.update(_race_fields(erow))
            elif kind == "qualifying":
                out.update(_qualifying_fields(erow))
            if out.get("_ergast_only"):
                out["Position"] = math.nan
                out["GridPosition"] = math.nan
        out["BroadcastName"] = broadcast_name(out.get("FirstName"), out.get("LastName"))
        out.pop("_ergast_only", None)
        merged.append(out)
    return merged


def unique_driver_labels(entries: list[tuple[str, str]]) -> list[str]:
    """Index labels for ``(DriverNumber, Abbreviation)`` rows: the number, made unique.

    A shared car (two classification rows under one number, common before
    the 1960s) keeps the plain number for its first driver; every later
    driver of that car is labelled ``number/Abbreviation`` so that a label
    always names one row while the ``DriverNumber`` column keeps the car
    number. Labels of 2018+ sessions are the plain numbers.
    """
    labels: list[str] = []
    seen: set[str] = set()
    for number, code in entries:
        label = number
        if label in seen:
            label = f"{number}/{code or len(labels) + 1}"
            ordinal = 2
            while label in seen:
                label = f"{number}/{code or len(labels) + 1}-{ordinal}"
                ordinal += 1
        seen.add(label)
        labels.append(label)
    return labels


def finalize_results_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """fastf1's ``SessionResults`` contract plus tif1's extras, indexed by driver number.

    Every column of :data:`RESULTS_COLUMN_ORDER` exists with its dtype (str
    columns object, positions/points float64, times ``timedelta64``,
    ``DateOfBirth`` ``datetime64``); rows are sorted by ``Position`` with
    unknown positions last (stable), and the index is ``DriverNumber``, made
    unique for shared cars by :func:`unique_driver_labels`.
    """
    frame = pd.DataFrame(rows) if rows else pd.DataFrame(columns=list(RESULTS_COLUMN_ORDER))
    for column in _STR_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA if column in _NO_FILL_STR else ""
        frame[column] = frame[column].astype(object)
        if column not in _NO_FILL_STR:
            frame[column] = frame[column].where(frame[column].notna(), "")
    for column in _FLOAT_COLUMNS:
        if column not in frame.columns:
            frame[column] = math.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")
    for column in _TIMEDELTA_COLUMNS:
        if column not in frame.columns or frame[column].isna().all():
            frame[column] = pd.Series(pd.NaT, index=frame.index, dtype="timedelta64[ns]")
        else:
            frame[column] = pd.to_timedelta(frame[column], errors="coerce").astype(
                "timedelta64[ns]"
            )
    for column in _DATETIME_COLUMNS:
        if column not in frame.columns or frame[column].isna().all():
            frame[column] = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
        else:
            frame[column] = pd.to_datetime(frame[column], errors="coerce").astype("datetime64[ns]")
    frame = frame.loc[:, list(RESULTS_COLUMN_ORDER)]
    if len(frame):
        frame = frame.sort_values("Position", kind="stable", na_position="last")
        numbers = frame["DriverNumber"].astype(str).tolist()
        codes = frame["Abbreviation"].astype(str).tolist()
        frame.index = pd.Index(unique_driver_labels(list(zip(numbers, codes))), name=None)
    return frame


def results_columns() -> tuple[tuple[str, ...], tuple[str, ...]]:
    """``(fastf1 columns, tif1 extras)`` of ``Session.results``."""
    return FASTF1_RESULTS_COLUMNS, TIF1_RESULTS_EXTRA_COLUMNS


# -- standings -----------------------------------------------------------------


def build_standings_frame(payload: dict[str, Any] | None, kind: str) -> pd.DataFrame:
    """Driver or constructor standings as a frame (``kind``: ``"driver"``/``"constructor"``)."""
    leaf = "DriverStandings" if kind == "driver" else "ConstructorStandings"
    rows: list[dict[str, Any]] = []
    lists = (
        payload.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists", [])
        if isinstance(payload, dict)
        else []
    )
    standings = lists[0] if lists else {}
    for row in standings.get(leaf, []) if isinstance(standings, dict) else []:
        position = _as_int(row.get("position"))
        out: dict[str, Any] = {
            "Position": float(position) if position is not None else math.nan,
            "PositionText": str(row.get("positionText", "")),
            "Points": _as_float(row.get("points")),
            "Wins": _as_float(row.get("wins")),
        }
        if kind == "driver":
            driver = row.get("Driver", {}) if isinstance(row.get("Driver"), dict) else {}
            constructors = [c for c in row.get("Constructors", []) if isinstance(c, dict)]
            date_of_birth = date_from_ergast(driver.get("dateOfBirth"))
            out.update(
                {
                    "DriverNumber": str(driver.get("permanentNumber", "")),
                    "Abbreviation": str(driver.get("code", "")),
                    "DriverId": str(driver.get("driverId", "")),
                    "FirstName": str(driver.get("givenName", "")),
                    "LastName": str(driver.get("familyName", "")),
                    "FullName": f"{driver.get('givenName', '')} {driver.get('familyName', '')}".strip(),
                    "DateOfBirth": pd.Timestamp(date_of_birth)
                    if date_of_birth is not None
                    else pd.NaT,
                    "DriverNationality": str(driver.get("nationality", "")),
                    "TeamIds": [str(c.get("constructorId", "")) for c in constructors],
                    "TeamNames": [str(c.get("name", "")) for c in constructors],
                    "DriverUrl": str(driver.get("url", "")),
                }
            )
        else:
            constructor = (
                row.get("Constructor", {}) if isinstance(row.get("Constructor"), dict) else {}
            )
            out.update(
                {
                    "TeamId": str(constructor.get("constructorId", "")),
                    "TeamName": str(constructor.get("name", "")),
                    "TeamNationality": str(constructor.get("nationality", "")),
                    "TeamUrl": str(constructor.get("url", "")),
                }
            )
        rows.append(out)
    columns = (
        [
            "Position",
            "PositionText",
            "Points",
            "Wins",
            "DriverNumber",
            "Abbreviation",
            "DriverId",
            "FirstName",
            "LastName",
            "FullName",
            "DateOfBirth",
            "DriverNationality",
            "TeamIds",
            "TeamNames",
            "DriverUrl",
        ]
        if kind == "driver"
        else [
            "Position",
            "PositionText",
            "Points",
            "Wins",
            "TeamId",
            "TeamName",
            "TeamNationality",
            "TeamUrl",
        ]
    )
    frame = pd.DataFrame(rows, columns=columns)
    if "DateOfBirth" in frame.columns:
        frame["DateOfBirth"] = pd.to_datetime(frame["DateOfBirth"], errors="coerce")
    for column in ("Position", "Points", "Wins"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")
    return frame


# -- pit stops -----------------------------------------------------------------


def _time_of_day(value: Any) -> Any:
    """``"15:05:40"`` as a Timedelta since midnight (local track time), ``pd.NaT`` otherwise."""
    if not isinstance(value, str) or not value:
        return pd.NaT
    try:
        parts = [int(part) for part in value.split(":")]
    except ValueError:
        return pd.NaT
    while len(parts) < 3:
        parts.insert(0, 0)
    hours, minutes, seconds = parts[-3:]
    return pd.Timedelta(hours=hours, minutes=minutes, seconds=seconds)


def build_pit_stops_frame(
    race: dict[str, Any] | None, classification: list[dict[str, Any]]
) -> pd.DataFrame:
    """Ergast pit stops of a race as a frame (``DriverNumber``/``Driver`` from the results)."""
    identity: dict[str, tuple[str, str]] = {}
    for row in classification:
        driver = row.get("Driver", {}) if isinstance(row.get("Driver"), dict) else {}
        driver_id = driver.get("driverId")
        if driver_id:
            identity[str(driver_id)] = (_number(row), str(driver.get("code") or driver_id))
    rows: list[dict[str, Any]] = []
    stops = race.get("PitStops", []) if isinstance(race, dict) else []
    for stop in stops:
        if not isinstance(stop, dict):
            continue
        driver_id = str(stop.get("driverId", ""))
        number, code = identity.get(driver_id, ("", driver_id))
        rows.append(
            {
                "DriverNumber": number,
                "Driver": code,
                "DriverId": driver_id,
                "Stop": _as_float(stop.get("stop")),
                "Lap": _as_float(stop.get("lap")),
                "TimeOfDay": _time_of_day(stop.get("time")),
                "Duration": _timedelta(stop.get("duration")),
            }
        )
    columns = ["DriverNumber", "Driver", "DriverId", "Stop", "Lap", "TimeOfDay", "Duration"]
    frame = pd.DataFrame(rows, columns=columns)
    for column in ("Stop", "Lap"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")
    for column in ("TimeOfDay", "Duration"):
        frame[column] = pd.to_timedelta(frame[column], errors="coerce")
    return frame


# -- laps from Ergast lap timings ---------------------------------------------

_EMPTY_LAP_KEYS = (
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


def build_laps_payloads(
    laps_race: dict[str, Any] | None,
    classification: list[dict[str, Any]],
    pit_stops_race: dict[str, Any] | None,
) -> list[tuple[dict[str, list[Any]], str, str]]:
    """Per-driver laptime payloads (TracingInsights key names) from Ergast lap timings.

    Lap times, positions and lap numbers come from the timings; ``sesT``
    (session time at the end of the lap) and ``lST`` (lap start) are the
    cumulative race time, ``stint`` is derived from the pit-stop laps (a stop
    on lap ``L`` starts a new stint on ``L + 1``), ``dNum`` and the team come
    from the race results. Channels Ergast does not know (sectors, tyres,
    speeds, pit times) are ``None``. Returns ``(payload, driver_code, team)``
    tuples for :func:`tif1.core._merge_lap_payloads`.
    """
    identity: dict[str, tuple[str, str, str]] = {}
    for row in classification:
        driver = row.get("Driver", {}) if isinstance(row.get("Driver"), dict) else {}
        constructor = row.get("Constructor", {}) if isinstance(row.get("Constructor"), dict) else {}
        driver_id = driver.get("driverId")
        if not driver_id:
            continue
        identity[str(driver_id)] = (
            driver_code(row),
            _number(row),
            str(constructor.get("name", "")),
        )

    stop_laps: dict[str, list[int]] = defaultdict(list)
    stops = pit_stops_race.get("PitStops", []) if isinstance(pit_stops_race, dict) else []
    for stop in stops:
        lap = _as_int(stop.get("lap")) if isinstance(stop, dict) else None
        if lap is not None:
            stop_laps[str(stop.get("driverId", ""))].append(lap)

    per_driver: dict[str, list[tuple[int, float | None, int | None]]] = defaultdict(list)
    laps = laps_race.get("Laps", []) if isinstance(laps_race, dict) else []
    for lap in laps:
        if not isinstance(lap, dict):
            continue
        number = _as_int(lap.get("number"))
        if number is None:
            continue
        for timing in lap.get("Timings", []):
            if not isinstance(timing, dict):
                continue
            driver_id = str(timing.get("driverId", ""))
            parsed = timedelta_from_ergast(timing.get("time"))
            seconds = parsed.total_seconds() if parsed is not None else None
            per_driver[driver_id].append((number, seconds, _as_int(timing.get("position"))))

    payloads: list[tuple[dict[str, list[Any]], str, str]] = []
    for driver_id in sorted(per_driver, key=lambda key: identity.get(key, (key, "", ""))[0]):
        entries = sorted(per_driver[driver_id], key=lambda item: item[0])
        code, number, team = identity.get(driver_id, (driver_id, "", ""))
        stops_before = sorted(stop_laps.get(driver_id, []))
        elapsed = 0.0
        payload: dict[str, list[Any]] = defaultdict(list)
        for lap_number, seconds, position in entries:
            start = elapsed
            if seconds is not None:
                elapsed += seconds
            stint = 1 + sum(1 for stop_lap in stops_before if stop_lap < lap_number)
            payload["lap"].append(lap_number)
            payload["time"].append(seconds)
            payload["pos"].append(position)
            payload["sesT"].append(elapsed if seconds is not None else None)
            payload["lST"].append(start)
            payload["stint"].append(stint)
            payload["dNum"].append(number)
            payload["iacc"].append(True)
            payload["ff1G"].append(False)
            payload["del"].append(False)
            for key in _EMPTY_LAP_KEYS:
                payload[key].append(None)
        payloads.append((dict(payload), code, team))
    return payloads


def total_laps_from_rows(classification: list[dict[str, Any]]) -> int | None:
    """The leader's lap count from the race classification (None when unknown)."""
    counts = [_as_int(row.get("laps")) for row in classification]
    valid = [count for count in counts if count is not None]
    return max(valid) if valid else None


__all__ = [
    "LEAF_KEYS",
    "SessionKind",
    "broadcast_name",
    "build_laps_payloads",
    "build_pit_stops_frame",
    "build_standings_frame",
    "driver_code",
    "ergast_endpoint",
    "ergast_rows",
    "finalize_results_frame",
    "merge_ergast_rows",
    "order_driver_rows",
    "results_columns",
    "session_kind",
    "synthesize_driver_row",
    "team_color_map",
    "total_laps_from_rows",
    "unique_driver_labels",
]
