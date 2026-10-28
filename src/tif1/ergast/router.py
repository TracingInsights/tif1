"""Route an Ergast query to the Stats repository or to Jolpica.

Every ``Ergast.get_*`` call becomes a :class:`QueryPlan`; :func:`execute`
answers it with a canonical ``{"MRData": {...}}`` payload plus the source it
came from, so the flattening code never knows which source answered.

Routing rule (the design's decision D2 "derive as much as possible from
Stats"): anything scoped to a season that the Stats files can answer
*exactly* is assembled from them, season-wide answers from the per-round
files in one parallel fetch; everything else goes to Jolpica:

- ``seasons`` and ``drivers`` lists (Jolpica lists practice-only drivers,
  which no Stats file contains),
- ``status`` (needs Ergast's numeric ``statusId``) and any ``status=`` filter,
- queries without a season, and the ``current``/``last`` selectors, which
  are resolved through Jolpica first (cached for an hour) and then routed
  like plain integers.

A Stats file that is missing on every mirror or empty (``total == 0``) falls
through to Jolpica for that round alone; the answer is cached either way.
"""

from __future__ import annotations

import copy
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from tif1.config import get_config
from tif1.ergast.jolpica import DEFAULT_PAGE_SIZE, JolpicaClient
from tif1.ergast.stats import RoundInfo, StatsSource, get_stats_source
from tif1.exceptions import DataNotFoundError, ErgastInvalidRequestError, NetworkError

logger = logging.getLogger(__name__)

SOURCE_STATS = "stats"
SOURCE_JOLPICA = "jolpica"

_RESULT_LEAVES = {
    "results": "Results",
    "qualifying": "QualifyingResults",
    "sprint": "SprintResults",
}
#: Selectors applied as local row filters per results-like endpoint; any
#: other selector on these endpoints routes to Jolpica (it needs a table
#: the Stats files do not carry, e.g. the race grid for qualifying rows).
_ROW_FILTERS: dict[str, frozenset[str]] = {
    "results": frozenset(
        {"driver", "constructor", "grid_position", "results_position", "fastest_rank"}
    ),
    "sprint": frozenset(
        {"driver", "constructor", "grid_position", "results_position", "fastest_rank"}
    ),
    "qualifying": frozenset({"driver", "constructor", "results_position"}),
}
#: Selectors that make a schedule/circuit/constructor query depend on results.
_RESULT_DEPENDENT = frozenset(
    {"driver", "constructor", "grid_position", "results_position", "fastest_rank"}
)
_STATS_ENDPOINTS = frozenset(
    {
        "races",
        "results",
        "qualifying",
        "sprint",
        "driverStandings",
        "constructorStandings",
        "laps",
        "pitstops",
        "constructors",
        "circuits",
    }
)
_STANDINGS_LEAVES = {
    "driverStandings": "DriverStandings",
    "constructorStandings": "ConstructorStandings",
}
_MAX_STANDINGS_PROBES = 3


class StatsMissError(Exception):
    """The Stats repository cannot answer this query (route to Jolpica)."""


@dataclass(slots=True)
class QueryPlan:
    """One Ergast request: endpoint, response table/category and selectors."""

    endpoint: str
    table: str
    category: dict[str, Any]
    subcategory: dict[str, Any] | None
    selectors: dict[str, Any] = field(default_factory=dict)
    limit: int | None = None
    offset: int | None = None
    source: str = "auto"


@dataclass(slots=True)
class ExecutedQuery:
    """A routed answer: the payload, the source that produced it and when."""

    payload: dict[str, Any]
    source: str
    fetched_at: float


def _fallback_enabled() -> bool:
    return bool(get_config().get("ergast_fallback", True))


def _jolpica_disabled_error(what: str) -> ErgastInvalidRequestError:
    return ErgastInvalidRequestError(
        f"{what} needs the Jolpica API, which is disabled (ergast_fallback=False / "
        "TIF1_ERGAST_FALLBACK=0)"
    )


def resolve_selectors(selectors: dict[str, Any], jolpica: JolpicaClient) -> dict[str, Any]:
    """Normalize the ``season`` and ``round`` selectors to integers.

    Digit strings (``"2024"``, what fastf1 users often pass) become integers so
    the Stats routing sees them; ``"current"`` and ``"last"`` resolve through
    Jolpica.
    """
    resolved = dict(selectors)
    for key in ("season", "round"):
        value = resolved.get(key)
        if isinstance(value, str) and value.strip().isdigit():
            resolved[key] = int(value.strip())
    season = resolved.get("season")
    round_number = resolved.get("round")
    if season == "current":
        if not _fallback_enabled():
            raise _jolpica_disabled_error("season='current'")
        resolved["season"] = jolpica.resolve_current_season()
    if round_number == "last":
        if not _fallback_enabled():
            raise _jolpica_disabled_error("round='last'")
        season_value = resolved.get("season")
        if season_value is None:
            raise ErgastInvalidRequestError("round='last' requires a season")
        resolved["round"] = jolpica.resolve_last_round(season_value)
    return resolved


def stats_applicable(plan: QueryPlan, selectors: dict[str, Any]) -> bool:
    """True when the routing matrix lets the Stats repository answer ``plan``."""
    if plan.endpoint not in _STATS_ENDPOINTS:
        return False
    if selectors.get("status") is not None:
        return False
    season = selectors.get("season")
    if not isinstance(season, int) or isinstance(season, bool):
        return False
    round_number = selectors.get("round")
    if round_number is not None and not isinstance(round_number, int):
        return False
    if plan.endpoint in ("laps", "pitstops") and round_number is None:
        return False
    if plan.endpoint in _ROW_FILTERS:
        allowed = _ROW_FILTERS[plan.endpoint] | {"season", "round", "circuit"}
        extra = {k for k, v in selectors.items() if v is not None and k not in allowed}
        if extra:
            return False
    return True


def execute(
    plan: QueryPlan,
    *,
    stats: StatsSource | None = None,
    jolpica: JolpicaClient | None = None,
) -> ExecutedQuery:
    """Answer ``plan`` from Stats or Jolpica according to the routing matrix.

    Raises:
        DataNotFoundError: ``source="stats"`` was forced and Stats cannot answer.
        ErgastInvalidRequestError: Jolpica is needed but disabled, or rejected the request.
        NetworkError: every source failed.
    """
    stats = stats if stats is not None else get_stats_source()
    jolpica = jolpica if jolpica is not None else JolpicaClient(loader=stats.loader)
    selectors = resolve_selectors(plan.selectors, jolpica)
    fetched_at = time.time()

    use_stats = plan.source != SOURCE_JOLPICA and stats_applicable(plan, selectors)
    if plan.source == SOURCE_STATS and not use_stats:
        raise DataNotFoundError(endpoint=plan.endpoint, reason="not answerable from Stats")

    if use_stats:
        try:
            payload = _execute_stats(plan, selectors, stats, jolpica)
        except StatsMissError as miss:
            if plan.source == SOURCE_STATS:
                raise DataNotFoundError(endpoint=plan.endpoint, reason=str(miss)) from miss
            if not _fallback_enabled():
                raise DataNotFoundError(endpoint=plan.endpoint, reason=str(miss)) from miss
            logger.debug("Stats cannot answer %s (%s); using Jolpica", plan.endpoint, miss)
        else:
            payload = apply_paging(payload, plan.table, limit=plan.limit, offset=plan.offset)
            return ExecutedQuery(payload, SOURCE_STATS, fetched_at)

    if not _fallback_enabled():
        raise _jolpica_disabled_error(f"The '{plan.endpoint}' query")
    payload = _execute_jolpica(plan, selectors, jolpica)
    return ExecutedQuery(payload, SOURCE_JOLPICA, fetched_at)


# -- Jolpica -----------------------------------------------------------------


def _execute_jolpica(plan: QueryPlan, selectors: dict[str, Any], jolpica: JolpicaClient) -> dict:
    url = jolpica.url(plan.endpoint, **selectors)
    payload = jolpica.get_rows(url, limit=plan.limit, offset=plan.offset)
    # Cached payloads are shared; hand the caller its own copy.
    return copy.deepcopy(payload)


# -- Stats -------------------------------------------------------------------


def _execute_stats(
    plan: QueryPlan, sel: dict[str, Any], stats: StatsSource, jolpica: JolpicaClient
) -> dict:
    endpoint = plan.endpoint
    season = int(sel["season"])
    round_number = sel.get("round")
    now = datetime.now(UTC)

    if endpoint in _RESULT_LEAVES:
        races = _collect_results(stats, jolpica, season, round_number, endpoint, sel, now)
        return _race_table(season, round_number, races, sel, endpoint)
    if endpoint == "races":
        return _stats_races(stats, jolpica, season, round_number, sel, now)
    if endpoint in ("laps", "pitstops"):
        if round_number is None:
            raise StatsMissError(f"{endpoint} queries need a round")
        handler = _stats_laps if endpoint == "laps" else _stats_pitstops
        return handler(stats, jolpica, season, int(round_number), sel, now)
    if endpoint in _STANDINGS_LEAVES:
        return _stats_standings(stats, season, round_number, endpoint, sel, now)
    if endpoint == "constructors":
        return _stats_constructors(stats, jolpica, season, round_number, sel, now)
    if endpoint == "circuits":
        return _stats_circuits(stats, jolpica, season, round_number, sel, now)
    raise StatsMissError(f"endpoint {endpoint!r} is not served from Stats")


def _envelope(table: str, body: dict, *, total: int, url: str) -> dict:
    return {
        "MRData": {
            "xmlns": "",
            "series": "f1",
            "url": url,
            "limit": str(total),
            "offset": "0",
            "total": str(total),
            table: body,
        }
    }


def _stats_url(endpoint: str, sel: dict[str, Any]) -> str:
    query = "&".join(f"{key}={sel[key]}" for key in ("season", "round") if sel.get(key) is not None)
    return f"stats://{endpoint}?{query}" if query else f"stats://{endpoint}"


def _season_rounds(
    stats: StatsSource,
    season: int,
    round_number: int | None,
    *,
    sprint_only: bool,
    now: datetime,
) -> list[RoundInfo]:
    rounds = stats.rounds(season, round_number, sprint_only=sprint_only, now=now)
    if rounds is None:
        raise StatsMissError(f"season {season} is not in the Stats repository")
    return rounds


def _matches(row: dict, sel: dict[str, Any], endpoint: str) -> bool:
    """Apply the local row filters of a results-like row."""
    driver = sel.get("driver")
    if driver is not None and row.get("Driver", {}).get("driverId") != driver:
        return False
    constructor = sel.get("constructor")
    if constructor is not None and row.get("Constructor", {}).get("constructorId") != constructor:
        return False
    grid = sel.get("grid_position")
    if grid is not None and _as_int(row.get("grid")) != int(grid):
        return False
    position = sel.get("results_position")
    if position is not None and _as_int(row.get("position")) != int(position):
        return False
    fastest = sel.get("fastest_rank")
    return fastest is None or _as_int(row.get("FastestLap", {}).get("rank")) == int(fastest)


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _jolpica_round(
    jolpica: JolpicaClient, endpoint: str, season: int, round_number: int, sel: dict[str, Any]
) -> dict | None:
    """``Races[0]`` of a per-round Jolpica answer (complete), or None when unavailable."""
    if not _fallback_enabled():
        return None
    selectors = {"season": season, "round": round_number}
    for key in ("driver", "constructor", "circuit", "grid_position", "results_position"):
        if sel.get(key) is not None:
            selectors[key] = sel[key]
    if endpoint == "laps":
        for key in ("lap_number",):
            if sel.get(key) is not None:
                selectors[key] = sel[key]
    if endpoint == "pitstops":
        for key in ("stop_number", "lap_number"):
            if sel.get(key) is not None:
                selectors[key] = sel[key]
    if sel.get("fastest_rank") is not None and endpoint in _RESULT_LEAVES:
        selectors["fastest_rank"] = sel["fastest_rank"]
    payload = jolpica.get_rows(jolpica.url(endpoint, **selectors), complete=True)
    race = StatsSource.first_race(payload)
    if race is None:
        return None
    if endpoint == "laps" and "Laps" in race:
        from tif1.ergast.jolpica import merge_split_laps

        race = {**race, "Laps": merge_split_laps(race["Laps"])}
    return copy.deepcopy(race)


def _collect_results(
    stats: StatsSource,
    jolpica: JolpicaClient,
    season: int,
    round_number: int | None,
    endpoint: str,
    sel: dict[str, Any],
    now: datetime,
) -> list[dict]:
    """Race dicts (with filtered leaf rows) of a results-like endpoint, in round order."""
    leaf = _RESULT_LEAVES[endpoint]
    rounds = _season_rounds(stats, season, round_number, sprint_only=endpoint == "sprint", now=now)
    circuit = sel.get("circuit")
    if circuit is not None:
        rounds = [info for info in rounds if info.circuit_id == circuit]
    fetched = stats.files(season, rounds, endpoint)
    races: list[dict] = []
    for info in rounds:
        payload = fetched.get(info.round)
        race = StatsSource.first_race(None if isinstance(payload, Exception) else payload)
        if race is None:
            fallback = _jolpica_round(jolpica, endpoint, season, info.round, sel)
            if fallback is None:
                if isinstance(payload, Exception):
                    # The round could not be read from Stats (transport error)
                    # and nothing confirmed it from Jolpica: never drop it silently.
                    raise NetworkError(
                        url=f"stats/{season}/{info.slug}", status_code=None
                    ) from payload
                logger.warning(
                    "Ergast %s for %s round %s (%s): no Stats file and %s; "
                    "the round is left out of the answer",
                    endpoint,
                    season,
                    info.round,
                    info.slug,
                    "Jolpica has no rows" if _fallback_enabled() else "the Jolpica fallback is off",
                )
                continue
            race = fallback
        rows = [row for row in race.get(leaf, []) if _matches(row, sel, endpoint)]
        if not rows:
            continue
        races.append({**race, leaf: rows})
    return races


def _race_table(
    season: int, round_number: int | None, races: list[dict], sel: dict[str, Any], endpoint: str
) -> dict:
    leaf = _RESULT_LEAVES.get(endpoint)
    total = sum(len(race.get(leaf, [])) for race in races) if leaf else len(races)
    body: dict[str, Any] = {"season": str(season)}
    if round_number is not None:
        body["round"] = str(round_number)
    body["Races"] = races
    return _envelope("RaceTable", body, total=total, url=_stats_url(endpoint, sel))


def _rounds_with_results(
    stats: StatsSource,
    jolpica: JolpicaClient,
    season: int,
    round_number: int | None,
    sel: dict[str, Any],
    now: datetime,
) -> set[int]:
    """Rounds whose race results match the result-dependent selectors."""
    races = _collect_results(stats, jolpica, season, round_number, "results", sel, now)
    return {_as_int(race.get("round")) or 0 for race in races}


def _stats_races(
    stats: StatsSource,
    jolpica: JolpicaClient,
    season: int,
    round_number: int | None,
    sel: dict[str, Any],
    now: datetime,
) -> dict:
    index = stats.season_index(season)
    if index is None:
        raise StatsMissError(f"season {season} is not in the Stats repository")
    rounds = [index[key] for key in sorted(index)]
    if round_number is not None:
        rounds = [info for info in rounds if info.round == int(round_number)]
    circuit = sel.get("circuit")
    if circuit is not None:
        rounds = [info for info in rounds if info.circuit_id == circuit]
    if any(sel.get(key) is not None for key in _RESULT_DEPENDENT):
        keep = _rounds_with_results(stats, jolpica, season, round_number, sel, now)
        rounds = [info for info in rounds if info.round in keep]
    races = [copy.deepcopy(info.race) for info in rounds]
    body: dict[str, Any] = {"season": str(season)}
    if round_number is not None:
        body["round"] = str(round_number)
    body["Races"] = races
    return _envelope("RaceTable", body, total=len(races), url=_stats_url("races", sel))


def _stats_laps(
    stats: StatsSource,
    jolpica: JolpicaClient,
    season: int,
    round_number: int,
    sel: dict[str, Any],
    now: datetime,
) -> dict:
    rounds = _season_rounds(stats, season, round_number, sprint_only=False, now=now)
    race = None
    if rounds:
        payload = stats.file(season, round_number, "laps")
        race = StatsSource.first_race(payload)
        if race is None:
            race = _jolpica_round(jolpica, "laps", season, round_number, sel)
    laps: list[dict] = []
    if race is not None:
        lap_number = sel.get("lap_number")
        driver = sel.get("driver")
        for lap in race.get("Laps", []):
            if lap_number is not None and _as_int(lap.get("number")) != int(lap_number):
                continue
            timings = lap.get("Timings", [])
            if driver is not None:
                timings = [t for t in timings if t.get("driverId") == driver]
            if not timings:
                continue
            laps.append({**lap, "Timings": timings})
    total = sum(len(lap["Timings"]) for lap in laps)
    races: list[dict] = []
    if race is not None and laps:
        races.append({**{k: v for k, v in race.items() if k != "Laps"}, "Laps": laps})
    body = {"season": str(season), "round": str(round_number), "Races": races}
    return _envelope("RaceTable", body, total=total, url=_stats_url("laps", sel))


def _stats_pitstops(
    stats: StatsSource,
    jolpica: JolpicaClient,
    season: int,
    round_number: int,
    sel: dict[str, Any],
    now: datetime,
) -> dict:
    rounds = _season_rounds(stats, season, round_number, sprint_only=False, now=now)
    race = None
    if rounds:
        payload = stats.file(season, round_number, "pitstops")
        race = StatsSource.first_race(payload)
        if race is None:
            race = _jolpica_round(jolpica, "pitstops", season, round_number, sel)
    stops: list[dict] = []
    if race is not None:
        stop_number = sel.get("stop_number")
        lap_number = sel.get("lap_number")
        driver = sel.get("driver")
        for stop in race.get("PitStops", []):
            if stop_number is not None and _as_int(stop.get("stop")) != int(stop_number):
                continue
            if lap_number is not None and _as_int(stop.get("lap")) != int(lap_number):
                continue
            if driver is not None and stop.get("driverId") != driver:
                continue
            stops.append(stop)
    races: list[dict] = []
    if race is not None and stops:
        races.append({**{k: v for k, v in race.items() if k != "PitStops"}, "PitStops": stops})
    body = {"season": str(season), "round": str(round_number), "Races": races}
    return _envelope("RaceTable", body, total=len(stops), url=_stats_url("pitstops", sel))


def last_completed_round(
    stats: StatsSource, season: int, endpoint: str, now: datetime | None = None
) -> tuple[int, dict] | None:
    """``(round, payload)`` of the latest round whose ``endpoint`` file holds rows.

    Walks back from the last round that has started by ``now``, probing at
    most :data:`_MAX_STANDINGS_PROBES` rounds (the repository lags a round or
    two right after a race weekend).
    """
    moment = now if now is not None else datetime.now(UTC)
    rounds = stats.rounds(season, None, now=moment)
    if not rounds:
        return None
    for info in reversed(rounds[-_MAX_STANDINGS_PROBES:]):
        payload = stats.file(season, info.round, endpoint)
        if payload is not None:
            return info.round, payload
    return None


def _stats_standings(
    stats: StatsSource,
    season: int,
    round_number: int | None,
    endpoint: str,
    sel: dict[str, Any],
    now: datetime,
) -> dict:
    leaf = _STANDINGS_LEAVES[endpoint]
    if round_number is not None:
        rounds = _season_rounds(stats, season, int(round_number), sprint_only=False, now=now)
        if not rounds:
            raise StatsMissError(f"round {round_number} of {season} is not in Stats")
        payload = stats.file(season, int(round_number), endpoint)
        if payload is None:
            raise StatsMissError(f"no {endpoint} file for {season} round {round_number}")
        resolved_round = int(round_number)
    else:
        found = last_completed_round(stats, season, endpoint, now)
        if found is None:
            raise StatsMissError(f"no {endpoint} file for {season}")
        resolved_round, payload = found

    standings, rows = StatsSource.standings_rows(payload, leaf)
    if standings is None:
        raise StatsMissError(f"malformed {endpoint} file for {season} round {resolved_round}")
    driver = sel.get("driver")
    constructor = sel.get("constructor")
    position = sel.get("standings_position")
    kept = [
        row
        for row in rows
        if (driver is None or row.get("Driver", {}).get("driverId") == driver)
        and (constructor is None or row.get("Constructor", {}).get("constructorId") == constructor)
        and (position is None or _as_int(row.get("position")) == int(position))
    ]
    lists = [{**{k: v for k, v in standings.items() if k != leaf}, leaf: kept}] if kept else []
    body = {"season": str(season), "round": str(resolved_round), "StandingsLists": lists}
    return _envelope("StandingsTable", body, total=len(kept), url=_stats_url(endpoint, sel))


def _stats_constructors(
    stats: StatsSource,
    jolpica: JolpicaClient,
    season: int,
    round_number: int | None,
    sel: dict[str, Any],
    now: datetime,
) -> dict:
    row_sel = {k: v for k, v in sel.items() if k != "constructor"}
    races = _collect_results(stats, jolpica, season, round_number, "results", row_sel, now)
    wanted = sel.get("constructor")
    constructors: dict[str, dict] = {}
    for race in races:
        for row in race.get("Results", []):
            constructor = row.get("Constructor")
            if not isinstance(constructor, dict):
                continue
            constructor_id = constructor.get("constructorId")
            if wanted is not None and constructor_id != wanted:
                continue
            if constructor_id is not None and constructor_id not in constructors:
                constructors[constructor_id] = copy.deepcopy(constructor)
    items = [constructors[key] for key in sorted(constructors)]
    body: dict[str, Any] = {"season": str(season)}
    if round_number is not None:
        body["round"] = str(round_number)
    body["Constructors"] = items
    return _envelope(
        "ConstructorTable", body, total=len(items), url=_stats_url("constructors", sel)
    )


def _stats_circuits(
    stats: StatsSource,
    jolpica: JolpicaClient,
    season: int,
    round_number: int | None,
    sel: dict[str, Any],
    now: datetime,
) -> dict:
    index = stats.season_index(season)
    if index is None:
        raise StatsMissError(f"season {season} is not in the Stats repository")
    rounds = [index[key] for key in sorted(index)]
    if round_number is not None:
        rounds = [info for info in rounds if info.round == int(round_number)]
    if any(sel.get(key) is not None for key in _RESULT_DEPENDENT):
        row_sel = {k: v for k, v in sel.items() if k != "circuit"}
        keep = _rounds_with_results(stats, jolpica, season, round_number, row_sel, now)
        rounds = [info for info in rounds if info.round in keep]
    wanted = sel.get("circuit")
    circuits: dict[str, dict] = {}
    for info in rounds:
        circuit = info.race.get("Circuit")
        if not isinstance(circuit, dict):
            continue
        circuit_id = circuit.get("circuitId")
        if wanted is not None and circuit_id != wanted:
            continue
        if circuit_id is not None and circuit_id not in circuits:
            circuits[circuit_id] = copy.deepcopy(circuit)
    items = [circuits[key] for key in sorted(circuits)]
    body: dict[str, Any] = {"season": str(season)}
    if round_number is not None:
        body["round"] = str(round_number)
    body["Circuits"] = items
    return _envelope("CircuitTable", body, total=len(items), url=_stats_url("circuits", sel))


# -- paging ------------------------------------------------------------------


def _slice_window(count: int, seen: int, offset: int, limit: int) -> tuple[int, int]:
    start = min(max(offset - seen, 0), count)
    end = min(max(offset + limit - seen, 0), count)
    return start, end


def apply_paging(payload: dict, table: str, *, limit: int | None, offset: int | None) -> dict:
    """Slice a complete payload to fastf1's ``limit``/``offset`` window over leaf rows.

    Leaf rows are what Ergast counts: result rows, lap timings, pit stops,
    standings rows, races, circuits, constructors. The envelope's ``total``
    keeps the full count; ``limit`` and ``offset`` describe the window, so
    :attr:`ErgastResponseMixin.is_complete` and paging behave like fastf1.
    ``limit=None`` means every row from ``offset``.
    """
    mrdata = payload["MRData"]
    body = mrdata.get(table)
    if not isinstance(body, dict):
        return payload
    start = int(offset or 0)
    list_key = next((k for k, v in body.items() if isinstance(v, list)), None)
    if list_key is None:
        return payload
    items = body[list_key]

    if table == "RaceTable":
        leaf = next(
            (
                k
                for k in ("Results", "QualifyingResults", "SprintResults", "PitStops", "Laps")
                if items and k in items[0]
            ),
            None,
        )
        if leaf == "Laps":
            kept, total = _page_laps(items, start, limit)
        elif leaf is not None:
            kept, total = _page_leaf(items, leaf, start, limit)
        else:
            total = len(items)
            kept = items[start : None if limit is None else start + limit]
    elif table == "StandingsTable":
        leaf = next(
            (k for k in ("DriverStandings", "ConstructorStandings") if items and k in items[0]),
            None,
        )
        if leaf is not None:
            kept, total = _page_leaf(items, leaf, start, limit)
        else:
            total = len(items)
            kept = items[start : None if limit is None else start + limit]
    else:
        total = len(items)
        kept = items[start : None if limit is None else start + limit]

    window = total - start if limit is None else limit
    new_body = {**body, list_key: kept}
    new_mrdata = {**mrdata, table: new_body}
    new_mrdata["total"] = str(total)
    new_mrdata["limit"] = str(max(window, 0))
    new_mrdata["offset"] = str(start)
    return {"MRData": new_mrdata}


def _page_leaf(
    items: list[dict], leaf: str, offset: int, limit: int | None
) -> tuple[list[dict], int]:
    total = sum(len(item.get(leaf, [])) for item in items)
    window = total if limit is None else limit
    kept: list[dict] = []
    seen = 0
    for item in items:
        rows = item.get(leaf, [])
        start, end = _slice_window(len(rows), seen, offset, window)
        seen += len(rows)
        if end > start:
            kept.append({**item, leaf: rows[start:end]})
    return kept, total


def _page_laps(items: list[dict], offset: int, limit: int | None) -> tuple[list[dict], int]:
    total = sum(len(lap.get("Timings", [])) for item in items for lap in item.get("Laps", []))
    window = total if limit is None else limit
    kept: list[dict] = []
    seen = 0
    for item in items:
        laps_kept: list[dict] = []
        for lap in item.get("Laps", []):
            timings = lap.get("Timings", [])
            start, end = _slice_window(len(timings), seen, offset, window)
            seen += len(timings)
            if end > start:
                laps_kept.append({**lap, "Timings": timings[start:end]})
        if laps_kept:
            kept.append({**item, "Laps": laps_kept})
    return kept, total


def default_page_size(source: str) -> int | None:
    """fastf1's default page size for a source: 30 on Jolpica, everything on Stats."""
    return DEFAULT_PAGE_SIZE if source == SOURCE_JOLPICA else None


__all__ = [
    "SOURCE_JOLPICA",
    "SOURCE_STATS",
    "ExecutedQuery",
    "QueryPlan",
    "StatsMissError",
    "apply_paging",
    "default_page_size",
    "execute",
    "last_completed_round",
    "resolve_selectors",
    "stats_applicable",
]
