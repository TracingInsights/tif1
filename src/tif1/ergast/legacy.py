"""Legacy helpers kept from fastf1 (``fetch_results``, ``fetch_season``, ``fetch_day``).

They answer with the raw ``Races`` lists of the Ergast response, routed like
every other query (Stats repository first, Jolpica fallback).
"""

from __future__ import annotations

from typing import Any

from tif1.ergast import structure
from tif1.ergast.router import QueryPlan, execute

_SESSION_ENDPOINTS = {
    "results": ("results", structure.Races_RaceResults, structure.RaceResults),
    "qualifying": ("qualifying", structure.Races_QualifyingResults, structure.QualifyingResults),
    "sprint": ("sprint", structure.Races_SprintResults, structure.SprintResults),
}


def _races(endpoint: str, category: dict, subcategory: dict | None, **selectors: Any) -> list:
    plan = QueryPlan(
        endpoint=endpoint,
        table="RaceTable",
        category=category,
        subcategory=subcategory,
        selectors={k: v for k, v in selectors.items() if v is not None},
    )
    executed = execute(plan)
    return executed.payload["MRData"].get("RaceTable", {}).get("Races", [])


def fetch_results(year: int, gp: int, session: str) -> list:
    """Results of one session: ``'Race'``, ``'Qualifying'``, ``'Sprint'`` or ``'Sprint Qualifying'``.

    Returns the list of result rows of that race (``Results``,
    ``QualifyingResults`` or ``SprintResults``), as fastf1's helper did.
    """
    if session == "Race":
        day, sel = "results", "Results"
    elif session == "Qualifying":
        day, sel = "qualifying", "QualifyingResults"
    elif session in ("Sprint Qualifying", "Sprint"):
        day, sel = "sprint", "SprintResults"
    else:
        raise ValueError(f"Unsupported session {session!r}")
    races = fetch_day(year, gp, day)
    return races[0][sel] if races else []


def fetch_season(year: int) -> list:
    """The ``Races`` list of a season's schedule."""
    return _races("races", structure.Races_Schedule, None, season=year)


def fetch_day(year: int, gp: int, day: str) -> list:
    """The ``Races`` list of one round's ``results``, ``qualifying`` or ``sprint``."""
    if day not in _SESSION_ENDPOINTS:
        raise ValueError(f"Unsupported day {day!r}; use 'results', 'qualifying' or 'sprint'")
    endpoint, category, subcategory = _SESSION_ENDPOINTS[day]
    return _races(endpoint, category, subcategory, season=year, round=gp)


__all__ = ["fetch_day", "fetch_results", "fetch_season"]
