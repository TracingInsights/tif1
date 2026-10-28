"""The ``TracingInsights-Archive/Stats`` repository as an Ergast data source.

Layout (raw Jolpica ``MRData`` payloads, one file per table and round)::

    {year}/events.json                      /{year}/races/
    {year}/{slug}/event_info.json           one Race object of the schedule
    {year}/{slug}/results.json              /{year}/{round}/results
    {year}/{slug}/quali_results.json        /{year}/{round}/qualifying
    {year}/{slug}/sprint_results.json       /{year}/{round}/sprint (sprint rounds)
    {year}/{slug}/laptimes.json             /{year}/{round}/laps, all pages merged (1996-)
    {year}/{slug}/pitstops.json             /{year}/{round}/pitstops (2011-)
    {year}/{slug}/driverPoints.json         /{year}/{round}/driverstandings
    {year}/{slug}/teamPoints.json           /{year}/{round}/constructorstandings

``slug`` is ``raceName.lower().replace(" ", "-")`` of the race as
``events.json`` names it, so it is always resolved through that file by round
(the repository has stale folders and renamed races). The season-level
``driverPoints.json``/``teamPoints.json`` files are stale and never read.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from tif1.ergast.jolpica import merge_split_laps
from tif1.ergast.payloads import ErgastPayloadLoader, get_ergast_loader, is_empty_payload

logger = logging.getLogger(__name__)

#: Ergast endpoint -> Stats file per round.
STATS_FILES: dict[str, str] = {
    "results": "results.json",
    "qualifying": "quali_results.json",
    "sprint": "sprint_results.json",
    "laps": "laptimes.json",
    "pitstops": "pitstops.json",
    "driverStandings": "driverPoints.json",
    "constructorStandings": "teamPoints.json",
    "event": "event_info.json",
}
EVENTS_FILE = "events.json"


def race_slug(race_name: str) -> str:
    """Folder name of a race in the Stats repository (the generator's rule)."""
    return race_name.lower().replace(" ", "-")


@dataclass(slots=True)
class RoundInfo:
    """One round of a season as ``events.json`` lists it."""

    round: int
    race_name: str
    slug: str
    race: dict[str, Any]

    @property
    def has_sprint(self) -> bool:
        return "Sprint" in self.race

    @property
    def circuit_id(self) -> str | None:
        circuit = self.race.get("Circuit")
        return circuit.get("circuitId") if isinstance(circuit, dict) else None

    @property
    def race_datetime(self) -> datetime | None:
        """Race start in UTC (midnight UTC when the schedule has no time)."""
        date = self.race.get("date")
        if not isinstance(date, str) or not date:
            return None
        time_part = self.race.get("time")
        text = (
            f"{date}T{time_part}"
            if isinstance(time_part, str) and time_part
            else f"{date}T00:00:00Z"
        )
        try:
            start = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        # Ergast times are UTC; a time without an offset is read as UTC too.
        return start if start.tzinfo is not None else start.replace(tzinfo=UTC)

    def completed(self, now: datetime) -> bool:
        """True when the race has started by ``now`` (its files can exist)."""
        start = self.race_datetime
        return start is None or start <= now


class StatsSource:
    """Season index and per-round files of the Stats repository.

    Args:
        loader: Payload pipeline (defaults to the shared Ergast loader).
    """

    def __init__(self, loader: ErgastPayloadLoader | None = None) -> None:
        self._loader = loader
        # Season index keyed on the events.json race list it was built from
        # (the list itself, held so a later object can never recycle its
        # identity): a refreshed payload (current-season TTL) rebuilds it,
        # and a season the repository does not hold is never remembered.
        self._index: dict[int, tuple[list[dict[str, Any]], dict[int, RoundInfo]]] = {}

    @property
    def loader(self) -> ErgastPayloadLoader:
        return self._loader if self._loader is not None else get_ergast_loader()

    # -- season index --------------------------------------------------------

    def races(self, season: int, *, fast: bool = False) -> list[dict[str, Any]] | None:
        """The ``Races`` list of ``{season}/events.json``, or None when Stats has no season.

        ``fast`` makes one attempt per mirror (no retry/backoff).
        """
        payload = self.loader.stats_payload(season, "", EVENTS_FILE, fast=fast)
        if payload is None:
            return None
        races = payload.get("MRData", {}).get("RaceTable", {}).get("Races")
        return races if isinstance(races, list) else None

    def season_index(self, season: int) -> dict[int, RoundInfo] | None:
        """``{round: RoundInfo}`` for ``season`` (memoized), or None when unknown to Stats."""
        races = self.races(season)
        if races is None:
            return None
        cached = self._index.get(season)
        if cached is not None and cached[0] is races:
            return cached[1]
        index: dict[int, RoundInfo] = {}
        for race in races:
            try:
                number = int(str(race.get("round")))
            except (TypeError, ValueError):
                continue
            name = str(race.get("raceName", ""))
            index[number] = RoundInfo(number, name, race_slug(name), race)
        self._index[season] = (races, index)
        return index

    def round_info(self, season: int, round_number: int) -> RoundInfo | None:
        index = self.season_index(season)
        return None if index is None else index.get(int(round_number))

    def rounds(
        self,
        season: int,
        round_number: int | None = None,
        *,
        sprint_only: bool = False,
        now: datetime | None = None,
    ) -> list[RoundInfo] | None:
        """Rounds of ``season`` whose files can exist, in round order.

        ``round_number`` restricts to one round (an unknown round yields an
        empty list). ``sprint_only`` keeps sprint rounds. Rounds that have not
        started by ``now`` are dropped: their files cannot exist yet.
        """
        index = self.season_index(season)
        if index is None:
            return None
        if round_number is not None:
            info = index.get(int(round_number))
            selected = [info] if info is not None else []
        else:
            selected = [index[key] for key in sorted(index)]
        if sprint_only:
            selected = [info for info in selected if info.has_sprint]
        moment = now if now is not None else datetime.now(UTC)
        return [info for info in selected if info.completed(moment)]

    # -- files ---------------------------------------------------------------

    def file(self, season: int, round_number: int, endpoint: str) -> dict | None:
        """The normalized payload of one round file, or None when missing or empty."""
        info = self.round_info(season, round_number)
        if info is None:
            return None
        payload = self.loader.stats_payload(season, info.slug, STATS_FILES[endpoint])
        return self.normalize(payload, endpoint)

    def files(
        self, season: int, rounds: list[RoundInfo], endpoint: str
    ) -> dict[int, dict | Exception | None]:
        """Payloads of one file type for many rounds (parallel fetch).

        Each value is the normalized payload, ``None`` (missing or empty) or
        the exception its fetch raised.
        """
        file_name = STATS_FILES[endpoint]
        refs = [(season, info.slug, file_name) for info in rounds]
        fetched = self.loader.stats_payloads(refs)
        result: dict[int, dict | Exception | None] = {}
        for info, payload in zip(rounds, fetched):
            if isinstance(payload, Exception):
                result[info.round] = payload
            else:
                result[info.round] = self.normalize(payload, endpoint)
        return result

    @staticmethod
    def normalize(payload: dict | None, endpoint: str) -> dict | None:
        """None for missing/empty payloads; laps with page-split entries merged."""
        if payload is None or is_empty_payload(payload):
            return None
        if endpoint != "laps":
            return payload
        races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        if not races or "Laps" not in races[0]:
            return payload
        race = races[0]
        merged_laps = merge_split_laps(race["Laps"])
        if len(merged_laps) == len(race["Laps"]):
            return payload
        mrdata = dict(payload["MRData"])
        table = dict(mrdata["RaceTable"])
        table["Races"] = [{**race, "Laps": merged_laps}, *races[1:]]
        mrdata["RaceTable"] = table
        return {"MRData": mrdata}

    @staticmethod
    def first_race(payload: dict | None) -> dict | None:
        """``Races[0]`` of a RaceTable payload, or None."""
        if payload is None:
            return None
        races = payload.get("MRData", {}).get("RaceTable", {}).get("Races")
        if isinstance(races, list) and races and isinstance(races[0], dict):
            return races[0]
        return None

    @staticmethod
    def standings_rows(payload: dict | None, leaf: str) -> tuple[dict | None, list[dict]]:
        """``(standings_list, rows)`` of a StandingsTable payload (rows empty when absent)."""
        if payload is None:
            return None, []
        lists = payload.get("MRData", {}).get("StandingsTable", {}).get("StandingsLists")
        if not isinstance(lists, list) or not lists:
            return None, []
        standings = lists[0]
        rows = standings.get(leaf, [])
        return standings, rows if isinstance(rows, list) else []


_shared_source: StatsSource | None = None


def get_stats_source() -> StatsSource:
    """The process-wide :class:`StatsSource` over the shared loader."""
    global _shared_source
    if _shared_source is None:
        _shared_source = StatsSource()
    return _shared_source


def reset_stats_source() -> None:
    """Drop the shared source and its season index (tests)."""
    global _shared_source
    _shared_source = None


__all__ = [
    "EVENTS_FILE",
    "STATS_FILES",
    "RoundInfo",
    "StatsSource",
    "get_stats_source",
    "race_slug",
    "reset_stats_source",
]
