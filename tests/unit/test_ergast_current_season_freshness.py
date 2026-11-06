"""Current-season Ergast freshness (Corinth C4): settled classifications, stored season index."""

from __future__ import annotations

import math
from datetime import UTC, datetime

import pytest

from tif1.cdn import CDNManager, CDNSource
from tif1.ergast import payloads as payloads_module
from tif1.ergast.payloads import (
    SETTLED_AFTER_SECONDS,
    ErgastFetchNeeded,
    ErgastPayloadLoader,
    cache_only,
    is_settled_classification,
)
from tif1.ergast.stats import StatsSource
from tif1.payload_loader import InMemoryTransport

YEAR = payloads_module.current_season()
DAY = 86400.0
RACE_START = datetime(YEAR, 1, 10, 14, 0, tzinfo=UTC).timestamp()
NEXT_START = datetime(YEAR, 1, 24, 14, 0, tzinfo=UTC).timestamp()


def _race(round_number: int, name: str, date: str) -> dict:
    return {
        "season": str(YEAR),
        "round": str(round_number),
        "raceName": name,
        "date": date,
        "time": "14:00:00Z",
    }


ROUND_1 = _race(1, "First Grand Prix", f"{YEAR}-01-10")
ROUND_2 = _race(2, "Second Grand Prix", f"{YEAR}-01-24")
EVENTS = {"MRData": {"total": "2", "RaceTable": {"season": str(YEAR), "Races": [ROUND_1, ROUND_2]}}}


def _results(race: dict) -> dict:
    row = {"number": "1", "position": "1", "Driver": {"driverId": "max"}}
    return {
        "MRData": {
            "total": "1",
            "RaceTable": {
                "season": str(YEAR),
                "round": race["round"],
                "Races": [{**race, "Results": [row]}],
            },
        }
    }


def _at(clock: list[float]) -> datetime:
    return datetime.fromtimestamp(clock[0], tz=UTC)


def _manager() -> CDNManager:
    manager = CDNManager()
    manager.sources = [
        CDNSource(name="A", base_url="https://a.example/gh/TracingInsights", priority=1)
    ]
    manager._failure_counts = {"A": 0}
    manager.reset()
    return manager


@pytest.fixture(autouse=True)
def _clear_memo():
    payloads_module.clear_memo()
    yield
    payloads_module.clear_memo()


def _loader(transport: InMemoryTransport, clock: list[float]) -> ErgastPayloadLoader:
    return ErgastPayloadLoader(
        transport=transport, cdn_manager=_manager(), enable_cache=False, clock=lambda: clock[0]
    )


class TestSettledClassification:
    key = f"stats/{YEAR}/first-grand-prix/results.json"

    def test_settled_two_weeks_after_the_race(self):
        payload = _results(ROUND_1)
        assert is_settled_classification(self.key, payload, RACE_START + SETTLED_AFTER_SECONDS)
        assert not is_settled_classification(
            self.key, payload, RACE_START + SETTLED_AFTER_SECONDS - 1
        )

    def test_only_classification_files_with_rows(self):
        payload = _results(ROUND_1)
        late = RACE_START + 30 * DAY
        for name in ("quali_results.json", "sprint_results.json"):
            assert is_settled_classification(f"stats/{YEAR}/first-grand-prix/{name}", payload, late)
        for name in ("laptimes.json", "pitstops.json", "driverPoints.json"):
            assert not is_settled_classification(
                f"stats/{YEAR}/first-grand-prix/{name}", payload, late
            )
        assert not is_settled_classification(f"stats/{YEAR}/events.json", EVENTS, late)
        assert not is_settled_classification(f"jolpica/{YEAR}/1/results.json", payload, late)
        empty = {"MRData": {"total": "0", "RaceTable": {"Races": []}}}
        assert not is_settled_classification(self.key, empty, late)
        assert not is_settled_classification(self.key, None, late)
        undated = _results({**ROUND_1, "date": ""})
        assert not is_settled_classification(self.key, undated, late)

    def test_a_race_without_a_time_starts_at_midnight_utc(self):
        payload = _results({**ROUND_1, "time": ""})
        midnight = datetime(YEAR, 1, 10, tzinfo=UTC).timestamp()
        assert is_settled_classification(self.key, payload, midnight + SETTLED_AFTER_SECONDS)

    def test_entry_ttl(self):
        ttl = payloads_module.ergast_ttl_seconds()
        payload = _results(ROUND_1)
        late = RACE_START + 20 * DAY
        entry_ttl = ErgastPayloadLoader._entry_ttl
        assert entry_ttl(YEAR, payload, late, self.key) == math.inf
        assert entry_ttl(YEAR, payload, RACE_START + DAY, self.key) == ttl
        # Past seasons are permanent whatever the file.
        assert entry_ttl(2000, payload, 0.0, "stats/2000/x/laptimes.json") == math.inf


class TestLoaderKeepsSettledRounds:
    def test_latest_round_refreshes_until_settled(self):
        transport = InMemoryTransport({"first-grand-prix/results.json": _results(ROUND_1)})
        clock = [RACE_START + DAY]
        loader = _loader(transport, clock)
        assert loader.stats_payload(YEAR, "first-grand-prix", "results.json") is not None
        clock[0] += payloads_module.ergast_ttl_seconds() + 1
        loader.stats_payload(YEAR, "first-grand-prix", "results.json")
        assert len(transport.calls) == 2  # the latest round keeps ergast_ttl_hours
        clock[0] = RACE_START + 15 * DAY
        loader.stats_payload(YEAR, "first-grand-prix", "results.json")
        assert len(transport.calls) == 3  # fetched once more past the window...
        clock[0] += 365 * DAY
        loader.stats_payload(YEAR, "first-grand-prix", "results.json")
        assert len(transport.calls) == 3  # ...then settled

    def test_missing_file_is_never_settled(self):
        transport = InMemoryTransport({})
        clock = [RACE_START + 30 * DAY]
        loader = _loader(transport, clock)
        assert loader.stats_payload(YEAR, "first-grand-prix", "results.json") is None
        clock[0] += payloads_module.ergast_ttl_seconds() + 1
        assert loader.stats_payload(YEAR, "first-grand-prix", "results.json") is None
        assert len(transport.calls) == 2


class TestStoredSeasonIndex:
    def _source(self, clock: list[float]) -> tuple[StatsSource, InMemoryTransport]:
        transport = InMemoryTransport(
            {
                f"{YEAR}/events.json": EVENTS,
                "first-grand-prix/results.json": _results(ROUND_1),
                "second-grand-prix/results.json": _results(ROUND_2),
            }
        )
        return StatsSource(_loader(transport, clock)), transport

    def test_finished_round_needs_no_index_refresh(self):
        clock = [NEXT_START + DAY]  # both races ran before the index was fetched
        source, transport = self._source(clock)
        assert source.season_index(YEAR) is not None
        clock[0] += payloads_module.ergast_ttl_seconds() + 1
        assert not source.season_index_cached(YEAR)
        assert source.season_index_stored(YEAR)
        (info,) = source.rounds(YEAR, 2, now=_at(clock)) or []
        assert info.slug == "second-grand-prix"
        assert source.file(YEAR, 2, "results") is not None
        assert [call.rsplit("/", 1)[-1] for call in transport.calls] == [
            "events.json",
            "results.json",
        ]
        # A season-wide lookup still refreshes the index.
        source.rounds(YEAR, now=_at(clock))
        assert [call.rsplit("/", 1)[-1] for call in transport.calls][-1] == "events.json"

    def test_round_ahead_of_the_stored_index_refreshes_it(self):
        clock = [RACE_START + DAY]  # round 2 had not started when the index was fetched
        source, transport = self._source(clock)
        source.season_index(YEAR)
        clock[0] = NEXT_START + DAY
        assert source.settled_round(YEAR, 1) is not None
        assert source.settled_round(YEAR, 2) is None
        assert source.settled_round(YEAR, 3) is None
        (info,) = source.rounds(YEAR, 2, now=_at(clock)) or []
        assert info.round == 2
        assert [call.rsplit("/", 1)[-1] for call in transport.calls] == [
            "events.json",
            "events.json",
        ]

    def test_cache_only_answers_a_settled_round_from_a_stale_index(self):
        clock = [NEXT_START + 20 * DAY]
        source, transport = self._source(clock)
        source.season_index(YEAR)
        source.file(YEAR, 1, "results")
        clock[0] += 30 * DAY  # index and latest-round TTLs long past
        with cache_only():
            assert source.file(YEAR, 1, "results") is not None
            with pytest.raises(ErgastFetchNeeded):
                source.rounds(YEAR, now=_at(clock))
        assert len(transport.calls) == 2

    def test_no_stored_index(self):
        clock = [NEXT_START + DAY]
        source, _transport = self._source(clock)
        assert not source.season_index_stored(YEAR)
        assert source.settled_round(YEAR, 1) is None
