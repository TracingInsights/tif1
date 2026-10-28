"""Jolpica client: URL building, rate limiting, paging and error mapping."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tif1.ergast import jolpica
from tif1.ergast.jolpica import JolpicaClient, RateLimiter, build_url, merge_pages, merge_split_laps
from tif1.ergast.payloads import ErgastPayloadLoader
from tif1.exceptions import (
    ErgastInvalidRequestError,
    ErgastJsonError,
    NetworkError,
    RateLimitExceededError,
)
from tif1.payload_loader import InMemoryTransport

FIXTURES = Path(__file__).parent.parent / "fixtures" / "ergast"
BASE = "https://api.jolpi.ca/ergast/f1"


class TestBuildUrl:
    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"endpoint": "seasons"}, f"{BASE}/seasons.json"),
            ({"endpoint": "races", "season": 2024}, f"{BASE}/2024/races.json"),
            ({"endpoint": "results", "season": 2024, "round": 3}, f"{BASE}/2024/3/results.json"),
            (
                {"endpoint": "results", "season": 2024, "driver": "alonso"},
                f"{BASE}/2024/drivers/alonso/results.json",
            ),
            ({"endpoint": "drivers", "driver": "alonso"}, f"{BASE}/drivers/alonso.json"),
            (
                {"endpoint": "results", "season": 2024, "grid_position": 1, "fastest_rank": 1},
                f"{BASE}/2024/grid/1/fastest/1/results.json",
            ),
            (
                {"endpoint": "driverStandings", "season": 2024, "standings_position": 1},
                f"{BASE}/2024/driverStandings/1.json",
            ),
            (
                {"endpoint": "results", "season": 2024, "results_position": 1},
                f"{BASE}/2024/results/1.json",
            ),
            (
                {"endpoint": "laps", "season": 2024, "round": 1, "lap_number": 5},
                f"{BASE}/2024/1/laps/5.json",
            ),
            (
                {"endpoint": "pitstops", "season": 2024, "round": 1, "stop_number": 2},
                f"{BASE}/2024/1/pitstops/2.json",
            ),
            ({"endpoint": "status", "season": 2024, "status": 1}, f"{BASE}/2024/status/1.json"),
            ({"endpoint": "circuits", "circuit": "monza"}, f"{BASE}/circuits/monza.json"),
            (
                {"endpoint": "races", "season": "current", "round": "last"},
                f"{BASE}/current/last/races.json",
            ),
        ],
    )
    def test_matches_fastf1(self, kwargs, expected):
        assert build_url(base=BASE, **kwargs) == expected

    def test_uses_configured_base(self, monkeypatch):
        from tif1.config import get_config

        monkeypatch.setitem(get_config()._config, "jolpica_base_url", "https://mirror.example/f1/")
        assert build_url("seasons") == "https://mirror.example/f1/seasons.json"


class TestRateLimiter:
    def test_burst_then_wait(self):
        clock = [0.0]
        sleeps: list[float] = []

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            clock[0] += seconds

        limiter = RateLimiter(2, 500, clock=lambda: clock[0], sleep=sleep)
        assert limiter.acquire() == 0.0
        assert limiter.acquire() == 0.0
        waited = limiter.acquire()
        assert waited == pytest.approx(0.5)
        assert sleeps == [pytest.approx(0.5)]

    def test_hourly_window(self):
        clock = [0.0]
        sleeps: list[float] = []

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            clock[0] += seconds

        limiter = RateLimiter(1000, 3, clock=lambda: clock[0], sleep=sleep)
        for _ in range(3):
            limiter.acquire()
        limiter.acquire()
        assert sleeps
        assert sleeps[0] == pytest.approx(3600.0)

    def test_process_limiter_reads_config(self, monkeypatch):
        from tif1.config import get_config

        jolpica.reset_rate_limiter()
        monkeypatch.setitem(get_config()._config, "ergast_rate_limit_burst", 7)
        try:
            assert jolpica.get_rate_limiter().burst_per_second == 7
        finally:
            jolpica.reset_rate_limiter()


class TestMergeSplitLaps:
    def test_merges_duplicate_lap_numbers(self):
        laps = [
            {"number": "6", "Timings": [{"driverId": "a"}]},
            {"number": "6", "Timings": [{"driverId": "b"}, {"driverId": "c"}]},
            {"number": "7", "Timings": [{"driverId": "a"}]},
        ]
        merged = merge_split_laps(laps)
        assert [lap["number"] for lap in merged] == ["6", "7"]
        assert [t["driverId"] for t in merged[0]["Timings"]] == ["a", "b", "c"]
        assert laps[0]["Timings"] == [{"driverId": "a"}]  # input untouched

    def test_stats_laptimes_fixture_has_split_laps(self):
        payload = json.loads(
            (FIXTURES / "stats" / "2025" / "chinese-grand-prix" / "laptimes.json").read_text()
        )
        laps = payload["MRData"]["RaceTable"]["Races"][0]["Laps"]
        merged = merge_split_laps(laps)
        assert len(laps) == 66
        assert len(merged) == 56
        assert sum(len(lap["Timings"]) for lap in merged) == 1065


class TestMergePages:
    def _page(self, races: list, offset: int, limit: int = 100, total: int = 3) -> dict:
        return {
            "MRData": {
                "xmlns": "",
                "series": "f1",
                "url": "u",
                "limit": str(limit),
                "offset": str(offset),
                "total": str(total),
                "RaceTable": {"season": "2024", "Races": races},
            }
        }

    def test_merges_races_split_across_pages(self):
        page1 = self._page([{"season": "2024", "round": "1", "Results": [{"n": 1}, {"n": 2}]}], 0)
        page2 = self._page(
            [
                {"season": "2024", "round": "1", "Results": [{"n": 3}]},
                {"season": "2024", "round": "2", "Results": [{"n": 4}]},
            ],
            2,
        )
        merged = merge_pages([page1, page2])
        races = merged["MRData"]["RaceTable"]["Races"]
        assert [race["round"] for race in races] == ["1", "2"]
        assert [r["n"] for r in races[0]["Results"]] == [1, 2, 3]
        assert merged["MRData"]["limit"] == "4"
        assert merged["MRData"]["offset"] == "0"
        assert merged["MRData"]["total"] == "3"

    def test_merges_standings_and_plain_lists(self):
        def standings(rows):
            return {
                "MRData": {
                    "total": "4",
                    "limit": "2",
                    "offset": "0",
                    "StandingsTable": {
                        "StandingsLists": [
                            {"season": "2024", "round": "1", "DriverStandings": rows}
                        ]
                    },
                }
            }

        merged = merge_pages([standings([{"p": 1}, {"p": 2}]), standings([{"p": 3}])])
        rows = merged["MRData"]["StandingsTable"]["StandingsLists"][0]["DriverStandings"]
        assert [r["p"] for r in rows] == [1, 2, 3]

        def seasons(rows):
            return {
                "MRData": {
                    "total": "4",
                    "limit": "2",
                    "offset": "0",
                    "SeasonTable": {"Seasons": rows},
                }
            }

        merged = merge_pages([seasons([{"s": 1}]), seasons([{"s": 2}])])
        assert merged["MRData"]["SeasonTable"]["Seasons"] == [{"s": 1}, {"s": 2}]


def _client(transport: InMemoryTransport, **kwargs) -> JolpicaClient:
    loader = ErgastPayloadLoader(transport=transport, enable_cache=False)
    limiter = RateLimiter(1000, 100000)
    return JolpicaClient(
        transport=transport,
        loader=loader,
        limiter=limiter,
        base=BASE,
        sleep=lambda _s: None,
        **kwargs,
    )


class TestJolpicaClient:
    def test_get_returns_payload_and_caches_in_memo(self, ergast_transport):
        client = _client(ergast_transport)
        payload = client.get(f"{BASE}/seasons.json", {"limit": 5})
        assert payload["MRData"]["SeasonTable"]["Seasons"][0]["season"] == "1950"
        client.get(f"{BASE}/seasons.json", {"limit": 5})
        assert ergast_transport.calls.count(f"{BASE}/seasons.json?limit=5") == 1

    def test_404_maps_to_invalid_request(self, ergast_transport):
        client = _client(ergast_transport)
        with pytest.raises(ErgastInvalidRequestError, match="Not Found"):
            client.fetch(f"{BASE}/1900/results.json")

    def test_non_dict_body_is_json_error(self):
        transport = InMemoryTransport({"bad.json": {"nope": 1}})
        with pytest.raises(ErgastJsonError):
            _client(transport).fetch("https://api.jolpi.ca/ergast/f1/bad.json")

    def test_429_retries_then_raises(self):
        attempts: list[str] = []

        def throttled(url: str):
            attempts.append(url)
            raise NetworkError(url=url, status_code=429, retry_after=1.5)

        transport = InMemoryTransport({"x.json": throttled})
        client = _client(transport)
        with pytest.raises(RateLimitExceededError, match="retry after 2s"):
            client.fetch("https://api.jolpi.ca/ergast/f1/x.json")
        assert len(attempts) == 4

    def test_429_recovers(self):
        state = {"n": 0}

        def flaky(url: str):
            state["n"] += 1
            if state["n"] == 1:
                raise NetworkError(url=url, status_code=429)
            return {
                "MRData": {
                    "total": "0",
                    "limit": "30",
                    "offset": "0",
                    "SeasonTable": {"Seasons": []},
                }
            }

        client = _client(InMemoryTransport({"x.json": flaky}))
        assert client.fetch("https://api.jolpi.ca/ergast/f1/x.json")["MRData"]["total"] == "0"

    def test_other_http_errors_are_invalid_requests(self):
        def boom(url: str):
            raise NetworkError(url=url, status_code=500)

        client = _client(InMemoryTransport({"x.json": boom}))
        with pytest.raises(ErgastInvalidRequestError, match="500"):
            client.fetch("https://api.jolpi.ca/ergast/f1/x.json")

    def test_transport_error_propagates(self):
        def down(url: str):
            raise NetworkError(url=url, status_code=None)

        client = _client(InMemoryTransport({"x.json": down}))
        with pytest.raises(NetworkError):
            client.fetch("https://api.jolpi.ca/ergast/f1/x.json")

    def test_offline_mode_never_requests(self, ergast_transport):
        from tif1.config import get_config

        client = _client(ergast_transport)
        get_config().set("offline_mode", True)
        try:
            with pytest.raises(NetworkError):
                client.fetch(f"{BASE}/seasons.json")
        finally:
            get_config().set("offline_mode", False)
        assert ergast_transport.calls == []

    def test_get_rows_single_request_within_page_cap(self, ergast_transport):
        client = _client(ergast_transport)
        client.get_rows(f"{BASE}/2024/drivers.json", limit=30)
        assert ergast_transport.calls == [f"{BASE}/2024/drivers.json?limit=30"]

    def test_get_rows_pages_beyond_cap(self):
        rows = [{"season": "2024", "round": "1", "PitStops": [{"stop": str(i)}]} for i in range(5)]

        def page(url: str):
            offset = int(url.split("offset=")[1])
            return {
                "MRData": {
                    "total": "5",
                    "limit": "100",
                    "offset": str(offset),
                    "RaceTable": {"season": "2024", "Races": rows[offset : offset + 2]},
                }
            }

        transport = InMemoryTransport({"pitstops.json": page})
        client = _client(transport)
        jolpica.MAX_PAGE_SIZE = 2
        try:
            merged = client.get_rows(
                "https://api.jolpi.ca/ergast/f1/2024/1/pitstops.json", complete=True
            )
        finally:
            jolpica.MAX_PAGE_SIZE = 100
        stops = merged["MRData"]["RaceTable"]["Races"][0]["PitStops"]
        assert [s["stop"] for s in stops] == ["0", "1", "2", "3", "4"]
        assert len(transport.calls) == 3

    def test_resolve_current_and_last(self, ergast_transport):
        client = _client(ergast_transport)
        assert client.resolve_current_season() == 2026
        assert client.resolve_last_round(2026) == 18
