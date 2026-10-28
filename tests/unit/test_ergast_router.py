"""Stats source and router: slug resolution, routing matrix, fallbacks, paging."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tif1.config import get_config
from tif1.ergast import structure
from tif1.ergast.jolpica import JolpicaClient
from tif1.ergast.payloads import ErgastPayloadLoader
from tif1.ergast.router import (
    QueryPlan,
    apply_paging,
    execute,
    last_completed_round,
    resolve_selectors,
    stats_applicable,
)
from tif1.ergast.stats import StatsSource, race_slug
from tif1.exceptions import DataNotFoundError, ErgastInvalidRequestError, NetworkError

NOW = datetime(2026, 10, 28, tzinfo=UTC)


def _plan(endpoint: str, table: str = "RaceTable", **selectors) -> QueryPlan:
    categories = {
        "results": (structure.Races_RaceResults, structure.RaceResults),
        "qualifying": (structure.Races_QualifyingResults, structure.QualifyingResults),
        "sprint": (structure.Races_SprintResults, structure.SprintResults),
        "laps": (structure.Races_Laps, structure.Laps),
        "pitstops": (structure.Races_PitStops, structure.PitStops),
        "races": (structure.Races_Schedule, None),
        "driverStandings": (structure.StandingsLists_Driver, structure.DriverStandings),
        "constructorStandings": (
            structure.StandingsLists_Constructor,
            structure.ConstructorStandings,
        ),
        "constructors": (structure.Constructors, None),
        "circuits": (structure.Circuits, None),
        "drivers": (structure.Drivers, None),
        "seasons": (structure.Seasons, None),
        "status": (structure.Status, None),
    }
    category, subcategory = categories[endpoint]
    limit = selectors.pop("limit", None)
    offset = selectors.pop("offset", None)
    source = selectors.pop("source", "auto")
    return QueryPlan(endpoint, table, category, subcategory, selectors, limit, offset, source)


@pytest.mark.usefixtures("ergast_offline")
class TestStatsSource:
    def test_slug_rule(self):
        assert race_slug("São Paulo Grand Prix") == "são-paulo-grand-prix"
        assert race_slug("Bahrain Grand Prix in Malaysia") == "bahrain-grand-prix-in-malaysia"

    def test_season_index_and_rounds(self):
        stats = StatsSource()
        index = stats.season_index(2025)
        assert index is not None
        assert len(index) == 24
        assert index[2].slug == "chinese-grand-prix"
        assert index[2].has_sprint
        assert not index[1].has_sprint
        assert index[2].circuit_id == "shanghai"
        assert stats.season_index(1900) is None
        rounds = stats.rounds(2025, sprint_only=True, now=NOW)
        assert [r.round for r in rounds] == [2, 6, 13, 19, 21, 23]
        assert stats.rounds(2025, 99, now=NOW) == []

    def test_rounds_before_start_are_skipped(self):
        stats = StatsSource()
        early = datetime(2025, 3, 20, tzinfo=UTC)
        assert [r.round for r in stats.rounds(2025, now=early)] == [1]

    def test_season_index_is_not_sticky(self, ergast_offline):
        stats = StatsSource()
        assert stats.season_index(1951) is None
        ergast_offline.add(
            "1951/events.json",
            {
                "MRData": {
                    "total": "1",
                    "RaceTable": {
                        "season": "1951",
                        "Races": [
                            {"round": "1", "raceName": "Swiss Grand Prix", "date": "1951-05-27"}
                        ],
                    },
                }
            },
        )
        from tif1.ergast.payloads import clear_memo

        clear_memo()
        index = stats.season_index(1951)
        assert index is not None
        assert index[1].slug == "swiss-grand-prix"
        assert stats.season_index(1951) is index  # memoized on the same payload

    def test_naive_race_time_is_read_as_utc(self):
        from tif1.ergast.stats import RoundInfo

        info = RoundInfo(1, "X", "x", {"date": "2024-03-02", "time": "15:00:00"})
        assert info.race_datetime == datetime(2024, 3, 2, 15, 0, tzinfo=UTC)
        assert info.completed(NOW)

    def test_file_normalizes_split_laps(self):
        stats = StatsSource()
        payload = stats.file(2025, 2, "laps")
        laps = payload["MRData"]["RaceTable"]["Races"][0]["Laps"]
        assert len(laps) == 56
        assert stats.file(1950, 1, "qualifying") is None  # total 0 -> empty
        assert stats.file(1950, 1, "laps") is None  # no file

    def test_files_parallel(self):
        stats = StatsSource()
        rounds = stats.rounds(2025, now=NOW)[:3]
        fetched = stats.files(2025, rounds, "results")
        assert fetched[2] is not None
        assert fetched[1] is None
        assert fetched[3] is None


class TestApplicability:
    @pytest.mark.parametrize(
        ("endpoint", "selectors", "expected"),
        [
            ("results", {"season": 2024}, True),
            ("results", {"season": 2024, "status": "1"}, False),
            ("results", {}, False),
            ("results", {"season": "current"}, False),
            ("qualifying", {"season": 2024, "grid_position": 1}, False),
            ("qualifying", {"season": 2024, "driver": "alonso"}, True),
            ("laps", {"season": 2024}, False),
            ("laps", {"season": 2024, "round": 1}, True),
            ("drivers", {"season": 2024}, False),
            ("seasons", {}, False),
            ("status", {"season": 2024}, False),
            ("circuits", {"season": 2024, "driver": "alonso"}, True),
        ],
    )
    def test_matrix(self, endpoint, selectors, expected):
        plan = _plan(endpoint, **selectors)
        assert stats_applicable(plan, plan.selectors) is expected


@pytest.mark.usefixtures("ergast_offline")
class TestExecute:
    def test_results_round_from_stats(self):
        executed = execute(_plan("results", season=2025, round=2))
        assert executed.source == "stats"
        races = executed.payload["MRData"]["RaceTable"]["Races"]
        assert len(races) == 1
        assert len(races[0]["Results"]) == 20
        assert executed.payload["MRData"]["total"] == "20"

    def test_local_filters(self):
        executed = execute(_plan("results", season=2025, round=2, driver="norris"))
        rows = executed.payload["MRData"]["RaceTable"]["Races"][0]["Results"]
        assert [r["Driver"]["driverId"] for r in rows] == ["norris"]
        executed = execute(_plan("results", season=2025, round=2, grid_position=1))
        assert (
            executed.payload["MRData"]["RaceTable"]["Races"][0]["Results"][0]["Driver"]["driverId"]
            == "piastri"
        )
        executed = execute(_plan("results", season=2025, round=2, fastest_rank=1))
        assert (
            executed.payload["MRData"]["RaceTable"]["Races"][0]["Results"][0]["Driver"]["driverId"]
            == "norris"
        )
        executed = execute(_plan("results", season=2025, round=2, constructor="ferrari"))
        assert len(executed.payload["MRData"]["RaceTable"]["Races"][0]["Results"]) == 2
        executed = execute(_plan("results", season=2025, round=2, circuit="monza"))
        assert executed.payload["MRData"]["RaceTable"]["Races"] == []

    def test_season_wide_assembly_skips_missing_rounds_when_fallback_off(self, caplog):
        get_config().set("ergast_fallback", False)
        with caplog.at_level("WARNING", logger="tif1.ergast.router"):
            executed = execute(_plan("results", season=2025))
        races = executed.payload["MRData"]["RaceTable"]["Races"]
        assert [race["round"] for race in races] == ["2"]  # only round 2 is in the fixtures
        # Every dropped round is reported, never left out silently.
        dropped = [
            record for record in caplog.records if "left out of the answer" in record.message
        ]
        assert dropped
        assert "round 1 (" in dropped[0].message
        assert "the Jolpica fallback is off" in dropped[0].message

    def test_digit_string_selectors_route_to_stats(self):
        get_config().set("ergast_fallback", False)  # Jolpica would raise
        executed = execute(_plan("results", season="2025", round="2"))
        assert executed.source == "stats"
        assert len(executed.payload["MRData"]["RaceTable"]["Races"][0]["Results"]) == 20
        assert resolve_selectors({"season": "2025", "round": " 2 "}, JolpicaClient()) == {
            "season": 2025,
            "round": 2,
        }

    def test_missing_round_falls_through_to_jolpica(self, ergast_offline):
        # Round 3 has no Stats file: Jolpica is asked for that round alone.
        ergast_offline.add(
            "2025/3/results.json?limit=100&offset=0",
            {
                "MRData": {
                    "total": "1",
                    "limit": "100",
                    "offset": "0",
                    "RaceTable": {
                        "season": "2025",
                        "round": "3",
                        "Races": [
                            {
                                "season": "2025",
                                "round": "3",
                                "raceName": "Japanese Grand Prix",
                                "Results": [
                                    {
                                        "number": "1",
                                        "position": "1",
                                        "Driver": {"driverId": "max_verstappen"},
                                    }
                                ],
                            }
                        ],
                    },
                }
            },
        )
        executed = execute(_plan("results", season=2025, round=3))
        assert executed.source == "stats"
        races = executed.payload["MRData"]["RaceTable"]["Races"]
        assert [r["round"] for r in races] == ["3"]
        assert races[0]["Results"][0]["Driver"]["driverId"] == "max_verstappen"
        assert any("2025/3/results.json" in call for call in ergast_offline.calls)

    def test_missing_round_jolpica_404_propagates(self):
        with pytest.raises(ErgastInvalidRequestError):
            execute(_plan("results", season=2025, round=3))

    def test_network_error_on_a_round_raises_when_fallback_off(self, ergast_offline, monkeypatch):
        get_config().set("ergast_fallback", False)
        original = ergast_offline.get_json

        def flaky(url: str, *, timeout=None):
            if "chinese-grand-prix/results.json" in url:
                raise NetworkError(url=url, status_code=None)
            return original(url, timeout=timeout)

        monkeypatch.setattr(ergast_offline, "get_json", flaky)
        with pytest.raises(NetworkError):
            execute(_plan("results", season=2025, round=2))

    def test_round_fetch_error_without_confirmation_raises(self, ergast_offline, monkeypatch):
        original = ergast_offline.get_json

        def flaky(url: str, *, timeout=None):
            if "chinese-grand-prix/results.json" in url:
                raise NetworkError(url=url, status_code=None)
            return original(url, timeout=timeout)

        monkeypatch.setattr(ergast_offline, "get_json", flaky)
        ergast_offline.add(
            "2025/2/results.json?limit=100&offset=0",
            {"MRData": {"total": "0", "limit": "100", "offset": "0", "RaceTable": {"Races": []}}},
        )
        with pytest.raises(NetworkError):
            execute(_plan("results", season=2025, round=2))

    def test_sprint_only_rounds(self):
        get_config().set("ergast_fallback", False)
        executed = execute(_plan("sprint", season=2025))
        races = executed.payload["MRData"]["RaceTable"]["Races"]
        assert [race["round"] for race in races] == ["2"]
        assert executed.payload["MRData"]["total"] == "20"

    def test_qualifying_round(self):
        executed = execute(_plan("qualifying", season=2025, round=2, results_position=1))
        rows = executed.payload["MRData"]["RaceTable"]["Races"][0]["QualifyingResults"]
        assert [r["Driver"]["driverId"] for r in rows] == ["piastri"]

    def test_laps_and_pitstops(self):
        executed = execute(_plan("laps", season=2025, round=2, lap_number=5, driver="norris"))
        laps = executed.payload["MRData"]["RaceTable"]["Races"][0]["Laps"]
        assert len(laps) == 1
        assert laps[0]["Timings"][0]["driverId"] == "norris"
        assert executed.payload["MRData"]["total"] == "1"
        executed = execute(_plan("pitstops", season=2025, round=2, stop_number=2))
        stops = executed.payload["MRData"]["RaceTable"]["Races"][0]["PitStops"]
        assert all(stop["stop"] == "2" for stop in stops)
        executed = execute(_plan("laps", season=2025, round=2, driver="nobody"))
        assert executed.payload["MRData"]["RaceTable"]["Races"] == []

    def test_standings_round_and_filters(self):
        executed = execute(_plan("driverStandings", "StandingsTable", season=2025, round=2))
        lists = executed.payload["MRData"]["StandingsTable"]["StandingsLists"]
        assert lists[0]["DriverStandings"][0]["Driver"]["driverId"] == "norris"
        executed = execute(
            _plan("driverStandings", "StandingsTable", season=2025, round=2, standings_position=3)
        )
        rows = executed.payload["MRData"]["StandingsTable"]["StandingsLists"][0]["DriverStandings"]
        assert len(rows) == 1
        assert rows[0]["position"] == "3"
        executed = execute(
            _plan(
                "constructorStandings",
                "StandingsTable",
                season=2025,
                round=2,
                constructor="ferrari",
            )
        )
        rows = executed.payload["MRData"]["StandingsTable"]["StandingsLists"][0][
            "ConstructorStandings"
        ]
        assert rows[0]["Constructor"]["constructorId"] == "ferrari"

    def test_standings_without_round_use_last_completed_round(self):
        stats = StatsSource()
        # Only round 2 of 2025 is in the fixtures; walking back from the
        # last round finds nothing within the probe window.
        assert last_completed_round(stats, 2025, "driverStandings", NOW) is None
        early = datetime(2025, 3, 25, tzinfo=UTC)
        found = last_completed_round(stats, 2025, "driverStandings", early)
        assert found is not None
        assert found[0] == 2
        after_round_one = datetime(1950, 5, 20, tzinfo=UTC)
        assert last_completed_round(stats, 1950, "driverStandings", after_round_one)[0] == 1
        assert last_completed_round(stats, 1950, "driverStandings", NOW) is None

    def test_standings_without_season_rejected(self):
        with pytest.raises(ErgastInvalidRequestError):
            execute(_plan("driverStandings", "StandingsTable"))

    def test_races_circuits_constructors(self):
        executed = execute(_plan("races", season=2025, circuit="shanghai"))
        races = executed.payload["MRData"]["RaceTable"]["Races"]
        assert [r["raceName"] for r in races] == ["Chinese Grand Prix"]
        executed = execute(_plan("circuits", "CircuitTable", season=2025))
        ids = [c["circuitId"] for c in executed.payload["MRData"]["CircuitTable"]["Circuits"]]
        assert ids == sorted(ids)
        assert "shanghai" in ids
        executed = execute(_plan("constructors", "ConstructorTable", season=2025, round=2))
        ids = [
            c["constructorId"]
            for c in executed.payload["MRData"]["ConstructorTable"]["Constructors"]
        ]
        assert ids == sorted(ids)
        assert len(ids) == 10
        executed = execute(
            _plan("constructors", "ConstructorTable", season=2025, round=2, driver="norris")
        )
        assert [
            c["constructorId"]
            for c in executed.payload["MRData"]["ConstructorTable"]["Constructors"]
        ] == ["mclaren"]

    def test_jolpica_only_endpoints(self):
        executed = execute(_plan("seasons", "SeasonTable", limit=5))
        assert executed.source == "jolpica"
        assert executed.payload["MRData"]["SeasonTable"]["Seasons"][0]["season"] == "1950"
        executed = execute(_plan("drivers", "DriverTable", season=2024, limit=30))
        assert executed.source == "jolpica"
        assert len(executed.payload["MRData"]["DriverTable"]["Drivers"]) == 25
        executed = execute(_plan("status", "StatusTable", season=2024))
        assert executed.source == "jolpica"

    def test_current_and_last_resolve_through_jolpica(self):
        get_config().set("ergast_fallback", False)
        with pytest.raises(ErgastInvalidRequestError, match="Jolpica"):
            execute(_plan("results", season="current"))
        get_config().set("ergast_fallback", True)
        executed = execute(_plan("races", season="current", round="last"))
        assert executed.source == "stats"
        races = executed.payload["MRData"]["RaceTable"]["Races"]
        assert [r["round"] for r in races] == ["18"]

    def test_forced_sources(self):
        with pytest.raises(DataNotFoundError):
            execute(_plan("seasons", "SeasonTable", source="stats"))
        with pytest.raises(DataNotFoundError):
            execute(_plan("results", season=1900, round=1, source="stats"))
        executed = execute(_plan("drivers", "DriverTable", season=2024, limit=30, source="jolpica"))
        assert executed.source == "jolpica"

    def test_fallback_disabled_blocks_jolpica(self):
        get_config().set("ergast_fallback", False)
        with pytest.raises(ErgastInvalidRequestError, match="disabled"):
            execute(_plan("seasons", "SeasonTable"))
        with pytest.raises(DataNotFoundError):
            execute(_plan("results", season=1900, round=1))

    def test_explicit_jolpica_client(self, ergast_offline):
        loader = ErgastPayloadLoader(transport=ergast_offline, enable_cache=False)
        client = JolpicaClient(transport=ergast_offline, loader=loader)
        executed = execute(_plan("seasons", "SeasonTable", limit=5), jolpica=client)
        assert executed.source == "jolpica"
        assert executed.payload is not loader.jolpica_payload  # a copy, never the cached object


@pytest.mark.usefixtures("ergast_offline")
class TestPaging:
    def _results_payload(self):
        return execute(_plan("results", season=2025, round=2)).payload

    def test_slice_results(self):
        payload = self._results_payload()
        page = apply_paging(payload, "RaceTable", limit=5, offset=0)
        rows = page["MRData"]["RaceTable"]["Races"][0]["Results"]
        assert len(rows) == 5
        assert page["MRData"]["total"] == "20"
        assert page["MRData"]["limit"] == "5"
        page = apply_paging(payload, "RaceTable", limit=5, offset=18)
        assert len(page["MRData"]["RaceTable"]["Races"][0]["Results"]) == 2
        page = apply_paging(payload, "RaceTable", limit=5, offset=40)
        assert page["MRData"]["RaceTable"]["Races"] == []
        page = apply_paging(payload, "RaceTable", limit=None, offset=15)
        assert len(page["MRData"]["RaceTable"]["Races"][0]["Results"]) == 5
        assert page["MRData"]["limit"] == "5"

    def test_slice_laps_counts_timings(self):
        payload = execute(_plan("laps", season=2025, round=2)).payload
        page = apply_paging(payload, "RaceTable", limit=25, offset=15)
        laps = page["MRData"]["RaceTable"]["Races"][0]["Laps"]
        assert sum(len(lap["Timings"]) for lap in laps) == 25
        assert [lap["number"] for lap in laps] == ["1", "2"]
        assert page["MRData"]["total"] == "1065"

    def test_slice_plain_lists_and_standings(self):
        payload = execute(_plan("circuits", "CircuitTable", season=2025)).payload
        page = apply_paging(payload, "CircuitTable", limit=3, offset=1)
        assert len(page["MRData"]["CircuitTable"]["Circuits"]) == 3
        payload = execute(_plan("driverStandings", "StandingsTable", season=2025, round=2)).payload
        page = apply_paging(payload, "StandingsTable", limit=2, offset=0)
        rows = page["MRData"]["StandingsTable"]["StandingsLists"][0]["DriverStandings"]
        assert [r["position"] for r in rows] == ["1", "2"]

    def test_plan_limit_applies_to_stats_answers(self):
        executed = execute(_plan("results", season=2025, round=2, limit=3, offset=2))
        rows = executed.payload["MRData"]["RaceTable"]["Races"][0]["Results"]
        assert [r["position"] for r in rows] == ["3", "4", "5"]
        assert executed.payload["MRData"]["offset"] == "2"
