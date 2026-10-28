"""The Ergast payload loader: memo, SQLite tier, TTL by season age, verdict guard."""

from __future__ import annotations

import pytest

from tif1.cdn import CDNManager, CDNSource
from tif1.ergast import payloads as payloads_module
from tif1.ergast.payloads import (
    ErgastPayloadLoader,
    is_current_season,
    is_empty_payload,
    stats_cache_key,
    stats_path,
)
from tif1.exceptions import DataNotFoundError, NetworkError
from tif1.payload_loader import InMemoryTransport

EMPTY = {"MRData": {"total": "0", "RaceTable": {"Races": []}}}
FULL = {"MRData": {"total": "1", "RaceTable": {"Races": [{"round": "1"}]}}}


def _manager(*names: str) -> CDNManager:
    manager = CDNManager()
    manager.sources = [
        CDNSource(
            name=name, base_url=f"https://{name.lower()}.example/gh/TracingInsights", priority=i
        )
        for i, name in enumerate(names, 1)
    ]
    manager._failure_counts = dict.fromkeys(names, 0)
    manager.reset()
    return manager


@pytest.fixture(autouse=True)
def _clear_memo():
    payloads_module.clear_memo()
    yield
    payloads_module.clear_memo()


class TestHelpers:
    def test_paths_and_keys(self):
        assert stats_path(2025, "são-paulo-grand-prix", "results.json") == (
            "2025/s%C3%A3o-paulo-grand-prix/results.json"
        )
        assert stats_path(2025, "", "events.json") == "2025/events.json"
        assert stats_cache_key(2025, "monaco-grand-prix", "results.json") == (
            "stats/2025/monaco-grand-prix/results.json"
        )
        assert stats_cache_key(2025, "", "events.json") == "stats/2025/events.json"

    def test_empty_detection(self):
        assert is_empty_payload(EMPTY)
        assert is_empty_payload({})
        assert is_empty_payload(None)
        assert not is_empty_payload(FULL)
        assert is_empty_payload({"MRData": {"total": "3", "RaceTable": {"Races": []}}})

    def test_current_season(self):
        assert is_current_season(payloads_module.current_season())
        assert not is_current_season(2000)


class TestStatsPayload:
    def test_memo_hit_skips_transport(self):
        transport = InMemoryTransport({"2024/events.json": FULL})
        loader = ErgastPayloadLoader(
            transport=transport, cdn_manager=_manager("A"), enable_cache=False
        )
        assert loader.stats_payload(2024, "", "events.json") == FULL
        assert loader.stats_payload(2024, "", "events.json") == FULL
        assert len(transport.calls) == 1

    def test_missing_everywhere_records_verdict(self):
        transport = InMemoryTransport({})
        loader = ErgastPayloadLoader(
            transport=transport, cdn_manager=_manager("A", "B"), enable_cache=False
        )
        assert loader.stats_payload(2024, "x", "results.json") is None
        assert loader.stats_payload(2024, "x", "results.json") is None
        assert len(transport.calls) == 2  # one walk, verdict served from memo afterwards

    def test_403_is_retried_once_and_never_persisted(self, monkeypatch):
        monkeypatch.setattr(payloads_module.time, "sleep", lambda _s: None)
        attempts: list[str] = []

        def refuse(url: str):
            attempts.append(url)
            raise NetworkError(url=url, status_code=403)

        transport = InMemoryTransport({"results.json": refuse})
        clock = [0.0]
        loader = ErgastPayloadLoader(
            transport=transport,
            cdn_manager=_manager("A", "B"),
            enable_cache=False,
            clock=lambda: clock[0],
        )
        assert loader.stats_payload(2024, "x", "results.json") is None
        assert len(attempts) == 4  # two mirrors, one retry
        assert loader.stats_payload(2024, "x", "results.json") is None
        assert len(attempts) == 4  # a transient miss is remembered briefly in-process
        clock[0] += 61
        assert loader.stats_payload(2024, "x", "results.json") is None
        assert len(attempts) == 8  # nothing persisted: probed again after the pause
        payloads_module.clear_memo()
        other = ErgastPayloadLoader(
            transport=transport, cdn_manager=_manager("A", "B"), enable_cache=False
        )
        assert other.stats_payload(2024, "x", "results.json") is None
        assert len(attempts) == 12  # another loader (process) starts from scratch

    def test_mixed_403_and_404_not_persisted(self, monkeypatch):
        monkeypatch.setattr(payloads_module.time, "sleep", lambda _s: None)
        calls: list[str] = []

        def answer(url: str):
            calls.append(url)
            if "a.example" in url:
                raise NetworkError(url=url, status_code=403)
            raise DataNotFoundError(url=url)

        transport = InMemoryTransport({"results.json": answer})
        clock = [0.0]
        loader = ErgastPayloadLoader(
            transport=transport,
            cdn_manager=_manager("A", "B"),
            enable_cache=False,
            clock=lambda: clock[0],
        )
        assert loader.stats_payload(2024, "x", "results.json") is None
        assert len(calls) == 4
        clock[0] += 61
        loader.stats_payload(2024, "x", "results.json")
        assert len(calls) == 8  # not persisted: probed again once the pause is over

    def test_sqlite_tier_round_trip_and_past_season_never_expires(self):
        from tif1.cache import get_cache

        transport = InMemoryTransport({"2000/events.json": FULL})
        clock = [1000.0]
        loader = ErgastPayloadLoader(
            transport=transport,
            cdn_manager=_manager("A"),
            enable_cache=True,
            clock=lambda: clock[0],
        )
        assert loader.stats_payload(2000, "", "events.json") == FULL
        entry = get_cache().get_entry("json", "stats/2000/events.json")
        assert entry["payload"] == FULL
        assert entry["fetched_at"] == 1000.0
        payloads_module.clear_memo()
        clock[0] += 10 * 365 * 86400
        assert loader.stats_payload(2000, "", "events.json") == FULL
        assert len(transport.calls) == 1

    def test_current_season_expires_after_ttl(self):
        from tif1.config import get_config

        year = payloads_module.current_season()
        transport = InMemoryTransport({f"{year}/events.json": FULL})
        clock = [1000.0]
        loader = ErgastPayloadLoader(
            transport=transport,
            cdn_manager=_manager("A"),
            enable_cache=False,
            clock=lambda: clock[0],
        )
        get_config().set("ergast_ttl_hours", 1.0)
        try:
            loader.stats_payload(year, "", "events.json")
            clock[0] += 1800
            loader.stats_payload(year, "", "events.json")
            assert len(transport.calls) == 1
            clock[0] += 1801
            loader.stats_payload(year, "", "events.json")
            assert len(transport.calls) == 2
        finally:
            get_config().set("ergast_ttl_hours", 6.0)

    def test_empty_past_season_file_is_reprobed_after_missing_ttl(self):
        from tif1.config import get_config

        transport = InMemoryTransport({"quali_results.json": EMPTY})
        clock = [0.0]
        loader = ErgastPayloadLoader(
            transport=transport,
            cdn_manager=_manager("A"),
            enable_cache=False,
            clock=lambda: clock[0],
        )
        get_config().set("missing_payloads_ttl_days", 1.0)
        try:
            loader.stats_payload(1950, "x", "quali_results.json")
            clock[0] += 3600
            loader.stats_payload(1950, "x", "quali_results.json")
            assert len(transport.calls) == 1
            clock[0] += 86400
            loader.stats_payload(1950, "x", "quali_results.json")
            assert len(transport.calls) == 2
        finally:
            get_config().set("missing_payloads_ttl_days", 7.0)

    def test_stale_copy_served_when_fetch_fails(self):
        state = {"fail": False}

        def answer(url: str):
            if state["fail"]:
                raise NetworkError(url=url, status_code=None)
            return FULL

        year = payloads_module.current_season()
        transport = InMemoryTransport({"events.json": answer})
        clock = [0.0]
        loader = ErgastPayloadLoader(
            transport=transport,
            cdn_manager=_manager("A"),
            enable_cache=False,
            clock=lambda: clock[0],
        )
        loader.stats_payload(year, "", "events.json")
        state["fail"] = True
        clock[0] += 10 * 3600
        assert loader.stats_payload(year, "", "events.json") == FULL

    def test_network_failure_without_cache_raises(self):
        def down(url: str):
            raise NetworkError(url=url, status_code=None)

        loader = ErgastPayloadLoader(
            transport=InMemoryTransport({"events.json": down}),
            cdn_manager=_manager("A"),
            enable_cache=False,
        )
        with pytest.raises(NetworkError):
            loader.stats_payload(2024, "", "events.json")

    def test_offline_mode(self):
        from tif1.config import get_config

        transport = InMemoryTransport({"2024/events.json": FULL})
        loader = ErgastPayloadLoader(
            transport=transport, cdn_manager=_manager("A"), enable_cache=False
        )
        get_config().set("offline_mode", True)
        try:
            with pytest.raises(NetworkError):
                loader.stats_payload(2024, "", "events.json")
        finally:
            get_config().set("offline_mode", False)
        loader.stats_payload(2024, "", "events.json")
        get_config().set("offline_mode", True)
        try:
            assert loader.stats_payload(2024, "", "events.json") == FULL
        finally:
            get_config().set("offline_mode", False)

    def test_stats_payloads_serves_memo_hits_without_threads(self, monkeypatch):
        transport = InMemoryTransport({"a/results.json": FULL, "b/results.json": FULL})
        loader = ErgastPayloadLoader(
            transport=transport, cdn_manager=_manager("A"), enable_cache=False
        )
        refs = [(2024, "a", "results.json"), (2024, "b", "results.json")]
        assert loader.stats_payloads(refs) == [FULL, FULL]
        calls = len(transport.calls)

        def no_pool(*_a, **_k):
            raise AssertionError("memo hits must not start a thread pool")

        monkeypatch.setattr("concurrent.futures.ThreadPoolExecutor", no_pool)
        assert loader.stats_payloads(refs) == [FULL, FULL]
        assert len(transport.calls) == calls

    def test_stats_payloads_parallel_reports_errors_per_ref(self):
        def down(url: str):
            raise NetworkError(url=url, status_code=None)

        transport = InMemoryTransport({"a/results.json": FULL, "b/results.json": down})
        loader = ErgastPayloadLoader(
            transport=transport, cdn_manager=_manager("A"), enable_cache=False
        )
        results = loader.stats_payloads(
            [(2024, "a", "results.json"), (2024, "b", "results.json"), (2024, "c", "results.json")]
        )
        assert results[0] == FULL
        assert isinstance(results[1], NetworkError)
        assert results[2] is None

    def test_jolpica_payload_ttl_and_stale(self):
        clock = [0.0]
        loader = ErgastPayloadLoader(
            transport=InMemoryTransport({}),
            cdn_manager=_manager("A"),
            enable_cache=False,
            clock=lambda: clock[0],
        )
        calls = {"n": 0}

        def fetch():
            calls["n"] += 1
            if calls["n"] == 3:
                raise NetworkError(url="x", status_code=None)
            return {"MRData": {"total": str(calls["n"])}}

        assert loader.jolpica_payload("x.json", fetch, ttl=100)["MRData"]["total"] == "1"
        assert loader.jolpica_payload("x.json", fetch, ttl=100)["MRData"]["total"] == "1"
        clock[0] += 101
        assert loader.jolpica_payload("x.json", fetch, ttl=100)["MRData"]["total"] == "2"
        clock[0] += 101
        assert (
            loader.jolpica_payload("x.json", fetch, ttl=100)["MRData"]["total"] == "2"
        )  # stale on failure


class TestCdnRepoLayout:
    def test_format_repo_url(self):
        js = CDNSource(name="jsDelivr", base_url="https://cdn.jsdelivr.net/gh/TracingInsights")
        sd = CDNSource(
            name="StaticDelivr", base_url="https://cdn.staticdelivr.com/gh/TracingInsights"
        )
        hf = CDNSource(
            name="HuggingFace", base_url="https://huggingface.co/buckets/tracinginsights"
        )
        repo, path = "TracingInsights-Archive/Stats", "2025/events.json"
        assert js.format_repo_url(repo, "main", path) == (
            "https://cdn.jsdelivr.net/gh/TracingInsights-Archive/Stats@main/2025/events.json"
        )
        assert sd.format_repo_url(repo, "main", path) == (
            "https://cdn.staticdelivr.com/gh/TracingInsights-Archive/Stats/main/2025/events.json"
        )
        assert hf.format_repo_url("TracingInsights/2025", "main", "a/b.json") == (
            "https://huggingface.co/buckets/tracinginsights/2025/resolve/a/b.json"
        )
        assert not hf.serves(repo)
        assert hf.serves("TracingInsights/2025")
        assert js.serves(repo)

    def test_try_sources_repo_skips_hf_and_reports_statuses(self):
        manager = CDNManager()
        manager.sources = [
            CDNSource(
                name="jsDelivr", base_url="https://cdn.jsdelivr.net/gh/TracingInsights", priority=1
            ),
            CDNSource(
                name="HuggingFace",
                base_url="https://huggingface.co/buckets/tracinginsights",
                priority=2,
            ),
            CDNSource(
                name="StaticDelivr",
                base_url="https://cdn.staticdelivr.com/gh/TracingInsights",
                priority=3,
            ),
        ]
        manager._failure_counts = {s.name: 0 for s in manager.sources}
        manager.reset()
        urls: list[str] = []

        def fetch(url: str):
            urls.append(url)
            if "jsdelivr" in url:
                raise NetworkError(url=url, status_code=403)
            raise DataNotFoundError(url=url)

        with pytest.raises(DataNotFoundError) as info:
            manager.try_sources_repo("TracingInsights-Archive/Stats", "main", "x.json", fetch)
        assert info.value.context["statuses"] == [403, 404]
        assert all("huggingface" not in url for url in urls)
        assert manager._failure_counts["jsDelivr"] == 0  # a 403 is not a health signal

    def test_try_sources_repo_network_error_marks_failure(self):
        manager = CDNManager()
        manager.sources = [CDNSource(name="A", base_url="https://a.example/gh/X", priority=1)]
        manager._failure_counts = {"A": 0}
        manager.reset()

        def fetch(url: str):
            raise NetworkError(url=url, status_code=None)

        with pytest.raises(NetworkError):
            manager.try_sources_repo("X/Y", "main", "x.json", fetch)
        assert manager._failure_counts["A"] == 1
