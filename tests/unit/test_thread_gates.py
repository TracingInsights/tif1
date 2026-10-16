"""Every background thread and pool stays off when the runtime has none (Pyodide)."""

from __future__ import annotations

import pytest

from tif1 import cache as cache_module
from tif1 import core, events, retry
from tif1.config import get_config
from tif1.exceptions import NetworkError


class TestCacheGates:
    def test_frame_writer_runs_inline(self, monkeypatch):
        monkeypatch.setattr(cache_module, "THREADS_AVAILABLE", False)
        ran: list[str] = []
        cache_module.register_frame_writer(lambda: ran.append("written"))
        assert ran == ["written"]
        assert cache_module._frame_writers == []

    def test_pools_report_a_single_worker(self, monkeypatch):
        monkeypatch.setattr(cache_module, "THREADS_AVAILABLE", False)
        assert cache_module._usable_cpu_count() == 1
        assert cache_module._decompress_workers() == 1
        assert cache_module._frame_compress_pool(8) is None
        assert cache_module._can_decompress_in_parallel(object()) is False

    def test_flush_commits_pending_writes(self, tmp_path):
        config = get_config()
        previous = config.get("cache_dir")
        config.set("cache_dir", str(tmp_path))
        try:
            cache = cache_module.Cache()
            try:
                cache.set("2025/GP/R/drivers.json", {"drivers": []})
                cache._pending_writes = 5
                cache.flush()
                assert cache._pending_writes == 0
            finally:
                cache.close()
        finally:
            config.set("cache_dir", previous)


class TestCoreGates:
    def test_preconnect_is_skipped(self, monkeypatch):
        monkeypatch.setattr(core, "THREADS_AVAILABLE", False)
        monkeypatch.setattr(core, "_preconnect_started", False)
        core._preconnect_cdn(None)  # never touches the session
        assert core._preconnect_started is False

    def test_background_telemetry_prefetch_is_skipped(self, monkeypatch):
        import pandas as pd

        monkeypatch.setattr(core, "THREADS_AVAILABLE", False)
        config = get_config()
        flags = (
            "prefetch_all_telemetry_on_first_lap_request",
            "prefetch_all_telemetry_after_laps_load",
        )
        previous = {flag: config.get(flag) for flag in flags}
        for flag in flags:
            config.set(flag, True)
        try:
            session = core.Session(2025, "Monaco Grand Prix", "Race", False, "pandas")
            session._laps = pd.DataFrame({"LapNumber": [1]})
            session._maybe_start_background_telemetry_prefetch()
            assert session._telemetry_background_prefetch_started is False
        finally:
            for flag, value in previous.items():
                config.set(flag, value)

    def test_session_table_prefetch_never_imports_niquests(self, monkeypatch):
        monkeypatch.setattr(core, "THREADS_AVAILABLE", False)
        session = core.Session(2025, "Monaco Grand Prix", "Race", False, "pandas")
        session._requested_session_tables.update({"weather.json", "rcm.json"})
        monkeypatch.setattr(session._payload_loader, "_cache_get", lambda _key: None)
        import builtins

        real_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name == "niquests":
                raise AssertionError("niquests imported without threads")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", guarded_import)
        session._prefetch_session_tables()
        assert session._session_tables_prefetched is True


class TestEventsGate:
    def test_schedule_refresh_runs_inline(self, monkeypatch):
        monkeypatch.setattr(events, "THREADS_AVAILABLE", False)
        fetched: list[int] = []

        def fake_fetch(year):
            fetched.append(year)
            return {"year": year}

        monkeypatch.setattr(events, "_fetch_f1schedule_year", fake_fetch)
        monkeypatch.setattr(events, "_schedule_generation", {})
        events._refresh_schedule_in_background(2099)
        assert fetched == [2099]
        assert events._schedule_generation[2099] == 1
        assert 2099 not in events._refresh_threads
        assert 2099 not in events._refreshing_years


class TestRetryGate:
    def test_backoff_skips_sleep_without_jspi(self, monkeypatch):
        monkeypatch.setattr(retry, "IS_EMSCRIPTEN", True)
        monkeypatch.setattr(retry, "can_block_on_awaitables", lambda: False)
        monkeypatch.setattr(retry.time, "sleep", lambda _s: pytest.fail("slept"))
        attempts: list[int] = []

        @retry.retry_with_backoff(max_retries=3, jitter=False, exceptions=(NetworkError,))
        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise NetworkError(url="https://cdn.example/x", status_code=None)
            return "ok"

        assert flaky() == "ok"
        assert len(attempts) == 3
