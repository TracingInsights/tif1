"""CDN pre-connect (Argos A8, Olympia H11): cold sessions only, started before pandas/polars.

Offline: the warm-up itself is replaced by a recorder, so no socket is opened.
"""

from __future__ import annotations

import threading
import time

import pytest

from tif1 import core, http_session
from tif1.cache import get_cache
from tif1.config import get_config


@pytest.fixture
def preconnect(monkeypatch):
    """Turn the pre-connect on for one test and record the warm-up URLs."""
    config = get_config()
    monkeypatch.setitem(config._config, "cdn_preconnect", True)
    monkeypatch.setattr(core, "_preconnect_started", False)
    monkeypatch.setattr(core, "THREADS_AVAILABLE", True)
    calls: list[list[str]] = []

    def _record(urls: list[str], timeout: float) -> None:
        assert timeout >= 1
        calls.append(list(urls))

    monkeypatch.setattr(http_session, "warm_connections", _record)
    return calls


def test_cold_session_warms_one_connection_per_first_wave_request(preconnect):
    session = core.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    assert len(preconnect) == 1
    urls = preconnect[0]
    assert [url.rsplit("/", 1)[-1] for url in urls] == list(core._FIRST_WAVE_PATHS)
    assert all("Bahrain%20Grand%20Prix/Race/" in url for url in urls)
    # The probe that decided "cold" is handed to the session, not run again.
    assert session._memo.has_session_data is False
    assert core._preconnect_started is True


def test_cached_session_never_warms(preconnect):
    get_cache().set("2025/Bahrain%20Grand%20Prix/Race/drivers.json", {"drivers": []})
    session = core.get_session(2025, "Bahrain Grand Prix", "Race", lib="pandas")
    assert preconnect == []
    assert session._memo.has_session_data is True
    assert core._preconnect_started is False


def test_cache_disabled_session_warms_without_probe(preconnect):
    session = core.get_session(2025, "Bahrain Grand Prix", "Race", enable_cache=False)
    assert len(preconnect) == 1
    assert session._memo.has_session_data is None


def test_one_warm_up_per_process(preconnect):
    core.get_session(2025, "Bahrain Grand Prix", "Race")
    second = core.get_session(2025, "Monaco Grand Prix", "Race")
    assert len(preconnect) == 1
    # The second session probes lazily, as before.
    assert second._memo.has_session_data is None


@pytest.mark.parametrize(
    ("session_name", "file_name"),
    [("Race", "results.json"), ("Qualifying", "quali_results.json")],
)
def test_cached_ergast_only_session_never_warms(preconnect, session_name, file_name):
    # Ergast-only seasons keep their payloads in the Stats tier, which the
    # timing-data probe does not see.
    get_cache().set(f"stats/2010/monaco-grand-prix/{file_name}", {"payload": {}})
    assert core._preconnect_cdn(2010, "Monaco Grand Prix", session_name, None) is False
    assert preconnect == []
    assert core._preconnect_started is False


def test_uncached_ergast_only_session_warms(preconnect):
    # Another session of the same round is cached, not this one's classification.
    get_cache().set("stats/2010/monaco-grand-prix/quali_results.json", {"payload": {}})
    assert core._preconnect_cdn(2010, "Monaco Grand Prix", "Race", None) is False
    assert len(preconnect) == 1


def test_has_json_entry_probes_every_tier():
    cache = get_cache()
    key = "stats/2010/monaco-grand-prix/results.json"
    assert cache.has_json_entry(key) is False
    cache.set(key, {"payload": {}})
    assert cache.has_json_entry(key) is True
    cache._memory_cache.clear()
    cache._parsed_cache.clear()
    assert cache.has_json_entry(key) is True  # SQLite row, nothing decoded
    assert cache.has_json_entry("stats/2010/monaco-grand-prix/laptimes.json") is False


def test_offline_mode_skips_everything(preconnect, monkeypatch):
    monkeypatch.setitem(get_config()._config, "offline_mode", True)
    assert core._preconnect_cdn(2025, "Bahrain Grand Prix", "Race", None) is None
    assert preconnect == []


class _FakeHttp:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.urls: list[str] = []

    def head(self, url: str, timeout: float) -> None:
        assert timeout == 1
        self.urls.append(url)
        if self.fail:
            raise OSError("unreachable")


@pytest.mark.parametrize("fail", [False, True])
def test_warm_connections_always_signal_done(monkeypatch, fail):
    fake = _FakeHttp(fail=fail)
    monkeypatch.setattr(http_session, "get_session", lambda: fake)
    monkeypatch.setattr(http_session, "_warmups", [])
    http_session.warm_connections(["https://a/x.json", "https://a/y.json"], timeout=1)
    started = time.monotonic()
    http_session.wait_for_warm_connections()
    assert time.monotonic() - started < 0.9
    assert sorted(fake.urls) == ["https://a/x.json", "https://a/y.json"]
    assert http_session._warmups == []


def test_wait_for_warm_connections_is_bounded(monkeypatch):
    stuck = threading.Event()
    monkeypatch.setattr(http_session, "_warmups", [stuck])
    # The bound runs from the warm-up's start, not from the request.
    monkeypatch.setattr(http_session, "_warmup_deadline", time.monotonic() + 0.05)
    started = time.monotonic()
    http_session.wait_for_warm_connections()
    assert 0.03 <= time.monotonic() - started < 1.0
    # A warm-up that missed its window is never waited for again.
    assert http_session._warmups == []
    started = time.monotonic()
    http_session.wait_for_warm_connections()
    assert time.monotonic() - started < 0.01
