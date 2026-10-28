"""Shared pytest configuration and fixtures."""

import json
import socket
from pathlib import Path
from typing import Any

import pytest

# Markers whose tests are allowed to open real network connections. Everything
# else runs hermetically: a CDN stall must never hang the unit matrix.
_NETWORK_MARKERS = ("integration", "compliance_online", "benchmark")


_BLOCK_MESSAGE = (
    "network access is blocked in the unit suite; mark the test "
    "'integration' (or 'compliance_online') to allow real connections"
)


def _blocked_connect(self, *args, **kwargs):
    raise OSError(_BLOCK_MESSAGE)


def _blocked_getaddrinfo(*args, **kwargs):
    # Name resolution can stall long before connect() runs, so block it too.
    raise socket.gaierror(_BLOCK_MESSAGE)


@pytest.fixture(autouse=True)
def block_network(request, monkeypatch):
    """Fail real socket connections fast unless the test is marked for network.

    Mocked transports (``InMemoryTransport``) never touch sockets, so they are
    unaffected. Library code that reaches a real CDN gets an immediate
    ``OSError`` instead of waiting on a slow or stalled connection.
    """
    if any(request.node.get_closest_marker(m) for m in _NETWORK_MARKERS):
        yield
        return
    if "/integration/" in request.node.nodeid or request.node.nodeid.startswith(
        "tests/integration"
    ):
        yield
        return
    monkeypatch.setattr(socket.socket, "connect", _blocked_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", _blocked_connect)
    monkeypatch.setattr(socket.socket, "sendto", _blocked_connect)
    monkeypatch.setattr(socket, "getaddrinfo", _blocked_getaddrinfo)
    yield


@pytest.fixture(autouse=True)
def _config_leak_guard():
    """Fail the test that leaves the config singleton changed.

    Declared before ``isolate_cache_dir`` and without a ``monkeypatch``
    dependency, so it is set up first and torn down last, after every
    monkeypatch undo. A test that swaps ``Config._config`` or sets a value
    without restoring it would otherwise poison every later test of the
    same worker process (2026-10-28: ``ergast_results`` stayed off and the
    CI integration merge test failed with no warning).
    """
    from tif1 import config as config_module

    config = config_module.get_config()
    mapping = config._config
    before = dict(mapping)
    yield
    # tif1.core and core_utils.helpers bind the instance at import: a test
    # that leaves a fresh singleton behind (Config._instance = None) makes
    # every later get_config().set(...) invisible to them.
    assert config_module.Config._instance is config, (
        "the test replaced the Config singleton; reset it with "
        "monkeypatch.setattr(Config, '_instance', None) so the original comes back"
    )
    after = config._config
    assert after is mapping, (
        "the test replaced Config._config; patch one key with "
        "monkeypatch.setitem(get_config()._config, key, value) instead"
    )
    leaked = {
        key: (before.get(key), after.get(key))
        for key in set(before) | set(after)
        if before.get(key) != after.get(key)
    }
    assert not leaked, f"the test leaked config changes into the process: {leaked}"


@pytest.fixture(autouse=True)
def isolate_cache_dir(request, tmp_path_factory, monkeypatch):
    """Point every cache tier at a per-test temporary directory.

    Tests that build ``Cache()`` or ``tif1.get_cache()`` under a stub config
    (``cache_dir`` missing or None) used to write into ``./None``; once that
    fell back to the real default directory they could clear the developer's
    own ``~/.cache/tif1``. Nothing in the unit suite may touch it.
    """
    if request.node.get_closest_marker("real_cache_dir") is not None:
        yield
        return
    if "/integration/" in request.node.nodeid or request.node.nodeid.startswith(
        "tests/integration"
    ):
        yield
        return
    from tif1 import cache as cache_module
    from tif1 import config as config_module
    from tif1 import fastf1_compat

    cache_dir = tmp_path_factory.mktemp("tif1_cache")
    monkeypatch.setenv("TIF1_CACHE_DIR", str(cache_dir))
    # Argos A8: never spawn the background CDN pre-connect thread in tests.
    monkeypatch.setenv("TIF1_CDN_PRECONNECT", "false")
    monkeypatch.setattr(config_module, "_default_cache_dir", lambda: cache_dir)
    monkeypatch.setattr(cache_module, "_default_cache_dir", lambda: cache_dir, raising=False)
    monkeypatch.setattr(fastf1_compat, "_default_cache_dir", lambda: cache_dir, raising=False)
    config = config_module.get_config()
    # Item-level patches on the live config dictionary (never config.set with a
    # manual restore): monkeypatch undoes them last, after any test that
    # swapped the whole dictionary has had its own patch undone, so the
    # values can never leak into later tests of the same worker process.
    monkeypatch.setitem(config._config, "cache_dir", str(cache_dir))
    monkeypatch.setitem(config._config, "cdn_preconnect", False)
    # The Ergast classification merge reaches a CDN; the socket-blocked unit
    # suite keeps it off unless a test opts in (config.set("ergast_results", True)).
    monkeypatch.setitem(config._config, "ergast_results", False)
    monkeypatch.setattr(fastf1_compat.Cache, "_cache_dir", None)
    stale = cache_module._cache
    cache_module._cache = None
    try:
        yield
    finally:
        current = cache_module._cache
        if current is not None and current is not stale:
            try:
                current.close()
            except Exception:
                pass
        cache_module._cache = stale


@pytest.fixture(autouse=True)
def reset_global_state():
    """Reset global state before each test to prevent cross-test contamination."""
    from tif1 import async_fetch as async_fetch_module
    from tif1 import browser as browser_module
    from tif1 import payload_loader as payload_loader_module
    from tif1.cdn import get_cdn_manager
    from tif1.retry import reset_circuit_breaker

    def _reset() -> None:
        from tif1.ergast import jolpica as jolpica_module
        from tif1.ergast import payloads as ergast_payloads
        from tif1.ergast import stats as ergast_stats

        ergast_payloads.reset_ergast_loader()
        ergast_stats.reset_stats_source()
        jolpica_module.reset_rate_limiter()
        async_fetch_module.cleanup_resources()
        async_fetch_module._async_session = None
        get_cdn_manager().reset()
        reset_circuit_breaker()
        browser_module._reset_pyodide_transport()
        payload_loader_module._url_loader = None

    # Reset before test
    _reset()

    yield

    # Reset after test
    _reset()


# -- Ergast fixtures (tif1.ergast) --------------------------------------------

_ERGAST_FIXTURES = Path(__file__).parent / "fixtures" / "ergast"
#: Jolpica request URL fragment -> fixture file.
_JOLPICA_FIXTURES = {
    "api.jolpi.ca/ergast/f1/seasons.json?limit=5": "seasons_limit5.json",
    "api.jolpi.ca/ergast/f1/2024/drivers.json?limit=30": "drivers_2024.json",
    "api.jolpi.ca/ergast/f1/2024/status.json": "status_2024.json",
    "api.jolpi.ca/ergast/f1/2024/constructors.json": "constructors_2024.json",
    "api.jolpi.ca/ergast/f1/2024/circuits.json": "circuits_2024.json",
    "api.jolpi.ca/ergast/f1/current.json?limit=1": "current.json",
    "api.jolpi.ca/ergast/f1/2026/last.json?limit=1": "current_last.json",
    "api.jolpi.ca/ergast/f1/2024/1/laps.json?limit=100&offset=0": "laps_2024_1_page0.json",
}


def load_ergast_fixture_payloads() -> dict[str, Any]:
    """URL fragment -> parsed payload for every Stats and Jolpica fixture file."""
    payloads: dict[str, Any] = {}
    stats_dir = _ERGAST_FIXTURES / "stats"
    for path in stats_dir.rglob("*.json"):
        payloads[path.relative_to(stats_dir).as_posix()] = json.loads(
            path.read_text(encoding="utf-8")
        )
    for fragment, name in _JOLPICA_FIXTURES.items():
        payloads[fragment] = json.loads(
            (_ERGAST_FIXTURES / "jolpica" / name).read_text(encoding="utf-8")
        )
    return payloads


@pytest.fixture
def ergast_transport():
    """In-memory transport serving the Stats and Jolpica fixture files."""
    from tif1.payload_loader import InMemoryTransport

    return InMemoryTransport(load_ergast_fixture_payloads())


@pytest.fixture
def ergast_offline(monkeypatch, ergast_transport):
    """Route every Ergast fetch (Stats and Jolpica) through the fixture transport.

    Enables the Session results merge and the Jolpica fallback for the test
    (the fallback answers only the fixture URLs) and keeps the SQLite tier
    off so the process memo is the only cache.
    """
    from tif1.config import get_config
    from tif1.ergast import jolpica as jolpica_module
    from tif1.ergast import payloads as ergast_payloads
    from tif1.ergast import stats as ergast_stats

    ergast_payloads.reset_ergast_loader()
    ergast_stats.reset_stats_source()
    jolpica_module.reset_rate_limiter()
    monkeypatch.setattr(ergast_payloads, "default_transport", lambda: ergast_transport)
    monkeypatch.setattr(jolpica_module, "default_transport", lambda: ergast_transport)
    config = get_config()
    monkeypatch.setitem(config._config, "ergast_results", True)
    monkeypatch.setitem(config._config, "ergast_fallback", True)
    monkeypatch.setitem(config._config, "enable_cache", False)
    try:
        yield ergast_transport
    finally:
        ergast_payloads.reset_ergast_loader()
        ergast_stats.reset_stats_source()
