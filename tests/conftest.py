"""Shared pytest configuration and fixtures."""

import socket

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
    previous = config.get("cache_dir")
    config.set("cache_dir", str(cache_dir))
    previous_preconnect = config.get("cdn_preconnect")
    config.set("cdn_preconnect", False)
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
        config.set("cache_dir", previous)
        config.set("cdn_preconnect", previous_preconnect)


@pytest.fixture(autouse=True)
def reset_global_state():
    """Reset global state before each test to prevent cross-test contamination."""
    from tif1 import async_fetch as async_fetch_module
    from tif1.cdn import get_cdn_manager
    from tif1.retry import reset_circuit_breaker

    # Reset before test
    async_fetch_module.cleanup_resources()
    async_fetch_module._async_session = None
    get_cdn_manager().reset()
    reset_circuit_breaker()

    yield

    # Reset after test
    async_fetch_module.cleanup_resources()
    async_fetch_module._async_session = None
    get_cdn_manager().reset()
    reset_circuit_breaker()
