"""fetch_json_async in a browser runtime: the async transport replaces niquests and the pool."""

from __future__ import annotations

from urllib.parse import urlsplit

import pytest

from tif1 import async_fetch
from tif1.exceptions import DataNotFoundError, NetworkError
from tif1.payload_loader import InMemoryTransport, get_url_loader


class BytesTransport:
    """An async transport serving raw bytes (the ``PyodideTransport`` shape)."""

    def __init__(self, payloads: dict[str, object]) -> None:
        self.payloads = payloads
        self.calls: list[str] = []

    def get_json(self, url, *, timeout=None):  # noqa: ARG002 - transport interface
        raise AssertionError("sync path used")

    async def get_json_async(self, url, *, timeout=None):  # noqa: ARG002 - transport interface
        raise AssertionError("json path used")

    async def get_bytes_async(self, url, *, timeout=None):  # noqa: ARG002 - transport interface
        self.calls.append(url)
        for key, value in self.payloads.items():
            if key in url:
                if isinstance(value, BaseException):
                    raise value
                return value
        raise DataNotFoundError(url=url)


@pytest.fixture
def browser_runtime(monkeypatch):
    monkeypatch.setattr(async_fetch, "IS_EMSCRIPTEN", True)
    monkeypatch.setattr(async_fetch, "_import_niquests", lambda: pytest.fail("niquests imported"))
    monkeypatch.setattr(
        async_fetch, "_get_async_session", lambda: pytest.fail("HTTP session created")
    )
    monkeypatch.setattr(async_fetch, "_get_executor", lambda: pytest.fail("thread pool created"))


async def _fetch(path: str, **kwargs):
    kwargs.setdefault("use_cache", False)
    kwargs.setdefault("write_cache", False)
    kwargs.setdefault("validate_payload", False)
    return await async_fetch.fetch_json_async(2025, "Test%20GP", "Race", path, **kwargs)


async def test_json_transport_serves_the_payload(browser_runtime):
    transport = InMemoryTransport({"drivers.json": {"drivers": [{"driver": "VER"}]}})
    get_url_loader().transport = transport
    assert await _fetch("drivers.json") == {"drivers": [{"driver": "VER"}]}
    assert len(transport.calls) == 1
    assert urlsplit(transport.calls[0]).hostname == "cdn.jsdelivr.net"


async def test_bytes_transport_persists_the_raw_body(browser_runtime, monkeypatch):
    transport = BytesTransport({"weather.json": b'{"wT": [10.0]}'})
    get_url_loader().transport = transport
    writes: dict = {}

    class StubCache:
        def _get_from_memory(self, key):  # noqa: ARG002
            return None

        def get(self, key):  # noqa: ARG002
            return None

        def is_known_missing(self, key):  # noqa: ARG002
            return False

        def set_raw(self, key, blob):
            writes["raw"] = (key, blob)

        def set(self, key, data):
            writes["set"] = (key, data)

        def discard_missing(self, key):
            writes["discard"] = key

    monkeypatch.setattr(async_fetch, "get_cache", lambda: StubCache())
    data = await _fetch("weather.json", use_cache=True, write_cache=True)
    assert data == {"wT": [10.0]}
    assert writes["raw"] == ("2025/Test%20GP/Race/weather.json", b'{"wT": [10.0]}')
    assert "set" not in writes
    assert writes["discard"] == "2025/Test%20GP/Race/weather.json"


async def test_telemetry_bytes_take_the_typed_decoder(browser_runtime):
    transport = BytesTransport({"_tel.json": b'{"tel": {"Speed": [1, 2]}}'})
    get_url_loader().transport = transport
    data = await _fetch("VER/1_tel.json")
    assert list(data["tel"]["Speed"]) == [1, 2]


async def test_404_on_every_cdn_is_data_not_found_with_session_context(browser_runtime):
    transport = InMemoryTransport({})
    get_url_loader().transport = transport
    with pytest.raises(DataNotFoundError) as info:
        await _fetch("drivers.json")
    assert info.value.context["year"] == 2025
    assert len(transport.calls) == 3  # one request per CDN


async def test_http_errors_exhaust_the_cdns_as_network_error(browser_runtime):
    transport = InMemoryTransport({}, default=NetworkError(url="x", status_code=503))
    get_url_loader().transport = transport
    with pytest.raises(NetworkError):
        await _fetch("drivers.json", max_retries=1)
    assert len(transport.calls) >= 3


def test_async_transport_falls_back_to_the_shared_pyodide_transport(monkeypatch):
    class SyncOnly:
        def get_json(self, url, *, timeout=None):  # noqa: ARG002
            return {}

    get_url_loader().transport = SyncOnly()
    from tif1.browser import PyodideTransport

    assert isinstance(async_fetch._get_async_transport(), PyodideTransport)
    get_url_loader().transport = InMemoryTransport()
    assert isinstance(async_fetch._get_async_transport(), InMemoryTransport)
