"""Stdlib first wave of a cold load (Elis E2), offline: fake http.client connections."""

from __future__ import annotations

import asyncio
import gzip
import threading
import time
from types import SimpleNamespace
from typing import Any

import orjson
import pytest

from tif1 import async_fetch, first_wave, http_session, payload_loader
from tif1.cdn import get_cdn_manager

GP, SESSION = "Bahrain%20Grand%20Prix", "Race"


class _FakeResponse:
    def __init__(self, status: int, body: bytes, headers: dict[str, str], will_close: bool):
        self.status = status
        self._body = body
        self._headers = headers
        self.will_close = will_close

    def read(self) -> bytes:
        return self._body

    def getheader(self, name: str) -> str | None:
        return self._headers.get(name)


class _FakeServer:
    """Answers per request target; records every connection and request."""

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.connections: list[_FakeConnection] = []
        self.requests: list[tuple[str, dict[str, str]]] = []
        self.lock = threading.Lock()

    def factory(self, host: str, port: int, *, timeout: float, context: Any) -> _FakeConnection:
        assert (host, port) == ("cdn.jsdelivr.net", 443)
        assert timeout >= 1
        assert context == "tls-context"
        connection = _FakeConnection(self)
        with self.lock:
            self.connections.append(connection)
        return connection


class _FakeConnection:
    def __init__(self, server: _FakeServer) -> None:
        self.server = server
        self.sock = None
        self.closed = False
        self.target = ""
        self.fail_next = False

    def request(self, method: str, target: str, headers: dict[str, str]) -> None:
        assert method == "GET"
        if self.closed or self.fail_next:
            raise ConnectionResetError("closed by peer")
        self.target = target
        with self.server.lock:
            self.server.requests.append((target, dict(headers)))

    def getresponse(self) -> _FakeResponse:
        route = self.server.routes.get(self.target.rsplit("/", 1)[-1], (404, b"", {}))
        if isinstance(route, BaseException):
            raise route
        status, body, headers = route
        return _FakeResponse(status, body, headers, will_close=False)

    def close(self) -> None:
        self.closed = True


def _url(path: str) -> str:
    return get_cdn_manager().get_sources()[0].format_url(2025, GP, SESSION, path)


@pytest.fixture
def server(monkeypatch):
    """A fake CDN behind the first wave (TLS context stubbed, no proxy)."""
    first_wave.reset()
    fake = _FakeServer({})
    monkeypatch.setattr(first_wave, "_connection_factory", fake.factory)
    monkeypatch.setattr(first_wave, "_tls_context", lambda: "tls-context")
    yield fake
    first_wave.reset()


def _start(*paths: str) -> list[str]:
    urls = [_url(path) for path in paths]
    first_wave.start(urls, timeout=5)
    return urls


DRIVERS = {"drivers": [{"driver": "VER", "dn": "1"}]}
LAPTIMES = {"time": [90.1, 91.2], "lap": [1, 2]}


def test_bodies_are_handed_over_once_with_niquests_headers(server):
    server.routes["drivers.json"] = (200, orjson.dumps(DRIVERS), {})
    server.routes["session_laptimes.json"] = (
        200,
        gzip.compress(orjson.dumps(LAPTIMES)),
        {"Content-Encoding": "gzip"},
    )
    drivers_url, laptimes_url = _start("drivers.json", "session_laptimes.json")
    assert first_wave.pending(drivers_url)
    assert orjson.loads(first_wave.take(drivers_url) or b"") == DRIVERS
    assert orjson.loads(first_wave.take(laptimes_url) or b"") == LAPTIMES
    # Handed over once: a second request of the same URL goes to niquests.
    assert not first_wave.pending(drivers_url)
    assert first_wave.take(drivers_url) is None
    # One connection per file, or one reused when the first answer came back
    # before the second request left (instant on this fake).
    assert 1 <= len(server.connections) <= 2
    assert len(server.requests) == 2
    for target, headers in server.requests:
        assert target.startswith("/gh/")
        assert "Bahrain%20Grand%20Prix/Race/" in target
        assert headers["User-Agent"] == http_session.user_agent()
        assert headers["Accept-Encoding"] == "gzip, deflate"


@pytest.mark.parametrize(
    "route",
    [
        (404, b"", {}),
        (403, b"forbidden", {}),
        (301, b"", {"Location": "https://elsewhere/"}),
        (200, b"\x00\x01", {"Content-Encoding": "br"}),
        TimeoutError("read timed out"),
    ],
)
def test_anything_but_a_decodable_200_goes_to_niquests(server, route):
    server.routes["drivers.json"] = route
    (url,) = _start("drivers.json")
    assert first_wave.take(url) is None


def test_unknown_url_is_not_waited_for(server):
    server.routes["drivers.json"] = (200, b"{}", {})
    _start("drivers.json")
    started = time.monotonic()
    assert first_wave.take(_url("rcm.json")) is None
    assert time.monotonic() - started < 0.05


def test_take_is_bounded_by_the_wave_deadline(server, monkeypatch):
    release = threading.Event()

    def _stuck(*args: Any, **kwargs: Any) -> _FakeConnection:
        release.wait(5)
        raise ConnectionRefusedError("too late")

    monkeypatch.setattr(first_wave, "_connection_factory", _stuck)
    monkeypatch.setattr(first_wave, "WAIT_LIMIT", 0.05)
    (url,) = _start("drivers.json")
    started = time.monotonic()
    assert first_wave.take(url) is None
    assert time.monotonic() - started < 1.0
    release.set()


def test_proxy_or_missing_trust_store_falls_back_to_niquests_warm_up(server, monkeypatch):
    monkeypatch.setattr(first_wave, "_tls_context", lambda: None)
    warmed: list[list[str]] = []
    done = threading.Event()

    def _warm(urls: list[str], timeout: float) -> None:
        warmed.append(list(urls))
        done.set()

    monkeypatch.setattr(http_session, "warm_connections", _warm)
    urls = _start("drivers.json", "session_laptimes.json")
    assert first_wave.take(urls[0]) is None
    assert done.wait(2)
    assert warmed == [urls]
    assert server.connections == []
    assert not first_wave.has_client()


def test_tls_context_skips_the_wave_behind_a_proxy(monkeypatch):
    import urllib.request

    monkeypatch.setattr(urllib.request, "getproxies", lambda: {"https": "http://proxy:3128"})
    assert first_wave._tls_context() is None


def test_single_requests_reuse_the_wave_connections(server):
    server.routes["drivers.json"] = (200, orjson.dumps(DRIVERS), {})
    server.routes["36_tel.json"] = (200, b'{"tel": {}}', {})
    (url,) = _start("drivers.json")
    assert first_wave.take(url) is not None
    assert first_wave.has_client()
    tel_url = url.replace("drivers.json", "PIA/36_tel.json")
    assert first_wave.fetch(tel_url, timeout=5) == b'{"tel": {}}'
    assert len(server.connections) == 1  # the wave's kept-alive connection
    # A connection the server closed meanwhile is replaced once.
    server.connections[0].fail_next = True
    assert first_wave.fetch(tel_url, timeout=5) == b'{"tel": {}}'
    assert len(server.connections) == 2
    # Refusals and transport errors hand back None (the caller uses niquests).
    assert first_wave.fetch(url.replace("drivers.json", "missing.json"), timeout=5) is None
    server.routes["36_tel.json"] = OSError("unreachable")
    assert first_wave.fetch(tel_url, timeout=5) is None


def test_fetch_without_a_wave_is_none():
    first_wave.reset()
    assert first_wave.fetch(_url("drivers.json"), timeout=5) is None


def _forbid_niquests(monkeypatch) -> None:
    def _no_niquests() -> Any:
        raise AssertionError("niquests must not be needed")

    monkeypatch.setattr(async_fetch, "_import_niquests", _no_niquests)
    monkeypatch.setattr(payload_loader, "_import_niquests", _no_niquests)


def _fetch_async(path: str) -> dict[str, Any]:
    return asyncio.run(
        async_fetch.fetch_json_async(
            2025, GP, SESSION, path, use_cache=False, write_cache=False, validate_payload=False
        )
    )


def test_async_wave_consumes_the_first_wave_without_niquests(server, monkeypatch):
    server.routes["drivers.json"] = (200, orjson.dumps(DRIVERS), {})
    _start("drivers.json")
    _forbid_niquests(monkeypatch)
    assert _fetch_async("drivers.json") == DRIVERS


class _FakeNiquestsResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.status_code = 200
        self.content = orjson.dumps(payload)

    def raise_for_status(self) -> None:
        return None


class _FakeNiquestsSession:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def get(self, url: str, timeout: Any) -> _FakeNiquestsResponse:  # noqa: ARG002
        self.urls.append(url)
        return _FakeNiquestsResponse({"from": "niquests"})


_FAKE_NIQUESTS = SimpleNamespace(
    RequestException=type("RequestException", (Exception,), {}),
    exceptions=SimpleNamespace(HTTPError=type("HTTPError", (Exception,), {})),
)


def test_async_wave_refetches_a_refused_file_on_niquests(server, monkeypatch):
    server.routes["drivers.json"] = (403, b"", {})
    (url,) = _start("drivers.json")
    fake = _FakeNiquestsSession()
    monkeypatch.setattr(async_fetch, "_import_niquests", lambda: _FAKE_NIQUESTS)
    monkeypatch.setattr(async_fetch, "_get_async_session", lambda: fake)
    assert _fetch_async("drivers.json") == {"from": "niquests"}
    assert fake.urls == [url]


def test_sync_transport_uses_the_wave_until_a_niquests_session_exists(server, monkeypatch):
    server.routes["drivers.json"] = (200, orjson.dumps(DRIVERS), {})
    server.routes["36_tel.json"] = (200, b'{"tel": {"speed": [1]}}', {})
    (url,) = _start("drivers.json")
    tel_url = url.replace("drivers.json", "PIA/36_tel.json")
    monkeypatch.setattr(http_session, "_shared_session", None)
    _forbid_niquests(monkeypatch)
    transport = payload_loader.NiquestsTransport()
    assert transport.get_json(url) == DRIVERS
    assert transport.get_json(tel_url) == {"tel": {"speed": [1]}}
    # Once niquests is up (a telemetry wave ran), it serves every request.
    fake = _FakeNiquestsSession()
    monkeypatch.setattr(http_session, "_shared_session", fake)
    monkeypatch.setattr(payload_loader, "_import_niquests", lambda: _FAKE_NIQUESTS)
    assert transport.get_json(tel_url) == {"from": "niquests"}
    assert fake.urls == [tel_url]


TEL = {"tel": {"speed": [1.0, 2.0], "time": [0.0, 0.1]}}


@pytest.fixture
def warm_ups(monkeypatch) -> SimpleNamespace:
    """Record the niquests warm-ups the bridge starts (no niquests, no socket)."""
    record = SimpleNamespace(calls=[], called=threading.Event())

    def _warm(urls: list[str], timeout: float) -> None:
        assert timeout >= 1
        record.calls.append(list(urls))
        record.called.set()

    monkeypatch.setattr(http_session, "warm_connections", _warm)
    monkeypatch.setattr(http_session, "_shared_session", None)
    monkeypatch.setattr(http_session, "_warmups", [])
    return record


def test_telemetry_wave_starts_on_the_stdlib_while_niquests_warms(server, monkeypatch, warm_ups):
    server.routes["drivers.json"] = (200, orjson.dumps(DRIVERS), {})
    server.routes["1_tel.json"] = (
        200,
        gzip.compress(orjson.dumps(TEL)),
        {"Content-Encoding": "gzip"},
    )
    server.routes["2_tel.json"] = (200, orjson.dumps(TEL), {})
    (url,) = _start("drivers.json")
    assert first_wave.take(url) is not None
    _forbid_niquests(monkeypatch)
    for path in ("VER/1_tel.json", "VER/2_tel.json"):
        data = _fetch_async(path)
        assert list(data["tel"]["speed"]) == [1.0, 2.0]
        assert list(data["tel"]["time"]) == [0.0, 0.1]
    # niquests is warmed once, in a daemon thread, on the wave's URLs.
    assert warm_ups.called.wait(2)
    assert warm_ups.calls == [[url]]
    assert [target.rsplit("/", 1)[-1] for target, _ in server.requests] == [
        "drivers.json",
        "1_tel.json",
        "2_tel.json",
    ]
    assert len(server.connections) == 1  # all on the wave's kept-alive connection


def test_bridge_stays_open_until_the_niquests_warm_up_connected(server, monkeypatch, warm_ups):
    server.routes["drivers.json"] = (200, orjson.dumps(DRIVERS), {})
    (url,) = _start("drivers.json")
    assert first_wave.take(url) is not None
    connecting = threading.Event()
    monkeypatch.setattr(http_session, "_shared_session", _FakeNiquestsSession())
    monkeypatch.setattr(http_session, "_warmups", [connecting])
    assert first_wave.bridge_open()
    connecting.set()
    assert not first_wave.bridge_open()
    # The warm-up thread found the session built and did not warm again.
    time.sleep(0.05)
    assert warm_ups.calls == []


def test_telemetry_wave_on_niquests_once_it_is_warm(server, monkeypatch, warm_ups):
    server.routes["drivers.json"] = (200, orjson.dumps(DRIVERS), {})
    server.routes["1_tel.json"] = (200, orjson.dumps(TEL), {})
    (url,) = _start("drivers.json")
    assert first_wave.take(url) is not None
    fake = _FakeNiquestsSession()
    monkeypatch.setattr(http_session, "_shared_session", fake)
    monkeypatch.setattr(async_fetch, "_import_niquests", lambda: _FAKE_NIQUESTS)
    monkeypatch.setattr(async_fetch, "_get_async_session", lambda: fake)
    assert _fetch_async("VER/1_tel.json") == {"from": "niquests"}
    assert fake.urls == [url.replace("drivers.json", "VER/1_tel.json")]
    assert len(server.requests) == 1  # drivers.json only


def test_refused_bridge_request_goes_to_niquests(server, monkeypatch, warm_ups):
    server.routes["drivers.json"] = (200, orjson.dumps(DRIVERS), {})
    (url,) = _start("drivers.json")
    assert first_wave.take(url) is not None
    fake = _FakeNiquestsSession()
    monkeypatch.setattr(async_fetch, "_import_niquests", lambda: _FAKE_NIQUESTS)
    monkeypatch.setattr(async_fetch, "_get_async_session", lambda: fake)
    missing = url.replace("drivers.json", "VER/9_tel.json")  # 404 on the fake CDN
    assert _fetch_async("VER/9_tel.json") == {"from": "niquests"}
    assert fake.urls == [missing]
    assert server.requests[-1][0].endswith("/VER/9_tel.json")


def test_no_bridge_without_a_wave(monkeypatch, warm_ups):
    first_wave.reset()
    assert not first_wave.bridge_open()
    assert warm_ups.calls == []


def test_sync_transport_refused_single_request_goes_to_niquests(server, monkeypatch):
    server.routes["drivers.json"] = (200, orjson.dumps(DRIVERS), {})
    (url,) = _start("drivers.json")
    assert first_wave.take(url) is not None
    fake = _FakeNiquestsSession()

    def _session() -> _FakeNiquestsSession:
        return fake

    monkeypatch.setattr(http_session, "_shared_session", None)
    monkeypatch.setattr(http_session, "get_session", _session)
    monkeypatch.setattr(payload_loader, "_import_niquests", lambda: _FAKE_NIQUESTS)
    missing = url.replace("drivers.json", "results.json")  # 404 on the fake CDN
    assert payload_loader.NiquestsTransport().get_json(missing) == {"from": "niquests"}
    assert fake.urls == [missing]
