"""tif1.browser: the Pyodide transport and cache persistence, against fake runtime modules."""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from tif1 import browser
from tif1 import cache as cache_module
from tif1.cache import SessionMemo
from tif1.config import get_config
from tif1.exceptions import DataNotFoundError, InvalidDataError, NetworkError, TIF1Error
from tif1.payload_loader import PayloadLoader


class FakeResponse:
    def __init__(self, status: int, data: bytes = b"{}") -> None:
        self.status = status
        self.data = data


def install_fake_urllib3(monkeypatch, handler):
    """A ``urllib3`` whose PoolManager answers through ``handler(url)``."""
    module = types.ModuleType("urllib3")
    exceptions = types.ModuleType("urllib3.exceptions")

    class HTTPError(Exception):
        pass

    exceptions.HTTPError = HTTPError
    module.exceptions = exceptions
    state: dict = {"pool_kwargs": None, "requests": []}

    class PoolManager:
        def __init__(self, **kwargs):
            state["pool_kwargs"] = kwargs

        def request(self, method, url, **kwargs):
            state["requests"].append((method, url, kwargs))
            result = handler(url)
            if isinstance(result, BaseException):
                raise result
            return result

    module.PoolManager = PoolManager
    monkeypatch.setitem(sys.modules, "urllib3", module)
    monkeypatch.setitem(sys.modules, "urllib3.exceptions", exceptions)
    state["HTTPError"] = HTTPError
    return state


class FakeFetchResponse:
    def __init__(self, status: int, body: bytes = b"{}") -> None:
        self.status = status
        self._body = body

    async def bytes(self) -> bytes:
        return self._body


def install_fake_pyodide(monkeypatch, handler):
    """Fake ``pyodide.http.pyfetch`` (+ ``pyodide.ffi``/``js``) answering through ``handler(url)``."""
    http = types.ModuleType("pyodide.http")
    ffi = types.ModuleType("pyodide.ffi")

    class JsException(Exception):  # noqa: N818 - mirrors pyodide.ffi.JsException
        pass

    class AbortError(Exception):
        pass

    ffi.JsException = JsException
    http.AbortError = AbortError
    calls: list = []

    async def pyfetch(url, **kwargs):
        calls.append((url, kwargs))
        result = handler(url)
        if isinstance(result, BaseException):
            raise result
        return result

    http.pyfetch = pyfetch
    package = types.ModuleType("pyodide")
    package.http = http
    package.ffi = ffi
    js = types.ModuleType("js")

    class AbortSignal:
        @staticmethod
        def timeout(ms):
            return ("signal", ms)

    js.AbortSignal = AbortSignal
    for name, mod in (
        ("pyodide", package),
        ("pyodide.http", http),
        ("pyodide.ffi", ffi),
        ("js", js),
    ):
        monkeypatch.setitem(sys.modules, name, mod)
    return calls, JsException, AbortError


class TestSyncTransport:
    def test_serves_json_with_pipeline_owned_retries(self, monkeypatch):
        state = install_fake_urllib3(
            monkeypatch, lambda _url: FakeResponse(200, b'{"drivers": []}')
        )
        transport = browser.PyodideTransport()
        assert transport.get_json("https://cdn.example/drivers.json") == {"drivers": []}
        assert state["pool_kwargs"] == {"retries": False}
        method, url, kwargs = state["requests"][0]
        assert (method, url) == ("GET", "https://cdn.example/drivers.json")
        assert kwargs["preload_content"] is True
        assert kwargs["headers"] == {"Accept": "application/json"}
        assert kwargs["timeout"] == float(get_config().get("timeout", 30))

    def test_explicit_timeout_is_at_least_one_second(self, monkeypatch):
        state = install_fake_urllib3(monkeypatch, lambda _url: FakeResponse(200))
        browser.PyodideTransport().get_json("https://cdn.example/x.json", timeout=0.1)
        assert state["requests"][0][2]["timeout"] == 1.0

    def test_404_is_data_not_found(self, monkeypatch):
        install_fake_urllib3(monkeypatch, lambda _url: FakeResponse(404))
        with pytest.raises(DataNotFoundError) as info:
            browser.PyodideTransport().get_json("https://cdn.example/missing.json")
        assert info.value.context["url"] == "https://cdn.example/missing.json"

    def test_http_error_status_is_kept_for_the_cdn_manager(self, monkeypatch):
        install_fake_urllib3(monkeypatch, lambda _url: FakeResponse(403))
        with pytest.raises(NetworkError) as info:
            browser.PyodideTransport().get_json("https://cdn.example/x.json")
        assert info.value.context["status_code"] == 403

    def test_transport_failures_are_network_errors(self, monkeypatch):
        state = install_fake_urllib3(monkeypatch, lambda _url: state["HTTPError"]("boom"))
        with pytest.raises(NetworkError) as info:
            browser.PyodideTransport().get_json("https://cdn.example/x.json")
        assert info.value.context["status_code"] is None
        install_fake_urllib3(monkeypatch, lambda _url: OSError("offline"))
        with pytest.raises(NetworkError):
            browser.PyodideTransport().get_json("https://cdn.example/x.json")

    @pytest.mark.parametrize("body", [b"[1, 2]", b"{not json"])
    def test_bad_bodies_are_invalid_data(self, monkeypatch, body):
        install_fake_urllib3(monkeypatch, lambda _url: FakeResponse(200, body))
        with pytest.raises(InvalidDataError):
            browser.PyodideTransport().get_json("https://cdn.example/x.json")

    def test_payload_loader_falls_through_cdns_on_404(self, monkeypatch):
        def handler(url):
            if "jsdelivr" in url:
                return FakeResponse(404)
            return FakeResponse(200, b'{"drivers": [{"driver": "VER"}]}')

        state = install_fake_urllib3(monkeypatch, handler)
        loader = PayloadLoader(
            2025,
            "Test%20GP",
            "Race",
            transport=browser.PyodideTransport(),
            memo=SessionMemo(),
            enable_cache=False,
        )
        assert loader.fetch_from_cdn("drivers.json", fast=True) == {"drivers": [{"driver": "VER"}]}
        assert len(state["requests"]) == 2


class TestAsyncTransport:
    async def test_fetches_with_an_abort_signal(self, monkeypatch):
        calls, _, _ = install_fake_pyodide(
            monkeypatch, lambda _url: FakeFetchResponse(200, b'{"wT": [10.0]}')
        )
        transport = browser.PyodideTransport()
        assert await transport.get_json_async("https://cdn.example/w.json", timeout=5) == {
            "wT": [10.0]
        }
        _url, kwargs = calls[0]
        assert kwargs["method"] == "GET"
        assert kwargs["headers"] == {"Accept": "application/json"}
        assert kwargs["signal"] == ("signal", 5000)

    async def test_bytes_are_returned_raw(self, monkeypatch):
        install_fake_pyodide(monkeypatch, lambda _url: FakeFetchResponse(200, b'{"a": 1}'))
        assert (
            await browser.PyodideTransport().get_bytes_async("https://cdn.example/a") == b'{"a": 1}'
        )

    async def test_status_mapping(self, monkeypatch):
        install_fake_pyodide(monkeypatch, lambda _url: FakeFetchResponse(404))
        with pytest.raises(DataNotFoundError):
            await browser.PyodideTransport().get_bytes_async("https://cdn.example/a")
        install_fake_pyodide(monkeypatch, lambda _url: FakeFetchResponse(500))
        with pytest.raises(NetworkError) as info:
            await browser.PyodideTransport().get_bytes_async("https://cdn.example/a")
        assert info.value.context["status_code"] == 500

    async def test_js_failures_are_network_errors(self, monkeypatch):
        holder: dict = {}

        def handler(_url):
            return holder["error"]("fetch failed")

        _, js_exception, abort_error = install_fake_pyodide(monkeypatch, handler)
        for error in (js_exception, abort_error, OSError):
            holder["error"] = error
            with pytest.raises(NetworkError):
                await browser.PyodideTransport().get_bytes_async("https://cdn.example/a")

    async def test_non_object_json_is_invalid(self, monkeypatch):
        install_fake_pyodide(monkeypatch, lambda _url: FakeFetchResponse(200, b"[]"))
        with pytest.raises(InvalidDataError):
            await browser.PyodideTransport().get_json_async("https://cdn.example/a")

    async def test_without_abort_signal_support(self, monkeypatch):
        calls, _, _ = install_fake_pyodide(monkeypatch, lambda _url: FakeFetchResponse(200))
        monkeypatch.setitem(sys.modules, "js", types.ModuleType("js"))
        await browser.PyodideTransport().get_json_async("https://cdn.example/a")
        assert "signal" not in calls[0][1]


def test_shared_transport_singleton():
    first = browser.get_pyodide_transport()
    assert browser.get_pyodide_transport() is first
    browser._reset_pyodide_transport()
    assert browser.get_pyodide_transport() is not first


class FakeFS:
    def __init__(self, error=None) -> None:
        self.calls: list = []
        self.filesystems = types.SimpleNamespace(IDBFS="IDBFS")
        self.error = error

    def mkdirTree(self, path):  # noqa: N802 - Emscripten API name
        self.calls.append(("mkdirTree", path))

    def mount(self, filesystem, options, path):  # noqa: ARG002 - Emscripten API shape
        self.calls.append(("mount", filesystem, path))

    def syncfs(self, populate, callback):
        self.calls.append(("syncfs", populate))
        callback(self.error)


def install_fake_fs(monkeypatch, fs: FakeFS) -> None:
    pyodide_js = types.ModuleType("pyodide_js")
    pyodide_js.FS = fs
    ffi = types.ModuleType("pyodide.ffi")
    ffi.create_once_callable = lambda fn: fn
    package = types.ModuleType("pyodide")
    package.ffi = ffi
    js = types.ModuleType("js")
    js.Object = types.SimpleNamespace(new=dict)
    for name, mod in (
        ("pyodide_js", pyodide_js),
        ("pyodide", package),
        ("pyodide.ffi", ffi),
        ("js", js),
    ):
        monkeypatch.setitem(sys.modules, name, mod)
    monkeypatch.setattr(browser, "is_pyodide", lambda: True)


class TestPersistence:
    async def test_enable_persistent_cache_mounts_and_loads(self, monkeypatch, tmp_path: Path):
        fs = FakeFS()
        install_fake_fs(monkeypatch, fs)
        target = tmp_path / "idb-cache"
        assert await browser.enable_persistent_cache(target) == target
        assert fs.calls == [
            ("mkdirTree", str(target)),
            ("mount", "IDBFS", str(target)),
            ("syncfs", True),
        ]
        assert get_config().get("cache_dir") == str(target)

    async def test_enable_persistent_cache_defaults_to_the_configured_dir(self, monkeypatch):
        fs = FakeFS()
        install_fake_fs(monkeypatch, fs)
        configured = Path(str(get_config().get("cache_dir"))).expanduser()
        assert await browser.enable_persistent_cache() == configured
        assert fs.calls[0] == ("mkdirTree", str(configured))

    async def test_persist_cache_flushes_then_syncs(self, monkeypatch):
        fs = FakeFS()
        install_fake_fs(monkeypatch, fs)
        flushed: list[str] = []
        stub = types.SimpleNamespace(flush=lambda: flushed.append("flush"))
        monkeypatch.setattr(cache_module, "_cache", stub)
        await browser.persist_cache()
        assert flushed == ["flush"]
        assert fs.calls == [("syncfs", False)]

    async def test_sync_errors_surface(self, monkeypatch):
        fs = FakeFS(error="QuotaExceeded")
        install_fake_fs(monkeypatch, fs)
        monkeypatch.setattr(cache_module, "_cache", None)
        with pytest.raises(TIF1Error, match="QuotaExceeded"):
            await browser.persist_cache()

    async def test_outside_pyodide(self):
        with pytest.raises(TIF1Error, match="only available inside Pyodide"):
            await browser.enable_persistent_cache()
        with pytest.raises(TIF1Error, match="only available inside Pyodide"):
            await browser.persist_cache()
