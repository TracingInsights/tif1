"""Unit tests for the payload loading pipeline (tif1.payload_loader)."""

import asyncio
from typing import Any

import pytest

from tif1.cache import SessionMemo
from tif1.cdn import CDNManager
from tif1.exceptions import DataNotFoundError, InvalidDataError, NetworkError
from tif1.payload_loader import (
    InMemoryTransport,
    NiquestsTransport,
    PayloadLoader,
    get_url_loader,
)


def _make_loader(**overrides: Any) -> PayloadLoader:
    """Build a PayloadLoader with an in-memory transport and hermetic storage."""
    kwargs: dict[str, Any] = {
        "year": 2025,
        "gp": "Test GP",
        "session": "Race",
        "transport": InMemoryTransport({"test.json": {"data": "value"}}),
        "memo": SessionMemo(),
        "enable_cache": False,
    }
    kwargs.update(overrides)
    return PayloadLoader(**kwargs)


class TestInMemoryTransport:
    """Test the in-memory HTTP transport test adapter."""

    def test_maps_payloads_by_url_fragment(self):
        transport = InMemoryTransport({"test.json": {"data": 1}})
        assert transport.get_json("https://cdn.example/test.json") == {"data": 1}
        assert transport.calls == ["https://cdn.example/test.json"]

    def test_longest_fragment_wins(self):
        transport = InMemoryTransport({"laptimes.json": {"a": 1}, "VER/laptimes.json": {"b": 2}})
        assert transport.get_json("https://cdn.example/VER/laptimes.json") == {"b": 2}

    def test_raises_configured_exception(self):
        transport = InMemoryTransport({"test.json": InvalidDataError(reason="bad")})
        with pytest.raises(InvalidDataError, match="bad"):
            transport.get_json("https://cdn.example/test.json")

    def test_callable_values_are_invoked(self):
        transport = InMemoryTransport({"test.json": lambda url: {"url": url}})
        assert transport.get_json("https://cdn.example/x/test.json") == {
            "url": "https://cdn.example/x/test.json"
        }

    def test_missing_mapping_raises_not_found(self):
        transport = InMemoryTransport()
        with pytest.raises(DataNotFoundError):
            transport.get_json("https://cdn.example/missing.json")


class TestPayloadLoaderGet:
    """Test PayloadLoader.get memo/cache/fetch behavior."""

    def test_fetches_through_transport_and_memoizes(self):
        # "VER/laptimes.json" contains "/", so the SessionMemo retains it.
        loader = _make_loader(
            transport=InMemoryTransport({"VER/laptimes.json": {"data": "value"}})
        )
        first = loader.get("VER/laptimes.json")
        assert first == {"data": "value"}
        # Second call must be served from the payload memo (no new HTTP hit).
        assert loader.get("VER/laptimes.json") == {"data": "value"}
        assert len(loader.transport.calls) == 1

    def test_non_dict_payload_raises_invalid_data(self):
        loader = _make_loader(transport=InMemoryTransport({"list.json": [1, 2, 3]}))
        with pytest.raises(InvalidDataError, match="Expected dict"):
            loader.get("list.json")

    def test_none_payload_raises_invalid_data(self):
        loader = _make_loader(transport=InMemoryTransport({"none.json": None}))
        with pytest.raises(InvalidDataError, match="Expected dict"):
            loader.get("none.json")

    def test_invalid_payload_raises_even_with_callable_patch(self, monkeypatch):
        """There is no mock/patch escape hatch: validation failures always raise."""
        loader = _make_loader(transport=InMemoryTransport({"test.json": [1, 2, 3]}))

        def always_list(_path: str, *, fast: bool) -> list[int]:
            _ = fast
            return [1, 2, 3]

        # Even monkeypatching the fetch layer cannot bypass validation.
        monkeypatch.setattr(loader, "_fetch", always_list)
        with pytest.raises(InvalidDataError, match="Expected dict"):
            loader.get("test.json")

    def test_cache_hit_skips_transport(self):
        loader = _make_loader(
            cache_get=lambda _key: {"cached": True},
            transport=InMemoryTransport(),
        )
        assert loader.get("test.json") == {"cached": True}
        assert loader.transport.calls == []

    def test_cache_write_back_on_fetch(self):
        written: dict[str, Any] = {}
        loader = _make_loader(cache_set=lambda key, value: written.setdefault(key, value))
        loader.get("test.json")
        cache_key = loader.cache_key("test.json")
        assert written[cache_key] == {"data": "value"}

    def test_use_cache_false_bypasses_storage(self):
        loader = _make_loader(
            cache_get=lambda _key: {"cached": True},
            cache_set=lambda _key, _value: pytest.fail("must not write cache"),
        )
        assert loader.get("test.json", use_cache=False, write_cache=False) == {"data": "value"}

    def test_validate_false_skips_schema_but_keeps_dict_coercion(self):
        loader = _make_loader(
            transport=InMemoryTransport({"test.json": {"unexpected": "schema-free"}})
        )
        assert loader.get("test.json", validate=False) == {"unexpected": "schema-free"}


class TestPayloadLoaderFetchFromCdn:
    """Test direct CDN fetching and fallback between sources."""

    def test_fallback_to_second_source(self):
        payloads = {
            "staticdelivr.com": NetworkError(url="x", status_code=None),
            "jsdelivr.net": {"ok": True},
        }
        loader = _make_loader(transport=InMemoryTransport(payloads))
        assert loader.fetch_from_cdn("test.json", fast=True) == {"ok": True}
        assert len(loader.transport.calls) == 2

    def test_get_fetches_and_validates(self):
        loader = _make_loader()
        assert loader.get("test.json", use_cache=False) == {"data": "value"}

    def test_all_sources_down_raises_network_error(self):
        loader = _make_loader(
            transport=InMemoryTransport(
                {"test.json": NetworkError(url="x", status_code=None)}
            )
        )
        with pytest.raises(NetworkError, match="Network request failed"):
            loader.fetch_from_cdn("test.json", fast=True)

    def test_404_raises_data_not_found(self):
        loader = _make_loader(transport=InMemoryTransport())
        with pytest.raises(DataNotFoundError, match="Data not found"):
            loader.fetch_from_cdn("missing.json", fast=True)


class TestPayloadLoaderGetMany:
    """Test the async multi-fetch surface."""

    def test_get_many_delegates_to_async_fetch(self, monkeypatch):
        loader = _make_loader()
        payloads = [
            {"time": [90.5], "lap": [1]},
            {"time": [91.0], "lap": [1]},
        ]
        seen: dict[str, Any] = {}

        async def fake_fetch_multiple(requests, **kwargs):
            seen["requests"] = requests
            seen["kwargs"] = kwargs
            return payloads

        monkeypatch.setattr(
            "tif1.async_fetch.fetch_multiple_async", fake_fetch_multiple
        )
        results = asyncio.run(
            loader.get_many(
                ["VER/laptimes.json", "HAM/laptimes.json"],
                max_retries=1,
                use_cache=False,
            )
        )
        assert results == payloads
        assert seen["requests"] == [
            (2025, "Test GP", "Race", "VER/laptimes.json"),
            (2025, "Test GP", "Race", "HAM/laptimes.json"),
        ]
        assert seen["kwargs"]["max_retries"] == 1
        assert seen["kwargs"]["use_cache"] is False

    def test_get_many_forwards_failures_as_none(self, monkeypatch):
        loader = _make_loader()

        async def fake_fetch_multiple(requests, **_kwargs):
            return [None for _ in requests]

        monkeypatch.setattr(
            "tif1.async_fetch.fetch_multiple_async", fake_fetch_multiple
        )
        results = asyncio.run(loader.get_many(["bad.json"], max_retries=1))
        assert results == [None]


class TestUrlLoader:
    """Test the process-global URL loader used by events.py."""

    def test_singleton(self):
        assert get_url_loader() is get_url_loader()

    def test_get_url_uses_transport(self):
        loader = get_url_loader()
        original = loader.transport
        loader.transport = InMemoryTransport({"example": {"data": 1}})
        try:
            assert loader.get_url("https://example.com/example") == {"data": 1}
        finally:
            loader.transport = original

    def test_get_url_maps_errors(self):
        loader = get_url_loader()
        original = loader.transport
        loader.transport = InMemoryTransport()
        try:
            with pytest.raises(DataNotFoundError):
                loader.get_url("https://example.com/missing")
        finally:
            loader.transport = original


class TestTrySourcesAsync:
    """Test the async CDN source fallback loop."""

    async def test_uses_first_working_source(self):
        manager = CDNManager()
        calls: list[tuple[str, str]] = []

        async def fetch_func(source, url):
            calls.append((source.name, url))
            if "staticdelivr" in url:
                raise NetworkError(url=url, status_code=None)
            return {"ok": True}

        result = await manager.try_sources_async(2024, "Test", "Race", "test.json", fetch_func)
        assert result == {"ok": True}
        assert len(calls) == 2

    async def test_raises_after_all_sources_fail(self):
        manager = CDNManager()

        async def fetch_func(source, url):
            raise NetworkError(url=url, status_code=None)

        with pytest.raises(NetworkError, match="Network request failed"):
            await manager.try_sources_async(2024, "Test", "Race", "test.json", fetch_func)

    async def test_data_not_found_is_not_retried(self):
        manager = CDNManager()
        calls = 0

        async def fetch_func(source, url):
            nonlocal calls
            calls += 1
            raise DataNotFoundError(url=url)

        with pytest.raises(DataNotFoundError):
            await manager.try_sources_async(2024, "Test", "Race", "test.json", fetch_func)
        assert calls == 1

    async def test_invalid_data_is_not_retried(self):
        manager = CDNManager()
        calls = 0

        async def fetch_func(source, url):
            nonlocal calls
            calls += 1
            raise InvalidDataError(reason="bad json")

        with pytest.raises(InvalidDataError):
            await manager.try_sources_async(2024, "Test", "Race", "test.json", fetch_func)
        assert calls == 1


class TestNiquestsTransport:
    """Smoke-test the default transport's error mapping."""

    def test_404_maps_to_data_not_found(self, monkeypatch):
        class FakeResponse:
            status_code = 404

            def raise_for_status(self):
                pass

        monkeypatch.setattr(
            "tif1.http_session.get_session",
            lambda: type("S", (), {"get": staticmethod(lambda *_a, **_k: FakeResponse())})(),
        )
        with pytest.raises(DataNotFoundError):
            NiquestsTransport().get_json("https://example.com/x.json")
