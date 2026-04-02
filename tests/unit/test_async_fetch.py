"""Tests for async_fetch module."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import tif1.async_fetch as async_fetch_module
import tif1.http_session as http_mod
from tif1.async_fetch import fetch_json_async, fetch_multiple_async
from tif1.exceptions import DataNotFoundError, NetworkError
from tif1.http_session import _create_session
from tif1.http_session import close_session as close_http_session
from tif1.http_session import get_session as get_http_session_fn


@pytest.fixture(autouse=True)
def reset_async_fetch_state():
    from tif1.cdn import get_cdn_manager
    from tif1.retry import reset_circuit_breaker

    async_fetch_module.cleanup_resources()
    async_fetch_module._async_session = None
    get_cdn_manager().reset()
    reset_circuit_breaker()
    yield
    async_fetch_module.cleanup_resources()
    async_fetch_module._async_session = None
    get_cdn_manager().reset()
    reset_circuit_breaker()


class StubResponse:
    """Simple HTTP response stub for thread-safe async fetch tests."""

    def __init__(self, status_code: int = 200, payload=None, error: Exception | None = None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self._error = error

    def raise_for_status(self):
        if self._error is not None:
            raise self._error

    def json(self):
        return self._payload


class StubCache:
    """Thread-safe cache stub."""

    def __init__(self, preset=None):
        self._preset = preset
        self._store: dict[str, dict] = {}

    def get(self, key: str):
        if self._preset is not None:
            return self._preset
        return self._store.get(key)

    def set(self, key: str, value: dict):
        self._store[key] = value


class StubSession:
    """Thread-safe session stub."""

    def __init__(self, response: StubResponse | None = None, error: Exception | None = None):
        self._response = response
        self._error = error

    def get(self, *_args, **_kwargs):
        if self._error is not None:
            raise self._error
        return self._response


@pytest.mark.asyncio
class TestAsyncFetch:
    """Test async fetch functionality."""

    async def test_fetch_json_async_offline_mode_cache_miss_raises(self):
        """Offline mode should never perform network fetches on cache miss."""
        mock_cache = StubCache()

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.config.get_config") as mock_config,
        ):
            mock_config.return_value.get.side_effect = lambda key, default=None: {
                "offline_mode": True,
                "ci_mode": False,
            }.get(key, default)

            with pytest.raises(NetworkError):
                await fetch_json_async(2025, "Test%20GP", "Race", "drivers.json")

    async def test_fetch_json_async_ci_mode_skips_parsed_cache(self):
        """CI mode should bypass parsed cache reads and writes."""
        mock_response = StubResponse(status_code=200, payload={"drivers": [{"driver": "VER"}]})
        mock_cache = StubCache(preset={"drivers": [{"driver": "HAM"}]})
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
            patch("tif1.config.get_config") as mock_config,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests
            mock_config.return_value.get.side_effect = lambda key, default=None: {
                "offline_mode": False,
                "ci_mode": True,
                "max_retries": 1,
                "timeout": 30,
                "retry_backoff_factor": 1.0,
                "retry_jitter": False,
                "max_retry_delay": 60.0,
            }.get(key, default)

            result = await fetch_json_async(2025, "Test%20GP", "Race", "drivers.json")

            assert result == {"drivers": [{"driver": "VER"}]}
            assert mock_cache._store == {}

    async def test_fetch_json_async_success(self):
        """Test successful async JSON fetch."""
        mock_response = StubResponse(status_code=200, payload={"drivers": []})
        mock_cache = StubCache()
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests

            result = await fetch_json_async(2025, "Test%20GP", "Race", "drivers.json")

            assert result == {"drivers": []}

    async def test_fetch_json_async_drivers_preserves_extended_fields(self):
        """Driver validation keeps extended drivers fields."""
        mock_response = StubResponse(
            status_code=200,
            payload={
                "drivers": [
                    {
                        "driver": "VER",
                        "team": "Red Bull",
                        "dn": "1",
                        "fn": "Max",
                        "ln": "Verstappen",
                        "tc": "#3671C6",
                        "url": "https://example.com/ver.png",
                    }
                ]
            },
        )
        mock_cache = StubCache()
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
            patch("tif1.config.get_config") as mock_config,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests
            mock_config.return_value.get.side_effect = lambda key, default=None: {
                "validate_data": True,
                "max_retries": 3,
                "timeout": 30,
            }.get(key, default)

            result = await fetch_json_async(2025, "Test%20GP", "Race", "drivers.json")

            driver = result["drivers"][0]
            assert driver["dn"] == "1"
            assert driver["fn"] == "Max"
            assert driver["ln"] == "Verstappen"
            assert driver["tc"] == "#3671C6"
            assert driver["url"] == "https://example.com/ver.png"

    async def test_fetch_json_async_race_control_preserves_fields(self):
        """Race control validation keeps and normalizes all known fields."""
        mock_response = StubResponse(
            status_code=200,
            payload={
                "time": [10.0, 20.0],
                "cat": ["Other", "Track"],
                "msg": ["Incident", "Green flag"],
                "status": [None, "Clear"],
                "flag": [None, "GREEN"],
                "scope": [None, "Sector"],
                "sector": [None, 2],
                "dNum": [None, "1"],
                "lap": [5, 6],
            },
        )
        mock_cache = StubCache()
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
            patch("tif1.config.get_config") as mock_config,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests
            mock_config.return_value.get.side_effect = lambda key, default=None: {
                "validate_data": True,
                "max_retries": 3,
                "timeout": 30,
            }.get(key, default)

            result = await fetch_json_async(2025, "Test%20GP", "Race", "rcm.json")

            assert result["category"] == ["Other", "Track"]
            assert result["message"] == ["Incident", "Green flag"]
            assert result["racing_number"] == [None, "1"]
            assert "cat" not in result
            assert "msg" not in result
            assert "dNum" not in result

    async def test_fetch_json_async_weather_preserves_fields(self):
        """Weather validation keeps and normalizes all known fields."""
        mock_response = StubResponse(
            status_code=200,
            payload={
                "wT": [10.0, 20.0],
                "wAT": [25.0, 25.2],
                "wH": [40.0, 41.0],
                "wP": [1012.0, 1011.8],
                "wR": [False, False],
                "wTT": [30.0, 30.2],
                "wWD": [180.0, 181.0],
                "wWS": [2.2, 2.1],
            },
        )
        mock_cache = StubCache()
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
            patch("tif1.config.get_config") as mock_config,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests
            mock_config.return_value.get.side_effect = lambda key, default=None: {
                "validate_data": True,
                "max_retries": 3,
                "timeout": 30,
            }.get(key, default)

            result = await fetch_json_async(2025, "Test%20GP", "Race", "weather.json")

            assert result["time"] == [10.0, 20.0]
            assert result["air_temp"] == [25.0, 25.2]
            assert result["rainfall"] == [False, False]
            assert "wT" not in result
            assert "wAT" not in result

    async def test_fetch_json_async_telemetry_preserves_extended_tel_keys(self):
        """Telemetry validation keeps extended CDN telemetry fields."""
        mock_response = StubResponse(
            status_code=200,
            payload={
                "tel": {
                    "time": [0.0, 0.1],
                    "speed": [100.0, 101.0],
                    "DriverAhead": ["VER", "VER"],
                    "DistanceToDriverAhead": [12.0, 11.7],
                    "dataKey": ["k1", "k2"],
                }
            },
        )
        mock_cache = StubCache()
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
            patch("tif1.config.get_config") as mock_config,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests
            mock_config.return_value.get.side_effect = lambda key, default=None: {
                "validate_telemetry": True,
                "max_retries": 3,
                "timeout": 30,
            }.get(key, default)

            result = await fetch_json_async(2025, "Test%20GP", "Race", "VER/1_tel.json")

            assert result["tel"]["driver_ahead"] == ["VER", "VER"]
            assert result["tel"]["distance_to_driver_ahead"] == [12.0, 11.7]
            assert result["tel"]["data_key"] == ["k1", "k2"]
            assert "DriverAhead" not in result["tel"]
            assert "DistanceToDriverAhead" not in result["tel"]
            assert "dataKey" not in result["tel"]

    async def test_fetch_json_async_laptimes_preserves_extended_fields(self):
        """Lap-time validation keeps extended CDN lap fields."""
        mock_response = StubResponse(
            status_code=200,
            payload={
                "time": [90.5],
                "lap": [1],
                "compound": ["SOFT"],
                "stint": [1],
                "s1": [30.0],
                "s2": [30.0],
                "s3": [30.5],
                "life": [1],
                "pos": [1],
                "status": ["Valid"],
                "pb": [True],
                "sesT": [100.0],
                "drv": ["VER"],
                "dNum": ["1"],
                "vi1": [280.0],
                "vi2": [290.0],
                "vfl": [300.0],
                "vst": [305.0],
                "fresh": [True],
                "team": ["Red Bull"],
                "lST": [0.0],
                "lSD": ["2025-03-01T10:00:00"],
                "del": [False],
                "delR": [None],
                "ff1G": [False],
                "iacc": [True],
                "wT": [100.0],
                "wAT": [25.1],
                "wH": [40.0],
                "wP": [1012.0],
                "wR": [False],
                "wTT": [32.5],
                "wWD": [180.0],
                "wWS": [2.2],
            },
        )
        mock_cache = StubCache()
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
            patch("tif1.config.get_config") as mock_config,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests
            mock_config.return_value.get.side_effect = lambda key, default=None: {
                "validate_lap_times": True,
                "max_retries": 3,
                "timeout": 30,
            }.get(key, default)

            result = await fetch_json_async(2025, "Test%20GP", "Race", "VER/laptimes.json")

            assert result["session_time"] == [100.0]
            assert result["source_driver"] == ["VER"]
            assert result["driver_number"] == ["1"]
            assert result["speed_i1"] == [280.0]
            assert result["speed_i2"] == [290.0]
            assert result["source_team"] == ["Red Bull"]
            assert result["weather_time"] == [100.0]
            assert result["air_temp"] == [25.1]
            assert result["track_temp"] == [32.5]
            assert result["wind_direction"] == [180.0]
            assert result["wind_speed"] == [2.2]
            assert "sesT" not in result
            assert "drv" not in result
            assert "team" not in result
            assert "wAT" not in result

    async def test_fetch_json_async_404(self):
        """Test 404 error handling."""
        mock_response = StubResponse(status_code=404)
        mock_cache = StubCache()
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests

            with pytest.raises(DataNotFoundError):
                await fetch_json_async(2025, "Test%20GP", "Race", "missing.json")

    async def test_fetch_json_async_cached(self):
        """Test fetching from cache."""
        cached_data = {"drivers": ["VER", "HAM"]}

        mock_cache = StubCache(preset=cached_data)

        with patch("tif1.async_fetch.get_cache", return_value=mock_cache):
            result = await fetch_json_async(2025, "Test%20GP", "Race", "drivers.json")

            assert result == cached_data

    async def test_fetch_json_async_without_cache_io(self):
        """Test bypassing cache reads and writes."""
        mock_response = StubResponse(status_code=200, payload={"drivers": []})
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache") as mock_get_cache,
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests

            result = await fetch_json_async(
                2025,
                "Test%20GP",
                "Race",
                "drivers.json",
                use_cache=False,
                write_cache=False,
            )

            assert result == {"drivers": []}
            mock_get_cache.assert_not_called()

    async def test_fetch_json_async_without_validation(self):
        """Test bypassing payload validation."""
        mock_response = StubResponse(status_code=200, payload={"drivers": []})
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
            patch("tif1.async_fetch._validate_json_payload") as mock_validate_payload,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests

            result = await fetch_json_async(
                2025,
                "Test%20GP",
                "Race",
                "drivers.json",
                use_cache=False,
                write_cache=False,
                validate_payload=False,
            )

            assert result == {"drivers": []}
            mock_validate_payload.assert_not_called()

    async def test_fetch_json_async_retry(self):
        """Test retry logic on network error."""
        mock_cache = StubCache()

        class MockRequestError(Exception):
            pass

        mock_session = StubSession(error=MockRequestError("Network error"))

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=MockRequestError,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests

            with pytest.raises(NetworkError):
                await fetch_json_async(2025, "Test%20GP", "Race", "drivers.json", max_retries=1)

    async def test_fetch_json_async_reuses_shared_session(self):
        """Test that async fetch reuses a shared session across calls."""
        mock_cache = StubCache()
        mock_response = StubResponse(status_code=200, payload={"ok": True})
        mock_session = StubSession(response=mock_response)

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch(
                "tif1.async_fetch.get_http_session", return_value=mock_session
            ) as mock_get_session,
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
        ):
            mock_niquests = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            mock_niquests_module.return_value = mock_niquests

            await fetch_json_async(2025, "Test%20GP", "Race", "first.json")
            await fetch_json_async(2025, "Test%20GP", "Race", "second.json")

            assert mock_get_session.call_count == 1

    async def test_fetch_multiple_async(self):
        """Test fetching multiple files in parallel."""
        requests = [
            (2025, "Test%20GP", "Race", "VER/laptimes.json"),
            (2025, "Test%20GP", "Race", "HAM/laptimes.json"),
        ]

        with patch("tif1.async_fetch.fetch_json_async") as mock_fetch:
            mock_fetch.side_effect = [
                {"laps": [1, 2, 3]},
                {"laps": [4, 5, 6]},
            ]

            results = await fetch_multiple_async(requests)

            assert len(results) == 2
            assert results[0] == {"laps": [1, 2, 3]}
            assert results[1] == {"laps": [4, 5, 6]}

    async def test_fetch_multiple_async_with_errors(self):
        """Test graceful degradation with errors."""
        requests = [
            (2025, "Test%20GP", "Race", "VER/laptimes.json"),
            (2025, "Test%20GP", "Race", "INVALID/laptimes.json"),
        ]

        with patch("tif1.async_fetch.fetch_json_async") as mock_fetch:
            mock_fetch.side_effect = [
                {"laps": [1, 2, 3]},
                DataNotFoundError("Not found"),
            ]

            results = await fetch_multiple_async(requests)

            assert len(results) == 2
            assert results[0] == {"laps": [1, 2, 3]}
            assert results[1] is None

    async def test_fetch_json_async_zero_retry_uses_first_cdn(self):
        """Zero-retry mode should fetch from first CDN and skip retry loop."""
        mock_cache = StubCache()
        mock_session = MagicMock()
        mock_session.get.return_value = SimpleNamespace(
            status_code=200,
            content=b'{"drivers": []}',
            raise_for_status=lambda: None,
        )
        mock_cdn = SimpleNamespace(
            name="primary", format_url=MagicMock(return_value="https://cdn/a")
        )
        mock_cdn_manager = SimpleNamespace(get_sources=MagicMock(return_value=[mock_cdn]))
        mock_circuit_breaker = MagicMock()

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.cdn.get_cdn_manager", return_value=mock_cdn_manager),
            patch("tif1.retry.get_circuit_breaker", return_value=mock_circuit_breaker),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
        ):
            mock_niquests_module.return_value = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )

            result = await fetch_json_async(
                2025,
                "Test%20GP",
                "Race",
                "drivers.json",
                max_retries=0,
            )

        assert result == {"drivers": []}
        assert mock_session.get.call_count == 1
        assert "2025/Test%20GP/Race/drivers.json" in mock_cache._store

    async def test_fetch_json_async_zero_retry_fallback_to_second_cdn(self):
        """Zero-retry mode should try remaining CDNs immediately after a failure."""
        mock_cache = StubCache()
        call_urls = []

        def _get(url, timeout):
            del timeout
            call_urls.append(url)
            if url.endswith("/a"):
                return SimpleNamespace(
                    status_code=200,
                    content=b'{"drivers": []}',
                    raise_for_status=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
                )
            return SimpleNamespace(
                status_code=200,
                content=b'{"drivers": ["VER"]}',
                raise_for_status=lambda: None,
            )

        mock_session = MagicMock()
        mock_session.get.side_effect = _get

        cdn_a = SimpleNamespace(name="a", format_url=MagicMock(return_value="https://cdn/a"))
        cdn_b = SimpleNamespace(name="b", format_url=MagicMock(return_value="https://cdn/b"))
        mock_cdn_manager = SimpleNamespace(get_sources=MagicMock(return_value=[cdn_a, cdn_b]))
        mock_circuit_breaker = MagicMock()

        with (
            patch("tif1.async_fetch.get_cache", return_value=mock_cache),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.cdn.get_cdn_manager", return_value=mock_cdn_manager),
            patch("tif1.retry.get_circuit_breaker", return_value=mock_circuit_breaker),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
        ):
            mock_niquests_module.return_value = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            result = await fetch_json_async(
                2025,
                "Test%20GP",
                "Race",
                "drivers.json",
                max_retries=0,
            )

        assert result == {"drivers": ["VER"]}
        assert call_urls == ["https://cdn/a", "https://cdn/b"]

    async def test_fetch_json_async_zero_retry_all_cdns_fail(self):
        """Zero-retry mode should raise NetworkError when all CDNs fail."""
        mock_session = MagicMock()
        mock_session.get.side_effect = RuntimeError("down")
        cdn_a = SimpleNamespace(name="a", format_url=MagicMock(return_value="https://cdn/a"))
        cdn_b = SimpleNamespace(name="b", format_url=MagicMock(return_value="https://cdn/b"))
        mock_cdn_manager = SimpleNamespace(get_sources=MagicMock(return_value=[cdn_a, cdn_b]))
        mock_circuit_breaker = MagicMock()

        with (
            patch("tif1.async_fetch.get_cache", return_value=StubCache()),
            patch("tif1.async_fetch.get_http_session", return_value=mock_session),
            patch("tif1.cdn.get_cdn_manager", return_value=mock_cdn_manager),
            patch("tif1.retry.get_circuit_breaker", return_value=mock_circuit_breaker),
            patch("tif1.async_fetch._import_niquests") as mock_niquests_module,
        ):
            mock_niquests_module.return_value = SimpleNamespace(
                RequestException=Exception,
                exceptions=SimpleNamespace(HTTPError=Exception),
            )
            with pytest.raises(NetworkError):
                await fetch_json_async(2025, "Test%20GP", "Race", "drivers.json", max_retries=0)

    async def test_fetch_multiple_async_uses_semaphore_path(self):
        """fetch_multiple_async should use semaphore path when capped below request count."""
        requests = [
            (2025, "Test%20GP", "Race", "VER/laptimes.json"),
            (2025, "Test%20GP", "Race", "HAM/laptimes.json"),
            (2025, "Test%20GP", "Race", "LEC/laptimes.json"),
        ]

        with patch("tif1.async_fetch.fetch_json_async") as mock_fetch:
            mock_fetch.side_effect = [{"driver": "VER"}, {"driver": "HAM"}, {"driver": "LEC"}]
            results = await fetch_multiple_async(requests, max_concurrent_requests=1)

        assert [item["driver"] for item in results if item] == ["VER", "HAM", "LEC"]

    async def test_fetch_multiple_async_empty_requests(self):
        """fetch_multiple_async should fast-return for empty input."""
        assert await fetch_multiple_async([]) == []


class TestHttpGetSession:
    """Test get_session singleton behavior."""

    def test_returns_session_and_caches(self, monkeypatch):
        """get_session returns a session and subsequent calls return the same one."""
        monkeypatch.setattr(http_mod, "_shared_session", None)
        mock_session = MagicMock()
        monkeypatch.setattr(http_mod, "_create_session", lambda: mock_session)

        session1 = get_http_session_fn()
        session2 = get_http_session_fn()
        assert session1 is mock_session
        assert session2 is session1


class TestHttpCloseSession:
    """Test close_session behavior."""

    def test_close_session_resets_singleton(self, monkeypatch):
        """close_session closes the session and resets to None."""
        mock_session = MagicMock()
        monkeypatch.setattr(http_mod, "_shared_session", mock_session)

        close_http_session()
        mock_session.close.assert_called_once()
        assert http_mod._shared_session is None

    def test_close_session_when_no_session(self, monkeypatch):
        """close_session when no session exists is a no-op."""
        monkeypatch.setattr(http_mod, "_shared_session", None)
        close_http_session()
        assert http_mod._shared_session is None


class TestHttpCreateSession:
    """Test _create_session with various resolver configs."""

    def test_with_standard_resolver(self, monkeypatch):
        """Session created with standard (None) resolver."""
        mock_niquests = MagicMock()
        mock_session = MagicMock()
        mock_niquests.Session.return_value = mock_session
        monkeypatch.setattr(http_mod, "_niquests", mock_niquests)
        monkeypatch.setattr(
            "tif1.config.get_config",
            lambda: {
                "pool_connections": 10,
                "pool_maxsize": 20,
                "http_resolvers": ["standard"],
                "http_multiplexed": True,
                "http_disable_http3": False,
                "max_workers": 20,
                "max_concurrent_requests": 20,
                "telemetry_prefetch_max_concurrent_requests": 128,
                "keepalive_timeout": 120,
                "keepalive_max_requests": 1000,
            },
        )

        session = _create_session()
        assert session is mock_session
        mock_niquests.Session.assert_called_once_with(multiplexed=True, disable_http3=False)

    def test_with_doh_resolver(self, monkeypatch):
        """Session created with DoH resolver."""
        mock_niquests = MagicMock()
        mock_session = MagicMock()
        mock_niquests.Session.return_value = mock_session
        monkeypatch.setattr(http_mod, "_niquests", mock_niquests)
        monkeypatch.setattr(
            "tif1.config.get_config",
            lambda: {
                "pool_connections": 10,
                "pool_maxsize": 20,
                "http_resolvers": ["doh://cloudflare"],
                "http_multiplexed": True,
                "http_disable_http3": False,
                "max_workers": 20,
                "max_concurrent_requests": 20,
                "telemetry_prefetch_max_concurrent_requests": 128,
                "keepalive_timeout": 120,
                "keepalive_max_requests": 1000,
            },
        )

        session = _create_session()
        assert session is mock_session
        mock_niquests.Session.assert_called_once_with(
            multiplexed=True, resolver="doh://cloudflare", disable_http3=False
        )

    def test_all_resolvers_fail_raises_network_error(self, monkeypatch):
        """NetworkError raised when all resolvers fail."""
        mock_niquests = MagicMock()
        mock_niquests.Session.side_effect = Exception("connection failed")
        monkeypatch.setattr(http_mod, "_niquests", mock_niquests)
        monkeypatch.setattr(
            "tif1.config.get_config",
            lambda: {
                "pool_connections": 10,
                "pool_maxsize": 20,
                "http_resolvers": ["standard"],
                "http_multiplexed": True,
                "http_disable_http3": False,
                "max_workers": 20,
                "max_concurrent_requests": 20,
                "telemetry_prefetch_max_concurrent_requests": 128,
            },
        )

        with pytest.raises(NetworkError):
            _create_session()

    def test_empty_resolver_list_uses_defaults(self, monkeypatch):
        """Empty resolver list falls back to defaults."""
        mock_niquests = MagicMock()
        mock_session = MagicMock()
        mock_niquests.Session.return_value = mock_session
        monkeypatch.setattr(http_mod, "_niquests", mock_niquests)
        monkeypatch.setattr(
            "tif1.config.get_config",
            lambda: {
                "pool_connections": 10,
                "pool_maxsize": 20,
                "http_resolvers": [],
                "http_multiplexed": True,
                "http_disable_http3": False,
                "max_workers": 20,
                "max_concurrent_requests": 20,
                "telemetry_prefetch_max_concurrent_requests": 128,
                "keepalive_timeout": 120,
                "keepalive_max_requests": 1000,
            },
        )

        session = _create_session()
        assert session is mock_session
        # First call should use standard DNS (None resolver)
        first_call = mock_niquests.Session.call_args_list[0]
        assert first_call == ((), {"multiplexed": True, "disable_http3": False})
