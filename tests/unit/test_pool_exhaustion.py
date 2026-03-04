"""Tests for connection pool exhaustion handling."""

from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest

from tif1.exceptions import NetworkError


class PoolExhaustionError(Exception):
    """Mock pool exhaustion error."""

    def __init__(self, message: str = "Connection pool is full"):
        super().__init__(message)


def create_completed_future(result=None, exception=None):
    """Create a completed future for testing."""
    future = Future()
    if exception:
        future.set_exception(exception)
    else:
        future.set_result(result)
    return future


@pytest.mark.asyncio
async def test_pool_exhaustion_retry_with_backoff():
    """Test that pool exhaustion triggers retry with exponential backoff."""
    from tif1 import async_fetch

    # Mock dependencies
    with (
        patch.object(async_fetch, "get_cache") as mock_cache,
        patch.object(async_fetch, "_import_niquests") as mock_niquests_import,
        patch("tif1.cdn.get_cdn_manager") as mock_cdn_manager,
        patch("tif1.config.get_config") as mock_config,
        patch("tif1.retry.get_circuit_breaker") as mock_cb,
        patch("tif1.async_fetch.parse_response_json") as mock_parse_json,
    ):
        # Setup mocks
        mock_cache.return_value = None

        # Mock niquests module
        mock_niquests = MagicMock()
        mock_niquests.RequestException = Exception
        mock_niquests.exceptions.HTTPError = Exception
        mock_niquests_import.return_value = mock_niquests

        # Mock config
        config_dict = {
            "max_retries": 3,
            "timeout": 30,
            "retry_backoff_factor": 2.0,
            "retry_jitter": False,  # Disable jitter for predictable timing
            "max_retry_delay": 60.0,
            "max_workers": 4,
        }
        mock_config.return_value.get.side_effect = lambda k, default=None: config_dict.get(
            k, default
        )

        # Mock CDN manager
        mock_cdn_source = MagicMock()
        mock_cdn_source.name = "test-cdn"
        mock_cdn_source.format_url.return_value = "https://test.com/data.json"
        mock_cdn_manager.return_value.get_sources.return_value = [mock_cdn_source]

        # Mock circuit breaker
        mock_cb.return_value.check_and_update_state.return_value = (True, "closed")

        # Mock session that raises pool exhaustion error
        pool_error = PoolExhaustionError("Connection pool timeout")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_parse_json.return_value = {"test": "data"}

        call_count = 0

        def session_get_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # First two attempts fail with pool exhaustion
                raise pool_error
            # Third attempt succeeds
            return mock_response

        mock_session = MagicMock()
        mock_session.get.side_effect = session_get_side_effect

        # Track sleep calls to verify backoff
        sleep_calls = []

        async def mock_sleep(delay):
            sleep_calls.append(delay)

        with (
            patch.object(async_fetch, "_get_async_session", return_value=mock_session),
            patch("asyncio.sleep", side_effect=mock_sleep),
        ):
            # Execute fetch
            result = await async_fetch.fetch_json_async(
                year=2024,
                gp="bahrain",
                session="race",
                path="test.json",
                use_cache=False,
                write_cache=False,
                validate_payload=False,
            )

            # Verify result
            assert result == {"test": "data"}

            # Verify retries occurred
            assert call_count == 3

            # Verify backoff was applied
            # Pool exhaustion adds immediate backoff (0.5s base) before regular retry backoff
            # First failure: pool backoff ~0.5s
            # Second failure: pool backoff ~1.0s (0.5 * 2^1)
            # Then regular retry backoff: 1.0s, 2.0s
            assert len(sleep_calls) >= 4
            # Pool exhaustion backoffs come first
            assert 0.4 <= sleep_calls[0] <= 0.6  # First pool backoff
            assert 0.9 <= sleep_calls[1] <= 1.1  # Second pool backoff
            # Then regular retry backoffs
            assert 0.9 <= sleep_calls[2] <= 1.1  # First retry backoff
            assert 1.9 <= sleep_calls[3] <= 2.1  # Second retry backoff


@pytest.mark.asyncio
async def test_pool_exhaustion_logged_as_warning():
    """Test that pool exhaustion errors are logged with specific warning."""
    from tif1 import async_fetch

    with (
        patch.object(async_fetch, "get_cache") as mock_cache,
        patch.object(async_fetch, "_import_niquests") as mock_niquests_import,
        patch("tif1.cdn.get_cdn_manager") as mock_cdn_manager,
        patch("tif1.config.get_config") as mock_config,
        patch("tif1.retry.get_circuit_breaker") as mock_cb,
        patch("tif1.async_fetch.logger") as mock_logger,
    ):
        # Setup mocks
        mock_cache.return_value = None

        mock_niquests = MagicMock()
        mock_niquests.RequestException = Exception
        mock_niquests.exceptions.HTTPError = Exception
        mock_niquests_import.return_value = mock_niquests

        config_dict = {
            "max_retries": 2,
            "timeout": 30,
            "retry_backoff_factor": 2.0,
            "retry_jitter": False,
            "max_retry_delay": 60.0,
            "max_workers": 4,
        }
        mock_config.return_value.get.side_effect = lambda k, default=None: config_dict.get(
            k, default
        )

        mock_cdn_source = MagicMock()
        mock_cdn_source.name = "test-cdn"
        mock_cdn_source.format_url.return_value = "https://test.com/data.json"
        mock_cdn_manager.return_value.get_sources.return_value = [mock_cdn_source]

        mock_cb.return_value.check_and_update_state.return_value = (True, "closed")

        # Raise pool exhaustion error
        pool_error = PoolExhaustionError("Connection pool is full")
        mock_session = MagicMock()
        mock_session.get.side_effect = pool_error

        with (
            patch.object(async_fetch, "_get_async_session", return_value=mock_session),
            patch("asyncio.sleep"),
            pytest.raises(NetworkError),
        ):
            # Execute fetch (will fail after retries)
            await async_fetch.fetch_json_async(
                year=2024,
                gp="bahrain",
                session="race",
                path="test.json",
                use_cache=False,
                write_cache=False,
                validate_payload=False,
            )

        # Verify pool exhaustion was logged
        warning_calls = list(mock_logger.warning.call_args_list)
        pool_warnings = [call for call in warning_calls if "pool exhaustion" in str(call).lower()]
        assert len(pool_warnings) > 0, "Pool exhaustion should be logged as warning"


@pytest.mark.asyncio
async def test_pool_exhaustion_multiple_cdns():
    """Test pool exhaustion handling with multiple CDN sources."""
    from tif1 import async_fetch

    with (
        patch.object(async_fetch, "get_cache") as mock_cache,
        patch.object(async_fetch, "_import_niquests") as mock_niquests_import,
        patch("tif1.cdn.get_cdn_manager") as mock_cdn_manager,
        patch("tif1.config.get_config") as mock_config,
        patch("tif1.retry.get_circuit_breaker") as mock_cb,
        patch("tif1.async_fetch.parse_response_json") as mock_parse_json,
    ):
        # Setup mocks
        mock_cache.return_value = None

        mock_niquests = MagicMock()
        mock_niquests.RequestException = Exception
        mock_niquests.exceptions.HTTPError = Exception
        mock_niquests_import.return_value = mock_niquests

        config_dict = {
            "max_retries": 2,
            "timeout": 30,
            "retry_backoff_factor": 2.0,
            "retry_jitter": False,
            "max_retry_delay": 60.0,
            "max_workers": 4,
        }
        mock_config.return_value.get.side_effect = lambda k, default=None: config_dict.get(
            k, default
        )

        # Multiple CDN sources
        mock_cdn1 = MagicMock()
        mock_cdn1.name = "cdn1"
        mock_cdn1.format_url.return_value = "https://cdn1.com/data.json"

        mock_cdn2 = MagicMock()
        mock_cdn2.name = "cdn2"
        mock_cdn2.format_url.return_value = "https://cdn2.com/data.json"

        mock_cdn_manager.return_value.get_sources.return_value = [mock_cdn1, mock_cdn2]

        mock_cb.return_value.check_and_update_state.return_value = (True, "closed")

        # First CDN fails with pool exhaustion, second succeeds
        pool_error = PoolExhaustionError("Connection pool timeout")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_parse_json.return_value = {"test": "data"}

        call_count = 0

        def session_get_side_effect(url, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if "cdn1" in url:
                # First CDN always fails with pool exhaustion
                raise pool_error
            # Second CDN succeeds
            return mock_response

        mock_session = MagicMock()
        mock_session.get.side_effect = session_get_side_effect

        sleep_calls = []

        async def mock_sleep(delay):
            sleep_calls.append(delay)

        with (
            patch.object(async_fetch, "_get_async_session", return_value=mock_session),
            patch("asyncio.sleep", side_effect=mock_sleep),
        ):
            # Execute fetch
            result = await async_fetch.fetch_json_async(
                year=2024,
                gp="bahrain",
                session="race",
                path="test.json",
                use_cache=False,
                write_cache=False,
                validate_payload=False,
            )

            # Verify result from second CDN
            assert result == {"test": "data"}

            # Verify pool backoff was applied before trying second CDN
            assert len(sleep_calls) >= 1
            assert 0.4 <= sleep_calls[0] <= 0.6  # Pool backoff before cdn2


@pytest.mark.asyncio
async def test_non_pool_errors_no_extra_backoff():
    """Test that non-pool errors don't trigger pool exhaustion backoff."""
    from tif1 import async_fetch

    with (
        patch.object(async_fetch, "get_cache") as mock_cache,
        patch.object(async_fetch, "_import_niquests") as mock_niquests_import,
        patch("tif1.cdn.get_cdn_manager") as mock_cdn_manager,
        patch("tif1.config.get_config") as mock_config,
        patch("tif1.retry.get_circuit_breaker") as mock_cb,
        patch("tif1.async_fetch.parse_response_json") as mock_parse_json,
    ):
        # Setup mocks
        mock_cache.return_value = None

        mock_niquests = MagicMock()
        mock_niquests.RequestException = Exception
        mock_niquests.exceptions.HTTPError = Exception
        mock_niquests_import.return_value = mock_niquests

        config_dict = {
            "max_retries": 2,
            "timeout": 30,
            "retry_backoff_factor": 2.0,
            "retry_jitter": False,
            "max_retry_delay": 60.0,
            "max_workers": 4,
        }
        mock_config.return_value.get.side_effect = lambda k, default=None: config_dict.get(
            k, default
        )

        # Two CDN sources
        mock_cdn1 = MagicMock()
        mock_cdn1.name = "cdn1"
        mock_cdn1.format_url.return_value = "https://cdn1.com/data.json"

        mock_cdn2 = MagicMock()
        mock_cdn2.name = "cdn2"
        mock_cdn2.format_url.return_value = "https://cdn2.com/data.json"

        mock_cdn_manager.return_value.get_sources.return_value = [mock_cdn1, mock_cdn2]

        mock_cb.return_value.check_and_update_state.return_value = (True, "closed")

        # Regular connection error (not pool exhaustion)
        regular_error = ConnectionError("Connection refused")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_parse_json.return_value = {"test": "data"}

        call_count = 0

        def session_get_side_effect(url, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if "cdn1" in url:
                raise regular_error
            return mock_response

        mock_session = MagicMock()
        mock_session.get.side_effect = session_get_side_effect

        sleep_calls = []

        async def mock_sleep(delay):
            sleep_calls.append(delay)

        with (
            patch.object(async_fetch, "_get_async_session", return_value=mock_session),
            patch("asyncio.sleep", side_effect=mock_sleep),
        ):
            # Execute fetch
            result = await async_fetch.fetch_json_async(
                year=2024,
                gp="bahrain",
                session="race",
                path="test.json",
                use_cache=False,
                write_cache=False,
                validate_payload=False,
            )

            # Verify result
            assert result == {"test": "data"}

            # Verify NO pool backoff was applied (regular error doesn't trigger it)
            # Should go straight to cdn2 without extra sleep
            assert len(sleep_calls) == 0
