"""Unit tests for HTTP session initialization and resource cleanup."""

from unittest.mock import Mock, patch

import pytest


def test_session_creation_releases_resources_on_failure():
    """Test that failed session initialization releases all allocated resources."""
    from tif1 import http_session

    mock_session = Mock()
    mock_session.mount = Mock(side_effect=Exception("Mount failed"))
    mock_session.close = Mock()

    with patch.object(http_session, "_import_niquests") as mock_import:
        mock_niquests = Mock()
        mock_niquests.Session = Mock(return_value=mock_session)
        mock_niquests.adapters.HTTPAdapter = Mock()
        mock_import.return_value = mock_niquests

        with patch("tif1.config.get_config") as mock_config:
            config = Mock()
            config.get = Mock(
                side_effect=lambda key, default=None: {
                    "pool_connections": 50,
                    "pool_maxsize": 100,
                    "http_resolvers": ["standard"],
                }.get(key, default)
            )
            mock_config.return_value = config

            # Should raise NetworkError after trying all resolvers
            from tif1.exceptions import NetworkError

            with pytest.raises(NetworkError):
                http_session._create_session()

            # Verify session.close() was called to release resources
            mock_session.close.assert_called_once()


def test_session_creation_with_multiple_resolver_fallbacks():
    """Test that session creation tries multiple resolvers and cleans up on each failure."""
    from tif1 import http_session

    close_call_count = 0

    def create_failing_session(*args, **kwargs):
        nonlocal close_call_count
        mock_session = Mock()
        mock_session.mount = Mock(side_effect=Exception("Mount failed"))

        def track_close():
            nonlocal close_call_count
            close_call_count += 1

        mock_session.close = Mock(side_effect=track_close)
        return mock_session

    with patch.object(http_session, "_import_niquests") as mock_import:
        mock_niquests = Mock()
        mock_niquests.Session = Mock(side_effect=create_failing_session)
        mock_niquests.adapters.HTTPAdapter = Mock()
        mock_import.return_value = mock_niquests

        with patch("tif1.config.get_config") as mock_config:
            config = Mock()
            config.get = Mock(
                side_effect=lambda key, default=None: {
                    "pool_connections": 50,
                    "pool_maxsize": 100,
                    "http_resolvers": ["standard", "doh://cloudflare", "doh://google"],
                }.get(key, default)
            )
            mock_config.return_value = config

            from tif1.exceptions import NetworkError

            with pytest.raises(NetworkError):
                http_session._create_session()

            # Verify close was called for each failed attempt (3 resolvers)
            assert close_call_count == 3


def test_session_creation_success_does_not_close():
    """Test that successful session creation does not close the session."""
    from tif1 import http_session

    mock_session = Mock()
    mock_session.mount = Mock()
    mock_session.headers = Mock()
    mock_session.headers.update = Mock()
    mock_session.close = Mock()

    with patch.object(http_session, "_import_niquests") as mock_import:
        mock_niquests = Mock()
        mock_niquests.Session = Mock(return_value=mock_session)
        mock_niquests.adapters.HTTPAdapter = Mock()
        mock_import.return_value = mock_niquests

        with patch("tif1.config.get_config") as mock_config:
            config = Mock()
            config.get = Mock(
                side_effect=lambda key, default=None: {
                    "pool_connections": 50,
                    "pool_maxsize": 100,
                    "http_resolvers": ["standard"],
                }.get(key, default)
            )
            mock_config.return_value = config

            result = http_session._create_session()

            # Verify session was returned and not closed
            assert result is mock_session
            mock_session.close.assert_not_called()


def test_session_creation_failure_during_session_init():
    """Test resource cleanup when Session() constructor itself fails."""
    from tif1 import http_session

    with patch.object(http_session, "_import_niquests") as mock_import:
        mock_niquests = Mock()
        mock_niquests.Session = Mock(side_effect=Exception("Session init failed"))
        mock_import.return_value = mock_niquests

        with patch("tif1.config.get_config") as mock_config:
            config = Mock()
            config.get = Mock(
                side_effect=lambda key, default=None: {
                    "pool_connections": 50,
                    "pool_maxsize": 100,
                    "http_resolvers": ["standard"],
                }.get(key, default)
            )
            mock_config.return_value = config

            from tif1.exceptions import NetworkError

            # Should handle gracefully and try next resolver
            with pytest.raises(NetworkError):
                http_session._create_session()


def test_partial_initialization_cleanup():
    """Test that partial initialization (session created but mount fails) is cleaned up."""
    from tif1 import http_session

    mock_session = Mock()
    mock_session.close = Mock()

    # Simulate failure after session creation but during mount
    call_count = [0]

    def mount_side_effect(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise Exception("First mount failed")
        # Second call succeeds

    mock_session.mount = Mock(side_effect=mount_side_effect)
    mock_session.headers = Mock()
    mock_session.headers.update = Mock()

    with patch.object(http_session, "_import_niquests") as mock_import:
        mock_niquests = Mock()
        mock_niquests.Session = Mock(return_value=mock_session)
        mock_niquests.adapters.HTTPAdapter = Mock()
        mock_import.return_value = mock_niquests

        with patch("tif1.config.get_config") as mock_config:
            config = Mock()
            config.get = Mock(
                side_effect=lambda key, default=None: {
                    "pool_connections": 50,
                    "pool_maxsize": 100,
                    "http_resolvers": ["standard", "doh://cloudflare"],
                }.get(key, default)
            )
            mock_config.return_value = config

            result = http_session._create_session()

            # First attempt should have called close, second succeeded
            assert mock_session.close.call_count == 1
            assert result is mock_session


def test_request_timeout_handling():
    """Test that request timeouts are handled correctly."""
    import asyncio

    from tif1 import async_fetch
    from tif1.exceptions import NetworkError

    mock_session = Mock()
    mock_session.get = Mock(side_effect=TimeoutError("Request timed out"))

    with patch.object(async_fetch, "_get_async_session", return_value=mock_session):
        with patch.object(async_fetch, "_get_executor") as mock_executor:
            # Mock executor to run functions synchronously for testing
            mock_executor.return_value = None

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "max_retries": 3,
                        "timeout": 30,
                        "retry_backoff_factor": 2.0,
                        "retry_jitter": False,
                        "max_retry_delay": 60.0,
                    }.get(key, default)
                )
                mock_config.return_value = config

                with patch("tif1.cdn.get_cdn_manager") as mock_cdn:
                    cdn_manager = Mock()
                    cdn_source = Mock()
                    cdn_source.name = "test-cdn"
                    cdn_source.format_url = Mock(return_value="https://test.com/data.json")
                    cdn_manager.get_sources = Mock(return_value=[cdn_source])
                    mock_cdn.return_value = cdn_manager

                    with patch("tif1.retry.get_circuit_breaker") as mock_cb:
                        circuit_breaker = Mock()
                        circuit_breaker.check_and_update_state = Mock(return_value=(True, "closed"))
                        circuit_breaker.record_failure = Mock()
                        mock_cb.return_value = circuit_breaker

                        # Run async function
                        with pytest.raises(NetworkError):
                            asyncio.run(
                                async_fetch.fetch_json_async(
                                    2024, "Bahrain", "R", "drivers.json", use_cache=False
                                )
                            )

                        # Verify timeout was attempted multiple times (retries)
                        assert mock_session.get.call_count == 3  # max_retries
                        # Verify circuit breaker recorded failures
                        assert circuit_breaker.record_failure.call_count >= 1


def test_retry_logic_with_exponential_backoff():
    """Test that retry logic executes with exponential backoff."""
    import asyncio
    import time

    from tif1 import async_fetch
    from tif1.exceptions import NetworkError

    call_times = []

    def mock_get_with_timing(*args, **kwargs):
        call_times.append(time.monotonic())
        raise ConnectionError("Connection failed")

    mock_session = Mock()
    mock_session.get = Mock(side_effect=mock_get_with_timing)

    with patch.object(async_fetch, "_get_async_session", return_value=mock_session):
        with patch.object(async_fetch, "_get_executor") as mock_executor:
            mock_executor.return_value = None

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "max_retries": 3,
                        "timeout": 30,
                        "retry_backoff_factor": 2.0,
                        "retry_jitter": False,  # Disable jitter for predictable timing
                        "max_retry_delay": 60.0,
                    }.get(key, default)
                )
                mock_config.return_value = config

                with patch("tif1.cdn.get_cdn_manager") as mock_cdn:
                    cdn_manager = Mock()
                    cdn_source = Mock()
                    cdn_source.name = "test-cdn"
                    cdn_source.format_url = Mock(return_value="https://test.com/data.json")
                    cdn_manager.get_sources = Mock(return_value=[cdn_source])
                    mock_cdn.return_value = cdn_manager

                    with patch("tif1.retry.get_circuit_breaker") as mock_cb:
                        circuit_breaker = Mock()
                        circuit_breaker.check_and_update_state = Mock(return_value=(True, "closed"))
                        circuit_breaker.record_failure = Mock()
                        mock_cb.return_value = circuit_breaker

                        with pytest.raises(NetworkError):
                            asyncio.run(
                                async_fetch.fetch_json_async(
                                    2024, "Bahrain", "R", "drivers.json", use_cache=False
                                )
                            )

                        # Verify retries occurred
                        assert len(call_times) == 3  # max_retries

                        # Verify exponential backoff (approximate timing)
                        # First retry: ~1s delay (2^0)
                        # Second retry: ~2s delay (2^1)
                        if len(call_times) >= 3:
                            delay1 = call_times[1] - call_times[0]
                            delay2 = call_times[2] - call_times[1]
                            # Allow some tolerance for timing
                            assert delay1 >= 0.8  # ~1s with tolerance
                            assert delay2 >= 1.8  # ~2s with tolerance


def test_circuit_breaker_integration_open_state():
    """Test that circuit breaker prevents requests when open."""
    import asyncio

    from tif1 import async_fetch
    from tif1.exceptions import NetworkError

    mock_session = Mock()
    mock_session.get = Mock()

    with patch.object(async_fetch, "_get_async_session", return_value=mock_session):
        with patch.object(async_fetch, "_get_executor") as mock_executor:
            mock_executor.return_value = None

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "max_retries": 3,
                        "timeout": 30,
                        "retry_backoff_factor": 2.0,
                        "retry_jitter": False,
                        "max_retry_delay": 60.0,
                    }.get(key, default)
                )
                mock_config.return_value = config

                with patch("tif1.cdn.get_cdn_manager") as mock_cdn:
                    cdn_manager = Mock()
                    cdn_source = Mock()
                    cdn_source.name = "test-cdn"
                    cdn_source.format_url = Mock(return_value="https://test.com/data.json")
                    cdn_manager.get_sources = Mock(return_value=[cdn_source])
                    mock_cdn.return_value = cdn_manager

                    with patch("tif1.retry.get_circuit_breaker") as mock_cb:
                        circuit_breaker = Mock()
                        # Circuit breaker is open - should not proceed
                        circuit_breaker.check_and_update_state = Mock(return_value=(False, "open"))
                        mock_cb.return_value = circuit_breaker

                        with pytest.raises(NetworkError):
                            asyncio.run(
                                async_fetch.fetch_json_async(
                                    2024, "Bahrain", "R", "drivers.json", use_cache=False
                                )
                            )

                        # Verify session.get was NOT called (circuit breaker blocked it)
                        mock_session.get.assert_not_called()


def test_circuit_breaker_integration_half_open_state():
    """Test that circuit breaker allows one request in half-open state."""
    import asyncio

    from tif1 import async_fetch

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.raise_for_status = Mock()
    mock_response.text = '{"test": "data"}'

    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    with patch.object(async_fetch, "_get_async_session", return_value=mock_session):
        with patch.object(async_fetch, "_get_executor") as mock_executor:
            mock_executor.return_value = None

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "max_retries": 3,
                        "timeout": 30,
                        "validate_data": False,
                        "validate_lap_times": False,
                        "validate_telemetry": False,
                    }.get(key, default)
                )
                mock_config.return_value = config

                with patch("tif1.cdn.get_cdn_manager") as mock_cdn:
                    cdn_manager = Mock()
                    cdn_source = Mock()
                    cdn_source.name = "test-cdn"
                    cdn_source.format_url = Mock(return_value="https://test.com/data.json")
                    cdn_manager.get_sources = Mock(return_value=[cdn_source])
                    cdn_manager.mark_success = Mock()
                    mock_cdn.return_value = cdn_manager

                    with patch("tif1.retry.get_circuit_breaker") as mock_cb:
                        circuit_breaker = Mock()
                        # Circuit breaker is half-open - should allow request
                        circuit_breaker.check_and_update_state = Mock(
                            return_value=(True, "half-open")
                        )
                        circuit_breaker.record_success = Mock()
                        mock_cb.return_value = circuit_breaker

                        # Mock the JSON parsing to return a dict
                        with patch("tif1.async_fetch.parse_response_json") as mock_parse:
                            mock_parse.return_value = {"test": "data"}

                            result = asyncio.run(
                                async_fetch.fetch_json_async(
                                    2024, "Bahrain", "R", "drivers.json", use_cache=False
                                )
                            )

                            # Verify request was made
                            mock_session.get.assert_called_once()
                            # Verify circuit breaker recorded success
                            circuit_breaker.record_success.assert_called_once()
                            # Verify result
                            assert result == {"test": "data"}


def test_circuit_breaker_records_failures():
    """Test that circuit breaker records failures on network errors."""
    import asyncio

    from tif1 import async_fetch
    from tif1.exceptions import NetworkError

    mock_session = Mock()
    mock_session.get = Mock(side_effect=ConnectionError("Connection refused"))

    with patch.object(async_fetch, "_get_async_session", return_value=mock_session):
        with patch.object(async_fetch, "_get_executor") as mock_executor:
            mock_executor.return_value = None

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "max_retries": 2,
                        "timeout": 30,
                        "retry_backoff_factor": 2.0,
                        "retry_jitter": False,
                        "max_retry_delay": 60.0,
                    }.get(key, default)
                )
                mock_config.return_value = config

                with patch("tif1.cdn.get_cdn_manager") as mock_cdn:
                    cdn_manager = Mock()
                    cdn_source = Mock()
                    cdn_source.name = "test-cdn"
                    cdn_source.format_url = Mock(return_value="https://test.com/data.json")
                    cdn_manager.get_sources = Mock(return_value=[cdn_source])
                    cdn_manager.mark_failure = Mock()
                    mock_cdn.return_value = cdn_manager

                    with patch("tif1.retry.get_circuit_breaker") as mock_cb:
                        circuit_breaker = Mock()
                        circuit_breaker.check_and_update_state = Mock(return_value=(True, "closed"))
                        circuit_breaker.record_failure = Mock()
                        mock_cb.return_value = circuit_breaker

                        with pytest.raises(NetworkError):
                            asyncio.run(
                                async_fetch.fetch_json_async(
                                    2024, "Bahrain", "R", "drivers.json", use_cache=False
                                )
                            )

                        # Verify circuit breaker recorded failures (one per retry attempt)
                        # With 2 max_retries, we expect 2 attempts
                        assert circuit_breaker.record_failure.call_count >= 2


def test_http_error_status_codes_trigger_retry():
    """Test that HTTP error status codes trigger retry logic."""
    import asyncio

    from tif1 import async_fetch
    from tif1.exceptions import NetworkError

    # Import niquests to use the proper exception type
    mock_niquests = Mock()
    mock_niquests.RequestException = Exception
    mock_niquests.exceptions = Mock()
    mock_niquests.exceptions.HTTPError = Exception

    mock_response = Mock()
    mock_response.status_code = 503  # Service Unavailable
    mock_response.raise_for_status = Mock(side_effect=Exception("503 Server Error"))

    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    with patch.object(async_fetch, "_get_async_session", return_value=mock_session):
        with patch.object(async_fetch, "_get_executor") as mock_executor:
            mock_executor.return_value = None

            with patch.object(async_fetch, "_import_niquests", return_value=mock_niquests):
                with patch("tif1.config.get_config") as mock_config:
                    config = Mock()
                    config.get = Mock(
                        side_effect=lambda key, default=None: {
                            "max_retries": 3,
                            "timeout": 30,
                            "retry_backoff_factor": 2.0,
                            "retry_jitter": False,
                            "max_retry_delay": 60.0,
                        }.get(key, default)
                    )
                    mock_config.return_value = config

                    with patch("tif1.cdn.get_cdn_manager") as mock_cdn:
                        cdn_manager = Mock()
                        cdn_source = Mock()
                        cdn_source.name = "test-cdn"
                        cdn_source.format_url = Mock(return_value="https://test.com/data.json")
                        cdn_manager.get_sources = Mock(return_value=[cdn_source])
                        cdn_manager.mark_failure = Mock()
                        mock_cdn.return_value = cdn_manager

                        with patch("tif1.retry.get_circuit_breaker") as mock_cb:
                            circuit_breaker = Mock()
                            circuit_breaker.check_and_update_state = Mock(
                                return_value=(True, "closed")
                            )
                            circuit_breaker.record_failure = Mock()
                            mock_cb.return_value = circuit_breaker

                            with pytest.raises(NetworkError):
                                asyncio.run(
                                    async_fetch.fetch_json_async(
                                        2024, "Bahrain", "R", "drivers.json", use_cache=False
                                    )
                                )

                            # Verify retries occurred (3 attempts per retry)
                            assert mock_session.get.call_count >= 3


def test_connection_pool_exhaustion_handling():
    """Test that connection pool exhaustion is handled with backoff."""
    import asyncio

    from tif1 import async_fetch
    from tif1.exceptions import NetworkError

    # Simulate pool exhaustion error
    pool_error = ConnectionError("HTTPConnectionPool: Max retries exceeded (pool timeout)")

    mock_session = Mock()
    mock_session.get = Mock(side_effect=pool_error)

    with patch.object(async_fetch, "_get_async_session", return_value=mock_session):
        with patch.object(async_fetch, "_get_executor") as mock_executor:
            mock_executor.return_value = None

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "max_retries": 2,
                        "timeout": 30,
                        "retry_backoff_factor": 2.0,
                        "retry_jitter": False,
                        "max_retry_delay": 60.0,
                    }.get(key, default)
                )
                mock_config.return_value = config

                with patch("tif1.cdn.get_cdn_manager") as mock_cdn:
                    cdn_manager = Mock()
                    cdn_source = Mock()
                    cdn_source.name = "test-cdn"
                    cdn_source.format_url = Mock(return_value="https://test.com/data.json")
                    cdn_manager.get_sources = Mock(return_value=[cdn_source])
                    cdn_manager.mark_failure = Mock()
                    mock_cdn.return_value = cdn_manager

                    with patch("tif1.retry.get_circuit_breaker") as mock_cb:
                        circuit_breaker = Mock()
                        circuit_breaker.check_and_update_state = Mock(return_value=(True, "closed"))
                        circuit_breaker.record_failure = Mock()
                        mock_cb.return_value = circuit_breaker

                        with pytest.raises(NetworkError):
                            asyncio.run(
                                async_fetch.fetch_json_async(
                                    2024, "Bahrain", "R", "drivers.json", use_cache=False
                                )
                            )

                        # Verify retries occurred
                        assert mock_session.get.call_count == 2
                        # Verify circuit breaker recorded failures
                        assert circuit_breaker.record_failure.call_count >= 2


def test_successful_request_records_circuit_breaker_success():
    """Test that successful requests record success in circuit breaker."""
    import asyncio

    from tif1 import async_fetch

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.raise_for_status = Mock()
    mock_response.text = '{"drivers": ["VER", "HAM"]}'

    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    with patch.object(async_fetch, "_get_async_session", return_value=mock_session):
        with patch.object(async_fetch, "_get_executor") as mock_executor:
            mock_executor.return_value = None

            with patch("tif1.config.get_config") as mock_config:
                config = Mock()
                config.get = Mock(
                    side_effect=lambda key, default=None: {
                        "max_retries": 3,
                        "timeout": 30,
                        "validate_data": False,
                        "validate_lap_times": False,
                        "validate_telemetry": False,
                    }.get(key, default)
                )
                mock_config.return_value = config

                with patch("tif1.cdn.get_cdn_manager") as mock_cdn:
                    cdn_manager = Mock()
                    cdn_source = Mock()
                    cdn_source.name = "test-cdn"
                    cdn_source.format_url = Mock(return_value="https://test.com/data.json")
                    cdn_manager.get_sources = Mock(return_value=[cdn_source])
                    cdn_manager.mark_success = Mock()
                    mock_cdn.return_value = cdn_manager

                    with patch("tif1.retry.get_circuit_breaker") as mock_cb:
                        circuit_breaker = Mock()
                        circuit_breaker.check_and_update_state = Mock(return_value=(True, "closed"))
                        circuit_breaker.record_success = Mock()
                        mock_cb.return_value = circuit_breaker

                        # Mock the JSON parsing to return a dict
                        with patch("tif1.async_fetch.parse_response_json") as mock_parse:
                            mock_parse.return_value = {"drivers": ["VER", "HAM"]}

                            result = asyncio.run(
                                async_fetch.fetch_json_async(
                                    2024, "Bahrain", "R", "drivers.json", use_cache=False
                                )
                            )

                            # Verify circuit breaker recorded success
                            circuit_breaker.record_success.assert_called_once()
                            # Verify CDN manager recorded success
                            cdn_manager.mark_success.assert_called_once_with("test-cdn")
                            # Verify result
                            assert result == {"drivers": ["VER", "HAM"]}
