"""Async functionality for parallel data fetching."""

import asyncio
import atexit
import logging
import random
import threading
from functools import partial
from typing import TYPE_CHECKING, Any

from . import cache as _cache
from .core_utils.json_utils import json_loads, parse_response_json
from .exceptions import DataNotFoundError, InvalidDataError, NetworkError
from .http_session import close_session as close_http_session
from .http_session import get_session as get_http_session

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def get_cache():
    return _cache.get_cache()


def _validate_json_payload(path: str, data: dict[str, Any], config) -> dict[str, Any]:
    """Validate fetched payloads based on path and config toggles."""

    def _sanitize_telemetry_payload(payload: Any) -> Any:
        """Drop validator-only telemetry defaults that break DataFrame construction."""
        if not isinstance(payload, dict):
            return payload
        sanitized: dict[str, Any] = {}
        for key, value in payload.items():
            if key == "tel":
                continue
            if isinstance(value, list) and not value:
                continue
            sanitized[key] = value
        return sanitized

    try:
        if path == "drivers.json" and config.get("validate_data", True):
            from .validation import validate_drivers

            try:
                return validate_drivers(data).model_dump()
            except (InvalidDataError, KeyError, TypeError, ValueError) as e:
                logger.debug(f"Driver validation failed (non-strict) for {path}: {e}")
                return data

        if (path == "rcm.json" or path.endswith("/rcm.json")) and config.get("validate_data", True):
            from .validation import validate_race_control_data

            return validate_race_control_data(data, strict=False)

        if (path == "weather.json" or path.endswith("/weather.json")) and config.get(
            "validate_data", True
        ):
            from .validation import validate_weather_data

            return validate_weather_data(data, strict=False)

        if (
            path == "session_laptimes.json" or path.endswith("/laptimes.json")
        ) and config.get("validate_lap_times", True):
            from .validation import validate_lap_data

            return validate_lap_data(data, strict=False)

        if path.endswith("_tel.json") and config.get("validate_telemetry", True):
            from .validation import validate_telemetry_data

            if "tel" in data and isinstance(data["tel"], dict):
                validated = validate_telemetry_data(data["tel"], strict=False)
                validated = _sanitize_telemetry_payload(validated)
                merged = dict(data)
                merged["tel"] = validated
                return merged

            validated = validate_telemetry_data(data, strict=False)
            return _sanitize_telemetry_payload(validated)
    except (AttributeError, InvalidDataError, KeyError, TypeError, ValueError) as e:
        raise InvalidDataError(reason=f"Validation failed for {path}: {e}") from e

    return data


# Deferred imports
_niquests = None
_ThreadPoolExecutor = None
_ProcessPoolExecutor = None


def _import_niquests():
    global _niquests
    if _niquests is None:
        import niquests

        _niquests = niquests
    return _niquests


def _import_executor():
    global _ThreadPoolExecutor
    if _ThreadPoolExecutor is None:
        from concurrent.futures import ThreadPoolExecutor

        _ThreadPoolExecutor = ThreadPoolExecutor
    return _ThreadPoolExecutor


def _import_process_executor():
    global _ProcessPoolExecutor
    if _ProcessPoolExecutor is None:
        from concurrent.futures import ProcessPoolExecutor

        _ProcessPoolExecutor = ProcessPoolExecutor
    return _ProcessPoolExecutor


# Thread pool executor for async operations
_executor = None
_executor_lock = threading.Lock()
_json_parse_executor = None
_json_parse_executor_lock = threading.Lock()
_cleanup_lock = threading.Lock()
_async_session = None


def _get_executor() -> Any:  # Returns ThreadPoolExecutor
    """Get or create shared thread pool executor (thread-safe).

    Ensures no partial state remains if initialization fails.
    Only assigns to global after successful creation.

    Returns:
        ThreadPoolExecutor instance

    Raises:
        NetworkError: If executor initialization fails
    """
    global _executor
    if _executor is not None:
        return _executor

    with _executor_lock:
        if _executor is not None:
            return _executor

        temp_executor = None
        try:
            ThreadPoolExecutor = _import_executor()
            from .config import get_config

            config = get_config()
            max_workers = max(
                1,
                config.get("max_workers", 20),
                config.get("max_concurrent_requests", 20),
                config.get("telemetry_prefetch_max_concurrent_requests", 128),
            )

            # Create executor
            temp_executor = ThreadPoolExecutor(max_workers=max_workers)

            # Only assign to global after successful creation
            _executor = temp_executor
            logger.debug(f"Thread pool executor initialized (max_workers={max_workers})")
            return _executor

        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as e:
            # Ensure no partial state remains on failure
            if temp_executor is not None:
                try:
                    temp_executor.shutdown(wait=False)
                except (OSError, RuntimeError):
                    pass
            raise NetworkError(url="executor_init", status_code=None) from e


def _parse_json_payload_bytes(payload: bytes | bytearray | memoryview) -> Any:
    """Parse raw JSON bytes in a worker process."""
    return json_loads(payload)


def _get_json_parse_executor() -> Any | None:
    """Get or create shared process pool for JSON parsing."""
    global _json_parse_executor
    if _json_parse_executor is not None:
        return _json_parse_executor

    with _json_parse_executor_lock:
        if _json_parse_executor is not None:
            return _json_parse_executor

        from .config import get_config

        config = get_config()
        worker_count = int(config.get("json_parse_workers", 0))
        if worker_count <= 0:
            return None

        ProcessPoolExecutor = _import_process_executor()
        _json_parse_executor = ProcessPoolExecutor(max_workers=max(1, worker_count))
        logger.debug("JSON parse process pool initialized (max_workers=%s)", worker_count)
        return _json_parse_executor


def close_session() -> None:
    """Close shared HTTP session."""
    global _async_session
    with _cleanup_lock:
        if _async_session is not None:
            close_http_session()
            _async_session = None
            logger.debug("HTTP session closed")


def _get_async_session():
    """Get or create shared async HTTP session."""
    global _async_session
    if _async_session is not None:
        return _async_session

    with _cleanup_lock:
        if _async_session is None:
            _async_session = get_http_session()
        return _async_session


def close_executor() -> None:
    """Shutdown shared thread pool executor."""
    global _executor
    with _cleanup_lock:
        if _executor is not None:
            _executor.shutdown(wait=True)
            _executor = None
            logger.debug("Thread pool executor shutdown")


def cleanup_resources() -> None:
    """Cleanup all shared resources.

    Closes all executors and sessions regardless of their current state.
    Handles cleanup errors gracefully and logs failures without raising.

    This function safely cleans up resources by capturing references
    before acquiring the lock, preventing potential deadlocks from
    callbacks that might need the lock.
    """
    global _async_session, _executor, _json_parse_executor

    # Capture references outside the lock to avoid deadlock
    # if close() triggers callbacks that need the lock
    session_to_close = None
    executor_to_shutdown = None
    json_executor_to_shutdown = None

    with _cleanup_lock:
        session_to_close = _async_session
        executor_to_shutdown = _executor
        json_executor_to_shutdown = _json_parse_executor
        _async_session = None
        _executor = None
        _json_parse_executor = None

    # Close resources outside the lock
    # Handle each resource independently so one failure doesn't prevent others
    if session_to_close is not None:
        try:
            # Close the session directly
            if hasattr(session_to_close, "close"):
                session_to_close.close()
            logger.debug("HTTP session closed")
        except (AttributeError, OSError, RuntimeError) as e:
            logger.warning(f"Error closing HTTP session: {e}")

    if executor_to_shutdown is not None:
        try:
            executor_to_shutdown.shutdown(wait=True)
            logger.debug("Thread pool executor shutdown")
        except (AttributeError, OSError, RuntimeError) as e:
            logger.warning(f"Error shutting down executor: {e}")

    if json_executor_to_shutdown is not None:
        try:
            json_executor_to_shutdown.shutdown(wait=True)
            logger.debug("JSON parse process pool shutdown")
        except (AttributeError, OSError, RuntimeError) as e:
            logger.warning(f"Error shutting down JSON parse process pool: {e}")


atexit.register(cleanup_resources)


async def fetch_with_rate_limit(
    coro_func,
    *args,
    semaphore: asyncio.Semaphore | None = None,
    **kwargs,
):
    """Execute async function with rate limiting using semaphore.

    This function provides concurrency control for async operations by using
    a semaphore to limit the number of concurrent executions. If no semaphore
    is provided, one is created based on the configured max_concurrent_requests.

    Args:
        coro_func: Async function to execute
        *args: Positional arguments for coro_func
        semaphore: Optional semaphore for rate limiting. If None, creates one
                   based on max_concurrent_requests config
        **kwargs: Keyword arguments for coro_func

    Returns:
        Result from coro_func execution

    Raises:
        Any exception raised by coro_func

    Example:
        >>> semaphore = asyncio.Semaphore(5)
        >>> result = await fetch_with_rate_limit(
        ...     fetch_json_async, 2024, "Bahrain", "R", "drivers.json",
        ...     semaphore=semaphore
        ... )
    """
    if semaphore is None:
        from .config import get_config

        config = get_config()
        max_concurrent = max(1, config.get("max_concurrent_requests", 20))
        semaphore = asyncio.Semaphore(max_concurrent)

    async with semaphore:
        return await coro_func(*args, **kwargs)


async def fetch_json_async(
    year: int,
    gp: str,
    session: str,
    path: str,
    max_retries: int | None = None,
    timeout: int | None = None,
    *,
    use_cache: bool = True,
    write_cache: bool = True,
    validate_payload: bool = True,
) -> dict[str, Any]:
    """Fetch JSON data asynchronously with caching, retry logic, and CDN fallback.

    Args:
        year: Season year
        gp: Grand Prix name
        session: Session name
        path: Path to JSON file
        max_retries: Maximum retry attempts (defaults to config value)
        timeout: Request timeout in seconds (defaults to config value)
        use_cache: If True, read from cache before network fetch
        write_cache: If True, persist successful network responses to cache
        validate_payload: If True, run payload validation before returning data

    Returns:
        Parsed JSON data (never None, raises on error)

    Raises:
        NetworkError: If network request fails after retries
        DataNotFoundError: If data doesn't exist
        InvalidDataError: If data is corrupted
    """
    from .config import get_config

    config = get_config()
    if bool(config.get("ci_mode", False)):
        use_cache = False
        write_cache = False

    cache_key = f"{year}/{gp}/{session}/{path}"
    cache = get_cache() if (use_cache or write_cache) else None

    loop = asyncio.get_running_loop()
    executor = _get_executor()

    if use_cache and cache is not None:
        cached = None
        memory_get = getattr(cache, "_get_from_memory", None)
        if callable(memory_get):
            cached = memory_get(cache_key)
        if cached is not None:
            return cached

        cached = await loop.run_in_executor(executor, cache.get, cache_key)
        if cached is not None:
            return cached

    if bool(config.get("offline_mode", False)):
        raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None)

    from .cdn import get_cdn_manager
    from .retry import get_circuit_breaker

    if max_retries is None:
        max_retries = max(0, config.get("max_retries", 3))
    if timeout is None:
        timeout = max(1, config.get("timeout", 30))

    cdn_manager = get_cdn_manager()
    circuit_breaker = get_circuit_breaker()
    niquests = _import_niquests()
    session_client = _get_async_session()

    backoff_factor = config.get("retry_backoff_factor", 2.0)
    use_jitter = config.get("retry_jitter", True)
    max_delay = config.get("max_retry_delay", 60.0)

    # Fast path for zero-retry mode (ultra-cold start optimization)
    if max_retries == 0:
        cdn_sources = cdn_manager.get_sources()
        if not cdn_sources:
            raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None)

        # Try first CDN only, no retries
        cdn_source = cdn_sources[0]
        try:
            url = cdn_source.format_url(year, gp, session, path)
            response = await loop.run_in_executor(
                executor, partial(session_client.get, url, timeout=timeout)
            )

            from .http_session import _track_request

            _track_request(reused=True)

            if response.status_code == 404:
                raise DataNotFoundError(year=year, event=gp, session=session)

            response.raise_for_status()

            content = getattr(response, "content", None)
            if isinstance(content, bytes | bytearray | memoryview):
                data = json_loads(content)
            else:
                data = await loop.run_in_executor(executor, parse_response_json, response)

            if not isinstance(data, dict):
                raise InvalidDataError(reason=f"Expected dict, got {type(data).__name__}")

            if validate_payload:
                data = _validate_json_payload(path, data, config)

            if write_cache and cache is not None:
                await loop.run_in_executor(executor, cache.set, cache_key, data)

            return data
        except (DataNotFoundError, InvalidDataError):
            raise
        except Exception as e:
            # Try remaining CDNs without delay
            for cdn_source in cdn_sources[1:]:
                try:
                    url = cdn_source.format_url(year, gp, session, path)
                    response = await loop.run_in_executor(
                        executor, partial(session_client.get, url, timeout=timeout)
                    )
                    _track_request(reused=True)

                    if response.status_code == 404:
                        raise DataNotFoundError(year=year, event=gp, session=session)

                    response.raise_for_status()

                    content = getattr(response, "content", None)
                    if isinstance(content, bytes | bytearray | memoryview):
                        data = json_loads(content)
                    else:
                        data = await loop.run_in_executor(executor, parse_response_json, response)

                    if not isinstance(data, dict):
                        continue

                    if validate_payload:
                        data = _validate_json_payload(path, data, config)

                    if write_cache and cache is not None:
                        await loop.run_in_executor(executor, cache.set, cache_key, data)

                    return data
                except Exception:
                    continue

            raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None) from e

    async def _try_cdn(cdn_source, attempt_num):
        """Try fetching from a single CDN source."""
        try:
            url = cdn_source.format_url(year, gp, session, path)

            response = await loop.run_in_executor(
                executor, partial(session_client.get, url, timeout=timeout)
            )

            # Track connection reuse (niquests reuses connections from pool)
            from .http_session import _track_request

            _track_request(reused=True)

            if response.status_code == 404:
                raise DataNotFoundError(year=year, event=gp, session=session)

            response.raise_for_status()

            try:
                content = getattr(response, "content", None)
                json_parse_executor = _get_json_parse_executor()
                is_telemetry_payload = path.endswith("_tel.json")
                if isinstance(content, bytes | bytearray | memoryview):
                    if json_parse_executor and not is_telemetry_payload:
                        data = await loop.run_in_executor(
                            json_parse_executor, _parse_json_payload_bytes, bytes(content)
                        )
                    elif is_telemetry_payload:
                        # Telemetry-heavy cold starts perform better without cross-process IPC.
                        data = json_loads(content)
                    else:
                        data = await loop.run_in_executor(executor, json_loads, content)
                else:
                    data = await loop.run_in_executor(executor, parse_response_json, response)
            except (ValueError, TypeError, AttributeError) as e:
                raise InvalidDataError(reason=f"JSON parsing failed: {e}")

            if not isinstance(data, dict):
                raise InvalidDataError(reason=f"Expected dict, got {type(data).__name__}")

            if validate_payload:
                data = _validate_json_payload(path, data, config)

            if write_cache and cache is not None:
                await loop.run_in_executor(executor, cache.set, cache_key, data)

            cdn_manager.mark_success(cdn_source.name)
            circuit_breaker.record_success()

            logger.debug(f"Fetched: {cache_key} from {cdn_source.name}")
            return data, None

        except (DataNotFoundError, InvalidDataError) as e:
            # Fatal errors - don't retry
            return None, e
        except (
            niquests.RequestException,
            niquests.exceptions.HTTPError,
            TimeoutError,
            OSError,
            ConnectionError,
        ) as e:
            if (
                type(e).__name__ == "MultiplexingError"
                and "non-multiplexed response after a redirect" in str(e)
            ):
                cdn_source.enabled = False
                logger.warning(
                    "Disabling CDN %s after multiplexing redirect incompatibility",
                    cdn_source.name,
                )
                return None, e

            # Check if this is a pool exhaustion error
            is_pool_exhaustion = False
            error_msg = str(e).lower()
            if any(
                keyword in error_msg
                for keyword in ["pool", "connection pool", "max retries", "pool timeout"]
            ):
                is_pool_exhaustion = True
                logger.warning(
                    f"Connection pool exhaustion detected for {cdn_source.name}: {e}. "
                    f"Will retry with backoff."
                )

            logger.warning(f"CDN {cdn_source.name} failed: {type(e).__name__}: {e}")

            if not (hasattr(e, "response") and getattr(e.response, "status_code", None) == 404):
                cdn_manager.mark_failure(cdn_source.name)

            # For pool exhaustion, add immediate backoff
            if is_pool_exhaustion:
                pool_backoff_base = config.get("pool_exhaustion_backoff_base", 0.5)
                pool_backoff_max = config.get("pool_exhaustion_backoff_max", 5.0)
                pool_backoff_jitter = config.get("pool_exhaustion_backoff_jitter", 0.5)
                pool_backoff = min(pool_backoff_base * (2**attempt_num), pool_backoff_max)
                if use_jitter:
                    pool_backoff += random.uniform(0, pool_backoff_jitter)
                logger.debug(f"Pool exhaustion backoff: {pool_backoff:.2f}s")
                await asyncio.sleep(pool_backoff)

            return None, e

    last_error = None

    for attempt in range(max_retries):
        should_proceed, _cb_state = circuit_breaker.check_and_update_state()
        if not should_proceed:
            raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None)

        cdn_sources = cdn_manager.get_sources()
        if not cdn_sources:
            raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None)

        had_retryable_error = False
        for cdn_source in cdn_sources:
            data, error = await _try_cdn(cdn_source, attempt)
            if data is not None:
                return data
            if isinstance(error, DataNotFoundError | InvalidDataError):
                # Fatal error - raise immediately
                raise error
            if error is not None:
                last_error = error
                had_retryable_error = True

        if had_retryable_error:
            circuit_breaker.record_failure()

        # All CDNs failed, retry with backoff
        if attempt < max_retries - 1:
            delay = min(backoff_factor**attempt, max_delay)
            if use_jitter:
                jitter_max = config.get("retry_jitter_max", 1.0)
                delay += random.uniform(0, jitter_max)
            logger.warning(f"All CDNs failed, retry {attempt + 1}/{max_retries} after {delay:.2f}s")
            await asyncio.sleep(delay)

    raise NetworkError(
        url=f"{year}/{gp}/{session}/{path}",
        status_code=getattr(getattr(last_error, "response", None), "status_code", None),
    )


async def fetch_multiple_async(
    requests: list[tuple[int, str, str, str]],
    *,
    use_cache: bool = True,
    write_cache: bool = True,
    validate_payload: bool = True,
    max_retries: int | None = None,
    timeout: int | None = None,
    max_concurrent_requests: int | None = None,
) -> list[dict[str, Any] | None]:
    """Fetch multiple JSON files in parallel with optimized batch size.

    Args:
        requests: List of (year, gp, session, path) tuples
        use_cache: If True, read from cache before network fetch
        write_cache: If True, persist successful network responses to cache
        validate_payload: If True, run payload validation before returning data

    Returns:
        List of fetched data dictionaries (None for failed requests).
        Exceptions are logged and converted to None for graceful degradation.
        DataNotFoundError (404) is silently converted to None.
    """
    from .config import get_config

    config = get_config()
    configured_concurrent = config.get("max_concurrent_requests", 20)
    max_concurrent = max(
        1,
        configured_concurrent if max_concurrent_requests is None else max_concurrent_requests,
    )
    if not requests:
        return []

    if max_concurrent >= len(requests):
        results = await asyncio.gather(
            *(
                fetch_json_async(
                    *req,
                    use_cache=use_cache,
                    write_cache=write_cache,
                    validate_payload=validate_payload,
                    max_retries=max_retries,
                    timeout=timeout,
                )
                for req in requests
            ),
            return_exceptions=True,
        )
    else:
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(req):
            async with semaphore:
                return await fetch_json_async(
                    *req,
                    use_cache=use_cache,
                    write_cache=write_cache,
                    validate_payload=validate_payload,
                    max_retries=max_retries,
                    timeout=timeout,
                )

        tasks = [fetch_with_semaphore(req) for req in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    processed = []
    for req, result in zip(requests, results):
        if isinstance(result, Exception):
            if not isinstance(result, DataNotFoundError):
                logger.warning(f"Failed to fetch {req}: {type(result).__name__}: {result}")
            processed.append(None)
        else:
            processed.append(result)

    return processed
