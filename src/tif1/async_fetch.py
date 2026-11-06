"""Async functionality for parallel data fetching."""

import atexit
import logging
import random
import sys
import threading
from contextvars import ContextVar
from functools import partial
from typing import TYPE_CHECKING, Any

from . import cache as _cache
from . import first_wave
from .core_utils import json_utils
from .core_utils.constants import (
    COLUMNAR_FORMAT_VERSION,
    columnar_chunk_first_lap,
    columnar_file_path,
    telemetry_ref_from_path,
)
from .core_utils.json_utils import decode_telemetry_payload, json_loads, parse_response_json
from .core_utils.runtime import IS_EMSCRIPTEN, THREADS_AVAILABLE
from .exceptions import DataNotFoundError, InvalidDataError, NetworkError
from .http_session import close_session as close_http_session
from .http_session import get_session as get_http_session
from .http_session import wait_for_warm_connections

if TYPE_CHECKING:
    import asyncio

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
        if (path == "rcm.json" or path.endswith("/rcm.json")) and config.get("validate_data", True):
            from .validation import validate_race_control_data

            # normalize=False: the default no-validation path never scanned
            # element-wise; table load still swaps the token "None".
            return validate_race_control_data(data, strict=False, normalize=False)

        if (path == "weather.json" or path.endswith("/weather.json")) and config.get(
            "validate_data", True
        ):
            from .validation import validate_weather_data

            # Weather payloads are tiny; keep inline "None"-string coercion
            # (contract: test_coverage_core2 weather_json_none_strings_coerced).
            return validate_weather_data(data, strict=False)

        if (path == "session_laptimes.json" or path.endswith("/laptimes.json")) and config.get(
            "validate_lap_times", True
        ):
            from .validation import validate_lap_data

            return validate_lap_data(data, strict=False, normalize=False)

        if path.endswith("_tel.json") and config.get("validate_telemetry", True):
            from .validation import validate_telemetry_data

            if "tel" in data and isinstance(data["tel"], dict):
                validated = validate_telemetry_data(data["tel"], strict=False, normalize=False)
                validated = _sanitize_telemetry_payload(validated)
                merged = dict(data)
                merged["tel"] = validated
                return merged

            validated = validate_telemetry_data(data, strict=False, normalize=False)
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
    if not THREADS_AVAILABLE:
        # Pyodide: no worker threads; callers pass ``None`` to
        # ``run_in_executor`` and the WebLoop runs the function inline.
        return None

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


def _get_resolved(session_client: Any, url: str, timeout: Any) -> Any:
    """GET ``url`` and resolve the response inside the worker thread (Argos A12).

    With ``multiplexed=True`` niquests returns a *lazy* response: ``get()``
    comes back in ~1 ms and the first attribute access (``status_code``)
    blocks until the body has arrived. That access used to happen on the
    event-loop thread, so every in-flight response was waited for serially
    on the loop (instrumented: 1461 resolutions, 9.7 s of a 10.2 s batch).
    Touching ``status_code`` here moves the wait into the worker thread.
    The first requests of a cold load wait for the pre-connect's warm
    connections (bounded) rather than opening their own.
    """
    wait_for_warm_connections()
    response = session_client.get(url, timeout=timeout)
    _ = getattr(response, "status_code", None)
    return response


def _first_wave_payload(url: str) -> tuple[dict[str, Any], bytes] | None:
    """``(parsed, raw)`` of the stdlib first-wave body of ``url`` (Elis E2), or None.

    Waits (bounded) for the fetch ``get_session`` started. A body that does
    not parse to a JSON object is not handed over: niquests fetches it again
    and the pipeline reports what it finds.
    """
    content = first_wave.take(url)
    if content is None:
        return None
    try:
        data = json_loads(content)
    except (ValueError, TypeError):
        return None
    return (data, content) if isinstance(data, dict) else None


def _stdlib_payload(
    url: str, timeout: float, telemetry: bool
) -> tuple[dict[str, Any], bytes] | None:
    """``(parsed, raw)`` of ``url`` over the first wave's stdlib connections, or None.

    The bridge of a telemetry wave that starts before niquests is warm (Elis
    E9). Parsed exactly as the niquests path parses (``telemetry``: the typed
    decoder). None, and niquests fetches the file, for anything but a
    ``200`` with a JSON object.
    """
    content = first_wave.fetch(url, timeout=timeout)
    if content is None:
        return None
    try:
        data = decode_telemetry_payload(content) if telemetry else json_loads(content)
    except (ValueError, TypeError):
        return None
    return (data, content) if isinstance(data, dict) else None


async def _stdlib_served(
    loop: Any, executor: Any, url: str, timeout: float, telemetry: bool
) -> tuple[dict[str, Any], bytes] | None:
    """A payload the stdlib connections serve (Elis E2 first wave, E9 bridge), or None.

    A first-wave URL is handed over from its slot (and never fetched again
    over the stdlib when that failed); any other URL rides the stdlib
    connections while niquests is not warm (:func:`tif1.first_wave.bridge_open`,
    which starts the niquests warm-up only past one driver's wave of requests).
    Bridge requests are bounded by ``cdn_hedge_max_delay``: they are not
    hedged, so a stalled one goes to niquests (which hedges) after that.
    """
    if first_wave.pending(url):
        return await loop.run_in_executor(executor, _first_wave_payload, url)
    if not first_wave.bridge_open():
        return None
    from .config import get_config

    bound = min(float(timeout), float(get_config().get("cdn_hedge_max_delay", 4.0)))
    return await loop.run_in_executor(executor, _stdlib_payload, url, bound, telemetry)


def _parse_json_payload_bytes(payload: bytes | bytearray | memoryview) -> Any:
    """Parse raw JSON bytes in a worker process."""
    return json_loads(payload)


def _response_bytes(response: Any) -> Any:
    """Access the lazy response body off the event loop."""
    return getattr(response, "content", None)


def _decode_and_parse(response: Any, *, telemetry: bool = False) -> tuple[Any, Any]:
    """Decode the lazy response body and parse JSON off the event loop.

    Returns ``(parsed, raw_content)`` so the caller can persist the original
    bytes without re-touching the lazy attribute on the event loop.
    ``telemetry`` payloads go through :func:`decode_telemetry_payload` (the
    native decoder when installed, which releases the GIL so the worker
    threads parse concurrently).
    """
    content = getattr(response, "content", None)
    if isinstance(content, bytes | bytearray | memoryview):
        parsed = decode_telemetry_payload(content) if telemetry else json_loads(content)
        return parsed, content
    return parse_response_json(response), None


def _get_json_parse_executor() -> Any | None:
    """Get or create shared process pool for JSON parsing."""
    global _json_parse_executor
    if _json_parse_executor is not None:
        return _json_parse_executor
    if not THREADS_AVAILABLE:
        return None

    with _json_parse_executor_lock:
        if _json_parse_executor is not None:
            return _json_parse_executor

        from .config import get_config

        config = get_config()
        worker_count = int(config.get("json_parse_workers", 0))
        if worker_count <= 0:
            return None

        ProcessPoolExecutor = _import_process_executor()
        if sys.platform == "win32":
            # WaitForMultipleObjects limit; a larger value raises ValueError.
            # Windows also starts workers with spawn: a script using this
            # opt-in pool needs an ``if __name__ == "__main__":`` guard.
            worker_count = min(worker_count, 61)
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


def _get_async_transport() -> Any:
    """The async-capable transport of browser runtimes (``tif1.browser``).

    Resolved through the shared URL loader, so a test (or caller) that swaps
    ``get_url_loader().transport`` for one with ``get_json_async`` steers the
    async fan-out as well as the sync fetches.
    """
    from .payload_loader import get_url_loader

    transport = get_url_loader().transport
    if hasattr(transport, "get_json_async"):
        return transport
    from .browser import get_pyodide_transport

    return get_pyodide_transport()


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
            from . import http_session as _http_session

            if session_to_close is _http_session._shared_session:
                # The shared singleton: close it through its owner, so the next
                # fetch builds a fresh session instead of reusing a closed one.
                close_http_session()
            elif hasattr(session_to_close, "close"):
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


# Deferred telemetry cache writes: bulk telemetry batches collect the
# (year, gp, session, driver, lap, tel_payload) tuples during the fetch and
# persist them outside the semaphore slots. Writing inside the slots
# serializes 22 concurrent slots on the SQLite lock and adds seconds to cold
# batches on small machines (measured, see .agents/perf-kalchedon/RESULTS.md);
# a batch's collector is either a list flushed after the gather or a
# _StreamedTelemetryWrites whose writer thread persists rows as they arrive.

# Seconds a streamed-writes thread waits for the next row before it exits
# (the next row starts a new one).
_STREAMED_WRITES_IDLE_S = 0.5


class _StreamedTelemetryWrites:
    """A batch's deferred telemetry writes, persisted while the batch still fetches.

    Naxos N9: a cold bulk wave is bound by the link (~10 MB/s here) and
    leaves the GIL mostly idle, while the payload-tier flush (orjson dumps,
    zstd, SQLite: ~1.35 s for a race's 1128 laps) used to start only after
    the gather, where it fought the GIL-bound frame build for the same core
    (the build took ~1.9 s instead of ~0.75 s). Rows queued here are written
    by a writer thread during the wave instead, so only the rows of the last
    few responses remain after the gather.

    The writer is registered like the frame-tier writers (joined by
    ``Cache.close()``/``invalidate()`` and at exit). It exits after
    ``_STREAMED_WRITES_IDLE_S`` without a row or once :meth:`close` is called
    and the queue is empty, so an unclosed batch cannot hold the exit join;
    a later row starts a new writer.
    """

    __slots__ = ("_cache", "_closed", "_cond", "_pending", "_running")

    def __init__(self, cache: Any) -> None:
        self._cache = cache
        self._cond = threading.Condition(threading.Lock())
        self._pending: list[tuple[Any, ...]] = []
        self._running = False
        self._closed = False

    def append(self, entry: tuple[Any, ...]) -> None:
        """Queue one ``(year, gp, session, driver, lap, tel_payload)`` row."""
        with self._cond:
            self._pending.append(entry)
            if self._running:
                self._cond.notify()
                return
            self._running = True
        from .cache import register_frame_writer

        register_frame_writer(self._drain)

    def close(self) -> None:
        """No more rows: the writer exits once the queue is empty."""
        with self._cond:
            self._closed = True
            self._cond.notify()

    def _drain(self) -> None:
        set_telemetry = self._cache.set_telemetry
        while True:
            with self._cond:
                if not self._pending and not self._closed:
                    self._cond.wait(_STREAMED_WRITES_IDLE_S)
                if not self._pending:
                    self._running = False
                    return
                batch, self._pending = self._pending, []
            for year, gp, session, driver, lap, tel_payload in batch:
                try:
                    set_telemetry(year, gp, session, driver, lap, tel_payload)
                except (RuntimeError, TypeError, ValueError) as e:
                    logger.debug("Deferred telemetry cache write skipped: %s", e)


_TelemetryWriteCollector = list[tuple[Any, ...]] | _StreamedTelemetryWrites
_deferred_telemetry_writes: ContextVar[_TelemetryWriteCollector | None] = ContextVar(
    "tif1_deferred_telemetry_writes", default=None
)


def _flush_telemetry_writes(cache: Any, entries: list[tuple[Any, ...]]) -> None:
    """Drain deferred telemetry writes in one executor job (serial)."""
    set_telemetry = getattr(cache, "set_telemetry", None)
    if not callable(set_telemetry):
        return
    # ~1400 serial dumps+compress+SQL iterations allocate heavily; suspending
    # GC for the scoped loop is the same mechanism as the frame-tier read
    # (Rhodes R3) and restores the prior state afterwards.
    from .cache import _suspend_gc

    with _suspend_gc():
        for year, gp, session, driver, lap, tel_payload in entries:
            try:
                set_telemetry(year, gp, session, driver, lap, tel_payload)
            except (RuntimeError, TypeError, ValueError) as e:
                logger.debug("Deferred telemetry cache write skipped: %s", e)


# -- per-driver columnar telemetry files (Gortyn G3) ------------------------------
#
# A cold wave of a driver's laps asks for the driver's columnar files
# (``{driver}/tel.v2.{first}.bin``, ten laps each,
# :mod:`tif1.core_utils.columnar_telemetry`) once; every lap request of the
# wave awaits the download of its file and takes its lap from it, or falls
# back to its ``_tel.json`` when the file is missing, unreadable or lacks the
# lap. Downloads run in the fetch thread pool, so lap requests of any event
# loop (a batch's ``asyncio.run``, the lazy stream's thread) share them
# through a ``concurrent.futures.Future``.

_driver_files_lock = threading.Lock()
# (year, gp, session, driver, first lap) -> Future of {lap: tel} (None: no usable file).
_driver_files: dict[tuple[Any, ...], Any] = {}
# Sessions whose file probe answered 404: the data repository has not
# published files for them, so no other file is asked for in this process.
_sessions_without_files: set[tuple[Any, ...]] = set()
# Decoded files kept for their unread laps, oldest dropped first.
_DRIVER_FILES_CAP = 256
# Missing-payload key of a session without columnar files (no such path is fetched).
COLUMNAR_SESSION_MARKER = f"tel.v{COLUMNAR_FORMAT_VERSION}.session"


def _columnar_enabled(config: Any, validate_payload: bool) -> bool:
    """Whether telemetry waves ask for the per-driver files.

    Off with ``telemetry_columnar=False``, when telemetry payloads are
    validated (the validator reads the JSON lists), in the browser (no
    thread pool), offline, and without tif1-native's ``tel.v2`` kernels
    (the numpy decoder is ~6x slower per file than the native one).
    """
    return (
        bool(config.get("telemetry_columnar", False))
        and not (validate_payload and config.get("validate_telemetry", True))
        and THREADS_AVAILABLE
        and not IS_EMSCRIPTEN
        and not bool(config.get("offline_mode", False))
        and json_utils.native_columnar_decoder_available()
    )


def _load_driver_file(
    year: int, gp: str, session: str, driver: str, first_lap: int, use_cache: bool
) -> dict[int, dict[str, Any]] | None:
    """Download and decode one columnar file; None when there is none to use.

    The primary CDN answers for the file: a 404 there means the data
    repository holds no such file (the mirrors copy the same repository) and
    is remembered in the missing-payload tier, so the per-lap JSON follows at
    the cost of that one request. Any other failure tries the next mirror;
    nothing usable from any of them falls back to the JSON without a verdict.
    """
    from .cdn import get_cdn_manager
    from .config import get_config
    from .core_utils.columnar_telemetry import ColumnarFormatError, decode_driver_file
    from .payload_loader import get_url_loader

    path = columnar_file_path(driver, first_lap)
    cache_key = f"{year}/{gp}/{session}/{path}"
    # The session-wide verdict: the repository publishes a session's files
    # together, so one 404 stands for all of them until the verdict expires.
    session_key = f"{year}/{gp}/{session}/{COLUMNAR_SESSION_MARKER}"
    cache = get_cache() if use_cache else None
    if cache is not None and (
        cache.is_known_missing(session_key) or cache.is_known_missing(cache_key)
    ):
        with _driver_files_lock:
            _sessions_without_files.add((year, gp, session))
        return None
    get_bytes = getattr(get_url_loader().transport, "get_bytes", None)
    if not callable(get_bytes):
        return None
    timeout = max(1, get_config().get("timeout", 30))
    for source in get_cdn_manager().get_sources():
        url = source.format_url(year, gp, session, path)
        try:
            blob = get_bytes(url, timeout=timeout)
        except DataNotFoundError:
            with _driver_files_lock:
                _sessions_without_files.add((year, gp, session))
            if cache is not None:
                cache.record_missing(cache_key)
                cache.record_missing(session_key)
            return None
        except (NetworkError, InvalidDataError, OSError, ValueError) as e:
            logger.debug("Columnar telemetry file %s failed on %s: %s", path, source.name, e)
            continue
        try:
            return decode_driver_file(blob)
        except ColumnarFormatError as e:
            logger.warning("Columnar telemetry file %s unreadable, using the JSON laps: %s", url, e)
            return None
    return None


def _driver_file_future(
    year: int, gp: str, session: str, driver: str, lap: int, *, start: bool, use_cache: bool
) -> Any | None:
    """The download of the file holding ``lap``: the running or finished one, else a new one.

    ``start`` False only joins a download a wave started (a lone lap never
    pays for ten). None when there is none to join.
    """
    first_lap = columnar_chunk_first_lap(lap)
    key = (year, gp, session, driver, first_lap)
    with _driver_files_lock:
        future = _driver_files.get(key)
        if future is not None or not start or (year, gp, session) in _sessions_without_files:
            return future
        future = _get_executor().submit(
            _load_driver_file, year, gp, session, driver, first_lap, use_cache
        )
        _driver_files[key] = future
        if len(_driver_files) > _DRIVER_FILES_CAP:
            done = [k for k, f in _driver_files.items() if f.done()]
            for old in done[: len(_driver_files) - _DRIVER_FILES_CAP]:
                del _driver_files[old]
    return future


async def _columnar_lap(
    year: int, gp: str, session: str, path: str, *, start: bool, use_cache: bool
) -> dict[str, Any] | None:
    """``{"tel": ...}`` of a lap from its columnar file, or None (use the JSON).

    A lap is handed out once (the decoded file only keeps the laps not read
    yet); the frame and payload tiers serve it afterwards.
    """
    import asyncio  # lazy: only async/network paths need it

    ref = telemetry_ref_from_path(path)
    if ref is None:
        return None
    driver, lap = ref
    future = _driver_file_future(year, gp, session, driver, lap, start=start, use_cache=use_cache)
    if future is None:
        return None
    try:
        laps = await asyncio.wrap_future(future)
    except Exception as e:  # the download thread failed: the JSON path takes over
        logger.debug("Columnar telemetry file of %s lap %s failed: %s", driver, lap, e)
        return None
    if not laps:
        return None
    tel = laps.pop(lap, None)
    return {"tel": tel} if tel else None


async def _persist_telemetry_payload(
    cache: Any,
    loop: Any,
    executor: Any,
    year: int,
    gp: str,
    session: str,
    path: str,
    tel_payload: dict[str, Any],
) -> bool:
    """Write a lap's ``tel`` payload to the telemetry tier; False when ``path`` is no lap.

    Telemetry payloads persist to the telemetry table only (what every
    batch/single-ref reader consults first). Inside a bulk batch with
    deferred writes the row is collected for one serial flush after the
    gather instead.
    """
    set_telemetry = getattr(cache, "set_telemetry", None)
    tel_ref = telemetry_ref_from_path(path)
    if tel_ref is None or not callable(set_telemetry):
        return False
    driver, lap = tel_ref
    collector = _deferred_telemetry_writes.get()
    if collector is not None:
        collector.append((year, gp, session, driver, lap, tel_payload))
    else:
        await loop.run_in_executor(
            executor, set_telemetry, year, gp, session, driver, lap, tel_payload
        )
    return True


def reset_driver_files() -> None:
    """Forget the downloaded columnar files and the sessions known to have none (tests)."""
    with _driver_files_lock:
        _driver_files.clear()
        _sessions_without_files.clear()


async def fetch_with_rate_limit(
    coro_func,
    *args,
    semaphore: "asyncio.Semaphore | None" = None,
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
    import asyncio  # lazy: only async/network paths need it

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
    driver_file: bool = False,
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
        driver_file: For a ``{driver}/{lap}_tel.json`` path: the lap belongs
            to a wave of its driver, so its columnar file may be downloaded
            (once for the wave) instead of the lap's JSON. False still takes
            the lap from a file a wave already downloaded.

    Returns:
        Parsed JSON data (never None, raises on error)

    Raises:
        NetworkError: If network request fails after retries
        DataNotFoundError: If data doesn't exist
        InvalidDataError: If data is corrupted
    """
    import asyncio  # lazy: only async/network paths need it

    from .config import get_config

    config = get_config()
    if bool(config.get("ci_mode", False)):
        use_cache = False
        write_cache = False

    cache_key = f"{year}/{gp}/{session}/{path}"
    cache = get_cache() if (use_cache or write_cache) else None

    loop = asyncio.get_running_loop()
    # Pyodide has no thread pool: ``run_in_executor(None, ...)`` on its
    # WebLoop runs the function inline, so every offload below still works.
    executor = None if IS_EMSCRIPTEN else _get_executor()

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

        # Everywhere-missing verdict (all CDNs refused with 4xx recently):
        # skip the network walk entirely until the verdict expires.
        is_known_missing = getattr(cache, "is_known_missing", None)
        if callable(is_known_missing):
            known_missing = await loop.run_in_executor(executor, is_known_missing, cache_key)
            if known_missing:
                raise DataNotFoundError(year=year, event=gp, session=session)

    if bool(config.get("offline_mode", False)):
        raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None)

    if path.endswith("_tel.json") and _columnar_enabled(config, validate_payload):
        columnar = await _columnar_lap(
            year, gp, session, path, start=driver_file, use_cache=use_cache
        )
        if columnar is not None:
            if write_cache and cache is not None:
                await _persist_telemetry_payload(
                    cache, loop, executor, year, gp, session, path, columnar["tel"]
                )
                cache.discard_missing(cache_key)
            return columnar

    from .cdn import get_cdn_manager
    from .retry import get_circuit_breaker

    if max_retries is None:
        max_retries = max(0, config.get("max_retries", 3))
    if timeout is None:
        timeout = max(1, config.get("timeout", 30))

    cdn_manager = get_cdn_manager()
    circuit_breaker = get_circuit_breaker()
    # The niquests client is resolved on the first request that needs it:
    # a cold load's first wave may be served by the stdlib fetch started in
    # get_session (Elis E2), and then niquests is not imported for it.
    niquests_client: list[Any] = []
    transport_errors: tuple[type[BaseException], ...] = (
        TimeoutError,
        OSError,
        ConnectionError,
    )
    if IS_EMSCRIPTEN:
        # Browser: the fetch API through the async transport (tif1.browser).
        # niquests and the thread pool are never imported here.
        transport = _get_async_transport()
        transport_errors = (NetworkError, *transport_errors)
    else:
        transport = None

    def _niquests_session() -> Any:
        nonlocal transport_errors
        if not niquests_client:
            niquests = _import_niquests()
            niquests_client.append(_get_async_session())
            transport_errors = (
                niquests.RequestException,
                niquests.exceptions.HTTPError,
                *transport_errors,
            )
        return niquests_client[0]

    backoff_factor = config.get("retry_backoff_factor", 2.0)
    use_jitter = config.get("retry_jitter", True)
    max_delay = config.get("max_retry_delay", 60.0)

    async def _fetch_from_source(cdn_source, url: str, attempt_num: int) -> dict[str, Any]:
        """Fetch, parse, validate, and cache one payload from a single CDN URL.

        Implements the per-source work for the CDN fallback loop owned by
        :meth:`tif1.cdn.CDNManager.try_sources_async`: 404s
        (DataNotFoundError) and transport errors fall through to the next
        source, while validation errors (InvalidDataError) abort the loop.
        Retry/backoff policy across attempts stays here (circuit breaker,
        pool-exhaustion backoff).
        """
        import asyncio  # lazy: only async/network paths need it

        try:
            if transport is not None:
                # Browser fetch: the transport maps 404 -> DataNotFoundError,
                # other failures -> NetworkError (status_code set for HTTP
                # errors, so the CDN manager still recognises refusals).
                fetch_bytes = getattr(transport, "get_bytes_async", None)
                try:
                    if fetch_bytes is not None:
                        content = await fetch_bytes(url, timeout=timeout)
                        typed_decode = path.endswith("_tel.json") and not (
                            validate_payload and config.get("validate_telemetry", True)
                        )
                        try:
                            data = (
                                decode_telemetry_payload(content)
                                if typed_decode
                                else json_loads(content)
                            )
                        except (ValueError, TypeError) as e:
                            raise InvalidDataError(reason=f"JSON parsing failed: {e}")
                    else:
                        data = await transport.get_json_async(url, timeout=timeout)
                        content = None
                except DataNotFoundError as e:
                    raise DataNotFoundError(year=year, event=gp, session=session) from e
            elif not niquests_client and (
                served := await _stdlib_served(
                    loop,
                    executor,
                    url,
                    timeout,
                    path.endswith("_tel.json")
                    and not (validate_payload and config.get("validate_telemetry", True)),
                )
            ):
                data, content = served
            else:
                response = await loop.run_in_executor(
                    executor, partial(_get_resolved, _niquests_session(), url, timeout)
                )

                from .http_session import _response_reused_connection, _track_request

                _track_request(reused=_response_reused_connection(response))

                if response.status_code == 404:
                    raise DataNotFoundError(year=year, event=gp, session=session)

                response.raise_for_status()

                try:
                    json_parse_executor = _get_json_parse_executor()
                    is_telemetry_payload = path.endswith("_tel.json")
                    if json_parse_executor and not is_telemetry_payload:
                        # Opt-in process-pool parse: decode bytes in a worker
                        # thread, then parse in the process pool.
                        content = await loop.run_in_executor(executor, _response_bytes, response)
                        if isinstance(content, bytes | bytearray | memoryview):
                            data = await loop.run_in_executor(
                                json_parse_executor, _parse_json_payload_bytes, bytes(content)
                            )
                        else:
                            content = None
                            data = await loop.run_in_executor(
                                executor, parse_response_json, response
                            )
                    else:
                        # niquests' lazy content decode plus orjson parse cost
                        # milliseconds per response; paying that on the event loop
                        # serializes the whole batch. Decode + parse in a worker.
                        # The typed decoder only when the pydantic-free telemetry
                        # validator (list-shaped) will not run on the payload.
                        typed_decode = is_telemetry_payload and not (
                            validate_payload and config.get("validate_telemetry", True)
                        )
                        data, content = await loop.run_in_executor(
                            executor, partial(_decode_and_parse, response, telemetry=typed_decode)
                        )
                except (ValueError, TypeError, AttributeError) as e:
                    raise InvalidDataError(reason=f"JSON parsing failed: {e}")

            if not isinstance(data, dict):
                raise InvalidDataError(reason=f"Expected dict, got {type(data).__name__}")

            untransformed = True
            if validate_payload:
                validated = _validate_json_payload(path, data, config)
                untransformed = validated is data
                data = validated

            if write_cache and cache is not None:
                tel_payload = data.get("tel") if isinstance(data, dict) else None
                if not (
                    isinstance(tel_payload, dict)
                    and tel_payload
                    and await _persist_telemetry_payload(
                        cache, loop, executor, year, gp, session, path, tel_payload
                    )
                ):
                    set_raw = getattr(cache, "set_raw", None)
                    if (
                        untransformed
                        and callable(set_raw)
                        and isinstance(content, bytes | bytearray | memoryview)
                    ):
                        # Payload is byte-identical to what was parsed: persist the
                        # original blob and skip re-serialization.
                        await loop.run_in_executor(executor, set_raw, cache_key, bytes(content))
                    else:
                        await loop.run_in_executor(executor, cache.set, cache_key, data)

            circuit_breaker.record_success()

            # The payload exists: drop any stale everywhere-missing verdict.
            discard_missing = getattr(cache, "discard_missing", None)
            if callable(discard_missing):
                discard_missing(cache_key)

            logger.debug(f"Fetched: {cache_key} from {cdn_source.name}")
            return data
        except (DataNotFoundError, InvalidDataError):
            # Fatal errors - no fallback, no retry.
            raise
        except transport_errors as e:
            if (
                type(e).__name__ == "MultiplexingError"
                and "non-multiplexed response after a redirect" in str(e)
            ):
                cdn_source.enabled = False
                logger.warning(
                    "Disabling CDN %s after multiplexing redirect incompatibility",
                    cdn_source.name,
                )
                raise

            # Connection pool exhaustion gets an immediate per-attempt backoff.
            error_msg = str(e).lower()
            if any(
                keyword in error_msg
                for keyword in ["pool", "connection pool", "max retries", "pool timeout"]
            ):
                logger.warning(
                    f"Connection pool exhaustion detected for {cdn_source.name}: {e}. "
                    f"Will retry with backoff."
                )
                pool_backoff_base = config.get("pool_exhaustion_backoff_base", 0.01)
                pool_backoff_max = config.get("pool_exhaustion_backoff_max", 0.5)
                pool_backoff_jitter = config.get("pool_exhaustion_backoff_jitter", 0.01)
                pool_backoff = min(pool_backoff_base * (2**attempt_num), pool_backoff_max)
                if use_jitter:
                    pool_backoff += random.uniform(0, pool_backoff_jitter)
                logger.debug(f"Pool exhaustion backoff: {pool_backoff:.2f}s")
                await asyncio.sleep(pool_backoff)

            raise

    # Zero-retry (ultra-cold) mode is a single attempt through the same loop.
    attempts = max(1, max_retries)
    jitter_max = config.get("retry_jitter_max", 1.0)
    last_error: NetworkError | None = None

    for attempt in range(attempts):
        should_proceed, _cb_state = circuit_breaker.check_and_update_state()
        if not should_proceed:
            raise NetworkError(url=f"{year}/{gp}/{session}/{path}", status_code=None)

        try:
            return await cdn_manager.try_sources_async(
                year,
                gp,
                session,
                path,
                partial(_fetch_from_source, attempt_num=attempt),
            )
        except DataNotFoundError:
            # Every CDN source refused with a 4xx (all-missing): remember the
            # verdict so later loads skip the CDN walk until the TTL expires.
            record_missing = getattr(cache, "record_missing", None)
            if callable(record_missing):
                record_missing(cache_key)
            raise
        except InvalidDataError:
            # Fatal error - raise immediately
            raise
        except NetworkError as e:
            # Every CDN source failed this attempt; back off and retry.
            last_error = e
            circuit_breaker.record_failure()

        if attempt < attempts - 1:
            delay = min(backoff_factor**attempt, max_delay)
            if use_jitter:
                delay += random.uniform(0, jitter_max)
            logger.warning(f"All CDNs failed, retry {attempt + 1}/{attempts} after {delay:.2f}s")
            await asyncio.sleep(delay)

    raise NetworkError(
        url=f"{year}/{gp}/{session}/{path}",
        status_code=getattr(last_error, "status_code", None),
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
    defer_telemetry_writes: bool = False,
    telemetry_writes: list[tuple[Any, ...]] | None = None,
    driver_files: bool | None = None,
) -> list[dict[str, Any] | None]:
    """Fetch multiple JSON files in parallel with optimized batch size.

    Args:
        requests: List of (year, gp, session, path) tuples
        use_cache: If True, read from cache before network fetch
        write_cache: If True, persist successful network responses to cache
        validate_payload: If True, run payload validation before returning data
        defer_telemetry_writes: If True, telemetry payloads are written
            outside the concurrency slots: by a writer thread while the batch
            fetches (one serial flush after the gather without threads)
        telemetry_writes: A caller-owned list that collects the telemetry
            payload writes instead (with ``write_cache``); the caller hands
            it to :func:`schedule_telemetry_writes` when it sees fit (the
            lazy-telemetry stream issues one request per call)
        driver_files: Whether the batch's telemetry laps may come from their
            driver's columnar file (``telemetry_columnar``). None: for the
            drivers with two or more laps in the batch (a lone lap keeps its
            JSON); True: every lap (the lazy stream's one-lap calls belong to
            driver waves)

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

    collector: list[tuple[Any, ...]] | None = None
    streamed: _StreamedTelemetryWrites | None = None
    collector_token = None
    if telemetry_writes is not None and write_cache:
        collector_token = _deferred_telemetry_writes.set(telemetry_writes)
    elif defer_telemetry_writes and write_cache:
        cache_for_defer = get_cache() if (use_cache or write_cache) else None
        if cache_for_defer is not None and callable(
            getattr(cache_for_defer, "set_telemetry", None)
        ):
            if THREADS_AVAILABLE:
                # Naxos N9: rows are persisted during the wave.
                streamed = _StreamedTelemetryWrites(cache_for_defer)
                collector_token = _deferred_telemetry_writes.set(streamed)
            else:
                # No threads (Pyodide): one inline flush after the gather.
                collector = []
                collector_token = _deferred_telemetry_writes.set(collector)

    try:
        results = await _gather_fetches(
            requests,
            use_cache=use_cache,
            write_cache=write_cache,
            validate_payload=validate_payload,
            max_retries=max_retries,
            timeout=timeout,
            max_concurrent=max_concurrent,
            wave_flags=_driver_wave_flags(requests, driver_files),
        )
    finally:
        if collector_token is not None:
            _deferred_telemetry_writes.reset(collector_token)
        if streamed is not None:
            streamed.close()

    if collector:
        schedule_telemetry_writes(collector)

    return _process_fetch_results(requests, results)


def schedule_telemetry_writes(entries: list[tuple[Any, ...]]) -> None:
    """Persist collected telemetry payload writes in a background writer thread.

    Argos A3: the flush (orjson dumps + zstd + SQLite for ~1450 rows,
    ~0.4-0.8 s) only matters to the *next* process, so it runs in a
    background writer thread after the batch returns - the same registry the
    frame-tier write uses (Ithome U5), joined by Cache.close()/invalidate()/
    clear() and the atexit cache close, so every row is on disk before the
    process exits.
    """
    if not entries:
        return
    from .cache import register_frame_writer

    register_frame_writer(partial(_flush_telemetry_writes, get_cache(), entries))


def _driver_wave_flags(
    requests: list[tuple[int, str, str, str]], driver_files: bool | None
) -> list[bool] | None:
    """Per request, whether it may start its driver's columnar file (None: no request may)."""
    if driver_files is False:
        return None
    refs = [
        telemetry_ref_from_path(path) if path.endswith("_tel.json") else None
        for _, _, _, path in requests
    ]
    if driver_files:
        flags = [ref is not None for ref in refs]
    else:
        counts: dict[tuple[Any, ...], int] = {}
        keys = [
            (year, gp, session, ref[0]) if ref is not None else None
            for (year, gp, session, _), ref in zip(requests, refs, strict=True)
        ]
        for key in keys:
            if key is not None:
                counts[key] = counts.get(key, 0) + 1
        flags = [key is not None and counts[key] > 1 for key in keys]
    return flags if any(flags) else None


async def _gather_fetches(
    requests: list[tuple[int, str, str, str]],
    *,
    use_cache: bool,
    write_cache: bool,
    validate_payload: bool,
    max_retries: int | None,
    timeout: int | None,
    max_concurrent: int,
    wave_flags: list[bool] | None = None,
) -> list[Any]:
    """Gather per-request fetches under the concurrency cap."""
    import asyncio  # lazy: only async/network paths need it

    flags = wave_flags or [False] * len(requests)
    if max_concurrent < len(requests):
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(req, driver_file):
            async with semaphore:
                return await fetch_json_async(
                    *req,
                    use_cache=use_cache,
                    write_cache=write_cache,
                    validate_payload=validate_payload,
                    max_retries=max_retries,
                    timeout=timeout,
                    driver_file=driver_file,
                )

        if wave_flags is not None:
            _start_wave_driver_files(requests, wave_flags, use_cache, validate_payload)
        tasks = [
            fetch_with_semaphore(req, driver_file)
            for req, driver_file in zip(requests, flags, strict=True)
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)

    return await asyncio.gather(
        *(
            fetch_json_async(
                *req,
                use_cache=use_cache,
                write_cache=write_cache,
                validate_payload=validate_payload,
                max_retries=max_retries,
                timeout=timeout,
                driver_file=driver_file,
            )
            for req, driver_file in zip(requests, flags, strict=True)
        ),
        return_exceptions=True,
    )


def _start_wave_driver_files(
    requests: list[tuple[int, str, str, str]],
    wave_flags: list[bool],
    use_cache: bool,
    validate_payload: bool,
) -> None:
    """Start the wave's file downloads before the lap requests queue up.

    A batch larger than the concurrency cap (``fetch_all``: 20 drivers'
    laps behind 22 slots) would otherwise start a file only when its first
    lap gets a slot, i.e. a few files at a time.
    """
    from .config import get_config

    if not _columnar_enabled(get_config(), validate_payload):
        return
    started: set[tuple[Any, ...]] = set()
    for (year, gp, session, path), flag in zip(requests, wave_flags, strict=True):
        ref = telemetry_ref_from_path(path) if flag else None
        if ref is None:
            continue
        key = (year, gp, session, ref[0], columnar_chunk_first_lap(ref[1]))
        if key not in started:
            started.add(key)
            _driver_file_future(year, gp, session, *ref, start=True, use_cache=use_cache)


def _process_fetch_results(
    requests: list[tuple[int, str, str, str]], results: list[Any]
) -> list[dict[str, Any] | None]:
    """Convert gather results to the graceful-degradation list."""
    processed = []
    for req, result in zip(requests, results):
        if isinstance(result, Exception):
            if not isinstance(result, DataNotFoundError):
                logger.warning(f"Failed to fetch {req}: {type(result).__name__}: {result}")
            processed.append(None)
        elif isinstance(result, BaseException):
            # Cancellation and process-level control exceptions are not request
            # failures. Never turn them into a successful-looking None result.
            raise result
        else:
            processed.append(result)

    return processed
