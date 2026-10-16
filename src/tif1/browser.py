"""Browser (Pyodide) support: HTTP through the browser, cache persistence.

Inside Pyodide there are no sockets, so :class:`PyodideTransport` fetches
payloads through the browser instead of niquests:

- :meth:`PyodideTransport.get_json` (synchronous, the :class:`~tif1.payload_loader.HttpTransport`
  seam) uses the ``urllib3`` build that ships with Pyodide. Its Emscripten
  backend issues ``fetch`` when JavaScript Promise Integration is active and a
  synchronous ``XMLHttpRequest`` otherwise, so it works on the main thread and
  in Web Workers in every browser.
- :meth:`PyodideTransport.get_bytes_async` / :meth:`PyodideTransport.get_json_async`
  use ``pyodide.http.pyfetch``; ``tif1.async_fetch`` runs every bulk load
  through them, so one ``await session.load_async()`` fetches the whole session
  concurrently.

The in-memory Emscripten file system is lost on reload. :func:`enable_persistent_cache`
mounts the cache directory on IndexedDB and :func:`persist_cache` writes it back,
so the SQLite cache survives page reloads.

Every ``pyodide``/``js``/``urllib3`` import is local to the function that
needs it: the module imports on any platform (tests inject fakes) and the
browser runtime is only touched when a method runs.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from .core_utils.json_utils import json_loads
from .core_utils.runtime import is_pyodide
from .exceptions import DataNotFoundError, InvalidDataError, NetworkError, TIF1Error

logger = logging.getLogger(__name__)

_ACCEPT_JSON = {"Accept": "application/json"}


def _request_timeout(timeout: float | None) -> float:
    """Seconds to wait for one request (config ``timeout`` when not given)."""
    if timeout is None:
        from .config import get_config

        timeout = get_config().get("timeout", 30)
    return max(1.0, float(timeout))


def _payload_from(url: str, status: int, body: Any) -> dict[str, Any]:
    """Map an HTTP status and body to the transport contract."""
    if status == 404:
        raise DataNotFoundError(url=url)
    if status >= 400:
        raise NetworkError(url=url, status_code=status)
    try:
        data = json_loads(body)
    except (TypeError, ValueError) as e:
        raise InvalidDataError(reason=f"JSON parsing failed: {e}") from e
    if not isinstance(data, dict):
        raise InvalidDataError(reason=f"Expected dict, got {type(data).__name__}")
    return data


def _abort_signal(seconds: float) -> Any | None:
    """``AbortSignal.timeout`` for ``fetch`` when the runtime provides it."""
    try:
        import js  # ty: ignore[unresolved-import]
    except ImportError:
        return None
    factory = getattr(getattr(js, "AbortSignal", None), "timeout", None)
    if factory is None:
        return None
    return factory(int(seconds * 1000))


def _fetch_error_types() -> tuple[type[BaseException], ...]:
    """Exception types a browser ``fetch`` raises (network failure, abort)."""
    types: list[type[BaseException]] = [OSError]
    try:
        from pyodide.ffi import JsException  # ty: ignore[unresolved-import]

        types.append(JsException)
    except ImportError:
        pass
    try:
        from pyodide.http import AbortError  # ty: ignore[unresolved-import]

        types.append(AbortError)
    except ImportError:
        pass
    return tuple(types)


class PyodideTransport:
    """HTTP transport for Pyodide: ``urllib3`` for sync calls, ``pyfetch`` for async.

    Satisfies :class:`tif1.payload_loader.HttpTransport` and
    :class:`tif1.payload_loader.AsyncHttpTransport`. Retries stay with the
    pipeline (``retry_with_backoff`` / the async retry loop), so the pool
    manager runs with ``retries=False``; redirects, decompression and TLS are
    the browser's.
    """

    def __init__(self) -> None:
        self._pool: Any = None

    def _pool_manager(self) -> Any:
        if self._pool is None:
            import urllib3

            self._pool = urllib3.PoolManager(retries=False)
        return self._pool

    def get_json(self, url: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Fetch ``url`` synchronously and return the parsed JSON object."""
        import urllib3

        seconds = _request_timeout(timeout)
        try:
            response = self._pool_manager().request(
                "GET", url, headers=_ACCEPT_JSON, timeout=seconds, preload_content=True
            )
        except (urllib3.exceptions.HTTPError, OSError) as e:
            raise NetworkError(url=url, status_code=None) from e
        return _payload_from(url, int(response.status), response.data)

    async def get_bytes_async(self, url: str, *, timeout: float | None = None) -> bytes:
        """Fetch ``url`` with the browser ``fetch`` API and return the body bytes.

        Raises:
            DataNotFoundError: HTTP 404.
            NetworkError: Any other HTTP error status, a network failure or an abort.
        """
        from pyodide.http import pyfetch  # ty: ignore[unresolved-import]

        seconds = _request_timeout(timeout)
        options: dict[str, Any] = {"method": "GET", "headers": _ACCEPT_JSON}
        signal = _abort_signal(seconds)
        if signal is not None:
            options["signal"] = signal
        try:
            response = await pyfetch(url, **options)
            status = int(response.status)
            body = await response.bytes() if status < 400 else b""
        except _fetch_error_types() as e:
            raise NetworkError(url=url, status_code=None) from e
        if status == 404:
            raise DataNotFoundError(url=url)
        if status >= 400:
            raise NetworkError(url=url, status_code=status)
        return body

    async def get_json_async(self, url: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Fetch ``url`` with the browser ``fetch`` API and return the parsed JSON object."""
        body = await self.get_bytes_async(url, timeout=timeout)
        return _payload_from(url, 200, body)


_transport: PyodideTransport | None = None


def get_pyodide_transport() -> PyodideTransport:
    """The process-wide :class:`PyodideTransport` (one pool manager per runtime)."""
    global _transport
    if _transport is None:
        _transport = PyodideTransport()
    return _transport


def _reset_pyodide_transport() -> None:
    """Forget the shared transport (tests)."""
    global _transport
    _transport = None


def _require_pyodide(operation: str) -> None:
    if not is_pyodide():
        raise TIF1Error(f"{operation}() is only available inside Pyodide", operation=operation)


async def _syncfs(fs: Any, *, populate: bool) -> None:
    """Await Emscripten's callback-style ``FS.syncfs``."""
    from pyodide.ffi import create_once_callable  # ty: ignore[unresolved-import]

    loop = asyncio.get_running_loop()
    done: asyncio.Future[None] = loop.create_future()

    def _finished(error: Any = None) -> None:
        if done.done():
            return
        if error:
            done.set_exception(TIF1Error(f"IndexedDB sync failed: {error}"))
        else:
            done.set_result(None)

    fs.syncfs(populate, create_once_callable(_finished))
    await done


async def enable_persistent_cache(cache_dir: str | Path | None = None) -> Path:
    """Mount the cache directory on IndexedDB and load its previous contents.

    Call it once, before the first ``get_session()``: an open SQLite cache is
    closed first and the next ``get_cache()`` reopens it on the mounted
    directory. The mount keeps living in memory; :func:`persist_cache` copies
    the changes to IndexedDB.

    Args:
        cache_dir: Directory to mount (defaults to the configured ``cache_dir``).

    Returns:
        The mounted cache directory.

    Raises:
        TIF1Error: Outside Pyodide, or when IndexedDB refuses the sync.
    """
    _require_pyodide("enable_persistent_cache")
    from js import Object  # ty: ignore[unresolved-import]
    from pyodide_js import FS  # ty: ignore[unresolved-import]

    from . import cache as cache_module
    from .config import get_config

    config = get_config()
    target = Path(cache_dir) if cache_dir is not None else Path(str(config.get("cache_dir")))
    target = target.expanduser()
    cache_module._cleanup_cache()
    FS.mkdirTree(str(target))
    FS.mount(FS.filesystems.IDBFS, Object.new(), str(target))
    await _syncfs(FS, populate=True)
    config.set("cache_dir", str(target))
    logger.info("tif1 cache mounted on IndexedDB at %s", target)
    return target


async def persist_cache() -> None:
    """Write the cache directory to IndexedDB (after :func:`enable_persistent_cache`).

    Commits pending SQLite writes first. Browsers never run ``atexit`` hooks,
    so call it after the loads worth keeping.

    Raises:
        TIF1Error: Outside Pyodide, or when IndexedDB refuses the sync.
    """
    _require_pyodide("persist_cache")
    from pyodide_js import FS  # ty: ignore[unresolved-import]

    from . import cache as cache_module

    cache = cache_module._cache
    if cache is not None:
        cache.flush()
    await _syncfs(FS, populate=False)


__all__ = [
    "PyodideTransport",
    "enable_persistent_cache",
    "get_pyodide_transport",
    "persist_cache",
]
