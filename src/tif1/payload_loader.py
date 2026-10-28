"""Deep payload-loading pipeline: memo -> cache -> CDN fallback -> retry -> validate -> parse.

This module is the single owner of "how one JSON payload arrives":

- :class:`HttpTransport` — the HTTP seam (:class:`typing.Protocol`). Production
  code uses :class:`NiquestsTransport`; tests use :class:`InMemoryTransport`,
  which serves payloads (or exceptions) from an in-memory mapping and records
  every requested URL.
- :class:`PayloadLoader` — the two-method interface (:meth:`PayloadLoader.get`
  for a single path, :meth:`PayloadLoader.get_many` for async fan-out) hiding
  the whole pipeline. Per-session state (memoized payloads) and persistent
  caching are injected as callables, so ``tif1.core.Session`` keeps its
  existing storage/monkeypatch seams while the pipeline ordering lives here.

CDN fallback itself lives once in :class:`tif1.cdn.CDNManager`
(:meth:`~tif1.cdn.CDNManager.try_sources` for sync callers,
:meth:`~tif1.cdn.CDNManager.try_sources_async` for the async fan-out in
``tif1.async_fetch``); the loader merely supplies the per-URL fetch callable
built on the transport.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from .cdn import CDNManager, get_cdn_manager
from .core_utils.json_utils import parse_response_json
from .core_utils.runtime import IS_EMSCRIPTEN
from .exceptions import DataNotFoundError, InvalidDataError, NetworkError
from .retry import retry_with_backoff

if TYPE_CHECKING:
    from .cache import SessionMemo

logger = logging.getLogger(__name__)

# Exceptions the pipeline treats as expected fetch/validation failures.
_FETCH_ERRORS = (DataNotFoundError, InvalidDataError, NetworkError, TypeError, ValueError)

_MISSING: Any = object()


@runtime_checkable
class HttpTransport(Protocol):
    """HTTP seam for payload fetching.

    Implementations fetch one URL and return the parsed JSON object.

    Raises:
        DataNotFoundError: The resource does not exist (HTTP 404).
        NetworkError: The request failed at the transport level.
        InvalidDataError: The response body was not a JSON object.
    """

    def get_json(self, url: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Fetch ``url`` and return the parsed JSON object."""
        ...


@runtime_checkable
class AsyncHttpTransport(Protocol):
    """Optional async side of a transport, used by the async fan-out in the browser.

    ``tif1.async_fetch`` runs its per-URL fetch through this method when the
    runtime has no thread pool (Pyodide); a transport may also offer
    ``get_bytes_async`` returning the raw body so telemetry payloads keep
    their typed decode. Same exception contract as :class:`HttpTransport`.
    """

    async def get_json_async(self, url: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Fetch ``url`` without blocking and return the parsed JSON object."""
        ...


def default_transport() -> HttpTransport:
    """The production transport of this runtime.

    niquests over sockets natively; inside Pyodide (Emscripten) the browser's
    own fetch/XHR through :class:`tif1.browser.PyodideTransport`.
    """
    if IS_EMSCRIPTEN:
        from .browser import get_pyodide_transport

        return get_pyodide_transport()
    return NiquestsTransport()


def _import_niquests() -> Any:
    """Lazy niquests import (keeps module import cheap)."""
    import niquests

    return niquests


class NiquestsTransport:
    """Production :class:`HttpTransport` over the shared niquests session."""

    def get_json(self, url: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Fetch ``url`` with the pooled niquests session and parse the JSON body."""
        from .config import get_config
        from .http_session import _response_reused_connection, _track_request, get_session

        niquests = _import_niquests()
        if timeout is None:
            timeout = get_config().get("timeout", 30)

        try:
            response = get_session().get(url, timeout=timeout)
        except niquests.RequestException as e:
            raise NetworkError(url=url, status_code=None) from e
        _track_request(reused=_response_reused_connection(response))

        if response.status_code == 404:
            raise DataNotFoundError(url=url)
        try:
            response.raise_for_status()
        except niquests.RequestException as e:
            status_code = getattr(response, "status_code", None)
            if status_code == 429:
                # Jolpica (tif1.ergast) honours the server's Retry-After.
                raise NetworkError(
                    url=url, status_code=429, retry_after=_retry_after_seconds(response)
                ) from e
            raise NetworkError(url=url, status_code=status_code) from e

        data = parse_response_json(response)
        if not isinstance(data, dict):
            raise InvalidDataError(reason=f"Expected dict, got {type(data).__name__}")
        return data


def _retry_after_seconds(response: Any) -> float | None:
    """Seconds from a ``Retry-After`` header (delay form only), or None."""
    headers = getattr(response, "headers", None)
    value = headers.get("Retry-After") if headers is not None else None
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


class InMemoryTransport:
    """In-memory :class:`HttpTransport` for tests.

    Maps URL fragments (paths, full URLs, or any substring) to payloads,
    exceptions, or callables. Every requested URL is recorded in
    :attr:`calls`. Matching prefers an exact URL match, then the longest
    fragment contained in the URL.

    Example:
        >>> transport = InMemoryTransport({"drivers.json": {"drivers": []}})
        >>> transport.get_json("https://cdn.example.com/2025/main/GP/R/drivers.json")
        {'drivers': []}
    """

    def __init__(self, payloads: Mapping[str, Any] | None = None, *, default: Any = _MISSING):
        """Initialize the transport.

        Args:
            payloads: Mapping of URL fragment to payload (dict or any value),
                ``Exception`` instance/class to raise, or callable taking the
                URL and returning any of those.
            default: Fallback entry used when no mapping matches. When unset,
                unmatched URLs raise :class:`DataNotFoundError`.
        """
        self._payloads: dict[str, Any] = dict(payloads or {})
        self.default = default
        self.calls: list[str] = []

    def add(self, key: str, value: Any) -> None:
        """Register (or replace) the payload for a URL fragment."""
        self._payloads[key] = value

    def _lookup(self, url: str) -> Any:
        if url in self._payloads:
            return self._payloads[url]
        best_key = None
        for key in self._payloads:
            if key and key in url and (best_key is None or len(key) > len(best_key)):
                best_key = key
        if best_key is None:
            return _MISSING
        return self._payloads[best_key]

    @staticmethod
    def _resolve(entry: Any, url: str) -> Any:
        if callable(entry) and not isinstance(entry, type):
            entry = entry(url)
        if isinstance(entry, type) and issubclass(entry, BaseException):
            raise entry()
        if isinstance(entry, BaseException):
            raise entry
        return entry

    def get_json(self, url: str, *, timeout: float | None = None) -> dict[str, Any]:  # noqa: ARG002
        """Resolve ``url`` against the in-memory mapping.

        Raises:
            DataNotFoundError: No mapping matched and no default was provided.
            Exception: Whatever exception the matched entry specifies.
        """
        self.calls.append(url)
        entry = self._lookup(url)
        if entry is _MISSING:
            if self.default is _MISSING:
                raise DataNotFoundError(url=url)
            entry = self.default
        return self._resolve(entry, url)

    async def get_json_async(self, url: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Async twin of :meth:`get_json` (same mapping, same ``calls`` log)."""
        return self.get_json(url, timeout=timeout)


class PayloadLoader:
    """Single owner of the payload pipeline: memo -> cache -> CDN -> retry -> validate.

    The loader is parameterized by session coordinates (``year``/``gp``/
    ``session``) used to build cache keys and CDN URLs. All storage and the
    fetch step itself are injectable, which lets ``tif1.core.Session`` route
    the pipeline through its overridable ``_fetch_from_cdn`` /
    ``_fetch_from_cdn_fast`` delegates while keeping a single implementation
    of the ordering, coercion, validation, and bookkeeping.

    Args:
        year: Season year (``None`` only for absolute-URL loaders).
        gp: Grand Prix identifier (URL-encoded).
        session: Session identifier (URL-encoded).
        transport: HTTP seam implementation; defaults to
            :class:`NiquestsTransport`.
        cdn_manager: CDN fallback manager; defaults to the shared
            :func:`tif1.cdn.get_cdn_manager` instance resolved at call time.
        memo: Optional :class:`tif1.cache.SessionMemo` used by the default
            memo get/set callables.
        enable_cache: Gate for the default cache get/set callables.
        fetch: Optional override for the fetch step with signature
            ``fetch(path, *, fast: bool)``; defaults to
            :meth:`fetch_from_cdn`.
        memo_get: Optional override for the memo read (path -> dict or None).
        memo_set: Optional override for the memo write (path, dict).
        cache_get: Optional override for the persistent-cache read
            (cache key -> data or None).
        cache_set: Optional override for the persistent-cache write
            (cache key, dict).
    """

    def __init__(
        self,
        year: int | None = None,
        gp: str | None = None,
        session: str | None = None,
        *,
        transport: HttpTransport | None = None,
        cdn_manager: CDNManager | None = None,
        memo: SessionMemo | None = None,
        enable_cache: bool = True,
        fetch: Callable[..., Any] | None = None,
        memo_get: Callable[[str], dict[str, Any] | None] | None = None,
        memo_set: Callable[[str, dict[str, Any]], None] | None = None,
        cache_get: Callable[[str], Any | None] | None = None,
        cache_set: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.year = year
        self.gp = gp
        self.session = session
        self.transport: HttpTransport = transport if transport is not None else default_transport()
        self._cdn_manager = cdn_manager
        self.memo = memo
        self.enable_cache = enable_cache
        self._fetch_override = fetch
        self._memo_get_fn = memo_get
        self._memo_set_fn = memo_set
        self._cache_get_fn = cache_get
        self._cache_set_fn = cache_set

    @property
    def cdn_manager(self) -> CDNManager:
        """CDN manager (falls back to the process-global instance)."""
        if self._cdn_manager is not None:
            return self._cdn_manager
        return get_cdn_manager()

    @cdn_manager.setter
    def cdn_manager(self, value: CDNManager | None) -> None:
        self._cdn_manager = value

    def cache_key(self, path: str) -> str:
        """Build the persistent-cache key for a session-relative path."""
        return f"{self.year}/{self.gp}/{self.session}/{path}"

    def _data_not_found(self) -> DataNotFoundError:
        """Build a DataNotFoundError carrying session context when available."""
        if self.year is not None:
            return DataNotFoundError(year=self.year, event=self.gp, session=self.session)
        return DataNotFoundError()

    def _session_context(self) -> tuple[int, str, str]:
        """Return ``(year, gp, session)``, requiring session-scoped construction."""
        if self.year is None or self.gp is None or self.session is None:
            raise TypeError(
                "PayloadLoader has no session context; only absolute-URL fetches are supported"
            )
        return self.year, self.gp, self.session

    # -- storage seams ------------------------------------------------------

    def _memo_get(self, path: str) -> dict[str, Any] | None:
        if self._memo_get_fn is not None:
            return self._memo_get_fn(path)
        if self.memo is None:
            return None
        payload = self.memo.get("json", path)
        return payload if isinstance(payload, dict) else None

    def _memo_set(self, path: str, data: dict[str, Any]) -> None:
        if self._memo_set_fn is not None:
            self._memo_set_fn(path, data)
            return
        if self.memo is not None:
            self.memo.set("json", path, data)

    def _cache_get(self, cache_key: str) -> Any | None:
        if self._cache_get_fn is not None:
            return self._cache_get_fn(cache_key)
        if not self.enable_cache:
            return None
        from .cache import get_cache

        return get_cache().get(cache_key)

    def _known_missing(self, cache_key: str) -> bool:
        """True when the negative-result tier holds an all-4xx verdict for ``cache_key``."""
        if not self.enable_cache or self.year is None:
            return False
        from .cache import get_cache

        return get_cache().is_known_missing(cache_key)

    def _set_missing_verdict(self, cache_key: str, *, missing: bool) -> None:
        """Record (all-CDN 4xx) or drop (payload fetched) the verdict for ``cache_key``.

        Q17: the sync walk keeps the negative-result tier in step exactly like
        ``fetch_json_async`` does, so refs only ever touched through per-lap
        access get verdicts too.
        """
        if not self.enable_cache or self.year is None:
            return
        from .cache import get_cache

        cache = get_cache()
        if missing:
            cache.record_missing(cache_key)
        else:
            cache.discard_missing(cache_key)

    def _cache_set(self, cache_key: str, data: dict[str, Any]) -> None:
        if self._cache_set_fn is not None:
            self._cache_set_fn(cache_key, data)
            return
        if not self.enable_cache:
            return
        from .cache import get_cache

        get_cache().set(cache_key, data)

    # -- pipeline stages ----------------------------------------------------

    def _fetch(self, path: str, *, fast: bool) -> Any:
        if self._fetch_override is not None:
            return self._fetch_override(path, fast=fast)
        return self.fetch_from_cdn(path, fast=fast)

    def _coerce_payload(self, result: Any) -> dict[str, Any]:
        """Normalize a fetch result to a JSON object.

        Accepts dicts (returned as-is) and response-like objects
        (``status_code``/``raise_for_status``/``json``), which keeps the
        ``Session._fetch_from_cdn`` override seam permissive for test doubles.

        Raises:
            DataNotFoundError: Response-like result with status 404.
            InvalidDataError: Result is not (and does not parse to) a dict.
        """
        if isinstance(result, dict):
            return result
        if hasattr(result, "json"):
            if getattr(result, "status_code", None) == 404:
                raise self._data_not_found()
            raise_for_status = getattr(result, "raise_for_status", None)
            if callable(raise_for_status):
                raise_for_status()
            data = parse_response_json(result)
            if isinstance(data, dict):
                return data
            raise InvalidDataError(reason=f"Expected dict, got {type(data).__name__}")
        raise InvalidDataError(reason=f"Expected dict, got {type(result).__name__}")

    @staticmethod
    def _validate_payload(path: str, data: dict[str, Any]) -> dict[str, Any]:
        from .async_fetch import _validate_json_payload
        from .config import get_config

        return _validate_json_payload(path, data, get_config())

    # -- public interface ---------------------------------------------------

    def get(
        self,
        path: str,
        *,
        validate: bool = True,
        use_cache: bool = True,
        write_cache: bool = True,
        fast: bool = False,
    ) -> dict[str, Any]:
        """Fetch one session-relative JSON payload through the full pipeline.

        Order: session memo -> persistent cache -> CDN fetch -> validate ->
        memo/cache write-back. Validation failures always raise; there is no
        patched-callable escape hatch.

        Args:
            path: Session-relative payload path (e.g. ``"drivers.json"``).
            validate: Run payload validation before returning.
            use_cache: Consult the persistent cache before fetching.
            write_cache: Persist fetched payloads to the cache.
            fast: Skip per-source retry/backoff delays on the CDN fetch.

        Returns:
            The fetched (and optionally validated) JSON object.

        Raises:
            DataNotFoundError: Payload does not exist.
            InvalidDataError: Payload is not a JSON object or failed validation.
            NetworkError: All CDN sources failed, or ``offline_mode`` is on and
                the payload is not cached.
        """
        local_payload = self._memo_get(path)
        if local_payload is not None:
            return local_payload

        from .config import get_config

        config = get_config()
        # Same contract as the batch path (fetch_json_async): CI mode skips the
        # persistent cache, offline mode never touches the network.
        if config.get("ci_mode", False):
            use_cache = write_cache = False

        cache_key = self.cache_key(path)

        if use_cache:
            cached = self._cache_get(cache_key)
            if cached is not None:
                if isinstance(cached, dict):
                    self._memo_set(path, cached)
                return cached
            # Q16: same contract as the batch path (fetch_json_async) — an
            # everywhere-missing verdict skips the CDN walk (a warm
            # ``Laps.telemetry`` spent ~1 s re-confirming one missing lap).
            if self._known_missing(cache_key):
                raise self._data_not_found()

        if config.get("offline_mode", False):
            raise NetworkError(url=cache_key, status_code=None)

        try:
            try:
                result = self._fetch(path, fast=fast)
            except DataNotFoundError:
                # Every CDN refused with 4xx: later loads (any process, within
                # the TTL) skip the walk. CI mode leaves the cache untouched.
                if use_cache or write_cache:
                    self._set_missing_verdict(cache_key, missing=True)
                raise
            data = self._coerce_payload(result)
            if validate:
                data = self._validate_payload(path, data)
        except _FETCH_ERRORS as e:
            if path.endswith("_tel.json"):
                logger.debug("Telemetry fetch failed for %s: %s", cache_key, e)
            else:
                logger.error(f"Failed to fetch {cache_key}: {e}")
            raise

        self._memo_set(path, data)
        if write_cache:
            self._cache_set(cache_key, data)
            # The payload exists: drop any stale verdict (cache-bypassing
            # reads skip the verdict check, so one may still be on record).
            self._set_missing_verdict(cache_key, missing=False)
        if validate:
            logger.info(f"Fetched: {cache_key}")
        return data

    def fetch_from_cdn(self, path: str, *, fast: bool = False) -> dict[str, Any]:
        """Fetch a payload over HTTP with CDN fallback.

        CDN source ordering/fallback lives in
        :meth:`tif1.cdn.CDNManager.try_sources`; this method only builds the
        per-URL fetch callable on the configured transport, optionally wrapped
        in retry/backoff.

        Args:
            path: Session-relative payload path.
            fast: When True, skip per-source retry/backoff delays (the
                zero-retry "ultra-cold" path).

        Returns:
            Parsed JSON object from the first working CDN source.

        Raises:
            DataNotFoundError: Payload returned 404 on every CDN source.
            NetworkError: Every CDN source failed with a non-404 error.
        """
        from .config import get_config

        config = get_config()
        timeout = config.get("timeout", 30)
        transport = self.transport

        def fetch_from_url(url: str) -> dict[str, Any]:
            try:
                return transport.get_json(url, timeout=timeout)
            except DataNotFoundError as e:
                if self.year is None:
                    raise
                raise DataNotFoundError(year=self.year, event=self.gp, session=self.session) from e

        if fast:
            fetch = fetch_from_url
        else:
            fetch = retry_with_backoff(
                max_retries=config.get("max_retries", 3),
                backoff_factor=config.get("retry_backoff_factor", 2.0),
                jitter=config.get("retry_jitter", True),
                exceptions=(NetworkError,),
            )(fetch_from_url)

        year, gp, session = self._session_context()
        return self.cdn_manager.try_sources(year, gp, session, path, fetch)

    async def get_many(
        self,
        paths: list[str],
        *,
        use_cache: bool = True,
        write_cache: bool = True,
        validate: bool = True,
        max_retries: int | None = None,
        timeout: int | None = None,
        max_concurrent_requests: int | None = None,
    ) -> list[dict[str, Any] | None]:
        """Fetch many session-relative payloads concurrently (async fan-out).

        Delegates to :func:`tif1.async_fetch.fetch_multiple_async`, the async
        sibling pipeline whose per-source work shares the CDN fallback loop
        owned by :meth:`tif1.cdn.CDNManager.try_sources_async`.

        Args:
            paths: Session-relative payload paths.
            use_cache: Read from cache before network fetches.
            write_cache: Persist successful responses to the cache.
            validate: Run payload validation on fetched payloads.
            max_retries: Retry attempts per payload (defaults to config).
            timeout: Request timeout in seconds (defaults to config).
            max_concurrent_requests: Concurrency cap (defaults to config).

        Returns:
            One entry per requested path; ``None`` for failed fetches.
        """
        from .async_fetch import fetch_multiple_async

        year, gp, session = self._session_context()
        requests = [(year, gp, session, path) for path in paths]
        return await fetch_multiple_async(
            requests,
            use_cache=use_cache,
            write_cache=write_cache,
            validate_payload=validate,
            max_retries=max_retries,
            timeout=timeout,
            max_concurrent_requests=max_concurrent_requests,
        )

    def get_url(self, url: str, *, timeout: float | None = None) -> dict[str, Any]:
        """Fetch a JSON object from an absolute URL (no memo/cache/CDN fallback).

        Used for resources outside the per-session CDN layout (e.g. the
        f1schedule year payloads in ``tif1.events``).

        Args:
            url: Absolute URL to fetch.
            timeout: Request timeout in seconds (defaults to config).

        Returns:
            Parsed JSON object.
        """
        return self.transport.get_json(url, timeout=timeout)


_url_loader: PayloadLoader | None = None
_url_loader_lock = threading.Lock()


def get_url_loader() -> PayloadLoader:
    """Get the shared loader for absolute-URL payloads (thread-safe lazy init)."""
    global _url_loader
    if _url_loader is None:
        with _url_loader_lock:
            if _url_loader is None:
                _url_loader = PayloadLoader()
    return _url_loader


__all__ = [
    "AsyncHttpTransport",
    "default_transport",
    "HttpTransport",
    "InMemoryTransport",
    "NiquestsTransport",
    "PayloadLoader",
    "get_url_loader",
]
