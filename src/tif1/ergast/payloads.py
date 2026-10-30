"""Payload loading for the Ergast tier: Stats repository files and Jolpica answers.

One pipeline for both sources: process memo -> SQLite ``json`` tier -> fetch.
Every cached entry is a ``{"fetched_at": <epoch seconds>, "payload": <dict or
None>}`` wrapper (the pattern ``tif1.events`` uses for the f1schedule file),
so freshness is decided at read time:

- a past season never expires (its files do not change);
- the current season and every Jolpica answer expire after
  ``ergast_ttl_hours`` (results get corrected, standings move on, the Stats
  repository catches up a few hours after Jolpica);
- a missing (404 on every CDN) or empty (``total == 0``) Stats file is
  re-probed after ``missing_payloads_ttl_days`` for past seasons and after
  ``ergast_ttl_hours`` for the current one, so later additions upstream are
  picked up without holding a wrong verdict for a week.

A transient jsDelivr 403 ("package size exceeded", seen right after a
branch update) or a cold-edge 404 on one mirror is never persisted: the
missing verdict needs a 404 from every mirror, and a 403 from the last mirror
is retried once after a short pause.

:func:`cache_only` runs a lookup against the memo and SQLite tiers alone: a
lookup that would fetch raises :class:`ErgastFetchNeeded` instead, so a caller
can answer a warm session inline and keep a background thread for real fetches.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from tif1.cache import LRUCache, get_cache
from tif1.cdn import CDNManager, get_cdn_manager
from tif1.config import get_config
from tif1.core_utils.runtime import IS_EMSCRIPTEN, THREADS_AVAILABLE
from tif1.exceptions import CacheError, DataNotFoundError, InvalidDataError, NetworkError
from tif1.payload_loader import HttpTransport, default_transport
from tif1.retry import retry_with_backoff

logger = logging.getLogger(__name__)

STATS_CACHE_PREFIX = "stats/"
JOLPICA_CACHE_PREFIX = "jolpica/"
#: Pause before the single retry of a file every mirror refused with a 403.
_REFUSAL_RETRY_DELAY = 1.0
#: A miss that involved a transient refusal (403) is remembered in-process for
#: this long, so repeated lookups do not pay the mirror walk and the pause again.
_TRANSIENT_MISS_SECONDS = 60.0

_memo = LRUCache(maxsize=512)
_MISS: Any = object()
#: Set by :func:`cache_only` for the current thread (or task) only.
_CACHE_ONLY: ContextVar[bool] = ContextVar("tif1_ergast_cache_only", default=False)


class ErgastFetchNeeded(Exception):  # noqa: N818 - a signal, not an error condition
    """A :func:`cache_only` lookup reached a payload that only a fetch can provide."""


@contextmanager
def cache_only() -> Iterator[None]:
    """Answer lookups from the memo and SQLite tiers only, in this thread or task.

    Inside the block a lookup that would go to the network (a missing or
    expired entry, Stats file or Jolpica answer) raises
    :class:`ErgastFetchNeeded` before any request is made; everything else
    (fresh entries, offline mode, a remembered transient miss) behaves as
    usual.
    """
    token = _CACHE_ONLY.set(True)
    try:
        yield
    finally:
        _CACHE_ONLY.reset(token)


def current_season() -> int:
    """The calendar year, which is the season whose data still changes."""
    return datetime.now(UTC).year


def is_current_season(year: int | None) -> bool:
    """True for the current (or a future) season; its Stats files are still being written."""
    return year is None or year >= current_season()


def ergast_ttl_seconds() -> float:
    """``ergast_ttl_hours`` in seconds (0 means every lookup re-fetches)."""
    hours = get_config().get("ergast_ttl_hours", 6.0)
    try:
        return max(0.0, float(hours)) * 3600.0
    except (TypeError, ValueError):
        return 6.0 * 3600.0


def missing_ttl_seconds() -> float:
    """``missing_payloads_ttl_days`` in seconds (shared with the CDN negative tier)."""
    days = get_config().get("missing_payloads_ttl_days", 7.0)
    try:
        return max(0.0, float(days)) * 86400.0
    except (TypeError, ValueError):
        return 7.0 * 86400.0


def is_empty_payload(payload: Any) -> bool:
    """True when an ``MRData`` payload carries no rows (``total == 0`` or no table list)."""
    if not isinstance(payload, dict):
        return True
    mrdata = payload.get("MRData")
    if not isinstance(mrdata, dict):
        return True
    total = mrdata.get("total")
    try:
        if total is not None and int(total) == 0:
            return True
    except (TypeError, ValueError):
        pass
    for key, value in mrdata.items():
        if key.endswith("Table") and isinstance(value, dict):
            lists = [item for item in value.values() if isinstance(item, list)]
            return not any(lists)
    return True


def stats_path(year: int, slug: str, file_name: str) -> str:
    """Repository path of a Stats file (``{year}/{slug}/{file}``; season files have no slug)."""
    if slug:
        return f"{year}/{quote(slug, safe='-_.')}/{file_name}"
    return f"{year}/{file_name}"


def stats_cache_key(year: int, slug: str, file_name: str) -> str:
    """SQLite json-tier key of a Stats file (keyed on the decoded slug)."""
    return (
        f"{STATS_CACHE_PREFIX}{year}/{slug}/{file_name}"
        if slug
        else f"{STATS_CACHE_PREFIX}{year}/{file_name}"
    )


def clear_memo() -> None:
    """Drop the in-process payload memo (tests and ``Cache.invalidate``)."""
    _memo.clear()


class ErgastPayloadLoader:
    """Memo -> SQLite -> fetch pipeline for Stats files and Jolpica answers.

    Args:
        transport: HTTP seam (defaults to the runtime's production transport;
            :class:`tif1.payload_loader.InMemoryTransport` in tests).
        cdn_manager: CDN fallback manager (defaults to the shared instance).
        enable_cache: Persist to / read from the SQLite tier (defaults to the
            ``enable_cache`` setting; CI mode disables it).
        clock: Wall-clock seconds source (tests inject a fake clock).
    """

    def __init__(
        self,
        *,
        transport: HttpTransport | None = None,
        cdn_manager: CDNManager | None = None,
        enable_cache: bool | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._transport = transport
        self._cdn_manager = cdn_manager
        self._enable_cache = enable_cache
        self._clock = clock
        self._transient_misses: dict[str, float] = {}

    # -- configuration -------------------------------------------------------

    @property
    def transport(self) -> HttpTransport:
        if self._transport is None:
            self._transport = default_transport()
        return self._transport

    @transport.setter
    def transport(self, value: HttpTransport | None) -> None:
        self._transport = value

    @property
    def cdn_manager(self) -> CDNManager:
        return self._cdn_manager if self._cdn_manager is not None else get_cdn_manager()

    @property
    def enable_cache(self) -> bool:
        config = get_config()
        if config.get("ci_mode", False):
            return False
        if self._enable_cache is not None:
            return bool(self._enable_cache)
        return bool(config.get("enable_cache", True))

    @staticmethod
    def repo() -> tuple[str, str]:
        """``(owner/repo, branch)`` of the Stats repository from the config."""
        config = get_config()
        repo = config.get("stats_repo") or "TracingInsights-Archive/Stats"
        branch = config.get("stats_branch") or "main"
        return str(repo), str(branch)

    # -- storage tiers -------------------------------------------------------

    def _read_entry(self, cache_key: str) -> tuple[float, Any] | None:
        """``(fetched_at, payload)`` from the memo or the SQLite tier, else None."""
        memo_entry = _memo.get(cache_key, ordered=False)
        if memo_entry is not None:
            return memo_entry
        if not self.enable_cache:
            return None
        try:
            entry = get_cache().get_entry("json", cache_key)
        except (CacheError, OSError) as exc:
            logger.debug("Ergast cache read failed for %s: %s", cache_key, exc)
            return None
        if not isinstance(entry, dict) or "fetched_at" not in entry:
            return None
        fetched_at = entry.get("fetched_at")
        if not isinstance(fetched_at, int | float):
            return None
        payload = entry.get("payload")
        memo_entry = (float(fetched_at), payload if isinstance(payload, dict) else None)
        _memo.set(cache_key, memo_entry)
        return memo_entry

    def _write_entry(self, cache_key: str, payload: dict | None, fetched_at: float) -> None:
        _memo.set(cache_key, (fetched_at, payload))
        if not self.enable_cache:
            return
        try:
            get_cache().set_entry("json", cache_key, {"fetched_at": fetched_at, "payload": payload})
        except (CacheError, OSError) as exc:
            logger.debug("Ergast cache write failed for %s: %s", cache_key, exc)

    @staticmethod
    def _entry_ttl(year: int | None, payload: dict | None) -> float:
        """Seconds an entry stays fresh, by season age and by whether it holds rows."""
        if payload is None or is_empty_payload(payload):
            return ergast_ttl_seconds() if is_current_season(year) else missing_ttl_seconds()
        if is_current_season(year):
            return ergast_ttl_seconds()
        return math.inf

    # -- Stats repository ----------------------------------------------------

    def stats_payload(
        self, year: int, slug: str, file_name: str, *, fast: bool = False
    ) -> dict | None:
        """One Stats file as a parsed dict, or None when no mirror holds it.

        ``fast`` skips the per-source retry/backoff (one attempt per mirror)
        for best-effort lookups that have a fallback of their own.

        Raises:
            NetworkError: every mirror failed (or ``offline_mode`` and nothing
                cached); a stale cached copy is served instead when one exists.
            InvalidDataError: the file is not a JSON object.
        """
        cache_key = stats_cache_key(year, slug, file_name)
        now = self._clock()
        fresh = self._fresh_entry(cache_key, year, now)
        if fresh is not _MISS:
            return fresh
        entry = self._read_entry(cache_key)
        if self._transient_misses.get(cache_key, 0.0) > now:
            return None
        if get_config().get("offline_mode", False):
            if entry is not None:
                return entry[1]
            raise NetworkError(url=cache_key, status_code=None)
        if _CACHE_ONLY.get():
            raise ErgastFetchNeeded(cache_key)

        path = stats_path(year, slug, file_name)
        try:
            payload, persist = self._fetch_stats(path, fast=fast)
        except NetworkError:
            if entry is not None:
                logger.warning(
                    "Serving stale Ergast payload for %s after a fetch failure", cache_key
                )
                return entry[1]
            raise
        if persist:
            self._write_entry(cache_key, payload, now)
            self._transient_misses.pop(cache_key, None)
        else:
            # Transient refusal: nothing is persisted; the next lookup after a
            # short pause probes the mirrors again.
            _memo.pop(cache_key, None)
            self._transient_misses[cache_key] = now + _TRANSIENT_MISS_SECONDS
        return payload

    def is_fresh(self, year: int, slug: str, file_name: str) -> bool:
        """True when a Stats file is answered from the memo/SQLite tiers without a fetch.

        A fresh "missing" verdict counts: it is an answer too.
        """
        cache_key = stats_cache_key(year, slug, file_name)
        return self._fresh_entry(cache_key, year, self._clock()) is not _MISS

    def _fresh_entry(self, cache_key: str, year: int | None, now: float) -> Any:
        """The cached payload when its entry is still fresh, else :data:`_MISS`."""
        entry = self._read_entry(cache_key)
        if entry is None:
            return _MISS
        fetched_at, payload = entry
        if now - fetched_at < self._entry_ttl(year, payload):
            return payload
        return _MISS

    def _fetch_stats(self, path: str, *, fast: bool = False) -> tuple[dict | None, bool]:
        """``(payload, persist)``: the file, or ``(None, all_404)`` when every mirror refused."""
        config = get_config()
        timeout = config.get("timeout", 30)
        transport = self.transport
        repo, branch = self.repo()

        def fetch_from_url(url: str) -> dict[str, Any]:
            return transport.get_json(url, timeout=timeout)

        fetch = (
            fetch_from_url
            if fast
            else retry_with_backoff(
                max_retries=config.get("max_retries", 3),
                backoff_factor=config.get("retry_backoff_factor", 2.0),
                jitter=config.get("retry_jitter", True),
                exceptions=(NetworkError,),
            )(fetch_from_url)
        )

        for attempt in range(2):
            try:
                payload = self.cdn_manager.try_sources_repo(repo, branch, path, fetch)
            except DataNotFoundError as exc:
                statuses = exc.context.get("statuses") or []
                all_404 = bool(statuses) and all(status == 404 for status in statuses)
                if all_404:
                    return None, True
                if attempt == 0 and not IS_EMSCRIPTEN:
                    # A 403 is jsDelivr's transient answer for a repository
                    # over its package limit; one short pause, one retry.
                    time.sleep(_REFUSAL_RETRY_DELAY)
                    continue
                return None, False
            if not isinstance(payload, dict):
                raise InvalidDataError(reason=f"Expected dict, got {type(payload).__name__}")
            return payload, True
        return None, False

    def stats_payloads(self, refs: list[tuple[int, str, str]]) -> list[dict | Exception | None]:
        """Many Stats files at once (thread pool natively, sequential in Pyodide).

        Returns one entry per ref: the payload, ``None`` when missing, or the
        exception a fetch raised (so callers can tell an outage from an absent
        file instead of silently dropping a round).
        """
        if not refs:
            return []
        # Memo/SQLite hits are answered inline; only the misses need threads.
        now = self._clock()
        results: list[Any] = [_MISS] * len(refs)
        pending: list[int] = []
        for index, (year, slug, file_name) in enumerate(refs):
            fresh = self._fresh_entry(stats_cache_key(year, slug, file_name), year, now)
            if fresh is _MISS:
                pending.append(index)
            else:
                results[index] = fresh
        if not pending:
            return results
        max_workers = max(1, int(get_config().get("max_concurrent_requests", 20) or 20))
        # A cache-only lookup stays on this thread: the first fetch it needs raises.
        if not THREADS_AVAILABLE or len(pending) == 1 or max_workers == 1 or _CACHE_ONLY.get():
            for index in pending:
                results[index] = self._stats_payload_or_error(refs[index])
            return results
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=min(len(pending), max_workers)) as pool:
            fetched = list(pool.map(self._stats_payload_or_error, [refs[i] for i in pending]))
        for index, payload in zip(pending, fetched):
            results[index] = payload
        return results

    def _stats_payload_or_error(self, ref: tuple[int, str, str]) -> dict | Exception | None:
        year, slug, file_name = ref
        try:
            return self.stats_payload(year, slug, file_name)
        except (NetworkError, InvalidDataError) as exc:
            logger.debug("Stats fetch failed for %s/%s/%s: %s", year, slug, file_name, exc)
            return exc

    # -- Jolpica -------------------------------------------------------------

    def jolpica_payload(
        self,
        cache_key: str,
        fetch: Callable[[], dict],
        *,
        ttl: float | None = None,
    ) -> dict:
        """A Jolpica answer through the memo/SQLite tiers; ``fetch`` runs on a miss.

        ``ttl`` defaults to ``ergast_ttl_hours``. A stale copy is served when
        the fetch fails with a transport error.
        """
        key = (
            cache_key
            if cache_key.startswith(JOLPICA_CACHE_PREFIX)
            else JOLPICA_CACHE_PREFIX + cache_key
        )
        now = self._clock()
        ttl_seconds = ergast_ttl_seconds() if ttl is None else ttl
        entry = self._read_entry(key)
        if entry is not None and entry[1] is not None and now - entry[0] < ttl_seconds:
            return entry[1]
        if get_config().get("offline_mode", False):
            if entry is not None and entry[1] is not None:
                return entry[1]
            raise NetworkError(url=key, status_code=None)
        if _CACHE_ONLY.get():
            raise ErgastFetchNeeded(key)
        try:
            payload = fetch()
        except NetworkError:
            if entry is not None and entry[1] is not None:
                logger.warning("Serving stale Jolpica answer for %s after a fetch failure", key)
                return entry[1]
            raise
        self._write_entry(key, payload, now)
        return payload


_shared_loader: ErgastPayloadLoader | None = None


def get_ergast_loader() -> ErgastPayloadLoader:
    """The process-wide loader (tests swap its ``transport``)."""
    global _shared_loader
    if _shared_loader is None:
        _shared_loader = ErgastPayloadLoader()
    return _shared_loader


def reset_ergast_loader() -> None:
    """Drop the shared loader and the memo (tests)."""
    global _shared_loader
    _shared_loader = None
    clear_memo()


__all__ = [
    "ErgastFetchNeeded",
    "ErgastPayloadLoader",
    "JOLPICA_CACHE_PREFIX",
    "STATS_CACHE_PREFIX",
    "cache_only",
    "clear_memo",
    "current_season",
    "ergast_ttl_seconds",
    "get_ergast_loader",
    "is_current_season",
    "is_empty_payload",
    "reset_ergast_loader",
    "stats_cache_key",
    "stats_path",
]
