"""Session bundles on the read side (Corinth C10): one request for a session's small files.

With ``session_bundle`` on, the first fetch of any member of a session's
bundle (:data:`~tif1.core_utils.session_bundle.SESSION_MEMBERS`: the first-wave
files, ``session_info.json``, weather and race control) downloads the
session's ``session.v1.bin`` once for the process, and every member request
of that session takes its bytes from it instead of its own request: the
async fan-out (:func:`tif1.async_fetch.fetch_json_async`) and the sync loader
(:meth:`tif1.payload_loader.PayloadLoader.fetch_from_cdn`) run the payload
through the validation and cache write-back they give a fetched one. A cold
``get_session`` asks for the bundle in the stdlib first wave
(:mod:`tif1.first_wave`) instead of ``drivers.json`` and the laptimes.

The bundle's Stats entries (the classification and the season index) are
held here for the Ergast tier (:func:`stats_entry`, read by
``ErgastPayloadLoader._read_entry``), aged from the bundle's ``built_at`` so
the tier's freshness rules (past seasons forever, the current one
``ergast_ttl_hours``) apply to them unchanged.

Once decoded, a background writer (joined like the frame-tier writers) fills
the payload tier with every member the cache does not hold yet, exactly as a
fetch of that file alone would have stored it, records the absent ones as
missing (the per-file walk's all-404 verdict) and writes the Stats entries
with ``fetched_at`` = ``built_at``. So the next process finds the session's
small files in the cache whichever of them this one read.

A 404 for the bundle on the primary CDN means the data repository publishes
none for the session: it is remembered in the missing-payload tier (and for
the process), and the per-file requests follow as before, at the cost of
that one request. A transport failure or an unreadable bundle falls back to
the per-file requests without a verdict. Off in the browser (no thread pool)
and offline.
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from .core_utils.runtime import IS_EMSCRIPTEN, THREADS_AVAILABLE
from .core_utils.session_bundle import (
    BUNDLE_FILE,
    SESSION_MEMBERS,
    STATS_PREFIX,
    SessionBundle,
    SessionBundleError,
    decode_bundle,
)
from .exceptions import DataNotFoundError, InvalidDataError, NetworkError

if TYPE_CHECKING:
    from concurrent.futures import Future

logger = logging.getLogger(__name__)

#: A member the bundle lists as absent: the repository holds no such file.
ABSENT: Any = object()

_MEMBERS = frozenset(SESSION_MEMBERS)
_lock = threading.Lock()
#: (year, gp, session) -> Future of the decoded bundle (None: no usable bundle).
_bundles: OrderedDict[tuple[int, str, str], Future[SessionBundle | None]] = OrderedDict()
#: Decoded bundles kept for their later member requests, oldest dropped first.
_BUNDLES_CAP = 32
#: Stats cache key -> (built_at, bytes) of the bundles decoded in this process.
_stats: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
_STATS_CAP = 256


def enabled() -> bool:
    """Whether member requests ask for the session bundle (``session_bundle``).

    Off in the browser (no thread pool to share the download) and offline.
    """
    from .config import get_config

    config = get_config()
    return (
        bool(config.get("session_bundle", False))
        and THREADS_AVAILABLE
        and not IS_EMSCRIPTEN
        and not bool(config.get("offline_mode", False))
    )


def is_member(path: str) -> bool:
    """Whether ``path`` (session-relative) is a file the bundle carries."""
    return path in _MEMBERS


def start(year: int, gp: str, session: str, *, write_cache: bool = True) -> Future | None:
    """The download of the session's bundle: the running or finished one, else a new one.

    ``write_cache`` (of the request that starts it) lets the decoded bundle
    fill the payload tier. None when bundles are off.
    """
    if not enabled():
        return None
    key = (year, gp, session)
    with _lock:
        future = _bundles.get(key)
        if future is not None:
            return future
        from .async_fetch import _get_executor

        future = _get_executor().submit(_load, year, gp, session, write_cache)
        _bundles[key] = future
        while len(_bundles) > _BUNDLES_CAP:
            _bundles.popitem(last=False)
    return future


def member(year: int, gp: str, session: str, path: str, *, write_cache: bool = True) -> Any:
    """The bytes of ``path`` from the session's bundle (waits for the download).

    Returns:
        The member's bytes, :data:`ABSENT` when the bundle lists it as absent,
        or None when the per-file request must answer (bundles off, no
        usable bundle, or a member the bundle does not carry).
    """
    if not is_member(path):
        return None
    future = start(year, gp, session, write_cache=write_cache)
    if future is None:
        return None
    try:
        bundle = future.result()
    except Exception as e:  # the download thread failed: the per-file path answers
        logger.debug("Session bundle of %s/%s/%s failed: %s", year, gp, session, e)
        return None
    return _member_of(bundle, path)


async def member_async(
    year: int, gp: str, session: str, path: str, *, write_cache: bool = True
) -> Any:
    """:func:`member` for an event loop (awaits the shared download)."""
    if not is_member(path):
        return None
    future = start(year, gp, session, write_cache=write_cache)
    if future is None:
        return None
    import asyncio  # lazy: only async/network paths need it

    try:
        bundle = await asyncio.wrap_future(future)
    except Exception as e:  # the download thread failed: the per-file path answers
        logger.debug("Session bundle of %s/%s/%s failed: %s", year, gp, session, e)
        return None
    return _member_of(bundle, path)


def wait(year: int, gp: str, session: str, timeout: float | None = None) -> None:
    """Wait (bounded) for the session's bundle download when one was started; never raises.

    The Ergast prefetch thread calls it before its lookups, so the bundle's
    Stats entries answer them instead of requests racing the download.
    """
    with _lock:
        future = _bundles.get((year, gp, session))
    if future is None:
        return
    try:
        future.result(timeout=timeout)
    except Exception as e:  # timeout or a failed download: the lookups fetch
        logger.debug("Session bundle wait for %s/%s/%s ended: %s", year, gp, session, e)


def stats_entry(cache_key: str) -> tuple[float, bytes] | None:
    """``(built_at, bytes)`` of a Stats file a decoded bundle carried (never waits)."""
    if not _stats:
        return None
    return _stats.get(cache_key)


def reset() -> None:
    """Forget every bundle and Stats entry of this process (tests)."""
    with _lock:
        _bundles.clear()
        _stats.clear()


def _member_of(bundle: SessionBundle | None, path: str) -> Any:
    if bundle is None:
        return None
    body = bundle.entries.get(path)
    if body is not None:
        return body
    return ABSENT if path in bundle.absent else None


def _load(year: int, gp: str, session: str, write_cache: bool) -> SessionBundle | None:
    """Download and decode the session's bundle; None when there is none to use.

    The primary CDN answers for the bundle, as for the columnar telemetry
    files: a 404 there is remembered (missing-payload tier, then the process)
    and the per-file requests follow. Any other failure tries the next
    mirror; nothing usable from any of them falls back without a verdict.
    """
    from . import first_wave
    from .cdn import get_cdn_manager
    from .config import get_config
    from .payload_loader import get_url_loader

    config = get_config()
    cache_key = f"{year}/{gp}/{session}/{BUNDLE_FILE}"
    cache = None
    if bool(config.get("enable_cache", True)) and not bool(config.get("ci_mode", False)):
        from .cache import get_cache

        cache = get_cache()
        if cache.is_known_missing(cache_key):
            return None
    timeout = max(1, config.get("timeout", 30))
    get_bytes = getattr(get_url_loader().transport, "get_bytes", None)
    for source in get_cdn_manager().get_sources():
        url = source.format_url(year, gp, session, BUNDLE_FILE)
        blob: bytes | None = None
        if first_wave.pending(url):
            status, blob = first_wave.take_status(url)
            if status == 404:
                if cache is not None:
                    cache.record_missing(cache_key)
                return None
        if blob is None:
            if not callable(get_bytes):
                return None
            try:
                blob = get_bytes(url, timeout=timeout)
            except DataNotFoundError:
                if cache is not None:
                    cache.record_missing(cache_key)
                return None
            except (NetworkError, InvalidDataError, OSError, ValueError) as e:
                logger.debug("Session bundle %s failed on %s: %s", cache_key, source.name, e)
                continue
        try:
            bundle = decode_bundle(blob)
        except SessionBundleError as e:
            logger.warning("Session bundle %s unreadable, using the files: %s", url, e)
            return None
        _register(year, gp, session, bundle, cache if write_cache else None)
        return bundle
    return None


def _register(year: int, gp: str, session: str, bundle: SessionBundle, cache: Any | None) -> None:
    """Hold the bundle's Stats entries and start the payload-tier writer."""
    with _lock:
        for name, body in bundle.entries.items():
            if name.startswith(STATS_PREFIX):
                _stats[name] = (bundle.built_at, body)
                _stats.move_to_end(name)
        while len(_stats) > _STATS_CAP:
            _stats.popitem(last=False)
    if cache is None:
        return
    from functools import partial

    from .cache import register_frame_writer

    register_frame_writer(partial(_persist, cache, year, gp, session, bundle))


def _persist(cache: Any, year: int, gp: str, session: str, bundle: SessionBundle) -> None:
    """Fill the payload tier with the bundle's members the cache does not hold yet.

    Each member is validated and stored as :func:`tif1.async_fetch.fetch_json_async`
    stores a fetched file (its bytes when validation leaves the payload as it
    is, the validated object otherwise); an absent member gets the all-404
    verdict; a Stats entry is written with ``fetched_at`` = ``built_at``
    unless the tier holds a newer one.
    """
    from .async_fetch import _validate_json_payload
    from .config import get_config
    from .core_utils.json_utils import json_loads

    config = get_config()
    # With the validators off (the defaults) a fetched member is stored as
    # its bytes: no parse needed here either.
    validating = bool(config.get("validate_data", False)) or bool(
        config.get("validate_lap_times", False)
    )
    try:
        for name, body in bundle.entries.items():
            if name.startswith(STATS_PREFIX):
                _persist_stats(cache, name, body, bundle.built_at)
                continue
            if name not in _MEMBERS:
                continue
            key = f"{year}/{gp}/{session}/{name}"
            if cache.has_json_entry(key):
                continue
            stored: Any = body
            if validating:
                try:
                    data = json_loads(body)
                    if not isinstance(data, dict):
                        continue
                    validated = _validate_json_payload(name, data, config)
                except (InvalidDataError, TypeError, ValueError) as e:
                    logger.debug("Session bundle member %s not cached: %s", key, e)
                    continue
                if validated is not data:
                    stored = validated
            elif body.lstrip()[:1] != b"{":
                continue  # not a JSON object: a fetch would have refused it too
            if isinstance(stored, bytes):
                cache.set_raw(key, stored)
            else:
                cache.set(key, stored)
            cache.discard_missing(key)
        for name in bundle.absent:
            key = f"{year}/{gp}/{session}/{name}"
            if name in _MEMBERS and not cache.has_json_entry(key):
                cache.record_missing(key)
    except (RuntimeError, TypeError, ValueError) as e:  # a closed cache: next process fetches
        logger.debug("Session bundle cache fill skipped: %s", e)


def _persist_stats(cache: Any, key: str, body: bytes, built_at: float) -> None:
    from .core_utils.json_utils import json_loads

    existing = cache.get_entry("json", key)
    if isinstance(existing, dict):
        fetched_at = existing.get("fetched_at")
        if isinstance(fetched_at, int | float) and fetched_at >= built_at:
            return
    try:
        payload = json_loads(body)
    except (TypeError, ValueError):
        return
    if isinstance(payload, dict):
        cache.set_entry("json", key, {"fetched_at": built_at, "payload": payload})


__all__ = [
    "ABSENT",
    "enabled",
    "is_member",
    "member",
    "member_async",
    "reset",
    "start",
    "stats_entry",
    "wait",
]
