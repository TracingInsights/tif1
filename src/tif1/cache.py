"""Unified caching: SQLite tier, in-memory LRU tiers, backend lap caches, session memo.

This module owns every cache layer in tif1 so there is a single place that knows
what is cached where:

- :class:`LRUCache` — the one bounded-LRU implementation, shared by the
  in-memory tiers and the process-global backend lap caches.
- :class:`Cache` — persistent SQLite tier plus in-memory LRU tiers, fronted by
  a kind-based interface (:meth:`Cache.get_entry`, :meth:`Cache.set_entry`,
  :meth:`Cache.invalidate`). The long-standing ``get``/``set``/
  ``get_telemetry``/``set_telemetry``/``has_session_data`` methods are thin
  wrappers over it and remain the hot paths.
- :func:`get_backend_lap_cache` / :func:`clear_lap_cache` — the process-global
  pandas/polars lap-caching tier (``tif1.core.clear_lap_cache`` remains an
  alias for backward compatibility).
- :class:`SessionMemo` — the per-``Session`` memo tier: path-keyed JSON
  payloads, (driver, lap)-keyed telemetry payloads and materialized
  DataFrames, fastest-lap-reference state, the persistent-cache probe result,
  and per-driver telemetry failure tracking.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import gc
import logging
import pickle
import sqlite3
import threading
import time
import zlib
from collections import OrderedDict
from pathlib import Path
from typing import Any, Literal

from .core_utils.constants import MAX_CACHE_SIZE
from .core_utils.json_utils import json_dumps_bytes, json_loads

logger = logging.getLogger(__name__)


TelemetryCacheKey = tuple[int, str, str, str, int]
CacheKind = Literal["json", "telemetry"]
InvalidateScope = Literal["all", "memory", "json", "telemetry"]
SessionMemoKind = Literal["json", "telemetry_payload", "telemetry_df"]
FastestLapRefKind = Literal["laps", "drivers"]

# SQLite-tier compression: telemetry payloads compress ~12x (numeric JSON),
# cutting insert/WAL volume on cold write-cache loads. Values below the
# threshold stay plain TEXT so small rows keep the legacy storage shape.
# zstd-1 compresses ~4.5x faster than zlib-3 at a slightly better ratio and
# decompresses ~2x faster (measured on the 1452-payload Monaco set), so it is
# the codec when ``zstandard`` is importable; zlib remains the fallback and
# legacy zlib rows stay readable (zstd frames carry a 4-byte magic prefix).
_SQLITE_COMPRESS_MIN_BYTES = 4096
_SQLITE_COMPRESS_LEVEL = 3
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"

try:  # pragma: no cover - trivial import guard
    import zstandard as _zstandard

    _ZSTD_COMPRESSOR = _zstandard.ZstdCompressor(level=1)
    _ZSTD_DECOMPRESSOR = _zstandard.ZstdDecompressor()
except ImportError:  # pragma: no cover - defensive fallback only
    _ZSTD_COMPRESSOR = None
    _ZSTD_DECOMPRESSOR = None

# Materialized-frame codec: lz4 (level 0) halves both compress and decompress
# time vs zstd-1 on the 1452-frame Monaco set (~2x faster, +26% stored bytes)
# — the frame tier is write-once/read-many, so read/write speed beats ratio.
# zstd remains the fallback (and legacy zstd rows stay readable via magic).
try:  # pragma: no cover - trivial import guard
    from lz4 import frame as _lz4_frame

    _LZ4_MAGIC = b"\x04\x22\x4d\x18"
except ImportError:  # pragma: no cover - defensive fallback only
    _lz4_frame = None
    _LZ4_MAGIC = b""


def _compress_frame_blob(blob: bytes) -> bytes:
    """Compress a pickled frame for the frame tier (lz4 when available)."""
    if _lz4_frame is not None:
        return _lz4_frame.compress(blob, compression_level=0)
    assert _ZSTD_COMPRESSOR is not None  # callers gate on the zstd tier
    return _ZSTD_COMPRESSOR.compress(blob)


def _decompress_frame_blob(blob: bytes) -> bytes:
    """Decompress a frame-tier blob; both lz4 and legacy zstd rows read."""
    if _lz4_frame is not None and blob[:4] == _LZ4_MAGIC:
        return _lz4_frame.decompress(blob)
    assert _ZSTD_DECOMPRESSOR is not None  # callers gate on the zstd tier
    return _ZSTD_DECOMPRESSOR.decompress(blob)


# Parsed-object front tiers: bounded small because parsed payloads are far
# larger than blobs. Repeat hits skip orjson entirely (see RESULTS.md H4).
_PARSED_CACHE_MAX_ITEMS = 128
_PARSED_TELEMETRY_CACHE_MAX_ITEMS = 256

# Materialized-frame tables per backend: pandas frames live in
# ``telemetry_frames`` (pre-existing), polars frames in ``telemetry_frames_pl``
# so both backends can keep a warm frame tier without PK collisions and
# without migrating existing caches.
_FRAME_TABLES: dict[str, str] = {"pandas": "telemetry_frames", "polars": "telemetry_frames_pl"}


@contextlib.contextmanager
def _suspend_gc():
    """Pause cyclic GC for a short-lived burst of container allocations.

    Batch frame reads/writes allocate ~100 MB of short-lived objects in a
    tight loop; the resulting gen-0 collections cost ~40% of the unpickle
    loop (Rhodes R3: 470 -> 264 ms on the Monaco 1452-frame set) while no
    reference cycles are created. Restores the prior GC state, so nesting
    and callers that disabled GC themselves are safe.
    """
    if gc.isenabled():
        gc.disable()
        try:
            yield
        finally:
            gc.enable()
    else:
        yield


def _encode_sqlite_value(blob: bytes) -> bytes | str:
    """Serialize a JSON blob for the SQLite tier (compress large payloads)."""
    if len(blob) >= _SQLITE_COMPRESS_MIN_BYTES:
        if _ZSTD_COMPRESSOR is not None:
            return _ZSTD_COMPRESSOR.compress(blob)
        return zlib.compress(blob, _SQLITE_COMPRESS_LEVEL)
    return blob.decode("utf-8")


def _decode_sqlite_value(stored: Any) -> Any:
    """Invert :func:`_encode_sqlite_value`.

    ``bytes`` marks a compressed row (zstd frames start with a 4-byte magic;
    anything else is a legacy zlib row); ``str`` is plain legacy JSON. A
    corrupt compressed row raises ``ValueError`` so callers' existing error
    handling turns it into a cache miss.
    """
    if isinstance(stored, bytes):
        if _ZSTD_DECOMPRESSOR is not None and len(stored) >= 4 and stored[:4] == _ZSTD_MAGIC:
            try:
                return _ZSTD_DECOMPRESSOR.decompress(stored)
            except _zstandard.ZstdError as e:
                raise ValueError(f"Corrupt compressed cache entry: {e}") from e
        try:
            return zlib.decompress(stored)
        except zlib.error as e:
            raise ValueError(f"Corrupt compressed cache entry: {e}") from e
    return stored


# Sentinel returned by SessionMemo.get_fastest_lap_ref_if_current on a source-id miss.
FASTEST_LAP_REF_MISS: Any = object()

# JSON payload paths the session memo retains (plus any nested "DRIVER/..." path).
_MEMO_KNOWN_JSON_PATHS = frozenset(
    {
        "drivers.json",
        "rcm.json",
        "weather.json",
        "position.json",
        "car_data.json",
        "session_info.json",
        "session_laptimes.json",
    }
)


class LRUCache:
    """Thread-safe bounded LRU cache.

    The single LRU implementation backing both the :class:`Cache` in-memory
    tiers and the process-global backend lap caches.

    Reads support a lock-free fast path (``ordered=False``): ``OrderedDict.get``
    is atomic in CPython thanks to the GIL, so hot-path reads skip the lock
    entirely and LRU ordering is maintained through writes only.
    """

    def __init__(self, maxsize: int = MAX_CACHE_SIZE, *, lock: threading.Lock | None = None):
        """Initialize the cache.

        Args:
            maxsize: Maximum number of entries before oldest-first eviction.
            lock: Optional externally-owned lock (lets a container serialize its
                own operations against the same lock it exposes publicly).
        """
        self.cache: OrderedDict[Any, Any] = OrderedDict()
        self.maxsize = maxsize
        self.lock = lock if lock is not None else threading.Lock()

    def get(self, key: Any, *, ordered: bool = True) -> Any | None:
        """Get a value by key.

        Args:
            key: Cache key.
            ordered: When True (default), refresh the LRU position under the
                lock. When False, perform a lock-free read without touching
                LRU ordering (fast path for hot readers).
        """
        if not ordered:
            return self.cache.get(key)
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    def set(self, key: Any, value: Any) -> None:
        """Store a value, evicting the oldest entry when over capacity."""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.maxsize:
                self.cache.popitem(last=False)

    def clear(self) -> None:
        """Clear all cached items."""
        with self.lock:
            self.cache.clear()

    def pop(self, key: Any, default: Any = None) -> Any:
        """Remove key and return its value, or default when absent."""
        with self.lock:
            return self.cache.pop(key, default)

    def __contains__(self, key: Any) -> bool:
        return key in self.cache

    def __len__(self) -> int:
        return len(self.cache)

    def __iter__(self):
        """Lock-free key iteration.

        Concurrent mutation may raise ``RuntimeError``; callers that iterate
        without the lock must handle that (this matches the previous bare
        ``OrderedDict`` iteration semantics).
        """
        return iter(self.cache)


# Process-global backend lap caches (keyed by "{year}_{gp}_{session}_laps").
_global_lap_cache = LRUCache(maxsize=MAX_CACHE_SIZE)
_global_lap_cache_polars = LRUCache(maxsize=MAX_CACHE_SIZE)


def get_backend_lap_cache(lib: Literal["pandas", "polars"]) -> LRUCache:
    """Get the global lap cache instance for a specific DataFrame library."""
    return _global_lap_cache_polars if lib == "polars" else _global_lap_cache


def clear_lap_cache() -> None:
    """Clear both global backend lap caches (pandas and polars)."""
    _global_lap_cache.clear()
    _global_lap_cache_polars.clear()


class SessionMemo:
    """Per-session in-memory memo tier (one instance per ``tif1.core.Session``).

    Consolidates the session-scoped caches previously held as ~11 loose
    ``Session`` attributes behind one kind-based interface:

    - ``"json"``: path-keyed JSON payloads fetched during the session (only
      known payload paths or nested ``"DRIVER/..."`` paths are retained).
    - ``"telemetry_payload"``: ``(driver, lap)``-keyed raw telemetry payloads
      (non-empty dicts only).
    - ``"telemetry_df"``: ``(driver, lap)``-keyed materialized telemetry
      DataFrames.

    It also owns the fastest-lap-reference memo (tagged by laps/drivers source
    ids), the persistent-cache probe result (``has_session_data``), and
    per-driver telemetry failure tracking (counts, unavailable drivers, and
    suppressed-warning drivers).

    Not internally synchronized: the owning session's access patterns are
    effectively single-threaded per event loop.
    """

    def __init__(self) -> None:
        """Initialize empty memo state."""
        self._json_payloads: dict[str, dict[str, Any]] = {}
        self._telemetry_payloads: dict[tuple[str, int], dict[str, Any]] = {}
        self._telemetry_dfs: dict[tuple[str, int], Any] = {}
        self.fastest_lap_ref: tuple[str, int] | None = None
        self.fastest_lap_ref_laps_source_id: int | None = None
        self.fastest_lap_ref_driver_source_id: int | None = None
        self.has_session_data: bool | None = None
        self._telemetry_failure_counts: dict[str, int] = {}
        self._telemetry_unavailable_drivers: set[str] = set()
        self._telemetry_failure_suppressed_drivers: set[str] = set()

    def _store(self, kind: SessionMemoKind) -> dict:
        if kind == "json":
            return self._json_payloads
        if kind == "telemetry_payload":
            return self._telemetry_payloads
        if kind == "telemetry_df":
            return self._telemetry_dfs
        raise ValueError(f"Unknown session memo kind: {kind!r}")

    def get(self, kind: SessionMemoKind, key: Any) -> Any | None:
        """Get a memoized value by kind and key (None when absent)."""
        return self._store(kind).get(key)

    def contains(self, kind: SessionMemoKind, key: Any) -> bool:
        """Return True when kind/key is memoized."""
        return key in self._store(kind)

    def items(self, kind: SessionMemoKind):
        """Return the (key, value) items view for a kind."""
        return self._store(kind).items()

    def kind_size(self, kind: SessionMemoKind) -> int:
        """Return the number of memoized entries for a kind."""
        return len(self._store(kind))

    def set(self, kind: SessionMemoKind, key: Any, value: Any) -> bool:
        """Store a value under kind/key.

        Kind policies may reject values: ``"json"`` retains dicts at known
        payload paths or nested paths only, and ``"telemetry_payload"``
        retains non-empty dicts only. ``"telemetry_df"`` accepts any value.

        Returns:
            True when the value was stored, False when a kind policy rejected it.
        """
        if kind == "json":
            if not isinstance(value, dict):
                return False
            if key in _MEMO_KNOWN_JSON_PATHS or "/" in key:
                self._json_payloads[key] = value
                return True
            return False
        if kind == "telemetry_payload":
            if isinstance(value, dict) and value:
                self._telemetry_payloads[key] = value
                return True
            return False
        if kind == "telemetry_df":
            self._telemetry_dfs[key] = value
            return True
        raise ValueError(f"Unknown session memo kind: {kind!r}")

    def get_fastest_lap_ref_if_current(
        self, source_kind: FastestLapRefKind, source_id: int
    ) -> Any | tuple[str, int] | None:
        """Return the memoized fastest-lap ref when its source tag matches.

        Args:
            source_kind: "laps" to match against the loaded-laps source id,
                "drivers" to match against the drivers-payload source id.
            source_id: ``id()`` of the current source object.

        Returns:
            The memoized ``(driver, lap)`` ref (which is never None for a
            matching tag), or :data:`FASTEST_LAP_REF_MISS` on a source-id miss.
        """
        if source_kind == "laps":
            if self.fastest_lap_ref_laps_source_id == source_id:
                return self.fastest_lap_ref
        elif self.fastest_lap_ref_driver_source_id == source_id:
            return self.fastest_lap_ref
        return FASTEST_LAP_REF_MISS

    def set_fastest_lap_ref(
        self,
        ref: tuple[str, int] | None,
        *,
        source_kind: FastestLapRefKind,
        source_id: int | None,
    ) -> None:
        """Memoize the fastest-lap ref, tagged by the source it was derived from.

        Stores the source tag only when the ref is not None, and clears the
        other source kind's tag so a stale source can never match.
        """
        self.fastest_lap_ref = ref
        if source_kind == "laps":
            self.fastest_lap_ref_laps_source_id = source_id if ref is not None else None
            self.fastest_lap_ref_driver_source_id = None
        else:
            self.fastest_lap_ref_driver_source_id = source_id if ref is not None else None
            self.fastest_lap_ref_laps_source_id = None

    def record_telemetry_failure(self, driver: str) -> int:
        """Increment and return the per-driver telemetry failure count.

        Drivers reaching 3 failures are marked telemetry-unavailable so
        subsequent fetches for them can be short-circuited.
        """
        count = self._telemetry_failure_counts.get(driver, 0) + 1
        self._telemetry_failure_counts[driver] = count
        if count >= 3:
            self._telemetry_unavailable_drivers.add(driver)
        return count

    def telemetry_failure_count(self, driver: str) -> int:
        """Return the number of recorded telemetry failures for a driver."""
        return self._telemetry_failure_counts.get(driver, 0)

    def is_telemetry_unavailable(self, driver: str) -> bool:
        """Return True when telemetry fetches should be short-circuited for a driver."""
        return driver in self._telemetry_unavailable_drivers

    def is_failure_suppressed(self, driver: str) -> bool:
        """Return True when further failure warnings for a driver were already suppressed."""
        return driver in self._telemetry_failure_suppressed_drivers

    def suppress_failure_warnings(self, driver: str) -> None:
        """Mark a driver's further telemetry failure warnings as suppressed."""
        self._telemetry_failure_suppressed_drivers.add(driver)

    def clear(self) -> None:
        """Drop all memoized state (payloads, fastest-lap ref, probe result, failures)."""
        self._json_payloads.clear()
        self._telemetry_payloads.clear()
        self._telemetry_dfs.clear()
        self.fastest_lap_ref = None
        self.fastest_lap_ref_laps_source_id = None
        self.fastest_lap_ref_driver_source_id = None
        self.has_session_data = None
        self._telemetry_failure_counts.clear()
        self._telemetry_unavailable_drivers.clear()
        self._telemetry_failure_suppressed_drivers.clear()


class Cache:
    """Cache with SQLite backend, in-memory LRU tiers, and async support.

    Both in-memory tiers are :class:`LRUCache` instances. Reads are lock-free
    on the hot path (``LRUCache.get(..., ordered=False)``); the SQLite tier is
    consulted only on memory misses.

    The unified interface is :meth:`get_entry` / :meth:`set_entry` /
    :meth:`invalidate`; ``get``/``set``/``get_telemetry``/``set_telemetry``
    are thin wrappers over it.
    """

    def __init__(self, cache_dir: Path | None = None):
        """Initialize cache with optional custom directory.

        Args:
            cache_dir: Cache directory path
        """
        if cache_dir is None:
            from .config import get_config

            config = get_config()
            configured_path = config.get("cache_dir")
            cache_dir = Path(str(configured_path)).expanduser()

        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        try:
            # Harden permissions to 0o700 (user read/write/execute only)
            self.cache_dir.chmod(0o700)
        except OSError:
            logger.debug(f"Failed to set restricted permissions on {self.cache_dir}")

        self.conn = None
        self.read_only = False
        self._pending_writes = 0
        self._memory_cache_lock = threading.Lock()  # Lock for memory cache operations
        self._sqlite_lock = threading.Lock()  # Lock for SQLite operations

        # Load config values for cache constants
        from .config import get_config

        config = get_config()
        self._commit_interval = config.get("cache_commit_interval", 100)
        self._sqlite_timeout = config.get("sqlite_timeout", 30.0)
        self._memory_cache_max_items = config.get("memory_cache_max_items", 1024)
        self._memory_telemetry_cache_max_items = config.get(
            "memory_telemetry_cache_max_items", 2048
        )
        self._parsed_cache_max_items = _PARSED_CACHE_MAX_ITEMS
        self._parsed_telemetry_cache_max_items = _PARSED_TELEMETRY_CACHE_MAX_ITEMS
        self._missing_payloads_ttl_days = config.get("missing_payloads_ttl_days", 7.0)

        # Negative-result tier: payloads that returned 4xx on every CDN source
        # (all-missing verdicts). Lazily loaded from SQLite; key -> recorded_at.
        self._missing_payloads: dict[str, float] | None = None

        # Shared write lock for both memory tiers (serializes updates exactly
        # like the previous single OrderedDict lock). Kept separate from the
        # public _memory_cache_lock so compound operations can hold the latter
        # while mutating tiers without deadlocking on the non-reentrant locks.
        self._lru_lock = threading.Lock()
        self._memory_cache: LRUCache = LRUCache(
            maxsize=self._memory_cache_max_items, lock=self._lru_lock
        )
        self._memory_telemetry_cache: LRUCache = LRUCache(
            maxsize=self._memory_telemetry_cache_max_items, lock=self._lru_lock
        )
        self._parsed_cache: LRUCache = LRUCache(
            maxsize=self._parsed_cache_max_items, lock=self._lru_lock
        )
        self._parsed_telemetry_cache: LRUCache = LRUCache(
            maxsize=self._parsed_telemetry_cache_max_items, lock=self._lru_lock
        )

        self._init_sqlite()

    def _init_sqlite(self):
        """Initialize SQLite backend with WAL mode for better concurrency."""
        self.db_path = self.cache_dir / "cache.sqlite"
        conn = None
        try:
            conn = sqlite3.connect(
                str(self.db_path), check_same_thread=False, timeout=self._sqlite_timeout
            )

            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache

            conn.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, data TEXT)")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS missing_payloads ("
                "key TEXT PRIMARY KEY, recorded_at REAL)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry_frames (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    driver TEXT,
                    lap INTEGER,
                    frame BLOB,
                    PRIMARY KEY (year, gp, session, driver, lap)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry_frames_pl (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    driver TEXT,
                    lap INTEGER,
                    frame BLOB,
                    PRIMARY KEY (year, gp, session, driver, lap)
                )
                """
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_cache (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    driver TEXT,
                    lap INTEGER,
                    data TEXT,
                    PRIMARY KEY (year, gp, session, driver, lap)
                )
            """)
            conn.commit()

            # Only assign to self.conn after full success
            self.conn = conn
            self._pending_writes = 0
            logger.debug(f"SQLite cache initialized at {self.db_path} with WAL mode")
        except (OSError, sqlite3.Error, TypeError, ValueError) as e:
            # Cleanup on failure
            if conn is not None:
                try:
                    conn.close()
                except (OSError, sqlite3.Error):
                    pass
            self.conn = None
            logger.warning(f"SQLite cache unavailable: {e}")

    def _commit_if_needed(self, force: bool = False) -> None:
        """Commit batched writes periodically to reduce fsync overhead."""
        if self.conn is None:
            return
        if force or self._pending_writes >= self._commit_interval:
            self.conn.commit()
            self._pending_writes = 0

    def _get_from_memory(self, key: str) -> Any | None:
        """Get cache entry from in-memory LRU only (no SQLite access).

        Uses truly lock-free reads for maximum concurrency. OrderedDict.get() is
        atomic in CPython due to the GIL. No LRU updates on reads to avoid lock
        contention. LRU ordering is maintained only through writes.

        Args:
            key: Cache key to lookup

        Returns:
            Cached data or None if not found
        """
        if self.conn is None:
            return None
        try:
            # Parsed-object front tier: a repeat hit skips orjson entirely.
            parsed = self._parsed_cache.get(key, ordered=False)
            if parsed is not None:
                return parsed

            # Completely lock-free read - no LRU update
            json_data = self._memory_cache.get(key, ordered=False)

            if json_data is not None:
                parsed = json_loads(json_data)
                self._parsed_cache.set(key, parsed)
                return parsed

            return None
        except (RuntimeError, TypeError, ValueError) as e:
            logger.debug("Memory cache read error for %s: %s", key, e)
            return None

    def get_entry(self, kind: CacheKind, key: Any) -> Any | None:
        """Unified kind-based cache read (thread-safe).

        Args:
            kind: "json" for path-keyed session JSON payloads, "telemetry" for
                per-lap telemetry payloads.
            key: Cache key — a ``str`` for "json" or a
                ``(year, gp, session, driver, lap)`` tuple for "telemetry".

        Returns:
            Cached data or None if not found
        """
        if kind == "json":
            return self._get_json(key)
        if kind == "telemetry":
            year, gp, session, driver, lap = key
            return self._get_telemetry(year, gp, session, driver, lap)
        raise ValueError(f"Unknown cache kind: {kind!r}")

    def set_entry(self, kind: CacheKind, key: Any, value: Any) -> None:
        """Unified kind-based cache write (thread-safe).

        Args:
            kind: "json" or "telemetry" (see :meth:`get_entry`).
            key: Cache key (see :meth:`get_entry`).
            value: Value to cache (must be JSON-serializable).
        """
        if kind == "json":
            self._set_json(key, value)
            return
        if kind == "telemetry":
            year, gp, session, driver, lap = key
            self._set_telemetry(year, gp, session, driver, lap, value)
            return
        raise ValueError(f"Unknown cache kind: {kind!r}")

    # -- negative-result tier -----------------------------------------------

    def _load_missing_payloads(self) -> dict[str, float]:
        """Load the all-4xx verdicts once per process (TTL-filtered)."""
        if self._missing_payloads is not None:
            return self._missing_payloads
        missing: dict[str, float] = {}
        if self.conn is not None:
            try:
                cutoff = time.time() - self._missing_payloads_ttl_days * 86400.0
                with self._sqlite_lock:
                    rows = self.conn.execute(
                        "SELECT key, recorded_at FROM missing_payloads WHERE recorded_at > ?",
                        (cutoff,),
                    ).fetchall()
                missing = dict(rows)
            except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
                logger.debug("Missing-payloads load failed: %s", e)
        self._missing_payloads = missing
        return missing

    def is_known_missing(self, key: str) -> bool:
        """Return True when ``key`` was refused with 4xx by every CDN recently.

        The verdict is TTL-scoped (``missing_payloads_ttl_days``, default 7
        days): after that the payload is re-probed on the next request.
        """
        if self.conn is None:
            return False
        recorded = self._load_missing_payloads().get(key)
        if recorded is None:
            return False
        if time.time() - recorded > self._missing_payloads_ttl_days * 86400.0:
            # Expired verdict: drop it so the next request re-probes.
            self.discard_missing(key)
            return False
        return True

    def record_missing(self, key: str) -> None:
        """Persist an everywhere-missing (all-CDN 4xx) verdict for ``key``."""
        if self.conn is None or self.read_only:
            return
        now = time.time()
        try:
            missing = self._load_missing_payloads()
            missing[key] = now
            with self._sqlite_lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO missing_payloads VALUES (?, ?)", (key, now)
                )
                self._pending_writes += 1
                self._commit_if_needed()
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Missing-payload record skipped: %s", e)

    def discard_missing(self, key: str) -> None:
        """Drop a missing-verdict (the payload exists again or was invalidated)."""
        missing = self._missing_payloads
        if missing is None:
            # Not loaded yet: load once so the membership guard below can
            # skip the DELETE for the overwhelmingly common no-verdict case
            # (every successful fetch calls this; the SQL write must not run).
            missing = self._load_missing_payloads()
        if key not in missing:
            return
        missing.pop(key, None)
        if self.conn is None or self.read_only:
            return
        try:
            with self._sqlite_lock:
                self.conn.execute("DELETE FROM missing_payloads WHERE key = ?", (key,))
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Missing-payload discard skipped: %s", e)

    def invalidate(self, scope: InvalidateScope = "all") -> None:
        """Invalidate cached data by scope.

        Args:
            scope: "all" clears every tier (memory + SQLite, JSON and
                telemetry, equivalent to :meth:`clear`); "memory" clears only
                the in-memory LRU tiers (SQLite persists); "json" clears the
                JSON payload tier (memory + SQLite); "telemetry" clears the
                telemetry tier (memory + SQLite).
        """
        # Ithome U5: a pending background frame write must land (or be
        # dropped) before rows are wiped, otherwise it would re-insert
        # cleared rows afterwards.
        _join_frame_writers()
        if scope == "all":
            self.clear()
            return
        if scope == "memory":
            with self._memory_cache_lock:
                self._memory_cache.clear()
                self._memory_telemetry_cache.clear()
                self._parsed_cache.clear()
                self._parsed_telemetry_cache.clear()
            logger.info("Memory cache tiers invalidated")
            return
        if scope not in ("json", "telemetry"):
            raise ValueError(f"Unknown invalidate scope: {scope!r}")
        if self.conn is None or self.read_only:
            logger.warning("Cannot invalidate cache (no connection or read-only mode)")
            return

        table = "cache" if scope == "json" else "telemetry_cache"
        memory_tier = self._memory_cache if scope == "json" else self._memory_telemetry_cache
        parsed_tier = self._parsed_cache if scope == "json" else self._parsed_telemetry_cache
        with self._sqlite_lock:
            self.conn.execute(f"DELETE FROM {table}")
            # Missing-verdict keys are JSON-tier-shaped even for telemetry
            # paths, so dropping either tier must also drop its verdicts.
            self.conn.execute("DELETE FROM missing_payloads")
            if scope == "telemetry":
                # Frames are derived from telemetry payloads.
                self.conn.execute("DELETE FROM telemetry_frames")
            self.conn.commit()
            self._pending_writes = 0
        with self._memory_cache_lock:
            memory_tier.clear()
            parsed_tier.clear()
            if self._missing_payloads is not None:
                self._missing_payloads.clear()
        logger.info("Cache tier invalidated: %s", scope)

    def _get_json(self, key: str) -> Any | None:
        """Get cached JSON payload (thread-safe); core of ``get``."""
        if self.conn is None:
            return None
        try:
            # Try lock-free memory read first
            result = self._get_from_memory(key)
            if result is not None:
                logger.debug("Cache hit (memory): %s", key)
                return result

            # Memory cache miss - check SQLite with lock
            json_data = None
            with self._sqlite_lock:
                result = self.conn.execute(
                    "SELECT data FROM cache WHERE key = ?", (key,)
                ).fetchone()
                if result:
                    json_data = _decode_sqlite_value(result[0])

            # Update memory cache outside SQLite lock
            if json_data is not None:
                self._memory_cache.set(key, json_data)
                parsed = json_loads(json_data)
                self._parsed_cache.set(key, parsed)
                logger.debug("Cache hit (SQLite): %s", key)
                return parsed

            logger.debug("Cache miss: %s", key)
            return None
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Cache read error for %s: %s", key, e)
            return None

    def get(self, key: str) -> Any | None:
        """Get cached data (thread-safe).

        Uses lock-free memory cache reads for performance. Only acquires lock
        for SQLite access and LRU updates.
        Thin wrapper over ``get_entry("json", key)``.

        Args:
            key: Cache key to lookup

        Returns:
            Cached data or None if not found
        """
        return self.get_entry("json", key)

    async def get_async(self, key: str) -> Any | None:
        """Get cached data asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get, key)

    def _store_json_blob(self, key: str, blob: bytes) -> None:
        """Write a serialized JSON blob to both tiers (thread-safe); core of ``set``.

        The memory tier keeps the plain blob; the SQLite tier stores it
        compressed when large (see :func:`_encode_sqlite_value`).
        """
        if self.conn is None or self.read_only:
            return
        try:
            # Update memory cache first (fast operation, <1ms)
            self._memory_cache.set(key, blob)
            # The payload exists: drop any stale everywhere-missing verdict.
            if self._missing_payloads is not None:
                self._missing_payloads.pop(key, None)

            # Then update SQLite (slower operation)
            with self._sqlite_lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO cache VALUES (?, ?)", (key, _encode_sqlite_value(blob))
                )
                self._pending_writes += 1
                self._commit_if_needed()

            logger.debug("Cached: %s", key)
        except (RuntimeError, TypeError, ValueError, sqlite3.Error):
            logger.debug("Cache write skipped: %s", key)

    def _set_json(self, key: str, data: Any) -> None:
        """Set cached JSON payload (thread-safe); core of ``set``.

        The parsed-object tier is read-through: writes drop any parsed entry
        instead of populating it, so stale parsed objects can never survive.
        """
        self._store_json_blob(key, json_dumps_bytes(data))
        self._parsed_cache.pop(key, None)

    def set(self, key: str, data: Any) -> None:
        """Set cached data (thread-safe).

        Optimized to update memory cache first (fast operation, <1ms) before SQLite (slower).
        This ensures minimal lock duration for memory operations.
        Thin wrapper over ``set_entry("json", key, data)``.
        """
        self.set_entry("json", key, data)

    def set_raw(self, key: str, blob: bytes | bytearray | memoryview) -> None:
        """Set cached data from an already-serialized JSON blob (thread-safe).

        Skips re-serialization when the caller still holds the original payload
        bytes (e.g. the async fetch pipeline with validation disabled). The
        SQLite tier compresses large blobs exactly like :meth:`set`. The
        parsed-object entry (if any) is dropped; reads repopulate it.
        """
        self._store_json_blob(key, bytes(blob))
        self._parsed_cache.pop(key, None)

    async def set_async(self, key: str, data: Any) -> None:
        """Set cached data asynchronously."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.set, key, data)

    def _get_telemetry(self, year: int, gp: str, session: str, driver: str, lap: int) -> Any | None:
        """Get cached telemetry data (thread-safe); core of ``get_telemetry``."""
        if self.conn is None:
            return None
        try:
            cache_key = (year, gp, session, driver, lap)

            # Parsed-object front tier: a repeat hit skips orjson entirely.
            parsed = self._parsed_telemetry_cache.get(cache_key, ordered=False)
            if parsed is not None:
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                return parsed

            # Completely lock-free memory cache read - no LRU update
            json_data = self._memory_telemetry_cache.get(cache_key, ordered=False)

            if json_data is not None:
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                parsed = json_loads(json_data)
                self._parsed_telemetry_cache.set(cache_key, parsed)
                return parsed

            # Memory cache miss - check SQLite with lock
            with self._sqlite_lock:
                result = self.conn.execute(
                    "SELECT data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? AND driver = ? AND lap = ?",
                    cache_key,
                ).fetchone()
                if result:
                    json_data = _decode_sqlite_value(result[0])

            # Update memory cache outside SQLite lock
            if json_data is not None:
                self._memory_telemetry_cache.set(cache_key, json_data)
                parsed = json_loads(json_data)
                self._parsed_telemetry_cache.set(cache_key, parsed)
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                return parsed

            return None
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Telemetry cache read error: %s", e)
            return None

    def get_telemetry(self, year: int, gp: str, session: str, driver: str, lap: int) -> Any | None:
        """Get cached telemetry data (thread-safe).

        Uses lock-free memory cache reads for performance. Only acquires lock
        for SQLite access and LRU updates.
        Thin wrapper over ``get_entry("telemetry", (year, gp, session, driver, lap))``.

        Args:
            year: Season year
            gp: Grand Prix identifier
            session: Session type
            driver: Driver code
            lap: Lap number

        Returns:
            Cached telemetry data or None if not found
        """
        return self.get_entry("telemetry", (year, gp, session, driver, lap))

    async def get_telemetry_async(
        self, year: int, gp: str, session: str, driver: str, lap: int
    ) -> Any | None:
        """Get cached telemetry data asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_telemetry, year, gp, session, driver, lap)

    def get_telemetry_batch(
        self, year: int, gp: str, session: str, driver_laps: list[tuple[str, int]]
    ) -> dict[tuple[str, int], Any]:
        """Get multiple cached telemetry entries in a single batch (thread-safe)."""
        if self.conn is None or not driver_laps:
            return {}

        results = {}
        misses = []

        # 1. Try memory cache first (lock-free), parsed tier before blob tier
        for driver, lap in driver_laps:
            key = (year, gp, session, driver, lap)
            parsed = self._parsed_telemetry_cache.get(key, ordered=False)
            if parsed is not None:
                results[(driver, lap)] = parsed
                continue
            json_data = self._memory_telemetry_cache.get(key, ordered=False)
            if json_data:
                parsed = json_loads(json_data)
                self._parsed_telemetry_cache.set(key, parsed)
                results[(driver, lap)] = parsed
            else:
                misses.append((driver, lap))

        if not misses:
            return results

        # 2. Check SQLite for misses using IN clause for batch lookup
        try:
            with self._sqlite_lock:
                placeholders = ", ".join(["(?, ?)" for _ in misses])
                params = [year, gp, session]
                for driver_code, lap_num in misses:
                    params.extend([driver_code, lap_num])
                query = f"SELECT driver, lap, data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? AND (driver, lap) IN ({placeholders})"
                rows = self.conn.execute(query, params).fetchall()

            if rows:
                with _suspend_gc():
                    for driver_code, lap_num, json_data in rows:
                        decoded = _decode_sqlite_value(json_data)
                        self._memory_telemetry_cache.set(
                            (year, gp, session, driver_code, lap_num),
                            decoded,
                        )
                        parsed = json_loads(decoded)
                        self._parsed_telemetry_cache.set(
                            (year, gp, session, driver_code, lap_num), parsed
                        )
                        results[(driver_code, lap_num)] = parsed
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Telemetry batch cache read error: %s", e)

        return results

    async def get_telemetry_batch_async(
        self, year: int, gp: str, session: str, driver_laps: list[tuple[str, int]]
    ) -> dict[tuple[str, int], Any]:
        """Get multiple cached telemetry entries asynchronously in a single batch."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self.get_telemetry_batch, year, gp, session, driver_laps
        )

    # -- materialized telemetry-frame tier ------------------------------------
    #
    # Warm-load fast path: assembled per-(driver, lap) pandas DataFrames,
    # pickled and zstd-compressed. The payload tier stays the source of
    # truth; frames are derived data written on warm loads only, so cold
    # starts pay nothing for this tier. Unpickling trusts the same 0o700
    # user-local cache directory every other tier already trusts (a
    # corrupted or foreign blob degrades to a tier miss, never a crash).

    def get_telemetry_frames_batch(
        self,
        year: int,
        gp: str,
        session: str,
        driver_laps: list[tuple[str, int]],
        lib: Literal["pandas", "polars"] = "pandas",
    ) -> dict[tuple[str, int], Any]:
        """Batch-read materialized telemetry frames.

        Args:
            lib: Backend whose frame table to read (``pandas`` or ``polars``);
                rows from the other backend are never returned because a
                foreign frame object would surface in caller maps.

        Returns:
            Mapping of ``(driver, lap)`` to the unpickled DataFrame; refs with
            no (or corrupt) frame rows are absent and fall through to the
            payload tier.
        """
        if self.conn is None or not driver_laps or _ZSTD_DECOMPRESSOR is None:
            return {}
        table = _FRAME_TABLES.get(lib, "telemetry_frames")
        results: dict[tuple[str, int], Any] = {}
        try:
            with self._sqlite_lock:
                placeholders = ", ".join(["(?, ?)"] * len(driver_laps))
                params: list[Any] = [year, gp, session]
                for driver_code, lap_num in driver_laps:
                    params.extend([driver_code, lap_num])
                query = (
                    f"SELECT driver, lap, frame FROM {table} WHERE year = ? AND gp = ? "
                    f"AND session = ? AND (driver, lap) IN ({placeholders})"
                )
                rows = self.conn.execute(query, params).fetchall()
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Telemetry frames batch read error: %s", e)
            return results
        # ~100 MB of short-lived allocations; GC suspension halves this loop
        # (Rhodes R3) and pickle data holds no reference cycles.
        _foreign_marker = "_df" if lib == "polars" else "_mgr"
        with _suspend_gc():
            for driver_code, lap_num, blob in rows:
                try:
                    frame = pickle.loads(_decompress_frame_blob(blob))
                    # A foreign-backend blob (hand-migrated cache) degrades to
                    # a tier miss instead of surfacing in caller maps.
                    if not hasattr(frame, _foreign_marker):
                        raise TypeError("foreign frame object")
                    results[(driver_code, lap_num)] = frame
                except (
                    AttributeError,
                    EOFError,
                    ImportError,
                    IndexError,
                    TypeError,
                    ValueError,
                    pickle.UnpicklingError,
                    _zstandard.ZstdError,
                ):
                    logger.debug("Corrupt telemetry frame row skipped: %s/%s", driver_code, lap_num)
        return results

    def set_telemetry_frames_batch(
        self,
        year: int,
        gp: str,
        session: str,
        frames: list[tuple[str, int, Any]],
        lib: Literal["pandas", "polars"] = "pandas",
    ) -> int:
        """Materialize assembled telemetry frames in one bulk write.

        Args:
            frames: ``(driver, lap, DataFrame)`` tuples to persist.
            lib: Backend whose frame table to write (``pandas`` or ``polars``).

        Returns:
            Number of frames written (0 when the tier is unavailable).
        """
        if self.conn is None or self.read_only or _ZSTD_COMPRESSOR is None or not frames:
            return 0
        table = _FRAME_TABLES.get(lib, "telemetry_frames")
        written = 0
        try:
            with self._sqlite_lock:
                # executemany batches the 1400+ per-session frame rows into
                # one C-level loop (measured ~0.2 s vs ~0.36 s per-row execute
                # on the Monaco 1452-frame set). Serialization inside
                # _suspend_gc: the pickle+zstd burst allocates heavily and GC
                # pauses cost ~25% of this pass (Rhodes R3).
                with _suspend_gc():
                    self.conn.executemany(
                        f"INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            (
                                year,
                                gp,
                                session,
                                driver,
                                lap,
                                _compress_frame_blob(pickle.dumps(frame, protocol=5)),
                            )
                            for driver, lap, frame in frames
                        ),
                    )
                written = len(frames)
                self._pending_writes += written
                self._commit_if_needed()
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Telemetry frames write skipped: %s", e)
            return 0
        return written

    def _set_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        """Set cached telemetry data (thread-safe); core of ``set_telemetry``."""
        if self.conn is None or self.read_only:
            return
        try:
            blob = json_dumps_bytes(data)
            cache_key = (year, gp, session, driver, lap)

            # Update memory cache first (fast operation, <1ms); plain blob.
            # Parsed tier is read-through: drop any parsed entry on write.
            self._memory_telemetry_cache.set(cache_key, blob)
            self._parsed_telemetry_cache.pop(cache_key, None)

            # Then update SQLite (slower operation); compressed when large.
            with self._sqlite_lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO telemetry_cache VALUES (?, ?, ?, ?, ?, ?)",
                    (*cache_key, _encode_sqlite_value(blob)),
                )
                self._pending_writes += 1
                self._commit_if_needed()

            logger.debug("Telemetry cached: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Telemetry cache write skipped: %s", e)

    def set_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        """Set cached telemetry data (thread-safe).

        Optimized to update memory cache first (fast, <1ms) before SQLite (slower).
        This ensures minimal lock duration for memory operations.
        Thin wrapper over ``set_entry("telemetry", (year, gp, session, driver, lap), data)``.
        """
        self.set_entry("telemetry", (year, gp, session, driver, lap), data)

    async def set_telemetry_async(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        """Set cached telemetry data asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.set_telemetry, year, gp, session, driver, lap, data
        )

    def has_session_data(self, year: int, gp: str, session: str) -> bool:
        """Return True when JSON or telemetry cache contains entries for a session."""
        if self.conn is None:
            return False

        prefix = f"{year}/{gp}/{session}/"
        try:
            # Check memory cache first (lock-free read)
            if any(str(key).startswith(prefix) for key in self._memory_cache):
                return True

            # Check SQLite with appropriate lock
            with self._sqlite_lock:
                cache_hit = self.conn.execute(
                    "SELECT 1 FROM cache WHERE key LIKE ? LIMIT 1",
                    (f"{prefix}%",),
                ).fetchone()
                if cache_hit is not None:
                    return True

                telemetry_hit = self.conn.execute(
                    "SELECT 1 FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? LIMIT 1",
                    (year, gp, session),
                ).fetchone()
                return telemetry_hit is not None
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug(
                "Session cache availability probe failed for %s/%s/%s: %s",
                year,
                gp,
                session,
                e,
            )
            # Default to True on probe failures to preserve cache-read behavior.
            return True

    def clear(self) -> None:
        """Clear all cached data."""
        # Ithome U5: join pending background frame writers first so a
        # post-return write cannot re-insert rows after the wipe.
        _join_frame_writers()
        if self.conn is None or self.read_only:
            logger.warning("Cannot clear cache (no connection or read-only mode)")
            return

        # Acquire both locks to ensure consistency
        with self._sqlite_lock:
            self.conn.execute("DELETE FROM cache")
            self.conn.execute("DELETE FROM telemetry_cache")
            self.conn.execute("DELETE FROM telemetry_frames")
            self.conn.execute("DELETE FROM missing_payloads")
            self.conn.commit()
            self._pending_writes = 0

        with self._memory_cache_lock:
            self._memory_cache.clear()
            self._memory_telemetry_cache.clear()
            self._parsed_cache.clear()
            self._parsed_telemetry_cache.clear()
            if self._missing_payloads is not None:
                self._missing_payloads.clear()

        logger.info("Cache cleared")

    def close(self) -> None:
        """Close database connection."""
        # Ithome U5: background frame writers must complete before the
        # connection closes (atexit calls this), or their writes would be
        # dropped mid-flight.
        _join_frame_writers()
        # Acquire both locks to ensure clean shutdown.
        # Snapshot `self.conn` inside the sqlite lock so concurrent close() calls
        # cannot race into `None.close()`.
        with self._sqlite_lock:
            conn = self.conn
            if conn is not None:
                try:
                    self._commit_if_needed(force=True)
                    conn.close()
                except (OSError, RuntimeError, sqlite3.Error) as e:
                    logger.warning("Error closing cache connection: %s", e)
                finally:
                    self.conn = None

        with self._memory_cache_lock:
            self._memory_cache.clear()
            self._memory_telemetry_cache.clear()
            self._parsed_cache.clear()
            self._parsed_telemetry_cache.clear()

        if conn is not None:
            logger.debug("Cache connection closed")

    def __del__(self):
        """Destructor to ensure connection is closed."""
        self.close()


_cache = None
_cache_lock = threading.Lock()

# Ithome U5: post-return background frame-tier writes. fetch_all returns as
# soon as frames are assembled; the bulk tier write runs in a daemon thread
# and is joined by close()/invalidate() so the connection never closes (nor a
# clear wipes rows) under a live writer. A writer registered after close
# finds conn=None and drops out via the existing set_telemetry_frames_batch
# guard.
_frame_writers: list[threading.Thread] = []
_frame_writers_lock = threading.Lock()


def register_frame_writer(write_fn) -> None:
    """Start a daemon frame-tier writer thread and register it for joining."""
    thread = threading.Thread(target=write_fn, name="tif1-frame-write", daemon=True)
    with _frame_writers_lock:
        _frame_writers.append(thread)
    thread.start()


def _join_frame_writers() -> None:
    """Wait for registered frame writers (close/invalidate paths)."""
    with _frame_writers_lock:
        writers = list(_frame_writers)
        _frame_writers.clear()
    for writer in writers:
        if writer.is_alive():
            writer.join()


def get_cache() -> Cache:
    """Get global cache instance (lazy initialization, thread-safe)."""
    global _cache
    if _cache is not None:
        return _cache

    with _cache_lock:
        if _cache is None:
            _cache = Cache()
        return _cache


def _cleanup_cache() -> None:
    """Cleanup global cache on exit."""
    global _cache
    if _cache is not None:
        _cache.close()
        _cache = None


atexit.register(_cleanup_cache)
