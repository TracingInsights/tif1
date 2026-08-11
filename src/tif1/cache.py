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
import logging
import sqlite3
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any, Literal

from .core_utils.constants import MAX_CACHE_SIZE
from .core_utils.json_utils import json_dumps, json_loads

logger = logging.getLogger(__name__)


TelemetryCacheKey = tuple[int, str, str, str, int]
CacheKind = Literal["json", "telemetry"]
InvalidateScope = Literal["all", "memory", "json", "telemetry"]
SessionMemoKind = Literal["json", "telemetry_payload", "telemetry_df"]
FastestLapRefKind = Literal["laps", "drivers"]

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
        self._commit_interval = config.get("cache_commit_interval", 25)
        self._sqlite_timeout = config.get("sqlite_timeout", 30.0)
        self._memory_cache_max_items = config.get("memory_cache_max_items", 1024)
        self._memory_telemetry_cache_max_items = config.get(
            "memory_telemetry_cache_max_items", 2048
        )

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
            # Completely lock-free read - no LRU update
            json_data = self._memory_cache.get(key, ordered=False)

            if json_data is not None:
                return json_loads(json_data)

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

    def invalidate(self, scope: InvalidateScope = "all") -> None:
        """Invalidate cached data by scope.

        Args:
            scope: "all" clears every tier (memory + SQLite, JSON and
                telemetry, equivalent to :meth:`clear`); "memory" clears only
                the in-memory LRU tiers (SQLite persists); "json" clears the
                JSON payload tier (memory + SQLite); "telemetry" clears the
                telemetry tier (memory + SQLite).
        """
        if scope == "all":
            self.clear()
            return
        if scope == "memory":
            with self._memory_cache_lock:
                self._memory_cache.clear()
                self._memory_telemetry_cache.clear()
            logger.info("Memory cache tiers invalidated")
            return
        if scope not in ("json", "telemetry"):
            raise ValueError(f"Unknown invalidate scope: {scope!r}")
        if self.conn is None or self.read_only:
            logger.warning("Cannot invalidate cache (no connection or read-only mode)")
            return

        table = "cache" if scope == "json" else "telemetry_cache"
        memory_tier = self._memory_cache if scope == "json" else self._memory_telemetry_cache
        with self._sqlite_lock:
            self.conn.execute(f"DELETE FROM {table}")
            self.conn.commit()
            self._pending_writes = 0
        with self._memory_cache_lock:
            memory_tier.clear()
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
                    json_data = result[0]

            # Update memory cache outside SQLite lock
            if json_data is not None:
                self._memory_cache.set(key, json_data)
                logger.debug("Cache hit (SQLite): %s", key)
                return json_loads(json_data)

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

    def _set_json(self, key: str, data: Any) -> None:
        """Set cached JSON payload (thread-safe); core of ``set``."""
        if self.conn is None or self.read_only:
            return
        try:
            json_data = json_dumps(data)

            # Update memory cache first (fast operation, <1ms)
            self._memory_cache.set(key, json_data)

            # Then update SQLite (slower operation)
            with self._sqlite_lock:
                self.conn.execute("INSERT OR REPLACE INTO cache VALUES (?, ?)", (key, json_data))
                self._pending_writes += 1
                self._commit_if_needed()

            logger.debug("Cached: %s", key)
        except (RuntimeError, TypeError, ValueError, sqlite3.Error):
            logger.debug("Cache write skipped: %s", key)

    def set(self, key: str, data: Any) -> None:
        """Set cached data (thread-safe).

        Optimized to update memory cache first (fast, <1ms) before SQLite (slower).
        This ensures minimal lock duration for memory operations.
        Thin wrapper over ``set_entry("json", key, data)``.
        """
        self.set_entry("json", key, data)

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

            # Completely lock-free memory cache read - no LRU update
            json_data = self._memory_telemetry_cache.get(cache_key, ordered=False)

            if json_data is not None:
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                return json_loads(json_data)

            # Memory cache miss - check SQLite with lock
            with self._sqlite_lock:
                result = self.conn.execute(
                    "SELECT data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? AND driver = ? AND lap = ?",
                    cache_key,
                ).fetchone()
                if result:
                    json_data = result[0]

            # Update memory cache outside SQLite lock
            if json_data is not None:
                self._memory_telemetry_cache.set(cache_key, json_data)
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                return json_loads(json_data)

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

        # 1. Try memory cache first (lock-free)
        for driver, lap in driver_laps:
            key = (year, gp, session, driver, lap)
            json_data = self._memory_telemetry_cache.get(key, ordered=False)
            if json_data:
                results[(driver, lap)] = json_loads(json_data)
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
                for driver_code, lap_num, json_data in rows:
                    self._memory_telemetry_cache.set(
                        (year, gp, session, driver_code, lap_num),
                        json_data,
                    )
                    results[(driver_code, lap_num)] = json_loads(json_data)
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

    def _set_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        """Set cached telemetry data (thread-safe); core of ``set_telemetry``."""
        if self.conn is None or self.read_only:
            return
        try:
            json_data = json_dumps(data)
            cache_key = (year, gp, session, driver, lap)

            # Update memory cache first (fast operation, <1ms)
            self._memory_telemetry_cache.set(cache_key, json_data)

            # Then update SQLite (slower operation)
            with self._sqlite_lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO telemetry_cache VALUES (?, ?, ?, ?, ?, ?)",
                    (*cache_key, json_data),
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
        if self.conn is None or self.read_only:
            logger.warning("Cannot clear cache (no connection or read-only mode)")
            return

        # Acquire both locks to ensure consistency
        with self._sqlite_lock:
            self.conn.execute("DELETE FROM cache")
            self.conn.execute("DELETE FROM telemetry_cache")
            self.conn.commit()
            self._pending_writes = 0

        with self._memory_cache_lock:
            self._memory_cache.clear()
            self._memory_telemetry_cache.clear()

        logger.info("Cache cleared")

    def close(self) -> None:
        """Close database connection."""
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

        if conn is not None:
            logger.debug("Cache connection closed")

    def __del__(self):
        """Destructor to ensure connection is closed."""
        self.close()


_cache = None
_cache_lock = threading.Lock()


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
