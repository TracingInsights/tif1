"""Jolpica (``api.jolpi.ca``) client: URL building, rate limiting, paging.

Jolpica is the Ergast-compatible API fastf1 reads. tif1 only asks it what the
Stats repository cannot answer (see ``tif1.ergast.router``), so the traffic
stays well inside its unauthenticated limits: 4 requests per second and 500
per hour, HTTP 429 beyond that. The limiter here blocks before a request
would exceed either, a 429 honours ``Retry-After`` for up to three retries,
and the shared HTTP session sends the descriptive ``User-Agent`` Jolpica
requires.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any, Literal
from urllib.parse import urlencode

from tif1.config import get_config
from tif1.core_utils.runtime import IS_EMSCRIPTEN, can_block_on_awaitables
from tif1.ergast.payloads import ErgastPayloadLoader, get_ergast_loader
from tif1.exceptions import (
    DataNotFoundError,
    ErgastInvalidRequestError,
    ErgastJsonError,
    NetworkError,
    RateLimitExceededError,
)
from tif1.payload_loader import HttpTransport, default_transport

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.jolpi.ca/ergast/f1"
#: Jolpica caps ``limit`` at 100 rows per request.
MAX_PAGE_SIZE = 100
#: Jolpica's default page size when ``limit`` is omitted (fastf1's default too).
DEFAULT_PAGE_SIZE = 30
_MAX_429_RETRIES = 3
_CURRENT_TTL_SECONDS = 3600.0

#: Leaf lists that Jolpica pages across (a page boundary can split a race).
_RACE_LEAVES = ("Results", "QualifyingResults", "SprintResults", "Laps", "PitStops")
_STANDINGS_LEAVES = ("DriverStandings", "ConstructorStandings")


def base_url() -> str:
    """The configured Jolpica base URL (``jolpica_base_url``), without a trailing slash."""
    configured = get_config().get("jolpica_base_url") or DEFAULT_BASE_URL
    return str(configured).rstrip("/")


def build_url(
    endpoint: str | None,
    *,
    base: str | None = None,
    season: Literal["current"] | int | None = None,
    round: Literal["last"] | int | None = None,  # noqa: A002
    circuit: str | None = None,
    constructor: str | None = None,
    driver: str | None = None,
    grid_position: int | None = None,
    results_position: int | None = None,
    fastest_rank: int | None = None,
    status: str | None = None,
    lap_number: int | None = None,
    stop_number: int | None = None,
    standings_position: int | None = None,
) -> str:
    """Build an Ergast request URL exactly like fastf1's ``Ergast._build_url``."""
    selectors: list[str] = []

    if season is not None:
        selectors.append(f"/{season}")
    if round is not None:
        selectors.append(f"/{round}")
    if grid_position is not None:
        selectors.append(f"/grid/{grid_position}")
    if fastest_rank is not None:
        selectors.append(f"/fastest/{fastest_rank}")

    # Some specifiers double as the endpoint: when the endpoint matches, the
    # specifier is appended to it instead of being added as a selector.
    if driver is not None:
        if endpoint == "drivers":
            endpoint = f"drivers/{driver}"
        else:
            selectors.append(f"/drivers/{driver}")

    if constructor is not None:
        if endpoint == "constructors":
            endpoint = f"constructors/{constructor}"
        else:
            selectors.append(f"/constructors/{constructor}")

    if circuit is not None:
        if endpoint == "circuits":
            endpoint = f"circuits/{circuit}"
        else:
            selectors.append(f"/circuits/{circuit}")

    if status is not None:
        if endpoint == "status":
            endpoint = f"status/{status}"
        else:
            selectors.append(f"/status/{status}")

    if standings_position is not None:
        if endpoint == "driverStandings":
            endpoint = f"driverStandings/{standings_position}"
        elif endpoint == "constructorStandings":
            endpoint = f"constructorStandings/{standings_position}"

    if results_position is not None:
        if endpoint in ("results", "qualifying", "sprint"):
            endpoint = f"{endpoint}/{results_position}"
        else:
            selectors.append(f"/results/{results_position}")

    if lap_number is not None:
        if endpoint == "laps":
            endpoint = f"laps/{lap_number}"
        else:
            selectors.append(f"/laps/{lap_number}")

    if stop_number is not None:
        if endpoint == "pitstops":
            endpoint = f"pitstops/{stop_number}"
        else:
            selectors.append(f"/pitstops/{stop_number}")

    if endpoint is not None:
        selectors.append(f"/{endpoint}")

    return (base if base is not None else base_url()) + "".join(selectors) + ".json"


class RateLimiter:
    """Token bucket per second plus a sliding one-hour window, process-wide.

    ``acquire`` blocks until a request is allowed. Inside a browser without
    JavaScript Promise Integration nothing can block, so the limiter only
    records the request there.
    """

    def __init__(
        self,
        burst_per_second: int,
        hourly: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.burst_per_second = max(1, int(burst_per_second))
        self.hourly = max(1, int(hourly))
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._tokens = float(self.burst_per_second)
        self._last_refill = clock()
        self._window: deque[float] = deque()

    def _wait_time(self, now: float) -> float:
        elapsed = now - self._last_refill
        self._tokens = min(self.burst_per_second, self._tokens + elapsed * self.burst_per_second)
        self._last_refill = now
        while self._window and now - self._window[0] >= 3600.0:
            self._window.popleft()
        wait = 0.0
        if self._tokens < 1.0:
            wait = (1.0 - self._tokens) / self.burst_per_second
        if len(self._window) >= self.hourly:
            wait = max(wait, 3600.0 - (now - self._window[0]))
        return wait

    def acquire(self) -> float:
        """Block until a request may go out; return the seconds waited."""
        waited = 0.0
        can_block = not IS_EMSCRIPTEN or can_block_on_awaitables()
        while True:
            with self._lock:
                now = self._clock()
                wait = self._wait_time(now)
                if wait <= 0.0 or not can_block:
                    self._tokens -= 1.0
                    self._window.append(now)
                    return waited
            self._sleep(wait)
            waited += wait


_limiter: RateLimiter | None = None
_limiter_lock = threading.Lock()


def get_rate_limiter() -> RateLimiter:
    """The process-wide limiter built from ``ergast_rate_limit_burst``/``_hourly``."""
    global _limiter
    if _limiter is None:
        with _limiter_lock:
            if _limiter is None:
                config = get_config()
                _limiter = RateLimiter(
                    int(config.get("ergast_rate_limit_burst", 4) or 4),
                    int(config.get("ergast_rate_limit_hourly", 500) or 500),
                )
    return _limiter


def reset_rate_limiter() -> None:
    """Drop the process-wide limiter (tests and config changes)."""
    global _limiter
    _limiter = None


def merge_split_laps(laps: list[dict]) -> list[dict]:
    """Merge ``Laps`` entries that share a lap number (a page boundary split them).

    Jolpica pages over lap timings, so a lap can arrive as two entries with
    the same ``number``; the Stats repository stores the pages concatenated.
    Order is the first appearance of each lap; timings keep their order.
    """
    merged: dict[str, dict] = {}
    for lap in laps:
        number = str(lap.get("number"))
        existing = merged.get(number)
        if existing is None:
            merged[number] = {**lap, "Timings": list(lap.get("Timings", []))}
        else:
            existing["Timings"].extend(lap.get("Timings", []))
    return list(merged.values())


def _table_name(mrdata: dict) -> str | None:
    for key, value in mrdata.items():
        if key.endswith("Table") and isinstance(value, dict):
            return key
    return None


def _list_key(table: dict) -> str | None:
    for key, value in table.items():
        if isinstance(value, list):
            return key
    return None


def merge_pages(pages: list[dict]) -> dict:
    """Join Jolpica result pages into one payload with the first page's envelope.

    Races (``RaceTable``) and standings lists (``StandingsTable``) that span a
    page boundary are merged by ``(season, round)``; other tables simply
    extend their list. ``limit`` becomes the merged row count and ``offset``
    the first page's offset, so ``is_complete`` reads correctly.
    """
    if not pages:
        raise ValueError("No pages to merge")
    first = pages[0]["MRData"]
    table_name = _table_name(first)
    if table_name is None:
        return pages[0]
    merged_table = dict(first[table_name])
    list_key = _list_key(merged_table)
    if list_key is None:
        return pages[0]
    merged_items: list[dict] = []

    if table_name in ("RaceTable", "StandingsTable"):
        leaves = _RACE_LEAVES if table_name == "RaceTable" else _STANDINGS_LEAVES
        by_round: dict[tuple[str, str], dict] = {}
        for page in pages:
            table = page["MRData"].get(table_name, {})
            for item in table.get(list_key, []):
                key = (str(item.get("season")), str(item.get("round")))
                existing = by_round.get(key)
                if existing is None:
                    existing = dict(item)
                    by_round[key] = existing
                    merged_items.append(existing)
                    continue
                for leaf in leaves:
                    if leaf in item:
                        existing[leaf] = list(existing.get(leaf, [])) + list(item[leaf])
        for item in merged_items:
            if "Laps" in item:
                item["Laps"] = merge_split_laps(item["Laps"])
    else:
        for page in pages:
            table = page["MRData"].get(table_name, {})
            merged_items.extend(table.get(list_key, []))

    merged_table[list_key] = merged_items
    rows = _count_rows(table_name, list_key, merged_items)
    envelope = {k: v for k, v in first.items() if k != table_name}
    envelope["limit"] = str(rows)
    envelope["offset"] = str(first.get("offset", "0"))
    envelope[table_name] = merged_table
    return {"MRData": envelope}


def _count_rows(table_name: str, list_key: str, items: list[dict]) -> int:
    if table_name == "RaceTable":
        total = 0
        for item in items:
            counted = False
            for leaf in _RACE_LEAVES:
                if leaf in item:
                    counted = True
                    if leaf == "Laps":
                        total += sum(len(lap.get("Timings", [])) for lap in item[leaf])
                    else:
                        total += len(item[leaf])
            if not counted:
                total += 1
        return total
    if table_name == "StandingsTable":
        return sum(
            len(item.get(leaf, [])) for item in items for leaf in _STANDINGS_LEAVES if leaf in item
        )
    return len(items)


class JolpicaClient:
    """Rate-limited, cached access to the Jolpica API.

    Args:
        transport: HTTP seam (defaults to the runtime's production transport).
        loader: Cache pipeline (defaults to the shared Ergast loader).
        limiter: Rate limiter (defaults to the process-wide one).
        base: Base URL (defaults to ``jolpica_base_url``).
        timeout: Request timeout in seconds (defaults to ``ergast_timeout``).
    """

    def __init__(
        self,
        *,
        transport: HttpTransport | None = None,
        loader: ErgastPayloadLoader | None = None,
        limiter: RateLimiter | None = None,
        base: str | None = None,
        timeout: float | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._transport = transport
        self._loader = loader
        self._limiter = limiter
        self._base = base
        self._timeout = timeout
        self._sleep = sleep

    @property
    def transport(self) -> HttpTransport:
        if self._transport is None:
            self._transport = default_transport()
        return self._transport

    @property
    def loader(self) -> ErgastPayloadLoader:
        return self._loader if self._loader is not None else get_ergast_loader()

    @property
    def limiter(self) -> RateLimiter:
        return self._limiter if self._limiter is not None else get_rate_limiter()

    @property
    def base(self) -> str:
        return self._base.rstrip("/") if self._base else base_url()

    @property
    def timeout(self) -> float:
        if self._timeout is not None:
            return self._timeout
        value = get_config().get("ergast_timeout", 5.0)
        return float(value) if isinstance(value, int | float) and value > 0 else 5.0

    def url(self, endpoint: str | None, **selectors: Any) -> str:
        """Request URL for ``endpoint`` and fastf1-style selectors (no query string)."""
        return build_url(endpoint, base=self.base, **selectors)

    @staticmethod
    def _full_url(url: str, params: dict[str, Any] | None) -> str:
        query = {k: v for k, v in (params or {}).items() if v is not None}
        if not query:
            return url
        return f"{url}?{urlencode(sorted(query.items()))}"

    def fetch(self, url: str, params: dict[str, Any] | None = None) -> dict:
        """One rate-limited request (no cache). Raises the Ergast error family."""
        if get_config().get("offline_mode", False):
            raise NetworkError(url=url, status_code=None)
        full_url = self._full_url(url, params)
        retry_after: float | None = None
        for attempt in range(_MAX_429_RETRIES + 1):
            self.limiter.acquire()
            try:
                payload = self.transport.get_json(full_url, timeout=self.timeout)
            except DataNotFoundError as exc:
                raise ErgastInvalidRequestError(
                    f"Invalid request to Ergast ({full_url})\nServer response: 'Not Found'",
                    url=full_url,
                    status_code=404,
                ) from exc
            except NetworkError as exc:
                status = exc.context.get("status_code")
                if status == 429:
                    retry_after = exc.context.get("retry_after")
                    if attempt < _MAX_429_RETRIES:
                        delay = float(retry_after) if retry_after else float(2 ** (attempt + 1))
                        logger.warning("Jolpica throttled the request; retrying in %.0fs", delay)
                        if not IS_EMSCRIPTEN or can_block_on_awaitables():
                            self._sleep(delay)
                        continue
                    raise RateLimitExceededError(url=full_url, retry_after=retry_after) from exc
                if status is not None:
                    raise ErgastInvalidRequestError(
                        f"Invalid request to Ergast ({full_url})\nServer response: '{status}'",
                        url=full_url,
                        status_code=status,
                    ) from exc
                raise
            except ValueError as exc:
                raise ErgastJsonError(
                    f"Failed to parse Ergast response ({full_url})", url=full_url
                ) from exc
            if not isinstance(payload, dict) or "MRData" not in payload:
                raise ErgastJsonError(f"Failed to parse Ergast response ({full_url})", url=full_url)
            return payload
        raise RateLimitExceededError(url=full_url, retry_after=retry_after)

    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        *,
        ttl: float | None = None,
    ) -> dict:
        """A cached request (``ergast_ttl_hours``); see :meth:`fetch`."""
        full_url = self._full_url(url, params)
        return self.loader.jolpica_payload(full_url, lambda: self.fetch(url, params), ttl=ttl)

    def get_rows(
        self,
        url: str,
        *,
        limit: int | None = None,
        offset: int | None = None,
        complete: bool = False,
    ) -> dict:
        """Rows of ``url`` honouring Jolpica's page cap.

        ``limit`` above :data:`MAX_PAGE_SIZE` (or ``complete=True``) fetches
        100-row pages until the requested rows (or every row) arrived and
        merges them into one payload. Otherwise one request with the
        caller's ``limit``/``offset`` (``None`` means the API default of 30).
        """
        start = int(offset or 0)
        if not complete and (limit is None or limit <= MAX_PAGE_SIZE):
            return self.get(url, {"limit": limit, "offset": offset})

        pages: list[dict] = []
        page_offset = start
        wanted = None if complete else int(limit or 0)
        collected = 0
        total: int | None = None
        while True:
            page = self.get(url, {"limit": MAX_PAGE_SIZE, "offset": page_offset})
            pages.append(page)
            mrdata = page["MRData"]
            if total is None:
                try:
                    total = int(mrdata.get("total", 0))
                except (TypeError, ValueError):
                    total = 0
            table = _table_name(mrdata)
            if table:
                body = mrdata.get(table, {})
                key = _list_key(body) or ""
                page_rows = _count_rows(table, key, list(body.get(key, [])))
            else:
                page_rows = 0
            collected += page_rows
            page_offset += MAX_PAGE_SIZE
            if page_rows == 0 or page_offset >= total:
                break
            if wanted is not None and collected >= wanted:
                break
        merged = merge_pages(pages)
        if wanted is not None and collected > wanted:
            merged = _trim_rows(merged, wanted)
        merged["MRData"]["offset"] = str(start)
        if wanted is not None:
            merged["MRData"]["limit"] = str(wanted)
        return merged

    def resolve_current_season(self) -> int:
        """The season Jolpica calls ``current`` (cached for an hour)."""
        payload = self.get(self.url("current", season=None), {"limit": 1}, ttl=_CURRENT_TTL_SECONDS)
        table = payload["MRData"].get("RaceTable", {})
        try:
            return int(table.get("season"))
        except (TypeError, ValueError) as exc:
            raise ErgastJsonError("Jolpica did not report a current season") from exc

    def resolve_last_round(self, season: int | str) -> int:
        """The round Jolpica calls ``last`` for ``season`` (cached for an hour)."""
        payload = self.get(
            self.url(None, season=season, round="last"), {"limit": 1}, ttl=_CURRENT_TTL_SECONDS
        )
        table = payload["MRData"].get("RaceTable", {})
        value = table.get("round")
        if value is None:
            races = table.get("Races", [])
            value = races[0].get("round") if races else None
        try:
            return int(str(value))
        except (TypeError, ValueError) as exc:
            raise ErgastJsonError(f"Jolpica did not report a last round for {season}") from exc


def _trim_rows(payload: dict, wanted: int) -> dict:
    """Keep the first ``wanted`` leaf rows of a merged payload."""
    from tif1.ergast.router import apply_paging

    mrdata = payload["MRData"]
    table_name = _table_name(mrdata)
    if table_name is None:
        return payload
    return apply_paging(payload, table_name, limit=wanted, offset=0)


__all__ = [
    "DEFAULT_BASE_URL",
    "DEFAULT_PAGE_SIZE",
    "JolpicaClient",
    "MAX_PAGE_SIZE",
    "RateLimiter",
    "base_url",
    "build_url",
    "get_rate_limiter",
    "merge_pages",
    "merge_split_laps",
    "reset_rate_limiter",
]
