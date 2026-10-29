"""MultiViewer API requests (``fastf1.mvapi.api`` parity).

The API (``https://api.multiviewer.app``) needs no authentication. Requests go
through :func:`tif1.payload_loader.get_url_loader` so the shared transport
(niquests natively, the browser's fetch in Pyodide) and its
``User-Agent: tif1/{version}`` header apply. Responses are cached in the
SQLite ``json`` tier: a circuit layout permanently (layouts are static per
year), the circuit index for ``schedule_ttl_hours``. Nothing here raises for
a failed request: the functions return ``None`` and log at ``info``.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from tif1.cache import get_cache
from tif1.config import get_config
from tif1.exceptions import CacheError, DataNotFoundError, InvalidDataError, NetworkError
from tif1.payload_loader import get_url_loader

logger = logging.getLogger(__name__)

PROTO = "https"
HOST = "api.multiviewer.app"
BASE_URL = f"{PROTO}://{HOST}"
#: ``/api/v1/circuits``: every circuit MultiViewer knows (``name``, ``country``,
#: ``years``, ``circuitKey``, ``countryKey``, ``iocCountryCode``, ``pitLoss``).
CIRCUITS_INDEX_URL = f"{BASE_URL}/api/v1/circuits"
#: SQLite ``json`` tier keys.
CIRCUIT_CACHE_KEY_TEMPLATE = "mvapi/circuits/{circuit_key}/{year}.json"
CIRCUITS_INDEX_CACHE_KEY = "mvapi/circuits/index.json"
#: Seconds; the API answers in well under a second.
FETCH_TIMEOUT = 15
_DEFAULT_INDEX_TTL_HOURS = 6.0
_FETCH_ERRORS = (DataNotFoundError, NetworkError, InvalidDataError, TypeError, ValueError)


def circuit_url(*, year: int, circuit_key: int) -> str:
    """The per-year layout URL: ``/api/v1/circuits/{circuit_key}/{year}``."""
    return f"{BASE_URL}/api/v1/circuits/{int(circuit_key)}/{int(year)}"


def circuit_cache_key(*, year: int, circuit_key: int) -> str:
    """The SQLite ``json`` key of a layout payload."""
    return CIRCUIT_CACHE_KEY_TEMPLATE.format(circuit_key=int(circuit_key), year=int(year))


def _offline() -> bool:
    return bool(get_config().get("offline_mode", False))


def _read_cached(key: str) -> tuple[float, dict[str, Any]] | None:
    """``(fetched_at, payload)`` from the SQLite tier, or ``None``."""
    try:
        entry = get_cache().get_entry("json", key)
    except (CacheError, OSError) as exc:
        logger.debug("MultiViewer cache read failed for %s: %s", key, exc)
        return None
    if not isinstance(entry, dict):
        return None
    fetched_at = entry.get("fetched_at")
    payload = entry.get("payload")
    if not isinstance(fetched_at, int | float) or not isinstance(payload, dict):
        return None
    return float(fetched_at), payload


def _write_cached(key: str, payload: dict[str, Any]) -> None:
    try:
        get_cache().set_entry("json", key, {"fetched_at": time.time(), "payload": payload})
    except (CacheError, OSError) as exc:
        logger.debug("MultiViewer cache write failed for %s: %s", key, exc)


def _fetch(url: str) -> dict[str, Any] | None:
    """GET ``url`` as a JSON object; ``None`` (logged at info) on any failure."""
    try:
        data = get_url_loader().get_url(url, timeout=FETCH_TIMEOUT)
    except _FETCH_ERRORS as exc:
        logger.info("MultiViewer request failed for %s: %s", url, exc)
        return None
    if not isinstance(data, dict):
        logger.info("MultiViewer response for %s is not a JSON object", url)
        return None
    return data


def get_circuit(*, year: int, circuit_key: int) -> dict[str, Any] | None:
    """The MultiViewer layout of a circuit for a season (raw JSON) or ``None``.

    MultiViewer answers with its most recent layout at or before ``year``.
    The payload is cached permanently under
    ``mvapi/circuits/{circuit_key}/{year}.json``; in ``offline_mode`` only
    the cache is consulted.

    Args:
        year: The championship year.
        circuit_key: The circuit key (``circuitKey`` of ``/api/v1/circuits``,
            the F1 live-timing key; see
            :func:`tif1.core_utils.circuit_info.multiviewer_request_key` for Mugello).

    Returns:
        The response object, or ``None`` when the request failed, the circuit
        is unknown, or the cache has nothing while offline.
    """
    key = circuit_cache_key(year=year, circuit_key=circuit_key)
    cached = _read_cached(key)
    if cached is not None:
        return cached[1]
    url = circuit_url(year=year, circuit_key=circuit_key)
    if _offline():
        logger.info("offline_mode: MultiViewer circuit %s/%s is not cached", circuit_key, year)
        return None
    data = _fetch(url)
    if data is None:
        return None
    _write_cached(key, data)
    return data


def get_circuits_index() -> dict[str, Any] | None:
    """``/api/v1/circuits`` (circuit key -> entry) cached for ``schedule_ttl_hours``.

    A stale cached index is served when the refresh fails or ``offline_mode``
    is set.

    Returns:
        The index, or ``None`` when neither the network nor the cache has it.
    """
    ttl_hours = get_config().get("schedule_ttl_hours", _DEFAULT_INDEX_TTL_HOURS)
    ttl = float(ttl_hours) * 3600.0
    cached = _read_cached(CIRCUITS_INDEX_CACHE_KEY)
    if cached is not None and time.time() - cached[0] < ttl:
        return cached[1]
    stale = cached[1] if cached is not None else None
    if _offline():
        if stale is None:
            logger.info("offline_mode: the MultiViewer circuit index is not cached")
        return stale
    data = _fetch(CIRCUITS_INDEX_URL)
    if data is None:
        return stale
    _write_cached(CIRCUITS_INDEX_CACHE_KEY, data)
    return data


__all__ = [
    "BASE_URL",
    "CIRCUITS_INDEX_CACHE_KEY",
    "CIRCUITS_INDEX_URL",
    "CIRCUIT_CACHE_KEY_TEMPLATE",
    "FETCH_TIMEOUT",
    "circuit_cache_key",
    "circuit_url",
    "get_circuit",
    "get_circuits_index",
]
