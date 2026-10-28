"""Checks that run inside Pyodide, driven by tools/pyodide_smoke.mjs (Node) and
tools/pyodide_browser_check.mjs (Chrome, Firefox).

The harness loads this file with ``pyodide.runPython`` and then calls the
functions below, synchronously (``runPython``: a stack without JavaScript
Promise Integration) or through ``runPythonAsync`` (JSPI when the runtime has
it). Each function returns a JSON-serialisable dict that the harness merges
into its report.
"""

from __future__ import annotations

import json
import sys

import tif1
from tif1.browser import PyodideTransport, enable_persistent_cache, persist_cache
from tif1.cache import get_cache
from tif1.core_utils.runtime import (
    IS_EMSCRIPTEN,
    THREADS_AVAILABLE,
    can_block_on_awaitables,
    is_pyodide,
)
from tif1.payload_loader import InMemoryTransport, default_transport, get_url_loader

DRIVERS = {
    "drivers": [
        {"driver": "VER", "team": "Red Bull Racing", "dn": "1"},
        {"driver": "LEC", "team": "Ferrari", "dn": "16"},
    ]
}
SESSION_LAPTIMES = {
    "drv": ["VER", "VER", "VER", "LEC", "LEC", "LEC"],
    "lap": [1, 2, 3, 1, 2, 3],
    "time": [80.5, 80.4, 80.1, 81.0, 79.0, 80.1],
    "del": [False, False, False, False, True, False],
    "team": ["Red Bull Racing"] * 3 + ["Ferrari"] * 3,
}
WEATHER = {"wT": [25.0, 25.5], "tT": [40.0, 41.0]}


def _memory_transport() -> InMemoryTransport:
    return InMemoryTransport(
        {
            "drivers.json": DRIVERS,
            "session_laptimes.json": SESSION_LAPTIMES,
            "weather.json": WEATHER,
        }
    )


def _offline_session(transport: InMemoryTransport) -> tif1.Session:
    session = tif1.get_session(2025, "Bahrain", "R", enable_cache=False)
    session._payload_loader.transport = transport
    get_url_loader().transport = transport
    return session


def runtime_checks() -> dict:
    """Detection, transport selection and the absence of native-only packages."""
    assert sys.platform == "emscripten", sys.platform
    assert IS_EMSCRIPTEN
    assert is_pyodide()
    assert not THREADS_AVAILABLE
    assert isinstance(default_transport(), PyodideTransport)
    for forbidden in ("niquests", "nest_asyncio2", "rapidfuzz"):
        assert forbidden not in sys.modules, f"{forbidden} imported inside Pyodide"
    return {
        "python": sys.version.split()[0],
        "tif1": tif1.__version__,
        "jspi": can_block_on_awaitables(),
    }


async def offline_load_async() -> dict:
    """``load_async`` on a synthetic session: the async pipeline without a network."""
    transport = _memory_transport()
    session = _offline_session(transport)
    await session.load_async(telemetry=False, messages=False)
    laps = session.laps
    assert len(laps) == 6, len(laps)
    assert sorted(set(laps["Driver"])) == ["LEC", "VER"], sorted(set(laps["Driver"]))
    assert len(session.weather) == 2, len(session.weather)
    return {"offline_laps": len(laps), "offline_requests": len(transport.calls)}


def offline_sync_weather() -> dict:
    """The synchronous single-payload seam (``PayloadLoader.get``) needs no event loop.

    ``weather`` is one table fetch; ``drivers`` and ``results`` would rebuild
    the classification from the laps table and so need the async pipeline.
    """
    session = _offline_session(_memory_transport())
    weather = session.weather
    assert len(weather) == 2, len(weather)
    return {"offline_sync_weather_rows": len(weather)}


def sync_api_check() -> dict:
    """``load()`` blocks through JSPI, or raises AsyncRequiredError without it."""
    session = _offline_session(_memory_transport())
    if can_block_on_awaitables():
        session.load(telemetry=False, weather=False, messages=False)
        assert len(session.laps) == 6
        return {"sync_api": "load() ran through JSPI"}
    message = None
    try:
        _ = session.laps
    except tif1.AsyncRequiredError as exc:
        message = str(exc)
    if message is None:
        raise AssertionError("the synchronous API must raise without JSPI")
    assert "await session.laps_async()" in message, message
    return {"sync_api": f"AsyncRequiredError: {message}"}


ERGAST_RESULTS = {
    "MRData": {
        "xmlns": "",
        "series": "f1",
        "url": "",
        "limit": "30",
        "offset": "0",
        "total": "2",
        "RaceTable": {
            "season": "2025",
            "round": "1",
            "Races": [
                {
                    "season": "2025",
                    "round": "1",
                    "raceName": "Bahrain Grand Prix",
                    "Circuit": {"circuitId": "bahrain", "circuitName": "Bahrain", "Location": {}},
                    "date": "2025-03-16",
                    "Results": [
                        {
                            "number": "1",
                            "position": "1",
                            "positionText": "1",
                            "points": "25",
                            "Driver": {"driverId": "max_verstappen", "code": "VER"},
                            "Constructor": {"constructorId": "red_bull", "name": "Red Bull"},
                            "grid": "1",
                            "laps": "57",
                            "status": "Finished",
                            "Time": {"millis": "5400000", "time": "1:30:00.000"},
                        },
                        {
                            "number": "16",
                            "position": "2",
                            "positionText": "2",
                            "points": "18",
                            "Driver": {"driverId": "leclerc", "code": "LEC"},
                            "Constructor": {"constructorId": "ferrari", "name": "Ferrari"},
                            "grid": "2",
                            "laps": "57",
                            "status": "Finished",
                            "Time": {"millis": "5410000", "time": "+10.000"},
                        },
                    ],
                }
            ],
        },
    }
}
ERGAST_EVENTS = {
    "MRData": {
        "total": "1",
        "limit": "30",
        "offset": "0",
        "RaceTable": {
            "season": "2025",
            "Races": [
                {
                    "season": "2025",
                    "round": "1",
                    "raceName": "Bahrain Grand Prix",
                    "date": "2025-03-16",
                    "Circuit": {"circuitId": "bahrain", "circuitName": "Bahrain", "Location": {}},
                }
            ],
        },
    }
}


def ergast_check() -> dict:
    """``tif1.ergast`` answers from an in-memory Stats mirror without threads or sockets."""
    from tif1.config import get_config
    from tif1.ergast import Ergast
    from tif1.ergast import jolpica as jolpica_module
    from tif1.ergast import payloads as ergast_payloads
    from tif1.ergast import stats as ergast_stats

    transport = InMemoryTransport(
        {"2025/events.json": ERGAST_EVENTS, "2025/bahrain-grand-prix/results.json": ERGAST_RESULTS}
    )
    ergast_payloads.reset_ergast_loader()
    ergast_stats.reset_stats_source()
    jolpica_module.reset_rate_limiter()
    ergast_payloads.get_ergast_loader().transport = transport
    config = get_config()
    previous_fallback = config.get("ergast_fallback", True)
    config.set("ergast_fallback", False)
    try:
        response = Ergast().get_race_results(2025, 1)
    finally:
        config.set("ergast_fallback", previous_fallback)
    content = response.content[0]
    assert response.source == "stats", response.source
    assert content["driverId"].tolist() == ["max_verstappen", "leclerc"], content
    gap = content["totalRaceTimeGap"].iloc[1]
    assert gap.total_seconds() == 10.0, gap
    return {"ergast_source": response.source, "ergast_rows": len(content)}


def journal_mode_check() -> dict:
    cache = get_cache()
    mode = cache.conn.execute("PRAGMA journal_mode").fetchone()[0] if cache.conn else None
    assert mode == "memory", mode
    return {"journal_mode": mode}


def _live_transport() -> None:
    get_url_loader().transport = default_transport()


async def network_load_async(session_name: str = "R") -> dict:
    """A real 2025 Bahrain session through the CDNs with the async pipeline."""
    _live_transport()
    live = tif1.get_session(2025, "Bahrain", session_name)
    await live.load_async(telemetry=False)
    laps = len(live.laps)
    assert laps > 200, laps
    return {
        f"network_laps_{session_name}": laps,
        f"network_drivers_{session_name}": len(live.drivers),
        f"network_weather_rows_{session_name}": len(live.weather),
    }


def network_sync_weather(session_name: str = "Q") -> dict:
    """One synchronous payload fetch (urllib3: fetch under JSPI, XMLHttpRequest otherwise)."""
    _live_transport()
    live = tif1.get_session(2025, "Bahrain", session_name)
    weather = live.weather
    assert len(weather) > 0, len(weather)
    return {f"network_sync_weather_rows_{session_name}": len(weather)}


def network_sync_load(session_name: str = "Q") -> dict:
    """The synchronous ``load()`` against the CDNs (needs JSPI)."""
    assert can_block_on_awaitables()
    _live_transport()
    live = tif1.get_session(2025, "Bahrain", session_name)
    live.load(telemetry=False, weather=False, messages=False)
    laps = len(live.laps)
    assert laps > 200, laps
    return {f"network_sync_laps_{session_name}": laps}


async def persistence_enable() -> dict:
    """Mount the cache directory on IndexedDB (browser only)."""
    target = await enable_persistent_cache()
    return {"persistent_cache_dir": str(target)}


async def persistence_persist() -> dict:
    await persist_cache()
    return {"persisted": True}


def persistence_rows() -> dict:
    """Row counts of the SQLite tiers (after a reload: proof that IndexedDB kept them)."""
    cache = get_cache()
    assert cache.conn is not None
    payloads = cache.conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
    laps_frames = cache.conn.execute("SELECT COUNT(*) FROM laps_frames").fetchone()[0]
    assert payloads > 0, payloads
    assert laps_frames > 0, laps_frames
    return {"cache_payload_rows": int(payloads), "laps_frame_rows": int(laps_frames)}


def merge(*parts: dict) -> str:
    """JSON for the harness."""
    report: dict = {}
    for part in parts:
        report.update(part)
    return json.dumps(report)
