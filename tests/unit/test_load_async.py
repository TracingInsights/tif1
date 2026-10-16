"""Session.load_async: the async twin of load(), and the sync API without JSPI."""

from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from tif1 import core
from tif1.core_utils import runtime
from tif1.exceptions import AsyncRequiredError


@pytest.fixture
def session():
    return core.Session(2025, "Monaco Grand Prix", "Race", False, "pandas")


def _install_pyodide_without_jspi(monkeypatch) -> None:
    ffi = types.ModuleType("pyodide.ffi")
    ffi.can_run_sync = lambda: False
    package = types.ModuleType("pyodide")
    package.ffi = ffi
    monkeypatch.setitem(sys.modules, "pyodide", package)
    monkeypatch.setitem(sys.modules, "pyodide.ffi", ffi)


async def test_load_async_runs_every_phase(session, monkeypatch):
    laps = pd.DataFrame({"LapNumber": [1]})

    async def fake_laps_async():
        session._laps = laps
        return laps

    prefetch = AsyncMock()
    fetch_all = AsyncMock(return_value={})
    monkeypatch.setattr(session, "laps_async", fake_laps_async)
    monkeypatch.setattr(session, "_prefetch_session_tables_async", prefetch)
    monkeypatch.setattr(session, "_telemetry_load_pending", lambda: True)
    monkeypatch.setattr(session, "fetch_all_laps_telemetry_async", fetch_all)
    monkeypatch.setattr(core.Session, "weather", property(lambda _self: "weather"))
    monkeypatch.setattr(core.Session, "race_control_messages", property(lambda _self: "rcm"))

    assert await session.load_async() is session
    assert {"weather.json", "rcm.json"} <= session._requested_session_tables
    prefetch.assert_awaited_once()
    fetch_all.assert_awaited_once()


async def test_load_async_telemetry_forces_laps(session, monkeypatch):
    laps_async = AsyncMock(return_value=pd.DataFrame())
    monkeypatch.setattr(session, "laps_async", laps_async)
    monkeypatch.setattr(session, "_telemetry_load_pending", lambda: False)
    await session.load_async(laps=False, telemetry=True, weather=False, messages=False)
    laps_async.assert_awaited_once()


async def test_load_async_laps_only(session, monkeypatch):
    laps_async = AsyncMock(return_value=pd.DataFrame())
    prefetch = AsyncMock()
    fetch_all = AsyncMock()
    monkeypatch.setattr(session, "laps_async", laps_async)
    monkeypatch.setattr(session, "_prefetch_session_tables_async", prefetch)
    monkeypatch.setattr(session, "fetch_all_laps_telemetry_async", fetch_all)
    await session.load_async(laps=True, telemetry=False, weather=False, messages=False)
    laps_async.assert_awaited_once()
    prefetch.assert_not_awaited()
    fetch_all.assert_not_awaited()
    assert "weather.json" not in session._requested_session_tables


async def test_prefetch_session_tables_async_fills_the_memo(session, monkeypatch):
    session._requested_session_tables.update({"weather.json", "rcm.json"})
    seen: list[list[str]] = []

    async def fake_fetch_multiple(requests, **kwargs):
        seen.append([request[3] for request in requests])
        return [{"wT": [1.0]}, {"msg": ["x"]}, {"drivers": [{"driver": "VER"}]}]

    monkeypatch.setattr(core, "fetch_multiple_async", fake_fetch_multiple)
    await session._prefetch_session_tables_async()
    assert seen == [["weather.json", "rcm.json", "drivers.json"]]
    assert session._get_local_payload("weather.json") == {"wT": [1.0]}
    assert session._get_local_payload("rcm.json") == {"msg": ["x"]}
    assert session._get_local_payload("drivers.json") == {"drivers": [{"driver": "VER"}]}
    assert session._session_tables_prefetched is True


async def test_prefetch_session_tables_async_skips_memoised_tables(session, monkeypatch):
    session._requested_session_tables.add("weather.json")
    session._remember_local_payload("weather.json", {"wT": [1.0]})
    session._remember_local_payload("drivers.json", {"drivers": []})
    monkeypatch.setattr(
        core, "fetch_multiple_async", AsyncMock(side_effect=AssertionError("fetched"))
    )
    await session._prefetch_session_tables_async()


async def test_prefetch_session_tables_async_tolerates_fetch_failures(session, monkeypatch):
    from tif1.exceptions import NetworkError

    session._requested_session_tables.add("weather.json")
    monkeypatch.setattr(core, "fetch_multiple_async", AsyncMock(side_effect=NetworkError(url="x")))
    await session._prefetch_session_tables_async()
    assert session._get_local_payload("weather.json") is None


def test_sync_laps_without_jspi_points_to_laps_async(session, monkeypatch):
    monkeypatch.setattr(core, "IS_EMSCRIPTEN", True)
    monkeypatch.setattr(runtime, "IS_EMSCRIPTEN", True)
    _install_pyodide_without_jspi(monkeypatch)
    monkeypatch.setattr(session, "_laps_from_frame_tier", lambda *_args, **_kwargs: None)
    with pytest.raises(AsyncRequiredError, match=r"await session\.laps_async\(\)"):
        _ = session.laps


def test_telemetry_load_is_pending_without_a_cache(session):
    assert session._telemetry_load_pending() is True
