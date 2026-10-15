"""Contract tests for Pergamon Q9: the global lap cache must not pin sessions.

The process-global backend lap cache used to hold the session-bound ``Laps``
object, so every session loaded in a process (and its whole telemetry memo,
~100-150 MB per race) stayed reachable for up to ``MAX_CACHE_SIZE`` sessions
after the caller dropped it. The cache now holds a session-free shallow copy
and a cache hit binds a fresh shallow copy to the requesting session.
"""

from __future__ import annotations

import gc
import weakref

import pandas as pd
import pytest

from tif1 import core
from tif1.cache import clear_lap_cache, get_backend_lap_cache
from tif1.models import Laps


@pytest.fixture
def tier_laps(monkeypatch):
    frame = pd.DataFrame({"Driver": ["VER", "HAM"], "LapNumber": [1, 1], "LapTime": [90.1, 90.4]})
    calls: list[int] = []

    def fake_tier(self, ultra_cold_enabled=None):
        calls.append(1)
        return self._as_session_laps(frame.copy())

    monkeypatch.setattr(core.Session, "_laps_from_frame_tier", fake_tier)
    monkeypatch.setattr(
        core.Session, "_maybe_start_background_telemetry_prefetch", lambda _self: None
    )
    clear_lap_cache()
    yield frame, calls
    clear_lap_cache()


def _session() -> core.Session:
    return core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")


def test_global_lap_cache_holds_a_session_free_copy(tier_laps):
    frame, _ = tier_laps
    session = _session()
    laps = session.laps
    assert isinstance(laps, Laps)
    assert laps.session is session
    cached = get_backend_lap_cache("pandas").get(
        f"{session.year}_{session.gp}_{session.session}_laps"
    )
    assert isinstance(cached, Laps)
    assert cached is not laps
    assert cached.session is None
    assert cached.equals(laps)
    assert laps.equals(frame)


def test_dropped_session_is_not_pinned_by_the_lap_cache(tier_laps):
    session = _session()
    session._memo.set("telemetry_df", ("VER", 1), pd.DataFrame({"Speed": [1.0]}))
    _ = session.laps
    ref = weakref.ref(session)
    del session, _
    gc.collect()
    assert ref() is None


def test_lap_cache_hit_binds_laps_to_the_new_session(tier_laps):
    _, calls = tier_laps
    first = _session()
    first_laps = first.laps
    second = _session()
    second_laps = second.laps
    assert len(calls) == 1  # second load served by the global lap cache
    assert second_laps.session is second
    assert first_laps.session is first
    assert second_laps.equals(first_laps)
    # Copy-on-write shallow copies: mutating one session's laps leaves the
    # other session and the cached entry untouched.
    second_laps.loc[0, "LapTime"] = 1.0
    assert first_laps.loc[0, "LapTime"] == 90.1
    third = _session()
    assert third.laps.loc[0, "LapTime"] == 90.1
