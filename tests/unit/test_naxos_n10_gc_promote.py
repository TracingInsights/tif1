"""Naxos N10: a GC-suspended burst hands its survivors to the oldest generation.

``suspend_gc(promote=True)`` ends with ``gc.freeze(); gc.unfreeze()``, which
splices the young generations into the oldest one, so the first collection
after the burst no longer traverses the containers it built (a race's lap
frames: 9 ms of a warm pandas ``fetch_all_laps_telemetry()``). The move is
skipped below the young-collection threshold, when the process froze objects
itself, when the caller had GC off, on an exception, and where the move is not
O(1) (free-threaded builds, the incremental collector of CPython 3.14.0-3.14.4
and the 3.15 alphas, 3.16+ until measured). The imports are covered by
``test_naxos_n10_import_gc.py``.
"""

from __future__ import annotations

import gc
import sys
import weakref

import pandas as pd
import pytest

import tif1.cache as cache_mod
from tif1.cache import Cache, _suspend_gc
from tif1.core_utils import gc_control
from tif1.core_utils.gc_control import gc_promote_ok, promote_young_objects

# CPython 3.12 starts with interpreter objects in the permanent generation
# (375 on 3.12.14, before any import); tif1 never promotes in a process with
# frozen objects, so the promotion tests only run where the count starts at 0.
_FROZEN_AT_START = gc.get_freeze_count()

needs_promotion = pytest.mark.skipif(
    not gc_promote_ok() or _FROZEN_AT_START != 0,
    reason="no O(1) gc.freeze promotion here, or the interpreter starts with frozen objects",
)


class _Box:
    """A GC-tracked container."""

    __slots__ = ("__weakref__", "value")

    def __init__(self, value: object) -> None:
        self.value = value


def _burst(n: int | None = None) -> list[_Box]:
    """More tracked containers than the young-collection threshold."""
    count = n if n is not None else gc.get_threshold()[0] * 2 + 100
    return [_Box([i]) for i in range(count)]


def _in_oldest_generation(obj: object) -> bool:
    oldest = gc.get_objects(generation=2)
    return any(item is obj for item in oldest)


@pytest.fixture(autouse=True)
def _clean_gc_state():
    assert gc.isenabled()
    assert gc.get_freeze_count() == _FROZEN_AT_START
    gc.collect()
    yield
    gc.enable()
    if _FROZEN_AT_START == 0:  # never thaw the interpreter's own frozen objects
        gc.unfreeze()


@needs_promotion
def test_promote_moves_the_burst_into_the_oldest_generation() -> None:
    with _suspend_gc(promote=True):
        kept = _burst()
    assert gc.isenabled()
    assert gc.get_freeze_count() == 0
    assert gc.get_count()[0] < gc.get_threshold()[0]
    assert _in_oldest_generation(kept[0])
    assert _in_oldest_generation(kept[-1])


def test_without_promote_the_burst_is_left_to_the_young_collections() -> None:
    with _suspend_gc():
        kept = _burst()
    # The first collection after the burst traverses it and moves it up one
    # generation only; it never lands in the oldest generation at once.
    assert not _in_oldest_generation(kept[-1])


@needs_promotion
def test_promoted_objects_are_still_collected_by_a_full_collection() -> None:
    with _suspend_gc(promote=True):
        kept = _burst()
        cycle = _Box(None)
        cycle.value = cycle  # a reference cycle built inside the burst
    probe = weakref.ref(cycle)
    del cycle
    assert _in_oldest_generation(kept[0])
    gc.collect()
    assert probe() is None


def test_small_burst_is_not_promoted() -> None:
    gc.collect()
    with _suspend_gc(promote=True):
        kept = _burst(10)
    assert not _in_oldest_generation(kept[-1])


@needs_promotion
def test_a_process_that_froze_objects_keeps_them_frozen() -> None:
    frozen = _Box("frozen by the application")
    gc.freeze()
    count = gc.get_freeze_count()
    try:
        with _suspend_gc(promote=True):
            kept = _burst()
        assert gc.get_freeze_count() == count
        assert not _in_oldest_generation(frozen)
        assert not _in_oldest_generation(kept[-1])
    finally:
        gc.unfreeze()


def test_caller_with_gc_off_is_left_alone(monkeypatch) -> None:
    calls: list[int] = []
    monkeypatch.setattr(gc_control, "promote_young_objects", lambda: calls.append(1))
    gc.disable()
    try:
        with _suspend_gc(promote=True):
            _burst()
        assert not gc.isenabled()
    finally:
        gc.enable()
    assert calls == []


def test_no_promotion_on_an_exception(monkeypatch) -> None:
    calls: list[int] = []
    monkeypatch.setattr(gc_control, "promote_young_objects", lambda: calls.append(1))
    with pytest.raises(RuntimeError):
        with _suspend_gc(promote=True):
            raise RuntimeError("boom")
    assert gc.isenabled()
    assert calls == []


@pytest.mark.parametrize(
    ("gil_disabled", "version", "expected"),
    [
        (0, (3, 13, 0, "final", 0), True),
        (1, (3, 13, 0, "final", 0), False),
        (0, (3, 11, 0, "final", 0), True),
        # Corinth C5: 3.14.0-3.14.4 ran the incremental collector, 3.14.5 reverted it.
        (0, (3, 14, 0, "final", 0), False),
        (0, (3, 14, 4, "final", 0), False),
        (0, (3, 14, 5, "final", 0), True),
        (0, (3, 14, 8, "final", 0), True),
        (1, (3, 14, 8, "final", 0), False),
        (0, (3, 15, 0, "alpha", 8), False),
        (0, (3, 15, 0, "beta", 1), True),
        (0, (3, 15, 2, "final", 0), True),
        (0, (3, 16, 0, "final", 0), False),
    ],
)
def test_promotion_gate(monkeypatch, gil_disabled, version, expected) -> None:
    import sysconfig

    if sys.implementation.name != "cpython":
        pytest.skip("CPython only")
    real = sysconfig.get_config_var
    monkeypatch.setattr(
        sysconfig,
        "get_config_var",
        lambda name: gil_disabled if name == "Py_GIL_DISABLED" else real(name),
    )
    monkeypatch.setattr(gc_control.sys, "version_info", version)
    monkeypatch.setattr(gc_control, "_GC_PROMOTE_OK", None)
    assert gc_promote_ok() is expected
    monkeypatch.setattr(gc_control, "_GC_PROMOTE_OK", False)
    gc.disable()
    kept = _burst()
    try:
        promote_young_objects()  # gated off: nothing moves
        assert not _in_oldest_generation(kept[-1])
    finally:
        gc.enable()


def _driver_frame(driver: str, lap: int, n: int) -> pd.DataFrame:
    from tif1.core_utils.helpers import _create_telemetry_df

    payload = {
        "Time": [0.1 * i for i in range(n)],
        "Speed": [200.0 + i for i in range(n)],
        "nGear": [7] * n,
        "DriverAhead": ["44"] * n,
    }
    frame = _create_telemetry_df(payload, driver, lap, "pandas")
    assert frame is not None
    return frame


def test_frame_tier_batch_read_promotes_its_frames(tmp_path, monkeypatch) -> None:
    calls: list[int] = []
    real = gc_control.promote_young_objects
    monkeypatch.setattr(gc_control, "promote_young_objects", lambda: (calls.append(1), real()))
    key = (2026, "Bahrain%20Grand%20Prix", "Race")
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = {
            (d, lap): _driver_frame(d, lap, 5 + lap) for d in ("VER", "HAM") for lap in (1, 2)
        }
        cache.set_telemetry_frames_batch(*key, [(d, lap, f) for (d, lap), f in frames.items()])
        hits = cache.get_telemetry_frames_batch(*key, list(frames))
        assert set(hits) == set(frames)
        for ref, frame in frames.items():
            assert hits[ref].equals(frame)
        assert calls, "the batch read runs its decode under suspend_gc(promote=True)"
    finally:
        cache.close()
    assert cache_mod._suspend_gc is gc_control.suspend_gc


class _PendingSource:
    """A pending lap source that records whether GC was on while it built a frame."""

    def __init__(self, frames: dict[int, pd.DataFrame]) -> None:
        self.frames = frames
        self.laps = list(frames)
        self.gc_on: list[bool] = []

    def frame(self, lap: int) -> pd.DataFrame:
        self.gc_on.append(gc.isenabled())
        return self.frames[lap]


def test_per_lap_materialization_runs_under_promotion(monkeypatch) -> None:
    """Corinth C5: the lazy per-lap frame build (an iterlaps loop) pauses GC and promotes."""
    from tif1.cache import SessionMemo

    calls: list[int] = []
    real = gc_control.promote_young_objects
    monkeypatch.setattr(gc_control, "promote_young_objects", lambda: (calls.append(1), real()))
    frames = {lap: _driver_frame("VER", lap, 4) for lap in (1, 2)}
    source = _PendingSource(frames)
    memo = SessionMemo()
    adopted: list[bool] = []
    memo.frame_adopter = lambda _frame, _driver: adopted.append(gc.isenabled())
    assert memo.add_telemetry_source("VER", source) == [("VER", 1), ("VER", 2)]
    assert memo.get("telemetry_df", ("VER", 1)) is frames[1]
    assert source.gc_on == [False]
    assert adopted == [False]  # the adopter runs inside the paused span too
    assert calls == [1]
    assert gc.isenabled()
    assert memo.get("telemetry_df", ("VER", 1)) is frames[1]  # memoized, built once
    assert source.gc_on == [False]


def test_per_lap_materialization_missing_lap_drops_the_source() -> None:
    """A source that cannot serve its lap is dropped as before (Delphi D5), GC back on."""
    from tif1.cache import SessionMemo

    class _Broken(_PendingSource):
        def frame(self, lap: int) -> pd.DataFrame:
            raise KeyError(lap)

    memo = SessionMemo()
    memo.add_telemetry_source("VER", _Broken({1: _driver_frame("VER", 1, 3)}))
    assert memo.get("telemetry_df", ("VER", 1)) is None
    assert not memo._telemetry_sources
    assert gc.isenabled()
