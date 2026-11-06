"""A warm polars ``load()`` keeps the frame-tier driver rows lazy (Naxos N2).

When the IPC frame tier holds every lap (its rows' ``[lap, rows]`` spans say
so without reading the blobs), ``load(telemetry=True)`` registers the laps as
``load(telemetry="lazy")`` does instead of reading every driver's row: the
first driver touched reads its row alone, the second reads every remaining
row in one batch, and the frames are the ones the eager read serves. A lap of
an ``iterlaps()`` walk brings every driver of the walk in one read. Anything
the tier does not hold keeps the eager batch. Nothing here reaches the
network.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pandas as pd
import polars as pl
import pytest

from tif1 import core
from tif1.cache import _POLARS_IPC_TABLE, Cache
from tif1.models_polars import PolarsLaps, PolarsTelemetry

GP = "Monaco%20Grand%20Prix"
DRIVERS = ("VER", "LEC", "HAM")
LAPS = (1, 2, 3)


def _frame(driver: str, lap: int) -> pl.DataFrame:
    n = 4 + lap
    return pl.from_pandas(
        pd.DataFrame(
            {
                "Date": pd.date_range("2026-06-07 15:00", periods=n, freq="250ms"),
                "SessionTime": pd.to_timedelta([60.0 + 0.25 * i for i in range(n)], unit="s"),
                "Time": pd.to_timedelta([0.25 * i for i in range(n)], unit="s"),
                "Speed": [100.0 + 10 * i + lap + DRIVERS.index(driver) for i in range(n)],
                "LapNumber": pd.array([lap] * n, dtype="Int64"),
            }
        )
    )


@pytest.fixture
def cache(tmp_path, monkeypatch) -> Iterator[Cache]:
    cache = Cache(cache_dir=tmp_path)
    rows = [(driver, lap, _frame(driver, lap)) for driver in DRIVERS for lap in LAPS]
    assert cache.set_telemetry_frames_batch(2026, GP, "Race", rows, lib="polars") == len(rows)
    monkeypatch.setattr(core, "get_cache", lambda: cache)
    try:
        yield cache
    finally:
        cache.close()


@pytest.fixture
def reads(cache, monkeypatch) -> list[list[str]]:
    """The drivers of every frame-tier read, one list per read."""
    seen: list[list[str]] = []
    read = cache.get_telemetry_frames_batch

    def spy(year, gp, session_name, refs, **kwargs):
        seen.append(sorted({driver for driver, _ in refs}))
        return read(year, gp, session_name, refs, **kwargs)

    monkeypatch.setattr(cache, "get_telemetry_frames_batch", spy)
    return seen


def _session(lib: str = "polars") -> core.Session:
    session = core.Session(2026, "Monaco Grand Prix", "Race", True, lib)
    session._memo.has_session_data = True
    session._laps = pl.DataFrame(
        {
            "Driver": [driver for driver in DRIVERS for _ in LAPS],
            "LapNumber": [lap for _ in DRIVERS for lap in LAPS],
        }
    )
    return session


def _load(session: core.Session, **kwargs: Any) -> core.Session:
    return session.load(weather=False, messages=False, **kwargs)


def _tel(session: core.Session, driver: str, lap: int) -> Any:
    return session._get_telemetry_df_for_ref(driver, lap, ultra_cold=False)


def test_load_reads_no_driver_row(reads):
    session = _load(_session())
    assert reads == []
    assert session._lazy_telemetry_refs == {
        driver: [(driver, lap) for lap in LAPS] for driver in DRIVERS
    }


def test_two_drivers_read_their_rows_alone_then_the_third_reads_the_rest(cache, reads):
    rows = [("NOR", lap, _frame("VER", lap)) for lap in LAPS]
    assert cache.set_telemetry_frames_batch(2026, GP, "Race", rows, lib="polars") == len(rows)
    session = _session()
    session._laps = pl.concat(
        [session._laps, pl.DataFrame({"Driver": ["NOR"] * 3, "LapNumber": list(LAPS)})]
    )
    _load(session)
    _tel(session, "LEC", 2)
    _tel(session, "LEC", 3)
    assert reads == [["LEC"]]
    _tel(session, "VER", 1)
    assert reads == [["LEC"], ["VER"]]
    assert set(session._lazy_telemetry_refs) == {"HAM", "NOR"}
    _tel(session, "NOR", 1)
    assert reads == [["LEC"], ["VER"], ["HAM", "NOR"]]
    assert session._lazy_telemetry_refs == {}
    for driver in (*DRIVERS, "NOR"):
        for lap in LAPS:
            _tel(session, driver, lap)
    assert len(reads) == 3
    assert _tel(session, "VER", 2).equals(_frame("VER", 2))


def test_frames_equal_the_eager_read(reads):
    lazy = _load(_session())
    eager = _session()
    eager_all = eager.fetch_all_laps_telemetry()
    assert len(eager_all) == len(DRIVERS) * len(LAPS)
    for (driver, lap), expected in eager_all.items():
        got = _tel(lazy, driver, lap)
        assert type(got) is PolarsTelemetry is type(expected)
        assert got.session is lazy
        assert got.driver == driver
        assert got.schema == expected.schema
        assert got.equals(expected)
        assert got.equals(_frame(driver, lap))
        assert [s.n_chunks() for s in got.get_columns()] == [
            s.n_chunks() for s in expected.get_columns()
        ]


def test_a_driver_batch_and_fetch_all_after_a_lazy_load(reads):
    session = _load(_session())
    # The batch behind ``PolarsLaps.telemetry`` of one driver.
    frames = session._telemetry_frames_for_refs([("HAM", lap) for lap in LAPS])
    assert reads == [["HAM"]]
    assert [frame.height for frame in frames.values()] == [
        _frame("HAM", lap).height for lap in LAPS
    ]
    all_frames = session.fetch_all_laps_telemetry()
    assert reads == [["HAM"], ["LEC", "VER"]]
    assert list(all_frames) == [(driver, lap) for driver in DRIVERS for lap in LAPS]
    assert session._lazy_telemetry_refs == {}


def test_a_lap_the_tier_lacks_keeps_the_eager_batch(cache, reads, monkeypatch):
    session = _session()
    session._laps = pl.concat([session._laps, pl.DataFrame({"Driver": ["VER"], "LapNumber": [4]})])
    monkeypatch.setattr(session, "_all_known_missing", lambda _refs: False)
    fetched: list[list[tuple[str, int]]] = []
    monkeypatch.setattr(
        session,
        "_fetch_telemetry_batch_from_refs",
        lambda refs, **_: ([], fetched.append(refs) or [], []),
    )
    _load(session)
    assert reads == [["HAM", "LEC", "VER"]]  # the eager batch read every row in load()
    assert fetched == [[("VER", 4)]]
    assert session._lazy_telemetry_refs == {}


def test_a_known_missing_lap_does_not_stop_the_lazy_load(reads, monkeypatch):
    session = _session()
    session._laps = pl.concat([session._laps, pl.DataFrame({"Driver": ["VER"], "LapNumber": [4]})])
    asked: list[list[tuple[str, int]]] = []
    monkeypatch.setattr(session, "_all_known_missing", lambda refs: asked.append(refs) or True)
    _load(session)
    assert reads == []
    assert asked[0] == [("VER", 4)]
    assert session._lazy_telemetry_refs["VER"] == [("VER", lap) for lap in (*LAPS, 4)]


def test_an_empty_tier_keeps_the_eager_batch(cache, reads):
    with cache._sqlite_lock:
        cache.conn.execute(f"DELETE FROM {_POLARS_IPC_TABLE}")
    session = _session()
    assert session._telemetry_load_pending()
    assert session._lazy_telemetry_refs == {}


def test_a_session_the_cache_does_not_hold_keeps_the_eager_batch(reads):
    session = _session()
    session._memo.has_session_data = False
    assert session._telemetry_load_pending()
    assert reads == []
    assert session._lazy_telemetry_refs == {}


def test_ultra_cold_keeps_the_eager_batch(reads, monkeypatch):
    session = _session()
    monkeypatch.setattr(session, "_resolve_telemetry_ultra_cold_mode", lambda _value: True)
    assert session._telemetry_load_pending()
    assert session._lazy_telemetry_refs == {}


def test_memoized_laps_are_not_registered(reads):
    session = _session()
    _tel(session, "VER", 1)  # VER's lone lap: its cached payload or the tier
    session._memo.single_lap_drivers.add("LEC")
    _tel(session, "LEC", 2)  # reads LEC's whole row: every LEC lap is memoized
    before = len(reads)
    _load(session)
    assert len(reads) == before
    assert "LEC" not in session._lazy_telemetry_refs
    assert session._lazy_telemetry_refs["HAM"] == [("HAM", lap) for lap in LAPS]


def test_tier_lap_lists_read_the_polars_spans(cache):
    stored = cache.get_telemetry_frame_laps(2026, GP, "Race", ["VER", "XXX", "HAM"], lib="polars")
    assert stored == {"VER": list(LAPS), "HAM": list(LAPS)}
    with cache._sqlite_lock:
        cache.conn.execute(
            f"UPDATE {_POLARS_IPC_TABLE} SET laps = ? WHERE driver = ?", ("[1, 2]", "HAM")
        )
    stored = cache.get_telemetry_frame_laps(2026, GP, "Race", ["VER", "HAM"], lib="polars")
    assert stored == {"VER": list(LAPS)}  # an unreadable span list is absent


def _laps(session: core.Session, frame: pl.DataFrame | None = None) -> PolarsLaps:
    return PolarsLaps._from_frame(session._laps if frame is None else frame, session)


def test_a_walk_over_the_session_laps_reads_every_row_at_once(reads):
    session = _load(_session())
    frames = [lap.telemetry for _, lap in _laps(session).iterlaps()]
    assert reads == [["HAM", "LEC", "VER"]]
    assert session._lazy_telemetry_refs == {}
    expected = [_frame(driver, lap) for driver in DRIVERS for lap in LAPS]
    assert all(got.equals(want) for got, want in zip(frames, expected, strict=True))
    assert all(type(got) is PolarsTelemetry and got.session is session for got in frames)


def test_a_walk_over_one_driver_reads_that_row_only(reads):
    session = _load(_session())
    one = _laps(session, session._laps.filter(pl.col("Driver") == "LEC"))
    for _, lap in one.iterlaps():
        lap.get_car_data()
    assert reads == [["LEC"]]
    assert set(session._lazy_telemetry_refs) == {"VER", "HAM"}


def test_a_walk_after_a_touched_driver_reads_the_rest_at_once(reads):
    session = _load(_session())
    _tel(session, "VER", 1)
    walk = list(_laps(session).iterlaps())
    assert reads == [["VER"]]  # building the walk reads nothing
    _ = walk[0][1].telemetry  # VER: already read, the walk does not start a wave
    assert reads == [["VER"]]
    _ = walk[-1][1].telemetry
    assert reads == [["VER"], ["HAM", "LEC"]]


def test_a_walk_on_a_cold_lazy_load_keeps_the_per_driver_waves(reads, monkeypatch):
    session = _session()
    session._set_lazy_telemetry_refs([(driver, lap) for driver in DRIVERS for lap in LAPS])
    monkeypatch.setattr(session, "_lazy_telemetry_from_cache", lambda: False)
    waves: list[list[str]] = []
    monkeypatch.setattr(
        session,
        "_fetch_lazy_telemetry_wave",
        lambda refs: waves.append(list(dict.fromkeys(driver for driver, _ in refs))),
    )
    for _, lap in _laps(session).iterlaps():
        _ = lap.telemetry
        break
    assert waves == [["VER"]]


def test_laps_outside_a_walk_carry_none():
    session = _session()
    laps = _laps(session)
    assert laps.pick_fastest() is None or laps.pick_fastest()._walk is None
    walk_laps = [lap for _, lap in laps.iterlaps()]
    assert walk_laps[0]._walk is walk_laps[-1]._walk is not None
    assert walk_laps[0]._walk.refs() == [(d, lap) for d in DRIVERS for lap in LAPS]
    required = [lap for _, lap in laps.iterlaps(require=["LapNumber"])]
    assert required[0]._walk.refs() == walk_laps[0]._walk.refs()
