"""Frame-tier adoption and lap references (Olympia H5).

``Session._adopt_telemetry_frame`` binds a tier-built frame to its session:
pandas frames in place (nothing else to do when the clock is stored), polars
frames wrapped as ``PolarsTelemetry`` without cloning their columns, and a
polars lap-by-lap loop reads a driver's tier row once. The lap references of
``fetch_all_laps_telemetry`` come from column arrays, and the polars per-lap
build repeats its constant categorical columns. Nothing here reaches the
network.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import polars as pl
import pytest

from tif1 import core
from tif1.models import Telemetry
from tif1.models_polars import PolarsTelemetry


def _session(lib: str) -> core.Session:
    return core.Session(2025, "Bahrain Grand Prix", "Race", enable_cache=False, lib=lib)


def _reference_refs_pandas(laps: pd.DataFrame) -> list[tuple[str, int]]:
    """The previous extraction (row-wise over ``dropna``), the behaviour to keep."""
    refs = laps[["Driver", "LapNumber"]].dropna()
    return list(zip(refs["Driver"].astype(str), refs["LapNumber"].astype(int)))


@pytest.mark.parametrize("categorical", [True, False])
def test_pandas_lap_refs_match_the_row_wise_extraction(categorical):
    drivers = pd.Series(["VER", None, "LEC", "VER", "HAM", "LEC"], dtype=object)
    if categorical:
        drivers = drivers.astype("category")
    laps = pd.DataFrame(
        {"Driver": drivers, "LapNumber": [1.0, 2.0, np.nan, 3.0, 1.0, 2.0], "X": range(6)}
    )
    refs = _session("pandas")._telemetry_lap_refs(laps)
    assert refs == _reference_refs_pandas(laps)
    assert refs == [("VER", 1), ("VER", 3), ("HAM", 1), ("LEC", 2)]
    assert all(type(d) is str and type(n) is int for d, n in refs)


def test_pandas_lap_refs_accept_nullable_lap_numbers():
    laps = pd.DataFrame(
        {
            "Driver": pd.Series(["VER", "VER", "NOR"], dtype="category"),
            "LapNumber": pd.array([1, None, 7], dtype="Int64"),
        }
    )
    refs = _session("pandas")._telemetry_lap_refs(laps)
    assert refs == _reference_refs_pandas(laps) == [("VER", 1), ("NOR", 7)]


def test_polars_lap_refs_skip_missing_and_empty_drivers():
    laps = pl.DataFrame(
        {
            "Driver": ["VER", None, "", "LEC", "VER"],
            "LapNumber": pl.Series([1, 2, 3, None, 4], dtype=pl.Int64),
        }
    )
    refs = _session("polars")._telemetry_lap_refs(laps)
    assert refs == [("VER", 1), ("VER", 4)]
    assert all(type(d) is str and type(n) is int for d, n in refs)


def test_polars_lap_refs_without_the_columns_are_empty():
    session = _session("polars")
    assert session._telemetry_lap_refs(pl.DataFrame({"Driver": ["VER"]})) == []
    assert session._telemetry_lap_refs(pl.DataFrame({"LapNumber": [1]})) == []


def _tier_frame(with_clock: bool) -> pd.DataFrame:
    n = 3
    columns: dict[str, object] = {}
    if with_clock:
        columns["Date"] = pd.date_range("2025-04-13 15:00", periods=n, freq="250ms")
        columns["SessionTime"] = pd.to_timedelta([60.0, 60.25, 60.5], unit="s")
    columns["Time"] = pd.to_timedelta([0.0, 0.25, 0.5], unit="s")
    columns["Speed"] = [100.0, 110.0, 120.0]
    columns["LapNumber"] = pd.array([5] * n, dtype="Int64")
    return pd.DataFrame(columns)


def test_pandas_tier_frame_with_its_clock_is_bound_in_place(monkeypatch):
    session = _session("pandas")
    frame = Telemetry(_tier_frame(with_clock=True))
    before = list(frame.columns)
    calls = []
    monkeypatch.setattr(session, "_attach_session_clock", lambda f, d=None: calls.append(d) or f)
    adopted = session._adopt_telemetry_frame(frame, "VER")
    assert adopted is frame
    assert adopted.session is session
    assert adopted.driver == "VER"
    assert list(adopted.columns) == before
    assert calls == []  # the stored clock is kept as it is


def test_pandas_tier_frame_without_a_clock_still_gets_one(monkeypatch):
    session = _session("pandas")
    frame = Telemetry(_tier_frame(with_clock=False))
    calls = []
    monkeypatch.setattr(session, "_attach_session_clock", lambda f, d=None: calls.append(d) or f)
    adopted = session._adopt_telemetry_frame(frame, "VER")
    assert adopted is frame
    assert adopted.session is session
    assert calls == ["VER"]


def test_attach_session_clock_checks_the_column_index_without_listing_it():
    session = _session("pandas")
    frame = Telemetry(_tier_frame(with_clock=True))
    assert session._attach_session_clock(frame, "VER") is frame
    no_lap = Telemetry(_tier_frame(with_clock=False).drop(columns="LapNumber"))
    assert session._attach_session_clock(no_lap, "VER") is no_lap


def test_polars_tier_frame_is_wrapped_without_a_column_clone():
    session = _session("polars")
    frame = pl.from_pandas(_tier_frame(with_clock=True))
    expected = frame.clone()
    adopted = session._adopt_telemetry_frame(frame, "VER")
    assert type(adopted) is PolarsTelemetry
    assert adopted.session is session
    assert adopted.driver == "VER"
    assert adopted._df is frame._df  # taken over, not cloned
    assert adopted.equals(expected)
    assert adopted.schema == expected.schema
    # The same result the generic binding gives (which clones).
    bound = session._bind_telemetry(expected, "VER")
    assert type(bound) is PolarsTelemetry
    assert bound.equals(adopted)
    assert (bound.session, bound.driver) == (adopted.session, adopted.driver)


def test_polars_lap_by_lap_reads_the_driver_row_once(tmp_path, monkeypatch):
    from tif1.cache import Cache

    gp = "Monaco%20Grand%20Prix"
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = {}
        for driver in ("VER", "LEC"):
            for lap in (1, 2, 3):
                frame = _tier_frame(with_clock=True)
                frame["LapNumber"] = pd.array([lap] * len(frame), dtype="Int64")
                frame["Speed"] = frame["Speed"] + lap
                frames[(driver, lap)] = pl.from_pandas(frame)
        rows = [(d, lap, f) for (d, lap), f in frames.items()]
        assert cache.set_telemetry_frames_batch(2026, gp, "Race", rows, lib="polars") == 6
        monkeypatch.setattr(core, "get_cache", lambda: cache)
        session = core.Session(2026, "Monaco Grand Prix", "Race", True, "polars")
        session._memo.has_session_data = True
        session._laps = pl.DataFrame(
            {"Driver": ["VER"] * 3 + ["LEC"] * 3, "LapNumber": [1, 2, 3, 1, 2, 3]}
        )
        session._memo.single_lap_drivers.add("VER")  # past the lone-lap payload read
        reads = []
        read = cache.get_telemetry_frames_batch

        def spy(year, gp_, session_name, refs, **kwargs):
            reads.append(sorted(refs))
            return read(year, gp_, session_name, refs, **kwargs)

        monkeypatch.setattr(cache, "get_telemetry_frames_batch", spy)
        got = {
            lap: session._get_telemetry_df_for_ref("VER", lap, ultra_cold=False)
            for lap in (2, 1, 3)
        }
        assert reads == [[("VER", 1), ("VER", 2), ("VER", 3)]]
        for lap, tel in got.items():
            assert type(tel) is PolarsTelemetry
            assert tel.session is session
            assert tel.driver == "VER"
            assert tel.equals(frames[("VER", lap)])
            assert tel is session._memo.get("telemetry_df", ("VER", lap))
        assert session._memo.get("telemetry_df", ("LEC", 1)) is None
    finally:
        cache.close()


@pytest.mark.parametrize("n", [0, 1, 2, 3, 700])
@pytest.mark.parametrize("value", ["car", "OnTrack", ""])
def test_polars_constant_categorical_is_the_list_built_series(n, value):
    from tif1.core_utils import helpers

    assert helpers._ensure_polars_available()
    expected = pl.Series("Source", [value] * n, dtype=pl.Categorical)
    got = helpers._polars_constant_categorical("Source", value, n)
    assert got.name == expected.name
    assert got.dtype == expected.dtype
    assert got.equals(expected)
    assert got.flags == expected.flags
    assert got.n_chunks() == expected.n_chunks()
    assert got.to_arrow().equals(expected.to_arrow())


def test_polars_tier_frame_without_a_clock_goes_through_the_binding(monkeypatch):
    session = _session("polars")
    frame = pl.from_pandas(_tier_frame(with_clock=False))
    calls = []
    original = session._bind_telemetry

    def spy(f, d):
        calls.append(d)
        return original(f, d)

    monkeypatch.setattr(session, "_bind_telemetry", spy)
    adopted = session._adopt_telemetry_frame(frame, "VER")
    assert calls == ["VER"]
    assert type(adopted) is PolarsTelemetry
    assert adopted.session is session
