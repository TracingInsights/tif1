"""Rhodes-series performance contracts.

Covers the kept experiments:
- R3: GC suspension around batch frame read/write restores prior GC state and
  never leaks a disabled GC.
- R4: polars keeps its own materialized frame tier
  (``telemetry_frames_pl``); cross-backend reads never surface foreign frame
  objects; polars warm loads read the frame tier like pandas does.
"""

from __future__ import annotations

import asyncio
import gc

import pandas as pd
import pytest

from tif1.cache import Cache

polars = pytest.importorskip("polars", reason="polars not installed")


def _make_polars_frame():
    from tif1.core_utils import helpers

    return helpers._create_telemetry_df({"Speed": [300.0, 301.0], "Time": [1.0, 2.0]}, "VER", 1, "polars")


class TestR3GcSuspension:
    def test_suspend_restores_enabled_gc(self):
        from tif1.cache import _suspend_gc

        assert gc.isenabled()
        with _suspend_gc():
            assert not gc.isenabled()
        assert gc.isenabled()

    def test_suspend_restores_disabled_gc(self):
        from tif1.cache import _suspend_gc

        gc.disable()
        try:
            with _suspend_gc():
                assert not gc.isenabled()
            assert not gc.isenabled(), "prior disabled state must be preserved"
        finally:
            gc.enable()

    def test_suspend_restores_on_exception(self):
        from tif1.cache import _suspend_gc

        with pytest.raises(RuntimeError):
            with _suspend_gc():
                raise RuntimeError("boom")
        assert gc.isenabled()


class TestR4PolarsFrameTier:
    def test_polars_round_trip_parity(self, tmp_path):
        from tif1.core_utils import helpers

        cache = Cache(tmp_path)
        payload = {"Speed": [300.0, 301.0], "Time": [1.0, 2.0], "nGear": [5, 6]}
        frame = helpers._create_telemetry_df(payload, "VER", 1, "polars")

        written = cache.set_telemetry_frames_batch(
            2026, "GP", "Race", [("VER", 1, frame)], lib="polars"
        )
        assert written == 1

        hits = cache.get_telemetry_frames_batch(
            2026, "GP", "Race", [("VER", 1), ("HAM", 1)], lib="polars"
        )
        assert set(hits) == {("VER", 1)}
        restored = hits[("VER", 1)]
        assert isinstance(restored, polars.DataFrame)
        assert restored.equals(frame)

    def test_backends_are_isolated(self, tmp_path):
        from tif1.core_utils import helpers

        cache = Cache(tmp_path)
        pd_frame = helpers._create_telemetry_df({"Speed": [1.0]}, "VER", 1, "pandas")
        pl_frame = _make_polars_frame()
        cache.set_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1, pd_frame)])
        cache.set_telemetry_frames_batch(2026, "GP", "Race", [("HAM", 2, pl_frame)], lib="polars")

        pd_hits = cache.get_telemetry_frames_batch(
            2026, "GP", "Race", [("VER", 1), ("HAM", 2)]
        )
        pl_hits = cache.get_telemetry_frames_batch(
            2026, "GP", "Race", [("VER", 1), ("HAM", 2)], lib="polars"
        )
        # pandas read sees only its row; polars read sees only its row.
        assert set(pd_hits) == {("VER", 1)}
        assert isinstance(pd_hits[("VER", 1)], pd.DataFrame)
        assert set(pl_hits) == {("HAM", 2)}
        assert isinstance(pl_hits[("HAM", 2)], polars.DataFrame)

    def test_foreign_blob_degrades_to_miss(self, tmp_path):
        """A blob unpickling to the wrong backend degrades to a tier miss."""
        from tif1.core_utils import helpers

        cache = Cache(tmp_path)
        pd_frame = helpers._create_telemetry_df({"Speed": [1.0]}, "VER", 1, "pandas")
        # Simulate a foreign/migrated cache: pandas object in the polars table.
        cache.set_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1, pd_frame)])
        blob = cache.conn.execute(
            "SELECT frame FROM telemetry_frames WHERE driver = 'VER'"
        ).fetchone()[0]
        cache.conn.execute(
            "INSERT OR REPLACE INTO telemetry_frames_pl VALUES (?, ?, ?, ?, ?, ?)",
            (2026, "GP", "Race", "VER", 1, blob),
        )
        cache.conn.commit()
        assert cache.get_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1)], lib="polars") == {}

    def test_corrupt_polars_blob_degrades_to_miss(self, tmp_path):
        cache = Cache(tmp_path)
        with cache._sqlite_lock:
            cache.conn.execute(
                "INSERT OR REPLACE INTO telemetry_frames_pl VALUES (?, ?, ?, ?, ?, ?)",
                (2026, "GP", "Race", "VER", 1, b"not-a-zstd-frame"),
            )
            cache.conn.commit()
        assert cache.get_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1)], lib="polars") == {}

    def test_polars_warm_load_uses_frame_tier(self, tmp_path, monkeypatch):
        """R4: a warm polars session reads materialized frames instead of
        re-parsing the payload tier on every load."""
        from tif1 import core

        cache = Cache(tmp_path)
        session = core.Session(2026, "Monaco Grand Prix", "Race", True, "polars")
        session._laps = core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")._laps
        # laps as a polars frame with one ref
        session._laps = polars.DataFrame(
            {"Driver": ["VER"], "LapNumber": [1]}
        ).with_columns(polars.col("LapNumber").cast(polars.Int64))
        session._memo.has_session_data = True  # warm session

        frame = _make_polars_frame()
        cache.set_telemetry_frames_batch(
            2026, session.gp, "Race", [("VER", 1, frame)], lib="polars"
        )
        monkeypatch.setattr(core, "get_cache", lambda: cache)

        tel_map = asyncio.run(session.fetch_all_laps_telemetry_async())
        assert ("VER", 1) in tel_map
        assert tel_map[("VER", 1)].equals(frame)


class TestR7MergedTelemetry:
    def test_merged_row_order_and_batch_read(self, tmp_path):
        """R7: the merged driver-telemetry flow reads the payload tier in one
        batched query and keeps laps-row order regardless of SQL row order."""
        from tif1 import core
        from tif1.cache import Cache

        cache = Cache(tmp_path)
        # Seed payloads out of laps-row order (SQL may return any order).
        for lap in (3, 1, 2):
            # payload tier stores the unwrapped channel dict
            cache.set_telemetry(2026, "Monaco%20Grand%20Prix", "Race", "ALB", lap,
                                {"Speed": [300.0 + lap]})

        session = core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")
        laps = pd.DataFrame(
            {
                "Driver": ["ALB"] * 3,
                "LapNumber": pd.array([1, 2, 3], dtype="Int64"),
            }
        )
        from tif1.models import Laps

        laps_obj = Laps(laps)
        laps_obj.session = session
        session._laps = laps_obj
        session._memo.has_session_data = True  # warm session: cache reads allowed
        # warm telemetry semantics (default config ultra_cold_start=True would
        # skip cache reads on this flow)
        session._resolve_telemetry_ultra_cold_mode = lambda *_a, **_k: False

        import tif1.models as models_mod

        orig_get_cache = models_mod.get_cache
        try:
            models_mod.get_cache = lambda: cache
            tel = laps_obj.telemetry
        finally:
            models_mod.get_cache = orig_get_cache

        # rows emitted in laps-row order (lap 1, 2, 3), not PK/SQl order
        assert list(tel["Speed"]) == [301.0, 302.0, 303.0]
