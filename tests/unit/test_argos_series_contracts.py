"""Contracts for the Argos-series performance changes.

- A1/A2: typed telemetry frames are built consolidated with the same columns,
  dtypes and values as the dict-constructor path; the vectorized
  seconds->timedelta cast is bit-identical to ``pd.to_timedelta(unit="s")``.
- A3: the deferred telemetry flush completes before the cache closes.
- A4/A5: laps frames leave ``_process_lap_df`` consolidated; ``pick_fastest``
  and ``iterlaps`` keep their row/column semantics.
- A6: telemetry slicing helpers keep their results.
- A11: a 403 from a CDN is a per-file refusal (no health penalty, falls
  through, still ``DataNotFoundError`` when every source refuses).
"""

from __future__ import annotations

import asyncio

import numpy as np
import pandas as pd
import pytest

from tif1.cache import Cache, _join_frame_writers
from tif1.cdn import CDNManager
from tif1.core_utils.helpers import (
    _create_telemetry_df,
    _numeric_seconds_to_timedelta,
    _process_lap_df,
    _seconds_to_td64ns,
)
from tif1.exceptions import DataNotFoundError, NetworkError
from tif1.models import Laps, Telemetry


def _payload(n: int = 50, leader: bool = False) -> dict:
    rng = np.random.default_rng(0)
    return {
        "time": np.cumsum(rng.random(n) * 0.25).tolist(),
        "rpm": rng.integers(5000, 12000, n).tolist(),
        "speed": (rng.random(n) * 300).tolist(),
        "gear": rng.integers(1, 8, n).tolist(),
        "throttle": rng.integers(0, 100, n).tolist(),
        "brake": rng.integers(0, 2, n).tolist(),
        "drs": rng.integers(0, 14, n).tolist(),
        "distance": (rng.random(n) * 3000).tolist(),
        "rel_distance": rng.random(n).tolist(),
        "driver_ahead": ["None"] * n if leader else ["1"] * n,
        "distance_to_driver_ahead": ["None"] * n if leader else (rng.random(n) * 100).tolist(),
        "x": rng.random(n).tolist(),
        "y": rng.random(n).tolist(),
        "z": rng.random(n).tolist(),
    }


class TestA1A2TypedFrames:
    @pytest.mark.parametrize("leader", [False, True])
    def test_frame_matches_dict_constructor_semantics(self, leader):
        frame = _create_telemetry_df(_payload(leader=leader), "VER", 3, "pandas")
        assert frame is not None
        assert str(frame["Time"].dtype) == "timedelta64[ns]"
        assert str(frame["nGear"].dtype) == "Int64"
        assert str(frame["LapNumber"].dtype) == "Int64"
        assert frame["Driver"].dtype == object
        assert frame["Brake"].dtype == bool
        assert frame["RPM"].dtype == "int64"
        assert frame["Speed"].dtype == "float64"
        assert frame["DistanceToDriverAhead"].dtype == "float64"
        if leader:
            assert frame["DistanceToDriverAhead"].isna().all()
            assert frame["DriverAhead"].isna().all()
        # A1: consolidated blocks (one block per column was the old shape).
        assert len(frame._mgr.blocks) < len(frame.columns)

    def test_padded_and_sentinel_payloads_still_build(self):
        payload = _payload()
        payload["speed"] = payload["speed"][:40]  # padded with None
        payload["x"][3] = "None"  # sentinel mid-channel
        frame = _create_telemetry_df(payload, "VER", 3, "pandas")
        assert frame is not None
        assert len(frame) == 50
        assert frame["Speed"].isna().sum() == 10
        assert np.isnan(frame["X"].iloc[3])

    def test_seconds_to_td64ns_matches_pandas(self):
        rng = np.random.default_rng(1)
        for _ in range(20):
            seconds = np.cumsum(rng.random(400) * 0.3) + rng.integers(0, 7000)
            seconds[rng.integers(0, 400, 3)] = np.nan
            expected = pd.to_timedelta(seconds, unit="s").values.view("i8")
            got = _seconds_to_td64ns(seconds).view("i8")
            assert np.array_equal(expected, got)

    def test_out_of_range_seconds_follow_pandas(self):
        """inf / huge values leave the vectorized cast and raise like pandas."""
        from tif1.core_utils.helpers import _td_cast_is_safe

        assert _td_cast_is_safe(np.array([1.5, np.nan, 80.0]))
        assert not _td_cast_is_safe(np.array([1.5, np.inf]))
        assert not _td_cast_is_safe(np.array([1.0e10]))
        assert not _td_cast_is_safe(np.array([], dtype="float64"))
        # Beyond the nanosecond range pandas widens the resolution; the
        # fallback hands the values to pandas instead of the vectorized cast.
        huge = _numeric_seconds_to_timedelta(pd.Series([1.0, 1.0e10, None], dtype="float64"))
        assert huge.dtype.kind == "m"
        assert huge.iloc[1].total_seconds() == 1.0e10
        assert pd.isna(huge.iloc[2])
        with pytest.raises(OverflowError):  # pandas' own behaviour for inf
            _numeric_seconds_to_timedelta(pd.Series([1.0, np.inf]))
        # Telemetry Time beyond the ns range: integral seconds widen to a
        # coarser resolution through pandas; a mix with fractional seconds is
        # unrepresentable and pandas raises, so the frame drops to None exactly
        # as the constructor path did.
        payload = _payload()
        payload["time"] = [1.0e10 + float(i) for i in range(len(payload["time"]))]
        frame = _create_telemetry_df(payload, "VER", 3, "pandas")
        assert frame is not None
        assert frame["Time"].dtype.kind == "m"
        assert frame["Time"].iloc[0].total_seconds() == 1.0e10
        payload = _payload()
        payload["time"][0] = 1.0e10
        assert _create_telemetry_df(payload, "VER", 3, "pandas") is None

    def test_numeric_seconds_to_timedelta_keeps_contract(self):
        values = pd.Series([80.5, None, "x", 0.0], dtype=object, name="LapTime")
        out = _numeric_seconds_to_timedelta(values)
        assert str(out.dtype) == "timedelta64[ns]"
        assert out.name == "LapTime"
        assert out.iloc[0] == pd.Timedelta(seconds=80.5)
        assert pd.isna(out.iloc[1])
        assert pd.isna(out.iloc[2])
        assert out.iloc[3] == pd.Timedelta(0)


class TestA3DeferredFlush:
    def test_flush_is_joined_before_close(self, tmp_path):
        from functools import partial

        from tif1.async_fetch import _flush_telemetry_writes
        from tif1.cache import register_frame_writer

        cache = Cache(tmp_path)
        entries = [(2026, "GP", "Race", "VER", lap, {"speed": [1.0] * 10}) for lap in range(5)]
        register_frame_writer(partial(_flush_telemetry_writes, cache, entries))
        _join_frame_writers()
        for lap in range(5):
            assert cache.get_telemetry(2026, "GP", "Race", "VER", lap) == {"speed": [1.0] * 10}
        cache.close()


class TestA4A5LapsModel:
    def _laps(self) -> Laps:
        n = 12
        raw = pd.DataFrame(
            {
                "lap": list(range(1, n + 1)),
                "time": [90.0 + (i % 5) * 0.1 for i in range(n)],
                "Driver": ["VER"] * n,
                "Team": ["Red Bull"] * n,
                "compound": ["SOFT"] * n,
                "deleted": [False] * n,
                "stint": [1] * n,
            }
        )
        raw.loc[3, "time"] = None
        processed = _process_lap_df(raw, "pandas")
        laps = Laps(processed)
        return laps

    def test_laps_frame_is_consolidated(self):
        laps = self._laps()
        assert len(laps._mgr.blocks) < len(laps.columns)

    def test_pick_fastest_matches_nsmallest(self):
        laps = self._laps()
        fastest = laps.pick_fastest()
        assert fastest is not None
        valid = laps[laps["LapTime"].notna()]
        expected = valid.nsmallest(1, "LapTime").iloc[0]
        assert fastest["LapNumber"] == expected["LapNumber"]
        assert fastest.name == expected.name

    def test_iterlaps_drops_null_columns_and_skips_missing_required(self):
        laps = self._laps()
        rows = list(laps.iterlaps())
        assert len(rows) == 11  # the NaT lap is skipped (LapTime required)
        for index, lap in rows:
            assert not lap.isna().any()
            assert lap.name == index
            assert lap["Driver"] == "VER"
        with pytest.raises(KeyError):
            list(laps.iterlaps(require=["NoSuchColumn"]))


class TestA6TelemetryHelpers:
    def _tel(self) -> Telemetry:
        frame = _create_telemetry_df(_payload(), "VER", 3, "pandas")
        assert frame is not None
        return Telemetry(frame)

    def test_first_non_zero_time_index(self):
        tel = self._tel()
        assert tel.get_first_non_zero_time_index() == 0
        tel2 = Telemetry(tel.copy())
        tel2.loc[0, "Time"] = pd.Timedelta(0)
        assert tel2.get_first_non_zero_time_index() == 1

    def test_slice_by_time_and_mask(self):
        tel = self._tel()
        window = tel.slice_by_time(pd.Timedelta(seconds=1), pd.Timedelta(seconds=3))
        assert isinstance(window, Telemetry)
        assert (window["Time"] >= pd.Timedelta(0)).all()
        mask = np.zeros(len(tel), dtype=bool)
        mask[10:20] = True
        padded = tel.slice_by_mask(mask, pad=2, pad_side="before")
        assert list(padded.index) == list(range(8, 20))
        assert not mask[8]  # caller's mask untouched


def _manager(statuses: list[int | None]):
    manager = CDNManager()
    sources = []
    for i, status in enumerate(statuses):
        src = type("Src", (), {})()
        src.name = f"src{i}"
        src.enabled = True
        src.priority = i
        src.format_url = lambda year, gp, session, path: f"https://src/{path}"  # noqa: ARG005
        src.fail_status = status
        sources.append(src)
    manager.sources = sources
    manager._failure_counts = {s.name: 0 for s in sources}
    return manager


def _fetch_for(manager):
    def _fetch(source, url):
        async def _inner():
            status = source.fail_status
            if status is None:
                return {"ok": url}
            if status == 404:
                raise DataNotFoundError(year=2026, event="GP", session="Race")
            err = NetworkError(url=url, status_code=status)
            err.response = type("R", (), {"status_code": status})()
            raise err

        return _inner()

    return _fetch


class TestA11FileLevel403:
    def test_403_falls_through_without_health_penalty(self):
        manager = _manager([403, None])
        fetch = _fetch_for(manager)
        for _ in range(5):
            result = asyncio.run(manager.try_sources_async(2026, "GP", "Race", "p.json", fetch))
            assert result == {"ok": "https://src/p.json"}
        assert manager._failure_counts["src0"] == 0
        assert [s.name for s in manager.get_sources()] == ["src0", "src1"]

    def test_sync_403_falls_through_without_health_penalty(self):
        manager = _manager([403, None])

        calls: list[str] = []

        def sync_fetch(url):
            calls.append(url)
            if len(calls) == 1:
                err = NetworkError(url=url, status_code=403)
                err.response = type("R", (), {"status_code": 403})()
                raise err
            return {"ok": url}

        assert manager.try_sources(2026, "GP", "Race", "p.json", sync_fetch) == {
            "ok": "https://src/p.json"
        }
        assert manager._failure_counts["src0"] == 0

    def test_all_refusals_still_raise_data_not_found(self):
        manager = _manager([403, 404, 403])
        fetch = _fetch_for(manager)
        with pytest.raises(DataNotFoundError):
            asyncio.run(manager.try_sources_async(2026, "GP", "Race", "p.json", fetch))
        assert all(v == 0 for v in manager._failure_counts.values())

    def test_5xx_still_counts_as_failure(self):
        manager = _manager([503, None])
        fetch = _fetch_for(manager)
        asyncio.run(manager.try_sources_async(2026, "GP", "Race", "p.json", fetch))
        assert manager._failure_counts["src0"] == 1
