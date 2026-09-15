"""Tests for the N-series performance contracts.

- N1: cold (network-fetched) telemetry batches materialize assembled frames
  into the frame tier, so the *next* process loads from the frame tier
  directly; the write is gated on the session's cache enablement.
- N3: ``_prefetch_session_tables`` consults the persistent cache before the
  network and persists what it fetches, so a cold ``load()`` warms the cache
  for later processes (previously the fetched tables were memo-only and were
  re-downloaded by every fresh process).
- N5: the frame-tier bulk write (executemany) round-trips every frame with
  the same parity contract as the per-row write it replaced.
"""

from __future__ import annotations

import asyncio

import pandas as pd

from tif1.cache import Cache


def _make_session(monkeypatch, tmp_path, cache=None):
    from tif1 import core

    cache = cache or Cache(tmp_path)
    session = core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")
    monkeypatch.setattr(core, "get_cache", lambda: cache)
    return session, cache


class TestN1ColdFetchMaterializesFrames:
    def test_network_batch_writes_frame_tier(self, tmp_path, monkeypatch):
        session, cache = _make_session(monkeypatch, tmp_path)
        session._memo.has_session_data = True
        session._laps = pd.DataFrame(
            {"Driver": ["VER", "VER"], "LapNumber": pd.array([1, 2], dtype="Int64")}
        )
        payload = {"Speed": [300.0, 301.0], "Time": [1.0, 2.0], "nGear": [5, 6]}

        async def fake_fetch_multiple_async(requests, **kwargs):
            return [{"tel": payload} for _ in requests]

        monkeypatch.setattr("tif1.async_fetch.fetch_multiple_async", fake_fetch_multiple_async)

        tel_map = asyncio.run(session.fetch_all_laps_telemetry_async())
        assert len(tel_map) == 2

        hits = cache.get_telemetry_frames_batch(
            session.year, session.gp, session.session, [("VER", 1), ("VER", 2)]
        )
        assert set(hits) == {("VER", 1), ("VER", 2)}, "cold batch must materialize frames"
        for ref, frame in hits.items():
            assert list(frame.columns) == list(tel_map[ref].columns)
            assert list(frame.dtypes) == list(tel_map[ref].dtypes)

    def test_network_batch_skips_frame_tier_when_cache_disabled(self, tmp_path, monkeypatch):
        session, cache = _make_session(monkeypatch, tmp_path)
        session.enable_cache = False
        session._laps = pd.DataFrame({"Driver": ["VER"], "LapNumber": pd.array([1], dtype="Int64")})
        payload = {"Speed": [300.0], "Time": [1.0], "nGear": [5]}

        async def fake_fetch_multiple_async(requests, **kwargs):
            return [{"tel": payload} for _ in requests]

        monkeypatch.setattr("tif1.async_fetch.fetch_multiple_async", fake_fetch_multiple_async)

        tel_map = asyncio.run(session.fetch_all_laps_telemetry_async())
        assert len(tel_map) == 1
        assert (
            cache.get_telemetry_frames_batch(
                session.year, session.gp, session.session, [("VER", 1)]
            )
            == {}
        )


class TestN3PrefetchPersistsAndReadsCache:
    def _install_fake_http(self, monkeypatch, payloads, calls):
        import niquests

        class _Resp:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

            _payload = None

        class _FakeSession:
            def __init__(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def get(self, url, timeout=None):  # noqa: ARG002
                calls.append(url)
                resp = _Resp()
                resp._payload = payloads[url]
                return resp

        monkeypatch.setattr(niquests, "Session", _FakeSession)
        from tif1.cdn import get_cdn_manager

        manager = get_cdn_manager()
        source = manager.sources[0]
        monkeypatch.setattr(manager, "sources", [source])
        monkeypatch.setattr(
            source,
            "format_url",
            lambda year, gp, session, path: f"https://fake/{path}",  # noqa: ARG005
        )

    def test_prefetch_persists_fetched_tables(self, tmp_path, monkeypatch):
        session, cache = _make_session(monkeypatch, tmp_path)
        session._requested_session_tables = {"weather.json", "rcm.json", "drivers.json"}
        drivers_payload = {"drivers": [{"driver": "VER", "team": "RBR", "dn": "1"}]}
        weather_payload = {"AirTemp": [20.0], "TrackTemp": [25.0]}
        rcm_payload = {"Messages": []}
        calls: list[str] = []
        self._install_fake_http(
            monkeypatch,
            {
                "https://fake/drivers.json": drivers_payload,
                "https://fake/weather.json": weather_payload,
                "https://fake/rcm.json": rcm_payload,
            },
            calls,
        )

        drivers = session._drivers_data
        assert drivers == [{"driver": "VER", "team": "RBR", "dn": "1"}]

        for path, payload in (
            ("drivers.json", drivers_payload),
            ("weather.json", weather_payload),
            ("rcm.json", rcm_payload),
        ):
            key = f"2026/{session.gp}/Race/{path}"
            cached = cache.get(key)
            assert cached == payload, f"{path} must be persisted for later processes"
        assert len(calls) == 3

    def test_prefetch_uses_persistent_cache_before_network(self, tmp_path, monkeypatch):
        session, cache = _make_session(monkeypatch, tmp_path)
        session._requested_session_tables = {"weather.json", "rcm.json", "drivers.json"}
        drivers_payload = {"drivers": [{"driver": "VER", "team": "RBR", "dn": "1"}]}
        weather_payload = {"AirTemp": [20.0], "TrackTemp": [25.0]}
        rcm_payload = {"Messages": []}
        for path, payload in (
            ("drivers.json", drivers_payload),
            ("weather.json", weather_payload),
            ("rcm.json", rcm_payload),
        ):
            cache.set(f"2026/{session.gp}/Race/{path}", payload)

        calls: list[str] = []
        self._install_fake_http(monkeypatch, {}, calls)

        drivers = session._drivers_data
        assert drivers == [{"driver": "VER", "team": "RBR", "dn": "1"}]
        local_weather = session._get_local_payload("weather.json")
        assert local_weather == weather_payload
        assert calls == [], "persistent-cache hits must not touch the network"


class TestN5ExecutemanyFrameWrites:
    def test_bulk_write_round_trips_all_frames(self, tmp_path):
        from tif1.core_utils import helpers

        cache = Cache(tmp_path)
        payload = {"Speed": [300.0, 301.0], "Time": [1.0, 2.0], "nGear": [5, 6]}
        refs = [("VER", 1), ("VER", 2), ("HAM", 1)]
        frames = [
            (drv, lap, helpers._create_telemetry_df(payload, drv, lap, "pandas"))
            for drv, lap in refs
        ]
        frames = [(drv, lap, fr) for drv, lap, fr in frames if fr is not None]

        written = cache.set_telemetry_frames_batch(2026, "GP", "Race", frames)
        assert written == len(frames)

        hits = cache.get_telemetry_frames_batch(2026, "GP", "Race", refs)
        assert set(hits) == set(refs)
        for drv, lap, frame in frames:
            stored = hits[(drv, lap)]
            assert list(stored.columns) == list(frame.columns)
            assert list(stored.dtypes) == list(frame.dtypes)
            pd.testing.assert_frame_equal(stored, frame)
