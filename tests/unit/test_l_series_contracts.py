"""Tests for the L-series performance contracts.

- L2: everywhere-missing (all-CDN 4xx) payloads are remembered across
  processes (TTL-scoped) so warm loads skip the CDN walk; successful writes
  and invalidation drop the verdicts, and discards are no-ops when no
  verdict exists (no per-fetch SQL writes on the cold path).
- L3: a repeat ``fetch_all_laps_telemetry()`` reuses memoized frames instead
  of re-assembling every frame.
- L7: the materialized telemetry-frame tier round-trips assembled DataFrames
  (pickle + zstd) with exact dtype/value parity; corrupt rows degrade to
  misses; invalidation clears the tier.
"""

from __future__ import annotations

import asyncio
import time

import pandas as pd

from tif1.async_fetch import fetch_multiple_async
from tif1.cache import Cache


def _make_config(monkeypatch, overrides: dict | None = None):
    values = {
        "ci_mode": False,
        "offline_mode": False,
        "max_retries": 0,
        "timeout": 5,
        "max_concurrent_requests": 20,
        "validate_data": False,
        "validate_lap_times": False,
        "validate_telemetry": False,
        "retry_backoff_factor": 2.0,
        "retry_jitter": False,
        "max_retry_delay": 60.0,
        "retry_jitter_max": 0.0,
        "missing_payloads_ttl_days": 7.0,
    }
    values.update(overrides or {})

    monkeypatch.setattr(
        "tif1.config.get_config",
        lambda: type(
            "C",
            (),
            {
                "get": classmethod(
                    lambda cls, key, default=None: values.get(key, default)  # noqa: ARG005
                )
            },
        )(),
    )


def _install_fake_transport(monkeypatch, cache, responses):
    """Serve ``responses`` (url -> status/payload) through a counting session.

    The real async fetch pipeline runs; only the HTTP session, cache, circuit
    breaker and the CDN source list are faked.
    """

    class _Resp:
        def __init__(self, status, payload):
            self.status_code = status
            self._payload = payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"status {self.status_code}")

        def json(self):
            return self._payload

    calls: list[str] = []

    class _Session:
        def get(self, url, timeout=None):  # noqa: ARG002
            calls.append(url)
            kind, payload = responses[url]
            return _Resp(200, payload) if kind == "ok" else _Resp(404, None)

    import tif1.async_fetch as af

    monkeypatch.setattr("tif1.async_fetch.get_cache", lambda: cache)
    monkeypatch.setattr(af, "_get_async_session", lambda: _Session())
    monkeypatch.setattr(af, "_get_json_parse_executor", lambda: None)
    monkeypatch.setattr(
        "tif1.http_session._track_request",
        lambda reused=True: None,  # noqa: ARG005
    )
    _make_config(monkeypatch)
    monkeypatch.setattr(
        "tif1.retry.get_circuit_breaker",
        lambda: type(
            "CB",
            (),
            {
                "check_and_update_state": classmethod(lambda cls: (True, "closed")),  # noqa: ARG005
                "record_success": classmethod(lambda cls: None),  # noqa: ARG005
                "record_failure": classmethod(lambda cls: None),  # noqa: ARG005
            },
        )(),
    )

    from tif1.cdn import get_cdn_manager

    manager = get_cdn_manager()
    source = manager.sources[0]
    monkeypatch.setattr(manager, "sources", [source])
    monkeypatch.setattr(manager, "_failure_counts", {source.name: 0})
    monkeypatch.setattr(
        source,
        "format_url",
        lambda year, gp, session, path: "https://x/payload",  # noqa: ARG005
    )
    return calls


class TestL2NegativeResultCache:
    def test_all_4xx_records_and_short_circuits(self, tmp_path, monkeypatch):
        cache = Cache(tmp_path)
        calls = _install_fake_transport(monkeypatch, cache, {"https://x/payload": ("404", None)})

        key = "2026/GP/Race/VER/1_tel.json"
        assert not cache.is_known_missing(key)

        results = asyncio.run(
            fetch_multiple_async([(2026, "GP", "Race", "VER/1_tel.json")], use_cache=True)
        )
        assert results[0] is None
        assert len(calls) >= 1
        assert cache.is_known_missing(key)

        calls.clear()
        results = asyncio.run(
            fetch_multiple_async([(2026, "GP", "Race", "VER/1_tel.json")], use_cache=True)
        )
        assert results[0] is None
        assert calls == [], "verdict should skip the CDN walk"

    def test_ttl_expiry_reprobes(self, tmp_path, monkeypatch):
        cache = Cache(tmp_path)
        _install_fake_transport(monkeypatch, cache, {"https://x/payload": ("404", None)})

        asyncio.run(fetch_multiple_async([(2026, "GP", "Race", "VER/1_tel.json")], use_cache=True))
        key = "2026/GP/Race/VER/1_tel.json"
        assert cache.is_known_missing(key)

        # Age the verdict beyond the TTL.
        with cache._sqlite_lock:
            cache.conn.execute(
                "UPDATE missing_payloads SET recorded_at = ?", (time.time() - 8 * 86400,)
            )
            cache.conn.commit()
        cache._missing_payloads = None
        assert not cache.is_known_missing(key)

    def test_successful_write_discards_verdict(self, tmp_path):
        cache = Cache(tmp_path)
        key = "2026/GP/Race/drivers.json"
        cache.record_missing(key)
        assert cache.is_known_missing(key)
        cache.set(key, {"drivers": []})
        assert not cache.is_known_missing(key)

    def test_discard_without_verdict_is_noop_and_lazy_safe(self, tmp_path):
        """Cold-path contract: discards for keys without verdicts never write SQL."""
        cache = Cache(tmp_path)
        cache.discard_missing("2026/GP/Race/VER/1_tel.json")  # lazy-loads empty tier
        with cache._sqlite_lock:
            cache.conn.execute(
                "INSERT OR REPLACE INTO missing_payloads VALUES (?, ?)",
                ("2026/GP/Race/VER/2_tel.json", time.time()),
            )
            cache.conn.commit()
        cache.discard_missing("2026/GP/Race/VER/2_tel.json")
        assert not cache.is_known_missing("2026/GP/Race/VER/2_tel.json")

    def test_invalidate_scopes_purge_verdicts(self, tmp_path):
        cache = Cache(tmp_path)
        cache.record_missing("2026/GP/Race/drivers.json")
        cache.invalidate("json")
        assert not cache.is_known_missing("2026/GP/Race/drivers.json")

        cache.record_missing("2026/GP/Race/VER/1_tel.json")
        cache.invalidate("telemetry")
        assert not cache.is_known_missing("2026/GP/Race/VER/1_tel.json")

    def test_clear_purges_verdicts(self, tmp_path):
        cache = Cache(tmp_path)
        cache.record_missing("2026/GP/Race/drivers.json")
        cache.clear()
        assert not cache.is_known_missing("2026/GP/Race/drivers.json")
        with cache._sqlite_lock:
            n = cache.conn.execute("SELECT COUNT(*) FROM missing_payloads").fetchone()[0]
        assert n == 0

    def test_verdict_survives_new_cache_instance(self, tmp_path):
        cache = Cache(tmp_path)
        cache.record_missing("2026/GP/Race/VER/66_tel.json")
        cache.close()

        cache2 = Cache(tmp_path)
        assert cache2.is_known_missing("2026/GP/Race/VER/66_tel.json")


class TestL3MemoizedFrameReuse:
    def test_repeat_fetch_all_reuses_frames(self, tmp_path, monkeypatch):
        from tif1 import core

        cache = Cache(tmp_path)
        session = core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")
        session._memo.has_session_data = True  # force warm-cache semantics
        monkeypatch.setattr(core, "get_cache", lambda: cache)
        session._laps = pd.DataFrame(
            {"Driver": ["VER", "VER"], "LapNumber": pd.array([1, 2], dtype="Int64")}
        )
        payload = {"Speed": [300.0, 301.0], "Time": [1.0, 2.0], "nGear": [5, 6]}
        session._remember_telemetry_payload("VER", 1, payload)
        session._remember_telemetry_payload("VER", 2, payload)

        build_calls: list[tuple[str, int]] = []
        real_create = core._create_telemetry_df

        def counting_create(tel_data, driver, lap_num, lib):
            build_calls.append((driver, lap_num))
            return real_create(tel_data, driver, lap_num, lib)

        monkeypatch.setattr(core, "_create_telemetry_df", counting_create)

        tel_map = asyncio.run(session.fetch_all_laps_telemetry_async())
        assert len(tel_map) == 2
        assert len(build_calls) == 2

        build_calls.clear()
        tel_map2 = asyncio.run(session.fetch_all_laps_telemetry_async())
        assert len(tel_map2) == 2
        assert build_calls == [], "repeat call must reuse memoized frames"
        assert all(tel_map[k] is tel_map2[k] for k in tel_map)


class TestL7MaterializedFrames:
    def test_round_trip_exact_parity(self, tmp_path):
        from tif1.core_utils import helpers

        cache = Cache(tmp_path)
        payload = {"Speed": [300.0, 301.0], "Time": [1.0, 2.0], "nGear": [5, 6]}
        frame = helpers._create_telemetry_df(payload, "VER", 1, "pandas")

        written = cache.set_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1, frame)])
        assert written == 1

        hits = cache.get_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1), ("HAM", 1)])
        assert set(hits) == {("VER", 1)}
        restored = hits[("VER", 1)]
        assert restored.equals(frame)
        assert str(restored.dtypes) == str(frame.dtypes)

    def test_corrupt_blob_degrades_to_miss(self, tmp_path):
        cache = Cache(tmp_path)
        with cache._sqlite_lock:
            cache.conn.execute(
                "INSERT OR REPLACE INTO telemetry_frames VALUES (?, ?, ?, ?, ?, ?)",
                (2026, "GP", "Race", "VER", 1, b"not-a-zstd-frame"),
            )
            cache.conn.commit()
        assert cache.get_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1)]) == {}

    def test_invalidate_telemetry_clears_frames(self, tmp_path):
        from tif1.core_utils import helpers

        cache = Cache(tmp_path)
        frame = helpers._create_telemetry_df({"Speed": [1.0]}, "VER", 1, "pandas")
        cache.set_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1, frame)])
        cache.invalidate("telemetry")
        assert cache.get_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1)]) == {}

    def test_read_only_cache_writes_nothing(self, tmp_path):
        cache = Cache(tmp_path)
        cache.read_only = True
        assert cache.set_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1, object())]) == 0

    def test_frames_tier_skips_uncached_sessions(self, tmp_path, monkeypatch):
        """Cold-start contract: the frames tier is never READ for cold
        sessions, and frames materialize only from cache/memo-sourced
        payloads (the network fresh-results path never writes the tier)."""
        from tif1 import core

        cache = Cache(tmp_path)
        read_calls: list = []
        real_get = cache.get_telemetry_frames_batch

        def counting_get(*args, **kwargs):
            read_calls.append(args)
            return real_get(*args, **kwargs)

        monkeypatch.setattr(cache, "get_telemetry_frames_batch", counting_get)
        monkeypatch.setattr(core, "get_cache", lambda: cache)

        session = core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")
        session._memo.has_session_data = False  # cold session
        session._laps = pd.DataFrame({"Driver": ["VER"], "LapNumber": pd.array([1], dtype="Int64")})
        session._remember_telemetry_payload("VER", 1, {"Speed": [1.0, 2.0]})

        tel_map = asyncio.run(session.fetch_all_laps_telemetry_async())
        assert read_calls == [], "cold sessions must not read the frames tier"
        # The memo-sourced payload materialized its frame; a network-fresh
        # payload (none in this test) would not.
        assert len(tel_map) == 1
        hits = cache.get_telemetry_frames_batch(2026, session.gp, "Race", [("VER", 1)])
        assert ("VER", 1) in hits
        assert hits[("VER", 1)].equals(tel_map[("VER", 1)])
