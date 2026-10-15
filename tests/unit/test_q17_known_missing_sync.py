"""Q17 contract: the synchronous payload path honours the negative-result cache.

A payload every CDN refused (all-4xx verdict in ``missing_payloads``) must not
reach the transport again within the verdict TTL, on the sync path
(``PayloadLoader.get`` -> ``Lap.telemetry`` / ``Laps.telemetry``) just like the
async batch path (the read side shipped as Q16). Q17: the sync CDN walk also
records the verdict on all-4xx and drops a stale one when the payload arrives,
so refs only ever touched through per-lap access stop walking after one probe.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tif1.cache import Cache, clear_lap_cache
from tif1.cdn import CDNManager
from tif1.core_utils.helpers import _create_telemetry_df
from tif1.exceptions import DataNotFoundError
from tif1.payload_loader import InMemoryTransport, PayloadLoader

GP = "Monaco%20Grand%20Prix"
TEL = "LEC/66_tel.json"
KEY = f"2026/{GP}/Race/{TEL}"


@pytest.fixture
def cache(tmp_path, monkeypatch):
    from tif1 import cache as cache_module

    instance = Cache(cache_dir=tmp_path)
    monkeypatch.setattr(cache_module, "get_cache", lambda: instance)
    yield instance
    instance.close()


def _loader(transport: InMemoryTransport, *, enable_cache: bool = True) -> PayloadLoader:
    return PayloadLoader(
        2026,
        GP,
        "Race",
        transport=transport,
        cdn_manager=CDNManager(),
        enable_cache=enable_cache,
    )


def test_known_missing_payload_never_reaches_the_transport(cache):
    walking = InMemoryTransport()  # unmatched URL -> 404 on every source
    with pytest.raises(DataNotFoundError) as walked:
        _loader(walking).get(TEL)
    assert walking.calls
    assert cache.is_known_missing(KEY)

    transport = InMemoryTransport()
    with pytest.raises(DataNotFoundError) as skipped:
        _loader(transport).get(TEL)
    assert transport.calls == []
    # Same exception type, message and context as the all-404 CDN walk.
    assert type(skipped.value) is type(walked.value)
    assert str(skipped.value) == str(walked.value)
    assert skipped.value.context == walked.value.context


def test_expired_verdict_is_reprobed(cache):
    cache.record_missing(KEY)
    cache._missing_payloads[KEY] -= 8 * 86400.0  # older than the 7-day TTL
    transport = InMemoryTransport({TEL: {"tel": {"Speed": [1.0]}}})
    assert _loader(transport).get(TEL, validate=False) == {"tel": {"Speed": [1.0]}}
    assert len(transport.calls) == 1
    assert not cache.is_known_missing(KEY)


def test_disabled_cache_ignores_and_never_records_verdicts(cache):
    cache.record_missing(KEY)
    transport = InMemoryTransport()
    with pytest.raises(DataNotFoundError):
        _loader(transport, enable_cache=False).get(TEL)
    assert len(transport.calls) >= 1
    other = f"2026/{GP}/Race/STR/58_tel.json"
    with pytest.raises(DataNotFoundError):
        _loader(transport, enable_cache=False).get("STR/58_tel.json")
    assert not cache.is_known_missing(other)


def test_cache_bypassing_fetch_drops_a_stale_verdict(cache):
    cache.record_missing(KEY)
    transport = InMemoryTransport({TEL: {"tel": {"Speed": [1.0]}}})
    _loader(transport).get(TEL, validate=False, use_cache=False)
    assert len(transport.calls) == 1
    assert not cache.is_known_missing(KEY)


def test_lap_and_driver_telemetry_skip_the_walk_for_known_missing_laps(tmp_path, monkeypatch):
    from tif1 import cache as cache_module
    from tif1 import core
    from tif1 import models as models_module
    from tif1.models import Telemetry

    clear_lap_cache()
    cache = Cache(cache_dir=tmp_path)
    payload = {
        "Time": [0.0, 0.1],
        "RPM": [10000.0, 10001.0],
        "Speed": [200.0, 201.0],
        "nGear": [7, 7],
        "Throttle": [100.0, 100.0],
        "Brake": [0, 0],
        "DRS": [0, 0],
        "DriverAhead": ["44", "44"],
    }
    frame = _create_telemetry_df(payload, "LEC", 65, "pandas")
    assert frame is not None
    # Production warm caches hold both tiers: the payload and its built frame.
    cache.set_telemetry(2026, GP, "Race", "LEC", 65, payload)
    cache.set_telemetry_frames_batch(2026, GP, "Race", [("LEC", 65, frame)])
    laps = pd.DataFrame(
        {
            "Driver": ["LEC", "LEC"],
            "LapNumber": pd.array([65, 66], dtype="Int64"),
            "LapTime": pd.to_timedelta([80.0, 81.0], unit="s"),
        }
    )
    cache.set_laps_frame(2026, GP, "Race", laps)
    cache.record_missing(KEY)
    cache.conn.commit()
    for module in (core, models_module, cache_module):
        monkeypatch.setattr(module, "get_cache", lambda: cache)
    try:
        session = core.Session(2026, "Monaco Grand Prix", "Race", True, "pandas")
        session._memo.has_session_data = True
        session._resolve_ultra_cold_mode = lambda *_a, **_k: False
        session._resolve_telemetry_ultra_cold_mode = lambda *_a, **_k: False
        transport = InMemoryTransport()
        session._payload_loader.transport = transport

        loaded = session.laps
        missing_lap = loaded.pick_drivers("LEC").pick_laps(66).iloc[0]
        tel = missing_lap.telemetry
        assert isinstance(tel, Telemetry)
        assert tel.empty
        merged = loaded.pick_drivers("LEC").telemetry
        assert len(merged) == len(frame)
        assert transport.calls == []
    finally:
        cache.close()
