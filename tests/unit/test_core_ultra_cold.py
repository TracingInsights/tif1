"""Tests for ultra-cold startup behavior in core Session."""

from __future__ import annotations

from unittest.mock import patch

import tif1.core as core_module
from tif1.core import Driver, Lap, Session


class _StubCache:
    """In-memory cache stub with telemetry call tracking."""

    def __init__(self) -> None:
        self.telemetry_get_calls = 0

    def get_telemetry(self, *_args, **_kwargs):  # pragma: no cover - trivial helper
        self.telemetry_get_calls += 1

    def set(self, key: str, data: dict) -> None:
        _ = (key, data)

    def set_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: dict
    ) -> None:
        _ = (year, gp, session, driver, lap, data)


def _build_tel_payload(lap_num: int) -> dict:
    return {
        "tel": {
            "time": [0.0, 1.0, 2.0],
            "speed": [100, 110, 120],
            "throttle": [100, 98, 95],
            "brake": [0, 0, 1],
            "lap": [lap_num, lap_num, lap_num],
        }
    }


def _build_laptime_payload(best_lap: int, best_time: float) -> dict:
    return {
        "lap": [1, best_lap, 3],
        "time": [95.0, best_time, 96.0],
        "compound": ["SOFT", "SOFT", "MEDIUM"],
    }


def _parse_driver_and_lap(path: str) -> tuple[str, int]:
    driver, lap_resource = path.split("/", maxsplit=1)
    lap_num = int(lap_resource.split("_", maxsplit=1)[0])
    return driver, lap_num


def test_get_fastest_lap_tel_ultra_cold_skips_telemetry_cache_lookup(monkeypatch):
    """Ultra-cold mode should skip sync telemetry cache read on critical path."""
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
    session._drivers = [{"driver": "VER", "team": "Red Bull"}]
    session._fastest_lap_ref = ("VER", 5)
    session._fastest_lap_ref_driver_source_id = id(session._drivers)

    cache = _StubCache()
    monkeypatch.setattr("tif1.core.get_cache", lambda: cache)
    monkeypatch.setattr(session, "_fetch_json_unvalidated", lambda _path: _build_tel_payload(5))

    previous_backfill = core_module.config.get("ultra_cold_background_cache_fill", False)
    core_module.config.set("ultra_cold_background_cache_fill", True)
    try:
        with patch.object(session, "_schedule_background_cache_fill") as backfill_mock:
            tel = session.get_fastest_lap_tel(ultra_cold=True)
    finally:
        core_module.config.set("ultra_cold_background_cache_fill", previous_backfill)

    assert not tel.empty
    assert tel["Driver"].iloc[0] == "VER"
    assert int(tel["LapNumber"].iloc[0]) == 5
    assert cache.telemetry_get_calls == 0
    backfill_mock.assert_called_once()


def test_find_fastest_lap_reference_ultra_cold_schedules_backfill(monkeypatch):
    """Ultra-cold laptime scan should trigger background cache backfill."""
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
    drivers = [
        {"driver": "VER", "team": "Red Bull"},
        {"driver": "HAM", "team": "Mercedes"},
    ]

    async def fake_fetch_multiple_async(requests, **_kwargs):
        _ = requests
        return [_build_laptime_payload(7, 88.1), _build_laptime_payload(12, 89.2)]

    monkeypatch.setattr("tif1.core.fetch_multiple_async", fake_fetch_multiple_async)

    previous_backfill = core_module.config.get("ultra_cold_background_cache_fill", False)
    core_module.config.set("ultra_cold_background_cache_fill", True)
    try:
        with patch.object(session, "_schedule_background_cache_fill") as backfill_mock:
            fastest_ref = session._find_fastest_lap_reference_from_raw(drivers, ultra_cold=True)
    finally:
        core_module.config.set("ultra_cold_background_cache_fill", previous_backfill)

    assert fastest_ref == ("VER", 7)
    backfill_mock.assert_called_once()
    payloads = backfill_mock.call_args.kwargs.get("json_payloads", [])
    # Expect 3 payloads: session_laptimes.json + 2 driver laptimes
    assert len(payloads) == 3


def test_get_fastest_lap_tel_auto_cold_start_uses_unvalidated_path(monkeypatch):
    """First default fastest-lap telemetry call should auto-enable cold fast path."""
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")

    def _unexpected_fetch_json(_path: str):
        raise AssertionError("validated fetch path should not run during auto cold-start")

    def _fake_fetch_unvalidated(path: str):
        if path == "drivers.json":
            return {"drivers": [{"driver": "VER", "team": "Red Bull"}]}
        if path.endswith("_tel.json"):
            return _build_tel_payload(7)
        raise AssertionError(f"Unexpected path: {path}")

    async def _fake_fetch_multiple_async(requests, **_kwargs):
        _ = requests
        return [_build_laptime_payload(7, 88.1)]

    monkeypatch.setattr(session, "_fetch_json", _unexpected_fetch_json)
    monkeypatch.setattr(session, "_fetch_json_unvalidated", _fake_fetch_unvalidated)
    monkeypatch.setattr("tif1.core.fetch_multiple_async", _fake_fetch_multiple_async)
    monkeypatch.setattr(session, "_session_cache_available", lambda: False)

    previous_ultra = core_module.config.get("ultra_cold_start", False)
    previous_backfill = core_module.config.get("ultra_cold_background_cache_fill", False)
    core_module.config.set("ultra_cold_start", False)
    core_module.config.set("ultra_cold_background_cache_fill", False)
    try:
        with patch.object(session, "_schedule_background_cache_fill") as backfill_mock:
            tel = session.get_fastest_lap_tel()
    finally:
        core_module.config.set("ultra_cold_start", previous_ultra)
        core_module.config.set("ultra_cold_background_cache_fill", previous_backfill)

    assert not tel.empty
    assert tel["Driver"].iloc[0] == "VER"
    assert int(tel["LapNumber"].iloc[0]) == 7
    assert backfill_mock.call_count == 0


def test_get_fastest_lap_tel_auto_cold_start_memoizes_result(monkeypatch):
    """Auto cold-start path should memoize telemetry to avoid duplicate fetches."""
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
    telemetry_fetch_calls = 0

    def _fake_fetch_unvalidated(path: str):
        nonlocal telemetry_fetch_calls
        if path == "drivers.json":
            return {"drivers": [{"driver": "VER", "team": "Red Bull"}]}
        if path.endswith("_tel.json"):
            telemetry_fetch_calls += 1
            return _build_tel_payload(7)
        raise AssertionError(f"Unexpected path: {path}")

    async def _fake_fetch_multiple_async(requests, **_kwargs):
        _ = requests
        return [_build_laptime_payload(7, 88.1)]

    monkeypatch.setattr(session, "_fetch_json_unvalidated", _fake_fetch_unvalidated)
    monkeypatch.setattr("tif1.core.fetch_multiple_async", _fake_fetch_multiple_async)
    monkeypatch.setattr(session, "_session_cache_available", lambda: False)

    previous_ultra = core_module.config.get("ultra_cold_start", False)
    core_module.config.set("ultra_cold_start", False)
    try:
        with patch.object(session, "_schedule_background_cache_fill"):
            first = session.get_fastest_lap_tel()
            second = session.get_fastest_lap_tel()
    finally:
        core_module.config.set("ultra_cold_start", previous_ultra)

    assert telemetry_fetch_calls == 1
    assert first is second


def test_get_fastest_laps_tels_auto_cold_start_skips_cache_and_validation(monkeypatch):
    """First bulk fastest-laps telemetry call should use the ultra-cold critical path."""
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
    fetch_kwargs: list[dict] = []
    cache = _StubCache()

    async def _fake_fetch_multiple_async(requests, **kwargs):
        fetch_kwargs.append(dict(kwargs))
        results = []
        for _year, _gp, _session, path in requests:
            if path.endswith("/laptimes.json"):
                driver_code = path.split("/", maxsplit=1)[0]
                if driver_code == "VER":
                    results.append(_build_laptime_payload(7, 88.1))
                elif driver_code == "HAM":
                    results.append(_build_laptime_payload(8, 89.2))
                else:
                    results.append(None)
                continue

            if path.endswith("_tel.json"):
                driver, lap_num = _parse_driver_and_lap(path)
                payload = _build_tel_payload(lap_num)
                payload["tel"]["speed"] = [100 + (10 if driver == "HAM" else 0), 110, 120]
                results.append(payload)
                continue

            results.append(None)

        return results

    def _unexpected_fetch_json(_path: str):
        raise AssertionError("validated fetch path should not run during auto cold-start")

    def _fake_fetch_unvalidated(path: str):
        if path == "drivers.json":
            return {
                "drivers": [
                    {"driver": "VER", "team": "Red Bull"},
                    {"driver": "HAM", "team": "Mercedes"},
                ]
            }
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr("tif1.core.get_cache", lambda: cache)
    monkeypatch.setattr("tif1.core.fetch_multiple_async", _fake_fetch_multiple_async)
    monkeypatch.setattr(session, "_fetch_json", _unexpected_fetch_json)
    monkeypatch.setattr(session, "_fetch_json_unvalidated", _fake_fetch_unvalidated)

    previous_ultra = core_module.config.get("ultra_cold_start", False)
    previous_backfill = core_module.config.get("ultra_cold_background_cache_fill", False)
    core_module.config.set("ultra_cold_start", False)
    core_module.config.set("ultra_cold_background_cache_fill", False)
    try:
        tels = session.get_fastest_laps_tels(by_driver=True)
    finally:
        core_module.config.set("ultra_cold_start", previous_ultra)
        core_module.config.set("ultra_cold_background_cache_fill", previous_backfill)

    assert not tels.empty
    assert tels["Driver"].iloc[0] == "VER"
    assert set(tels["Driver"]) == {"VER", "HAM"}
    assert cache.telemetry_get_calls == 0
    assert len(fetch_kwargs) == 2
    for kwargs in fetch_kwargs:
        assert kwargs.get("use_cache") is False
        assert kwargs.get("write_cache") is False
        assert kwargs.get("validate_payload") is False


def test_get_fastest_laps_tels_ultra_cold_schedules_telemetry_backfill(monkeypatch):
    """Ultra-cold bulk telemetry fetch should backfill telemetry cache asynchronously."""
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")

    async def _fake_fetch_multiple_async(requests, **_kwargs):
        results = []
        for _year, _gp, _session, path in requests:
            if path.endswith("/laptimes.json"):
                driver_code = path.split("/", maxsplit=1)[0]
                if driver_code == "VER":
                    results.append(_build_laptime_payload(6, 87.1))
                elif driver_code == "HAM":
                    results.append(_build_laptime_payload(9, 88.4))
                else:
                    results.append(None)
                continue
            if path.endswith("_tel.json"):
                _driver, lap_num = _parse_driver_and_lap(path)
                results.append(_build_tel_payload(lap_num))
                continue
            results.append(None)
        return results

    def _fake_fetch_unvalidated(path: str):
        if path == "drivers.json":
            return {
                "drivers": [
                    {"driver": "VER", "team": "Red Bull"},
                    {"driver": "HAM", "team": "Mercedes"},
                ]
            }
        if path == "session_laptimes.json":
            # Return empty to force per-driver fetch
            return {}
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr("tif1.core.fetch_multiple_async", _fake_fetch_multiple_async)
    monkeypatch.setattr(session, "_fetch_json_unvalidated", _fake_fetch_unvalidated)

    previous_ultra = core_module.config.get("ultra_cold_start", False)
    previous_backfill = core_module.config.get("ultra_cold_background_cache_fill", False)
    core_module.config.set("ultra_cold_start", True)
    core_module.config.set("ultra_cold_background_cache_fill", True)
    try:
        with patch.object(session, "_schedule_background_cache_fill") as backfill_mock:
            tels = session.get_fastest_laps_tels(by_driver=True)
    finally:
        core_module.config.set("ultra_cold_start", previous_ultra)
        core_module.config.set("ultra_cold_background_cache_fill", previous_backfill)

    assert not tels.empty
    telemetry_backfill_calls = [
        call for call in backfill_mock.call_args_list if call.kwargs.get("telemetry_payloads")
    ]
    assert telemetry_backfill_calls
    telemetry_payloads = telemetry_backfill_calls[0].kwargs["telemetry_payloads"]
    assert len(telemetry_payloads) == 2


@patch("tif1.core.Session._fetch_from_cdn")
def test_driver_get_fastest_lap_tel_uses_prefetched_laptime_payload(mock_fetch, monkeypatch):
    """Driver fastest telemetry should reuse prefetched laptime payload without fallback path."""
    mock_fetch.return_value = {"drivers": [{"driver": "VER", "dn": "1", "team": "Red Bull"}]}
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
    driver = Driver(
        session,
        "VER",
        prefetched_lap_data=_build_laptime_payload(best_lap=11, best_time=88.123),
    )

    monkeypatch.setattr(session, "_resolve_ultra_cold_mode", lambda _value: True)
    monkeypatch.setattr(
        session,
        "_fetch_json_unvalidated",
        lambda path: _build_tel_payload(11) if path == "VER/11_tel.json" else {},
    )
    monkeypatch.setattr(
        session,
        "get_fastest_laps_tels",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected fallback path")),
    )

    tel = driver.get_fastest_lap_tel()

    assert not tel.empty
    assert tel["Driver"].iloc[0] == "VER"
    assert int(tel["LapNumber"].iloc[0]) == 11


def test_lap_telemetry_reuses_session_memoized_payload(monkeypatch):
    """Lap telemetry should reuse session memoized payload before fetching from network/cache."""
    session = Session(2025, "Test GP", "Race", enable_cache=True, lib="pandas")
    session._remember_telemetry_payload("VER", 19, _build_tel_payload(19)["tel"])
    lap = Lap({"Driver": "VER", "LapNumber": 19}, session=session)

    monkeypatch.setattr(
        lap,
        "_fetch_telemetry",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected telemetry fetch")),
    )

    telemetry = lap.telemetry

    assert not telemetry.empty
    assert "Speed" in telemetry.columns
    assert "Time" in telemetry.columns
