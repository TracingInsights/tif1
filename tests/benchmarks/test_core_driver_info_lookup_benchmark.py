"""Benchmarks for Session driver info lookup performance."""

import pytest

from tif1.core import Session


def _build_session_with_drivers(count: int = 500) -> Session:
    session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
    session._drivers = [
        {"driver": f"D{i:03d}", "team": f"Team {i % 10}", "number": i + 1} for i in range(count)
    ]
    session._refresh_driver_indices()
    return session


def _build_lookup_codes(count: int = 5_000, max_driver: int = 500) -> list[str]:
    return [f"D{((i * 13) % max_driver):03d}" for i in range(count)]


def _legacy_get_driver_info(session: Session, driver_code: str) -> dict:
    return next(
        (
            driver_info
            for driver_info in session._drivers
            if driver_info.get("driver") == driver_code
        ),
        {"driver": driver_code, "team": ""},
    )


def _run_legacy_lookup(session: Session, lookup_codes: list[str]) -> int:
    checksum = 0
    for driver_code in lookup_codes:
        checksum += len(_legacy_get_driver_info(session, driver_code).get("team", ""))
    return checksum


def _run_production_lookup(session: Session, lookup_codes: list[str], cold: bool) -> int:
    if cold:
        session._driver_codes = None
        session._driver_info_by_code = None
        session._driver_index_source_id = None

    checksum = 0
    for driver_code in lookup_codes:
        checksum += len(session._get_driver_info(driver_code).get("team", ""))
    return checksum


def test_driver_info_lookup_parity():
    lookup_codes = _build_lookup_codes()
    lookup_codes.extend(["D999", "D888"])

    legacy_session = _build_session_with_drivers()
    optimized_session = _build_session_with_drivers()

    legacy_checksum = _run_legacy_lookup(legacy_session, lookup_codes)
    optimized_checksum = _run_production_lookup(optimized_session, lookup_codes, cold=True)

    assert optimized_checksum == legacy_checksum


@pytest.mark.benchmark(group="core_driver_info_lookup")
class TestCoreDriverInfoLookupBenchmark:
    def test_legacy_linear_lookup(self, benchmark):
        session = _build_session_with_drivers()
        lookup_codes = _build_lookup_codes()

        checksum = benchmark(_run_legacy_lookup, session, lookup_codes)
        assert checksum > 0

    def test_optimized_lookup_cold(self, benchmark):
        session = _build_session_with_drivers()
        lookup_codes = _build_lookup_codes()

        checksum = benchmark(_run_production_lookup, session, lookup_codes, True)
        assert checksum > 0

    def test_optimized_lookup_warm(self, benchmark):
        session = _build_session_with_drivers()
        lookup_codes = _build_lookup_codes()

        session._get_driver_info(lookup_codes[0])
        checksum = benchmark(_run_production_lookup, session, lookup_codes, False)
        assert checksum > 0
