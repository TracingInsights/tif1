"""Benchmarks for Driver.get_lap lookup performance."""

import pandas as pd
import pytest

from tif1.core import Driver, Lap, Session
from tif1.exceptions import LapNotFoundError


def _build_driver_with_laps(size: int = 10_000) -> Driver:
    session = Session(2025, "Test GP", "Race", enable_cache=False, lib="pandas")
    session._drivers = [{"driver": "VER", "team": "Red Bull Racing", "number": 1}]
    session._refresh_driver_indices()
    driver = Driver(session, "VER")
    driver._laps = pd.DataFrame(
        {
            "LapNumber": list(range(1, size + 1)),
            "LapTime": [90.0 + ((i * 17) % 700) / 1000 for i in range(size)],
        }
    )
    return driver


def _build_lookup_numbers(count: int = 2_000, max_lap: int = 10_000) -> list[int]:
    return [((i * 37) % max_lap) + 1 for i in range(count)]


def _legacy_get_lap(driver: Driver, lap_number: int) -> Lap:
    laps = driver._laps
    if laps is not None and not laps.empty:
        lap_col = "LapNumber" if "LapNumber" in laps.columns else "lap"
        lap_exists = lap_number in laps[lap_col].values if lap_col in laps.columns else False
        if not lap_exists:
            raise LapNotFoundError(
                lap_number=lap_number,
                driver=driver.driver,
                year=driver.session.year,
                event=driver.session.gp,
                session=driver.session.session,
            )
        # Extract the lap row from the DataFrame
        lap_row = laps[laps[lap_col] == lap_number].iloc[0]
        return Lap(lap_row, session=driver.session)
    # Return empty Lap if no laps data
    return Lap({}, session=driver.session)


def _run_legacy_get_lap(driver: Driver, lookup_numbers: list[int]) -> int:
    found = 0
    for lap_number in lookup_numbers:
        try:
            _legacy_get_lap(driver, lap_number)
            found += 1
        except LapNotFoundError:
            continue
    return found


def _run_production_get_lap(driver: Driver, lookup_numbers: list[int], cold: bool) -> int:
    if cold:
        driver._lap_numbers = None
        driver._lap_numbers_df_id = None

    found = 0
    for lap_number in lookup_numbers:
        try:
            driver.get_lap(lap_number)
            found += 1
        except LapNotFoundError:
            continue
    return found


def test_driver_get_lap_parity():
    lookup_numbers = _build_lookup_numbers()
    lookup_numbers.extend([10_001, 11_000])

    legacy_driver = _build_driver_with_laps()
    optimized_driver = _build_driver_with_laps()

    legacy_hits = _run_legacy_get_lap(legacy_driver, lookup_numbers)
    optimized_hits = _run_production_get_lap(optimized_driver, lookup_numbers, cold=True)

    assert optimized_hits == legacy_hits


@pytest.mark.benchmark(group="core_driver_get_lap")
class TestCoreDriverGetLapBenchmark:
    def test_legacy_repeated_lookup(self, benchmark):
        driver = _build_driver_with_laps()
        lookup_numbers = _build_lookup_numbers()

        found = benchmark(_run_legacy_get_lap, driver, lookup_numbers)
        assert found == len(lookup_numbers)

    def test_optimized_lookup_cold(self, benchmark):
        driver = _build_driver_with_laps()
        lookup_numbers = _build_lookup_numbers()

        found = benchmark(_run_production_get_lap, driver, lookup_numbers, True)
        assert found == len(lookup_numbers)

    def test_optimized_lookup_warm(self, benchmark):
        driver = _build_driver_with_laps()
        lookup_numbers = _build_lookup_numbers()

        driver.get_lap(1)
        found = benchmark(_run_production_get_lap, driver, lookup_numbers, False)
        assert found == len(lookup_numbers)
