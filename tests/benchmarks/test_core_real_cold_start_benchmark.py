"""Real-world cold-start benchmarks for core session workflows."""

from __future__ import annotations

import cProfile
import os
import pstats
import shutil
import statistics
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

import tif1
import tif1.cache as cache_module
import tif1.core as core_module
from tif1.async_fetch import close_executor
from tif1.async_fetch import close_session as close_async_session
from tif1.core import clear_lap_cache
from tif1.http_session import close_session as close_http_session

_YEAR = 2025
_EVENT = "Abu Dhabi Grand Prix"
_SESSION = "Race"

_BASELINE_NO_CACHE_CONFIG = {
    "validate_data": True,
    "validate_lap_times": True,
    "validate_telemetry": True,
    "ultra_cold_start": False,
    "ultra_cold_background_cache_fill": False,
    "ultra_cold_skip_retries": False,
}

_FASTEST_NO_CACHE_CONFIG = {
    "validate_data": False,
    "validate_lap_times": False,
    "validate_telemetry": False,
    "ultra_cold_start": True,
    "ultra_cold_background_cache_fill": False,
    "ultra_cold_skip_retries": True,
    "prefetch_driver_laps_on_get_driver": True,
}


def _benchmark_rounds() -> int:
    raw = os.getenv("TIF1_REAL_BENCHMARK_ROUNDS", "2")
    try:
        return max(1, int(raw))
    except ValueError:
        return 2


def _profile_rounds() -> int:
    raw = os.getenv("TIF1_REAL_PROFILE_ROUNDS", "1")
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


def _reset_cold_state(cache_dir: Path) -> None:
    """Force a true cold start by resetting all process and disk caches."""
    close_async_session()
    close_executor()
    close_http_session()
    clear_lap_cache()
    cache_module._cleanup_cache()
    shutil.rmtree(cache_dir, ignore_errors=True)


@contextmanager
def _temporary_config(overrides: dict[str, Any]) -> Iterator[None]:
    previous_values = {key: core_module.config.get(key) for key in overrides}
    try:
        for key, value in overrides.items():
            core_module.config.set(key, value)
        yield
    finally:
        for key, value in previous_values.items():
            core_module.config.set(key, value)


def _summarize_profile(profile: cProfile.Profile, top_n: int = 8) -> list[str]:
    stats = pstats.Stats(profile)
    tif1_rows: list[tuple[float, float, int, str, int, str]] = []

    for stat_key, stat_values in stats.stats.items():
        filename, line_no, func_name = stat_key
        normalized_filename = filename.replace("\\", "/")
        if "/src/tif1/" not in normalized_filename:
            continue

        _, total_calls, total_time, cumulative_time, _ = stat_values
        tif1_rows.append((cumulative_time, total_time, total_calls, filename, line_no, func_name))

    if not tif1_rows:
        return ["<no tif1 frames captured>"]

    tif1_rows.sort(key=lambda item: item[0], reverse=True)
    return [
        (
            f"{Path(filename).name}:{line_no}:{func_name} "
            f"cum={cumulative_time:.4f}s self={total_time:.4f}s calls={total_calls}"
        )
        for cumulative_time, total_time, total_calls, filename, line_no, func_name in tif1_rows[
            :top_n
        ]
    ]


def _run_user_sequence(
    *,
    enable_cache: bool,
    profile_each_step: bool = False,
) -> tuple[dict[str, float], dict[str, int], dict[str, list[str]]]:
    timings: dict[str, float] = {}
    profile_summaries: dict[str, list[str]] = {}

    def timed(label: str, fn: Callable[[], Any]) -> Any:
        profiler = cProfile.Profile() if profile_each_step else None
        start = time.perf_counter()
        if profiler is not None:
            profiler.enable()
        try:
            value = fn()
        finally:
            if profiler is not None:
                profiler.disable()

        elapsed = time.perf_counter() - start
        timings[label] = elapsed
        if profiler is not None:
            profile_summaries[label] = _summarize_profile(profiler)
        return value

    session = timed(
        "session = tif1.get_session(...)",
        lambda: tif1.get_session(_YEAR, _EVENT, _SESSION, enable_cache=enable_cache, lib="pandas"),
    )
    drivers = timed("drivers = session.drivers", lambda: session.drivers)
    laps = timed("laps = session.laps", lambda: session.laps)
    ver = timed('ver = session.get_driver("VER")', lambda: session.get_driver("VER"))
    ver_laps = timed("ver_laps = ver.laps", lambda: ver.laps)
    _ = timed("ver.get_fastest_lap()", lambda: ver.get_fastest_lap())
    fastest_tel = timed(
        "fastest_tel = session.get_fastest_lap_tel()",
        lambda: session.get_fastest_lap_tel(),
    )
    fastest_tels = timed(
        "fastest_tels = session.get_fastest_laps_tels(by_driver=True)",
        lambda: session.get_fastest_laps_tels(by_driver=True),
    )
    ver_fastest_tel = timed(
        "ver_fastest_tel = ver.get_fastest_lap_tel()", lambda: ver.get_fastest_lap_tel()
    )
    fastest_laps = timed(
        "session.get_fastest_laps(by_driver=True)",
        lambda: session.get_fastest_laps(by_driver=True),
    )
    lap = timed("lap = ver.get_lap(12)", lambda: ver.get_lap(12))
    telemetry = timed("telemetry = lap.telemetry", lambda: lap.telemetry)

    row_counts = {
        "drivers": len(drivers),
        "laps": len(laps),
        "ver_laps": len(ver_laps),
        "fastest_tel": len(fastest_tel),
        "fastest_tels": len(fastest_tels),
        "ver_fastest_tel": len(ver_fastest_tel),
        "fastest_laps": len(fastest_laps),
        "telemetry": len(telemetry),
    }
    return timings, row_counts, profile_summaries


def _assert_sequence_rows(rows: dict[str, int]) -> None:
    assert rows["drivers"] > 0
    assert rows["laps"] > 0
    assert rows["ver_laps"] > 0
    assert rows["fastest_tel"] > 0
    assert rows["fastest_tels"] > 0
    assert rows["ver_fastest_tel"] > 0
    assert rows["fastest_laps"] > 0
    assert rows["telemetry"] > 0


def _print_timing_summary(title: str, timings: list[dict[str, float]]) -> None:
    print(title)
    if not timings:
        return

    aggregates: list[tuple[str, float, float, float]] = []
    for label in timings[0]:
        samples = [timing[label] for timing in timings]
        avg = statistics.mean(samples)
        aggregates.append((label, avg, min(samples), max(samples)))

    for label, avg, minimum, maximum in aggregates:
        print(f"{label}: avg={avg:.4f}s min={minimum:.4f}s max={maximum:.4f}s")

    print("Top bottlenecks by average step time:")
    for label, avg, _, _ in sorted(aggregates, key=lambda item: item[1], reverse=True)[:5]:
        print(f"  {label}: avg={avg:.4f}s")


def _run_single_operation_benchmark(
    operation: Callable[[], Any],
    validator: Callable[[Any], Any],
    cache_dir: Path,
    rounds: int,
) -> tuple[list[float], list[str]]:
    samples: list[float] = []
    profile_summary: list[str] = []
    for _ in range(rounds):
        _reset_cold_state(cache_dir)
        profiler = cProfile.Profile()
        start = time.perf_counter()
        profiler.enable()
        try:
            result = operation()
        finally:
            profiler.disable()
        samples.append(time.perf_counter() - start)
        validation_result = validator(result)
        assert validation_result is not False
        if not profile_summary:
            profile_summary = _summarize_profile(profiler)
    return samples, profile_summary


def _build_operations(
    enable_cache: bool,
) -> list[tuple[str, Callable[[], Any], Callable[[Any], Any]]]:
    def session_factory():
        return tif1.get_session(
            _YEAR,
            _EVENT,
            _SESSION,
            enable_cache=enable_cache,
            lib="pandas",
        )

    return [
        (
            "session.drivers",
            lambda: session_factory().drivers,
            lambda value: value and len(value) > 0,
        ),
        (
            "session.laps",
            lambda: session_factory().laps,
            lambda value: len(value) > 0,
        ),
        (
            'session.get_driver("VER")',
            lambda: session_factory().get_driver("VER"),
            lambda value: value.driver == "VER",
        ),
        (
            "ver.laps",
            lambda: session_factory().get_driver("VER").laps,
            lambda value: len(value) > 0,
        ),
        (
            "ver.get_fastest_lap()",
            lambda: session_factory().get_driver("VER").get_fastest_lap(),
            lambda value: len(value) > 0,
        ),
        (
            "session.get_fastest_lap_tel()",
            lambda: session_factory().get_fastest_lap_tel(),
            lambda value: len(value) > 0,
        ),
        (
            "session.get_fastest_laps_tels(by_driver=True)",
            lambda: session_factory().get_fastest_laps_tels(by_driver=True),
            lambda value: len(value) > 0,
        ),
        (
            "ver.get_fastest_lap_tel()",
            lambda: session_factory().get_driver("VER").get_fastest_lap_tel(),
            lambda value: len(value) > 0,
        ),
        (
            "session.get_fastest_laps(by_driver=True)",
            lambda: session_factory().get_fastest_laps(by_driver=True),
            lambda value: len(value) > 0,
        ),
        (
            "lap.telemetry",
            lambda: session_factory().get_driver("VER").get_lap(12).telemetry,
            lambda value: len(value) > 0,
        ),
    ]


@pytest.mark.integration
@pytest.mark.benchmark
def test_real_world_cold_sequence_abu_dhabi_2025(tmp_path):
    """Benchmark the exact user workflow on real data from true no-cache cold starts."""
    cache_dir = tmp_path / "tif1-real-benchmark-sequence-cache"
    os.environ["TIF1_CACHE_DIR"] = str(cache_dir)
    rounds = _benchmark_rounds()

    all_timings: list[dict[str, float]] = []
    all_rows: list[dict[str, int]] = []
    for _ in range(rounds):
        _reset_cold_state(cache_dir)
        with _temporary_config(_BASELINE_NO_CACHE_CONFIG):
            timings, rows, _ = _run_user_sequence(enable_cache=False)
        all_timings.append(timings)
        all_rows.append(rows)

    for row in all_rows:
        _assert_sequence_rows(row)

    _print_timing_summary(
        f"Real cold-start sequence benchmark (no-cache baseline, {rounds} rounds)",
        all_timings,
    )


@pytest.mark.integration
@pytest.mark.benchmark
@pytest.mark.xfail(reason="Network-dependent test may return empty data for some operations")
def test_real_world_individual_cold_calls_profile_abu_dhabi_2025(tmp_path):
    """Profile each key call independently from no-cache cold starts."""
    cache_dir = tmp_path / "tif1-real-benchmark-calls-cache"
    os.environ["TIF1_CACHE_DIR"] = str(cache_dir)
    rounds = _profile_rounds()
    operations = _build_operations(enable_cache=False)

    print(f"Real cold-start individual-call profile (no-cache baseline, {rounds} rounds)")
    with _temporary_config(_BASELINE_NO_CACHE_CONFIG):
        for label, operation, validator in operations:
            samples, profile_summary = _run_single_operation_benchmark(
                operation,
                validator,
                cache_dir,
                rounds,
            )
            assert len(samples) == rounds
            print(
                f"{label}: avg={statistics.mean(samples):.4f}s "
                f"min={min(samples):.4f}s max={max(samples):.4f}s"
            )
            print("  Top tif1 cumulative frames:")
            for frame in profile_summary:
                print(f"    {frame}")


@pytest.mark.integration
@pytest.mark.benchmark
def test_real_world_cold_sequence_fastest_mode_no_cache_comparison_abu_dhabi_2025(tmp_path):
    """Compare baseline no-cache mode vs fastest no-cache mode for full workflow runtime."""
    cache_dir = tmp_path / "tif1-real-benchmark-mode-comparison-cache"
    os.environ["TIF1_CACHE_DIR"] = str(cache_dir)
    rounds = _benchmark_rounds()

    mode_configs = [
        ("baseline_no_cache", _BASELINE_NO_CACHE_CONFIG),
        ("fastest_no_cache", _FASTEST_NO_CACHE_CONFIG),
    ]
    mode_totals: dict[str, list[float]] = {}

    for mode_name, mode_config in mode_configs:
        all_timings: list[dict[str, float]] = []
        totals: list[float] = []
        for _ in range(rounds):
            _reset_cold_state(cache_dir)
            with _temporary_config(mode_config):
                timings, rows, _ = _run_user_sequence(enable_cache=False)
            _assert_sequence_rows(rows)
            all_timings.append(timings)
            totals.append(sum(timings.values()))

        mode_totals[mode_name] = totals
        print(
            f"{mode_name}: avg_total={statistics.mean(totals):.4f}s "
            f"min_total={min(totals):.4f}s max_total={max(totals):.4f}s"
        )
        _print_timing_summary(
            f"{mode_name} step timings ({rounds} rounds)",
            all_timings,
        )

    baseline_avg = statistics.mean(mode_totals["baseline_no_cache"])
    fastest_avg = statistics.mean(mode_totals["fastest_no_cache"])
    delta = baseline_avg - fastest_avg
    percent = (delta / baseline_avg * 100.0) if baseline_avg > 0 else 0.0
    print(f"fastest_no_cache vs baseline_no_cache: delta={delta:.4f}s ({percent:+.2f}%)")

    assert baseline_avg > 0
    assert fastest_avg > 0
