"""Baseline benchmark extracted from tif1_fastest_no_cache_v2_pandas.ipynb.

Runs both async and sync workflows with detailed step-level timing
and cProfile profiling, saving results to scripts/baseline_results/.
"""

import asyncio
import cProfile
import json
import pstats
import statistics
import time
from io import StringIO
from pathlib import Path

import nest_asyncio2

nest_asyncio2.apply()

import tif1

# ── Configuration ──────────────────────────────────────────────────────────────
YEAR = 2025
EVENT = "Mexico City Grand Prix"
SESSION_NAME = "Race"
BACKEND = "pandas"
ENABLE_CACHE = False
MAX_CONCURRENT = 32
MAX_WORKERS = 32
DRIVER_CODE = "VER"
LAP_NUMBER = 12
NUM_RUNS = 1

RESULTS_DIR = Path(__file__).parent / "baseline_results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# ── Fast config ────────────────────────────────────────────────────────────────
def apply_fast_config():
    cfg = tif1.get_config()
    fast = {
        "validate_data": False,
        "validate_lap_times": False,
        "validate_telemetry": False,
        "ultra_cold_start": True,
        "ultra_cold_skip_retries": True,
        "prefetch_driver_laps_on_get_driver": True,
        "max_concurrent_requests": MAX_CONCURRENT,
        "max_workers": MAX_WORKERS,
    }
    for key, value in fast.items():
        cfg.set(key, value)
    print("Applied fast config:")
    for k in fast:
        print(f"  {k}={cfg.get(k)}")
    print()


# ── Helpers ────────────────────────────────────────────────────────────────────
def timed_sync(label, fn, bucket):
    start = time.perf_counter()
    value = fn()
    bucket[label] = time.perf_counter() - start
    return value


async def timed_async(label, coro_fn, bucket):
    start = time.perf_counter()
    value = await coro_fn()
    bucket[label] = time.perf_counter() - start
    return value


def is_polars_df(df):
    return df.__class__.__module__.startswith("polars")


def first_row_dict(df):
    if len(df) == 0:
        raise ValueError("DataFrame is empty")
    if is_polars_df(df):
        return dict(df.row(0, named=True))
    return df.iloc[0].to_dict()


def extract_lap_number(row):
    for key in ("LapNumber", "lap", "Lap", "lap_number"):
        value = row.get(key)
        if value is not None:
            return int(value)
    raise KeyError("Could not find lap number column in row")


def lap_ref_from_fastest_df(df, driver_default=None):
    row = first_row_dict(df)
    driver_value = row.get("Driver", driver_default)
    if driver_value is None:
        raise KeyError("Could not find driver column in row")
    return str(driver_value), extract_lap_number(row)


def filter_telemetry_by_ref(tel_df, driver, lap_number):
    if is_polars_df(tel_df):
        return tel_df.filter((tel_df["Driver"] == driver) & (tel_df["LapNumber"] == lap_number))
    filtered = tel_df[(tel_df["Driver"] == driver) & (tel_df["LapNumber"] == lap_number)]
    return filtered.reset_index(drop=True)


def unique_values(df, column):
    if is_polars_df(df):
        return set(df[column].to_list())
    return set(df[column].tolist())


def print_timing_table(run_name, step_timings):
    total = sum(step_timings.values())
    print(f"\n{run_name} total elapsed: {total:.4f}s")
    for label, elapsed in sorted(step_timings.items(), key=lambda item: item[1], reverse=True):
        print(f"  {elapsed:8.4f}s  {label}")
    return total


# ── Async workflow ─────────────────────────────────────────────────────────────
async def run_async_workflow():
    timings = {}

    session = timed_sync(
        "session = tif1.get_session(...)",
        lambda: tif1.get_session(YEAR, EVENT, SESSION_NAME, enable_cache=ENABLE_CACHE, lib=BACKEND),
        timings,
    )
    drivers = timed_sync("drivers = session.drivers", lambda: session.drivers, timings)
    fastest_tels = await timed_async(
        "fastest_tels = await session.get_fastest_laps_tels_async(by_driver=True)",
        lambda: session.get_fastest_laps_tels_async(by_driver=True),
        timings,
    )
    session_fastest_laps = await timed_async(
        "session_fastest_laps = await session.get_fastest_laps_async(by_driver=True)",
        lambda: session.get_fastest_laps_async(by_driver=True),
        timings,
    )
    ver = timed_sync(
        f'ver = session.get_driver("{DRIVER_CODE}")',
        lambda: session.get_driver(DRIVER_CODE),
        timings,
    )
    ver_fastest_lap = timed_sync(
        "ver_fastest_lap = ver.get_fastest_lap()", lambda: ver.get_fastest_lap(), timings
    )
    lap = timed_sync(f"lap = ver.get_lap({LAP_NUMBER})", lambda: ver.get_lap(LAP_NUMBER), timings)
    telemetry = timed_sync("telemetry = lap.telemetry", lambda: lap.telemetry, timings)

    overall_fastest_ref = timed_sync(
        "overall_fastest_ref",
        lambda: lap_ref_from_fastest_df(session_fastest_laps),
        timings,
    )
    ver_fastest_ref = timed_sync(
        "ver_fastest_ref",
        lambda: lap_ref_from_fastest_df(ver_fastest_lap, driver_default=DRIVER_CODE),
        timings,
    )
    fastest_tel = timed_sync(
        "fastest_tel = filter by overall ref",
        lambda: filter_telemetry_by_ref(fastest_tels, *overall_fastest_ref),
        timings,
    )
    ver_fastest_tel = timed_sync(
        "ver_fastest_tel = filter by driver ref",
        lambda: filter_telemetry_by_ref(fastest_tels, *ver_fastest_ref),
        timings,
    )

    artifacts = {
        "drivers": drivers,
        "fastest_tels": fastest_tels,
        "session_fastest_laps": session_fastest_laps,
        "ver_fastest_lap": ver_fastest_lap,
        "fastest_tel": fastest_tel,
        "ver_fastest_tel": ver_fastest_tel,
        "telemetry": telemetry,
        "overall_fastest_ref": overall_fastest_ref,
        "ver_fastest_ref": ver_fastest_ref,
    }
    return timings, artifacts


# ── Sync workflow ──────────────────────────────────────────────────────────────
def run_sync_workflow():
    timings = {}

    session = timed_sync(
        "session = tif1.get_session(...)",
        lambda: tif1.get_session(YEAR, EVENT, SESSION_NAME, enable_cache=ENABLE_CACHE, lib=BACKEND),
        timings,
    )
    drivers = timed_sync("drivers = session.drivers", lambda: session.drivers, timings)
    fastest_tels = timed_sync(
        "fastest_tels = session.get_fastest_laps_tels(by_driver=True)",
        lambda: session.get_fastest_laps_tels(by_driver=True),
        timings,
    )
    session_fastest_laps = timed_sync(
        "session_fastest_laps = session.get_fastest_laps(by_driver=True)",
        lambda: session.get_fastest_laps(by_driver=True),
        timings,
    )
    ver = timed_sync(
        f'ver = session.get_driver("{DRIVER_CODE}")',
        lambda: session.get_driver(DRIVER_CODE),
        timings,
    )
    ver_fastest_lap = timed_sync(
        "ver_fastest_lap = ver.get_fastest_lap()", lambda: ver.get_fastest_lap(), timings
    )
    lap = timed_sync(f"lap = ver.get_lap({LAP_NUMBER})", lambda: ver.get_lap(LAP_NUMBER), timings)
    telemetry = timed_sync("telemetry = lap.telemetry", lambda: lap.telemetry, timings)

    overall_fastest_ref = timed_sync(
        "overall_fastest_ref",
        lambda: lap_ref_from_fastest_df(session_fastest_laps),
        timings,
    )
    ver_fastest_ref = timed_sync(
        "ver_fastest_ref",
        lambda: lap_ref_from_fastest_df(ver_fastest_lap, driver_default=DRIVER_CODE),
        timings,
    )
    fastest_tel = timed_sync(
        "fastest_tel = filter by overall ref",
        lambda: filter_telemetry_by_ref(fastest_tels, *overall_fastest_ref),
        timings,
    )
    ver_fastest_tel = timed_sync(
        "ver_fastest_tel = filter by driver ref",
        lambda: filter_telemetry_by_ref(fastest_tels, *ver_fastest_ref),
        timings,
    )

    artifacts = {
        "drivers": drivers,
        "fastest_tels": fastest_tels,
        "session_fastest_laps": session_fastest_laps,
        "ver_fastest_lap": ver_fastest_lap,
        "fastest_tel": fastest_tel,
        "ver_fastest_tel": ver_fastest_tel,
        "telemetry": telemetry,
        "overall_fastest_ref": overall_fastest_ref,
        "ver_fastest_ref": ver_fastest_ref,
    }
    return timings, artifacts


# ── Assertions ─────────────────────────────────────────────────────────────────
def assert_run(run_name, artifacts):
    required = (
        "drivers",
        "fastest_tels",
        "session_fastest_laps",
        "ver_fastest_lap",
        "fastest_tel",
        "ver_fastest_tel",
        "telemetry",
        "overall_fastest_ref",
        "ver_fastest_ref",
    )
    for key in required:
        assert key in artifacts, f"{run_name}: missing artifact {key}"
        if key.endswith("_ref"):
            continue
        assert len(artifacts[key]) > 0, f"{run_name}: {key} is empty"

    overall_driver, overall_lap = artifacts["overall_fastest_ref"]
    ver_driver, ver_lap = artifacts["ver_fastest_ref"]
    assert ver_driver == DRIVER_CODE, f"{run_name}: expected {DRIVER_CODE}, got {ver_driver}"

    overall_tel = artifacts["fastest_tel"]
    ver_tel = artifacts["ver_fastest_tel"]
    assert unique_values(overall_tel, "Driver") == {overall_driver}
    assert {int(v) for v in unique_values(overall_tel, "LapNumber")} == {overall_lap}
    assert unique_values(ver_tel, "Driver") == {ver_driver}
    assert {int(v) for v in unique_values(ver_tel, "LapNumber")} == {ver_lap}


# ── cProfile helper ───────────────────────────────────────────────────────────
def profile_callable(name, fn):
    """Run fn under cProfile; return (result, pstats_text, prof_path)."""
    prof_path = RESULTS_DIR / f"{name}.prof"
    profiler = cProfile.Profile()
    profiler.enable()
    result = fn()
    profiler.disable()
    profiler.dump_stats(str(prof_path))

    buf = StringIO()
    ps = pstats.Stats(profiler, stream=buf)
    ps.sort_stats("cumulative")
    ps.print_stats(60)
    return result, buf.getvalue(), prof_path


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    apply_fast_config()

    # Ensure event loop exists for nest_asyncio2
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    all_results = {"async": [], "sync": []}

    # ── Profiled single run (async) ────────────────────────────────────────────
    print("=" * 70)
    print("PROFILED ASYNC RUN (single, with cProfile)")
    print("=" * 70)
    (async_timings, async_artifacts), async_profile_text, async_prof_path = profile_callable(
        "async_profile",
        lambda: loop.run_until_complete(run_async_workflow()),
    )
    assert_run("Async profiled", async_artifacts)
    print_timing_table("Async profiled", async_timings)
    print(f"\ncProfile saved to: {async_prof_path}")
    print("\nTop cumulative functions:")
    print(async_profile_text)

    # ── Profiled single run (sync) ─────────────────────────────────────────────
    print("=" * 70)
    print("PROFILED SYNC RUN (single, with cProfile)")
    print("=" * 70)
    (sync_timings, sync_artifacts), sync_profile_text, sync_prof_path = profile_callable(
        "sync_profile",
        run_sync_workflow,
    )
    assert_run("Sync profiled", sync_artifacts)
    print_timing_table("Sync profiled", sync_timings)
    print(f"\ncProfile saved to: {sync_prof_path}")
    print("\nTop cumulative functions:")
    print(sync_profile_text)

    # ── Repeated runs for statistics ───────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"REPEATED RUNS ({NUM_RUNS}x each, no cProfile overhead)")
    print("=" * 70)

    for i in range(NUM_RUNS):
        print(f"\n── Run {i + 1}/{NUM_RUNS} ──")

        at, aa = loop.run_until_complete(run_async_workflow())
        assert_run(f"Async run {i + 1}", aa)
        a_total = sum(at.values())
        all_results["async"].append({"run": i + 1, "total": a_total, "steps": dict(at)})
        print(f"  Async: {a_total:.4f}s")

        st, sa = run_sync_workflow()
        assert_run(f"Sync run {i + 1}", sa)
        s_total = sum(st.values())
        all_results["sync"].append({"run": i + 1, "total": s_total, "steps": dict(st)})
        print(f"  Sync:  {s_total:.4f}s")

    # ── Summary statistics ─────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    summary = {}
    for mode in ("async", "sync"):
        totals = [r["total"] for r in all_results[mode]]
        s = {
            "mean": statistics.mean(totals),
            "median": statistics.median(totals),
            "stdev": statistics.stdev(totals) if len(totals) > 1 else 0.0,
            "min": min(totals),
            "max": max(totals),
            "runs": totals,
        }
        summary[mode] = s
        print(f"\n{mode.upper()} ({NUM_RUNS} runs):")
        print(
            f"  mean={s['mean']:.4f}s  median={s['median']:.4f}s  "
            f"stdev={s['stdev']:.4f}s  min={s['min']:.4f}s  max={s['max']:.4f}s"
        )

    # Aggregate per-step averages
    for mode in ("async", "sync"):
        step_totals: dict[str, list[float]] = {}
        for r in all_results[mode]:
            for step, elapsed in r["steps"].items():
                step_totals.setdefault(step, []).append(elapsed)
        step_avgs = {step: statistics.mean(vals) for step, vals in step_totals.items()}
        summary[f"{mode}_step_averages"] = step_avgs
        print(f"\n{mode.upper()} per-step averages:")
        for step, avg in sorted(step_avgs.items(), key=lambda x: x[1], reverse=True):
            print(f"  {avg:8.4f}s  {step}")

    if summary["async"]["mean"] and summary["sync"]["mean"]:
        faster = "async" if summary["async"]["mean"] < summary["sync"]["mean"] else "sync"
        slower = "sync" if faster == "async" else "async"
        speedup = summary[slower]["mean"] / summary[faster]["mean"]
        print(f"\n{faster.upper()} is {speedup:.2f}x faster than {slower.upper()} on average.")

    # ── Save results ───────────────────────────────────────────────────────────
    results_path = RESULTS_DIR / "baseline_results.json"
    with open(results_path, "w") as f:
        json.dump(
            {
                "config": {
                    "year": YEAR,
                    "event": EVENT,
                    "session": SESSION_NAME,
                    "backend": BACKEND,
                    "cache": ENABLE_CACHE,
                    "max_concurrent": MAX_CONCURRENT,
                    "max_workers": MAX_WORKERS,
                    "num_runs": NUM_RUNS,
                },
                "summary": summary,
                "raw_runs": all_results,
            },
            f,
            indent=2,
        )

    with open(RESULTS_DIR / "async_profile.txt", "w") as f:
        f.write(async_profile_text)
    with open(RESULTS_DIR / "sync_profile.txt", "w") as f:
        f.write(sync_profile_text)

    print(f"\nResults saved to: {results_path}")
    print(f"Profiles saved to: {RESULTS_DIR}/{{async,sync}}_profile.txt")
    print(f"Binary profiles: {RESULTS_DIR}/{{async,sync}}_profile.prof")
    print("\nAll assertions passed. Baseline complete.")


if __name__ == "__main__":
    main()
