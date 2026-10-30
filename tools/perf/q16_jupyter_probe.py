"""Q16 probe: warm tif1 sync APIs called from inside a running event loop (Jupyter cell).

A Jupyter cell runs while the kernel's asyncio loop is running on the main
thread; ``asyncio.run(main())`` with ``main`` calling the sync APIs directly
reproduces that. Per call this records wall time, ``asyncio.run`` calls, event
loops created, whether ``nest_asyncio2`` got applied and threads started.

Usage (one fresh process per run):
    uv run python tools/perf/q16_jupyter_probe.py --cache-dir DIR [--src-dir SRC]
        [--scenario notebook|lapfirst|loopcost] [--lib pandas|polars] [--no-loop]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--scenario", default="notebook")
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--no-loop", action="store_true", help="plain script (no running loop)")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--force-nest", action="store_true", help="apply nest_asyncio2 at the end")
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, str(Path(args.src_dir).resolve()))
    os.environ["TIF1_CACHE_DIR"] = args.cache_dir
    if args.offline:
        os.environ["TIF1_OFFLINE_MODE"] = "true"

    import asyncio  # the kernel has asyncio imported and a loop running
    import threading

    counters = {"asyncio_run": 0, "loops": 0, "threads": 0, "nest_apply": 0}
    orig_run = asyncio.run

    def counting_run(*a, **k):
        counters["asyncio_run"] += 1
        return orig_run(*a, **k)

    asyncio.run = counting_run
    import asyncio.base_events as be

    orig_init = be.BaseEventLoop.__init__

    def counting_init(self, *a, **k):
        counters["loops"] += 1
        orig_init(self, *a, **k)

    be.BaseEventLoop.__init__ = counting_init
    orig_start = threading.Thread.start

    def counting_start(self, *a, **k):
        counters["threads"] += 1
        return orig_start(self, *a, **k)

    threading.Thread.start = counting_start

    import nest_asyncio2

    orig_apply = nest_asyncio2.apply

    def counting_apply(*a, **k):
        counters["nest_apply"] += 1
        t0 = time.perf_counter()
        r = orig_apply(*a, **k)
        counters["nest_apply_s"] = round(time.perf_counter() - t0, 6)
        return r

    nest_asyncio2.apply = counting_apply

    calls: list[dict] = []
    frames: dict = {}

    def step(name, fn):
        before = dict(counters)
        t0 = time.perf_counter()
        error = None
        try:
            value = fn()
        except Exception as exc:  # recorded, the cell goes on
            value, error = None, f"{type(exc).__name__}: {exc}"[:200]
        dt = time.perf_counter() - t0
        rec = {"call": name, "s": round(dt, 6)}
        if error:
            rec["error"] = error
        for key in ("asyncio_run", "loops", "threads", "nest_apply"):
            delta = counters[key] - before[key]
            if delta:
                rec[key] = delta
        calls.append(rec)
        return value

    def cell() -> None:
        import tif1

        session = step(
            "get_session", lambda: tif1.get_session(2026, "Monaco Grand Prix", "Race", lib=args.lib)
        )
        if args.scenario == "notebook":
            step("load", lambda: session.load())
            laps = step("laps", lambda: session.laps)
            tel = step("fetch_all_laps_telemetry", lambda: session.fetch_all_laps_telemetry())
            frames["n_tel"] = len(tel)
            if args.lib == "pandas":
                drv = step("pick_driver_telemetry", lambda: laps.pick_drivers("LEC").telemetry)
                frames["drv_rows"] = len(drv)
                lt = step("lap_get_telemetry", lambda: laps.pick_fastest().get_telemetry())
                frames["lap_rows"] = len(lt)
            step("weather", lambda: session.weather)
            step("race_control_messages", lambda: session.race_control_messages)
            step("results", lambda: session.results)
        elif args.scenario == "lapfirst":
            # Typical notebook: no bulk fetch, straight to one lap's telemetry.
            laps = step("laps", lambda: session.laps)
            lt = step("lap_get_telemetry", lambda: laps.pick_fastest().get_telemetry())
            frames["lap_rows"] = len(lt)
            drv = step("pick_driver_telemetry", lambda: laps.pick_drivers("LEC").telemetry)
            frames["drv_rows"] = len(drv)
            step("weather", lambda: session.weather)
            step("results", lambda: session.results)
        elif args.scenario == "loopcost":
            step("load", lambda: session.load())
        elif args.scenario == "coldtel":
            # Laps warm (frame tier), telemetry not cached: the cold path in a loop.
            laps = step("laps", lambda: session.laps)
            tel = step("fetch_all_laps_telemetry", lambda: session.fetch_all_laps_telemetry())
            frames["n_tel"] = len(tel) if tel is not None else None
            lt = step("lap_get_telemetry", lambda: laps.pick_fastest().get_telemetry())
            frames["lap_rows"] = len(lt) if lt is not None else None
        if args.force_nest:
            import tif1.core

            step("force_nest_apply", lambda: tif1.core._ensure_nested_loop_support("probe"))

    async def kernel() -> dict:
        cell()
        # Kernel-loop throughput after the cell (does a patched loop cost more?).
        # gc first: the cell's allocation history must not leak into the timing.
        import gc

        gc.collect()
        t0 = time.perf_counter()
        for _ in range(20000):
            await asyncio.sleep(0)
        sleep0 = time.perf_counter() - t0
        t0 = time.perf_counter()
        loop = asyncio.get_running_loop()
        for _ in range(200):
            fut = loop.create_future()
            loop.call_soon(fut.set_result, None)
            await fut
        tasks = [asyncio.ensure_future(asyncio.sleep(0)) for _ in range(5000)]
        await asyncio.gather(*tasks)
        tasks_s = time.perf_counter() - t0
        return {"sleep0_20k_s": round(sleep0, 5), "tasks_5k_s": round(tasks_s, 5)}

    t0 = time.perf_counter()
    if args.no_loop:
        cell()
        loopstats: dict = {}
    else:
        loopstats = orig_run(kernel())
    total = time.perf_counter() - t0
    core = sys.modules.get("tif1.core")
    print(
        json.dumps(
            {
                "total_s": round(total, 5),
                "calls": calls,
                "nest_applied": bool(getattr(core, "_NEST_ASYNCIO2_APPLIED", False)),
                "nest_apply_s": counters.get("nest_apply_s"),
                "counters": counters,
                "frames": frames,
                **loopstats,
            }
        )
    )


if __name__ == "__main__":
    main()
