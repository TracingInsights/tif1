"""Naxos N3: how far the eager resampler is from the query plan, value by value (in ulps).

For every lap of six drivers in the 2025 Bahrain race and qualifying and the
rules 0.1 s and 1 s, resamples the lap's telemetry with the eager kernels and
twice with the query plan (``_resample_eager`` disabled), then counts the
float values that differ and their distance in ulps: eager vs plan, and plan
vs plan (the plan is not deterministic on every frame). Run from the repo
root with a warm cache (``TIF1_CACHE_DIR``, ``TIF1_OFFLINE_MODE=true``).
"""

from __future__ import annotations

import collections
import math

import polars as pl

import tif1
from tif1 import models_polars as mp

DRIVERS = ("VER", "LEC", "NOR", "PIA", "HAM", "RUS")


def _ulps(a: float, b: float) -> int:
    if a == b or (math.isnan(a) and math.isnan(b)):
        return 0
    return round(abs(a - b) / math.ulp(max(abs(a), abs(b))))


def main() -> None:
    eager_vs_plan: collections.Counter[int] = collections.Counter()
    plan_vs_plan: collections.Counter[int] = collections.Counter()
    values = frames = frames_diff = frames_plan_diff = 0
    for session_name in ("Race", "Qualifying"):
        session = tif1.get_session(2025, "Bahrain Grand Prix", session_name, lib="polars")
        for driver in DRIVERS:
            for _, lap in session.laps.pick_driver(driver).iterlaps():
                tel = lap.get_telemetry()
                if tel.height == 0:
                    continue
                for rule in ("0.1s", "1s"):
                    step = mp._rule_to_ns(rule)
                    eager = mp._resample_eager(tel, "Date", step)
                    if eager is None:
                        continue
                    saved = mp._resample_eager
                    mp._resample_eager = lambda *_args: None
                    try:
                        plans = [mp._resample_by_rule(tel, step) for _ in range(2)]
                    finally:
                        mp._resample_eager = saved
                    frames += 1
                    differs = plan_differs = False
                    for column, dtype in eager.schema.items():
                        if dtype != pl.Float64:
                            assert eager[column].equals(plans[0][column]), column
                            continue
                        e, p, q = (f[column].to_list() for f in (eager, *plans))
                        for x, y, z in zip(e, p, q, strict=True):
                            if x is None or y is None:
                                assert x is None
                                assert y is None
                                continue
                            values += 1
                            eager_vs_plan[_ulps(x, y)] += 1
                            plan_vs_plan[_ulps(y, z)] += 1
                            differs |= x != y
                            plan_differs |= y != z
                    frames_diff += differs
                    frames_plan_diff += plan_differs
    print(f"{frames} resampled frames, {values} float values")
    print(
        f"eager vs plan: {frames_diff} frames differ; ulps histogram {sorted(eager_vs_plan.items())}"
    )
    print(
        f"plan vs plan:  {frames_plan_diff} frames differ; ulps histogram "
        f"{sorted(plan_vs_plan.items())}"
    )


if __name__ == "__main__":
    main()
