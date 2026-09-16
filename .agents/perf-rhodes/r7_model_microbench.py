"""R7 micro: model-layer per-access costs on the warm primed cache.

Flows measured (fresh process, warm session):
- session.laps.pick_driver("ALB") then laps.telemetry (merged path: iterrows +
  per-lap payload SQL reads)
- same flow after fetch_all_laps_telemetry() (memoized payloads)
- lap.telemetry via iterlaps for a single lap
- LazyTelemetryDict access
"""

from __future__ import annotations

import json
import os
import time

os.environ["TIF1_CACHE_DIR"] = "/tmp/tif1-warm-cache"

import tif1  # noqa: E402


def main() -> None:
    out: dict = {}
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    laps = session.laps

    t0 = time.perf_counter()
    ver = laps.pick_driver("ALB")
    out["pick_driver_ms"] = round((time.perf_counter() - t0) * 1000, 2)

    # THE FastF1 idiom: driver laps -> .telemetry (payload tier, per-lap SQL)
    t0 = time.perf_counter()
    tel = ver.telemetry
    out["ver_laps_telemetry_cold_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    out["ver_laps_rows"] = int(len(ver))
    out["ver_telemetry_rows"] = int(len(tel))

    # repeat (payloads now memoized in parsed tiers)
    ver2 = laps.pick_driver("ALB")
    t0 = time.perf_counter()
    tel2 = ver2.telemetry
    out["ver_laps_telemetry_repeat_ms"] = round((time.perf_counter() - t0) * 1000, 2)

    # single-lap access
    t0 = time.perf_counter()
    lap1 = ver.pick_lap(1)
    out["pick_lap_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    t0 = time.perf_counter()
    t3 = lap1.telemetry
    out["lap_telemetry_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    out["lap_telemetry_rows"] = int(len(t3))

    # LazyTelemetryDict
    t0 = time.perf_counter()
    _ = session.laps  # ensure loaded
    lazy = tif1.models.LazyTelemetryDict(session)
    v = lazy["ALB"]
    out["lazy_dict_access_ms"] = round((time.perf_counter() - t0) * 1000, 2)

    # pick_fastest
    t0 = time.perf_counter()
    f = laps.pick_fastest()
    out["pick_fastest_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    t0 = time.perf_counter()
    ft = f.telemetry
    out["fastest_lap_telemetry_ms"] = round((time.perf_counter() - t0) * 1000, 2)

    # full batch flow for comparison (the fetch_all path, frames not yet memoized)
    t0 = time.perf_counter()
    all_tel = session.fetch_all_laps_telemetry()
    out["fetch_all_ms"] = round((time.perf_counter() - t0) * 1000, 2)
    out["fetch_all_frames"] = len(all_tel)

    # now ver.telemetry again — payloads memoized by fetch_all
    ver3 = laps.pick_driver("ALB")
    t0 = time.perf_counter()
    tel3 = ver3.telemetry
    out["ver_laps_telemetry_after_fetchall_ms"] = round((time.perf_counter() - t0) * 1000, 2)

    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
