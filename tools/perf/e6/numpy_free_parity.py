"""E6 parity: numpy-free polars reads vs the same reads with numpy loaded (fresh processes).

The child builds the frames (``block``: numpy unimportable from the start;
``numpy``: numpy imported first, so every numpy path runs) and writes them as
IPC files; the parent compares them frame by frame (values, dtypes, order).
Usage: TIF1_CACHE_DIR=... TIF1_OFFLINE_MODE=true python tools/perf/e6/numpy_free_parity.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

if len(sys.argv) > 2:
    mode, out = sys.argv[1], Path(sys.argv[2])
    if mode == "block":
        sys.modules["numpy"] = None  # type: ignore[assignment]
    else:
        import numpy as np  # noqa: F401

    import tif1

    race = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib="polars")
    frames = {"fastest_lap_tel": race.get_fastest_lap_tel()}  # before laps: payload clock
    laps = race.laps
    frames["fastest_laps"] = race.get_fastest_laps()
    frames["ver_tel"] = laps.pick_driver("VER").telemetry
    for driver in ("PIA", "HAM"):
        for _, lap in laps.pick_driver(driver).iterlaps():
            frames[f"{driver}_{lap['LapNumber']}"] = lap.get_car_data()
    quali = tif1.get_session(2025, "Bahrain Grand Prix", "Qualifying", lib="polars")
    for i, segment in enumerate(quali.laps.split_qualifying_sessions(), 1):
        frames[f"Q{i}"] = segment
    frames["quali_fastest_lap_tel"] = quali.get_fastest_lap_tel()
    for name, frame in frames.items():
        frame.write_ipc(out / f"{name}.arrow")
    print(len(frames), "numpy loaded:", sys.modules.get("numpy") is not None)
    raise SystemExit(0)

import polars as pl

env = dict(os.environ, TIF1_OFFLINE_MODE="true")
with tempfile.TemporaryDirectory() as tmp:
    dirs = {}
    for mode in ("block", "numpy"):
        dirs[mode] = Path(tmp) / mode
        dirs[mode].mkdir()
        done = subprocess.run(
            [sys.executable, __file__, mode, str(dirs[mode])],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        print(mode, done.stdout.strip())
    names = sorted(p.name for p in dirs["block"].iterdir())
    assert names == sorted(p.name for p in dirs["numpy"].iterdir())
    differ = [
        name
        for name in names
        if not pl.read_ipc(dirs["block"] / name).equals(pl.read_ipc(dirs["numpy"] / name))
        or pl.read_ipc(dirs["block"] / name).schema != pl.read_ipc(dirs["numpy"] / name).schema
    ]
    print(f"{len(names)} frames compared, {len(differ)} differ: {differ}")
