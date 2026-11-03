"""How far pandas 3.1's total_seconds() moves Distance on real laps (G5 report)."""

import numpy as np
import pandas as pd

import tif1

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
session.load(laps=True, telemetry=False, weather=False, messages=False)
worst_seconds = 0
worst_distance = 0.0
worst_rel = 0.0
n = 0
for driver in ("VER", "NOR", "LEC", "HAM"):
    for _, lap in session.laps.pick_drivers(driver).iterlaps():
        tel = lap.get_car_data()
        time = tel["Time"].array
        old = time.asi8 / 1e9
        new = time.total_seconds()
        ok = ~np.isnan(new)
        worst_seconds = max(worst_seconds, int(np.count_nonzero(old[ok] != new[ok])))
        speed = np.nan_to_num(tel["Speed"].to_numpy(dtype=float)) / 3.6

        def dist(sec):
            sec = np.where(np.isnan(sec), 0.0, sec)
            dt = np.diff(sec, prepend=sec[:1])
            return np.cumsum(speed * np.where(dt >= 0, dt, 0.0))

        d_old, d_new = dist(old), dist(new)
        delta = float(np.max(np.abs(d_old - d_new))) if len(d_old) else 0.0
        worst_distance = max(worst_distance, delta)
        if len(d_old) and d_old[-1]:
            worst_rel = max(worst_rel, delta / d_old[-1])
        n += 1
print(pd.__version__, "laps", n, "max samples differing per lap", worst_seconds)
print("max |Distance delta| m", worst_distance, "relative", worst_rel)
