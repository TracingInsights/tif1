"""Generate multi-driver speed comparison visualization."""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.collections import LineCollection

import tif1

# Load session
session = tif1.get_session(2023, "Bahrain", "Q")
drivers = ["VER", "PER", "LEC"]

# Collect telemetry
telemetry_data = []
for driver in drivers:
    lap = session.laps.pick_driver(driver).pick_fastest()
    telemetry = lap.get_car_data()
    telemetry["Driver"] = driver
    driver_info = session.get_driver(driver)
    telemetry["Color"] = driver_info["TeamColor"]
    telemetry_data.append(telemetry)

all_telemetry = pd.concat(telemetry_data)

# Create visualization
fig, ax = plt.subplots(figsize=(14, 10))

for driver in drivers:
    driver_tel = all_telemetry[all_telemetry["Driver"] == driver]
    x = driver_tel["X"].values
    y = driver_tel["Y"].values
    speed = driver_tel["Speed"].values

    points = np.array([x, y]).T.reshape(-1, 1, 2)
    segments = np.concatenate([points[:-1], points[1:]], axis=1)

    norm = plt.Normalize(speed.min(), speed.max())
    lc = LineCollection(segments, cmap="plasma", norm=norm, linewidth=3, alpha=0.7, label=driver)
    lc.set_array(speed)
    ax.add_collection(lc)

ax.set_aspect("equal")
ax.axis("off")
ax.legend(loc="upper right", fontsize=12)
plt.suptitle(f"{session.event['EventName']} {session.event.year} - Speed Comparison")
plt.tight_layout()

# Save
plt.savefig("docs/assets/multi_driver_speed_comparison.png", dpi=300, bbox_inches="tight")
print("Generated multi_driver_speed_comparison.png")
