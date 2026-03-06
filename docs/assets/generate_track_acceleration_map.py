"""Generate track acceleration map visualization."""

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection

import tif1

# Load session
session = tif1.get_session(2023, "Suzuka", "Q")
fastest_lap = session.laps.pick_fastest()

# Get telemetry
telemetry = fastest_lap.get_car_data()

# Calculate acceleration
speed_ms = telemetry["Speed"] / 3.6
time_s = telemetry["Time"].dt.total_seconds()
lon_acc = np.gradient(speed_ms, time_s) / 9.81

# Extract position
x = telemetry["X"]
y = telemetry["Y"]

# Prepare segments
points = np.array([x, y]).T.reshape(-1, 1, 2)
segments = np.concatenate([points[:-1], points[1:]], axis=1)

# Create plot
fig, ax = plt.subplots(figsize=(12, 10))

# Color-coded track
norm = plt.Normalize(lon_acc.min(), lon_acc.max())
lc = LineCollection(segments, cmap="RdBu_r", norm=norm, linewidth=4)
lc.set_array(lon_acc)
line = ax.add_collection(lc)

# Colorbar
cbar = plt.colorbar(line, ax=ax, label="Longitudinal Acceleration (g)")

# Format
ax.set_aspect("equal")
ax.axis("off")
plt.suptitle(f"{session.event['EventName']} {session.event.year} - Acceleration Map")
plt.tight_layout()

# Save
plt.savefig("docs/assets/track_acceleration_map.png", dpi=300, bbox_inches="tight")
print("Generated track_acceleration_map.png")
