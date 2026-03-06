"""Generate track speed map visualization."""

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection

import tif1

# Load session
session = tif1.get_session(2023, "Monaco", "Q")
fastest_lap = session.laps.pick_fastest()

# Get telemetry
telemetry = fastest_lap.get_car_data()
x = telemetry["X"]
y = telemetry["Y"]
speed = telemetry["Speed"]

# Prepare segments
points = np.array([x, y]).T.reshape(-1, 1, 2)
segments = np.concatenate([points[:-1], points[1:]], axis=1)

# Create plot
fig, ax = plt.subplots(figsize=(12, 10))

# Color-coded track
norm = plt.Normalize(speed.min(), speed.max())
lc = LineCollection(segments, cmap="plasma", norm=norm, linewidth=4)
lc.set_array(speed)
line = ax.add_collection(lc)

# Colorbar
cbar = plt.colorbar(line, ax=ax, label="Speed (km/h)")

# Format
ax.set_aspect("equal")
ax.axis("off")
plt.suptitle(f"{session.event['EventName']} {session.event.year} - Speed Map")
plt.tight_layout()

# Save
plt.savefig("docs/assets/track_speed_map.png", dpi=300, bbox_inches="tight")
print("Generated track_speed_map.png")
