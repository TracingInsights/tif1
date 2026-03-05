"""Generate track throttle map visualization."""
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection

import tif1

# Load session
session = tif1.get_session(2023, "Silverstone", "Q")
fastest_lap = session.laps.pick_fastest()

# Get telemetry
telemetry = fastest_lap.get_car_data()
x = telemetry["X"]
y = telemetry["Y"]
throttle = telemetry["Throttle"]

# Prepare segments
points = np.array([x, y]).T.reshape(-1, 1, 2)
segments = np.concatenate([points[:-1], points[1:]], axis=1)

# Create plot
fig, ax = plt.subplots(figsize=(12, 10))

# Color-coded track
norm = plt.Normalize(0, 100)
lc = LineCollection(segments, cmap="RdYlGn", norm=norm, linewidth=4)
lc.set_array(throttle)
line = ax.add_collection(lc)

# Colorbar
cbar = plt.colorbar(line, ax=ax, label="Throttle (%)")

# Format
ax.set_aspect("equal")
ax.axis("off")
plt.suptitle(f"{session.event['EventName']} {session.event.year} - Throttle Map")
plt.tight_layout()

# Save
plt.savefig("docs/assets/track_throttle_map.png", dpi=300, bbox_inches="tight")
print("Generated track_throttle_map.png")
