"""Generate gear shifts on track visualization for documentation."""

import sys

sys.path.insert(0, "../../src")

import tif1
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import colormaps
from matplotlib.collections import LineCollection

# Setup plotting
tif1.plotting.setup_mpl(mpl_timedelta_support=False, color_scheme="fastf1")

print("Generating gear shifts on track visualization...")

# Load qualifying session
session = tif1.get_session(2021, "Austrian Grand Prix", "Q")

# Get fastest lap
lap = session.laps.pick_fastest()
tel = lap.get_telemetry()

# Prepare the data for plotting by converting to numpy arrays
x = np.array(tel["X"].values)
y = np.array(tel["Y"].values)

points = np.array([x, y]).T.reshape(-1, 1, 2)
segments = np.concatenate([points[:-1], points[1:]], axis=1)
gear = tel["nGear"].to_numpy().astype(float)

# Create a line collection with segmented colormap
cmap = colormaps["Paired"]
lc_comp = LineCollection(segments, norm=plt.Normalize(1, cmap.N + 1), cmap=cmap)
lc_comp.set_array(gear)
lc_comp.set_linewidth(4)

# Create the plot
fig, ax = plt.subplots(figsize=(10, 8))
ax.add_collection(lc_comp)
ax.axis("equal")
ax.tick_params(labelleft=False, left=False, labelbottom=False, bottom=False)

plt.suptitle(
    f"Fastest Lap Gear Shift Visualization\n"
    f"{lap['Driver']} - {session.event['EventName']} {session.event.year}",
    fontsize=14,
    fontweight="bold",
)

# Add colorbar with centered ticks for each gear
cbar = plt.colorbar(mappable=lc_comp, label="Gear", boundaries=np.arange(1, 10))
cbar.set_ticks(np.arange(1.5, 9.5))
cbar.set_ticklabels(np.arange(1, 9))

plt.tight_layout()

# Save the figure
output_path = "gear_shifts_on_track.png"
plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
print(f"✓ Chart saved to {output_path}")
plt.close()
