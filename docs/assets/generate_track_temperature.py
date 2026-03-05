"""Generate track temperature plot for documentation."""

import sys

import matplotlib.pyplot as plt

sys.path.insert(0, "../../src")

import tif1

# Setup plotting
tif1.plotting.setup_mpl(color_scheme="fastf1")

# Load race session
session = tif1.get_session(2023, "Monaco", "R")

# Get laps from driver with most laps
laps = session.laps
driver_counts = laps.groupby("Driver", observed=False)["LapNumber"].count()
most_laps_driver = driver_counts.idxmax()
driver_laps = laps.pick_drivers(most_laps_driver)

# Create plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(
    driver_laps["LapNumber"],
    driver_laps["TrackTemp"],
    color="#ff4444",
    linewidth=2.5,
    label="Track Temperature",
)

ax.set_xlabel("Lap Number", fontsize=12)
ax.set_ylabel("Track Temperature (°C)", fontsize=12)
ax.legend(fontsize=11)
ax.grid(color="w", which="major", axis="both", alpha=0.3)

plt.suptitle(
    f"Track Temperature - {session.event['EventName']} {session.event.year}",
    fontsize=14,
    fontweight="bold",
)

plt.tight_layout()

# Save the figure
OUTPUT_PATH = "track_temperature.png"
plt.savefig(OUTPUT_PATH, dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
print(f"Chart saved to {OUTPUT_PATH}")
plt.close()
