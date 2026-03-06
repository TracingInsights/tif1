"""Generate lap time delta comparison chart."""

import matplotlib.pyplot as plt
from matplotlib.patches import Patch

import tif1
from tif1.plotting import get_team_color, setup_mpl

# Setup plotting with default style
setup_mpl(color_scheme="fastf1")

# Load session
session = tif1.get_session(2024, "Monaco", "R")
laps = session.laps

# Select two drivers to compare
driver1 = "VER"
driver2 = "LEC"

# Get laps for both drivers
driver1_laps = laps[laps["Driver"] == driver1][["LapNumber", "LapTime"]].copy()
driver2_laps = laps[laps["Driver"] == driver2][["LapNumber", "LapTime"]].copy()

# Convert lap times to seconds
driver1_laps["LapTime"] = driver1_laps["LapTime"].dt.total_seconds()
driver2_laps["LapTime"] = driver2_laps["LapTime"].dt.total_seconds()

# Merge on lap number to compare
merged = driver1_laps.merge(driver2_laps, on="LapNumber", suffixes=("_d1", "_d2"))

# Calculate delta (positive = driver2 faster, negative = driver1 faster)
merged["Delta"] = merged["LapTime_d1"] - merged["LapTime_d2"]

# Color based on who was faster
merged["Color"] = merged["Delta"].apply(
    lambda x: get_team_color("Red Bull Racing") if x < 0 else get_team_color("Ferrari")
)

# Create the plot
fig, ax = plt.subplots(figsize=(14, 8))

# Bar chart of deltas
ax.bar(
    merged["LapNumber"],
    merged["Delta"],
    color=merged["Color"],
    width=0.8,
    edgecolor="white",
    linewidth=0.5,
)

# Add zero line
ax.axhline(0, color="white", linestyle="--", linewidth=1, alpha=0.7)

# Labels and title
ax.set_xlabel("Lap Number", fontsize=14)
ax.set_ylabel("Time Delta (seconds)", fontsize=14)
ax.set_title(
    f"{session.event.year} {session.event['EventName']} - Lap Time Delta\n{driver1} vs {driver2}",
    fontsize=16,
    fontweight="bold",
    pad=20,
)

# Add legend
legend_elements = [
    Patch(facecolor=get_team_color("Red Bull Racing"), label=f"{driver1} faster"),
    Patch(facecolor=get_team_color("Ferrari"), label=f"{driver2} faster"),
]
ax.legend(handles=legend_elements, loc="upper right", fontsize=12)

# Grid
ax.grid(True, alpha=0.3, axis="y")
ax.set_ylim(-2, 2)

# Clean up spines
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

plt.tight_layout()
plt.savefig("lap_delta.png", dpi=300, bbox_inches="tight")
print("Chart saved to lap_delta.png")
