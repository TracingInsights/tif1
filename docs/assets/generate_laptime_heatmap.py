"""Generate lap time heatmap visualization for documentation."""

import matplotlib.pyplot as plt
import seaborn as sns

import tif1

# Setup plotting
tif1.plotting.setup_mpl(color_scheme="fastf1")

# Load race session
session = tif1.get_session(2023, "Monaco Grand Prix", "R")
laps = session.laps

# Convert lap times to seconds for easier processing
laps_clean = laps.copy()
laps_clean["LapTimeSeconds"] = laps_clean["LapTime"].dt.total_seconds()

# Filter out invalid laps (pit stops, deleted laps, etc.)
laps_clean = laps_clean[~laps_clean["Deleted"]]
laps_clean = laps_clean[laps_clean["PitInTime"].isna()]
laps_clean = laps_clean[laps_clean["PitOutTime"].isna()]

# Remove outliers (laps slower than 107% of fastest)
fastest_lap = laps_clean["LapTimeSeconds"].min()
laps_clean = laps_clean[laps_clean["LapTimeSeconds"] < fastest_lap * 1.07]

# Create pivot table: drivers as rows, lap numbers as columns
heatmap_data = laps_clean.pivot_table(
    index="Driver", columns="LapNumber", values="LapTimeSeconds", aggfunc="first"
)

# Sort drivers by average lap time
driver_avg = heatmap_data.mean(axis=1).sort_values()
heatmap_data = heatmap_data.loc[driver_avg.index]

# Create the heatmap
fig, ax = plt.subplots(figsize=(16, 10))

sns.heatmap(
    heatmap_data,
    cmap="RdYlGn_r",  # Red (slow) to Green (fast)
    vmin=fastest_lap,
    vmax=fastest_lap * 1.07,
    cbar_kws={"label": "Lap Time (seconds)", "aspect": 40},
    linewidths=0.5,
    linecolor="#1a1a1a",
    xticklabels=5,  # Show every 5th lap number
    ax=ax,
)

# Styling
ax.set_xlabel("Lap Number", fontsize=12, fontweight="bold")
ax.set_ylabel("Driver", fontsize=12, fontweight="bold")
ax.set_title("2023 Monaco Grand Prix - Lap Time Heatmap", fontsize=14, fontweight="bold", pad=20)

# Rotate y-axis labels for better readability
plt.yticks(rotation=0, fontsize=10)
plt.xticks(fontsize=10)

plt.tight_layout()
plt.savefig("docs/assets/laptime_heatmap.png", dpi=300, bbox_inches="tight")
print("Generated: docs/assets/laptime_heatmap.png")
