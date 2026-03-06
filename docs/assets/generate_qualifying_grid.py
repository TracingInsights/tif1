"""Generate qualifying results grid chart for documentation."""

import sys

sys.path.insert(0, "../../src")

import matplotlib.pyplot as plt
import pandas as pd

import tif1

# Setup plotting with timedelta support
tif1.plotting.setup_mpl(mpl_timedelta_support=True, color_scheme="fastf1")

print("Loading 2023 Spanish Grand Prix qualifying session...")

# Load qualifying session
session = tif1.get_session(2023, "Spanish Grand Prix", "Q")
laps = session.laps

# Get all unique drivers
drivers = pd.unique(laps["Driver"])
print(f"Found {len(drivers)} drivers")

# Get fastest lap for each driver
list_fastest_laps = []
for drv in drivers:
    drv_laps = laps[laps["Driver"] == drv]
    if len(drv_laps) > 0:
        fastest_idx = drv_laps["LapTime"].idxmin()
        list_fastest_laps.append(drv_laps.loc[fastest_idx])

# Create DataFrame and sort by lap time
fastest_laps = pd.DataFrame(list_fastest_laps).sort_values(by="LapTime").reset_index(drop=True)

# Calculate time delta from pole
pole_lap_time = fastest_laps["LapTime"].iloc[0]
fastest_laps["LapTimeDelta"] = fastest_laps["LapTime"] - pole_lap_time

print("\nQualifying Results:")
print(fastest_laps[["Driver", "LapTime", "LapTimeDelta"]].head(10))

# Get team colors for each driver
team_colors = []
for _, lap in fastest_laps.iterrows():
    color = tif1.plotting.get_team_color(team=lap["Team"], session=session)
    team_colors.append(color)

# Create the plot
fig, ax = plt.subplots(figsize=(10, 8))
ax.barh(
    fastest_laps.index,
    fastest_laps["LapTimeDelta"],
    color=team_colors,
    edgecolor="grey",
    linewidth=0.5,
)

# Set driver labels
ax.set_yticks(fastest_laps.index)
ax.set_yticklabels(fastest_laps["Driver"])

# Show fastest at the top
ax.invert_yaxis()

# Add grid lines
ax.set_axisbelow(True)
ax.xaxis.grid(True, which="major", linestyle="--", color="white", alpha=0.3, zorder=-1000)

# Labels
ax.set_xlabel("Gap to Pole (seconds)", fontsize=11)
ax.set_ylabel("Driver", fontsize=11)

# Format pole lap time for title
pole_driver = fastest_laps["Driver"].iloc[0]
pole_time_str = (
    str(pole_lap_time).split()[-1]
    if isinstance(pole_lap_time, pd.Timedelta)
    else f"{pole_lap_time:.3f}"
)

plt.suptitle(
    f"2023 Spanish Grand Prix Qualifying Results\nPole Position: {pole_time_str} ({pole_driver})",
    fontsize=13,
    fontweight="bold",
)

plt.tight_layout()

# Save the figure
output_path = "qualifying_grid.png"
plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
print(f"\n✓ Chart saved to {output_path}")
plt.close()
