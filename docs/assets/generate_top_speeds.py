"""Generate top speeds by team plot for documentation."""

import sys

sys.path.insert(0, "../../src")

import matplotlib.pyplot as plt

import tif1

# Setup plotting
tif1.plotting.setup_mpl(color_scheme="fastf1")

# Load qualifying session from Monza (known for high speeds)
session = tif1.get_session(2023, "Italian Grand Prix", "Q")

# Find which speed trap recorded the highest speeds
speed_columns = ["SpeedI1", "SpeedI2", "SpeedST", "SpeedFL"]
speed_data = session.laps[speed_columns]
fastest_trap = speed_data.idxmax(axis=1).value_counts().index[0]

print(f"Using speed trap: {fastest_trap}")

# Calculate maximum speeds by team
team_speeds = session.laps[[fastest_trap, "Team"]].copy()
max_speeds = team_speeds.groupby("Team")[fastest_trap].max().reset_index()
max_speeds.columns = ["Team", "MaxSpeed"]

# Sort by speed (descending)
max_speeds = max_speeds.sort_values("MaxSpeed", ascending=False)

# Calculate difference from slowest team
max_speeds["Diff"] = max_speeds["MaxSpeed"] - max_speeds["MaxSpeed"].min()

# Get team colors
team_colors = {}
for team in max_speeds["Team"]:
    team_colors[team] = tif1.plotting.get_team_color(team, session=session)

max_speeds["Color"] = max_speeds["Team"].map(team_colors)

# Create the plot
fig, ax = plt.subplots(figsize=(10, 8))

# Create horizontal bars
bars = ax.barh(
    y=max_speeds["Team"], width=max_speeds["Diff"], color=max_speeds["Color"], height=0.7
)

# Add speed value labels
for i, (speed, diff) in enumerate(zip(max_speeds["MaxSpeed"], max_speeds["Diff"])):
    ax.text(diff + 0.2, i, f"{int(speed)} km/h", va="center", fontsize=10, fontweight="bold")

# Styling
ax.set_xlabel("Speed Difference (km/h)", fontsize=11)
ax.set_title(
    f"{session.event.year} {session.event['EventName']} - Top Speeds by Team",
    fontsize=13,
    fontweight="bold",
    pad=20,
)
ax.invert_yaxis()
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(axis="x", alpha=0.3, linestyle="--")

plt.tight_layout()

# Save the figure
output_path = "top_speeds.png"
plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
print(f"Chart saved to {output_path}")
plt.close()
