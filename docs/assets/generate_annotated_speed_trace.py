"""Generate annotated speed trace plot with corner markers for documentation."""

import sys

import matplotlib.pyplot as plt

sys.path.insert(0, "../../src")

import tif1

# Setup plotting
tif1.plotting.setup_mpl(mpl_timedelta_support=True, color_scheme="fastf1")

# Load qualifying session
session = tif1.get_session(2021, "Spanish Grand Prix", "Q")

# Get the fastest lap
fastest_lap = session.laps.pick_fastest()
car_data = fastest_lap.get_car_data().add_distance()

# Get circuit info with corner locations
circuit_info = session.get_circuit_info()

# Get team color
team_color = tif1.plotting.get_team_color(fastest_lap["Team"], session=session)

# Create the plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(
    car_data["Distance"],
    car_data["Speed"],
    color=team_color,
    label=fastest_lap["Driver"],
    linewidth=2,
)

# Draw vertical dotted lines at each corner
v_min = car_data["Speed"].min()
v_max = car_data["Speed"].max()
ax.vlines(
    x=circuit_info.corners["Distance"],
    ymin=v_min - 20,
    ymax=v_max + 20,
    linestyles="dotted",
    colors="grey",
)

# Plot corner numbers below each vertical line
for _, corner in circuit_info.corners.iterrows():
    txt = f"{corner['Number']}{corner['Letter']}"
    ax.text(corner["Distance"], v_min - 30, txt, va="center_baseline", ha="center", size="small")

ax.set_xlabel("Distance (m)", fontsize=12)
ax.set_ylabel("Speed (km/h)", fontsize=12)
ax.legend(fontsize=11)
ax.grid(color="w", which="major", axis="both", alpha=0.3)

# Adjust y-axis to include corner numbers
ax.set_ylim([v_min - 40, v_max + 20])

plt.suptitle(
    f"Speed Trace with Corner Annotations\n{session.event['EventName']} {session.event.year} Qualifying",
    fontsize=14,
    fontweight="bold",
)

plt.tight_layout()

# Save the figure
OUTPUT_PATH = "annotated_speed_trace.png"
plt.savefig(OUTPUT_PATH, dpi=150, bbox_inches="tight", facecolor="#1a1a1a")
print(f"Chart saved to {OUTPUT_PATH}")
plt.close()
