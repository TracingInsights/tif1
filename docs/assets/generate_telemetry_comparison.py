"""Generate telemetry comparison chart with acceleration analysis."""

import math

import matplotlib.pyplot as plt
import numpy as np

import tif1
from tif1.plotting import get_team_color, setup_mpl

# Setup plotting style
setup_mpl(color_scheme="fastf1")


def smooth_derivative(t_in, v_in):
    """Compute smooth derivative using low-noise differentiator.

    Reference: http://holoborodko.com/pavel/numerical-methods/
    """
    t = t_in.copy()
    v = v_in.copy()

    # Convert time to seconds
    try:
        for i in range(len(t)):
            t.iloc[i] = t.iloc[i].total_seconds()
    except Exception:
        pass

    t = np.array(t)
    v = np.array(v)

    dvdt = np.zeros(t.size)

    # Handle edge points
    dvdt[0] = (v[1] - v[0]) / (t[1] - t[0])
    dvdt[1] = (v[2] - v[0]) / (t[2] - t[0])
    dvdt[2] = (v[3] - v[1]) / (t[3] - t[1])

    n = t.size
    dvdt[n - 1] = (v[n - 1] - v[n - 2]) / (t[n - 1] - t[n - 2])
    dvdt[n - 2] = (v[n - 1] - v[n - 3]) / (t[n - 1] - t[n - 3])
    dvdt[n - 3] = (v[n - 2] - v[n - 4]) / (t[n - 2] - t[n - 4])

    # Smooth derivative for interior points
    c = [5.0 / 32.0, 4.0 / 32.0, 1.0 / 32.0]
    for i in range(3, t.size - 3):
        for j in range(1, 4):
            dvdt[i] += 2 * j * c[j - 1] * (v[i + j] - v[i - j]) / (t[i + j] - t[i - j])

    return dvdt


def compute_accelerations(telemetry):
    """Compute longitudinal and lateral accelerations."""
    # Convert speed to m/s
    v = np.array(telemetry["Speed"]) / 3.6

    # Longitudinal acceleration from speed
    lon_acc = smooth_derivative(telemetry["Time"], v) / 9.81

    # Lateral acceleration from curvature
    dx = smooth_derivative(telemetry["Distance"], telemetry["X"])
    dy = smooth_derivative(telemetry["Distance"], telemetry["Y"])

    # Calculate heading angle
    theta = np.zeros(dx.size)
    theta[0] = math.atan2(dy[0], dx[0])

    for i in range(1, dx.size):
        angle_diff = math.atan2(dy[i], dx[i]) - theta[i - 1]
        # Normalize to [-pi, pi]
        angle_diff = (angle_diff + math.pi) % (2 * math.pi) - math.pi
        theta[i] = theta[i - 1] + angle_diff

    # Curvature and lateral acceleration
    kappa = smooth_derivative(telemetry["Distance"], theta)
    lat_acc = v * v * kappa / 9.81

    # Remove outliers (> 7.5g)
    lon_acc = np.clip(lon_acc, -7.5, 7.5)
    lat_acc = np.clip(lat_acc, -7.5, 7.5)

    return lon_acc, lat_acc


# Load session
session = tif1.get_session(2024, "Monaco", "Q")
laps = session.laps

# Get fastest laps for two drivers
driver_1 = "VER"
driver_2 = "LEC"

laps_d1 = laps.pick_driver(driver_1).pick_fastest()
laps_d2 = laps.pick_driver(driver_2).pick_fastest()

# Get telemetry
tel_d1 = laps_d1.get_telemetry().add_distance()
tel_d2 = laps_d2.get_telemetry().add_distance()

# Compute accelerations
lon_acc_d1, lat_acc_d1 = compute_accelerations(tel_d1)
lon_acc_d2, lat_acc_d2 = compute_accelerations(tel_d2)

tel_d1["LongAcc"] = lon_acc_d1
tel_d1["LatAcc"] = lat_acc_d1
tel_d2["LongAcc"] = lon_acc_d2
tel_d2["LatAcc"] = lat_acc_d2

# Get team colors
try:
    team_d1 = laps_d1["Team"].iloc[0] if "Team" in laps_d1.columns else None
    team_d2 = laps_d2["Team"].iloc[0] if "Team" in laps_d2.columns else None
except Exception:
    team_d1 = None
    team_d2 = None

color_d1 = get_team_color(team_d1, session) if team_d1 else "#0600ef"
color_d2 = get_team_color(team_d2, session) if team_d2 else "#dc0000"

# Label driver actions
tel_d1.loc[tel_d1["Brake"] > 0, "Action"] = "Brake"
tel_d1.loc[tel_d1["Throttle"] == 100, "Action"] = "Full Throttle"
tel_d1.loc[(tel_d1["Brake"] == 0) & (tel_d1["Throttle"] < 100), "Action"] = "Lift"

tel_d2.loc[tel_d2["Brake"] > 0, "Action"] = "Brake"
tel_d2.loc[tel_d2["Throttle"] == 100, "Action"] = "Full Throttle"
tel_d2.loc[(tel_d2["Brake"] == 0) & (tel_d2["Throttle"] < 100), "Action"] = "Lift"

# Create action segments
tel_d1["ActionID"] = (tel_d1["Action"] != tel_d1["Action"].shift(1)).cumsum()
tel_d2["ActionID"] = (tel_d2["Action"] != tel_d2["Action"].shift(1)).cumsum()

actions_d1 = (
    tel_d1[["ActionID", "Action", "Distance"]]
    .groupby(["ActionID", "Action"])
    .max("Distance")
    .reset_index()
)
actions_d2 = (
    tel_d2[["ActionID", "Action", "Distance"]]
    .groupby(["ActionID", "Action"])
    .max("Distance")
    .reset_index()
)

actions_d1["DistanceDelta"] = actions_d1["Distance"] - actions_d1["Distance"].shift(1)
actions_d1.loc[0, "DistanceDelta"] = actions_d1.loc[0, "Distance"]

actions_d2["DistanceDelta"] = actions_d2["Distance"] - actions_d2["Distance"].shift(1)
actions_d2.loc[0, "DistanceDelta"] = actions_d2.loc[0, "Distance"]

# Calculate average speed difference
distance_min, distance_max = 1000, 2500
avg_speed_d1 = tel_d1.loc[
    (tel_d1["Distance"] >= distance_min) & (tel_d1["Distance"] <= distance_max), "Speed"
].mean()
avg_speed_d2 = tel_d2.loc[
    (tel_d2["Distance"] >= distance_min) & (tel_d2["Distance"] <= distance_max), "Speed"
].mean()

if avg_speed_d1 > avg_speed_d2:
    speed_text = f"{driver_1} {round(avg_speed_d1 - avg_speed_d2, 2)} km/h faster"
else:
    speed_text = f"{driver_2} {round(avg_speed_d2 - avg_speed_d1, 2)} km/h faster"

# Create plot
fig, ax = plt.subplots(4, figsize=(16, 12), sharex=True)

# Speed comparison
ax[0].plot(tel_d1["Distance"], tel_d1["Speed"], label=driver_1, color=color_d1, linewidth=2)
ax[0].plot(tel_d2["Distance"], tel_d2["Speed"], label=driver_2, color=color_d2, linewidth=2)
ax[0].set_ylabel("Speed (km/h)", fontsize=14)
ax[0].legend(loc="lower right", fontsize=12)
ax[0].text(distance_min + 50, 280, speed_text, fontsize=12, color="lime")
ax[0].grid(True, alpha=0.3)

# Longitudinal acceleration
ax[1].plot(tel_d1["Distance"], tel_d1["LongAcc"], label=driver_1, color=color_d1, linewidth=2)
ax[1].plot(tel_d2["Distance"], tel_d2["LongAcc"], label=driver_2, color=color_d2, linewidth=2)
ax[1].set_ylabel("Long. Acc (g)", fontsize=14)
ax[1].axhline(0, color="white", linestyle="--", alpha=0.5)
ax[1].legend(loc="lower right", fontsize=12)
ax[1].grid(True, alpha=0.3)

# Lateral acceleration
ax[2].plot(tel_d1["Distance"], tel_d1["LatAcc"], label=driver_1, color=color_d1, linewidth=2)
ax[2].plot(tel_d2["Distance"], tel_d2["LatAcc"], label=driver_2, color=color_d2, linewidth=2)
ax[2].set_ylabel("Lat. Acc (g)", fontsize=14)
ax[2].axhline(0, color="white", linestyle="--", alpha=0.5)
ax[2].legend(loc="lower right", fontsize=12)
ax[2].grid(True, alpha=0.3)

# Driver actions
action_colors = {"Full Throttle": "lime", "Lift": "grey", "Brake": "red"}

for _driver, actions, y_pos in [(driver_1, actions_d1, 0), (driver_2, actions_d2, 1)]:
    previous_end = 0
    for _, action in actions.iterrows():
        ax[3].barh(
            [y_pos],
            action["DistanceDelta"],
            left=previous_end,
            color=action_colors[action["Action"]],
            height=0.8,
        )
        previous_end += action["DistanceDelta"]

ax[3].set_yticks([0, 1])
ax[3].set_yticklabels([driver_1, driver_2])
ax[3].set_xlabel("Distance (m)", fontsize=14)
ax[3].set_ylabel("Driver Actions", fontsize=14)

# Add legend for actions
handles = [plt.Rectangle((0, 0), 1, 1, color=action_colors[label]) for label in action_colors]
ax[3].legend(handles, action_colors.keys(), loc="upper right", fontsize=10)

# Set x-axis limits
for a in ax:
    a.set_xlim(distance_min, distance_max)

plt.suptitle(
    f"{session.event.year} {session.event['EventName']} - {session.name}",
    fontsize=18,
    y=0.995,
)
plt.tight_layout()
plt.savefig("docs/assets/telemetry_comparison.png", dpi=300, bbox_inches="tight")
print("Chart saved to docs/assets/telemetry_comparison.png")
