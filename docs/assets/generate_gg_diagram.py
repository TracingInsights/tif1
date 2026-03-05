"""Generate G-G diagram (acceleration envelope) chart."""

import matplotlib.pyplot as plt
import numpy as np

import tif1
from tif1.plotting import get_team_color, setup_mpl

# Setup plotting style
setup_mpl(color_scheme="fastf1")


def compute_accelerations(telemetry):
    """Calculate longitudinal and lateral accelerations in g."""
    # Convert speed to m/s
    v = np.array(telemetry["Speed"]) / 3.6

    # Longitudinal acceleration from speed change over time
    time_seconds = telemetry["Time"].dt.total_seconds()
    lon_acc = np.gradient(v, time_seconds) / 9.81

    # Lateral acceleration from position change (curvature)
    dx = np.gradient(telemetry["X"], telemetry["Distance"])
    dy = np.gradient(telemetry["Y"], telemetry["Distance"])

    # Calculate track curvature
    curvature = (dx * np.gradient(dy) - dy * np.gradient(dx)) / (dx**2 + dy**2) ** 1.5
    lat_acc = v * v * curvature / 9.81

    # Remove unrealistic values (> 7.5g)
    lon_acc = np.clip(lon_acc, -7.5, 7.5)
    lat_acc = np.clip(lat_acc, -7.5, 7.5)

    return lon_acc, lat_acc


# Load session
session = tif1.get_session(2024, "Monaco", "Q")
laps = session.laps

# Select drivers to compare
drivers = ["VER", "LEC", "NOR"]

# Create plot
fig, ax = plt.subplots(figsize=(12, 12))

# Process each driver
for driver in drivers:
    try:
        # Get fastest lap telemetry
        driver_lap = laps.pick_driver(driver).pick_fastest()
        telemetry = driver_lap.get_telemetry().add_distance()

        # Compute accelerations
        lon_acc, lat_acc = compute_accelerations(telemetry)

        # Get team color
        try:
            team = driver_lap["Team"] if "Team" in driver_lap.index else None
            color = get_team_color(team) if team else None
        except Exception:
            color = None

        if color is None:
            # Fallback colors
            color_map = {"VER": "#0600ef", "LEC": "#dc0000", "NOR": "#ff8700"}
            color = color_map.get(driver, "#ffffff")

        # Plot scatter points
        ax.scatter(
            lat_acc,
            lon_acc,
            s=20,
            alpha=0.4,
            color=color,
            label=driver,
        )

        # Draw performance envelope
        points = np.column_stack([lat_acc, lon_acc])
        points = points[~np.isnan(points).any(axis=1)]

        if len(points) > 10:
            # Find boundary by sampling angles
            angles = np.linspace(0, 2 * np.pi, 72, endpoint=False)
            boundary_points = []

            center = np.mean(points, axis=0)
            centered = points - center

            for angle in angles:
                direction = np.array([np.cos(angle), np.sin(angle)])
                projections = np.dot(centered, direction)
                max_idx = np.argmax(projections)
                boundary_points.append(points[max_idx])

            boundary = np.array(boundary_points)
            boundary = np.vstack([boundary, boundary[0]])

            ax.plot(
                boundary[:, 0],
                boundary[:, 1],
                color=color,
                linewidth=2.5,
                alpha=0.9,
            )

    except Exception as e:
        print(f"Warning: Could not process {driver}: {e}")
        continue

# Styling
ax.set_xlabel("Lateral Acceleration (g)", fontsize=14)
ax.set_ylabel("Longitudinal Acceleration (g)", fontsize=14)
ax.set_title(
    f"G-G Diagram - {session.event.year} {session.event['EventName']} {session.name}",
    fontsize=16,
    fontweight="bold",
    pad=20,
)

# Add grid and axis lines
ax.grid(True, alpha=0.3, linestyle="--")
ax.axhline(0, color="white", linestyle="-", alpha=0.5, linewidth=0.8)
ax.axvline(0, color="white", linestyle="-", alpha=0.5, linewidth=0.8)

# Set equal aspect ratio for proper g-force visualization
ax.set_aspect("equal")

# Legend
ax.legend(loc="lower right", fontsize=12)

plt.tight_layout()
plt.savefig("docs/assets/gg_diagram.png", dpi=300, bbox_inches="tight")
print("Chart saved to docs/assets/gg_diagram.png")
