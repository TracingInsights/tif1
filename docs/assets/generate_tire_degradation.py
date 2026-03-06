"""Generate tire degradation visualization."""

import matplotlib.pyplot as plt
import numpy as np

import tif1
from tif1.plotting import setup_mpl


def rolling_median(data, window=5):
    """Simple rolling median for smoothing."""
    result = []
    for i in range(len(data)):
        start = max(0, i - window // 2)
        end = min(len(data), i + window // 2 + 1)
        result.append(np.median(data[start:end]))
    return np.array(result)


# Setup plotting style (use FastF1 dark background)
setup_mpl(color_scheme="fastf1")

# Load session data
session = tif1.get_session(2024, "Hungarian Grand Prix", "Race")
laps = session.laps

# Convert LapTime to seconds
laps["LapTimeSeconds"] = laps["LapTime"].dt.total_seconds()

# Clean data: remove outliers and first lap
clean_laps = laps[
    (laps["LapNumber"] > 1) & (laps["LapTimeSeconds"] < laps["LapTimeSeconds"].min() * 1.07)
].copy()

# Fuel correction: estimate 0.03s per lap improvement from fuel burn
max_lap = clean_laps["LapNumber"].max()
clean_laps["FuelCorrectedTime"] = clean_laps["LapTimeSeconds"] - (
    0.03 * (max_lap - clean_laps["LapNumber"])
)

# Create figure
fig, ax = plt.subplots(figsize=(14, 8))

# Compound colors and data
compounds = {
    "SOFT": {"color": "red", "alpha": 0.3},
    "MEDIUM": {"color": "gold", "alpha": 0.3},
    "HARD": {"color": "lightgray", "alpha": 0.3},
}

# Plot each compound
for compound, style in compounds.items():
    compound_laps = clean_laps[clean_laps["Compound"] == compound]

    if len(compound_laps) > 10:
        # Scatter plot of raw data
        ax.scatter(
            compound_laps["TyreLife"],
            compound_laps["FuelCorrectedTime"],
            color=style["color"],
            alpha=style["alpha"],
            s=30,
            zorder=1,
        )

        # Group by tire life and get median
        grouped = compound_laps.groupby("TyreLife")["FuelCorrectedTime"].median()
        tire_life = grouped.index.values
        lap_times = grouped.values

        # Apply rolling median for smoothing
        if len(lap_times) > 5:
            smoothed_times = rolling_median(lap_times, window=5)
        else:
            smoothed_times = lap_times

        # Plot smoothed line
        ax.plot(
            tire_life,
            smoothed_times,
            color=style["color"],
            linewidth=4,
            label=compound,
            zorder=2,
        )

        # Add annotation at end of line
        ax.annotate(
            compound,
            xy=(tire_life[-1], smoothed_times[-1]),
            xytext=(tire_life[-1] + 2, smoothed_times[-1]),
            fontsize=14,
            fontweight="bold",
            color=style["color"],
            va="center",
        )

# Styling
ax.set_xlabel("Tire Life (Laps)", fontsize=16, fontweight="bold")
ax.set_ylabel("Fuel-Corrected Lap Time (s)", fontsize=16, fontweight="bold")
ax.set_title(
    f"Tire Degradation Analysis - {session.event['EventName']} {session.event['EventDate'].year}",
    fontsize=18,
    fontweight="bold",
    pad=20,
)

# Grid and legend
ax.grid(True, alpha=0.3, linestyle="--")
ax.legend(fontsize=14, loc="upper left", framealpha=0.9)

# Set reasonable y-axis limits
y_min = clean_laps["FuelCorrectedTime"].quantile(0.01)
y_max = clean_laps["FuelCorrectedTime"].quantile(0.99)
ax.set_ylim(y_min - 0.5, y_max + 0.5)

plt.tight_layout()
plt.savefig("docs/assets/tire_degradation.png", dpi=300, bbox_inches="tight")
print("✓ Tire degradation chart saved to docs/assets/tire_degradation.png")
