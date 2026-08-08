"""Generate the lap time heatmap chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_laptime_heatmap(
        2023,
        "Monaco Grand Prix",
        "R",
        save_path=Path(__file__).resolve().parent / "laptime_heatmap.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/laptime_heatmap.png")
