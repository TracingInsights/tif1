"""Generate the lap time delta comparison chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_lap_delta(
        2024,
        "Monaco Grand Prix",
        "R",
        drivers=["VER", "LEC"],
        save_path=Path(__file__).resolve().parent / "lap_delta.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/lap_delta.png")
