"""Generate the throttle distance chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_throttle_distance(
        2023,
        "Bahrain Grand Prix",
        "Q",
        save_path=Path(__file__).resolve().parent / "throttle_distance.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/throttle_distance.png")
