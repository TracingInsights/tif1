"""Generate the tire degradation chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_tire_degradation(
        2024,
        "Hungarian Grand Prix",
        "Race",
        save_path=Path(__file__).resolve().parent / "tire_degradation.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/tire_degradation.png")
