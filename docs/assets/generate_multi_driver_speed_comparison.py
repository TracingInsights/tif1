"""Generate the multi-driver speed comparison chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_multi_driver_speed_comparison(
        2023,
        "Bahrain Grand Prix",
        "Q",
        drivers=["VER", "PER", "LEC"],
        save_path=Path(__file__).resolve().parent / "multi_driver_speed_comparison.png",
        dpi=300,
        facecolor="#1a1a1a",
    )
    print("Chart saved to docs/assets/multi_driver_speed_comparison.png")
