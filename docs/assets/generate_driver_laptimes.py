"""Generate the driver lap times chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_driver_laptimes(
        2023,
        "Azerbaijan Grand Prix",
        "R",
        drivers=["ALO"],
        save_path=Path(__file__).resolve().parent / "driver_laptimes_example.png",
        dpi=150,
        facecolor="#1a1a1a",
    )
    print("Chart saved to docs/assets/driver_laptimes_example.png")
