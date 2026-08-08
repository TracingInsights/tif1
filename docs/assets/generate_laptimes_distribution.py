"""Generate the lap times distribution chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_laptimes_distribution(
        2023,
        "Azerbaijan Grand Prix",
        "R",
        save_path=Path(__file__).resolve().parent / "laptimes_distribution.png",
        dpi=150,
        facecolor="#1a1a1a",
    )
    print("Chart saved to docs/assets/laptimes_distribution.png")
