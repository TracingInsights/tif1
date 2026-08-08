"""Generate the top speeds by team chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_top_speeds(
        2023,
        "Italian Grand Prix",
        "Q",
        save_path=Path(__file__).resolve().parent / "top_speeds.png",
        dpi=150,
        facecolor="#1a1a1a",
    )
    print("Chart saved to docs/assets/top_speeds.png")
