"""Generate the downforce levels chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_downforce_levels(
        2023,
        "Monaco Grand Prix",
        "Q",
        save_path=Path(__file__).resolve().parent / "downforce_levels.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/downforce_levels.png")
