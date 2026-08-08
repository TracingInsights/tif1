"""Generate the race position changes chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_position_changes(
        2023,
        1,
        "R",
        save_path=Path(__file__).resolve().parent / "race_position_changes.png",
        dpi=150,
        facecolor=None,
    )
    print("Chart saved to docs/assets/race_position_changes.png")
