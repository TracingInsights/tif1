"""Generate the qualifying grid chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_qualifying_grid(
        2023,
        "Spanish Grand Prix",
        "Q",
        save_path=Path(__file__).resolve().parent / "qualifying_grid.png",
        dpi=150,
        facecolor="#1a1a1a",
    )
    print("Chart saved to docs/assets/qualifying_grid.png")
