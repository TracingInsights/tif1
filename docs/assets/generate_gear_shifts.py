"""Generate the gear shifts on track chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_gear_shifts(
        2021,
        "Austrian Grand Prix",
        "Q",
        save_path=Path(__file__).resolve().parent / "gear_shifts_on_track.png",
        dpi=150,
        facecolor="#1a1a1a",
    )
    print("Chart saved to docs/assets/gear_shifts_on_track.png")
