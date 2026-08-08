"""Generate the track brake zones chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_track_brake_zones(
        2023,
        "Monza",
        "Q",
        save_path=Path(__file__).resolve().parent / "track_brake_zones.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/track_brake_zones.png")
