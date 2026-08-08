"""Generate the track acceleration map chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_track_acceleration_map(
        2023,
        "Suzuka",
        "Q",
        save_path=Path(__file__).resolve().parent / "track_acceleration_map.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/track_acceleration_map.png")
