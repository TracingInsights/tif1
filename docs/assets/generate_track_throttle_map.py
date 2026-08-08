"""Generate the track throttle map chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_track_throttle_map(
        2023,
        "Silverstone",
        "Q",
        save_path=Path(__file__).resolve().parent / "track_throttle_map.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/track_throttle_map.png")
