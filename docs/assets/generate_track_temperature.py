"""Generate the track temperature chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_track_temperature(
        2023,
        "Monaco Grand Prix",
        "R",
        save_path=Path(__file__).resolve().parent / "track_temperature.png",
        dpi=150,
        facecolor="#1a1a1a",
    )
    print("Chart saved to docs/assets/track_temperature.png")
