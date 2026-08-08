"""Generate the G-G diagram chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_gg_diagram(
        2024,
        "Monaco Grand Prix",
        "Q",
        drivers=["VER", "LEC", "NOR"],
        save_path=Path(__file__).resolve().parent / "gg_diagram.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/gg_diagram.png")
