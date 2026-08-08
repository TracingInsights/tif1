"""Generate the annotated speed trace chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_annotated_speed_trace(
        2021,
        "Spanish Grand Prix",
        "Q",
        save_path=Path(__file__).resolve().parent / "annotated_speed_trace.png",
        dpi=150,
        facecolor="#1a1a1a",
    )
    print("Chart saved to docs/assets/annotated_speed_trace.png")
