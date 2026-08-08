"""Generate the telemetry comparison chart for documentation."""

import sys
from pathlib import Path

sys.path.insert(0, "../../src")

import tif1

if __name__ == "__main__":
    tif1.charts.plot_telemetry_comparison(
        2024,
        "Monaco Grand Prix",
        "Q",
        save_path=Path(__file__).resolve().parent / "telemetry_comparison.png",
        dpi=300,
        facecolor=None,
    )
    print("Chart saved to docs/assets/telemetry_comparison.png")
