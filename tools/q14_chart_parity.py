"""Render every chart of the 2026 Monaco race to PNG + pixel arrays (Pergamon Q14 parity).

Usage:
    TIF1_CACHE_DIR=<copy> MPLBACKEND=Agg uv run python tools/q14_chart_parity.py \
        --out-dir /tmp/tif1-q-Q14/parity/cand [--src-dir <control src>]
"""

from __future__ import annotations

import argparse
import io
import sys
import warnings
from pathlib import Path

CHARTS = (
    "speed_traces",
    "annotated_speed_trace",
    "telemetry_comparison",
    "gg_diagram",
    "driver_laptimes",
    "laptimes_distribution",
    "laptime_heatmap",
    "qualifying_grid",
    "lap_delta",
    "position_changes",
    "track_temperature",
    "downforce_levels",
    "throttle_distance",
    "tire_degradation",
    "race_launch_ratings",
    "top_speeds",
    "gear_shifts",
    "multi_driver_speed_comparison",
    "track_speed_map",
    "track_throttle_map",
    "track_brake_zones",
    "track_acceleration_map",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--charts", default=",".join(CHARTS))
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, args.src_dir)
    warnings.simplefilter("ignore")
    import matplotlib.pyplot as plt
    import numpy as np
    from PIL import Image

    from tif1 import charts

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name in args.charts.split(","):
        fn = getattr(charts, f"plot_{name}")
        try:
            fig, _ax = fn(2026, "Monaco Grand Prix", "R")
        except Exception as exc:
            (out / f"{name}.err").write_text(f"{type(exc).__name__}: {exc}\n")
            print(f"{name}: ERROR {type(exc).__name__}: {exc}")
            continue
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        plt.close(fig)
        (out / f"{name}.png").write_bytes(buf.getvalue())
        np.save(out / f"{name}.npy", np.asarray(Image.open(io.BytesIO(buf.getvalue()))))
        print(f"{name}: ok {len(buf.getvalue())} bytes")


if __name__ == "__main__":
    main()
