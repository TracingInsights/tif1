"""Fresh-process chart timing probe (Pergamon Q14).

Times ``import tif1`` -> ``import tif1.charts`` -> first chart (construction
incl. warm data access) -> ``savefig`` to a BytesIO, then a second chart in
the same process. Prints one JSON line; ``--png-dir`` writes the PNG bytes
for pixel comparison between trees.

Usage:
    TIF1_CACHE_DIR=<copy> MPLBACKEND=Agg uv run python tools/q14_chart_probe.py \
        [--src-dir /tmp/tif1-pergamon-control/src] --charts speed_traces,top_speeds
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
from pathlib import Path

YEAR, EVENT, SESSION = 2026, "Monaco Grand Prix", "R"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir", default=None)
    parser.add_argument("--charts", default="speed_traces,top_speeds")
    parser.add_argument("--png-dir", default=None)
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()
    if args.src_dir:
        sys.path.insert(0, args.src_dir)

    out: dict = {}
    t0 = time.perf_counter()
    import tif1

    t1 = time.perf_counter()
    from tif1 import charts

    t2 = time.perf_counter()
    out["import_tif1_s"] = t1 - t0
    out["import_charts_s"] = t2 - t1
    out["src"] = str(Path(tif1.__file__).parent)

    prof = None
    if args.profile:
        import cProfile

        prof = cProfile.Profile()
        prof.enable()
    for i, name in enumerate(args.charts.split(",")):
        fn = getattr(charts, f"plot_{name}")
        ta = time.perf_counter()
        fig, _ax = fn(YEAR, EVENT, SESSION)
        tb = time.perf_counter()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        tc = time.perf_counter()
        import matplotlib.pyplot as plt

        plt.close(fig)
        out[f"chart{i}_{name}_build_s"] = tb - ta
        out[f"chart{i}_{name}_save_s"] = tc - tb
        out[f"chart{i}_s"] = tc - ta
        if args.png_dir:
            Path(args.png_dir).mkdir(parents=True, exist_ok=True)
            (Path(args.png_dir) / f"{i}_{name}.png").write_bytes(buf.getvalue())
    if prof is not None:
        prof.disable()
        prof.dump_stats(args.profile)
    out["first_total_s"] = out["import_tif1_s"] + out["import_charts_s"] + out["chart0_s"]
    out["total_s"] = time.perf_counter() - t0
    print(json.dumps({k: (round(v, 4) if isinstance(v, float) else v) for k, v in out.items()}))


if __name__ == "__main__":
    main()
