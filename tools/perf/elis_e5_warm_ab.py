"""Elis E5: ``delphi_warm_ab.py`` over ``elis_e5_warm_bench.py`` (adds ``load_then_N_drivers``).

Same arguments and output as ``delphi_warm_ab.py``; only the child bench differs.

Usage:
    python tools/perf/elis_e5_warm_ab.py --control-src /tmp/tif1-elis-control/src \
        --control-cache /tmp/tif1-delphi-warm --candidate-src /tmp/tif1-elis/E5/src \
        --candidate-cache /tmp/tif1-elis-warm-E5 --lib pandas --pairs 6 \
        --workloads load_then_1_driver,load_then_2_drivers,notebook
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import delphi_warm_ab

delphi_warm_ab.BENCH = Path(__file__).resolve().parent / "elis_e5_warm_bench.py"

if __name__ == "__main__":
    delphi_warm_ab.main()
