"""Naxos N2: ``delphi_warm_ab.py`` over ``naxos_n2_warm_bench.py``.

Same arguments and output as ``delphi_warm_ab.py``; the child bench adds
``load_then_drivers_loop`` and ``load_then_6_shuffled`` to the Elis E5 bench.

Usage:
    python tools/perf/naxos_n2_warm_ab.py --control-src /tmp/naxos/control/src \
        --control-cache /tmp/naxos/warm-N2 --candidate-src /tmp/naxos/N2/src \
        --candidate-cache /tmp/naxos/warm-N2 --lib polars --pairs 8 \
        --workloads load_then_drivers_loop,load_then_6_shuffled
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import delphi_warm_ab

delphi_warm_ab.BENCH = Path(__file__).resolve().parent / "naxos_n2_warm_bench.py"

if __name__ == "__main__":
    delphi_warm_ab.main()
