"""Elis E2: modules a warm (offline, primed cache) laps process imports, one per line.

Run once per tree and diff: ``PYTHONPATH=<src> TIF1_CACHE_DIR=<cache> python
tools/perf/elis_e2_warm_modules.py <out>``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ["TIF1_OFFLINE_MODE"] = "true"
import tif1

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=sys.argv[2])
_ = session.laps
Path(sys.argv[1]).write_text("\n".join(sorted(sys.modules)) + "\n", encoding="utf-8")
