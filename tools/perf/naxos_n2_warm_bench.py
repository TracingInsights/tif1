"""Naxos N2: ``elis_e5_warm_bench.py`` plus two worst-case shapes of the lazy polars load.

Every workload of the Elis E5 bench (the Delphi bench plus
``load_then_N_drivers``) runs unchanged; two are added:

- ``load_then_drivers_loop``: ``load()``, then the fastest lap's car data
  of every driver in ``session.drivers`` order (laps-row order).
- ``load_then_6_shuffled``: ``load()``, then the fastest lap's car data of
  six drivers out of laps-row order (VER, ALO, PIA, HAM, SAI, LEC): four
  read alone, the fifth reads the rest in one batch.
- ``load_then_top2``: the same for the winner and the runner-up (PIA, RUS:
  the first two drivers in laps-row order).

``naxos_n2_warm_ab.py`` (same folder) runs it interleaved.

Usage:
    TIF1_CACHE_DIR=/tmp/naxos/warm-N2 python tools/perf/naxos_n2_warm_bench.py \
        --child load_then_drivers_loop --lib polars
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import elis_e5_warm_bench

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
SHUFFLED = ("VER", "ALO", "PIA", "HAM", "SAI", "LEC")
TOP2 = ("PIA", "RUS")
EXTRA = ("load_then_drivers_loop", "load_then_6_shuffled", "load_then_top2")


def _child(workload: str, lib: str) -> None:
    out: dict[str, float] = {}
    t_proc = time.perf_counter()

    def step(name, fn):
        t0 = time.perf_counter()
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        return r

    import tif1

    out["import"] = round((time.perf_counter() - t_proc) * 1000, 2)
    session = step("get_session", lambda: tif1.get_session(YEAR, GP, SESSION, lib=lib))
    step("load", lambda: session.load())

    def driver_codes():
        if workload == "load_then_6_shuffled":
            return SHUFFLED
        if workload == "load_then_top2":
            return TOP2
        return [session.get_driver(number)["Abbreviation"] for number in session.drivers]

    step(
        "access",
        lambda: [
            session.laps.pick_drivers(code).pick_fastest().get_car_data() for code in driver_codes()
        ],
    )
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    print(json.dumps(out))


def main() -> None:
    if "--child" in sys.argv:
        workload = sys.argv[sys.argv.index("--child") + 1]
        lib = sys.argv[sys.argv.index("--lib") + 1] if "--lib" in sys.argv else "pandas"
        if workload in EXTRA:
            _child(workload, lib)
            return
    elis_e5_warm_bench.main()


if __name__ == "__main__":
    main()
