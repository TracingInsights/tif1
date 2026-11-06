"""Naxos N3: ``delphi_warm_bench.py`` plus the telemetry-arithmetic workloads of N3.

Every workload of the Delphi bench (``tools/perf/delphi_warm_bench.py`` in the
main checkout, or next to this file) is delegated to it; N3 adds:

- ``fastest_dist_x4``: ``get_driver(d).get_fastest_lap().get_telemetry()
  .add_distance()`` for VER, LEC, NOR and PIA (the phase ``fastest_dist_x4``).
- ``analysis_x4``: the notebook's ``telemetry_analysis_4`` phase alone, after
  ``session.laps`` (four fastest laps: ``add_distance``,
  ``add_differential_distance``, ``resample_channels("0.1s")``,
  ``fill_missing``, ``slice_by_time``, ``merge_channels``).

Usage (the parent mode is the Delphi bench's): ``--child <workload> --lib polars``.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_MAIN_PERF = Path("/home/harish/Documents/tif1/tools/perf")
sys.path.insert(0, str(_HERE if (_HERE / "delphi_warm_bench.py").exists() else _MAIN_PERF))

import delphi_warm_bench as base  # noqa: E402

YEAR, GP = base.YEAR, base.GP
N3_WORKLOADS = ("fastest_dist_x4", "analysis_x4")


def _analysis(laps, lib: str) -> list:
    fastest = []
    drivers = laps["Driver"].unique()
    for d in sorted(drivers.tolist() if lib == "pandas" else drivers.to_list()):
        lap = laps.pick_driver(d).pick_fastest()
        if lap is not None:
            fastest.append(lap)
        if len(fastest) == 4:
            break
    tels = []
    for lap in fastest:
        tel = lap.get_telemetry().add_distance()
        tel = tel.add_differential_distance()
        tel.resample_channels(rule="0.1s")
        tel.fill_missing()
        if lib == "polars":
            tel.slice_by_time(tel["Time"][0], tel["Time"][len(tel) // 2])
        else:
            tel.slice_by_time(tel["Time"].iloc[0], tel["Time"].iloc[len(tel) // 2])
        tels.append(tel)
    tels[0].merge_channels(tels[1])
    return tels


def _child(workload: str, lib: str) -> None:
    if workload not in N3_WORKLOADS:
        base._child(workload, lib)
        return
    out: dict[str, float] = {}
    t_proc = time.perf_counter()

    def step(name, fn):
        t0 = time.perf_counter()
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        return r

    import tif1

    out["import"] = round((time.perf_counter() - t_proc) * 1000, 2)
    session = step("get_session", lambda: tif1.get_session(YEAR, GP, "Race", lib=lib))
    if workload == "fastest_dist_x4":
        step(
            "fastest_dist_x4",
            lambda: [
                session.get_driver(d).get_fastest_lap().get_telemetry().add_distance()
                for d in ("VER", "LEC", "NOR", "PIA")
            ],
        )
    else:
        laps = step("laps", lambda: session.laps)
        step("telemetry_analysis_4", lambda: _analysis(laps, lib))
    out["numpy_loaded"] = float("numpy" in sys.modules)
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    print(json.dumps(out))


if __name__ == "__main__":
    if "--child" in sys.argv:
        args = sys.argv
        _child(args[args.index("--child") + 1], args[args.index("--lib") + 1])
    else:
        base.main()
