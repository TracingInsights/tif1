"""Gortyn G4: digest of every converted selection on the real 2025 Bahrain race and qualifying.

Run once per tree; ``--block-numpy`` makes numpy unimportable after the laps
load (candidate only: the control's masks need numpy). Prints a JSON object
``{surface: sha1}``; ``--compare a.json b.json`` diffs two of them.

Usage (from the repo root):
    TIF1_CACHE_DIR=/tmp/tif1-gortyn-warm-G4 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/gortyn_g4/real_parity.py [--block-numpy] > out.json
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def _compare(a: str, b: str) -> None:
    left = json.loads(Path(a).read_text(encoding="utf-8"))
    right = json.loads(Path(b).read_text(encoding="utf-8"))
    print("numpy loaded:", left.pop("numpy_loaded", None), right.pop("numpy_loaded", None))
    raised = sum(str(v).startswith("raised") for v in left.values())
    differ = sorted(k for k in left.keys() | right.keys() if left.get(k) != right.get(k))
    for key in differ:
        print("DIFF", key, left.get(key), right.get(key))
    total = len(left.keys() | right.keys())
    print(f"{len(differ)} differing surfaces out of {total} ({raised} raise in both)")


def _digest(value) -> str:
    if value is None:
        text = "None"
    elif hasattr(value, "schema") and hasattr(value, "rows"):
        text = repr((type(value).__name__, list(value.schema.items()), value.rows()))
    elif hasattr(value, "frame"):  # PolarsLap / PolarsDriverResult
        frame = value.frame
        text = repr((type(value).__name__, value.name, list(frame.schema.items()), frame.rows()))
    else:
        text = repr(value)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _cases(session, laps, table) -> dict:
    """Every converted selection of one session, as zero-argument callables."""
    codes = laps["Driver"].unique(maintain_order=True).to_list()
    teams = laps["Team"].unique(maintain_order=True).to_list()
    cases = {
        "pick_drivers_2": lambda: laps.pick_drivers(["VER", "lec"]),
        "pick_drivers_6": lambda: laps.pick_drivers([*codes[:5], "44"]),
        "pick_drivers_numbers": lambda: laps.pick_drivers([1, "16", "81"]),
        "pick_quicklaps": lambda: laps.pick_quicklaps(),
        "pick_quicklaps_1": lambda: laps.pick_quicklaps(1.0),
        "pick_quicklaps_105": lambda: laps.pick_quicklaps(1.05),
        "pick_fastest": lambda: laps.pick_fastest(),
        "pick_fastest_time": lambda: laps.pick_fastest(only_by_time=True),
        "pick_laps_2": lambda: laps.pick_laps([5, 6]),
        "pick_laps_10": lambda: laps.pick_laps(range(1, 11)),
        "pick_laps_slice": lambda: laps.pick_laps(slice(3, 9)),
        "pick_lap": lambda: laps.pick_lap(5),
        "pick_compounds": lambda: laps.pick_compounds(["SOFT", "hard"]),
        "pick_tyre": lambda: laps.pick_tyre("MEDIUM"),
        "pick_teams": lambda: laps.pick_teams(teams[:3]),
        "pick_wo_box": lambda: laps.pick_wo_box(),
        "pick_box_laps": lambda: laps.pick_box_laps(),
        "pick_box_in": lambda: laps.pick_box_laps("in"),
        "pick_box_out": lambda: laps.pick_box_laps("out"),
        "pick_not_deleted": lambda: laps.pick_not_deleted(),
        "pick_accurate": lambda: laps.pick_accurate(),
        "iterlaps_require": lambda: [
            (i, lap.frame.rows()) for i, lap in laps.iterlaps(require=["LapTime", "PitOutTime"])
        ],
        "iterlaps_require_sectors": lambda: [
            i for i, _ in laps.iterlaps(require=["Sector1Time", "Sector2Time", "Sector3Time"])
        ],
        "results_get_driver": lambda: [table.get_driver(c).name for c in codes],
    }
    for team in teams:
        cases[f"pick_team[{team}]"] = lambda team=team: laps.pick_team(team)
    for status in ("1", "2", "4", "6", "12"):
        for how in ("equals", "contains", "excludes", "any", "none"):
            cases[f"pick_track_status[{status},{how}]"] = lambda status=status, how=how: (
                laps.pick_track_status(status, how)
            )
    for code in codes:
        cases[f"pick_driver[{code}].pick_fastest"] = lambda code=code: laps.pick_driver(
            code
        ).pick_fastest()
        cases[f"get_driver[{code}].get_fastest_lap"] = lambda code=code: session.get_driver(
            code
        ).get_fastest_lap()
        cases[f"get_driver[{code}].get_lap(2)"] = lambda code=code: session.get_driver(
            code
        ).get_lap(2)
    return cases


def main() -> None:
    if "--compare" in sys.argv:
        i = sys.argv.index("--compare")
        _compare(sys.argv[i + 1], sys.argv[i + 2])
        return
    import tif1

    out: dict[str, str] = {}
    sessions = {
        name: tif1.get_session(2025, "Bahrain Grand Prix", name, lib="polars")
        for name in ("Race", "Qualifying")
    }
    all_laps = {name: s.laps for name, s in sessions.items()}
    results = {name: s.results for name, s in sessions.items()}
    if "--block-numpy" in sys.argv:
        sys.modules["numpy"] = None  # type: ignore[assignment]
    for name, laps in all_laps.items():
        for key, fn in _cases(sessions[name], laps, results[name]).items():
            try:
                out[f"{name}/{key}"] = _digest(fn())
            except Exception as error:
                out[f"{name}/{key}"] = f"raised {type(error).__name__}"
    out["numpy_loaded"] = str(sys.modules.get("numpy") is not None)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
