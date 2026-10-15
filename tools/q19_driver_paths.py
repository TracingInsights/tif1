"""Fresh-process timing + parity dump of the Driver paths and classification (Pergamon Q19).

One run = one fresh process on a warm cache: times the ``Driver`` entry
points as a chart script hits them right after ``get_session`` and the
classification-ordered tables on a second ``Session`` of the same event,
then (``--dump``) pickles every returned frame for a parity comparison.

Usage:
    python tools/q19_driver_paths.py --src-dir SRC --cache-dir DIR [--dump out.pkl]
    python tools/q19_driver_paths.py --compare a.pkl b.pkl
    python tools/q19_driver_paths.py --ab --control-src SRC --cache-dir DIR --pairs 8
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import statistics
import subprocess
import sys
import time
from pathlib import Path

EVENT = (2026, "Monaco Grand Prix", "Race")
DRIVER = "LEC"
OTHER = "HAM"


def _frame_info(frame) -> dict:
    import pandas as pd

    session = getattr(frame, "session", None)
    return {
        "frame": pd.DataFrame(frame, copy=False),
        "class": type(frame).__name__,
        "session_bound": session is not None,
    }


def run_once(src_dir: str, cache_dir: str, dump: str | None) -> dict:
    sys.path.insert(0, str(Path(src_dir).resolve()))
    os.environ["TIF1_CACHE_DIR"] = cache_dir
    t0 = time.perf_counter()
    import tif1

    t_import = time.perf_counter() - t0
    timings: dict[str, float] = {"import": t_import}

    def timed(name, fn):
        start = time.perf_counter()
        result = fn()
        timings[name] = time.perf_counter() - start
        return result

    s = timed("get_session", lambda: tif1.get_session(*EVENT))
    drv = timed("get_driver_first", lambda: s.get_driver(DRIVER))
    laps = timed("driver_laps_first", lambda: drv.laps)
    fastest = timed("get_fastest_lap", drv.get_fastest_lap)
    lap = timed("get_lap", lambda: drv.get_lap(30))
    tel = timed("get_fastest_lap_tel", drv.get_fastest_lap_tel)
    drv2 = timed("get_driver_second", lambda: s.get_driver(OTHER))
    laps2 = timed("driver_laps_second", lambda: drv2.laps)
    # Chart idiom: a fresh Session per chart, first call get_driver().get_fastest_lap().
    s2 = tif1.get_session(*EVENT)
    fastest2 = timed("chart_get_fastest_lap", lambda: s2.get_driver(DRIVER).get_fastest_lap())
    # Classification on yet another Session (charts call get_session per chart).
    s3 = tif1.get_session(*EVENT)
    drivers = timed("drivers", lambda: s3.drivers)
    drivers_df = timed("drivers_df", lambda: s3.drivers_df)
    results = timed("results", lambda: s3.results)
    timings["driver_flow_total"] = sum(
        timings[k]
        for k in (
            "get_driver_first",
            "driver_laps_first",
            "get_fastest_lap",
            "get_lap",
            "get_fastest_lap_tel",
            "get_driver_second",
            "driver_laps_second",
        )
    )
    if dump:
        s4 = tif1.get_session(*EVENT)
        all_laps = {}
        for code in sorted({str(x) for x in s.laps["Driver"].dropna().unique()}):
            d = s4.get_driver(code)
            all_laps[code] = {
                "laps": _frame_info(d.laps),
                "fastest": _frame_info(d.get_fastest_lap()),
                "laps_session_is_s4": getattr(d.laps, "session", None) is s4,
            }
        payload = {
            "laps": _frame_info(laps),
            "laps_session_is_s": getattr(laps, "session", None) is s,
            "fastest": _frame_info(fastest),
            "lap": {
                "series": __import__("pandas").Series(lap, copy=False),
                "class": type(lap).__name__,
            },
            "tel": _frame_info(tel),
            "laps2": _frame_info(laps2),
            "fastest2": _frame_info(fastest2),
            "drivers": drivers,
            "drivers_df": _frame_info(drivers_df),
            "results": _frame_info(results),
            "all": all_laps,
        }
        with open(dump, "wb") as fh:
            pickle.dump(payload, fh)
    return timings


def _same_frame(a, b) -> list[str]:
    problems = []
    if list(a.columns) != list(b.columns):
        problems.append("columns")
        return problems
    if a.columns.dtype != b.columns.dtype or type(a.columns) is not type(b.columns):
        problems.append("columns index dtype")
    if not a.dtypes.equals(b.dtypes):
        problems.append("dtypes")
    if not a.index.equals(b.index) or type(a.index) is not type(b.index):
        problems.append("index")
    if not a.equals(b):
        problems.append("values")
    for col in a.columns:
        if isinstance(a[col].dtype, __import__("pandas").CategoricalDtype):
            if a[col].cat.codes.dtype != b[col].cat.codes.dtype:
                problems.append(f"codes dtype {col}")
    sig_a = sorted((str(x.dtype), tuple(x.mgr_locs.as_array.tolist())) for x in a._mgr.blocks)
    sig_b = sorted((str(x.dtype), tuple(x.mgr_locs.as_array.tolist())) for x in b._mgr.blocks)
    if sig_a != sig_b:
        problems.append("blocks")
    # Missing-value representation in object columns (equals treats None == NaN).
    for col in a.columns:
        if a[col].dtype == object:
            ra = [type(v).__name__ for v in a[col].tolist()]
            rb = [type(v).__name__ for v in b[col].tolist()]
            if ra != rb:
                problems.append(f"object element types {col}")
    return problems


def compare(path_a: str, path_b: str) -> int:
    with open(path_a, "rb") as fh:
        a = pickle.load(fh)
    with open(path_b, "rb") as fh:
        b = pickle.load(fh)
    issues: list[str] = []

    def frame_entry(name, ea, eb):
        if ea["class"] != eb["class"] or ea["session_bound"] != eb["session_bound"]:
            issues.append(f"{name}: class/session {ea['class']}/{eb['class']}")
        issues.extend(f"{name}: {p}" for p in _same_frame(ea["frame"], eb["frame"]))

    for key in ("laps", "fastest", "tel", "laps2", "fastest2"):
        frame_entry(key, a[key], b[key])
    if a["laps_session_is_s"] != b["laps_session_is_s"]:
        issues.append("laps session binding")
    la, lb = a["lap"]["series"], b["lap"]["series"]
    if a["lap"]["class"] != b["lap"]["class"] or not la.equals(lb) or la.dtype != lb.dtype:
        issues.append("get_lap")
    if list(la.index) != list(lb.index) or la.name != lb.name:
        issues.append("get_lap index/name")
    if a["drivers"] != b["drivers"]:
        issues.append("drivers")
    frame_entry("drivers_df", a["drivers_df"], b["drivers_df"])
    frame_entry("results", a["results"], b["results"])
    if sorted(a["all"]) != sorted(b["all"]):
        issues.append("driver set")
    for code in a["all"]:
        frame_entry(f"all[{code}].laps", a["all"][code]["laps"], b["all"][code]["laps"])
        frame_entry(f"all[{code}].fastest", a["all"][code]["fastest"], b["all"][code]["fastest"])
        if a["all"][code]["laps_session_is_s4"] != b["all"][code]["laps_session_is_s4"]:
            issues.append(f"all[{code}] session binding")
    print(json.dumps({"drivers_compared": len(a["all"]), "issues": issues}, indent=1))
    return 1 if issues else 0


def ab(control_src: str, candidate_src: str, cache_dir: str, pairs: int, out: str | None) -> None:
    runs: list[dict] = []
    for i in range(pairs):
        order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
        pair = {}
        for arm in order:
            src = control_src if arm == "control" else candidate_src
            proc = subprocess.run(
                [sys.executable, "-I", __file__, "--src-dir", src, "--cache-dir", cache_dir],
                check=True,
                capture_output=True,
                text=True,
            )
            pair[arm] = json.loads(proc.stdout.strip().splitlines()[-1])
        runs.append(pair)
        print(
            f"pair {i + 1}: flow ctl {pair['control']['driver_flow_total'] * 1e3:.1f} ms "
            f"cand {pair['candidate']['driver_flow_total'] * 1e3:.1f} ms",
            flush=True,
        )
    summary = {}
    for key in runs[0]["control"]:
        ctl = [r["control"][key] * 1e3 for r in runs]
        cand = [r["candidate"][key] * 1e3 for r in runs]
        wins = sum(1 for r in runs if r["candidate"][key] < r["control"][key])
        summary[key] = {
            "control_median_ms": round(statistics.median(ctl), 3),
            "candidate_median_ms": round(statistics.median(cand), 3),
            "candidate_faster": f"{wins}/{len(runs)}",
        }
        print(
            f"{key:24s} control {summary[key]['control_median_ms']:9.3f}  "
            f"candidate {summary[key]['candidate_median_ms']:9.3f}  "
            f"faster {summary[key]['candidate_faster']}"
        )
    if out:
        Path(out).write_text(json.dumps({"runs": runs, "summary": summary}, indent=1) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-dir")
    parser.add_argument("--cache-dir")
    parser.add_argument("--dump")
    parser.add_argument("--compare", nargs=2)
    parser.add_argument("--ab", action="store_true")
    parser.add_argument("--control-src")
    parser.add_argument("--candidate-src", default=str(Path(__file__).resolve().parents[1] / "src"))
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--out")
    args = parser.parse_args()
    if args.compare:
        sys.exit(compare(*args.compare))
    if args.ab:
        ab(args.control_src, args.candidate_src, args.cache_dir, args.pairs, args.out)
        return
    timings = run_once(args.src_dir, args.cache_dir, args.dump)
    print(json.dumps(timings))


if __name__ == "__main__":
    main()
