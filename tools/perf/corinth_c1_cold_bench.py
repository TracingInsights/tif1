"""Corinth C1: cold season-loop benchmark (fresh process, empty cache, live CDN).

A copy of ``delphi_cold_bench.py`` with the season-loop workloads of the
Corinth survey. Each run is a fresh interpreter with a throwaway cache
directory; phases are timed in-process and the parent measures the
whole-process wall time. With ``--control-src`` the parent interleaves
control and candidate (ABAB...) so CDN edge drift hits both arms. Every
season workload also prints a digest of each race's frames (computed after
the timed phases): the parent compares the digests of the two arms.

Workloads:
    season          get_event_schedule(2025), then get_session(2025, r, "R")
                    .laps and .results for every round (the survey loop)
    season_load     the same with ``session.load()`` first (fastf1's loop),
                    over the first ``--rounds`` rounds
    season_laps     ``.laps`` only (no results), every round
    season_tel      ``.laps``, ``get_fastest_lap_tel()`` and ``.results`` per
                    round (a foreground telemetry fetch beside the lookahead)
    laps, laps_results, fastest_lap_tel   one cold race (2025 Bahrain)

Usage:
    python tools/perf/corinth_c1_cold_bench.py --lib pandas --reps 5 \
        --control-src /tmp/corinth/control/src --candidate-src src \
        --workloads season --out FILE
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
WORKLOADS = (
    "season",
    "season_load",
    "season_laps",
    "season_tel",
    "laps",
    "laps_results",
    "fastest_lap_tel",
)


def _frame_digest(frame, lib: str) -> str:
    """Columns, dtypes, index and a hash of every row (cheap: it runs inside the wall)."""
    if frame is None:
        return "none"
    if lib == "pandas":
        import pandas as pd

        text = repr(list(frame.columns)) + repr(list(frame.dtypes.astype(str)))
        text += repr(list(frame.index))
        try:
            text += pd.util.hash_pandas_object(frame, index=True).to_numpy().tobytes().hex()
        except TypeError:  # unhashable cells
            text += frame.to_csv()
    else:
        text = repr(frame.columns) + repr([str(d) for d in frame.dtypes])
        text += repr(frame.hash_rows(seed=1).to_list())
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _child(workload: str, lib: str, rounds_limit: int) -> None:
    out: dict = {}
    t_proc = time.perf_counter()

    def step(name, fn):
        t0 = time.perf_counter()
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        return r

    import tif1

    out["import"] = round((time.perf_counter() - t_proc) * 1000, 2)
    if workload.startswith("season"):
        sched = step("schedule", lambda: tif1.get_event_schedule(YEAR))
        column = sched["RoundNumber"]
        values = column.to_list() if lib == "polars" else list(column)
        rounds = [int(r) for r in values if int(r) > 0]
        if rounds_limit:
            rounds = rounds[:rounds_limit]
        kept = []
        errors = []

        def loop():
            for r in rounds:
                try:
                    s = tif1.get_session(YEAR, r, "R", lib=lib)
                    if workload == "season_load":
                        s.load()
                    laps = s.laps
                    if workload == "season_tel":
                        s.get_fastest_lap_tel()
                    results = None if workload == "season_laps" else s.results
                    kept.append((r, laps, results))
                except Exception as e:
                    errors.append(f"{r}:{type(e).__name__}")

        step("loop", loop)
        out["rounds"] = len(rounds)
        out["errors"] = len(errors)
        out["until_loop_end"] = round((time.perf_counter() - t_proc) * 1000, 2)
        digests = step(
            "digest",
            lambda: {
                str(r): [_frame_digest(laps, lib), _frame_digest(results, lib)]
                for r, laps, results in kept
            },
        )
        out["digest"] = hashlib.sha256(json.dumps(digests).encode()).hexdigest()[:16]
        out["digests"] = digests
    else:
        session = step("get_session", lambda: tif1.get_session(YEAR, GP, SESSION, lib=lib))
        if workload == "laps":
            step("laps", lambda: session.laps)
        elif workload == "laps_results":
            step("laps", lambda: session.laps)
            step("results", lambda: session.results)
        elif workload == "fastest_lap_tel":
            step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    print(json.dumps(out))


def _run_child(workload: str, lib: str, src_dir: str | None, rounds: int) -> dict:
    cache_dir = tempfile.mkdtemp(prefix="tif1-c1-cold-")
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir)
    env.pop("TIF1_OFFLINE_MODE", None)
    if src_dir:
        env["PYTHONPATH"] = str(Path(src_dir).resolve())
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        workload,
        "--lib",
        lib,
        "--rounds",
        str(rounds),
    ]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    shutil.rmtree(cache_dir, ignore_errors=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    rec["warnings_403"] = proc.stderr.count("403")
    return rec


def _summarize(recs: list[dict]) -> dict:
    phases = {}
    for key, first in recs[0].items():
        if not isinstance(first, int | float):
            continue
        vals = sorted(r[key] for r in recs)
        p90 = vals[min(len(vals) - 1, round(0.9 * (len(vals) - 1)))]
        phases[key] = {
            "median": round(statistics.median(vals), 1),
            "min": round(min(vals), 1),
            "p90": round(p90, 1),
        }
    return phases


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--rounds", type=int, default=0, help="season rounds (0: all)")
    parser.add_argument("--control-src", default=None)
    parser.add_argument("--candidate-src", default=None)
    parser.add_argument("--workloads", default="season")
    parser.add_argument("--out", default=None)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib, args.rounds)
        return
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        ctl: list[dict] = []
        cand: list[dict] = []
        for i in range(args.reps):
            order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
            for arm in order:
                src = args.control_src if arm == "control" else args.candidate_src
                rec = _run_child(workload, args.lib, src, args.rounds)
                (ctl if arm == "control" else cand).append(rec)
                print(
                    f"  {workload} rep {i} {arm:9s} wall {rec['process_wall']:.0f}"
                    + (f" loop {rec['loop']:.0f}" if "loop" in rec else "")
                    + (f" errors {rec['errors']}" if rec.get("errors") else ""),
                    flush=True,
                )
        wins = sum(
            1 for a, b in zip(ctl, cand, strict=True) if b["process_wall"] < a["process_wall"]
        )
        digests = sorted({r["digest"] for r in ctl + cand if "digest" in r})
        summary[workload] = {
            "control": _summarize(ctl),
            "candidate": _summarize(cand),
            "candidate_faster_pairs": f"{wins}/{args.reps}",
            "digests": digests,
            "runs": {"control": ctl, "candidate": cand},
        }
        c = summary[workload]["control"]["process_wall"]
        d = summary[workload]["candidate"]["process_wall"]
        print(
            f"[{args.lib}] {workload:16s} control {c['median']:.0f} (p90 {c['p90']:.0f})"
            f"  candidate {d['median']:.0f} (p90 {d['p90']:.0f})  faster {wins}/{args.reps}"
            + (f"  digests {digests}" if digests else ""),
            flush=True,
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {"lib": args.lib, "label": args.label, "reps": args.reps, "summary": summary},
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
