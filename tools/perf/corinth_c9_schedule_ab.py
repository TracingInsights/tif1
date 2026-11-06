"""Corinth C9: interleaved fresh-process A/B of the schedule APIs (control vs candidate).

A copy of ``delphi_warm_ab.py``'s protocol (ABAB, the order flipped every
pair, whole-process wall time plus in-process phases, medians and pairs won)
over schedule workloads the Delphi bench does not have:

- ``schedule_pandas``: ``get_event_schedule(2025)`` (fastf1's ``EventSchedule``).
- ``schedule_polars``: ``get_event_schedule(2025, lib="polars")``; the control
  has no ``lib=``, so its arm builds the pandas schedule, what a polars
  script had to do.
- ``remaining_pandas`` / ``remaining_polars``: ``get_events_remaining`` at
  2025-07-01, same split.
- ``get_event``: ``get_event(2025, "Monza")`` then ``get_event(2025, 12)``.
- ``season_polars`` / ``season_pandas``: the survey's season loop: the 2025
  schedule (polars where available), then ``get_session(2025, r, "R").laps``
  and ``.results`` for every race round (run it on a copy of the season cache).
- ``current_schedule``: ``get_event_schedule(<this year>)`` (polars where
  available); with ``--cold`` every run gets an empty cache and goes online.
- ``<workload>_preimported``: ``import pandas`` as its own phase first, so the
  next phase is the schedule construction alone.

Usage:
    python tools/perf/corinth_c9_schedule_ab.py --control-src /tmp/corinth/control/src \
        --candidate-src src --cache /tmp/corinth/warm-C9 --pairs 10 \
        --workloads schedule_pandas,schedule_polars [--cold] [--out FILE]
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

WORKLOADS = (
    "schedule_pandas",
    "schedule_polars",
    "remaining_pandas",
    "remaining_polars",
    "get_event",
    "season_polars",
    "season_pandas",
    "current_schedule",
)


def _schedule(tif1, year: int, polars: bool, **kwargs):
    if polars:
        try:
            return tif1.get_event_schedule(year, lib="polars", **kwargs), "polars"
        except TypeError:  # the control tree has no ``lib=``
            pass
    return tif1.get_event_schedule(year, **kwargs), "pandas"


def _child(workload: str) -> None:
    out: dict = {}
    t_proc = time.perf_counter()

    def step(name, fn):
        t0 = time.perf_counter()
        r = fn()
        out[name] = round((time.perf_counter() - t0) * 1000, 2)
        return r

    import tif1

    out["import"] = round((time.perf_counter() - t_proc) * 1000, 2)
    if workload.endswith("_preimported"):
        # ``import pandas as pd`` at the top of a script: the phase after it
        # is the schedule construction alone.
        step("import_pandas", lambda: __import__("pandas"))
        workload = workload.removesuffix("_preimported")
    polars = workload.endswith("_polars") or workload == "current_schedule"
    if workload in ("schedule_pandas", "schedule_polars"):
        _, kind = step("schedule", lambda: _schedule(tif1, 2025, polars))
        out["kind"] = kind
    elif workload in ("remaining_pandas", "remaining_polars"):
        from datetime import UTC, datetime

        dt = datetime(2025, 7, 1, tzinfo=UTC).replace(tzinfo=None)

        def remaining():
            if polars:
                try:
                    return tif1.get_events_remaining(dt, lib="polars")
                except TypeError:
                    pass
            return tif1.get_events_remaining(dt)

        step("remaining", remaining)
    elif workload == "get_event":
        step("get_event_name", lambda: tif1.get_event(2025, "Monza"))
        step("get_event_round", lambda: tif1.get_event(2025, 12))
    elif workload in ("season_polars", "season_pandas"):
        lib = "polars" if polars else "pandas"
        schedule, kind = step("schedule", lambda: _schedule(tif1, 2025, polars))
        out["kind"] = kind
        rounds = [int(r) for r in list(schedule["RoundNumber"]) if int(r) > 0]

        def loop():
            n = 0
            for r in rounds:
                session = tif1.get_session(2025, r, "R", lib=lib)
                n += len(session.laps)
                _ = session.results
            return n

        out["laps_rows"] = step("loop_laps_results", loop)
        out["rounds"] = len(rounds)
    elif workload == "current_schedule":
        from datetime import UTC, datetime

        _, kind = step("schedule", lambda: _schedule(tif1, datetime.now(UTC).year, True))
        out["kind"] = kind
    else:
        raise SystemExit(f"unknown workload {workload}")
    out["pandas_imported"] = "pandas" in sys.modules
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    print(json.dumps(out))


def _run_child(workload: str, src_dir: str, cache_dir: str | None, cold: bool) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(src_dir).resolve())
    tmp = None
    if cold:
        tmp = tempfile.TemporaryDirectory(prefix="c9-cold-")
        env["TIF1_CACHE_DIR"] = tmp.name
        env.pop("TIF1_OFFLINE_MODE", None)
    else:
        env["TIF1_CACHE_DIR"] = str(cache_dir)
        env["TIF1_OFFLINE_MODE"] = "true"
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", workload]
    try:
        t0 = time.perf_counter()
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
        wall = (time.perf_counter() - t0) * 1000
    finally:
        if tmp is not None:
            tmp.cleanup()
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} ({src_dir}) failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--control-src")
    parser.add_argument("--candidate-src")
    parser.add_argument("--cache", help="primed cache both arms read (warm runs)")
    parser.add_argument("--cold", action="store_true", help="empty cache per run, online")
    parser.add_argument("--pairs", type=int, default=10)
    parser.add_argument("--workloads", default="schedule_pandas,schedule_polars")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    if args.child:
        _child(args.child)
        return
    arms = {"control": args.control_src, "candidate": args.candidate_src}
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        recs: dict[str, list[dict]] = {"control": [], "candidate": []}
        for i in range(args.pairs):
            order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
            for arm in order:
                recs[arm].append(_run_child(workload, arms[arm], args.cache, args.cold))
        phases: dict[str, dict] = {}
        for key, first in recs["control"][0].items():
            if key not in recs["candidate"][0] or not isinstance(first, int | float):
                continue
            if isinstance(first, bool):
                continue
            c = [r[key] for r in recs["control"]]
            d = [r[key] for r in recs["candidate"]]
            wins = sum(1 for a, b in zip(c, d, strict=True) if b < a)
            phases[key] = {
                "control_median": round(statistics.median(c), 1),
                "candidate_median": round(statistics.median(d), 1),
                "control_p90": round(sorted(c)[min(len(c) - 1, int(0.9 * len(c)))], 1),
                "candidate_p90": round(sorted(d)[min(len(d) - 1, int(0.9 * len(d)))], 1),
                "control_min": round(min(c), 1),
                "candidate_min": round(min(d), 1),
                "candidate_faster_pairs": f"{wins}/{args.pairs}",
            }
        info = {
            arm: {
                k: sorted({str(r.get(k)) for r in recs[arm]})
                for k in ("kind", "pandas_imported", "laps_rows", "rounds")
                if k in recs[arm][0]
            }
            for arm in recs
        }
        summary[workload] = {"phases": phases, "info": info}
        w = phases["process_wall"]
        print(
            f"{workload:18s} wall control {w['control_median']:.0f} (p90 {w['control_p90']:.0f})"
            f"  candidate {w['candidate_median']:.0f} (p90 {w['candidate_p90']:.0f})"
            f"  faster {w['candidate_faster_pairs']}  | "
            + "  ".join(
                f"{k}={v['control_median']:.1f}->{v['candidate_median']:.1f}"
                for k, v in phases.items()
                if k not in ("process_wall", "in_process_total", "laps_rows", "rounds")
            )
            + f"  {info}",
            flush=True,
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {
                    "pairs": args.pairs,
                    "cold": args.cold,
                    "control_src": args.control_src,
                    "candidate_src": args.candidate_src,
                    "cache": args.cache,
                    "summary": summary,
                },
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
