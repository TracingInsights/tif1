"""Corinth C3: interleaved fresh-process A/B of ``tif1.ergast`` queries, cold or warm.

Each run is a fresh interpreter that imports ``tif1.ergast``, builds an
``Ergast`` (``result_type`` from ``--lib``) and answers one query; the parent
measures the whole-process wall, the child reports the import and query phases
and a digest of the answer (class, columns, dtypes, index, values, ``source``,
paging attributes). Control and candidate alternate (ABAB, the order flipped
every pair). ``--mode cold`` gives every run a new empty cache directory (live
CDN); ``--mode warm`` primes one cache directory per arm once (with that arm)
and runs offline. ``--pre-import pandas`` imports pandas (or polars) before
tif1, the notebook shape. Exit status 1 when a digest differs between arms.

Usage:
    python tools/perf/corinth_c3_ergast_ab.py --control-src /tmp/corinth/control/src \
        --candidate-src /tmp/corinth/C3/src --mode cold --lib pandas --pairs 5 \
        [--queries race_results_2024,...] [--out .agents/perf-corinth/C3_cold_pandas.json]
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

QUERIES = {
    "race_results_2024": "e.get_race_results(season=2024)",
    "qualifying_2023_r5": "e.get_qualifying_results(season=2023, round=5)",
    "sprint_2024": "e.get_sprint_results(season=2024)",
    "driver_standings_2024": "e.get_driver_standings(season=2024)",
    "constructor_standings_2025_r10": "e.get_constructor_standings(season=2025, round=10)",
    "schedule_2024": "e.get_race_schedule(season=2024)",
    "laps_2024_r1": "e.get_lap_times(season=2024, round=1)",
    "pitstops_2024_r1": "e.get_pit_stops(season=2024, round=1)",
    "driver_info_2024": "e.get_driver_info(season=2024)",
    "race_results_2026": "e.get_race_results(season=2026)",
}


def _frame_digest(frame) -> list:
    module = type(frame).__module__
    if hasattr(frame, "schema") and not hasattr(frame, "iloc"):  # polars
        return [
            module,
            type(frame).__name__,
            [(name, str(dtype)) for name, dtype in frame.schema.items()],
            repr(frame.rows()),
        ]
    return [
        module,
        type(frame).__name__,
        list(map(str, frame.columns)),
        [str(dtype) for dtype in frame.dtypes],
        repr(frame.index.tolist()),
        repr(frame.to_dict("split")["data"]),
    ]


def _answer_digest(answer) -> str:
    parts: list = [type(answer).__module__, type(answer).__name__]
    parts.extend(
        repr(getattr(answer, attr, None)) for attr in ("source", "total_results", "is_complete")
    )
    if hasattr(answer, "content") and hasattr(answer, "description"):
        parts.append(_frame_digest(answer.description))
        parts.extend(_frame_digest(frame) for frame in answer.content)
    elif isinstance(answer, list):
        parts.append(repr(list(answer)))
    else:
        parts.append(_frame_digest(answer))
    return hashlib.sha256(repr(parts).encode()).hexdigest()[:16]


def child(query: str, lib: str, pre_import: str) -> None:
    t0 = time.perf_counter()
    if pre_import == "pandas":
        import pandas as pd  # noqa: F401
    elif pre_import == "polars":
        import polars as pl  # noqa: F401
    t1 = time.perf_counter()
    from tif1.ergast import Ergast

    e = Ergast(result_type="polars") if lib == "polars" else Ergast()
    t2 = time.perf_counter()
    answer = eval(QUERIES[query], {"e": e})  # the fixed QUERIES table above
    t3 = time.perf_counter()
    print(
        json.dumps(
            {
                "pre_import": round((t1 - t0) * 1000, 2),
                "import": round((t2 - t1) * 1000, 2),
                "query": round((t3 - t2) * 1000, 2),
                "in_process_total": round((t3 - t0) * 1000, 2),
                "digest": _answer_digest(answer),
            }
        )
    )


def _run_child(
    query: str, lib: str, src: str, cache_dir: str, *, offline: bool, pre_import: str
) -> dict:
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir, PYTHONPATH=str(Path(src).resolve()))
    if offline:
        env["TIF1_OFFLINE_MODE"] = "true"
    else:
        env.pop("TIF1_OFFLINE_MODE", None)
    cmd = [sys.executable, __file__, "--child", query, "--lib", lib, "--pre-import", pre_import]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    if proc.returncode != 0:
        raise RuntimeError(f"{query} ({src}) failed: {proc.stderr[-3000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--control-src")
    parser.add_argument("--candidate-src")
    parser.add_argument("--mode", default="cold", choices=["cold", "warm"])
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--pre-import", default="none", choices=["none", "pandas", "polars"])
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--queries", default=",".join(QUERIES))
    parser.add_argument("--scratch", default=None, help="parent of the cache directories")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    if args.child:
        child(args.child, args.lib, args.pre_import)
        return

    scratch = Path(args.scratch or tempfile.mkdtemp(prefix="c3-ab-"))
    scratch.mkdir(parents=True, exist_ok=True)
    arms = {"control": args.control_src, "candidate": args.candidate_src}
    queries = args.queries.split(",")
    warm_dirs: dict[str, str] = {}
    if args.mode == "warm":
        for arm, src in arms.items():
            cache = scratch / f"warm-{arm}"
            shutil.rmtree(cache, ignore_errors=True)
            cache.mkdir()
            warm_dirs[arm] = str(cache)
            for query in queries:
                _run_child(query, args.lib, src, str(cache), offline=False, pre_import="none")

    summary: dict[str, dict] = {}
    parity_failures: list[str] = []
    for query in queries:
        recs: dict[str, list[dict]] = {"control": [], "candidate": []}
        for i in range(args.pairs):
            order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
            for arm in order:
                if args.mode == "cold":
                    cache = scratch / f"cold-{arm}"
                    shutil.rmtree(cache, ignore_errors=True)
                    cache.mkdir()
                    cache_dir = str(cache)
                else:
                    cache_dir = warm_dirs[arm]
                recs[arm].append(
                    _run_child(
                        query,
                        args.lib,
                        arms[arm],
                        cache_dir,
                        offline=args.mode == "warm",
                        pre_import=args.pre_import,
                    )
                )
        digests = {arm: sorted({r["digest"] for r in recs[arm]}) for arm in recs}
        if digests["control"] != digests["candidate"]:
            parity_failures.append(f"{query}: {digests}")
        phases: dict[str, dict] = {}
        for key in ("process_wall", "in_process_total", "import", "query"):
            c = [r[key] for r in recs["control"]]
            d = [r[key] for r in recs["candidate"]]
            wins = sum(1 for a, b in zip(c, d, strict=True) if b < a)
            phases[key] = {
                "control_median": round(statistics.median(c), 1),
                "candidate_median": round(statistics.median(d), 1),
                "control_p90": round(sorted(c)[max(0, int(0.9 * len(c)) - 1)], 1)
                if len(c) >= 2
                else c[0],
                "candidate_p90": round(sorted(d)[max(0, int(0.9 * len(d)) - 1)], 1)
                if len(d) >= 2
                else d[0],
                "candidate_faster_pairs": f"{wins}/{args.pairs}",
            }
        phases["digests"] = digests
        summary[query] = phases
        w, q = phases["process_wall"], phases["query"]
        print(
            f"[{args.mode} {args.lib} pre={args.pre_import}] {query:32s} wall "
            f"{w['control_median']:.0f} -> {w['candidate_median']:.0f} "
            f"({w['candidate_faster_pairs']})  query {q['control_median']:.0f} -> "
            f"{q['candidate_median']:.0f} ({q['candidate_faster_pairs']})  "
            f"parity {'ok' if digests['control'] == digests['candidate'] else 'DIFF'}",
            flush=True,
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {
                    "mode": args.mode,
                    "lib": args.lib,
                    "pre_import": args.pre_import,
                    "pairs": args.pairs,
                    "control_src": args.control_src,
                    "candidate_src": args.candidate_src,
                    "summary": summary,
                    "parity_failures": parity_failures,
                },
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )
    if parity_failures:
        print("PARITY FAILURES:\n  " + "\n  ".join(parity_failures))
        sys.exit(1)


if __name__ == "__main__":
    main()
