"""Cold no-cache benchmark: fetch the ENTIRE telemetry of one session over the live CDN.

Default target: 2026 Monaco Grand Prix Race (the dedicated benchmark for the
performance-experiment series; see .agents/perf-monaco/RESULTS.md).

One "run" is a fresh Python process with a fresh throwaway cache directory
(``TIF1_CACHE_DIR``), so every payload is fetched cold from the CDN — no
memory/SQLite cache hits. Phases are timed separately with
``time.perf_counter``:

- ``import_s``      — ``import tif1``
- ``get_session_s`` — schedule fetch + session construction
- ``laps_s``        — ``session.laps`` (drivers + session laptimes)
- ``telemetry_s``   — ``session.fetch_all_laps_telemetry()`` (every lap, every driver)
- ``total_s``       — get_session + laps + telemetry (the cold fetch pipeline)

Usage:
    Single run (prints one JSON object):
        uv run python tools/monaco_telemetry_cold_benchmark.py

    Suite of N runs (fresh process per run), aggregated and written to --out:
        uv run python tools/monaco_telemetry_cold_benchmark.py \
            --runs 5 --label baseline --out .agents/perf-monaco/baseline.json

Runs whose telemetry completeness ratio drops below 0.9 (transient CDN flake)
are recorded as failures and replaced with fresh runs (at most ``--max-retries``
replacements).
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

MIN_COMPLETENESS = 0.9


def single_run(args: argparse.Namespace) -> dict:
    """One cold run in this process. Prints one JSON object to stdout."""
    if args.src_dir:
        # Shadow the installed tif1 with an alternative source tree (A/B runs).
        sys.path.insert(0, str(Path(args.src_dir).resolve()))

    cache_dir = tempfile.mkdtemp(prefix="tif1-bench-cache-")
    os.environ["TIF1_CACHE_DIR"] = cache_dir

    t0 = time.perf_counter()
    import tif1

    t_import = time.perf_counter() - t0

    t0 = time.perf_counter()
    session = tif1.get_session(args.year, args.gp, args.session)
    t_get = time.perf_counter() - t0

    t0 = time.perf_counter()
    laps = session.laps
    t_laps = time.perf_counter() - t0

    t0 = time.perf_counter()
    tel = session.fetch_all_laps_telemetry()
    t_tel = time.perf_counter() - t0

    # Completeness: same (driver, lap) refs the fetch used.
    expected = 0
    for _, row in laps.iterrows():
        driver = row.get("Driver")
        lap_num = row.get("LapNumber")
        if driver is not None and lap_num is not None:
            expected += 1

    result = {
        "import_s": round(t_import, 4),
        "get_session_s": round(t_get, 4),
        "laps_s": round(t_laps, 4),
        "telemetry_s": round(t_tel, 4),
        "total_s": round(t_get + t_laps + t_tel, 4),
        "n_laps": len(laps),
        "expected_refs": expected,
        "n_telemetry": len(tel),
        "completeness": round(len(tel) / expected, 4) if expected else 0.0,
        "telemetry_rows": sum(len(df) for df in tel.values()),
        "cache_dir": cache_dir,
        "python": sys.version.split()[0],
    }
    print(json.dumps(result))


def _spawn_run(args: argparse.Namespace) -> dict:
    """Spawn a fresh child process for one cold run; return its parsed result."""
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--year",
        str(args.year),
        "--gp",
        args.gp,
        "--session",
        args.session,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"benchmark child failed (rc={proc.returncode}): {proc.stderr[-2000:]}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def suite(args: argparse.Namespace) -> None:
    """Run N cold runs in fresh processes; aggregate; write JSON to --out."""
    import datetime as _dt

    runs: list[dict] = []
    failures: list[dict] = []
    attempts = 0
    while len(runs) < args.runs and attempts < args.runs + args.max_retries:
        attempts += 1
        try:
            run = _spawn_run(args)
            cache_dir = run.pop("cache_dir", None)
            if run["completeness"] < MIN_COMPLETENESS:
                failures.append({"attempt": attempts, "reason": "incomplete", **run})
                print(
                    f"run {attempts}: incomplete (completeness={run['completeness']}), replacing",
                    file=sys.stderr,
                )
            else:
                runs.append(run)
                print(
                    f"run {attempts}: total={run['total_s']}s "
                    f"telemetry={run['telemetry_s']}s n_tel={run['n_telemetry']}",
                    file=sys.stderr,
                )
            if cache_dir and not args.keep_cache:
                import shutil

                shutil.rmtree(cache_dir, ignore_errors=True)
        except (RuntimeError, json.JSONDecodeError, KeyError) as e:
            failures.append({"attempt": attempts, "reason": str(e)})
            print(f"run {attempts}: FAILED ({e}), replacing", file=sys.stderr)

    if len(runs) < args.runs:
        raise SystemExit(
            f"only {len(runs)}/{args.runs} valid runs after {attempts} attempts; aborting"
        )

    def stats(key: str) -> dict:
        vals = [r[key] for r in runs]
        return {
            "mean": round(statistics.mean(vals), 4),
            "median": round(statistics.median(vals), 4),
            "min": round(min(vals), 4),
            "max": round(max(vals), 4),
            "stdev": round(statistics.stdev(vals), 4) if len(vals) > 1 else 0.0,
            "values": vals,
        }

    out = {
        "label": args.label,
        "timestamp": _dt.datetime.now(_dt.UTC).isoformat(),
        "runs": runs,
        "failures": failures,
        "stats": {key: stats(key) for key in ("total_s", "telemetry_s", "laps_s", "get_session_s")},
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    print(f"wrote {out_path}")
    print(f"label={args.label} total_s median={out['stats']['total_s']['median']}")
    print(f"label={args.label} telemetry_s median={out['stats']['telemetry_s']['median']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--runs", type=int, default=0, help="suite mode: number of cold runs")
    parser.add_argument("--max-retries", type=int, default=3, help="max replacement runs")
    parser.add_argument("--label", default="unnamed")
    parser.add_argument("--out", default=".agents/perf-monaco/run.json")
    parser.add_argument(
        "--keep-cache", action="store_true", help="keep throwaway cache dirs after runs"
    )
    parser.add_argument(
        "--src-dir", default=None, help="alternative tif1 source tree (prepended to sys.path)"
    )
    args = parser.parse_args()
    if args.runs > 0:
        suite(args)
    else:
        single_run(args)


if __name__ == "__main__":
    main()
