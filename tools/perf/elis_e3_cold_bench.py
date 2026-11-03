"""Elis E3: cold (empty cache, live CDN) A/B with a "second process" candidate arm.

A copy of ``delphi_cold_bench.py`` extended for E3 (persisted OCSP verdicts).
Every rep runs three fresh interpreters, in rotating order:

- ``control``: the control tree, empty cache.
- ``first``: the candidate tree, empty cache (today's first-ever process; it
  writes the OCSP row).
- ``second``: the candidate tree on a cache that holds nothing but the OCSP
  row the previous ``first`` run wrote: a returning user's machine meeting an
  event it has not cached yet (every payload is fetched cold).

The ``second`` cache is the ``first`` run's SQLite file with every row of
every table deleted except the OCSP row (schema and ``user_version`` kept).

Usage (from the repository root):
    python tools/perf/elis_e3_cold_bench.py --lib pandas --reps 5 \
        --control-src /tmp/ctl/src --candidate-src src --workloads laps,fastest_lap_tel \
        [--out FILE]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

YEAR, GP, SESSION = 2025, "Bahrain Grand Prix", "Race"
WORKLOADS = ("laps", "fastest_lap_tel", "driver_tel", "notebook", "all_tel")
OCSP_KEY = "tif1/ocsp_verdicts"
ARMS = ("control", "first", "second")


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
    if workload == "laps":
        step("laps", lambda: session.laps)
    elif workload == "fastest_lap_tel":
        step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
    elif workload == "driver_tel":
        step("laps", lambda: session.laps)
        step("driver_tel", lambda: session.laps.pick_driver("VER").telemetry)
    elif workload == "all_tel":
        step("laps", lambda: session.laps)
        step("all_tel", lambda: session.fetch_all_laps_telemetry())
    elif workload == "notebook":
        step("load", lambda: session.load(weather=True, messages=True))
        laps = session.laps
        step("picks", lambda: [laps.pick_driver("VER"), laps.pick_quicklaps(), laps.pick_fastest()])
        step("driver_tel_x2", lambda: [laps.pick_driver(d).telemetry for d in ("VER", "LEC")])
        step("fastest_lap_tel", lambda: session.get_fastest_lap_tel())
        step("results", lambda: session.results)
        step("weather", lambda: session.weather_data)
    out["in_process_total"] = round((time.perf_counter() - t_proc) * 1000, 2)
    print(json.dumps(out))


def _ocsp_only_copy(cache_dir: str) -> str:
    """A new cache directory holding only the OCSP row of ``cache_dir``'s SQLite file."""
    target = tempfile.mkdtemp(prefix="tif1-e3-ocsp-")
    shutil.copy2(Path(cache_dir) / "cache.sqlite", Path(target) / "cache.sqlite")
    conn = sqlite3.connect(Path(target) / "cache.sqlite")
    tables = [
        r[0]
        for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        if not r[0].startswith("sqlite_")
    ]
    for table in tables:
        if table == "cache":
            conn.execute("DELETE FROM cache WHERE key != ?", (OCSP_KEY,))
        else:
            conn.execute(f"DELETE FROM {table}")
    conn.commit()
    kept = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
    conn.execute("VACUUM")
    conn.close()
    if kept != 1:
        raise RuntimeError(f"expected exactly the OCSP row, kept {kept} rows")
    return target


def _run_child(
    workload: str, lib: str, src_dir: str | None, cache_dir: str, keep: bool = False
) -> dict:
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir)
    env.pop("TIF1_OFFLINE_MODE", None)
    if src_dir:
        env["PYTHONPATH"] = str(Path(src_dir).resolve())
    cmd = [sys.executable, str(Path(__file__).resolve()), "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", env=env, check=False
    )
    wall = (time.perf_counter() - t0) * 1000
    if not keep:
        shutil.rmtree(cache_dir, ignore_errors=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    rec["warnings_403"] = proc.stderr.count("403")
    return rec


def _summarize(recs: list[dict]) -> dict:
    phases = {}
    for key in recs[0]:
        vals = [r[key] for r in recs]
        phases[key] = {"median": round(statistics.median(vals), 1), "min": round(min(vals), 1)}
    return phases


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", default=None)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--control-src", required=False)
    parser.add_argument("--candidate-src", required=False)
    parser.add_argument("--workloads", default="laps,fastest_lap_tel")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    if args.child:
        _child(args.child, args.lib)
        return
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        recs: dict[str, list[dict]] = {arm: [] for arm in ARMS}
        # Prime the OCSP row once so rep 0's "second" arm has one.
        prime_dir = tempfile.mkdtemp(prefix="tif1-e3-prime-")
        _run_child(workload, args.lib, args.candidate_src, prime_dir, keep=True)
        ocsp_dir = _ocsp_only_copy(prime_dir)
        shutil.rmtree(prime_dir, ignore_errors=True)
        for i in range(args.reps):
            order = ARMS[i % 3 :] + ARMS[: i % 3]
            for arm in order:
                if arm == "control":
                    rec = _run_child(
                        workload, args.lib, args.control_src, tempfile.mkdtemp(prefix="tif1-e3-")
                    )
                elif arm == "first":
                    first_dir = tempfile.mkdtemp(prefix="tif1-e3-")
                    rec = _run_child(workload, args.lib, args.candidate_src, first_dir, keep=True)
                    shutil.rmtree(ocsp_dir, ignore_errors=True)
                    ocsp_dir = _ocsp_only_copy(first_dir)
                    shutil.rmtree(first_dir, ignore_errors=True)
                else:
                    seeded_dir = tempfile.mkdtemp(prefix="tif1-e3-")
                    shutil.copy2(Path(ocsp_dir) / "cache.sqlite", Path(seeded_dir) / "cache.sqlite")
                    rec = _run_child(workload, args.lib, args.candidate_src, seeded_dir)
                recs[arm].append(rec)
        shutil.rmtree(ocsp_dir, ignore_errors=True)
        ctl = recs["control"]
        entry: dict[str, object] = {arm: _summarize(recs[arm]) for arm in ARMS}
        entry["raw_process_wall"] = {arm: [r["process_wall"] for r in recs[arm]] for arm in ARMS}
        line = (
            f"[{args.lib}] {workload:16s} control {entry['control']['process_wall']['median']:.0f}"
        )
        for arm in ("first", "second"):
            wins = sum(
                1
                for a, b in zip(ctl, recs[arm], strict=True)
                if b["process_wall"] < a["process_wall"]
            )
            entry[f"{arm}_faster_pairs"] = f"{wins}/{args.reps}"
            line += (
                f"  {arm} {entry[arm]['process_wall']['median']:.0f} (faster {wins}/{args.reps})"
            )
        summary[workload] = entry
        print(line, flush=True)
    if args.out:
        Path(args.out).write_text(
            json.dumps({"lib": args.lib, "reps": args.reps, "summary": summary}, indent=1) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
