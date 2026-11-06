"""Corinth C4: interleaved A/B of a current-season script in four cache states.

The script is ``current.py`` of the round (``get_session(2026, event, "R")``,
``.laps``, ``.results``) in a fresh interpreter per run. States:

- ``cold``: an empty cache directory per run (live CDN);
- ``warm``: the shared cache with every TTL'd row (the f1schedule calendar,
  the 2026 Stats files) marked fetched a minute ago;
- ``expired``: the same rows marked fetched 7 hours ago (``schedule_ttl_hours``
  and ``ergast_ttl_hours`` are 6), what a script pays once the last refresh is
  older than the TTLs; online;
- ``expired_offline``: the same, ``TIF1_OFFLINE_MODE=true``.

Before each warm/expired run both arms get the same row ages (the helper runs
on the control tree's ``Cache`` API), so the arms share one cache directory.
``--digest`` makes the child print a digest of ``laps`` and ``results``
(CSV + dtypes) for the parity check instead of timing.

Usage:
    python tools/perf/corinth_c4_ab.py --control-src /tmp/corinth/control/src \
        --candidate-src /tmp/corinth/C4/src --cache /tmp/corinth/home-C4/cur \
        --lib pandas --pairs 6 --states warm,expired --events "Mexico City Grand Prix" \
        --out .agents/perf-corinth/C4_ab_pandas.json
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

HERE = Path(__file__).resolve()

_AGE_SNIPPET = """
import sqlite3, sys, time
from tif1.cache import get_cache
age = float(sys.argv[1])
cache = get_cache()
keys = [row[0] for row in cache.conn.execute(
    "SELECT key FROM cache WHERE key LIKE 'stats/2026/%' OR key LIKE 'f1schedule/schedule_2026%'"
)]
now = time.time()
for key in keys:
    entry = cache.get_entry("json", key)
    if isinstance(entry, dict) and "fetched_at" in entry:
        entry["fetched_at"] = now - age
        cache.set_entry("json", key, entry)
cache.close()
print(len(keys))
"""


def _child(lib: str, event: str, digest: bool) -> None:
    t0 = time.perf_counter()
    import tif1

    out: dict[str, float | str] = {}

    def step(name, func):
        start = time.perf_counter()
        result = func()
        out[name] = round((time.perf_counter() - start) * 1000, 2)
        return result

    session = step("get_session", lambda: tif1.get_session(2026, event, "R", lib=lib))
    laps = step("laps", lambda: session.laps)
    results = step("results", lambda: session.results)
    out["in_process"] = round((time.perf_counter() - t0) * 1000, 2)
    if digest:
        import hashlib

        def frame_digest(frame) -> str:
            if lib == "polars":
                text = repr(frame.rows()) + repr(frame.schema) + repr(type(frame))
            else:
                text = frame.to_csv() + repr(frame.dtypes.to_dict()) + repr(type(frame))
            return hashlib.sha256(text.encode("utf-8")).hexdigest()

        out = {
            "laps": frame_digest(laps),
            "results": frame_digest(results),
            "drivers": repr(session.drivers),
            "laps_shape": repr(laps.shape),
            "results_shape": repr(results.shape),
        }
    print(json.dumps(out))


def _age_rows(control_src: str, cache_dir: str, age_seconds: float) -> None:
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir, PYTHONPATH=control_src)
    subprocess.run(
        [sys.executable, "-c", _AGE_SNIPPET, str(age_seconds)],
        env=env,
        check=True,
        capture_output=True,
    )


def _run(arm_src: str, lib: str, event: str, state: str, cache_dir: str, digest: bool) -> dict:
    env = dict(os.environ, PYTHONPATH=str(Path(arm_src).resolve()))
    env.pop("TIF1_OFFLINE_MODE", None)
    if state == "cold":
        env["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="c4cold-", dir=os.environ["HOME"])
    else:
        env["TIF1_CACHE_DIR"] = cache_dir
    if state == "expired_offline":
        env["TIF1_OFFLINE_MODE"] = "true"
    cmd = [sys.executable, str(HERE), "--child", "--lib", lib, "--event", event]
    if digest:
        cmd.append("--digest")
    start = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - start) * 1000
    if proc.returncode != 0:
        raise RuntimeError(f"{arm_src} {state} {event} failed: {proc.stderr[-3000:]}")
    record = json.loads(proc.stdout.strip().splitlines()[-1])
    if not digest:
        record["process_wall"] = round(wall, 2)
    if state == "cold":
        import shutil

        shutil.rmtree(env["TIF1_CACHE_DIR"], ignore_errors=True)
    return record


def _p90(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(0.9 * (len(ordered) - 1)))]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--digest", action="store_true")
    parser.add_argument("--control-src", default="/tmp/corinth/control/src")
    parser.add_argument("--candidate-src", default=str(HERE.parents[2] / "src"))
    parser.add_argument("--cache", default="/tmp/corinth/home-C4/cur")
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--pairs", type=int, default=6)
    parser.add_argument("--states", default="warm,expired,expired_offline,cold")
    parser.add_argument("--events", default="Mexico City Grand Prix,British Grand Prix")
    parser.add_argument("--out", default=None)
    parser.add_argument("--age", type=float, default=None, help="only age the rows (seconds)")
    args = parser.parse_args()
    if args.age is not None:
        _age_rows(args.control_src, args.cache, args.age)
        return
    if args.child:
        _child(args.lib, args.events.split(",")[0], args.digest)
        return
    arms = {"control": args.control_src, "candidate": args.candidate_src}
    ages = {"warm": 60.0, "expired": 7 * 3600.0, "expired_offline": 7 * 3600.0}
    summary: dict[str, dict] = {}
    raw: dict[str, dict] = {}
    for event in args.events.split(","):
        for state in args.states.split(","):
            records: dict[str, list[dict]] = {"control": [], "candidate": []}
            for pair in range(args.pairs):
                order = ("control", "candidate") if pair % 2 == 0 else ("candidate", "control")
                for arm in order:
                    if state in ages:
                        _age_rows(args.control_src, args.cache, ages[state])
                    records[arm].append(
                        _run(arms[arm], args.lib, event, state, args.cache, args.digest)
                    )
            label = f"{event} | {state}"
            raw[label] = records
            if args.digest:
                same = all(
                    a == b for a, b in zip(records["control"], records["candidate"], strict=True)
                )
                summary[label] = {"identical": same, "control": records["control"][0]}
                print(label, "identical" if same else "DIFFERENT", flush=True)
                continue
            phases: dict[str, dict] = {}
            for key in records["control"][0]:
                control = [r[key] for r in records["control"]]
                candidate = [r[key] for r in records["candidate"]]
                wins = sum(1 for a, b in zip(control, candidate, strict=True) if b < a)
                phases[key] = {
                    "control_median": round(statistics.median(control), 1),
                    "candidate_median": round(statistics.median(candidate), 1),
                    "control_p90": round(_p90(control), 1),
                    "candidate_p90": round(_p90(candidate), 1),
                    "candidate_faster_pairs": f"{wins}/{args.pairs}",
                }
            summary[label] = phases
            print(label, flush=True)
            for key, row in phases.items():
                print(
                    f"  {key:14s} control {row['control_median']:8.1f} (p90 "
                    f"{row['control_p90']:8.1f})  candidate {row['candidate_median']:8.1f} "
                    f"(p90 {row['candidate_p90']:8.1f})  won {row['candidate_faster_pairs']}",
                    flush=True,
                )
    if args.out:
        Path(args.out).write_text(
            json.dumps({"lib": args.lib, "summary": summary, "raw": raw}, indent=1),
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
