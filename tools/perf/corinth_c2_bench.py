"""Corinth C2: interleaved control/candidate A/B of Ergast-only (pre-2018) sessions.

Each run is a fresh interpreter: ``get_session`` + ``laps`` + ``results`` +
``drivers`` (+ ``pit_stops``) of one session. ``--mode cold`` gives every run an
empty cache (live CDN, Stats/Jolpica); ``--mode warm`` reads a cache each arm
primed itself beforehand (offline). The arms alternate ABAB/BABA per rep.

    python tools/perf/corinth_c2_bench.py --control-src /tmp/corinth/control/src \
        --candidate-src src --lib pandas --mode cold --reps 5 --out out.json

``--parity`` instead writes a digest of every session's frames per arm (from
the warm caches, offline) and compares them.
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

SESSIONS = (
    (1998, "Monaco", "R"),
    (2010, "Monza", "R"),
    (1975, "British", "R"),
    (1955, "Monaco", "R"),
    (1998, "Monaco", "Q"),
)


def _digest_frame(frame, lib: str) -> dict:
    if lib == "polars":
        text = repr(frame.rows())  # write_csv rejects Duration columns
        schema = [f"{name}:{dtype}" for name, dtype in frame.schema.items()]
        return {
            "type": type(frame).__name__,
            "shape": list(frame.shape),
            "schema": schema,
            "sha": hashlib.sha256(text.encode()).hexdigest(),
        }
    text = frame.to_csv()
    return {
        "type": type(frame).__name__,
        "shape": list(frame.shape),
        "dtypes": [f"{name}:{dtype}" for name, dtype in frame.dtypes.items()],
        "index": f"{type(frame.index).__name__}:{frame.index.dtype}:{frame.index.name}",
        "sha": hashlib.sha256(text.encode()).hexdigest(),
    }


def p90(xs: list[float]) -> float:
    return sorted(xs)[max(0, round(0.9 * len(xs)) - 1)]


def child(year: int, gp: str, session: str, lib: str, digest: bool) -> None:
    t0 = time.perf_counter()
    import tif1

    phases: dict[str, float] = {}

    def step(name, fn):
        started = time.perf_counter()
        value = fn()
        phases[name] = round((time.perf_counter() - started) * 1000, 1)
        return value

    s = step("get_session", lambda: tif1.get_session(year, gp, session, lib=lib))
    laps = step("laps", lambda: s.laps)
    results = step("results", lambda: s.results)
    drivers = step("drivers", lambda: s.drivers)
    phases["total"] = round((time.perf_counter() - t0) * 1000, 1)
    out: dict = {"phases": phases}
    if digest:
        out["digest"] = {
            "laps": _digest_frame(laps, lib),
            "results": _digest_frame(results, lib),
            "drivers": list(drivers),
            "pit_stops": _digest_frame(s.pit_stops, lib),
            "total_laps": str(getattr(s, "total_laps", None)),
        }
    print("RESULT " + json.dumps(out))


def _run(src: str, home: str, cache: str, key: tuple, lib: str, *, offline: bool, digest: bool):
    env = dict(os.environ)
    env.update(PYTHONPATH=src, HOME=home, TIF1_CACHE_DIR=cache)
    if offline:
        env["TIF1_OFFLINE_MODE"] = "true"
    year, gp, session = key
    cmd = [sys.executable, __file__, "--child", str(year), gp, session, lib]
    if digest:
        cmd.append("--digest")
    started = time.perf_counter()
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    wall = (time.perf_counter() - started) * 1000
    line = next((ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT ")), None)
    if line is None:
        raise RuntimeError(f"child failed for {key} ({src}):\n{proc.stdout}\n{proc.stderr}")
    out = json.loads(line[7:])
    out["wall"] = round(wall, 1)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", nargs=4)
    parser.add_argument("--digest", action="store_true")
    parser.add_argument("--control-src")
    parser.add_argument("--candidate-src")
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--mode", choices=("cold", "warm"), default="cold")
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--home", default=os.environ.get("HOME", "/tmp"))
    parser.add_argument("--warm-root", default=".scratch/c2warm")
    parser.add_argument("--parity", action="store_true")
    # The control cannot answer some sessions offline (jsDelivr 403s a missing
    # Stats file and the loader never persists that miss): warm runs are online.
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--out")
    parser.add_argument("--sessions", help="comma-separated indices into SESSIONS")
    args = parser.parse_args()
    if args.child:
        year, gp, session, lib = args.child
        child(int(year), gp, session, lib, args.digest)
        return
    global SESSIONS
    if args.sessions:
        SESSIONS = tuple(SESSIONS[int(i)] for i in args.sessions.split(","))

    arms = {
        "control": str(Path(args.control_src).resolve()),
        "candidate": str(Path(args.candidate_src).resolve()),
    }
    warm_root = Path(args.warm_root).resolve()

    def warm_cache(arm: str) -> str:
        return str(warm_root / arm)

    if args.mode == "warm" or args.parity:
        # Each arm primes its own cache (the candidate stores fewer entries).
        for arm, src in arms.items():
            for key in SESSIONS:
                # digest=True reads every surface (pit_stops too) into the cache.
                _run(src, args.home, warm_cache(arm), key, args.lib, offline=False, digest=True)

    if args.parity:
        digests = {}
        for arm, src in arms.items():
            digests[arm] = {
                f"{y} {g} {s}": _run(
                    src,
                    args.home,
                    warm_cache(arm),
                    (y, g, s),
                    args.lib,
                    offline=args.offline,
                    digest=True,
                )["digest"]
                for (y, g, s) in SESSIONS
            }
        differing = [
            (key, part)
            for key in digests["control"]
            for part in digests["control"][key]
            if digests["control"][key][part] != digests["candidate"][key][part]
        ]
        surfaces = sum(len(v) for v in digests["control"].values())
        print(f"parity {args.lib}: {len(differing)} differing of {surfaces} surfaces")
        for key, part in differing:
            print("  DIFF", key, part)
        if args.out:
            Path(args.out).write_text(
                json.dumps({"digests": digests, "differing": differing}, indent=1),
                encoding="utf-8",
            )
        return

    runs: dict[str, dict[str, list[dict]]] = {
        f"{y} {g} {s}": {"control": [], "candidate": []} for (y, g, s) in SESSIONS
    }
    for rep in range(args.reps):
        for index, key in enumerate(SESSIONS):
            order = ["control", "candidate"] if (rep + index) % 2 == 0 else ["candidate", "control"]
            for arm in order:
                if args.mode == "cold":
                    cache = tempfile.mkdtemp(prefix=f"c2cold-{arm}-", dir=str(warm_root.parent))
                    try:
                        out = _run(
                            arms[arm], args.home, cache, key, args.lib, offline=False, digest=False
                        )
                    finally:
                        shutil.rmtree(cache, ignore_errors=True)
                else:
                    out = _run(
                        arms[arm],
                        args.home,
                        warm_cache(arm),
                        key,
                        args.lib,
                        offline=args.offline,
                        digest=False,
                    )
                runs[f"{key[0]} {key[1]} {key[2]}"][arm].append(out)
        print(f"rep {rep + 1}/{args.reps} done", flush=True)

    summary = {}
    print(
        f"{args.mode} {args.lib}: session / metric / control median / candidate median / "
        "pairs won / control p90 / candidate p90"
    )
    for key, by_arm in runs.items():
        summary[key] = {}
        for metric in ("wall", "get_session", "laps", "results"):
            control, candidate = (
                [r["wall"] if metric == "wall" else r["phases"][metric] for r in by_arm[arm]]
                for arm in ("control", "candidate")
            )
            won = sum(1 for a, b in zip(control, candidate) if b < a)
            row = {
                "control": statistics.median(control),
                "candidate": statistics.median(candidate),
                "won": f"{won}/{len(control)}",
                "control_p90": p90(control),
                "candidate_p90": p90(candidate),
            }
            summary[key][metric] = row
            print(
                f"{key:18s} {metric:12s} {row['control']:8.0f} {row['candidate']:8.0f} "
                f"{row['won']:>5s} {row['control_p90']:8.0f} {row['candidate_p90']:8.0f}"
            )
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {"mode": args.mode, "lib": args.lib, "summary": summary, "runs": runs}, indent=1
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
