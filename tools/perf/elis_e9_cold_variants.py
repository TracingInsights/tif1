"""Elis E9: cold (live CDN) runs of the control tree and E9 trigger variants, interleaved.

Each rep runs every arm once in a rotated order through the ``--child`` entry
of ``elis_e3_cold_bench.py`` (phases: ``get_session``, ``laps``,
``driver_tel``, ...). Arms are ``control`` (the E2+E3 tree) and candidate
variants selected with the measurement-only ``TIF1_E9_TRIGGER`` switch
(``handover``: niquests import + warm-up in a daemon thread once the last
first-wave body is handed over; ``telemetry``: the same at the first
telemetry access; ``bridge``: the shipped form, the telemetry wave on the
stdlib connections while niquests warms; ``stdlib``: the whole wave on the
stdlib, niquests never imported; ``off``: E2). The switch is not in the
committed tree (it lived in ``first_wave.take``, ``first_wave._E9_TRIGGER``,
``Session._resolve_telemetry_ultra_cold_mode`` and ``async_fetch``): put it
back to re-run the comparison. By default every run starts from a
cache that holds only a persisted OCSP verdict row (a returning user meeting
an event it has not cached; E3); ``--first-ever`` uses empty caches. Run
from the repository root:

    python tools/perf/elis_e9_cold_variants.py --lib pandas --reps 5 \
        --variants handover,off --workloads laps,driver_tel --out FILE
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve()
CHILD = HERE.parent / "elis_e3_cold_bench.py"
CONTROL_SRC = "/tmp/tif1-elis-control-e9/src"
CANDIDATE_SRC = str(HERE.parents[2] / "src")
sys.path.insert(0, str(HERE.parent))

from elis_e3_cold_bench import _ocsp_only_copy  # noqa: E402


def run_child(workload: str, lib: str, src: str, seed: str | None, env_extra: dict) -> dict:
    cache_dir = tempfile.mkdtemp(prefix="tif1-e9-cold-")
    if seed:
        shutil.copy2(Path(seed) / "cache.sqlite", Path(cache_dir) / "cache.sqlite")
    env = dict(os.environ, TIF1_CACHE_DIR=cache_dir, PYTHONPATH=src, **env_extra)
    env.pop("TIF1_OFFLINE_MODE", None)
    cmd = [sys.executable, str(CHILD), "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", env=env, check=False
    )
    wall = (time.perf_counter() - t0) * 1000
    shutil.rmtree(cache_dir, ignore_errors=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def prime_seed(lib: str) -> str:
    """A cache directory holding only the OCSP row a control-tree driver_tel run wrote."""
    prime = tempfile.mkdtemp(prefix="tif1-e9-prime-")
    env = dict(os.environ, TIF1_CACHE_DIR=prime, PYTHONPATH=CONTROL_SRC)
    env.pop("TIF1_OFFLINE_MODE", None)
    cmd = [sys.executable, str(CHILD), "--child", "driver_tel", "--lib", lib]
    subprocess.run(cmd, capture_output=True, env=env, check=True)
    seed = _ocsp_only_copy(prime)
    shutil.rmtree(prime, ignore_errors=True)
    return seed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--variants", default="handover,off")
    parser.add_argument("--workloads", default="laps,driver_tel")
    parser.add_argument("--first-ever", action="store_true")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    arms: list[tuple[str, str, dict]] = [("control", CONTROL_SRC, {})]
    arms += [(v, CANDIDATE_SRC, {"TIF1_E9_TRIGGER": v}) for v in args.variants.split(",")]
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        seed = None if args.first_ever else prime_seed(args.lib)
        recs: dict[str, list[dict]] = {name: [] for name, _, _ in arms}
        for i in range(args.reps):
            k = i % len(arms)
            for name, src, env_extra in arms[k:] + arms[:k]:
                recs[name].append(run_child(workload, args.lib, src, seed, env_extra))
        if seed:
            shutil.rmtree(seed, ignore_errors=True)
        entry: dict[str, object] = {}
        line = f"[{args.lib}] {workload:16s}"
        for name, _, _ in arms:
            phases = {
                key: round(statistics.median(r[key] for r in recs[name]), 1)
                for key in recs[name][0]
            }
            wins = sum(
                1
                for a, b in zip(recs["control"], recs[name], strict=True)
                if b["process_wall"] < a["process_wall"]
            )
            entry[name] = {
                "median": phases,
                "raw_process_wall": [r["process_wall"] for r in recs[name]],
                "faster_pairs": f"{wins}/{args.reps}",
            }
            line += f"  {name} {phases['process_wall']:.0f}"
            if name != "control":
                line += f" ({wins}/{args.reps})"
        summary[workload] = entry
        print(line, flush=True)
        print("   phases:", {n: entry[n]["median"] for n, _, _ in arms}, flush=True)
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {"lib": args.lib, "reps": args.reps, "first_ever": args.first_ever, **summary},
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
