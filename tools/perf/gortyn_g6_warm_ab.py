"""Gortyn G6: interleaved fresh-process warm A/B with a per-arm wheel overlay.

A copy of G1's ``gortyn_g1_warm_ab.py`` with one addition:
``--control-site DIR`` / ``--candidate-site DIR`` (repeatable) put ``DIR``
ahead of that arm's ``src`` only (after the shared ``--site`` entries), so the
two arms can run two builds of tif1-native (G1's lz4_flex wheel against G6's
liblz4 wheel) on the same Python source.

G1's docstring: a copy of Elis E4's ``elis_e4_warm_ab.py`` with one addition: ``--site DIR``
(repeatable) puts ``DIR`` ahead of each arm's ``src`` on ``PYTHONPATH``, so
both arms can run the main venv with an unpacked tif1-native wheel shadowing
the installed one (the Gortyn G1 overlay, used instead of a private venv when
``/tmp`` has no room for one).

E4's docstring: a copy of the Delphi ``delphi_warm_ab.py`` with three additions: each arm may
run on its own interpreter (``--control-python`` / ``--candidate-python``,
default this interpreter), carry extra environment variables
(``--control-env`` / ``--candidate-env``, ``KEY=VALUE``, repeatable; e.g.
``TIF1_DISABLE_NATIVE=1``), and the workloads come from the Delphi
``delphi_warm_bench.py`` (``--bench``). Reports, per workload and phase, the
control/candidate medians and the pairs the candidate won.

Usage (repo root)::

    python tools/perf/elis_e4_warm_ab.py --control-src /tmp/tif1-elis-control/src \
        --control-cache /tmp/tif1-delphi-warm --control-python /tmp/tif1-elis-venv-E4/bin/python \
        --candidate-src /tmp/tif1-elis/E4/src --candidate-cache /tmp/tif1-elis-warm-E4 \
        --candidate-python /tmp/tif1-elis-venv-E4/bin/python --lib pandas --pairs 6 \
        --workloads all_tel,notebook,car_data_loop --out .agents/perf-elis/E4_ab_pandas.json
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_BENCH = "/home/harish/Documents/tif1/tools/perf/delphi_warm_bench.py"


def _run_child(
    bench: str,
    workload: str,
    lib: str,
    sites: list[str],
    src_dir: str,
    cache_dir: str,
    python: str,
    extra: dict,
) -> dict:
    env = dict(os.environ, TIF1_OFFLINE_MODE="true", TIF1_CACHE_DIR=cache_dir, **extra)
    env["PYTHONPATH"] = os.pathsep.join(
        [*(str(Path(site).resolve()) for site in sites), str(Path(src_dir).resolve())]
    )
    cmd = [python, bench, "--child", workload, "--lib", lib]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, check=False)
    wall = (time.perf_counter() - t0) * 1000
    if proc.returncode != 0:
        raise RuntimeError(f"{workload} ({src_dir}) failed: {proc.stderr[-2000:]}")
    rec = json.loads(proc.stdout.strip().splitlines()[-1])
    rec["process_wall"] = round(wall, 2)
    return rec


def _env_pairs(values: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in values:
        key, _, value = item.partition("=")
        out[key] = value
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--control-cache", required=True)
    parser.add_argument("--candidate-cache", required=True)
    parser.add_argument("--control-python", default=sys.executable)
    parser.add_argument("--candidate-python", default=sys.executable)
    parser.add_argument("--control-env", action="append", default=[])
    parser.add_argument("--candidate-env", action="append", default=[])
    parser.add_argument("--bench", default=DEFAULT_BENCH)
    parser.add_argument("--lib", default="pandas", choices=["pandas", "polars"])
    parser.add_argument("--pairs", type=int, default=6)
    parser.add_argument("--workloads", default="all_tel,notebook,car_data_loop")
    parser.add_argument("--site", action="append", default=[])
    parser.add_argument("--control-site", action="append", default=[])
    parser.add_argument("--candidate-site", action="append", default=[])
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    sites = {
        "control": [*args.site, *args.control_site],
        "candidate": [*args.site, *args.candidate_site],
    }
    arms = {
        "control": (
            args.control_src,
            args.control_cache,
            args.control_python,
            _env_pairs(args.control_env),
        ),
        "candidate": (
            args.candidate_src,
            args.candidate_cache,
            args.candidate_python,
            _env_pairs(args.candidate_env),
        ),
    }
    summary: dict[str, dict] = {}
    for workload in args.workloads.split(","):
        recs: dict[str, list[dict]] = {"control": [], "candidate": []}
        for i in range(args.pairs):
            order = ("control", "candidate") if i % 2 == 0 else ("candidate", "control")
            for arm in order:
                recs[arm].append(_run_child(args.bench, workload, args.lib, sites[arm], *arms[arm]))
        phases: dict[str, dict] = {}
        for key in recs["control"][0]:
            if key not in recs["candidate"][0]:
                continue
            c = [r[key] for r in recs["control"]]
            d = [r[key] for r in recs["candidate"]]
            wins = sum(1 for a, b in zip(c, d, strict=True) if b < a)
            phases[key] = {
                "control_median": round(statistics.median(c), 1),
                "candidate_median": round(statistics.median(d), 1),
                "control_min": round(min(c), 1),
                "candidate_min": round(min(d), 1),
                "candidate_faster_pairs": f"{wins}/{args.pairs}",
            }
        summary[workload] = phases
        w = phases["process_wall"]
        print(
            f"[{args.lib}] {workload:20s} wall control {w['control_median']:.0f}/{w['control_min']:.0f}"
            f"  candidate {w['candidate_median']:.0f}/{w['candidate_min']:.0f}"
            f"  faster {w['candidate_faster_pairs']}  | "
            + "  ".join(
                f"{k}={v['control_median']:.0f}->{v['candidate_median']:.0f}"
                f"({v['candidate_faster_pairs']})"
                for k, v in phases.items()
                if k not in ("process_wall", "in_process_total", "import")
            ),
            flush=True,
        )
    if args.out:
        Path(args.out).write_text(
            json.dumps(
                {
                    "lib": args.lib,
                    "pairs": args.pairs,
                    "site": sites,
                    "arms": {
                        arm: {"src": a[0], "cache": a[1], "python": a[2], "env": a[3]}
                        for arm, a in arms.items()
                    },
                    "summary": summary,
                },
                indent=1,
            )
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
