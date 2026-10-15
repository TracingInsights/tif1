"""Offline telemetry-assembly A/B: interleaved fresh-process runs per source tree (Thebes).

Each run is a fresh interpreter that parses the corpus once and times the
``_create_telemetry_df`` loop over every payload (best of N inside the
process, GC suspended like the production loop). Trees alternate so the two
arms see the same machine state.

Usage:
    uv run python tools/thebes_assembly_bench.py --src-a /tmp/tif1-control/src --src-b src \
        --corpus /tmp/tif1-perf/corpus_2026_monaco.pkl --pairs 5
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys

CHILD = r"""
import gc, pickle, sys, time
sys.path.insert(0, sys.argv[1])
import orjson
from tif1.core_utils.helpers import _create_telemetry_df
corpus = pickle.load(open(sys.argv[2], "rb"))
if "refs" in corpus:
    items = [((r[0], r[1]), corpus["raw"][r]) for r in corpus["refs"]]
else:
    items = [((k[-2], k[-1]), b) for k, b in sorted(corpus.items())]
lib = sys.argv[3]
tels = []
for (d, n), blob in items:
    data = orjson.loads(blob)
    tels.append((str(d), int(n), data.get("tel", data)))
best = 1e9
for _ in range(int(sys.argv[4])):
    gc.collect()
    gc.disable()
    t0 = time.perf_counter()
    frames = [_create_telemetry_df(t, d, n, lib) for d, n, t in tels]
    best = min(best, time.perf_counter() - t0)
    gc.enable()
print(json.dumps({"best_s": best, "frames": sum(f is not None for f in frames)}))
""".replace("print(json", "import json; print(json")


def run(src: str, corpus: str, lib: str, reps: int) -> float:
    proc = subprocess.run(
        [sys.executable, "-c", CHILD, src, corpus, lib, str(reps)],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])["best_s"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-a", required=True)
    parser.add_argument("--src-b", required=True)
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--reps", type=int, default=3)
    args = parser.parse_args()
    a_runs, b_runs = [], []
    for i in range(args.pairs):
        order = [("A", args.src_a), ("B", args.src_b)] if i % 2 == 0 else [("B", args.src_b), ("A", args.src_a)]
        for label, src in order:
            t = run(src, args.corpus, args.lib, args.reps)
            (a_runs if label == "A" else b_runs).append(t)
            print(f"pair {i + 1} {label}: {t * 1000:.0f} ms", flush=True)
    med_a, med_b = statistics.median(a_runs), statistics.median(b_runs)
    wins = sum(b < a for a, b in zip(a_runs, b_runs))
    print(
        json.dumps(
            {
                "A_median_ms": round(med_a * 1000),
                "B_median_ms": round(med_b * 1000),
                "delta_pct": round((med_b / med_a - 1) * 100, 1),
                "B_faster_pairs": f"{wins}/{len(a_runs)}",
                "A_runs_ms": [round(x * 1000) for x in a_runs],
                "B_runs_ms": [round(x * 1000) for x in b_runs],
            }
        )
    )


if __name__ == "__main__":
    main()
