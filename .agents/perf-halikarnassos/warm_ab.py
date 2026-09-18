"""Interleaved warm-cache e2e A/B: current tree vs stashed baseline source."""

from __future__ import annotations

import json
import statistics
import subprocess
import sys

RUNS = 8


def run(src: str) -> dict:
    out = subprocess.run(
        [sys.executable, "tools/warm_cache_benchmark.py", "--src-dir", src],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout.strip().splitlines()[-1])


def main() -> None:
    arms = {"candidate": "src", "baseline": "/tmp/perf-baseline-src/src"}
    results: dict[str, list[dict]] = {a: [] for a in arms}
    for _ in range(RUNS):
        for arm, src in arms.items():
            r = run(src)
            r["arm"] = arm
            results[arm].append(r)

    def med(rows, k):
        return statistics.median(r[k] for r in rows)

    for arm in arms:
        rows = results[arm]
        print(
            f"{arm:10s} n={RUNS} get_session={med(rows, 'get_session_s'):.3f}s "
            f"laps={med(rows, 'laps_s'):.4f}s telemetry={med(rows, 'telemetry_s'):.3f}s "
            f"total={med(rows, 'total_s'):.3f}s"
        )
    base = results["baseline"]
    cand = results["candidate"]
    for k in ("get_session_s", "laps_s", "telemetry_s", "total_s"):
        b = statistics.median(r[k] for r in base)
        c = statistics.median(r[k] for r in cand)
        print(f"{k}: baseline={b:.4f} candidate={c:.4f} ({(c / b - 1) * 100:+.1f}%)")


if __name__ == "__main__":
    main()
