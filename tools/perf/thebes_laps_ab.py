"""Interleaved A/B of the cold laps phase (Thebes TH6).

Each run is a fresh process with a throwaway cache that times
``get_session`` and ``session.laps`` and counts the serial CDN round trips
(distinct "waves": a request that starts after every earlier request has
finished opens a new wave).

Usage:
    uv run python tools/perf/thebes_laps_ab.py --src-a /tmp/tif1-control/src --src-b src --pairs 6
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys

CHILD = r"""
import json, os, sys, tempfile, time, threading
os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-laps-")
sys.path.insert(0, sys.argv[1])
year, gp, sess = int(sys.argv[2]), sys.argv[3], sys.argv[4]
import tif1
from tif1 import async_fetch
spans = []
lock = threading.Lock()
orig = async_fetch._get_resolved
def timed(session_client, url, timeout):
    t0 = time.perf_counter()
    try:
        return orig(session_client, url, timeout)
    finally:
        with lock:
            spans.append((t0, time.perf_counter(), url.rsplit("/", 1)[-1]))
async_fetch._get_resolved = timed
t0 = time.perf_counter()
s = tif1.get_session(year, gp, sess)
t1 = time.perf_counter()
laps = s.laps
t2 = time.perf_counter()
spans.sort()
waves = 0
frontier = -1.0
for start, end, _name in spans:
    if start >= frontier:
        waves += 1
    frontier = max(frontier, end)
print(json.dumps({"get_session_s": round(t1 - t0, 3), "laps_s": round(t2 - t1, 3), "rows": len(laps),
    "requests": len(spans), "waves": waves, "files": [n for _, _, n in spans]}))
"""


def run(src: str, args: argparse.Namespace) -> dict:
    proc = subprocess.run(
        [sys.executable, "-c", CHILD, src, str(args.year), args.gp, args.session],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-2000:])
    return json.loads(proc.stdout.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--src-a", required=True)
    parser.add_argument("--src-b", required=True)
    parser.add_argument("--pairs", type=int, default=6)
    args = parser.parse_args()
    runs: dict[str, list[dict]] = {"A": [], "B": []}
    for i in range(args.pairs):
        order = ["A", "B"] if i % 2 == 0 else ["B", "A"]
        for label in order:
            result = run(args.src_a if label == "A" else args.src_b, args)
            runs[label].append(result)
            print(label, json.dumps({k: v for k, v in result.items() if k != "files"}), flush=True)
    a = [r["laps_s"] for r in runs["A"]]
    b = [r["laps_s"] for r in runs["B"]]
    print(
        json.dumps(
            {
                "A_laps_median_s": round(statistics.median(a), 3),
                "B_laps_median_s": round(statistics.median(b), 3),
                "delta_pct": round((statistics.median(b) / statistics.median(a) - 1) * 100, 1),
                "B_faster_pairs": f"{sum(y < x for x, y in zip(a, b))}/{len(a)}",
                "A_waves": [r["waves"] for r in runs["A"]],
                "B_waves": [r["waves"] for r in runs["B"]],
                "A_requests": [r["requests"] for r in runs["A"]],
                "B_requests": [r["requests"] for r in runs["B"]],
            }
        )
    )


if __name__ == "__main__":
    main()
