"""Live cold-edge A/B on disjoint file slices of one never-fetched session (Thebes).

Both arms fetch different laps of the same session at the same moment, so
the CDN edge state (cold for every file) is shared. Each run is a fresh
process with a throwaway cache that resolves the session's laps, takes the
slice ``[offset, offset + files)`` of the (driver, lap) refs and runs the
production ``fetch_multiple_async`` on them with caching and validation
off. The report carries wall time, per-host request counts, timeouts and
the per-request latency quantiles.

Usage (ABBA order over four slices):
    uv run python tools/perf/thebes_live_slices.py --year 2026 --gp "Hungarian Grand Prix" \
        --session Race --src-a /tmp/tif1-control/src --src-b src --files 200
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

CHILD = r"""
import asyncio, json, os, sys, tempfile, time
os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-slice-")
sys.path.insert(0, sys.argv[1])
year, gp, sess, offset, count = int(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5]), int(sys.argv[6])
import tif1
from tif1 import async_fetch
records = []
orig = async_fetch._get_resolved
def timed(session_client, url, timeout):
    t0 = time.perf_counter()
    try:
        r = orig(session_client, url, timeout)
        records.append((url.split("/")[2], time.perf_counter() - t0, getattr(r, "status_code", None)))
        return r
    except Exception as e:
        records.append((url.split("/")[2], time.perf_counter() - t0, type(e).__name__))
        raise
async_fetch._get_resolved = timed
s = tif1.get_session(year, gp, sess)
t0 = time.perf_counter(); laps = s.laps; laps_s = time.perf_counter() - t0
refs = list(zip(laps["Driver"].astype(str), laps["LapNumber"].astype(int)))[offset:offset + count]
reqs = [(year, s.gp, s.session, f"{d}/{n}_tel.json") for d, n in refs]
records.clear()
t0 = time.perf_counter()
res = asyncio.run(async_fetch.fetch_multiple_async(reqs, use_cache=False, write_cache=False, validate_payload=False))
wall = time.perf_counter() - t0
ok = sum(1 for r in res if isinstance(r, dict))
lat = sorted(r[1] for r in records)
hosts = {}
for h, _, st in records:
    hosts[h] = hosts.get(h, 0) + 1
print(json.dumps({"laps_s": round(laps_s, 2), "n": len(reqs), "ok": ok, "wall_s": round(wall, 2),
    "files_per_s": round(len(reqs) / wall, 1), "p50": round(lat[len(lat) // 2], 2), "p90": round(lat[int(0.9 * len(lat))], 2),
    "max": round(lat[-1], 2), "timeouts": sum(1 for r in records if r[2] == "ReadTimeout"),
    "hosts": hosts}))
"""


def run(src: str, args: argparse.Namespace, offset: int) -> dict:
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            CHILD,
            src,
            str(args.year),
            args.gp,
            args.session,
            str(offset),
            str(args.files),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-2000:])
    return json.loads(proc.stdout.strip().splitlines()[-1])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--gp", required=True)
    parser.add_argument("--session", default="Race")
    parser.add_argument("--src-a", required=True)
    parser.add_argument("--src-b", required=True)
    parser.add_argument("--files", type=int, default=200)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--order", default="ABBA")
    args = parser.parse_args()
    offset = args.start
    for label in args.order:
        src = args.src_a if label == "A" else args.src_b
        result = run(src, args, offset)
        result["arm"] = label
        result["offset"] = offset
        print(json.dumps(result), flush=True)
        offset += args.files


if __name__ == "__main__":
    main()
