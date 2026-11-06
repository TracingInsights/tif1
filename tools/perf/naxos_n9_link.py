"""Naxos N9: measure the link with curl, outside tif1.

Downloads the 2025 Bahrain race telemetry lap files (the paths a cold
``fetch_all_laps_telemetry()`` asks for) with ``curl --parallel`` and reports
the aggregate wall time and throughput (bytes on the wire, gzip).

Usage (from the repo root)::

    python tools/perf/naxos_n9_link.py --paths paths.txt --mode h2 --parallel 22
    python tools/perf/naxos_n9_link.py --paths paths.txt --mode h1 --parallel 22
    python tools/perf/naxos_n9_link.py --paths paths.txt --mode h2 --cdn hf

``paths.txt`` lists ``{driver}/{lap}_tel.json`` lines (see the N9 report).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
import time
from pathlib import Path

BASES = {
    "jsdelivr": "https://cdn.jsdelivr.net/gh/TracingInsights/2025@main/Bahrain%20Grand%20Prix/Race/",
    "hf": "https://huggingface.co/buckets/tracinginsights/2025/resolve/Bahrain%20Grand%20Prix/Race/",
    "staticdelivr": "https://cdn.staticdelivr.com/gh/TracingInsights/2025/main/Bahrain%20Grand%20Prix/Race/",
}


def run(paths: list[str], base: str, mode: str, parallel: int) -> dict:
    """Run one curl over ``paths`` and return wall time, bytes, status counts."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = Path(tmp) / "curl.cfg"
        lines = []
        for p in paths:
            lines.append(f'url = "{base}{p}"')
            lines.append('output = "/dev/null"')
        cfg.write_text("\n".join(lines) + "\n", encoding="utf-8")
        cmd = [
            "curl",
            "-s",
            "-L",
            "--compressed",
            "--parallel",
            "--parallel-max",
            str(parallel),
            "-w",
            "%{http_code} %{size_download} %{time_starttransfer} %{time_total} %{http_version}\\n",
            "-K",
            str(cfg),
        ]
        # h2: curl multiplexes the parallel transfers on one connection per
        # host; h1: one connection per transfer slot.
        cmd.insert(1, "--http1.1" if mode == "h1" else "--http2")
        t0 = time.perf_counter()
        out = subprocess.run(cmd, capture_output=True, text=True, check=False).stdout
        wall = time.perf_counter() - t0
    codes: dict[str, int] = {}
    total = 0
    ttfb = []
    versions: dict[str, int] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) != 5:
            continue
        codes[parts[0]] = codes.get(parts[0], 0) + 1
        total += int(parts[1])
        ttfb.append(float(parts[2]))
        versions[parts[4]] = versions.get(parts[4], 0) + 1
    ttfb.sort()
    return {
        "wall_s": round(wall, 3),
        "bytes": total,
        "mb_s": round(total / wall / 1e6, 2),
        "codes": codes,
        "versions": versions,
        "ttfb_med": ttfb[len(ttfb) // 2] if ttfb else None,
        "ttfb_p90": ttfb[int(len(ttfb) * 0.9)] if ttfb else None,
    }


def main() -> None:
    """Parse arguments and print one JSON line per run."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--paths", required=True)
    ap.add_argument("--cdn", default="jsdelivr", choices=sorted(BASES))
    ap.add_argument("--mode", default="h2", choices=["h1", "h2"])
    ap.add_argument("--parallel", type=int, default=22)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--reps", type=int, default=1)
    args = ap.parse_args()
    paths = Path(args.paths).read_text(encoding="utf-8").split()
    if args.limit:
        paths = paths[: args.limit]
    import orjson

    for _ in range(args.reps):
        res = run(paths, BASES[args.cdn], args.mode, args.parallel)
        res.update(cdn=args.cdn, mode=args.mode, parallel=args.parallel, files=len(paths))
        print(orjson.dumps(res).decode(), flush=True)
    os.sys.stdout.flush()


if __name__ == "__main__":
    main()
