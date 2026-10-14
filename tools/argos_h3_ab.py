"""A7: live interleaved A/B of HTTP/3 (default) vs HTTP/2-only bulk telemetry fetches.

Each run is a fresh process: resolves the Monaco 2026 race refs, then fetches
the first ``--files`` telemetry payloads through the production bulk path
(``fetch_multiple_async``, cache reads/writes off) and reports wall time, the
status mix seen by the CDN manager and the negotiated protocol.

Usage:
    uv run python tools/argos_h3_ab.py --rounds 3 --files 150 --out .agents/perf-argos/a7_h3_ab.json
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


def single_run(args: argparse.Namespace) -> None:
    os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-a7-")
    if args.variant == "h2":
        os.environ["TIF1_HTTP_DISABLE_HTTP3"] = "true"
    import asyncio

    import tif1
    from tif1.async_fetch import fetch_multiple_async
    from tif1.cdn import get_cdn_manager
    from tif1.http_session import get_session as http_session

    session = tif1.get_session(args.year, args.gp, args.session)
    laps = session.laps
    refs = list(zip(laps["Driver"].astype(str), laps["LapNumber"].astype(int)))[: args.files]
    requests = [(args.year, session.gp, session.session, f"{d}/{n}_tel.json") for d, n in refs]
    versions: dict[str, int] = {}
    http = http_session()
    orig_get = http.get

    def counting_get(url, **kw):
        resp = orig_get(url, **kw)
        v = str(getattr(resp, "http_version", None))
        versions[v] = versions.get(v, 0) + 1
        return resp

    http.get = counting_get  # type: ignore[method-assign]
    t0 = time.perf_counter()
    results = asyncio.run(
        fetch_multiple_async(requests, use_cache=False, write_cache=False, validate_payload=False)
    )
    wall = time.perf_counter() - t0
    ok = sum(1 for r in results if isinstance(r, dict))
    print(
        json.dumps(
            {
                "variant": args.variant,
                "wall_s": round(wall, 3),
                "ok": ok,
                "n": len(requests),
                "versions": versions,
                "cdn_failures": dict(get_cdn_manager()._failure_counts),
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--files", type=int, default=150)
    parser.add_argument("--rounds", type=int, default=0)
    parser.add_argument("--variant", default="h3")
    parser.add_argument("--out", default=".agents/perf-argos/a7_h3_ab.json")
    args = parser.parse_args()
    if args.rounds == 0:
        single_run(args)
        return
    records = []
    for rnd in range(args.rounds):
        for variant in ("h3", "h2") if rnd % 2 == 0 else ("h2", "h3"):
            cmd = [sys.executable, __file__, "--variant", variant, "--files", str(args.files)]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            line = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "{}"
            rec = (
                json.loads(line)
                if line.startswith("{")
                else {"variant": variant, "error": proc.stderr[-500:]}
            )
            rec["round"] = rnd
            records.append(rec)
            print(rec, file=sys.stderr)
    summary = {}
    for variant in ("h3", "h2"):
        walls = [r["wall_s"] for r in records if r.get("variant") == variant and "wall_s" in r]
        summary[variant] = {"median": statistics.median(walls) if walls else None, "values": walls}
    Path(args.out).write_text(json.dumps({"records": records, "summary": summary}, indent=2))
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
