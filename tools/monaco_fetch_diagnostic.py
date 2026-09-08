"""Diagnostic: request-level view of a cold telemetry batch fetch (production path).

Builds the exact production workload (session.laps -> (driver, lap) refs),
then fetches N telemetry payloads through ``tif1.async_fetch.fetch_multiple_async``
with the shared niquests session's ``get`` wrapped to record per-request
start/end, duration, HTTP version, status and bytes.

Prints one JSON object: wall time, request count, throughput, latency
percentiles, HTTP-version/status histograms, per-second completion timeline.

Examples:
    uv run python tools/monaco_fetch_diagnostic.py --files 60
    TIF1_HTTP_MULTIPLEXED=false uv run python tools/monaco_fetch_diagnostic.py --files 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
import time

os.environ.setdefault("TIF1_CACHE_DIR", tempfile.mkdtemp(prefix="tif1-diag-"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--gp", default="Monaco Grand Prix")
    parser.add_argument("--session", default="Race")
    parser.add_argument("--skip", type=int, default=0, help="skip first N refs")
    args = parser.parse_args()

    import tif1
    from tif1.async_fetch import fetch_multiple_async
    from tif1.http_session import get_session

    session = tif1.get_session(args.year, args.gp, args.session)
    laps = session.laps

    refs: list[tuple[str, int]] = []
    for _, row in laps.iterrows():
        driver = row.get("Driver")
        lap_num = row.get("LapNumber")
        if driver is not None and lap_num is not None:
            refs.append((str(driver), int(lap_num)))
    refs = refs[args.skip : args.skip + args.files]
    if not refs:
        raise SystemExit("no refs to fetch")

    shared = get_session()
    records: list[dict] = []
    errors: list[str] = []
    orig_get = shared.get

    def tracked_get(url, **kwargs):
        t0 = time.perf_counter()
        try:
            resp = orig_get(url, **kwargs)
        except Exception as e:
            errors.append(f"{type(e).__name__}: {str(e)[:200]}")
            raise
        t1 = time.perf_counter()
        content = getattr(resp, "content", b"")
        headers = getattr(resp, "headers", {}) or {}
        records.append(
            {
                "t0": t0,
                "t1": t1,
                "s": round(t1 - t0, 4),
                "status": resp.status_code,
                "http_version": str(getattr(resp, "http_version", "?")),
                "bytes": len(content) if content else 0,
                "host": url.split("/")[2] if "://" in url else "?",
                "cache": str(headers.get("x-cache") or headers.get("cf-cache-status") or ""),
                "age": str(headers.get("age") or ""),
            }
        )
        return resp

    shared.get = tracked_get

    requests = [
        (args.year, args.gp, args.session, f"{driver}/{lap}_tel.json") for driver, lap in refs
    ]

    async def run():
        t0 = time.perf_counter()
        results = await fetch_multiple_async(
            requests,
            use_cache=False,
            write_cache=False,
            validate_payload=False,
            max_concurrent_requests=args.concurrency,
        )
        return time.perf_counter() - t0, results

    wall, results = asyncio.run(run())
    ok = [r for r in records if r["status"] == 200]
    total_bytes = sum(r["bytes"] for r in ok)

    # Peak concurrent in-flight requests (sweep-line over start/end events).
    peak = 0
    starts = sorted(r["t0"] for r in ok)
    ends = sorted(r["t1"] for r in ok)
    if starts and ends:
        cur = 0
        events = sorted([(t, 1) for t in starts] + [(t, -1) for t in ends])
        for _, delta in events:
            cur += delta
            peak = max(peak, cur)

    versions: dict[str, int] = {}
    statuses: dict[int, int] = {}
    hosts: dict[str, int] = {}
    cache_headers: dict[str, int] = {}
    slow_hosts: dict[str, dict[str, float]] = {}
    for r in records:
        versions[r["http_version"]] = versions.get(r["http_version"], 0) + 1
        statuses[r["status"]] = statuses.get(r["status"], 0) + 1
        hosts[r["host"]] = hosts.get(r["host"], 0) + 1
        key = r["cache"] or "(none)"
        cache_headers[key] = cache_headers.get(key, 0) + 1
        slot = slow_hosts.setdefault(r["host"], {"n": 0, "sum": 0.0, "max": 0.0})
        slot["n"] += 1
        slot["sum"] += r["s"]
        slot["max"] = max(slot["max"], r["s"])
    host_summary = {
        host: {
            "n": v["n"],
            "avg_s": round(v["sum"] / v["n"], 4),
            "max_s": round(v["max"], 4),
        }
        for host, v in slow_hosts.items()
    }

    # Per-second completion timeline (bucket request completions by wall second).
    base = min(r["t0"] for r in records) if records else 0.0
    timeline: dict[str, dict[str, int]] = {}
    for r in records:
        bucket = str(int(r["t1"] - base))
        timeline.setdefault(bucket, {}).setdefault("ok" if r["status"] == 200 else "bad", 0)
        timeline[bucket]["ok" if r["status"] == 200 else "bad"] += 1

    # Straggler analysis: the last 20 records to complete, with start/end
    # offsets from the batch start (shows queue delay vs in-flight time).
    base0 = min(r["t0"] for r in records) if records else 0.0
    stragglers = [
        {
            "start_at": round(r["t0"] - base0, 2),
            "end_at": round(r["t1"] - base0, 2),
            "s": r["s"],
            "status": r["status"],
            "host": r["host"],
        }
        for r in sorted(records, key=lambda r: r["t1"])[-20:]
    ]

    durations = sorted(r["s"] for r in ok)

    def pct(p: float) -> float:
        if not durations:
            return 0.0
        return round(durations[min(len(durations) - 1, int(p * len(durations)))], 4)

    out = {
        "wall_s": round(wall, 4),
        "requests": len(records),
        "ok": len(ok),
        "results_none": sum(1 for r in results if r is None),
        "total_mb": round(total_bytes / 1e6, 2),
        "throughput_mbps": round(total_bytes / 1e6 / wall, 3),
        "req_per_s": round(len(ok) / wall, 2),
        "peak_concurrency": peak,
        "http_versions": versions,
        "statuses": statuses,
        "hosts": hosts,
        "host_latency": host_summary,
        "cache_headers": cache_headers,
        "errors": errors[:10],
        "error_count": len(errors),
        "timeline": timeline,
        "stragglers": stragglers,
        "latency": {"p50": pct(0.5), "p90": pct(0.9), "p99": pct(0.99), "max": pct(1.0)},
    }
    print(json.dumps(out))


if __name__ == "__main__":
    main()
