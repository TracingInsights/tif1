"""Vendor compact Ergast race schedules from the Stats repository.

Reads ``{year}/events.json`` of ``TracingInsights-Archive/Stats`` for every
season from 1950 to the current year and writes
``src/tif1/data/schedules/ergast/schedule_{year}.json``: the ``Races`` list
minus the per-race ``season`` echo, minified. The files back
``get_event_schedule(year, backend="ergast")`` offline for past seasons and
the circuit columns of every schedule; the current season is fetched live
with a TTL and falls back to its vendored copy.

Usage::

    uv run python tools/vendor_ergast_schedules.py [--start 1950] [--end 2026]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

CDN = "https://cdn.jsdelivr.net/gh/TracingInsights-Archive/Stats@main"
OUT_DIR = Path(__file__).resolve().parents[1] / "src" / "tif1" / "data" / "schedules" / "ergast"
SESSION_KEYS = (
    "FirstPractice",
    "SecondPractice",
    "ThirdPractice",
    "Qualifying",
    "Sprint",
    "SprintQualifying",
    "SprintShootout",
)


def compact_race(race: dict) -> dict:
    """Keep the schedule fields tif1 reads; drop the ``season`` echo."""
    out: dict = {}
    for key in ("round", "url", "raceName", "date", "time"):
        if key in race:
            out[key] = race[key]
    circuit = race.get("Circuit")
    if isinstance(circuit, dict):
        location = circuit.get("Location", {})
        out["Circuit"] = {
            "circuitId": circuit.get("circuitId"),
            "url": circuit.get("url"),
            "circuitName": circuit.get("circuitName"),
            "Location": {
                "lat": location.get("lat"),
                "long": location.get("long"),
                "locality": location.get("locality"),
                "country": location.get("country"),
            },
        }
    for key in SESSION_KEYS:
        if key in race:
            out[key] = {k: v for k, v in race[key].items() if k in ("date", "time")}
    return out


def fetch_year(year: int) -> list[dict] | None:
    url = f"{CDN}/{year}/events.json"
    request = urllib.request.Request(url, headers={"User-Agent": "tif1-vendor/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
    except Exception as exc:
        print(f"{year}: fetch failed ({exc})", file=sys.stderr)
        return None
    races = payload.get("MRData", {}).get("RaceTable", {}).get("Races")
    return races if isinstance(races, list) else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=1950)
    parser.add_argument("--end", type=int, default=datetime.now(UTC).year)
    args = parser.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written = 0
    for year in range(args.start, args.end + 1):
        races = fetch_year(year)
        if not races:
            print(f"{year}: no races", file=sys.stderr)
            continue
        compact = {"season": year, "races": [compact_race(race) for race in races]}
        target = OUT_DIR / f"schedule_{year}.json"
        target.write_text(
            json.dumps(compact, separators=(",", ":"), ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",  # LF on Windows too: the files are committed
        )
        written += 1
        time.sleep(0.05)
    print(f"wrote {written} seasons to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
