"""Naxos N11: summarise a lazy-path timeline written by ``naxos_n11_cold_bench.py --trace``.

For each lazy wave: when it started (ms since interpreter start), its drivers and
laps, the foreground requests' first and last completion, the request time
distribution, how many requests went over the stdlib bridge vs niquests while
the wave ran, background requests overlapping it, the wave's wait, frame build
and frame-tier scheduling. Then the niquests warm-up and the exit hook time.

Usage:
    python tools/perf/naxos_n11_timeline.py TRACE.json [TRACE.json ...]
"""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path


def _within(ev: dict, start: float, end: float) -> bool:
    return ev["start"] >= start and ev["start"] < end


def summarize(path: str) -> None:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    events = data["events"]
    print(f"== {Path(path).name}")
    for ev in sorted(events, key=lambda e: e["start"]):
        if ev["kind"] in ("niquests_warmup", "warm_connections", "fetch_all", "stream_close"):
            print(
                f"  {ev['kind']:18s} {ev['start']:8.0f} -> {ev['end']:8.0f} ({ev['end'] - ev['start']:.0f})"
            )
    waves = sorted((e for e in events if e["kind"] == "wave"), key=lambda e: e["start"])
    reqs = [e for e in events if e["kind"] == "stream_req"]
    for wave in waves:
        s, e = wave["start"], wave["end"]
        drivers = set(wave["drivers"])
        fg = [r for r in reqs if r["key"][0] in drivers and not r["bg"] and r["end"] <= e + 1]
        bg_overlap = [r for r in reqs if r["bg"] and r["start"] < e and r["end"] > s]
        prefetched = [r for r in reqs if r["key"][0] in drivers and r["bg"]]
        waits = [x for x in events if x["kind"] == "wave_wait" and _within(x, s, e)]
        builds = [x for x in events if x["kind"] == "frames_build" and _within(x, s, e)]
        sched = [x for x in events if x["kind"] == "frames_schedule" and _within(x, s, e)]
        stdlib = [x for x in events if x["kind"] == "stdlib_get" and _within(x, s, e)]
        niq = [x for x in events if x["kind"] == "niquests_get" and _within(x, s, e)]
        durs = [r["end"] - r["start"] for r in fg]
        line = (
            f"  wave {','.join(wave['drivers'])} laps={wave['laps']} {s:7.0f}->{e:7.0f} "
            f"({e - s:.0f})"
        )
        if fg:
            line += (
                f" fg={len(fg)} first+{min(r['end'] for r in fg) - s:.0f} "
                f"last+{max(r['end'] for r in fg) - s:.0f} "
                f"req med {statistics.median(durs):.0f} max {max(durs):.0f}"
            )
        if prefetched:
            line += f" prefetched_bg={len(prefetched)}"
        line += f" bg_overlap={len(bg_overlap)} stdlib={len(stdlib)} niq={len(niq)}"
        if waits:
            line += f" wait={sum(x['end'] - x['start'] for x in waits):.0f}"
        if builds:
            line += f" build={sum(x['end'] - x['start'] for x in builds):.0f}"
        if sched:
            line += f" sched={sum(x['end'] - x['start'] for x in sched):.0f}"
        print(line)
    bg = [r for r in reqs if r["bg"]]
    if bg:
        print(
            f"  background requests: {len(bg)} "
            f"({min(r['start'] for r in bg):.0f} -> {max(r['end'] for r in bg):.0f})"
        )
    for ev in sorted((e for e in events if e["kind"] == "writer"), key=lambda e: e["start"]):
        print(
            f"  writer {ev['label']:24s} {ev['start']:8.0f} -> {ev['end']:8.0f} "
            f"({ev['end'] - ev['start']:.0f})"
        )
    print(f"  exit hook at {data['exit_hook']:.0f}")


def main() -> None:
    for path in sys.argv[1:]:
        summarize(path)


if __name__ == "__main__":
    main()
