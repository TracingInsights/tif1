"""Naxos N9: print the timeline of traces written by ``naxos_n9_trace.py --out``."""

from __future__ import annotations

import collections
import sys
from pathlib import Path

import orjson


def main() -> None:
    """Summarize each run of each trace file given on the command line."""
    for f in sys.argv[1:]:
        for run in orjson.loads(Path(f).read_bytes()):
            t = run["trace"]
            print(Path(f).name)
            print("  events:", [tuple(e) for e in t["events"]])
            print("  writers:", t["writers"])
            r = sorted(t["responses"], key=lambda x: x[1])
            b: collections.Counter = collections.Counter()
            for x in r:
                if x[2] > 0:
                    b[int(x[1] // 500)] += x[2]
            print(
                "  MB decoded per 0.5 s:",
                [(k * 0.5, round(v / 1e6, 1)) for k, v in sorted(b.items())],
            )
            dec = sorted(x[1] - x[0] for x in r if x[2] > 0)
            if dec:
                print(
                    "  decode ms med/p90/max",
                    dec[len(dec) // 2],
                    dec[int(len(dec) * 0.9)],
                    dec[-1],
                )
            s = t["samples"]
            prev = None
            out = []
            for x in s[::10]:
                c = sum(x["cpu"].values())
                if prev:
                    out.append(
                        (
                            round(x["t"] / 1000, 1),
                            round((c - prev[1]) / ((x["t"] - prev[0]) / 1000), 2),
                        )
                    )
                prev = (x["t"], c)
            print("  cores busy per ~0.5 s:", out)
            if t.get("stacks") and len(sys.argv) > 1:
                _stack_profile(t)


def _stack_profile(t: dict) -> None:
    """Most frequent leaf-ish frames of the main and writer threads after the wave."""
    ev = {e[0]: e[1] for e in reversed(t["events"])}
    ends = [e[1] for e in t["events"] if e[0] == "fetch_batch_end"]
    start = max(ends) if ends else 0.0
    stop = ev.get("in_process_end", 1e12)
    for who in ("main", "tif1-frame-write"):
        leaf: collections.Counter = collections.Counter()
        incl: collections.Counter = collections.Counter()
        n = 0
        for ts, st in t["stacks"]:
            if not (start <= ts <= stop) or who not in st:
                continue
            n += 1
            chain = st[who]
            leaf[" <- ".join(chain[:3])] += 1
            for fr in set(chain):
                incl[fr.rsplit(":", 1)[0]] += 1
        print(f"  [{who}] samples after the wave: {n}")
        for k, v in leaf.most_common(12):
            print(f"     leaf {v:4d}  {k}")
        for k, v in incl.most_common(25):
            print(f"     incl {v:4d}  {k}")


if __name__ == "__main__":
    main()
