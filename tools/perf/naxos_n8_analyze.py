"""Naxos N8: per-run breakdown of traced cold one-driver waves (see naxos_n8_trace.py)."""

import json
import statistics
import sys
from pathlib import Path

for fn in sys.argv[1:]:
    recs = json.loads(Path(fn).read_text(encoding="utf-8"))
    print(fn)
    for i, r in enumerate(recs):
        ev = r["events"]
        ds = next(e["t"] for e in ev if e["kind"] == "phase_start" and e["name"] == "driver_tel")
        de = next(e["t"] for e in ev if e["kind"] == "phase_end" and e["name"] == "driver_tel")
        fm = [e for e in ev if e["kind"] == "fetch_multiple" and e["n"] > 10]
        tel = [e for e in ev if e["kind"] == "stdlib" and e["path"].endswith("_tel.json")]
        niq = [e for e in ev if e["kind"] == "niq_body" and e["path"].endswith("_tel.json")]
        new = [e["t"] - e["start"] for e in tel if e["new_conn"]]
        old = [e["t"] - e["start"] for e in tel if not e["new_conn"]]
        conn = [e["connect"][1] - e["connect"][0] for e in tel if e["connect"]]
        ttfb_new = [e["ttfb"] - e["start"] for e in tel if e["new_conn"] and e["ttfb"]]
        ttfb_old = [e["ttfb"] - e["start"] for e in tel if not e["new_conn"] and e["ttfb"]]
        last_end = max([e["t"] for e in tel + niq] or [0])
        wave_start = fm[0]["start"] if fm else ds
        wave_end = fm[0]["t"] if fm else de
        miss = sum(1 for e in tel if (e.get("headers") or {}).get("x-cache", "").endswith("MISS"))
        md = lambda xs: statistics.median(xs) if xs else 0  # noqa: E731
        print(
            f" {i:2d} wall={r['process_wall']:5.0f} tel={de - ds:5.0f} pre={wave_start - ds:4.0f}"
            f" wave={wave_end - wave_start:5.0f} lastbody->fm_end={wave_end - last_end:4.0f}"
            f" post={de - wave_end:4.0f}  new={len(new):2d} med={md(new):4.0f} max={max(new or [0]):4.0f}"
            f" conn={md(conn):3.0f} ttfb_new={md(ttfb_new):4.0f}  old={len(old):2d} med={md(old):4.0f}"
            f" ttfb_old={md(ttfb_old):4.0f} niq={len(niq)} edge_miss={miss}"
        )
