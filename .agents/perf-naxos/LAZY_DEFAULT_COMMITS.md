# Intended commit (git commit was denied in this sandbox; everything else is staged)

Branch: perf/naxos-lazy-default (base d28ce70)

```
feat: lazy telemetry by default in Session.load()

load_telemetry_lazy now defaults to True: load() and load(telemetry=True)
fetch laps, weather and messages, and a driver's telemetry on the first
access of one of its laps (one parallel wave). telemetry="eager" (new) or
load_telemetry_lazy=False keeps fastf1's eager load. Frames are identical.

- A lazy load() of a session without laps no longer raises KeyError.
- fetch_all_laps_telemetry() keeps laps-row order when memoized frames and
  frame-tier hits mix (warm polars session after a lazy load).
- The perf gate's notebook workload pins telemetry="eager" (its baseline).
- Docs: the full list of differences from fastf1 in COMPATIBILITY.md and the
  migration guide; load() reference, config, env vars, caching, errors, FAQ.
```
