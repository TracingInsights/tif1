# E2 intended commits (git commit was denied in the sandbox; everything is staged)

1. `src/tif1/`, `tests/unit/`:

```
perf(elis-E2): cold first wave over the stdlib, niquests imported on demand

get_session fetches drivers.json and the session laptimes over http.client
(niquests' trust store, User-Agent, gzip) in daemon threads while pandas or
polars imports; the async wave and the sync transport take those bodies
before touching niquests, and single sync requests ride the kept-alive
stdlib connections until a niquests session exists. Anything but a 200
JSON object goes through niquests as before. Cold laps -33% on both
backends, cold fastest_lap_tel -21..-30%; cold driver_tel +2..12% (the
OCSP check no longer overlaps the pandas import).
```

2. `.agents/perf-elis/`, `tools/perf/elis_e2_*.py`:

```
perf(elis-E2): report, A/B results and measurement tools
```
