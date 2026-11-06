Corinth performance round (C1-C10): user flows earlier rounds never measured. Full results: `.agents/perf-corinth/RESULTS.md`; one report per hypothesis in `.agents/perf-corinth/C<k>.md`.

## Combined, cold (fresh process, empty cache, live CDN, 3 interleaved reps, all won)

| flow | before | after |
|---|---|---|
| season loop, 24 races, laps + results, pandas | 3673 ms | 2225 (-39%) |
| same, polars | 2844 | 2017 (-29%) |
| 1998 Monaco (Ergast-only), pandas | 2023 | 559 (-72%) |
| same, polars | 2304 | 478 (-79%) |
| `tif1.ergast` queries | 1358 | 630 (-54%) |
| current-season race, pandas | 1203 | 585 (-51%) |
| same, polars | 960 | 367 (-62%) |

Warm workloads are unchanged (8 + 12 pairs within noise).

## Shipped

| item | effect |
|---|---|
| C1 season lookahead (`session_lookahead`, on) | round r right after r-1 prefetches round r+1's files: season loop -35..-41% |
| C2 pre-2018 sessions | no requests for data that does not exist before 1996/2011, one first wave: 2.2-5.4x |
| C3 Ergast interface | Stats files over the stdlib client while pandas imports: cold queries -28..-46% |
| C4 current-season freshness | no niquests import for refreshes; settled results kept permanently: expired -14..-44%, cold -29..-48% |
| C5 Python 3.14 | GC promotion on 3.14.5+/3.15: pandas all_tel -5%, polars laps -11% |
| C6 plotting | `import tif1.plotting` 0.5-0.8 s -> 0.02 s; first chart 2.4 -> 1.3 s, figures pixel-identical |
| C7 CLI | session commands 0.31-0.70x; `tif1 fastest` fix on polars installs |
| C8 tel.v2 decode | one native call, GIL released: race 472 -> 74 ms on 8 threads (needs tif1-native 0.3.0) |
| C9 schedule | `get_event_schedule(..., lib="polars")`: -53..-55%, no pandas import |
| C10 session bundles (`session_bundle`, off) | one request per session once published: -15..-40% cold |

## Behaviour changes (documented)

- **Ergast caching**: a current-season results/qualifying/sprint file fetched 14 or more days after its race is cached permanently, like past seasons (the FIA right-of-review window; every 2026 file was committed once and never rewritten). The latest round keeps `ergast_ttl_hours`. `tif1 cache-clear --ergast` or `get_cache().invalidate("ergast")` picks up a later correction.
- **Fewer OCSP checks**: the calendar fetch, the Ergast refreshes and `tif1.ergast` queries go over the stdlib client (no niquests import), which verifies the chain and host name but skips niquests' revocation check, as first-wave requests already do. `cdn_first_wave=False` sends everything through niquests.
- **CLI**: `drivers`, `results` and `fastest` load polars sessions when polars is installed, unless `TIF1_LIB` or `.tif1rc` sets `lib`; output is identical.
- New: `get_event_schedule(..., lib="polars")`, `get_events_remaining(..., lib="polars")`, config keys `session_lookahead` and `session_bundle`, `Config.is_user_set()`.

## Checks

- Unit suite 4377 passed; ruff, format, ty, pre-commit clean.
- Parity 0/133 (pandas), 0/131 (polars); perf gate 0/120 regressed.
- C8's Rust kernels are covered by the CI `native` job.

## Follow-ups

- Release tif1-native 0.3.0 (N7 + C8 kernels), then publish `tel.v2` files and `session.v1.bin` bundles from the data pipeline (`tools/data/encode_driver_telemetry.py --verify`, `tools/data/encode_session_bundle.py --verify`) and turn `telemetry_columnar` / `session_bundle` on.
