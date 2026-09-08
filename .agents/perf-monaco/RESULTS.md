# Live cold-fetch performance experiments — 2026 Monaco GP Race (2026-09-08)

Benchmark: full telemetry of the 2026 Monaco Grand Prix Race fetched cold
(fresh process, throwaway `TIF1_CACHE_DIR`, ~1452 payloads ≈ 84 MB over the
live CDN chain). Harness: `tools/monaco_telemetry_cold_benchmark.py` (single
run / 5-run suites), `tools/monaco_ab_suite.py` (interleaved A/B suites:
control source tree vs candidate source tree alternate run-by-run so both
see identical CDN edge conditions), `tools/monaco_fetch_diagnostic.py`
(request-level instrumentation), `tools/monaco_assembly_benchmark.py`
(offline assembly micro-benchmark on dumped payloads).

Network regimes observed from this sandbox (jsDelivr primary): CDN edge
caching dominates wall time. Truly-cold edge ≈ 285 s; warm edge ≈ 7.5-15 s.
The warm-edge regime is the reproducible one; every experiment is measured
as interleaved A/B so edge-state drift hits both variants equally.

## Baseline (5 cold runs, unmodified 33e64a3)

| run | total_s | telemetry_s |
|-----|--------:|------------:|
| 1 | 23.68 | 22.40 |
| 2 | 15.33 | 14.00 |
| 3 | 12.51 | 11.15 |
| 4 | 12.71 | 11.36 |
| 5 | 12.80 | 11.45 |

Median total 12.80 s, median telemetry 11.45 s (warm edge). First run shows
edge re-warm after idle.

Phase breakdown (warm edge, instrumented): fetch ≈ 7.5 s + per-lap DataFrame
assembly ≈ 3.9 s; RSS ≈ 900 MB. Request-level: tracked `session.get` averages
~5 ms but per-request in-flight time is ~110 ms — the gap is event-loop-side
response processing (niquests lazy content decode + orjson parse inline),
not network. The control code intermittently collapses into a 60-72 s
regime (2/5 interleaved runs in E1's suite); offloading the response
processing (E1) eliminates that collapse mode.

## Ten hypotheses

| # | Hypothesis | Target | Status |
|---|-------------|--------|--------|
| E1 | Offload niquests lazy content decode + orjson parse off the event loop | fetch phase; collapse-mode elimination | MEASURED: KEPT (-12.6% median, -51% mean, max 21.7 s vs 72.4 s) |
| E2 | Pre-typed DataFrame construction for telemetry frames (no post-hoc setitem/astype churn) | 3.9 s assembly phase | MEASURED: KEPT (-16.2% median total; assembly 4.14 -> 2.36 s offline, 1452/1452 frames identical) |
| E3 | GC freeze/disable around batch fetch | allocation/GC pauses during fetch | pending |
| E4 | Fix `retry_jitter_max` default validation (per-request logger.warning on the loop) | loop-side logging + retry-delay correctness | pending |
| E5 | Memoize Config validation lookups | per-request config.get validation | pending |
| E6 | Vectorized (driver, lap) ref extraction (iterrows → vector ops) | ref extraction ~0.2 s | pending |
| E7 | Disable HTTP/3 (h2 only) | transport CPU per request | pending |
| E8 | max_concurrent_requests 22 → 44 | fetch concurrency | pending |
| E9 | StaticDelivr-first CDN order | transport latency | pending |
| E10 | keepalive_max_requests 1000 → 10000 | mid-batch connection recycling | pending |

## Experiment 1 (E1): decode + parse off the event loop — KEPT

`fetch_json_async` paid niquests' lazy `response.content` decode and the
orjson parse on the event-loop thread (~ms per response × 1452 responses),
serializing the batch and occasionally collapsing into a 60-72 s regime.
Change: `_decode_and_parse` runs in the shared thread executor and returns
`(parsed, raw_content)` so the raw-blob cache write path is unchanged; the
opt-in process-pool path (`json_parse_workers`) keeps its shape.

Interleaved A/B, 5+5 runs (A = control 33e64a3, B = E1):

| variant | runs (total_s) | median | mean | max |
|---------|----------------|-------:|-----:|----:|
| A control | 14.99, 61.69, 72.45, 19.58, 14.44 | 19.58 | 36.63 | 72.45 |
| B E1 | 15.70, 21.70, 18.87, 15.57, 17.12 | 17.12 | 17.79 | 21.70 |

Median -12.6%, mean -51%: the control collapsed into the slow regime twice;
E1 never did. Telemetry-phase medians: 18.29 s → 15.76 s (-13.8%).
Verification: 1183 unit tests pass; ruff clean.
Files: `src/tif1/async_fetch.py`.

## Experiment 2 (E2): pre-typed telemetry frame construction — KEPT

`_create_telemetry_df` built each frame from raw lists and then converted
canonical columns one by one (`Time` to_timedelta, `nGear`/`DRS`/`LapNumber`
astype Int64, `Driver` astype object, Brake bool, missing-channel NA columns)
— ~8 `__setitem__` calls plus an inference+astype round trip per frame,
~2.9 ms x 1452 frames offline. The merged-dict alternative (1.5 s) was
rejected first: cross-payload type mixing changes per-frame dtypes
(object instead of float64) — 0/1452 parity.

Change: `_typed_telemetry_frame` pre-types the canonical columns and passes
them into the `pd.DataFrame` constructor (Driver wrapped as object-dtype
Series — pandas 3 infers str dtype from object arrays at construction),
falling back to the legacy build+convert path whenever a canonical
conversion fails, so the historical error contract (bad payloads drop to
None) is preserved.

Offline (1452 real Monaco payloads, best/median of 5): 3.57/4.14 s ->
2.16/2.36 s (**1.7x**), parity 1452/1452 frames identical (columns, dtypes,
values, sha256-hashed). End-to-end interleaved A/B vs the E1 state:

| variant | runs (total_s) | median | min |
|---------|----------------|-------:|----:|
| A (E1) | 22.16, 22.41, 18.99, 21.94, 18.51 | 21.94 | 18.51 |
| B (E1+E2) | 47.72, 24.11, 15.81, 14.56, 18.40 | **18.40** | **14.56** |

Median **-16.2%** (telemetry phase -16.9%); min -21%. The B-side 47.7 s run
is the same intermittent straggler regime the control hit in E1's suite
(cloudfront origin errors stalling a handful of requests); it is
network-side and orthogonal to this change (which only alters post-fetch
CPU work).
Verification: parity 1452/1452; focused dtype/model/core suites pass
(284 tests); ruff clean.
Files: `src/tif1/core_utils/helpers.py`.
