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
| E3 | GC freeze/disable around batch fetch | allocation/GC pauses during fetch | MEASURED: REJECTED (+15.6% median; 3/5 interleaved pairs worse — with E1's offload in place GC no longer bites, and skipping collection during the batch hurt) |
| E4 | Fix `retry_jitter_max` default validation (per-request logger.warning on the loop) | loop-side logging + retry-delay correctness | MEASURED: KEPT (-15.0% median total, -18.6% telemetry; also fixes retry jitter silently inflated 0 -> 1.0 s) |
| E5 | Memoize Config validation lookups | per-request config.get validation | MEASURED: REJECTED (-1.8% median, 2/5 pairs — within noise; not worth the stale-memo hazard) |
| E6 | Vectorized (driver, lap) ref extraction (iterrows → vector ops) | ref extraction ~0.2 s | MEASURED: KEPT (8/10 pairs faster over two rounds; median paired delta -3.1 s) |
| E7 | Disable HTTP/3 (h2 only) | transport CPU per request | REJECTED during screening: the `http_disable_http3` knob does not prevent h3 negotiation (requests still h3; wall unchanged) — no implementable/behavior-changing variant |
| E8 | max_concurrent_requests 22 → 44 | fetch concurrency | MEASURED: REJECTED (4/5 pairs worse, +35% aggregate median; 44 amplifies the straggler regime, matching the earlier H7 finding that 64 lost to 22) |
| E9 | StaticDelivr-first CDN order | transport latency | MEASURED: KEPT (steady-state 4/5 pairs faster, aggregate median -53.5%: B 7.8-11.1 s vs A 15.5-24.5 s; first cold-edge touch is slower — see caveat) |
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

## Experiment 3 (E3): pause GC during batch fetch — REJECTED

`_gather_with_gc_pause` disabled the GC for batches >= 64 requests
(restored in finally). Interleaved A/B vs the E1+E2 state: median +15.6%
(15.17 -> 17.53 s), 3/5 pairs worse. With decode/parse already off the
event loop (E1), gen2 collections no longer stall the batch; skipping
collection during the batch only grew the heap. Change reverted; no PR.

## Experiment 4 (E4): `retry_jitter_max` default fails its own validation — KEPT

The shipped default is 0.0 but the float-validation group required > 0, so
every `config.get("retry_jitter_max")` on the fetch path emitted
`Invalid retry_jitter_max=0.0, using default=1.0` — ~1450 warnings per cold
full-telemetry fetch (logging machinery + a blocking stderr pipe write per
warning), and it silently replaced the configured 0.0 with 1.0, inflating
every retry backoff by up to +1 s. Fix: 0.0 is valid (jitter disabled); only
negative/non-numeric values are rejected.

Interleaved A/B vs the E1+E2 state:

| variant | runs (total_s) | median | min |
|---------|----------------|-------:|----:|
| A | 19.23, 16.91, 23.82, 20.75, 17.03 | 19.23 | 16.91 |
| B | 17.42, 26.55, 16.34, 13.72, 14.62 | **16.34** | **13.72** |

4/5 pairs favor B (the B-side 26.6 s run is the known straggler regime);
median **-15.0%**, telemetry median **-18.6%**.
Verification: config/retry unit + property suites pass (120 tests); ruff
clean; 0.0 now returned unchanged, -5 still rejected with a warning.
Files: `src/tif1/config.py`.

## Experiment 5 (E5): memoize Config.get validation — REJECTED

Memoized validation results per (key, default) with set() invalidation.
Interleaved A/B vs the E1+E2+E4 state: median -1.8%, 2/5 pairs — within
noise. With E4 already removing the per-request warning, the remaining
validation cost is microseconds. Not worth the stale-memo hazard for
callers that mutate `_config` directly. Change reverted; no PR.

## Experiment 6 (E6): vectorized lap-ref extraction — KEPT

`fetch_all_laps_telemetry_async` built (driver, lap) refs with
`laps.iterrows()` — a per-row Series construction (~200 ms and thousands of
allocations for ~1455 rows). Replaced with vectorized column selection +
dropna + astype (identical refs, order preserved).

Two interleaved A/B rounds vs the E1+E2+E4 state (10 pairs): 8/10 pairs
favor B; paired deltas (b-a): -0.30, -0.31, +3.87, -1.10, -36.57, -14.15,
-6.32, -5.07, +0.42, -15.58; median paired delta -3.1 s (round-2 aggregate
median -12.4%). Verification: core/parallel-fetch suites pass (63 tests);
ruff clean. Files: `src/tif1/core.py`.

## Experiment 7 (E7): disable HTTP/3 — REJECTED during screening

`TIF1_HTTP_DISABLE_HTTP3=true` (passed to `niquests.Session`) did not
change the negotiated protocol: 1436/1461 requests still used h3 in the
instrumented diagnostic, and the full-workload wall time was identical
(7.52 s vs the 7.4-7.7 s control band). With no implementable h2-only
variant, the hypothesis is not measurable; no code change, no PR.

## Experiment 8 (E8): max_concurrent_requests 22 -> 44 — REJECTED

Interleaved A/B vs the E1+E2+E4+E6 state: paired deltas -17.56, +1.18,
+33.54, +11.92, +1.34 — 4/5 pairs worse, aggregate median +35.5%, and the
candidate hit a 51.6 s collapse. Higher concurrency amplifies the
straggler/origin-error regime; matches the earlier H7 finding (64 lost to
22 on live CDN). Reverted; no PR.

## Experiment 9 (E9): StaticDelivr-first CDN order — KEPT (with caveat)

Motivation: prior single-file medians from this sandbox had StaticDelivr at
1.7 ms vs jsDelivr 7.9 ms (warm), but the shipped default is jsDelivr
primary. Measured at batch scale (interleaved A/B vs the E1+E2+E4+E6
state):

| variant | runs (total_s) | median |
|---------|----------------|-------:|
| A (jsDelivr first) | 26.24, 17.74, 17.46, 15.53, 24.49 | 17.74 |
| B (StaticDelivr first) | 97.65, 11.09, 8.24, 8.13, 7.85 | **8.24** |

Aggregate median **-53.5%**; steady-state B runs are 7.8-11.1 s vs A's
15.5-24.5 s (4/5 pairs). Caveat, disclosed for review: the very first B
run (97.6 s) paid a one-time StaticDelivr edge-cache warm-up — jsDelivr's
globally shared cache often arrives pre-warmed for popular files, while a
smaller CDN's regional edges may be cold on first touch. Steady-state
users (repeat/other-session fetches) get the 2-3x win; the very first
cold-edge fetch per region can be slower. The 404 fall-through chain is
unchanged, so correctness never depends on the order.

Verification: full unit suite passes after updating the two CDN-order
contract tests (`test_default_sources_order...`,
`test_empty_list_falls_back_to_defaults`) and the CDNManager fallback
list; ruff clean. Files: `src/tif1/config.py`, `src/tif1/cdn.py`,
`tests/unit/test_cdn.py`.
