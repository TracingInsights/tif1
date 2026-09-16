# Tyria Series — 10 New Performance Hypotheses

Benchmark corpus: 2026 Monaco GP Race telemetry (~1452 payloads / 84MB), same as prior series.
Sandbox: 1 vCPU (bursts to 2), 8 GiB. Prior shipped work (H/E/F/G/K/L/N) is NOT repeated.

## Baseline (this series, fresh sandbox)

- Cold fetch total ~228s historically (telemetry-bound, network physics). Cold assembly ~2.4s (~1%).
- Warm #1 (frames tier): ~1.08–1.19s telemetry. Third+ loads: ~1.5s.
- Target surface: warm-path SQLite→decompress→parse→assemble slices, dtype-coercion
  microbenchmarks, fetch-path per-request overhead, and one correctness bug.

## Hypotheses

| ID | Hypothesis | Area | Status |
|----|------------|------|--------|
| T1 | Fix inverted async cache-enable condition (`core.py:3213`) | correctness/cache | SHIPPED (correctness fix) |
| T2 | Eliminate LapTime double-parse (`to_numeric` + `to_timedelta` both run fully) | laps assembly | SHIPPED (~25% on LapTime block) |
| T3 | Single-C-call `_numeric_seconds_to_timedelta` (no NaT-Series + masked `.loc` assign) | laps/weather/RCM | REJECTED (breaks NaN-guard contract; safe variant slower) |
| T4 | `format="ISO8601"`/`"mixed"` for `LapStartDate` + RCM `Time` parsing | laps/RCM | SHIPPED (pandas ~15-19%; polars: determinism only — no measurable win on locked polars 1.44.1; uniform real shapes only) |
| T5 | Single-pass null-like string normalization (exact `isin` probe before strip/lower) | laps | REJECTED (no measurable win) |
| T6 | Single-blob session frames tier (1 SELECT + 1 decompress vs 1452) | warm cache | REJECTED (~9.35x slower) |
| T7 | Batch JSON-tier cache reads for laptime waves (one `IN` SELECT vs ~24) | laps fetch | EXCLUDED (cold-SQLite-only ~37%, negligible warm benefit) |
| T8 | Merged-dict assembly for `fetch_all_laps_telemetry_async` cold path | telemetry assembly | REJECTED (already shipped) |
| T9 | Per-request fetch overhead: circuit-breaker locks, semaphore-per-task, retry setup | fetch path | REJECTED (negligible ~0.008ms/req) |
| T10 | Import-time cost: `import tif1` + first-`Session` construction overhead | startup | SHIPPED (validation/pydantic out of import tree) |

## Method

- Offline assembly hypotheses (T2–T5, T8): synthetic payloads with identical schema/row
  counts, fresh processes, interleaved A/B, parity-checked dtypes + values.
- Cache hypotheses (T6–T7): primed local SQLite cache, phase-timed.
- Fetch hypotheses (T9): small live-CDN samples (bounded, ~60 payloads).
- T1: correctness-first; measure no-regression on warm path + `enable_cache=False` isolation.
- T10: `python -X importtime` + wall clock.
- Keep bar: measured improvement + parity + full suite green (≥1293 tests, 80% cov).
- Ship: single combined PR with all kept experiments.
