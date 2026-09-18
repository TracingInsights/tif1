# Ithome series — 10 new performance hypotheses (2026-09-16)

Prior series (NOT re-run): H1-H11 (.agents/perf), E1-E10 (.agents/perf-monaco),
F1-F10 + G1-G10 (.agents/perf-fab5), K1-K10 (.agents/perf-kalchedon),
L1-L10 (.agents/perf-sestos), N1-N10 (.agents/perf-lindos),
R1-R10 (.agents/perf-rhodes), T1-T10 (.agents/perf-tyria).
Kleitor's superseded G-plan (claim-dedupe, polars single-shot laps, telemetry-first
overlap) was planned but never measured; the unmeasured items are covered here as
U3/U6/U9 with fresh evidence.

Environment: 1 CPU (Modal burstable), Python 3.12, pandas 3.0.5, polars 1.44.1.
Benchmark corpus: 2026 Monaco GP Race telemetry (~1452 payloads / 84 MB raw /
24 MB frame tier), same as all prior series.

## Baselines (measured this session, /tmp/perf-u1 warm cache)

- import tif1: 47 ms (of which importlib.metadata ~37 ms)
- import tif1.core: 313 ms (pandas 306 ms — not deferrable, models.py subclasses pd.DataFrame)
- import tif1.cli: 387 ms
- Warm pandas: get_session 0.33 s | laps 0.067 s | telemetry 0.77-0.86 s | total ~1.2 s
  - frames_batch = 805 ms of 812 ms telemetry phase: SQL ~55 + zstd ~200 + pickle ~300 + rest allocator noise
- Warm polars (frames tier primed): get_session 0.43 | laps 0.039 | telemetry 0.248 | total 0.72 s
- Cold live: edge-dependent (11-35 s); laps load triggers 128-wide background prefetch
  that duplicates every explicit 22-wide telemetry download (measured live:
  2925 requests / 1463 unique = 1462 duplicate downloads)
- Cold assembly (_create_telemetry_df × 1452): 2136 ms; to_timedelta(list) = 790 µs/frame
- Laps.telemetry merged path (78 payloads): merge 4.7 ms + build 49 ms (+payload-tier reads)

## Hypotheses

| # | Change | Measured with |
|---|--------|---------------|
| U1 | Lazy `tif1.__version__` via PEP 562 (drop eager importlib.metadata call) | importtime A/B |
| U2 | LZ4 codec for materialized frame tier (pandas+polars), zstd legacy fallback | offline tight-loop 391→283 ms (−28%); warm benchmark A/B |
| U3 | Background-prefetch dedupe: cold = explicit fetch joins in-flight prefetch (kill 2x download); warm = prefetch consults frame tier first | live request counts + cold A/B + warm CPU |
| U4 | `pd.to_timedelta(np.asarray(v), unit="s")` in `_typed_telemetry_frame` (parity-verified) | offline assembly 2136→1896 ms (−11.2%), 1452/1452 parity |
| U5 | Post-return background frame-tier write (join at close/atexit) | live cold benchmark, full-process |
| U6 | Polars laps single-shot dict-of-lists assembly (kleitor G8) | offline polars assembly A/B |
| U8 | `Laps.telemetry` merged-result memoization | offline repeat A/B |
| U9 | Overlap weather/rcm table fetches with telemetry batch (kleitor G9) | live cold A/B |
| U10 | Lazy CLI `app` via PEP 562 | import A/B |
| U11 | `Laps.telemetry` merged path routes through memoized frames/frame tier first | offline + warm A/B |

## Pre-rejected (measured this session, not hypotheses)

- Merged-loop numpy concatenation: `_merge_telemetry_payloads` is 4.7 ms for 78
  payloads (list.extend is already C-speed); the 49 ms `_telemetry_frame_from_merged`
  build is dtype-contract work. No headroom.
- Lazy pandas in tif1.core: impossible without restructuring (models.py subclasses
  pd.DataFrame at class-creation time).
- Rust/pyo3 re-attempt: cargo is unavailable in-sandbox and N8/N9 already measured
  real pyo3 parse at parity with orjson; remaining costs are network-edge and
  pandas-object reconstruction, not parseable JSON.

## Method

- Offline deterministic: real payloads from the primed payload tier, GC-managed
  tight loops, parity gates on full 1452-frame sets.
- Warm A/B: `tools/warm_cache_benchmark.py` against /tmp/perf-u1, fresh process per run.
- Cold A/B: `tools/monaco_telemetry_cold_benchmark.py` (fresh process + throwaway
  cache per run) + `tools/monaco_ab_suite.py` interleaving when edge warmth varies.
- Keep = improvement beyond noise with parity (full or targeted suite green).
