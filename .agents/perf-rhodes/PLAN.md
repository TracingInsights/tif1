# Rhodes-series performance experiments — 2026-09-16

Ten NEW hypotheses. All prior series read and NOT re-run: H1–H11
(`.agents/perf/RESULTS.md`), E1–E10 (`.agents/perf-monaco/RESULTS.md`),
F1–F10 + G1–G10 (`.agents/perf-fab5/RESULTS.md`), K1–K10
(`.agents/perf-kalchedon/RESULTS.md`), L1–L10 (`.agents/perf-sestos/RESULTS.md`),
N1–N10 (`.agents/perf-lindos/RESULTS.md`), T1–T10
(`.agents/perf-tyria/RESULTS.md`), plus the never-run kleitor plan (folded into
K/N). Explicitly out of scope (measured before): pydantic variants, CDN
sharding/racing/order/brotli/.min.json, concurrency caps/ramps, uvloop, GC on
the cold fetch, msgpack/msgspec/Arrow/pyarrow, Rust/pyo3 (N8 measured parity),
numpy-first construction, merged-dict assembly, thread-parallel assembly,
SQLite mmap, zstd dictionaries, lazy imports (G9/H10), parallel warm decode
(K2), overlap flush-with-assembly (L8), per-request micros (F-series).

Benchmark: 2026 Monaco GP Race telemetry (~1452 payloads / 84 MB), live CDN,
fresh process per run. Warm: `tools/warm_cache_benchmark.py` (primed persistent
cache). Cold: `tools/monaco_telemetry_cold_benchmark.py` + interleaved A/B via
`tools/monaco_ab_suite.py`. Sandbox: Modal burstable (1 vCPU reserved / 2
burst).

## Current known state (from prior series, pre-Rhodes)

- Warm #2+ load: total ~1.41–1.48 s = get_session 0.40 (pandas-import bound,
  adjudicated structural) + laps 0.07 + telemetry 0.94–1.02.
- Warm telemetry phase split: frame-tier batch read ≈ 1.12 s
  (**pickle.loads 0.92 (82%)**, zstd 0.14, SQLite 0.05) + refs/memo overhead.
- Repeat `fetch_all_laps_telemetry()` in one process: 0.128 s (L3 memo hit) —
  the residual per-frame cost was never profiled.
- Cold: network-edge-bound fetch + ~2.5 s assembly + ~1.4 s post-gather writes
  (K1 flush + N1 materialization are two separate passes).

## The ten hypotheses

| # | Hypothesis | Target | Metric |
|---|-----------|--------|--------|
| R1 | Columnar frame tier v2: store raw column arrays + shared schema instead of per-frame DataFrame pickles; reconstruct frames with a direct constructor (bypass BlockManager unpickle machinery) | warm telemetry pickle.loads 0.92 s | warm telemetry phase; parity 1452/1452 (columns/dtypes/values) |
| R2 | Cache `(driver, lap)` refs on the Session + profile/fix the memo-hit loop (L3 residual 0.128 s ≈ 90 μs/frame) | repeat-call + warm refs extraction | repeat-call ms; warm telemetry phase |
| R3 | GC suspension around the warm batch read (disable/freeze during unpickle loop; E3 only measured the cold fetch) | GC pauses during 84 MB of read-path allocations | offline batch-read ms |
| R4 | Polars materialized frame tier: polars warm telemetry re-assembles from payload tier every load (frame tier is pandas-only); persist pickled polars frames | polars warm telemetry phase | polars warm load s |
| R5 | Cold orchestration overlap: extract refs from raw laptimes payloads (before `_process_lap_df`), start the telemetry fetch wave while laps assembly + weather/rcm parse still run | cold serial prelude (laps assembly before telemetry network) | interleaved cold A/B |
| R6 | Merge the weather/rcm thread-pool prefetch into the laptimes async wave (drivers is a dependency; weather/rcm are not) | one serial RTT + prefetch tail on cold | cold laps phase |
| R7 | Model-layer hot-path audit: profile `get_driver`, laps filtering, `LazyTelemetryDict`, `Telemetry`, `Lap` for repeated per-access parsing/copies | user-facing per-call costs | micro-bench (in-memory fakes) |
| R8 | Frame-tier compression tradeoff: raw pickle blobs (skip zstd, 4.3x bytes) vs zstd-1 (decompress 0.14 s) | warm read decompress | offline read ms + stored size |
| R9 | SQLite warm-path connection/transaction audit: single shared connection, one IN-query for frames, no per-phase reconnects | hidden per-phase SQLite setup | code verify + warm phases |
| R10 | Combined post-gather write pass: K1 deferred JSON flush + N1 frame materialization share one loop + one transaction on cold | ~1.4 s cold post-gather write CPU | offline write macro |

Verdict rule (same as prior series): keep only if measured, parity-safe, and
beyond the noise floor; combined single PR for everything kept.
