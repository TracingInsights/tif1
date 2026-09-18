# Halikarnassos series — 11 performance hypotheses (2026-09-18)

Prior series (NOT re-run): H1-H11 (.agents/perf), E1-E10 (.agents/perf-monaco),
F/G (.agents/perf-fab5), K1-K10 (.agents/perf-kalchedon), L1-L10
(.agents/perf-sestos), N1-N10 (.agents/perf-lindos), R1-R10 (.agents/perf-rhodes),
T1-T10 (.agents/perf-tyria), U1-U11 (.agents/perf-ithome).

Environment: Modal burstable (1 vCPU reserved / 2 burst), Python 3.12.3,
pandas 3.0.5, polars 1.44.1. Corpus: 2026 Monaco GP Race (~1452 telemetry
payloads / 84 MB raw / 31.5 MB lz4 frame tier), the same corpus as all prior
series. Warm cache: /tmp/perf-v1 (primed by tools/warm_cache_benchmark.py
--prime, 1452 frames).

## Baseline (measured this session, fresh process per run)

- import tif1: 13.1-13.6 ms (of which stdlib `logging` chain ~9.5 ms)
- import tif1.core: ~340 ms (pandas import; not deferrable per Ithome)
- Warm pandas: get_session 0.34-0.53 s (pandas import + ~20 ms work)
  | laps 0.067 s | telemetry 0.67-0.75 s | total ~1.15-1.30 s
- Warm telemetry breakdown (cProfile): unpickle ~550 ms + lz4 decompress
  ~170 ms + SQL ~46-60 ms; `pd.DataFrame.__setstate__` machinery dominates.
- Cold assembly (_create_telemetry_df x 1452): ~1031 us/frame = 1.50 s;
  stage breakdown: pd.DataFrame construct 565 us, typing loop 308 us
  (to_timedelta 142 us, Int64 pd.array 55 us, Driver Series 38 us,
  LapNumber 29 us), remainder ~160 us.
- Warm laps breakdown: _process_lap_df 73 ms of 106 ms phase, of which
  _replace_null_like_strings 38 ms and dtype pass 58 ms.
- First get_event_by_name per process: 14.6 ms (rapidfuzz import + fuzzy);
  repeat 0.3 ms.

## Hypotheses (all new; each measured before shipping)

| # | Change | Evidence at planning time | Measured with |
|---|--------|---------------------------|---------------|
| V1 | `_typed_telemetry_frame`: numeric-channel ndarray preconversion + direct IntegerArray for nGear/DRS/LapNumber | Offline: construct 565->409 us/frame with exact dtype/value parity; Int64 55->32 us; LapNumber 29->4 us | Offline full-set parity gate (1452 frames) + assembly A/B + live cold A/B |
| V2 | `to_timedelta` -> `TimedeltaIndex(seconds*1e9)` | Offline: 139 vs 143 us — and value parity FAILS (float ns rounding) | Offline parity harness — REJECT |
| V3 | `_replace_null_like_strings`: hoist the arrow probe conversion + cheaper per-column probe | 38 ms of the 73 ms laps dtype pass; string_arrow._from_sequence x43 = per-column probe re-conversion | Offline laps A/B + warm e2e A/B |
| V4 | Merged-path (Laps.telemetry) gets the V1 fast paths (ndarray preconvert + direct Int64) | Same construction costs as V1 on the 78-payload merged build (~49 ms) | Offline merged A/B + Laps.telemetry first-access A/B |
| V5 | Cache init lazy DDL/PRAGMA | Cache() on an existing dir = 0.5 ms (warm path); DDL 8.8 ms only on first-ever run | Measured — REJECT (nothing on the warm path) |
| V6 | `import tif1` further trim | 13.1 ms total, ~9.5 ms is stdlib logging chain imported by every module | Measured — REJECT (deferring logging is package-wide invasiveness for 9 ms) |
| V7 | SQLite page-cache PRAGMA for the frame-tier read | Interleaved: 18 vs 18 ms (single-pass sequential scan) | Measured — REJECT |
| V8 | Overlap lz4 decompress with unpickle in a thread | Measured: lz4.frame.decompress does NOT release the GIL (overlap 209 ms ≈ serial 206 ms) | Measured — REJECT |
| V9 | Driver column as raw object ndarray (skip Series) | DataFrame infers `str` dtype from raw ndarray — breaks the object-dtype contract | Measured — REJECT |
| V10 | `_find_event_by_name`: casefold-exact pre-pass before importing rapidfuzz | First resolution 14.6 ms (rapidfuzz import) vs 0.3 ms repeat; exact names are the common case | get_session first-call A/B (fresh process) |
| V11 | `get_telemetry_frames_batch`: plain range scan + Python-side filter for large batches (IN-list kept for small) | Interleaved A/B: IN-list 46 ms vs range-scan 19 ms for 1452 refs (earlier non-interleaved probe was noise) | Warm telemetry A/B (fresh process, interleaved) |

## Pre-hypothesis probes (documented so no future series retries them)

- lz4.block vs lz4.frame codec: dec 167 vs 173 ms, enc 116 vs 115 ms — no win.
- Naive per-Series columnar elision of the frame tier: unpickle 2251-2318 ms
  vs 545 ms (4x worse) — reconfirms the N/R series columnar rejection.
- DriverAhead str->object in stored frames: unpickle 550 vs 545 ms — no win.
- `validate_telemetry_data`: 3 us/payload (4 ms total); sanitize 2 us — saturated.
- Empty-cache `get_telemetry` probe: 4 us/req (6 ms total) — saturated.
- Schedule vendored load + validate: ~2 ms total — negligible.
- Warm loads make 0 network requests (an earlier probe that showed network
  calls had set TIF1_CACHE_DIR after importing tif1 submodules, so the config
  singleton resolved a default cold dir — instrumentation pitfall documented).
- `pd.DataFrame.__setstate__` internals + unpickle dominate the warm path;
  all storage-format alternatives (columnar, elision, single-blob, Arrow,
  str->object) measured at parity or worse — the frame tier is saturated.

## Method

- Offline deterministic: real payloads/frames from the primed tiers, GC-managed
  tight loops, full-set parity gates (columns, dtypes, values, row counts).
- Warm A/B: tools/warm_cache_benchmark.py against /tmp/perf-v1, fresh process
  per run, interleaved arms, medians reported.
- Cold A/B: fresh process + throwaway TIF1_CACHE_DIR per run, interleaved.
- Keep = improvement beyond run-to-run noise with exact parity + tests green.
