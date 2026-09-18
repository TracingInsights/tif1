# Halikarnassos Series — Results (11 hypotheses, all run)

Corpus: 2026 Monaco GP Race telemetry (~1452 payloads / 84 MB raw /
31.5 MB lz4 frame tier), the standard set of every prior series (H/E/F/G/
K/L/N/R/T/U — none re-run; see PLAN.md for the inventory). Sandbox: Modal
burstable 1 vCPU reserved / 2 vCPU burst, Python 3.12.3, pandas 3.0.5,
polars 1.44.1. Warm cache: /tmp/perf-v1 (1452 materialized frames).

## Baseline (measured this session, fresh process per run)

| Metric | Value |
|---|---:|
| `import tif1` | 13.1-13.6 ms (~9.5 ms of it: stdlib logging chain) |
| `import tif1.core` | ~340 ms (pandas; not deferrable — Ithome verdict) |
| Warm get_session | 0.34-0.44 s (pandas import + ~20 ms work) |
| Warm laps | 0.067 s |
| Warm telemetry | 0.67-0.80 s (unpickle ~550 + lz4 ~170 + SQL ~46-60 ms) |
| Warm total | ~1.15-1.30 s |
| Cold assembly (`_create_telemetry_df` x 1452) | ~1031 us/frame = 1.50 s |
| First `get_event_by_name` per process | 14.6 ms (rapidfuzz import + fuzzy) |

## Kept (shipped)

| ID | Change | Measured result | Parity |
|----|--------|-----------------|--------|
| V1 | `_typed_telemetry_frame`: numeric channels pass preconverted ndarrays to the constructor; `nGear`/`DRS` build `IntegerArray` directly from int ndarrays (zero mask); `LapNumber` via `np.full` + zero-mask `IntegerArray` | Offline full-set assembly **1568 → 1397 ms (−10.9%)** over 1452 frames; per-frame 1031 → ~920 us | **1452/1452 frames byte-identical** (columns, dtypes, values, row counts) vs the pre-V1 semantics; merged path untouched |
| V10 | `_find_event_by_name`: casefold-exact pre-pass returns exact names without importing rapidfuzz | Warm `get_session` **0.435 → 0.339 s median (−22.1%)**, n=8/arm interleaved; first resolve 14.6 → ~1 ms | Exact names return the same event the fuzzy path would (ratio 100 → exact, no warning); near-miss names still fuzzy |
| V11 | `get_telemetry_frames_batch`: whole-session batches (>= 50% of the session's frames, gated by one COUNT) read via one range scan + Python-side membership filter; small batches keep the IN query | SQL-only interleaved **46 → 19 ms (−59%)** for the 1452-ref batch; full batch-read parity in-process (397 vs 404 ms — unpickle dominates) | Same rows returned; partial batches never scan the session partition |

### Live cold A/B (3 runs/arm, interleaved, `tools/monaco_ab_suite.py`)

| Arm | total median | telemetry median |
|---|---:|---:|
| baseline | 10.668 s | 9.693 s |
| candidate | **10.084 s (−5.48%)** | **8.743 s (−9.79%)** |

The cold phase is CDN-edge-noisy (prior series saw 9-35 s spreads), but the
direction matches V1's deterministic assembly win (~150 ms of the phase).

### Warm e2e (8 runs/arm, interleaved)

| Phase | baseline | candidate |
|---|---:|---:|
| get_session | 0.4355 s | **0.3394 s (−22.1%)** |
| laps | 0.0676 s | 0.0696 s (+3.0%, noise) |
| telemetry | 0.7690 s | 0.7984 s (+3.8%, noise — within-arm spread ±40%: 0.66-1.09 s both arms) |
| total | 1.2368 s | 1.2269 s (−0.8%) |

V11's mechanical −27 ms SQL win is real (interleaved micro-benchmark) but
below the e2e noise floor of this 1-vCPU sandbox; the direct batch-read
measurement (397 vs 404 ms candidate vs baseline) confirms no regression.

## Rejected (all measured, none shipped)

| ID | Hypothesis | Numbers | Reason |
|----|------------|---------|--------|
| V2 | `to_timedelta` → `TimedeltaIndex(seconds*1e9)` | 139 vs 143 us/frame; **value parity FAILS** (float ns rounding differs from to_timedelta's conversion) | No win + wrong values |
| V3 | Null-like probe rework (hoisted arrow probe / unique-set / frame-level collapse) | Probe variants tie at **3.7 ms** on the real pre-dtype laps frame; replace path already arrow-vectorized; pandas 3.0 has no frame-level `.str` | No headroom; the 38 ms cProfile figure was instrumentation inflation |
| V4 | Merged-path (`Laps.telemetry`) ndarray preconversion | Merged build **49.7 → 58.0 ms (+17%)** — parity True but slower; pandas handles large lists efficiently; the per-frame win does not transfer to single big frames | Measured regression — reverted |
| V5 | Lazy Cache init DDL/PRAGMA | Cache() on existing dir = **0.5 ms** (warm path); DDL 8.8 ms only first-ever run | Nothing to save on the warm path |
| V6 | Further `import tif1` trim | 13.1 ms total, **~9.5 ms is the stdlib logging chain** imported by every module | Deferring logging is package-wide invasiveness for 9 ms |
| V7 | SQLite page-cache PRAGMA for the frame tier | Interleaved **18 vs 18 ms** — single-pass sequential scan | No effect |
| V8 | Overlap lz4 decompress with unpickle (thread) | `lz4.frame.decompress` does **NOT release the GIL**: overlap 209 ms ≈ serial 206 ms | Impossible |
| V9 | Driver column as raw object ndarray | DataFrame infers `str` dtype from raw ndarray (must stay `pd.Series(..., dtype=object)`) | Breaks the FastF1 object-dtype contract |

## Pre-hypothesis probes (documented so no future series retries them)

- lz4.block vs lz4.frame: dec 167 vs 173 ms, enc 116 vs 115 ms — parity, no win.
- Naive per-Series columnar elision of the frame tier: unpickle 2251-2318 ms
  vs 545 ms (4x worse) — reconfirms the N/R-series columnar rejection.
- DriverAhead `str`→`object` in stored frames: unpickle 550 vs 545 ms — no win.
- `validate_telemetry_data`: 3 us/payload (4 ms total); sanitize 2 us — saturated.
- Empty-cache `get_telemetry` probe: 4 us/req (6 ms total) — saturated.
- Vendored schedule load + validate: ~2 ms total — negligible.
- **Warm loads make 0 network requests.** An earlier probe that appeared to
  show network calls during warm laps had set `TIF1_CACHE_DIR` *after*
  importing tif1 submodules, so the config singleton resolved a default
  (cold) cache dir. Pitfall documented: set the env var before any tif1
  import when probing.
- The warm telemetry path (unpickle 65% + lz4 20% + SQL 7%) is saturated:
  every storage-format alternative (columnar, const-elision, single-blob,
  Arrow IPC, str→object, lz4.block, mmap/page-cache) measured at parity or
  worse across N/R/Ithome and this series.

## Flaky-test note

`tests/property/test_async_properties.py::test_parallel_operations_time_is_max_not_sum`
failed once under full-suite load on **both the pristine baseline and this
candidate** (1-vCPU burstable, xdist workers); it passes in isolation and
12/12 with an idle sandbox. Environmental timing flake, not a regression.

## Method

- Offline: real payloads/frames from the primed tiers, tight-loop best-of
  timing, full-set parity gates (v1_parity_ab.py).
- Warm A/B: `tools/warm_cache_benchmark.py`, fresh process per run,
  interleaved arms (warm_ab.py).
- Cold A/B: `tools/monaco_ab_suite.py` (fresh process + throwaway cache per
  run, interleaved; .agents/perf-halikarnassos/cold_ab.json).
- Tests: full suite 1314 passed + 1 environmental flake (see note); ty
  diagnostics identical to baseline; ruff clean incl. harnesses.
