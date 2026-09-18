# Ithome Series — Results (10 hypotheses, all run)

Benchmark corpus: 2026 Monaco GP Race telemetry (~1452 payloads / 84 MB raw /
24.5 MB zstd frame tier), same as all prior series (H/E/F/G/K/L/N/R/T — none
re-run; see PLAN.md for the prior-rejection inventory).

Sandbox: Modal burstable, **1 CPU**, Python 3.12.3, pandas 3.0.5, polars 1.44.1.
Warm A/B cache: `/tmp/perf-u1` (zstd frames, baseline) and `/tmp/perf-u2`
(lz4 frames, candidate).

## Baselines (measured this session)

| Metric | Value |
|---|---:|
| `import tif1` | 47 ms (importlib.metadata ≈ 37 ms of it) |
| `import tif1.cli` | 387 ms |
| Warm pandas (zstd frames) | total ~1.2 s; telemetry 0.77-0.86 s |
| Warm pandas telemetry breakdown | frames_batch 805 of 812 ms: SQL ~55 + zstd ~200 + pickle ~300 + allocator noise |
| Warm polars (frames primed) | total 0.72 s; telemetry 0.25 s |
| Cold live | edge-dependent (9-35 s); interleaved A/B required |
| Cold assembly (`_create_telemetry_df` × 1452) | 2136 ms |
| Laps.telemetry merged path (78 payloads) | ~90-105 ms (payload read + merge 4.7 ms + build 49 ms) |

## Kept (shipped)

| ID | Change | Measured result | Parity |
|----|--------|-----------------|--------|
| U1 | Lazy `tif1.__version__` via PEP 562 (no eager `importlib.metadata`) | `import tif1` **47 → 13.6 ms (−71%)** | `tif1.__version__` + `from tif1 import __version__` identical (0.8.0); CLI version test green |
| U2 | LZ4 (level 0) codec for the materialized frame tier (pandas + polars), zstd fallback + legacy zstd rows readable | Offline: decompress 75 → 40 ms, compress 170 → 79 ms (−54%), stored 24.5 → 30.8 MB (+26%). Warm pandas telemetry **0.803 → 0.680 s median (−15%)**; polars 0.248 → 0.227 s | 1452/1452 byte-identical roundtrip; lz4/zstd magic sniffed |
| U4 | `pd.to_timedelta(np.asarray(v), unit="s")` in `_typed_telemetry_frame` (ndarray path; list fallback for non-float) | Cold assembly **2136 → 1677 ms (−21%)** (isolated to_timedelta: 790 → 215 µs/frame) | 1452/1452 frames identical (columns, dtypes, values); Time parity re-verified 20/20 |
| U5 | Post-return background frame-tier write (writers joined by `Cache.close`/`invalidate`/`clear`/atexit) | Cold `fetch_all_laps_telemetry()` returns **~1.0 s sooner** (write = 1034 ms moved post-return); live A/B telemetry phase median 10.83 → 9.74 s | 1452/1452 frame rows at exit in every live run; writer drops safely if the connection is closed |
| U6 | Polars laps single-shot dict-of-lists assembly (superseded kleitor G8) | Offline polars laps assembly **20.4 → 9.1 ms (−55%)**; warm e2e polars laps 39 → 27 ms | shape/columns/schema/all-values identical on the real session |
| U8 | `Laps.telemetry` merged-result memoization (`(len, refs)` key, `object.__setattr__` storage) | Repeat access **30.5 → ~1 ms**; different slices cache independently | identity-stable; subset/invalidations verified; no pandas attribute warning |
| U10 | Lazy pandas/rich imports in `tif1.cli` (command-local imports) | `import tif1.cli` **387 → 59 ms (−85%)** | 12/12 CLI unit tests pass; `app` importable |
| U11 | `Laps.telemetry` frame-first routing (memoized frames → frame tier → legacy payload merge fallback) | First access (cold memo, warm cache) **~65 ms vs ~90-105 ms (−35%)**; parity vs merged path | 19/19 columns values+dtypes identical, row count equal; incomplete coverage falls back unchanged |

## Rejected (measured, not shipped)

| ID | Hypothesis | Numbers | Reason |
|----|------------|---------|--------|
| U3 | Background-prefetch dedupe (join the 128-wide background bulk fetch; premise: duplicate cold downloads) | Corrected live count: **1465 requests / 1465 unique / 0 duplicates** | Premise falsified: the original 2925-request count was an instrumentation artifact (the niquests session got wrapped twice by the counter). Default config (`prefetch_all_telemetry_* = False`) never starts the background thread at all. All U3 code reverted; the live count is retained as evidence |
| U7 (pre-hypothesis) | numpy concatenation in `_merge_telemetry_payloads` | merge loop is **4.7 ms** for 78 payloads (list.extend is C-speed); the 49 ms cost is the dtype-contract build | No headroom; dropped before implementation |
| U9 | Batch the background-prefetch payload persistence (per-payload `set_telemetry` → one executemany) | 1452-payload write: per-payload 743 ms vs batched 724 ms (**−2.5%**) | Within noise; commit gating already batches; no code change |

## U5 detail

The frame-tier bulk write (pickle protocol 5 + lz4 + executemany for 1452
frames) measures **1034 ms** — it was the last synchronous step of
`fetch_all_laps_telemetry`. U5 moves it into a daemon thread registered with
the cache; `close()`/`invalidate()`/`clear()` join pending writers (and the
atexit cache close guarantees completion before process exit — verified:
1452/1452 frame rows present at exit in every live run).

Live interleaved A/B (fresh process + throwaway cache per run, 2026 Monaco):

| Arm | telemetry_s (3 runs) | median | wall_s median |
|---|---|---:|---:|
| control (inline write) | 10.83 / 11.19 / 9.37 | 10.83 s | 11.36 s |
| U5 (background write) | 9.74 / 8.01 / 9.89 | **9.74 s (−10%)** | 12.30 s |

The phase win is the write cost moving post-return (~1 s); the full-process
wall pays it back at the atexit join, so this is a **latency** improvement
(callers receive frames ~1 s sooner), not a throughput change. CDN edge
warmth adds multi-second noise per run (2/3 pairs faster); the mechanical
1.0 s critical-path removal is deterministic. Kept.

## Method notes

- Warm A/B: `tools/warm_cache_benchmark.py`, fresh process per run, interleaved.
- Live cold A/B: fresh process + throwaway `TIF1_CACHE_DIR` per run, interleaved arms.
- The corrected request-counting harness (idempotent session wrap) is inline in
  this RESULTS.md history; single-wrap counting is mandatory for niquests.
