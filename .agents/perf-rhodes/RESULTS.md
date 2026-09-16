# Rhodes-Series Performance Experiments — Final

Ten hypotheses, all run to completion. Prior series were NOT re-run: H1–H11
(`.agents/perf/RESULTS.md`), E1–E10 (`.agents/perf-monaco/RESULTS.md`),
F1–F10 + G1–G10 (`.agents/perf-fab5/RESULTS.md`), K1–K10
(`.agents/perf-kalchedon/RESULTS.md`), L1–L10 (`.agents/perf-sestos/RESULTS.md`),
N1–N10 (`.agents/perf-lindos/RESULTS.md`), T1–T10
(`.agents/perf-tyria/RESULTS.md`).

Benchmark: 2026 Monaco GP Race telemetry (~1452 payloads / 84 MB), live CDN,
fresh process per run. Warm: `tools/warm_cache_benchmark.py` (primed persistent
cache, 5 runs). Polars warm: same protocol with `lib="polars"`. Cold:
`tools/monaco_telemetry_cold_benchmark.py` + interleaved A/B via
`tools/monaco_ab_suite.py` (control = HEAD 71a15f2 worktree). Sandbox: Modal
burstable (1 vCPU reserved / 2 burst), pandas 3.0.5, polars 1.44.1.

## Baseline (this vantage, pre-Rhodes)

| metric | value |
|---|---|
| warm pandas total (5-run median) | **1.28 s** (get_session 0.38 / laps 0.06 / telemetry **0.875**) |
| warm polars telemetry | **1.53–1.66 s** (total ~2.03–2.10) |
| warm telemetry phase split | frame read ≈ 0.92 s (pickle.loads 0.47–0.66, zstd 0.17–0.21, SQL 0.05) |
| repeat in-process `fetch_all_laps_telemetry` | 6.8 ms |
| cold (live) | laps prelude 0.85 s + telemetry network + assembly ~2.2–3.2 s + flush 0.75 s |

## Verdicts

| # | Hypothesis | Verdict | Key evidence |
|---|------------|---------|--------------|
| R1 | Columnar frame tier v2: store column arrays + shared schema, reconstruct frames directly | **REJECTED** | Real-frame micro (1452 frames, best-of-3): one-blob arrays + `pd.DataFrame` construct 1480–1502 ms; direct BlockManager route 982 ms (deprecated pandas API, dtype parity fails on ArrowString/Int64 columns); per-frame arrays + construct 1039–1170 ms — ALL worse than per-frame DataFrame unpickle at **264 ms** (gc-off). pandas' BlockManager unpickle is near-optimal; reconstruction from arrays pays more in sanitize/construction than it saves in deserialization. Confirms L7's format choice. |
| R2 | Cache `(driver, lap)` refs + fix memo-loop overhead (L-series residual 0.128 s repeat-call) | **FALSIFIED** | refs extraction 2.05 ms (1455 refs, already vectorized), memo loop 0.19 ms, repeat call **6.84 ms** — the L-series 0.128 s residual does not reproduce on current main; nothing to fix. |
| R3 | GC suspension around the short-lived allocation bursts (batch frame read/write, payload batch read, deferred flush, assembly loops) | **KEPT** | Unpickle loop 470 → **264 ms (−44%)**; combined zstd+loads best-of-5 327 ms. Live: pandas warm telemetry 0.875 → **~0.70 s (−20%)**, total 1.28 → ~1.10 (−14%). Offline cold assembly `_create_telemetry_df`×1452: 2170 → **1735 ms (−20%)**. Applied via a `_suspend_gc()` context manager scoped to synchronous loops only (restores prior state, never held across `await` — avoiding E3's cold-fetch GC regression). |
| R4 | Polars materialized frame tier (`telemetry_frames_pl`): polars warm loads currently re-read the payload tier + re-construct `pl.DataFrame` per frame on EVERY load | **KEPT** | Polars frame pickle round-trip is far lighter than pandas': zstd+loads = **135 ms** for 1452 frames vs 1.45 s payload read + 0.56 s construction. Live polars warm telemetry 1.53–1.66 → **0.25–0.37 s (−78–85%)**, total ~2.03 → **~0.77 s (−62%)**. First polars load after upgrade materializes once (fetch-network path and payload-tier path both write; parity `.equals` verified). |
| R5 | Cold orchestration overlap: extract refs from raw laptimes payloads, start the telemetry wave while laps assembly + weather/rcm parse still run | **REJECTED** | Measured serial prelude = laps phase **0.85 s** of a ~91 s truly-cold-edge run (0.3–1.3 s on a warm edge) → max upside 0.3–5%, below the ±1–2 s cold noise floor; and CPU work inside the fetch window is the exact pattern F2 (+13.7%) and L8 (+23–36%) measured as regressions on 1-core vantages. |
| R6 | Merge the weather/rcm thread-pool prefetch into the laptimes async wave | **REJECTED** | Bounded by the same ~0.1–0.2 s prelude slice — sub-noise; not worth re-tuning the `_prefetch_session_tables` design (N3's contract). |
| R7 | Model-layer hot path: `Laps.telemetry` (the FastF1 idiom `laps.pick_driver(x).telemetry`) used `iterrows` + one SQLite query per lap | **KEPT (marginal)** | 78-lap driver flow: 129 → **122–123 ms** first access (−5–6%, stable across runs; entries re-emitted in laps-row order so merged row order is unchanged); repeat 59 → 58 ms (parity); single-lap and `pick_fastest` unchanged. `_telemetry_merged` now extracts refs with vectorized column ops (E6 pattern) and reads warm-cache payloads in ONE batched IN query; still-missing refs keep the original per-ref chain (skip-verdicts, network fallback, failure recording). |
| R8 | Raw (uncompressed) frame-tier blobs to skip the 0.14–0.21 s zstd decompress | **REJECTED** | SQLite read of 107 MB raw + loads (gc-off) = 321 ms vs zstd+loads (gc-off) 327 ms — parity at best, with **4.3× cache size** (107 vs 25 MB). |
| R9 | SQLite warm-path connection/transaction audit | **FALSIFIED (no defect)** | Singleton connection, WAL + `synchronous=NORMAL`, 64 MB page cache, frames batch read is a single `(driver, lap) IN (...)` query at 46–50 ms. Nothing to fix. |
| R10 | Combined post-gather write pass: K1 deferred JSON flush + N1 frame materialization share one loop + one transaction | **REJECTED** | Offline macro on real payloads: combined dumps 2388 ms vs separate passes 2217 ms (slower); one transaction 73 ms vs two transactions 43 ms (no win — WAL already amortizes commits, confirming K5). The flush cost is serialization, not transaction overhead. |

## Shipped outcome (R3 + R4 + R7 in one PR)

Warm loads (primed persistent cache, fresh process, Monaco full telemetry):

| Backend | Phase | Before | After |
|---|---:|---:|---:|
| pandas | telemetry | 0.875 s | **~0.70 s (−20%)** |
| pandas | total | 1.28 s | **~1.10 s (−14%)** |
| polars | telemetry | 1.53–1.66 s | **0.25–0.37 s (−78–85%)** |
| polars | total | 2.03–2.10 s | **~0.77 s (−62%)** |

Cold loads: assembly phase −20% and flush GC-suspended (R3); no new work on
the network path. Interleaved cold A/B (5+5, control = HEAD 71a15f2): B faster
in 4/5 adjacent pairs, median total **−4.6% / telemetry −6.5%** (A 10.83 s vs
B 10.33 s median; B's one 51 s run was a CDN-edge straggler, the regime prior
series documented). Full record: `.agents/perf-rhodes/cold_ab.json`.

## Correctness

- Full suite: 1220 unit + 94 property/integration green; coverage 84.5%.
- R4: pandas/polars frame tables are isolated (`telemetry_frames` vs
  `telemetry_frames_pl`); foreign-backend blobs degrade to tier misses (contract
  test); corrupt rows degrade as before; `pl.DataFrame.equals` parity verified.
- R3: `_suspend_gc` restores prior GC state (enabled, disabled, and on
  exception — contract tests); never held across `await`.
- R7: failure-recording, skip-verdict and network-fallback contracts preserved
  (still-missing refs go through the original per-ref chain); row order of
  merged output unchanged (vectorized refs preserve row order).
