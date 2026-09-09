# G-series performance experiments — 2026-09-09

Prior series (NOT re-run): H1–H11 (`.agents/perf/RESULTS.md`), E1–E10
(`.agents/perf-monaco/RESULTS.md`), F1–F10 (`.agents/perf-fab5/RESULTS.md`).

## Pre-work: merge-regression restore (not a hypothesis)

Merge 931e9f3 ("superseded by main-rebased content") accidentally dropped two
already-merged, already-measured E-series wins from main:

- **E6** (3ff3240): vectorized (driver, lap) ref extraction in
  `fetch_all_laps_telemetry_async` — current HEAD is back to `iterrows()`
  (core.py:4032). `git log -m -S "refs_frame"` shows the loss at the merge.
- **E10** (0f080a0): `keepalive_max_requests` 10000 — current HEAD has 1000
  (config.py:95). The bake-off commit a81fd79 intentionally reverted only the
  CDN order; its diff did not touch keepalive.

Both are restored before the G-series so the baseline represents the intended
accepted state; original E-series A/B evidence stands (E6: 8/10 pairs, median
paired delta -3.1 s; E10: 4/5 pairs, aggregate median -16.5%).

## Ten hypotheses

| # | Hypothesis | Measured with |
|---|-----------|---------------|
| G1 | Cold `Session.load(telemetry=True)` double-fetches every telemetry payload: the laps-load background prefetch thread (128-wide, `write_cache=False`) races the explicit `fetch_all_laps_telemetry` batch (22-wide) on one shared niquests session — ~2x download volume + 150-wide contention. Dedupe via claim + wait-and-reuse so exactly one bulk fetch runs. | Live Monaco A/B |
| G2 | Laps frame assembly: `_process_lap_df` runs `to_numeric` AND `to_timedelta` (two full passes) on LapTime, then per-column astype + categorical + a full-frame reorder copy. Pre-type the merged-dict construction (E2-style for laps) and build in final column order. | Offline laps assembly micro + live A/B |
| G3 | SQLite `synchronous=NORMAL` still fsyncs per commit on a fully regenerable cache; `synchronous=OFF` (WAL already on) removes the fsync from the cold-write path. | Offline cold-write macro + live |
| G4 | zlib-3 at the SQLite boundary burns CPU per write; level 1 compresses ~2x faster at a slightly worse ratio. | Offline cold-write + warm-read |
| G5 | The fetch loop's Python-side scheduling (22 concurrent executor futures, per-response callbacks) is asyncio-overhead-bound; uvloop (optional extra, graceful fallback) cuts loop overhead. | Offline loop micro + live A/B |
| G6 | Per-payload cache writes (`set_raw`/`cache.set`) run inside the 22-slot semaphore — each ~1 ms SQLite insert throttles the next request. Defer writes to a post-gather flush outside the semaphore. | Offline cache-on macro |
| G7 | `_typed_telemetry_frame` builds Int64 masked arrays from Python lists per column (`pd.array(v)`); numpy-first construction (+ `np.full` for the constant LapNumber) should cut per-frame cost. | Offline assembly micro, parity-gated |
| G8 | Polars laps path still builds per-driver frames + `pl.concat` (the pandas H8 single-shot win never reached it): one dict-of-lists `pl.DataFrame` instead. | Offline polars micro, parity-gated |
| G9 | `Session.load` runs weather/rcm table loads between laps and telemetry; telemetry only depends on laps. Start the telemetry batch first and overlap the table wave. | Latency-injected offline harness + live |
| G10 | Cold bulk telemetry writes every payload to TWO cache tiers: `set_raw` (JSON tier, inside fetch) + `set_telemetry` (telemetry table, json_dumps+compress+insert after) — ~2 ms/payload of duplicate CPU+IO. Write one tier on the bulk path. | Offline cache-on macro + live |

## Method

- Live: `tools/monaco_telemetry_cold_benchmark.py` (fresh process + throwaway
  `TIF1_CACHE_DIR` per run), interleaved A/B via `tools/monaco_ab_suite.py`.
- Offline: `tools/perf_validation_experiment.py` (deterministic, InMemoryTransport)
  plus new purpose-built micro harnesses under `.agents/perf-kleitor/`.
- Keep = improvement beyond noise (live: median + paired deltas; offline:
  deterministic repeat). One PR per kept experiment; rejected experiments are
  reverted and documented here.
