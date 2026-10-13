# Kroton performance series — results

Corpus: 2026 Monaco Grand Prix Race (1452 telemetry payloads / materialized
frames, 1455 laps rows, warm cache). A/B protocol: fresh subprocess per arm,
interleaved rounds, identical warm cache; parity via byte-level frame
comparison (columns, dtypes, values) against pre-change snapshots.

## Shipped (3 of 10)

| # | Change | Evidence |
|---|--------|----------|
| KR-1 | **Laps frame tier (pandas)**: `laps_frames` SQLite table stores the processed (`_process_lap_df`-complete) laps DataFrame per (year, gp, session); warm loads unpickle it instead of re-running payload read + merge + dtype processing. Written after assembly on non-empty frames; reads gated on `enable_cache and not ultra_cold`; corrupt/foreign rows degrade to a payload-path miss. | Warm first-touch `session.laps`: **67.8 → 4.6 ms (median, 14x)** across interleaved rounds. Parity: payload-path vs tier frame equal (columns, dtypes, `.equals`). |
| KR-2 | **Typed direct block assembly** for `_typed_telemetry_frame`: bit-exact vectorized `Time` conversion (`_fast_td_ns` replicating pandas' base/frac/round-9 `cast_from_unit_vectorized` algorithm), pre-typed Driver/nGear/DRS/LapNumber/Brake columns, arrow string fast path, and direct `BlockManager`/`new_block` construction bypassing `pd.DataFrame(dict)`. Falls back to the constructor on any pandas-internals drift (`_direct_assembly_mode` probe) or conversion failure. | Cold per-frame assembly (payload → 1452 frames): **1447 → 1155 ms median (1.25x)**; offline probe best-of showed 940 → 576 µs/frame (1.64x) on the pure build+assemble loop. Parity: **1452/1452 frames identical** vs pre-change snapshot (dtype- and value-exact). |
| KR-10 | **Laps frame tier (polars)**: `laps_frames_pl` mirror of KR-1 for the polars backend (same blob format, foreign-marker guard). | Warm first-touch polars `session.laps`: **29.2 → 5.6 ms median (5.2x)**. Parity: `.equals` + schema equal on the round-trip. |

Also fixed while wiring the tiers: `Cache.clear()` and `invalidate("telemetry")`
did not delete `telemetry_frames_pl` (pre-existing gap from Rhodes R4); both
now wipe all frame/laps tables.

## Rejected / falsified (7 of 10)

| # | Hypothesis | Verdict | Evidence |
|---|------------|---------|----------|
| KR-3 | Pre-type laps columns at construction (bypass `_apply_laps_dtypes`) | Rejected | `_process_lap_df` = 75 ms of the 111 ms build, but sub-costs are scattered (null-probe 2.9 ms, timedelta 0.13 ms, to_datetime 0.84 ms, float64 0.79 ms, str astype 0.33 ms — no lever ≥ 10 ms); the KR-1 tier makes the pass one-shot per session. |
| KR-4 | Typed direct assembly for the merged-dict path (`_telemetry_frame_from_merged`) | Rejected (measured regression) | Interleaved A/B: B median +5.6% (1418 → 1498 ms). Cost split: constructor 922 ms dominates; `pd.to_timedelta` on the built column is 1 ms (no Time win at scale), while per-list pre-typing adds `infer_dtype` 248 ms + per-list conversions that cannot beat the constructor's C path on 614k-row columns. Confirms V4's finding from a new angle: the KR-2 per-frame win does not transfer to single big frames. Reverted. |
| KR-5 | Telemetry payload pydantic validation share of the cold path | Rejected (negligible) | `validate_telemetry_data` × 1452 payloads = **4 ms** total. |
| KR-6 | Frame-tier codec re-eval (zstd-1 vs lz4-0) | Rejected | Best-of-5 decode: lz4 38 ms vs zstd 67 ms; encode lz4 77 ms vs zstd 135 ms. lz4 wins both directions on current deps (lz4 4.4.5 / zstandard 0.25.0) — confirms Ithome U2's choice. |
| KR-7 | `get_session` first-call 334 ms breakdown | Rejected (falsified) | cProfile: the 334 ms is the lazy `tif1.core` import chain (pandas 468 ms cumtime); warm calls are 0.5 ms. No tif1-side cost left to remove (consistent with V6). |
| KR-8 | Block-level concat of frame-tier hits in `Laps.telemetry` merged path | Rejected (negligible) | `pd.concat` of a full driver's 78 materialized frames = **13.8 ms** of the ~40-120 ms first access; no headroom worth bypassing pandas' concat manager. (Note: the merged-result memoization and frame-tier sourcing originally scoped here already exist — Ithome U8/U11.) |
| KR-9 | `Session.results` first-touch tier/lazy work | Rejected (negligible) | Measured **0.7 ms**. |

## Verification

- Parity: 1452/1452 telemetry frames identical vs pre-change snapshot (per-frame
  path); laps frames equal on both backends; merged frame equal (KR-4 revert
  re-verified).
- Full test suite: **1315 passed**, coverage 84.66% (gate 80%).
- `ruff check src/ tests/` clean; `ruff format` clean; `ty check src/tif1` at
  the pre-change baseline (5 pre-existing diagnostics, no new ones).
- Cold-path behavior unchanged: tiers write only after a successful assembly;
  ultra-cold loads never read the tiers; corrupt rows degrade to misses.
