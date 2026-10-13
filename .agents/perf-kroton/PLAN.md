# Kroton performance series — plan

Corpus: 2026 Monaco Grand Prix Race (standard benchmark session, 1452 telemetry
payloads / materialized frames, warm cache at `/tmp/tif1-warm-cache`).

## Baseline (this session, warm cache, pandas backend)

| Phase | Cost |
|-------|------|
| `import tif1` | ~5.8 ms |
| `get_session` first call (includes lazy `tif1.core` import) | ~334 ms |
| `get_session` warm | ~0.5 ms |
| `Session.load()` warm (telemetry on) | laps first-touch ~66 ms, telemetry ~0.60-0.71 s, weather 2 ms, rcm 5 ms |
| `Laps.telemetry` (single driver, warm) first access | ~122 ms; repeat ~58 ms (R7-era numbers) |
| Cold telemetry assembly (payload tier -> frames) | ~0.94 ms/frame (~940 µs) |
| Warm frame-tier read | SQL 65 ms + lz4 38-175 ms + unpickle ~540 ms (saturated per V8/H) |

Prior-series audit: G/F/E (fab5/monaco), L/N (sestos/lindos), K (kalchedon),
R (rhodes), I (ithome), T (tyria), V (halikarnassos) read; rejections listed
there are excluded here (details in RESULTS.md).

## Hypotheses (all 10 measured; keep only real wins)

| # | Hypothesis | Distinct from prior series | Status |
|---|-----------|------------------------------|--------|
| KR-1 | **Laps frame tier**: materialize the processed laps DataFrame (post `_process_lap_df`) like the telemetry frame tier (L7/R4); warm loads read the pickled frame instead of JSON-decode + 66 ms dtype processing. | Telemetry-only tiers exist; laps has none. | implement + A/B |
| KR-2 | **Typed direct block assembly** for per-frame telemetry construction: bit-exact `fast_td_ns` timedelta replica (base/frac/round-9 algorithm from pandas' `cast_from_unit_vectorized`), object-ndarray Driver block, arrow string fast path, direct `BlockManager`/`new_block` assembly bypassing `pd.DataFrame(dict)`. | F4 rejected a timedelta cast with parity mismatches (this one is bit-exact); N4/R1 used slow constructor paths / hit parity failures this design fixes; V9 broke object dtype via the constructor (direct blocks keep object). | probe **1.64x, 1452/1452 parity**; integrate + A/B |
| KR-3 | **Typed laps processing**: pre-type laps columns (timedelta/float/bool/str) at construction via the KR-2 machinery instead of post-hoc `_apply_laps_dtypes` (56 ms of the 66 ms first-touch). | V3 cleared `_replace_null_like_strings` (3.7 ms actual); the dtype pass itself was never reworked. | implement + A/B |
| KR-4 | **Typed direct assembly for the merged telemetry path** (`_telemetry_frame_from_merged` still uses slow build-then-convert `_apply_telemetry_dtypes`). | V4 rejected ndarray preconversion *through the constructor*; direct block + pre-typed columns is untried. | implement + A/B |
| KR-5 | Telemetry payload pydantic validation share of the cold path. | untried | **measured: 4 ms/1452 — rejected (negligible)** |
| KR-6 | Frame-tier codec re-eval (zstd-1 vs current lz4-0), decode+encode. | U2 chose lz4 at older versions; re-check on current deps. | **measured: lz4 wins decode (38 vs 67 ms best-of-5) and encode (77 vs 135 ms) — rejected (confirms U2)** |
| KR-7 | `get_session` first-call 334 ms breakdown. | G1 kept lazy polars; what remains? | **measured: it is the `tif1.core` lazy import (pandas chain); warm calls 0.5 ms — rejected (no tif1-side cost left)** |
| KR-8 | Memoize merged `Laps.telemetry` result (session-level, keyed by driver set): repeat access 58 ms -> ~0. | L3 memoizes payload fetches; the merged *result* is rebuilt every access. | implement + A/B |
| KR-9 | `Session.results` first-touch cost (candidate for tier/lazy work). | untried | **measured: 0.7 ms — rejected (negligible)** |
| KR-10 | **Frame-tier-sourced merged telemetry**: `_telemetry_merged` reads materialized frames (unpickle ~29 ms for one driver's 78 frames) instead of payload-tier JSON (zstd+orjson ~73 ms), then block-concats. | R7 vectorized refs + IN query on the payload tier; frame-tier source for merges is untried. | implement + A/B |

## Status (final)

KR-8 and KR-10 as originally scoped turned out to be already implemented
(Ithome U8 merged-result memoization; Ithome U11 frame-tier sourcing in
`_telemetry_merged` — verified by code reading). The two slots were
replaced with genuinely new hypotheses and measured to completion:

- KR-8 (replaced): block-level concat of the frame-tier hits — rejected
  (concat = 13.8 ms of a ~40-120 ms first access; negligible).
- KR-10 (replaced): polars laps frame tier (`laps_frames_pl`) — **kept**
  (29.2 → 5.6 ms median warm first-touch).

Final verdicts: KR-1 **kept**, KR-2 **kept**, KR-3 rejected, KR-4 rejected
(measured regression, reverted), KR-5/6/7/9 rejected, KR-8 rejected, KR-10
**kept**. See RESULTS.md for evidence.

## Method

* A/B harness: fresh subprocess per arm, interleaved rounds, same warm cache,
  `pd.testing` parity on all 1452 frames where output changes (dtype- and
  value-identical), full test suite + ruff + ty before the PR.
* Keep = statistically clear win with parity; reject = no win, regression, or
  negligible (< ~10 ms) with no structural benefit.
