# Thebes series — results (2026-10-15)

Ten hypotheses, all run to a verdict (plan and baseline in `PLAN.md`).
Eight shipped, two rejected by measurement. Vantage: 32-core workstation
(load from other jobs varied between 5 and 26 during the day, so every
number below comes from interleaved or paired runs), Python 3.13.15, pandas
3.0.6, polars 2.0.0, numpy 2.5. Control = `main` @ fb07055 in a worktree
(`/tmp/tif1-control`).

## Verdict table

| # | Hypothesis | Verdict | Key evidence |
|---|-----------|---------|--------------|
| TH1 | Typed-columnar frame-tier encoding + direct block rebuild (pandas) | **KEPT** | Offline lz4+decode of the 2026 Monaco set 187 → 115 ms (−39 %), 3000 frames of 2018-2025 739 → 553 ms (−25 %), encode at parity with pickle, stored bytes −7 %. Fresh-process warm A/B, 8 interleaved pairs: telemetry 0.247 → 0.167 s (−32 %), total 0.552 → 0.446 s (−19 %), candidate faster 8/8. Parity 0 mismatches on both corpora; legacy pickled rows stay readable, corrupt rows are misses, unsupported frames fall back to pickle. |
| TH2 | Direct block assembly in `_typed_telemetry_frame` (no `DataFrame._from_arrays`) | **KEPT** | Offline assembly of 1452 parsed payloads, 5 interleaved fresh-process pairs: 877 → 562 ms (−36 %), candidate faster 5/5. Parity 1452/1452 (2026) and 8883/8883 (2018-2025) frames identical in columns, dtypes, values and block count. Probed once at import against the public constructor; any drift falls back to `_from_arrays`. |
| TH3 | Latency-aware CDN routing + straggler hedging | **KEPT** | Deterministic replay of the live distributions (`tools/thebes_cdn_sim.py`, 300 files, seed 1): cold 79.6 → 13.6 s (−83 %, 51 jsDelivr + 271 Hugging Face requests, +5.7 % bytes), flaky 31.1 → 4.4 s (−86 %), warm 6.9 → 4.6 s (−34 %, one hedge request). Live cold edge, 2026 Hungarian GP race, disjoint 200-file slices in ABBA order: control 47.3 s / 72.3 s (4.2 / 2.8 files/s, 2 read timeouts) vs candidate 20.1 s / 18.7 s (10.0 / 10.7 files/s, 0 timeouts) — candidate faster in both pairs, −65 % wall. |
| TH4 | Optional Rust telemetry decoder (`tif1-native`, GIL released, typed numpy output) | **KEPT (optional accelerator)** | Decode of the 2026 corpus: orjson 413 ms vs native 223 ms serial (−46 %), 35 ms on 8 threads (−92 %); 0/1452 payload mismatches, no fallbacks. Parse + assembly pipeline (interleaved, fresh process): 1228-1288 → 560-581 ms (−55 %). Simulated fetch + parse + assembly on a clean warm edge with hedging off, 1452 real payloads: fetch+parse 3.60 → 3.57 s (the fetch is latency-bound), assembly 1.07 → 0.29 s, total 4.68 → 3.87 s (−17 %). Frame parity 1452/1452 pandas, 8883/8883 cross-season, 1452/1452 polars. `pip install tif1` is unchanged; the extension is built from `native/` (maturin). |
| TH5 | pysimdjson typed decode | **REJECTED** | 2026 corpus: orjson 228 ms vs simdjson 228 ms (parity) — the `"None"` sentinel channels force `as_list()`; the −46 % seen on the sentinel-free 2024 corpus (697 → 374 ms) does not transfer. Threads do not scale either way (GIL held). |
| TH6 | One-wave laps prefetch (drivers + session laptimes + requested tables) | **KEPT** | Simulated laps phase (`--mode laps`), 3 seeds: cold 9.58 / 5.02 / 6.39 s → 4.67 / 3.49 / 4.46 s (−51 / −30 / −30 %); warm 0.17 / 0.13 → 0.13 / 0.11 s. Live warm-edge laps A/B (6 pairs, Monaco 2026, 50 ms RTT): 0.397 vs 0.423 s median — noise-bound, as expected for a single ~50 ms round trip. |
| TH7 | Stale-while-revalidate live schedule | **KEPT** | Deterministic: a stale cached row with a 200 ms upstream fetch answers `get_session` in < 0.15 s instead of blocking; the refresh lands in a daemon thread and the memo re-reads it (generation counter, race-free). |
| TH8 | Polars frames as per-driver Arrow IPC blobs | **KEPT** | Offline tier read 68 → 23 ms, write 112 → 30 ms (probe). First implementation read the blobs with `polars.read_ipc` and was slower end to end (0.134 → 0.155 s): polars' in-memory IPC reader costs 4-60 ms per call; the pyarrow reader + `pl.from_arrow` reads the 22 driver blobs in 18 ms. Fresh-process polars warm A/B, 6 interleaved pairs: telemetry 0.146 → 0.077 s (−47 %), total 0.501 → 0.414 s (−17 %), candidate faster 6/6. Tier parity 0/1452 (`DataFrame.equals` + schema). Partial batches merge per driver; schema mismatches fall back to per-frame rows; legacy rows stay readable. |
| TH9 | Byte-level `"None"` → `null` replace + numpy None→NaN fast paths | **REJECTED** | `bytes.replace` over the 84 MB corpus costs 184 ms (CPython's multi-byte search, ~450 MB/s) against 128 ms saved in orjson (fewer string objects) and ~30 ms in assembly; interleaved pipeline pairs showed parse +80 ms / assembly −31 ms. Per-channel micro: comprehension 15.2 µs vs `np.array(None list)` 13.4 µs — no headroom there. The sentinel cost moved into the native decoder (TH4) instead. The polars `Null`→`Float64` cast found while testing is kept as a robustness fix. |
| TH10 | Cold-edge adaptive concurrency (22 → 44/88) | **REJECTED (live)** | Disjoint 150-file slices of the cold 2026 Canadian GP race: jsDelivr cap 22 → 2.5 files/s (p50 6.2 s, 0 timeouts); cap 44 → 3.1 files/s but 9 read timeouts and p90 10.1 s; on Hugging Face cap 44 vs 88 → 25.3 vs 27.4 files/s (parity). Raising the cap buys little and trips the slow mirror's timeouts; TH3's re-ranking captures the throughput instead. |

## Baseline (this vantage, before the series)

| Metric | Value |
|---|---:|
| `import tif1.core` | ~280 ms (pandas 247 ms; pyarrow 46 ms inside pandas) |
| Warm pandas (5 runs) | total 0.51-0.55 s: get_session 0.25-0.29, laps 0.004, telemetry 0.249-0.258 |
| Warm polars (5 runs, frames primed) | total 0.44-0.47 s: get_session 0.31-0.33, laps 0.002, telemetry 0.123-0.158 |
| Cold Monaco 2026, warm edge (3 runs) | total 7.9 / 30.9 / 43.4 s — two of three hit jsDelivr 503s and 30 s read timeouts |
| Cold-edge live probe, 400 files, cap 22 | p50 1.67 s, p90 3.4 s, p99 6.9 s, max 30 s; 7.3 files/s |
| A full cold session via jsDelivr on this vantage | ~15 min (2024 Monaco race, 1237 files: 902 s) |
| Offline assembly, 1452 parsed payloads | 877-963 ms (`_from_arrays` ~250 µs/frame, sentinel scan ~180 ms, DriverAhead inference ~110 ms) |
| Frame-tier unpickle | ~127 µs/frame, ~80 µs of it the DataFrame skeleton (empty-frame unpickle 120 ms / 1452) |

## Shipped stack, end to end

Warm pandas (`tools/warm_cache_benchmark.py`, fresh process per run,
control cache primed by main, candidate cache primed by this branch,
8 interleaved pairs; load 5-26):

| phase | control median / min | candidate median / min | candidate faster |
|---|---:|---:|---|
| get_session | 0.298 / 0.224 s | 0.283 / 0.237 s | 6/8 (parity) |
| laps | 0.004 / 0.004 s | 0.002 / 0.002 s | 8/8 |
| telemetry | 0.247 / 0.238 s | 0.167 / 0.152 s | **8/8 (−32 %)** |
| total | 0.552 / 0.466 s | 0.446 / 0.405 s | **8/8 (−19 %)** |

Warm polars (same protocol, 6 pairs): telemetry 0.146 → 0.077 s (−47 %),
total 0.501 → 0.414 s (−17 %), candidate faster 6/6.

Cold Monaco 2026 (`tools/monaco_ab_suite.py`, fresh process + throwaway cache
per run, 3 interleaved pairs, warm edge, `cold_ab.json`):

| pair | control total / telemetry | candidate total / telemetry |
|---|---:|---:|
| 1 | 26.3 / 25.2 s | 7.5 / 6.8 s |
| 2 | 28.7 / 27.9 s | 13.1 / 12.4 s |
| 3 | 29.2 / 28.5 s | 13.9 / 13.2 s |

Candidate faster 3/3; medians 28.7 → 13.1 s total (−54 %), telemetry
27.9 → 12.4 s (−55 %). Two earlier pairs from a run that was cut short by a
session restart agreed: 35.5 / 48.9 s → 11.5 / 14.9 s. The control spends
its time in jsDelivr stragglers and 30 s read timeouts that the hedging
routes around; the in-process post-fetch work (parse + typed assembly)
drops from ~1.3 s to ~0.6 s with TH2 alone and to ~0.3 s with the native
decoder.

## Findings worth keeping

- On this vantage jsDelivr's edge fill for files of the over-limit 2026
  repository is pathological: p50 4-6 s per file, 2-5 % read timeouts
  (30 s each), while Hugging Face serves the same files at p50 0.5 s. The
  configured order is still right on vantages where jsDelivr is warm; the
  latency-aware re-ranking makes the choice per process from observed
  samples (8+ per source, 2x gap).
- `DataFrame._from_arrays` is not a cheap constructor: it re-sanitizes every
  column and rebuilds the column Index; for frames whose dtypes are known,
  building the blocks directly is ~25x cheaper per frame.
- Unpickling a DataFrame spends most of its time on the skeleton, not the
  data: 1452 empty frames unpickle in 120 ms, the same frames with 614k rows
  in 209 ms.
- polars' `read_ipc` from an in-memory buffer has a large per-call cost that
  pyarrow's reader does not; for many small blobs read through pyarrow.
- orjson spends ~130 ms of the 2026 corpus parse creating the 4.9 M
  `"None"` string objects; a decoder that maps the token to NaN at scan time
  avoids both that and the per-channel Python pass.
- CPython's `bytes.replace` with a multi-byte needle runs at ~450 MB/s, so
  byte-level JSON rewriting costs more than it saves.
- `_prefetch_session_tables` fetched `drivers.json` through its own thread
  pool and sync session; the laptimes file was fetched afterwards in a second
  wave even though its path does not depend on the driver list.

## Verification

- Parity gates (`tools/thebes_parity.py`): 2026 Monaco 1452/1452 pandas,
  1452/1452 polars; 2018-2025 cache corpus 8883/8883 pandas — for every
  shipped change, with the native decoder installed.
- Frame-tier round trips: TH1 codec 0 mismatches on both corpora; TH8 IPC
  tier 0/1452 (`equals` + schema).
- Unit suite (1541), property suite (81) and integration suite (13) green with the native decoder installed (the four
  `get_telemetry(...) == dict` assertions now compare through a
  list-normalising helper because channels may be numpy arrays when
  `tif1-native` is present); `ruff check`, `ruff format`, `ty check src/tif1`
  clean.
- Contract tests: `tests/unit/test_thebes_contracts.py` (direct block
  assembly, frame codec incl. legacy/corrupt/unsupported rows, hedging and
  re-ranking, one-wave laps prefetch, stale-while-revalidate, polars IPC
  tier incl. merge-on-write, schema mismatch and corrupt rows).

## Harnesses

- `tools/thebes_parity.py` — cross-season frame fingerprint gate.
- `tools/thebes_assembly_bench.py` — interleaved fresh-process assembly A/B.
- `tools/thebes_cdn_sim.py` — deterministic CDN latency replay (fetch and
  laps modes, optional real payload bytes).
- `tools/thebes_live_slices.py` — live cold-edge disjoint-slice A/B.
- `tools/thebes_laps_ab.py` — interleaved cold laps-phase A/B with wave count.
- `tools/warm_cache_benchmark.py --lib polars` — polars warm runs.
- `native/` — the `tif1-native` crate (`uvx maturin develop --release`).
