# Pergamon series — plan (2026-10-15)

Focus: the warm path — loading and using sessions that are already in the
persistent cache. Prior series (H/E/F/G/K/L/N/R/T/U/V/KR/A/TH, see the other
`.agents/perf-*/RESULTS.md`) are read and not re-run. Vantage: 32-core
workstation, Python 3.13.15, pandas 3.0.6, polars 2.0.0, pyarrow (lz4/zstd
codecs), load ~1. Control = `perf/thebes` @ cc472d1 in `/tmp/tif1-control`.
Corpus: 2026 Monaco GP Race (1452 frames, 614k rows), warm cache at
`/tmp/tif1-warm-cache` (copied from a Thebes bench cache, then primed for
weather/rcm/results and the polars tier).

## Baseline (fresh process per run, `tools/warm_cache_benchmark.py`)

| phase | pandas (5 runs) | polars (3 runs) |
|---|---:|---:|
| get_session | 0.240-0.289 s (import chain: pandas 199 + numpy 56 + asyncio 23 + tif1 ~20 ms; in-process work 4 ms) | 0.258-0.308 s |
| laps | 0.0034 s | 0.0016 s |
| telemetry | 0.149-0.159 s | 0.064-0.070 s |
| total | 0.394-0.452 s | 0.334-0.375 s |

Warm `session.load()` (everything) = 0.153-0.162 s; weather 1.8 ms, rcm
3.0 ms, results 2.4 ms, circuit info 1.7 ms — all tier hits.

Warm pandas telemetry phase, in-process split (1452 frames):
SQL range scan 18 ms, lz4 decompress 30-40 ms (python-lz4 holds the GIL:
4/8 threads do not scale), `decode_frame` 80 ms (55 us/frame of Python
skeleton work: `importlib.import_module` x2 per frame, `new_block` dispatch,
`TimedeltaArray._from_sequence`, `IntegerArray.__init__` validation,
`RangeIndex.__new__`, bytearray copy, header unpickle), orchestration ~15 ms.
The construction floor with ready values (block classes called directly,
`_simple_new`, cached RangeIndex + placements) measures 2.8 us/frame (4 ms).

Polars telemetry phase: `_read_polars_ipc` 31 ms (22 per-driver IPC reads),
`pl.from_arrow` 17 ms, SQL 13 ms, 1455 `slice` calls 7 ms.

Model layer on cached data: every `pick_*` < 1.5 ms; `Laps.telemetry` for a
driver 14.5 ms first / 0.8 ms repeat; single `lap.get_telemetry()` first
touch without a prior fetch_all 0.8 ms (payload tier: SQL+orjson 0.22 ms +
`_create_telemetry_df` 0.45 ms) vs 0.02 ms memo hit; `laps.iloc[i]` 0.24 ms
(pandas `fast_xs` interleaved-dtype resolution, not ours).

## Hypotheses

| # | Hypothesis | Mechanism | Expected |
|---|---|---|---|
| P1 | Fast frame-decode primitives | Hoist imports out of `decode_frame`; call block classes directly (cached per kind); `TimedeltaArray._simple_new` / `IntegerArray._simple_new`; cached `RangeIndex` and `BlockPlacement`; cheaper `str` rebuild. Also used by the cold typed assembly. | decode 80 -> ~25 ms |
| P2 | GIL-released parallel decompression through pyarrow's lz4 codec | Same stored lz4-frame blobs; `pa.Codec("lz4").decompress` releases the GIL (33 -> 9.6 ms on 8 threads in the probe) and returns a mutable buffer, so the bytearray copy goes too. Serial python-lz4 stays as the fallback. | lz4 35 -> ~10 ms |
| P3 | Per-driver columnar blobs (pandas) with zero-copy view slicing | One row per driver holding the driver's frames column-concatenated; per-frame build = slice views + direct blocks. 22 rows instead of 1452 (SQL 18 -> ~5 ms, 22 headers, 22 decompress calls). Per-frame rows stay readable (legacy); layout-mismatch frames fall back to per-frame rows. | SQL+decode ~45 -> ~20 ms |
| P4 | Polars per-driver tier read path | Parallel GIL-released IPC decompression, avoid the pyarrow->polars conversion cost where possible, keep slices. | 65 -> ~30 ms |
| P5 | asyncio-free warm fast path | Serve laps/telemetry tier hits synchronously and import asyncio lazily (23 ms of the import chain) — only network/async work pays for it. | get_session -20..25 ms |
| P6 | Single-lap telemetry from the frame tier | `Lap.telemetry` first touch reads the frame tier (and, with P3, memoizes the driver's frames in one go) instead of payload tier + assembly. | 0.8 -> ~0.1 ms/lap amortized |
| P7 | Speculative tier prefetch under the pandas import | Thread reads+decompresses the session's blobs while `tif1.core` imports. | probe first |
| P8 | Batch orchestration overhead | Measure and trim the non-decode remainder of `fetch_all_laps_telemetry` (ref extraction, memo sets, cache-availability probes, GC toggles). | ~15 -> ~5 ms |
| P9 | Materialization write cost | Per-driver encode (one concatenate per block kind + GIL-released compression) vs 1452 per-frame encodes; measured on the background writer. | write -50 % |
| P10 | Driver-merged telemetry (`Laps.telemetry`) straight from the per-driver blob | The driver's concatenated columns are the merged frame; build it as views instead of concat-ing 60-80 frames. | 14.5 -> ~1 ms first access |

Protocol: offline best-of micro for the mechanism, then fresh-process
interleaved A/B (control tree vs candidate tree, same primed cache dir —
candidate re-primes its own tier when the format changes), parity gates
(columns, dtypes, values, block layout) across the 2026 corpus and the
2018-2025 frames in `~/.cache/tif1`, unit/property/integration suites,
ruff, ty.
