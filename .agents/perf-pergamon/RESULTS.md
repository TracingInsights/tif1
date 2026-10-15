# Pergamon series — results (2026-10-15)

Ten hypotheses on the warm path (sessions already in the persistent cache),
all run to a verdict (plan and baseline in `PLAN.md`). Seven shipped, three
rejected by measurement. Vantage: 32-core workstation (load 1-5), Python
3.13.15, pandas 3.0.6, polars 2.0.0, pyarrow with lz4/zstd codecs. Control =
`perf/thebes` @ cc472d1 in `/tmp/tif1-control`. Corpus: 2026 Monaco GP Race
(1452 frames, 614k rows) plus the 9859 frames of eleven 2018-2025 sessions in
the user's own cache (per-lap pickled rows written by earlier releases).

## Verdict table

| # | Hypothesis | Verdict | Key evidence |
|---|-----------|---------|--------------|
| P1 | Fast frame-decode primitives (block classes called directly, `_simple_new` for `TimedeltaArray`/`IntegerArray`, shared `RangeIndex`/`BlockPlacement`, no per-frame `import_module`) | **KEPT** | Per-lap typed-columnar decode of the 1452 frames 79.6 → 43.4 ms (−45 %) best-of-5 offline; construction floor with ready values measured at 2.8 us/frame. Fresh-process warm telemetry 0.150 → 0.102 s. The cold typed assembly uses the same primitives. Parity through the existing TH1/TH2 contract tests and the import-time probe (block classes and value types compared against the public constructor). |
| P2 | GIL-released parallel lz4 decompression through pyarrow's codec | **KEPT** | python-lz4 holds the GIL (1452 blobs: 38 ms serial, 33 ms on 8 threads; lz4.block 35 → 24); pyarrow's lz4-frame decoder reads the same blobs with the GIL released: 33.3 → 9.6 ms on 8 threads, and returns a mutable buffer so the bytearray copy (6 ms) goes. Chunks are decoded as they complete. Fresh-process warm telemetry 0.102 → 0.089 s. A single 1.2 MB driver row decompresses in 0.93 ms vs 1.96 ms with python-lz4, so the serial path uses pyarrow too. |
| P3 | Per-driver typed-columnar blobs with view slicing (`telemetry_frames_drv`) | **KEPT** | Offline: decode 36.7 ms (per-lap rows, P1) → 21.4 ms (14.8 us/frame) for 1452 frames; 22 rows instead of 1452 (SQL 18 → ~5 ms); write 237 → 141 ms and 27.1 → 23.3 MB on disk. Fresh-process warm telemetry 0.089 → 0.070 s. Parity 0/1452 (columns, dtypes, values, block count and classes), 0/9859 on the 2018-2025 corpus both when served from the legacy rows and after the background rewrite to driver rows (`legacy_parity.json`); writable, mutation-isolated frames; pickling round-trips. Layout groups keep per-lap dtypes exact (the 2026 set has two layouts: `DriverAhead` as a `str` column in 122 frames and an object column in 1330). |
| P4 | Batched polars tier read | **KEPT** | 22 per-driver IPC reads 15.5 → 9.6 ms in a thread pool; `pl.from_arrow` once from the concatenated table 10.1 ms vs 25-27 ms for 22 conversions; `pl.read_ipc` from memory re-measured at 190 ms (TH8 finding holds). Parity 0/1452 (`equals` + schema), partial batches, corrupt rows and schema groups covered by contract tests. Fresh-process polars warm A/B, 6 interleaved pairs: telemetry 0.087 → 0.053 s (−40 %), total 0.358 → 0.313 s, candidate faster 6/6. |
| P5 | asyncio-free warm path | **KEPT** | `asyncio` (+ `ssl`, `concurrent.futures`) was 23 ms of the 280 ms `import tif1.core` chain and `asyncio.run` ~6 ms per warm load; the four modules now import it lazily (AST-driven rewrite, 19 functions), laps/telemetry tier hits are served synchronously, and refs the negative-result cache marks as missing everywhere (3 per Monaco race) no longer start the fetch pipeline. `sys.modules` has no `asyncio` after a warm `load()` (contract test). Fresh-process warm telemetry 0.070 → 0.048 s, laps 3.4 → 1.1 ms. |
| P6 | Single-lap telemetry from the frame tier, whole driver memoized | **KEPT (trade-off stated)** | First touch of a driver 1.6 → 4.3 ms (decodes the driver's row once: 0.9 ms decompress + 76 frames), every later lap of that driver 1.1 → 0.22 ms, 50 laps touched one by one 8.6 → 6.6 ms (`idioms_ab.json`, 5 interleaved fresh-process pairs). Frames are built as `Telemetry` directly (`frame_class`), so a memo hit no longer pays the shallow manager copy: `_get_telemetry_df_for_ref` 27.6 → 1.2 us, `lap.telemetry` 48 → 6 us; the wrapped object is kept in the memo. |
| P7 | Speculative tier prefetch under the pandas import | **REJECTED (probe)** | Thread reading + decompressing the session's blobs while `import pandas` runs: serial 0.288 / 0.356 / 0.340 s vs overlapped 0.647 / 0.425 / 0.377 s — the import lock and the GIL make the overlapped run slower in 3/3. Not implemented. |
| P8 | Batch orchestration overhead | **REJECTED (negligible after P5)** | Timed inside `fetch_all_laps_telemetry` in fresh processes: tier read 51.7-62.3 ms, remainder 2.3-2.6 ms (ref extraction + memo). The 33 ms seen in-process before P5 was `asyncio.run` + executor shutdown + order effects. Nothing left to trim. |
| P9 | Compact integer encoding of float channels | **REJECTED (probe)** | No telemetry channel is integral (RPM, Speed, Throttle are interpolated floats; X/Y/Z are 88 % NaN); float32 is not lossless either. The compact layout equals the float64 layout byte for byte (59.0 MB raw, 18.9 MB lz4). The write-cost measurement planned here moved into P3 (237 → 141 ms). |
| P10 | Driver-merged telemetry straight from the driver row | **KEPT** | `laps.pick_driver(x).telemetry` builds the merged frame as slices of the driver's group arrays (one slice per run of consecutively stored laps, concatenated when the layout groups split the run): 14.3 → 5.4 ms first access for a 76-lap driver (33 657 rows), `equals` + dtypes equal to the per-lap concat, laps-row order kept (R7 contract test green). |

## Baseline (this vantage, before the series)

| Metric | Value |
|---|---:|
| Warm pandas (5 runs) | total 0.394-0.452 s: get_session 0.240-0.289 (import chain), laps 0.0034, telemetry 0.149-0.159 |
| Warm polars (3 runs) | total 0.334-0.375 s: telemetry 0.064-0.070 |
| Warm telemetry split | SQL 18 ms, lz4 30-40 ms (GIL held), `decode_frame` 80 ms (55 us/frame of Python skeleton work), orchestration ~15 ms |
| `import tif1.core` | ~280-300 ms: pandas 199 + numpy 56 + asyncio 23 + tif1 ~20 ms |
| Single lap without a prior full load | 0.8 ms (payload tier: SQL+orjson 0.22 + assembly 0.45) |
| `Laps.telemetry` for a 76-lap driver | 14.5 ms first access |

## Shipped stack, end to end

Warm pandas (`tools/pergamon_warm_ab.py`, fresh process per run, control
cache `/tmp/tif1-warm-cache` primed by the control tree, candidate cache
`/tmp/tif1-warm-p3`, 8 interleaved pairs; `warm_ab_pandas.json`):

| phase | control median / min | candidate median / min | candidate faster |
|---|---:|---:|---|
| get_session | 0.239 / 0.203 s | 0.227 / 0.206 s | 7/8 (asyncio out of the import chain) |
| laps | 0.0029 / 0.0024 s | 0.0012 / 0.0011 s | 8/8 |
| telemetry | 0.179 / 0.178 s | 0.057 / 0.046 s | **8/8 (−68 %)** |
| total | 0.430 / 0.400 s | 0.279 / 0.263 s | **8/8 (−35 %)** |

Warm polars (same protocol, 6 pairs; `warm_ab_polars.json`): telemetry
0.087 → 0.053 s (−40 %), total 0.358 → 0.313 s (−12 %), candidate faster 6/6.

Caches written by earlier releases (`tools/pergamon_legacy_read_bench.py`,
fresh process per run, control reads the original per-lap pickled rows,
candidate reads the driver rows after the one-time background rewrite):

| session | frames | control | candidate |
|---|---:|---:|---:|
| 2018 Monaco Race | 1516 | 261-268 ms | 80-92 ms (−67 %) |
| 2024 Monaco Race | 1237 | 237-240 ms | 77-81 ms (−67 %) |
| 2025 British Race | 826 | 160-166 ms | 55-59 ms (−65 %) |

Per-lap idioms (`tools/pergamon_idioms_bench.py`, fresh process, 5 pairs +
3 candidate re-runs; `idioms_ab.json`): next lap of the same driver 1.1 →
0.22 ms, driver merged telemetry 14 → 5.4 ms, 50 laps one by one 8.6 →
6.6 ms, first touch of a driver 1.6 → 4.3 ms (see P6).

Cold Monaco 2026 (`tools/monaco_ab_suite.py`, fresh process + throwaway
cache per run, 3 interleaved pairs; `cold_ab.json`): control 22.0 / 5.27 /
6.89 s vs candidate 5.75 / 5.21 / 5.89 s — candidate faster 3/3, medians 6.89
→ 5.75 s. The first control run re-warmed the CDN edge; the other two pairs
are at parity (−1 %) and −15 %, as expected: no cold mechanism changed, the
materialization write (now per driver, 141 ms) still runs in the background.

## Findings worth keeping

- python-lz4 never releases the GIL (frame or block API), pyarrow's lz4
  codec does and reads the same frames; the content size python-lz4 stores
  in the frame header (FLG bit 3) is all pyarrow needs.
- pandas 3 `DataFrame.__init__(df, copy=False)` shallow-copies the manager
  under copy-on-write (~30-70 us for an 8-block frame); building the subclass
  through `_from_mgr` at decode time costs nothing and keeps the same
  isolation (a fresh manager per frame).
- `pl.from_arrow` has a per-call cost that dominates small tables: one
  conversion of a concatenated 22-table session is 2.5x cheaper than 22.
- Overlapping work with the pandas import does not pay: the import holds
  the GIL and the import lock, and a thread that imports pyarrow on the side
  more than doubles the wall time.
- The 2026 telemetry channels are all non-integral floats; compact integer
  storage has nothing to compress.
- A driver's laps can straddle two block layouts (`DriverAhead` `str` vs
  object when no car is ahead for the whole lap); merged telemetry built from
  group slices must concatenate the runs to keep `pd.concat`'s dtype
  promotion.

## Verification

- Parity gates: 2026 Monaco 1452/1452 pandas (driver tier vs per-lap
  decode), 1452/1452 polars (batched read vs per-driver read); 2018-2025
  cache corpus 9859/9859 both from legacy rows and after the rewrite
  (`tools/pergamon_legacy_gate.py`, `legacy_parity.json`).
- Unit suite 1560 passed (coverage 85.5 %), property + integration 94
  passed, `ruff check`/`ruff format` clean on `src/`, `tests/`, `tools/`,
  `ty check src/tif1` clean, docs STE baseline ok.
- Contract tests: `tests/unit/test_pergamon_contracts.py` (shared
  placements/indexes, writable decoded frames, `frame_class`, lz4 header
  size, parallel vs serial decompression incl. corrupt rows, driver blob
  round trip with layout groups, mutation isolation, unsupported-frame
  fallback, malformed blobs, partial-batch merge, `whole_driver`, legacy
  upgrade + deletion, corrupt driver row, invalidate/clear, merged frame vs
  concat in several orders, polars batched read, asyncio-free warm load,
  single-lap tier read + driver memoization, merged driver telemetry).
  Two Thebes/Rhodes tests that asserted the storage location of pandas rows
  now read the driver table.

## Harnesses

- `tools/pergamon_warm_ab.py` — interleaved fresh-process warm A/B (pandas
  or polars) against a control tree.
- `tools/pergamon_idioms_bench.py` — fresh-process per-lap idiom timings.
- `tools/pergamon_legacy_read_bench.py` — fresh-process frame-tier read of
  one cached session.
- `tools/pergamon_legacy_gate.py` — cross-season parity gate (legacy rows →
  driver rows) on a cache copy.
