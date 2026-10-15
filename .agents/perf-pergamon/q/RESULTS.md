# Pergamon wave 2 — results (2026-10-15)

Twenty more hypotheses on using cached data, each run by a subagent in its own
git worktree against a frozen control (`ef221ad`, the wave-1 state of PR #101)
and integrated by hand into one tree. Fifteen shipped, five rejected by
measurement. Per-hypothesis reports with every number are the `Q*.md` files in
this directory; the shared protocol is `BRIEF.md`.

Vantage: 32-core workstation, Python 3.13.15, pandas 3.0.6, polars 2.0.0. The
box was loaded by up to seven concurrent agents for most of the series and
rebooted once (the control tree and warm snapshot were rebuilt identically);
every number below comes from interleaved fresh-process pairs. Absolute times
in the final runs are ~1.5x the wave-1 runs (CPU governor `powersave`, load
3-5); ratios and win counts are what to read.

## Verdict table

| # | Hypothesis | Verdict | Key evidence |
|---|---|---|---|
| Q1 | Lazy lap-frame materialization in `load()` | **KEPT (trade-off)** | `load()` 58.0 → 38.6 ms (8/8). `fetch_all_laps_telemetry()` unchanged (still returns built frames). First touch of a driver 4.3 → 3.4 ms; touching 50 laps one by one +3 ms (frames built on demand + a GC pass moving into the loop). Parity 1452/1452 on both lazy paths. |
| Q2 | Background preload of numpy/pandas/pyarrow in interactive interpreters | **KEPT** | Notebook simulation (`import tif1`, think time, `get_session`): get_session 0.221 → 0.017 s (8/8); real `python -i`: 0.298 → 0.033 s (6/6). Scripts: thread never starts, parity. 60/60 deadlock stress runs clean; no `tif1.*` submodule imported by the thread. `TIF1_PRELOAD_IMPORTS=0/1` overrides. |
| Q3 | Trim tif1's own share of `import tif1.core` | REJECTED (below 5 ms bar) | tif1-attributable import self time is 9.15 ms, not 15-25; lazy probe + lazy CDN manager save 2.7 ms (22/25), ~1.5 ms of it reappears in the first telemetry build. Patch kept as `Q3.patch`. |
| Q4 | Laps tier consolidation → really: `Laps`/`Lap` result re-wrapping | **KEPT (re-scoped)** | The tier frame was already consolidated (17 blocks). The cost was pandas calling `Laps(df)`/`Lap(ser)` (a shallow manager copy, ~65 us) for every result; `_constructor_from_mgr` hooks wrap the fresh manager. Every `pick_*` −15 to −28 % (8/8), column get −48 %. Parity on 50 operation kinds. |
| Q5 | Tiers for weather / race control / results | REJECTED (noise) | Builders total ~4.1 ms inside `load()`; a best-case cached table saves 3.8 ms, invisible against the telemetry phase's 48-89 ms swing (3/8). |
| Q6 | Polars frame-tier read | **KEPT** | Streamed cursor into the pool, single-threaded IPC reads in workers, no rechunk, `string_view` storage: warm polars telemetry 69.5 → 45.4 ms (8/8), 81.7 → 55.1 ms on caches with old rows. Parity 1452/1452, single-chunk slices. |
| Q7 | Per-row lap access | **KEPT** | `list(laps.iterlaps())` −62 to −67 %, `laps.iloc[i]` −52 %, `iterlaps` + `lap.telemetry` loop −79 % (6/6). Cause: a column `Index` rebuilt per row and pandas' interleaved-dtype resolution per `iloc`. Exact parity vs pandas `_ixs` on 10 frame shapes. |
| Q8 | `Telemetry` analysis methods | **KEPT** | `merge_channels` −24 %, `resample_channels` −40 %, `fill_missing` −60 %, distance methods −77 to −89 %, slicing −34 to −72 %; 4-driver comparison block 84 → 54-60 ms (6/6, two runs). Parity 32,120 corpus + 414 edge cases. |
| Q9 | Season-scale multi-session loads | REJECTED (time) + **KEPT (retention fix)** | Per-session overhead after the first is ~1.7 ms; nothing to save. Found: the process-wide laps LRU held each session-bound `Laps` and so every session's telemetry. Fix stores a session-free copy: after 11 sessions, full GC 0.10 → 0.017 s, tracked objects 500k → 68k (16/16), RSS with periodic gen-2 1440 → 395 MB (3/3). 19,740 frames identical. |
| Q10 | Fastest-lap convenience paths on a warm cache | **KEPT** | They ran `asyncio.run` for cache hits and rescanned raw laptimes: `get_fastest_lap_tel` 41.0 → 10.7 ms, `get_fastest_laps_tels` 65.5 → 30.0 ms, `Driver.get_fastest_lap_tel` 35.1 → 11.5 ms (all 8/8). Fastest-lap choice from the laps tier = raw scan on 122 sessions (0 mismatches). |
| Q11 | SQLite read path of the driver rows | **KEPT** | Per-row lz4 tasks for big rows (the old chunk rule made 3 tasks for 22 rows), cursor streamed into a shared pool: full-batch read 51.3 → 41.8 ms (9/10); warm telemetry 51.7 → 42.0 ms. mmap, `blobopen` and skipping the lap pre-check measured and rejected. |
| Q12 | Stronger lz4 level for frame-tier writes | REJECTED (noise) | L9: −14 % bytes, ~2 ms read gain (A/A floor 1.4 ms), writer 0.13 → 1.2-3.1 s. zstd loses on the serial single-row path. |
| Q13 | Frame-tier writer | **KEPT** | One preallocated output buffer, memoized column names, pooled python-lz4 compression (it releases the GIL on compress): 1452-frame write 156.6 → 83.0 ms (7/7). Stored driver bodies byte-identical. |
| Q14 | Charts from cached data | **KEPT** | seaborn (and through it `scipy.stats`) imported at module level for 3 of 22 charts: import + first chart + PNG 1.84 → 1.05 s (8/8); 18/18 rendered charts byte-identical PNGs. Filters and team-colour lookups also cheaper. |
| Q15 | First warm load from the payload tier (frame tiers empty) | **KEPT** | zstd + native parse in worker threads, served without asyncio: 1.068 → 0.654 s with `tif1_native` (8/8), 2.316 → 2.237 s without (5/6), polars 2.239 → 1.867 s (4/4). Also fixed a shared-zstd-decompressor thread-safety bug in the P2 path; added a frame-writer lock. |
| Q16 | Jupyter (running event loop) warm path | REJECTED (probe) + **KEPT (two fixes)** | No warm call reaches `asyncio.run`. Found a P5 regression: `fetch_all_laps_telemetry()` raised inside a running loop when some telemetry missed the frame tier (now applies nest_asyncio2 first). Found the sync payload path ignoring known-missing verdicts: `laps.pick_drivers("LEC").telemetry` 0.97-1.1 s → 68-77 ms (8/8). |
| Q17 | Negative cache on the per-lap path | **KEPT (narrowed)** | Q16 made the sync path read verdicts; Q17 makes it record them (and drop stale ones), so missing laps first reached one at a time stop walking the CDN: second process 1.12-1.18 s → 7 ms; CDN walks 3/3 → 3/0 across two processes. |
| Q18 | Per-lap frame construction from driver rows | **KEPT** | A per-group build plan (one repeated array for constant `Int64` columns, `str` slices without `__getitem__`, block classes called positionally): decode −31 % (2026) to −40 % (2018), batch read 90.5 → 79.7 ms (10/10). Rust probed: 1 ms per legacy session, rejected. Parity 4205/4205 frames. |
| Q19 | `Driver` object paths | **KEPT** | A direct builder of `Driver.laps` from the payload (falls back to the pipeline on anything unusual): first `Driver.laps` 41.3 → 7.5 ms, chart idiom `get_driver(d).get_fastest_lap()` 38.1 → 9.1 ms, whole driver flow 85.0 → 27.5 ms (8/8). 2,434/2,434 drivers exact across 122 sessions; ~9,000 fuzzed payloads. |
| Q20 | CLI on a warm cache | **KEPT** | `core_utils/__init__` re-exports pulled pandas into `tif1.cache`; a module-level `pyarrow` import cost 160 ms; `events` imported pandas for name lookups. `cache-info`, `cache-clear`, `events`, `sessions` −65 to −74 % (8/8), `fastest` −30 ms. Output byte-identical on 1267 schedule and 492 `fastest` commands. |

## Integrated stack, end to end

Final tree = wave 1 + Q1, Q2, Q4, Q6-Q11, Q13-Q20. Control arms: wave 1
(`ef221ad`, frozen worktree, its own copy of the warm snapshot) and the PR's
base `perf/thebes` (`cc472d1`, its own cache with per-lap rows).

Warm load, `tools/pergamon_warm_ab.py` (medians, candidate faster):

| arm | phase | control | final | faster |
|---|---|---:|---:|---|
| vs wave 1, pandas (10 pairs) | telemetry | 98.7 ms | 74.6 ms | 10/10 |
| | total | 0.501 s | 0.477 s | 10/10 |
| vs wave 1, polars (8 pairs) | telemetry | 90.6 ms | 58.8 ms | 8/8 |
| vs Thebes, pandas (10 pairs) | telemetry | 269.4 ms | 76.1 ms | 10/10 |
| | total | 0.670 s | 0.462 s | 10/10 |
| vs Thebes, polars (8 pairs) | telemetry | 118.3 ms | 58.9 ms | 8/8 |
| | total | 0.601 s | 0.534 s | 8/8 |

`get_session` (the pandas import) is at parity in every arm.

Analysis-notebook benchmark (`tools/pergamon_notebook_bench.py`, offline, fresh
process, 8 interleaved rounds of three arms; `final_notebook_ab.json`):

| step | Thebes | wave 1 | final | final faster vs wave 1 |
|---|---:|---:|---:|---|
| `load()` | 288.3 ms | 114.3 ms | 54.0 ms | 8/8 |
| 8 × `pick_*` | 18.2 | 13.7 | 11.5 | 8/8 |
| `iterlaps` + `lap.telemetry` (1445 laps) | 371.4 | 207.2 | 87.2 | 8/8 |
| 200 × `laps.iloc[i]` | 57.4 | 56.7 | 27.6 | 8/8 |
| 4 × driver merged telemetry | 31.1 | 25.5 | 29.8 | 1/8 (see below) |
| 4-driver telemetry analysis | 70.1 | 68.4 | 41.0 | 8/8 |
| `get_fastest_lap_tel()` | 5.5 | 5.3 | 4.1 | 8/8 |
| 4 × `get_driver(d).get_fastest_lap()` | 32.5 | 46.6 | 20.2 | 8/8 |
| new session, `get_driver(d).laps` | 39.9 | 39.4 | 6.6 | 8/8 |
| **whole process** | **1350 ms** | **988 ms** | **705 ms** | **8/8** |

The one slower step is an allocator effect, not slower code. The final
process reaches it with ~18 MB less resident memory, because the earlier steps
copy less. Its output then lands on fresh pages: 3,560 minor faults against 8
in wave 1. The same calls are equal or faster in isolation (best-of-30, and
the first concat measured alone: 1.9-2.0 ms in both trees with identical fault
counts).

## Verification

- Parity, integrated tree vs wave 1 on the warm 2026 Monaco snapshot: 1452/1452
  pandas and 1452/1452 polars frames (values, columns, dtypes, polars schema,
  pandas block layout), laps and driver-merged telemetry equal. Same result
  after deleting both frame tiers and letting the new writers re-materialize
  them.
- Cross-season gate (`tools/pergamon_legacy_gate.py` on a copy of the user's
  2018-2025 cache): 9859/9859 frames equal, from legacy rows and after the
  background rewrite through the Q13 writer and Q18 decoder.
- Unit + property + integration: 1875 passed, coverage 86.9 %. One
  integration test (`test_lap_telemetry`) failed once under load; it reaches
  the live CDN and passed alone and in two further full runs (13/13). The
  property test `test_parallel_operations_with_varying_durations` failed once
  under the same load and passed alone (the timing flake earlier series
  documented).
- `ruff check` / `ruff format` on `src/`, `tests/`, `tools/`; `ty check
  src/tif1`; docs STE baseline: clean.

## Findings worth keeping

- pandas 3 subclass results go through `Subclass(df)`, which shallow-copies
  the fresh manager. `_constructor_from_mgr` / `_constructor_sliced_from_mgr`
  hooks that wrap the manager directly removed ~65 us per operation on `Laps`,
  `Lap` and `Telemetry` (Q4, Q8).
- python-lz4 releases the GIL on compress but not on decompress; pyarrow's
  lz4 compressor writes no content-size header, so its blobs would miss the
  parallel pyarrow decode path (Q12, Q13).
- A process-wide LRU that stores session-bound objects keeps whole sessions
  alive; cache session-free copies and bind on hit (Q9).
- Optional heavy dependencies imported at module level (seaborn → scipy,
  pyarrow in `cache.py`, pandas through package re-exports) cost far more than
  any data path on a warm cache (Q14, Q20).
- Two correctness bugs surfaced while measuring: a P5 regression inside Jupyter
  (Q16), and a shared zstd decompressor used from worker threads (Q15).
- Existing inconsistency left unchanged (needs a decision): for a driver with a
  missing lap, `Laps.telemetry` falls back to the payload build and returns
  `DriverAhead` as `str`, while drivers served from the tier slice get `object`
  (Q17). Four charts fail on the 2026 Monaco race in both trees
  (`plot_top_speeds`, `plot_qualifying_grid`, `plot_telemetry_comparison`,
  `plot_gg_diagram`; Q14).

## Harnesses

`tools/pergamon_notebook_bench.py` (analysis-notebook steps), plus each
hypothesis' own `tools/q*_*.py` / `tools/pergamon_q13_write_bench.py`
harnesses listed in its report.
