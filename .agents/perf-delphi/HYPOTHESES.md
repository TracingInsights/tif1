# Delphi hypotheses (one agent each; line numbers refer to commit 255759b)

## D1. gc.freeze() at interpreter exit (both backends)

Evidence: a fresh process ends with ~80 ms (pandas) / ~50 ms (polars) of
interpreter teardown that is neither tif1's atexit hooks (0.3 ms) nor thread
joins (1 ms): CPython's finalization runs full garbage collections over the
~300k objects pandas/numpy/pyarrow (or polars) created at import. Registering
`gc.freeze` as an atexit hook (objects move to the permanent generation, the
final collections skip them; memory is still released by refcounting and by
the OS) measured, interleaved 8 pairs, whole process: pandas laps 520 -> 477 ms
(8/8), polars 358 -> 332 ms (6/8).
Approach: register the hook once at `import tif1` (so it runs after hooks
registered later, e.g. tif1's cache cleanup `_cleanup_cache` in `cache.py`, and
after the user's). Config opt-out `exit_gc_freeze` (env `TIF1_EXIT_GC_FREEZE`,
default on) read inside the hook, not at import. Skip on Pyodide. Document in
`config.py`/docs config reference. Risk review: finalizers of cyclic garbage do
not run at exit (CPython never guaranteed them); check nothing in tif1 relies
on `__del__` at shutdown (the SQLite cache is flushed/closed by its own atexit
hook; verify the order with an explicit test). Measure all warm workloads on
both backends (`--workloads import_get_session,laps,all_tel,notebook`), plus
one cold workload (`delphi_cold_bench.py --workloads laps --reps 3`) to show
the gain holds there too.

## D2. Pandas-session startup trim (get_session with pandas already imported: 60 ms)

Evidence (pandas pre-imported, then `import tif1; get_session(...)`, warm,
cProfile): 60 ms total; `_load_pandas_models` 23 ms of which
`core_utils/pandas_blocks.py:269 _probe_direct_blocks` 19 ms (builds probe
frames: `sanitize_array`, `ArrowStringArray._from_sequence` 14 ms); 44
`marshal.loads` 9 ms (tif1's own modules); `_resolve_gp_name` 9 ms on the
first call (`core.py:7313`); `async_fetch` import 8 ms (`asyncio`) during
`tif1.core` import although a warm session never runs a coroutine (Pergamon
P5). `tif1.core` self-import ~24 ms (`core.py:1`).
Approach, in order of expected value: (a) run the direct-block probe lazily
at the first frame build that needs it (or memoize its verdict per pandas
version in the SQLite json tier / a module constant keyed on
`pd.__version__`, with the probe as the fallback); (b) import `async_fetch`
(and asyncio) lazily: only cold loads need it (`_run_coroutine_sync`,
`fetch_multiple_async`); check `payload_loader.py`, `cdn.py`, `http_session.py`
for eager asyncio imports; (c) look at what `_resolve_gp_name`'s first call
does (vendored schedule orjson load, alias table, fuzzy import?) and trim;
(d) any other eager import that a warm session never uses (`validation`
(pydantic) must stay lazy; check `plotting_constants`, `rich`, `typer`,
`rapidfuzz`, `pydantic` never load in `import_get_session`). Guard with a
test that lists the modules a warm pandas `get_session` may import (like
`test_polars_without_pandas.py`). Measure `import_get_session,laps,results`
(pandas) and confirm polars `import_get_session,laps` does not regress.
Target: 60 -> ~30 ms in-process; wall -30 ms.

## D3. Polars sessions: no pandas, no pyarrow on warm paths

Evidence: (1) `get_driver("VER").get_fastest_lap_tel()` on polars: 268 ms
(pandas 20): `core_utils/helpers.py:1215 _arrow_strings_from_codes` (called
from `_polars_typed_channel`, helpers.py:415, for the dictionary-encoded
`DriverAhead` channel of a native-decoded payload) imports pyarrow, and
`pa.array(numpy)` makes pyarrow import pandas (its pandas shim). (2)
`session.t0_date` / `session_start_time` on polars: 231 ms: `core.py:2586`
and `:2598` return `pd.Timestamp`/`pd.Timedelta`/`pd.NaT`, importing pandas
on a polars session. (3) `laps.pick_driver("VER").telemetry` on polars: 48 ms
of which ~27 ms is the pyarrow import for the IPC row read
(`cache.py:2340 _get_polars_ipc_frames`, `cache.py:622 _read_polars_ipc`,
Pergamon Q6 chose pyarrow's streamed reader). `fetch_all_laps_telemetry` on
polars pays it too.
Approach: (1) build the string column with polars/numpy only (e.g. a polars
`String` series from an object array gather `np.array([*cats, None],
dtype=object)[codes]`, or `pl.Series(cats, dtype=pl.String).gather(codes)`
with nulls for -1; measure, pick the fastest; dtype must stay `String` with
null for missing exactly as today, including the all-null case). (2) polars
sessions return `datetime.datetime` (naive UTC) / `datetime.timedelta` /
`None` from `t0_date`, `session_start_time`; pandas sessions unchanged. This
is an allowed surface change on polars: PolarsLap scalars are already stdlib
`datetime`/`timedelta`; document it in the docstrings and the docs page
(`docs/api-reference/core.mdx` t0_date entry, `docs/migration-from-fastf1.mdx`
if it lists the type) and in the changelog paragraph. Check
`session_info`, `total_laps`, `track_status`, `session_status` on polars for
any other pandas use (`grep -n "pd\." src/tif1/core.py` around the session
state code and `core_utils/session_state.py`). (3) read the polars IPC rows
with polars' own reader (`pl.read_ipc_stream` / `pl.read_ipc` on the bytes,
`rechunk=False`, measure `memory_map`), keeping the frame schema identical
(string_view vs string: Q6 noted a `string_view` cast; the parity digest must
match). If polars' reader is slower in-process than pyarrow's by more than the
import saves on a fresh process, keep pyarrow but import it lazily only when
the row is read (it already is) and report. (4) Extend
`tests/unit/test_polars_without_pandas.py` to cover
`get_driver(...).get_fastest_lap_tel()`, `t0_date`, `session_start_time`,
`track_status`, `session_status`, `laps.pick_driver(...).telemetry`,
`fetch_all_laps_telemetry()` in the fresh interpreter (use the in-memory
session doubles the test already uses, or a stubbed `_fetch_json`). Measure
polars `state,driver_fastest_tel,driver_tel,all_tel,fastest_lap_tel,notebook`
(the notebook workload runs on polars too). Parity: `--lib polars` digest;
the `t0_date`/`session_start_time` surfaces will differ by design (type).

## D4. Decode driver rows in the decompression workers (pandas frame tier)

Evidence: warm `fetch_all_laps_telemetry()` (pandas, 1128 laps) is ~110-140
ms; `load()` 95 ms. Serial parts measured: SQLite read of the 20 driver rows
(70 MB) 57 ms, parallel lz4 decompress + narrow-row widening 27 ms,
`DriverFrameSource(blob)` construction 107 ms serial for 20 drivers
(`core_utils/frame_codec.py:1121 __init__` -> `:891 _group_parts`: numpy
frombuffer views, `_decode_object` gathers, `_decode_str` pyarrow take +
`ArrowStringArray`; the first `_decode_str` call of a process costs 14 ms of
one-time pyarrow/StringDtype initialisation, later calls 0.4 ms). Today the
rows are decompressed in the pool (`cache.py:430 _submit_frame_rows`,
`:445 _completed_frame_rows`, `:368 _decompress_row`) and decoded one by one
on the main thread as they complete (`cache.py:2130 get_telemetry_frame_sources`,
`:2061 _get_driver_frames`), under `_suspend_gc`.
Approach: construct the `DriverFrameSource` (and for `_get_driver_frames`, the
lap frames or at least the group parts) inside the worker right after the
decompression of that row, so the 20 decodes overlap each other and the
SQLite read; the main thread only collects. Check which parts hold the GIL
(object-array gathers do; pyarrow take and frombuffer do not) and whether the
one-time pyarrow initialisation should be triggered early (e.g. in the first
worker). Keep `_suspend_gc` semantics. Watch the GIL contention the Pergamon
P2/Q11 notes describe; measure with 1, 4, 8 workers. Measure pandas
`all_tel,notebook,car_data_loop,driver_tel`. Parity: pandas digest must be
identical (every frame of `all_tel/frames`). Target: all_tel 114 -> ~70 ms,
notebook load 111 -> ~60 ms.

## D5. `load()` keeps driver rows lazy (pandas)

Evidence: a warm pandas `session.load()` reads and decodes every driver's
frame-tier row (57 ms SQLite + decode, 95 ms total in `load`) through
`core.py:1405 _telemetry_load_pending` -> `cache.get_telemetry_frame_sources`
even when the script then touches four drivers (the notebook workload: load
111 ms, then driver_merged_x4 76 ms re-reads those rows anyway, see D8). The
only reason the rows are read in `load()` is to learn which laps the tier
holds (`pending` refs) so that laps the tier lacks are fetched eagerly.
Approach: answer "which laps does the tier hold for this session" without
reading or decompressing the blobs: a small per-row `laps` column (JSON list
of lap numbers, written together with the row; `frame_codec.driver_blob_laps`
gives it at write time) in `_PANDAS_DRIVER_TABLE`, added with a schema
migration (`_FRAME_TIER_SCHEMA_VERSION` bump drops old frames, or an `ALTER
TABLE ADD COLUMN` with NULL meaning "unknown, read the blob" so existing
caches keep their rows). `load()` then registers lazy per-driver sources that
read and decode the row on the first lap access (the memo's
`add_telemetry_source`/`frame_adopter` machinery in `cache.py:825 SessionMemo`;
keep `fetch_all_laps_telemetry()` reading all rows in one batch as today).
Keep the eager network fetch for laps the tier lacks. Parity: identical
digests; `tests/unit` tests of `load()` behaviour (Pergamon Q1 tests) must
still pass or be adjusted with a justification. Measure pandas
`notebook,all_tel,driver_tel,car_data_loop`. Target: load 111 -> ~10 ms,
notebook wall -60..-80 ms, all_tel unchanged.

## D6. `resample_channels` / `fill_missing` without per-column pandas ffill/bfill (pandas Telemetry)

Evidence: the notebook's 4-lap analysis step is 65 ms (115 ms under cProfile)
of which `models.py:2507 resample_channels` is 75 ms under the profiler (~19
ms per lap): the "held" branch (`else:` at the end of the column loop) runs
`self[col].iloc[positions].reset_index(drop=True).ffill()` then
`.iloc[held].reset_index(drop=True).bfill()` for every non-continuous column
(18 columns x 4 pandas ops x ~0.25 ms). `models.py:1932 fill_missing` is 19 ms
for 4 laps (`ffill().bfill()` / `interpolate` per column with gaps).
Approach: compute one integer source-index map per lap with numpy (forward
fill = `np.maximum.accumulate` over the valid positions, then gather at
`held`, then backward-fill the leading gap to the first valid index; all-NA
columns stay NA) and take each column once with a dtype-preserving gather
(`values.take(idx)` on the ExtensionArray or `array[idx]` on numpy; masked
`Int64`, `str`/object, bool and timedelta/datetime columns must keep their
dtype and NA semantics exactly). For `fill_missing`, the same map for the
"hold" rule, numpy `np.interp` for the "interpolate" rule only where it
reproduces pandas' `interpolate(limit_direction="both")` on a float column
bit for bit (linear on the positional index; verify, else keep pandas for
that column). Keep the general (slow) path for non-unique columns and odd
dtypes. The polars twin (`models_polars.py:1439`, `:1835 _resample_by_rule`,
`:998 fill_missing`) must keep giving the same rows (the docstring promises
it); do not change it unless it shares the bug. Add unit tests with gaps at
the start, end and middle, an all-NA column, a masked Int64, a str column and
a boolean column, comparing against the pandas chain. Measure pandas
`notebook` (telemetry_analysis_4) and an in-process micro benchmark (median of
20 calls on the 4 fastest laps, control vs candidate). Parity: identical
digests (`analysis*/resample`, `resample1s`, `fill`).

## D7. `Driver` fast paths (pandas)

Evidence: `get_driver(d).get_fastest_lap()` costs ~9 ms per driver (notebook
driver_fastest_lap_x4 35 ms; 18 ms each under cProfile): `models.py:2852
Driver.__init__` 8 ms, of which `models.py:2787 _results_row_data` 5.5 ms
(boolean-mask frame selections on the 20-row results frame: `results[mask]`,
`.iloc[0]`, `astype(str)` of DriverNumber, `row.items()`), `Driver.laps`
(`:2936`) 3 ms, `get_fastest_lap` (`:3153`) 3.5 ms in `DataFrame.nsmallest`
on a 57-row frame plus `helpers.py:173 _filter_valid_laptimes` 3 ms.
`core.py:5270 get_driver` adds `_prefetch_driver_lookup_and_laps` and
`_has_driver_code`.
Approach: find the results row by position with numpy on the two column
arrays (Abbreviation, DriverNumber) and build the dict from the frame's
column arrays at that position (`frame.iloc[pos]` or faster: one `_ixs`
per column) without boolean-indexing the frame; pick the fastest lap with
`argmin` on the valid lap-time array (as `Laps.pick_fastest` does at
`models.py:746`) and return the one-row frame via `iloc[[i]]`; make
`_filter_valid_laptimes` mask-based if it is not; avoid rebuilding the
Series when the results row is already a Series. Keep every value, dtype,
the Series name and index (fastf1's `DriverResult` contract) identical.
Measure pandas `notebook` (driver_fastest_lap_x4), `driver_fastest_tel` and an
in-process micro benchmark (20x `get_driver(d).get_fastest_lap()` over 4
drivers). Parity: identical digests (`driver_row`, `driver_fastest_lap`,
`driver_fastest_tel`, `driver_laps`). Target: 9 -> ~2 ms per driver.

## D8. Driver merged telemetry from the memoized decoded row (pandas)

Evidence: `laps.pick_driver(d).telemetry` costs ~19 ms per driver after
`load()` (notebook driver_merged_x4 76 ms; 96 ms under cProfile):
`models.py:1038 _telemetry_merged` -> `cache.py:2176 get_driver_merged_frame`
re-reads the driver's row from SQLite, decompresses and widens it again
(`_decompress_frame_rows`, `_expand_narrow_row` 4 ms) and decodes it
(`frame_codec.py:953 decode_driver_merged` 9.5 ms, `_take_spans` 3 ms),
although `load()` (or an earlier lap access) already decoded the same row
into a `DriverFrameSource` held by the session memo
(`cache.py:912 SessionMemo.add_telemetry_source`); then `_joined_laps` 3 ms
and a `concat` 7 ms.
Approach: when the memo holds the driver's decoded source (or the decoded
lap frames), build the merged frame from its group arrays (the same span
take `decode_driver_merged` performs, `drop_repeats` semantics included)
without touching SQLite; otherwise read the row once and register the source
in the memo so later per-lap accesses reuse it. Look at `_joined_laps` /
`_join_lap_telemetry` and the concat for avoidable copies. Keep the result
identical (dtypes, the lap boundary handling, `Date`/`SessionTime` columns,
the `Telemetry` class and its `session` attribute). Measure pandas `notebook`
(driver_merged_x4), `driver_tel` and an in-process micro benchmark (4
drivers, 10 repetitions, cold memo each time by rebuilding the session with
`load()` first). Parity: identical digests (`*/driver_tel`). Target: 19 -> ~8
ms per driver after load(); driver_tel without load() not worse.

## D9. Frame-tier row read: 57 ms for 70 MB (pandas)

Evidence: `SELECT driver, frame FROM <pandas driver table> WHERE year/gp/session`
for the 20 rows of a race (70.6 MB of lz4 rows) takes 57 ms on one connection
(`cache.py:2130`, `:2061`; the cursor is iterated under `_sqlite_lock` and
rows are submitted to the lz4 pool as they arrive, so the read overlaps the
decompression but is itself serial: ~1.2 GB/s, mmap on). It bounds a warm
`fetch_all_laps_telemetry()` (110 ms) and `load()` (95 ms).
Approach, measure each: (a) read the rows from several SQLite connections in
parallel (one per worker, the module's sqlite3 releases the GIL in `step`;
rows are independent, `rowid` ranges or `driver IN (...)` partitions), so
the copy of 70 MB splits across threads; (b) a prepared `SELECT ... WHERE
rowid = ?` per driver from the workers after one cheap index query; (c) read
the row through `Connection.blobopen` (CPython 3.11+) into a preallocated
buffer the lz4 codec can decompress from, avoiding the Python `bytes` copy;
(d) check `PRAGMA` settings that affect blob reads (`cache_size`,
`mmap_size` value vs file size: is the 300 MB file fully mapped?). Keep the
single-file SQLite cache; no new storage format. Thread safety: the cache
already has `_sqlite_lock`; new connections must use `check_same_thread`
appropriately and be closed with the cache (`_release_resources`). Windows:
no file paths through `str()`. Measure pandas `all_tel,notebook,driver_tel`
and an in-process micro benchmark of the raw read (median of 10). Parity:
identical digests. Target: 57 -> ~20 ms, all_tel -25..-35 ms.

## D10. Polars sessions without numpy until needed

Evidence: `import polars` is ~180 ms and `import numpy` adds ~54 ms on top of
it (numpy pulls `inspect`, `ast`, `dis`, `pickle`); `tif1.core` imports numpy
at module import (`core.py` top, `core_utils/helpers.py`, `session_state.py`,
`models_polars.py`, `cache.py`, `frame_codec.py`, ...), so a polars
`get_session` + `laps` script (371 ms wall) pays for numpy although polars'
laps tier read and plain polars filtering never need it. Olympia (OH3) did the
same for pandas with `core_utils/lazy_import.lazy_module` (binds the global
to a proxy that rebinds the real module on first attribute use).
Approach: bind `np` through `lazy_module("numpy")` in the modules a polars
session imports; move module-level `np.dtype(...)`/`np.array(...)` constants
behind lazy accessors; keep `isinstance(x, np.ndarray)` checks from forcing
the import on polars-only paths (test `type(x).__module__` or import inside
the branch). pandas sessions are unaffected (pandas imports numpy). Guard with
a fresh-interpreter test like `test_polars_without_pandas.py` asserting that
after `get_session(lib="polars")` + `.laps` + `.results` (if feasible) numpy
is not in `sys.modules`, and that the first `pick_*`/telemetry access still
works. Measure polars `import_get_session,laps,results,state` and confirm
`driver_tel,all_tel,notebook` do not regress (numpy loads later there).
Parity: identical polars digest. Target: polars laps wall 364 -> ~310 ms.
Report which workloads keep numpy and why; if `results`/`state` cannot avoid
it cheaply, keep them as they are and say so.
