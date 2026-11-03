# D8 handoff: the worktree already holds an implementation; finish it

The worktree `/tmp/tif1-delphi/D8` (branch `perf/delphi-D8`) has an
uncommitted implementation of D8 by a previous agent that stopped before
writing its report (`git diff` shows it: `cache.py`, `frame_codec.py`,
`models.py`, `tests/unit/test_pergamon_contracts.py`). Read that diff first.

## What is there

- `SessionMemo._driver_sources` keeps the decoded `DriverFrameSource` per
  driver (`add_telemetry_source` stores it, `driver_source(driver)` reads
  it, `clear()` drops it).
- `Laps._merged_from_driver_source` (models.py) builds a driver's merged
  telemetry from the memoized source through a new
  `DriverFrameSource.merged(laps, drop_repeats=True)` method in
  `frame_codec.py` (the body of `decode_driver_merged` moved there; the
  `str` group parts now carry their dictionary and codes). When the memo
  has no source for the driver, the row is read with
  `get_telemetry_frame_sources([driver])` (now `parallel=False` for a single
  row) and registered in the memo so later per-lap accesses are hits.
- New tests at the end of `tests/unit/test_pergamon_contracts.py` (+164
  lines). Whether the whole unit suite passes was not confirmed: run it.

## What was measured

- `.agents/perf-delphi/D8_ab_pandas.json` (6 pairs): notebook
  `driver_merged_x4` 81 -> 40 ms (6/6), notebook wall 1079 -> 1010 (3/6);
  `load` 88 -> 112 (2/6) in that run: find out whether D8 touched the
  load path (the `parallel=` flag in `get_telemetry_frame_sources`?) or
  whether it was noise. `driver_tel` (a driver's `Laps.telemetry` without
  `load()`) 22 -> 24 ms (0/6).
- `.agents/perf-delphi/D8_ab_pandas_driver_tel.json` (10 pairs):
  `driver_tel` 23 -> 24 ms (1/10). Small but consistent: the brief says
  this path must not get worse.

## What to do

1. Find the `driver_tel` regression with in-process timers (control
   `get_driver_merged_frame` vs candidate `_merged_from_driver_source`,
   warm cache copy `/tmp/tif1-delphi-warm-D8`). Suspects: the single row
   now decompresses inline (`parallel=False`: a serial lz4 of a ~3.5 MB
   row vs the pool), `add_telemetry_source` registering every lap ref,
   `merged()` doing more than `decode_driver_merged` did (the str parts
   now keep the dictionary). Fix it so the no-`load()` path is at least as
   fast as control (e.g. keep decompressing in the pool and decode inline,
   or build the merged frame from the blob as control did and register
   the source only when it comes for free), while the after-`load()` path
   keeps its gain.
2. Check `load` is untouched (notebook `load` phase equal to control).
3. Unit suite, ruff, ty green. A/B (8-10 pairs, pandas) on
   `notebook,driver_tel,car_data_loop,all_tel`; pandas parity digest
   (every `*/driver_tel` surface identical); polars `driver_tel` A/B to
   confirm no change there.
4. Write `.agents/perf-delphi/D8.md` in the deliverable format of the brief
   and commit everything on the branch.
