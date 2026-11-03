# D5 handoff: the worktree already holds an implementation; finish it

The worktree `/tmp/tif1-delphi/D5` (branch `perf/delphi-D5`) has an
uncommitted implementation of D5 (`git diff` shows it). Read that diff first.

## What is there

- `src/tif1/cache.py`: `LazyDriverFrameSource` (a stand-in for a driver's
  pandas frame-tier row that reads and decodes the real
  `DriverFrameSource` on the first lap access), `hydrate_lazy_sources`
  (one `get_telemetry_frame_sources` batch for many lazy sources),
  `SessionMemo._hydrate_for` (the first driver's row is read alone; from
  the second driver on, every remaining lazy row is read in one batch),
  `SessionMemo.materialize_telemetry` hydrates all lazy rows in one batch,
  `Cache.get_telemetry_frame_laps` (reads the `laps` column of the rows,
  no blobs) and `Cache.lazy_telemetry_frame_sources`.
- `src/tif1/core.py`: `_telemetry_load_pending` registers the lazy sources
  instead of reading the rows.
- `tests/unit/test_pergamon_contracts.py`: three new tests at the end
  (`test_warm_load_reads_no_driver_row`,
  `test_bulk_reader_hydrates_lazy_rows_in_one_batch`,
  `test_lazy_row_that_cannot_be_read_falls_through`). Whether the whole
  unit suite passes with the change was not confirmed: run it.
- `tools/perf/delphi_warm_bench.py` in the main repo gained a `load_only`
  workload (`load()` then `laps` and `results`, the fastf1 habit).

## What was measured (`.agents/perf-delphi/D5_ab_pandas.json`, 6 pairs)

- `load_only`: `load` 90 -> 8 ms (6/6), wall 650 -> 550 ms (5/6). The win.
- `notebook`: `load` 85 -> 8 ms (6/6) but `iterlaps_tel_loop` 73 -> 158 ms
  (0/6), so the wall is unchanged (952 -> 956). This is the problem.
- `all_tel` 108 -> 113 ms (2/6), `driver_tel` 23 -> 25 (2/6): within noise
  but check them again with more pairs.

## What to do

1. Find why the iterlaps loop (every lap of every driver, `lap.telemetry`
   in `laps.iterlaps()` order) costs +85 ms. By design it should cost about
   what the eager `load()` read cost (~80 ms for the batch) plus one row
   read alone (~15-20 ms): the first driver's row is read on its own, the
   second driver's first lap triggers `hydrate_lazy_sources` for all the
   remaining lazy rows. Instrument it (timers around `_load`,
   `_hydrate_for`, `hydrate_lazy_sources`; cProfile of the loop) on the
   warm cache copy `/tmp/tif1-delphi-warm-D5`. Things to check: whether
   the batch really fires once (or every driver's row is read alone: 20 x
   ~15 ms), whether `get_telemetry_frame_sources` runs for one driver
   without the parallel path, whether every row of the session has a
   non-NULL `laps` column in that cache copy (`SELECT driver, laps IS NULL
   FROM telemetry_frames_drv WHERE year = 2025 AND gp =
   'Bahrain%20Grand%20Prix' AND session = 'Race'`; a NULL means the driver
   gets no lazy source and its laps take the eager batch or the payload
   tier), and whether `_suspend_gc` still covers the batch decode.
2. Fix it so the notebook's iterlaps loop is at most ~20 ms slower than
   control while `load` stays ~8 ms, and `all_tel`/`driver_tel`/
   `car_data_loop` are not worse. If a different trigger rule works better
   (e.g. the first lazy access hydrates every lazy row at once when the
   session has more than N drivers pending, or the batch is started in the
   decompression pool on the first access so later drivers find it done),
   measure the alternatives and keep the best.
3. Unit suite, ruff, ty green. Then the A/B (8 pairs, pandas) on
   `load_only,notebook,all_tel,driver_tel,car_data_loop`, the pandas parity
   digest, and a polars `laps,all_tel` A/B to confirm no change there.
4. Write `.agents/perf-delphi/D5.md` in the deliverable format of the brief
   and commit everything on the branch.
