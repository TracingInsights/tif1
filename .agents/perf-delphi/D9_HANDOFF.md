# D9 handoff: the worktree holds a promising implementation; finish it

The worktree `/tmp/tif1-delphi/D9` (branch `perf/delphi-D9`) has an
uncommitted implementation by a previous agent that stopped before writing
its report (`git diff` shows `src/tif1/cache.py`, +179/-31; plus the untracked
`tests/unit/test_delphi_d9_row_reads.py` and `tools/perf/delphi_d9_rowread.py`).
Read that diff and the two files first.

## What is there

- The pandas frame-tier driver rows are no longer read through one cursor on
  the caller's thread. The main connection answers one index query
  (`driver, rowid`); each row is then read on one of
  `_FRAME_READER_CONNECTIONS` (2) read-only reader connections by a
  decompression worker through `Connection.blobopen(...).read()` (GIL
  released, no SQLite record buffer, the worker's arena reuses its buffer)
  and decompressed there (`_read_frame_row`, `_submit_frame_row_reads`,
  `Cache._open_reader` / `_release_reader` / `_reader_slots`,
  `_read_frame_blob_locked`). A row the reader's snapshot does not hold
  (written in this process, not committed) falls back to the main
  connection under the cache lock; readers close with the cache; the
  serial path (no pool) still reads through the cursor.
- The previous agent's probe (`tools/perf/delphi_d9_rowread.py`) found,
  fresh process, 20 race rows: 1 reader 31 ms, 2 readers 20-26 ms, 4
  readers 20 ms, 8 readers 37 ms; 22 700 minor page faults and 44 ms of
  system time for the control read of 70 MB.

## What was measured

- The integrator's own probe (`.agents/perf-delphi/D9.md` in the main repo,
  a REJECT draft written before this implementation was seen) found no gain
  from parallel cursors on several connections; it did not try `blobopen`
  from the workers. Your report replaces that draft.
- `.agents/perf-delphi/D9_ab_pandas.json` (6 pairs, box loaded by other
  agents): inconclusive.
- `/home/harish/Documents/tif1/.agents/perf-delphi/D9_ab_pandas_rerun.json`
  (10 pairs, quieter box, this tree vs control): `all_tel` phase 110 -> 89
  ms, wall 668 -> 651 (7/10); notebook `load` 84 -> 61 ms, wall 950 -> 938
  (9/10); `driver_tel` 22 -> 22 (5/10). The D4 report
  (`/tmp/tif1-delphi/D4/.agents/perf-delphi/D4.md`, section 6) explains
  why this works where D4 failed: the cursor loop re-acquires the GIL once
  per row and convoys behind any Python-heavy thread; `blobopen` reads in
  the workers avoid the loop.

## What to do

1. Review the implementation for correctness and safety: reader connection
   lifecycle (opened lazily, closed in `_release_resources`/`close()`, never
   leaked when the pool is shut down; `check_same_thread=False` use is
   sound), the snapshot fallback (a row written in this process before its
   commit, and a row updated between the index query and the blob read),
   `rowid` stability (the table must not be `WITHOUT ROWID`; a VACUUM
   between the two steps cannot happen inside one call), behaviour when
   `blobopen` is unavailable or raises, the single-row case (one driver:
   is the worker path still faster than the cursor, or should it stay
   inline?), the legacy per-lap table path (unchanged?), Windows (no
   filesystem path through `str()`; the probe script uses `str(db_path)`
   for `sqlite3.connect`, which is fine for a connection string, but check
   the library code), and memory (2 readers x a 3.5 MB row in flight).
   Fix what is wrong.
2. Unit suite, ruff, ty green (the new test file included; add cases for
   the fallbacks you touched).
3. A/B 8-10 pairs, pandas, `all_tel,notebook,driver_tel,car_data_loop`;
   polars `all_tel,driver_tel` to confirm no change there (the polars tier
   has its own reader); the pandas parity digest (every `all_tel/frames`
   surface identical). Also try `_FRAME_READER_CONNECTIONS` of 3 and 4 in a
   quick fresh-process probe and keep the best.
4. Write `.agents/perf-delphi/D9.md` in the deliverable format of the brief
   (hypothesis, mechanism, measurements, verdict, changelog paragraph,
   integrator notes) and commit everything on the branch.
