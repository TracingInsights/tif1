# G4 handoff: finish a measured hypothesis whose agent was cut off by a full /tmp

The worktree `/tmp/tif1-gortyn/G4` holds a complete implementation of G4 by a
previous agent: every polars `pick_*` method, `iterlaps(require=...)`,
`get_driver()`, `get_fastest_lap()` and `get_lap()` filter with polars
Series masks instead of numpy (all in `src/tif1/models_polars.py`; the
telemetry calculations stay on numpy), with tests
(`tests/unit/test_gortyn_g4_series_masks.py`, an extended
`tests/unit/test_polars_without_pandas.py`), probes and a harness copy with a
new `lap_analysis` workload (`tools/perf/gortyn_g4/`), and A/B results
(`.agents/perf-gortyn/G4_ab_polars.json`, `G4_ab_polars_10.json`,
`G4_methods.json`; the intended commit message in `G4_COMMITS.md`).

The previous agent's own summary: warm polars, fresh process, interleaved:
`lap_analysis` wall 363 -> 275 ms (-24%, 10/10) and 368 -> 284 (6/6);
notebook `picks_x8` phase 98-114 -> 3 ms (6/6), notebook wall 585 -> 580
(6/10) and 627 -> 638 (2/6); `laps`, `fastest_lap_tel`, `quali_split` within
noise. It could not write the report or stage its last edits because `/tmp`
hit its quota (now cleared: 13 GB free).

What remains (everything else is done):
1. `git add -A` (some of `models_polars.py` is unstaged); read the diff once.
2. Gates on this tree: ruff check/format, ty, the unit suite (`-n 6`).
3. Parity: `delphi_parity.py --lib polars` against `/tmp/gortyn_parity_polars.json`
   (0 differing expected) and `--lib pandas` against `/tmp/gortyn_parity_pandas.json`
   (no pandas code changed).
4. If the 2/6 notebook run worries you, one more 8-pair `notebook,lap_analysis`
   A/B with the harness copy (both arms interleaved, `--lib polars`); the
   phase (`picks_x8`) carries the verdict, the wall must not be worse.
5. Write `.agents/perf-gortyn/G4.md` in the brief's deliverable format from
   the JSON artifacts and the summary above (method-by-method numbers are in
   `G4_methods.json`), and commit on the branch (or leave staged with
   `G4_COMMITS.md` if commits are denied).
