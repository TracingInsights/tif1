# G4: intended commits (`git commit` was denied in the sandbox; everything is staged)

1. `perf(gortyn-G4): polars pick_*, iterlaps(require=) and driver lookups on Series masks, no numpy`

   The selections of `PolarsLaps` (every `pick_*`), `iterlaps(require=...)`,
   `PolarsSessionResults._position_for` (`get_driver`) and `PolarsDriver.get_lap`
   built numpy masks, so the first of them imported numpy (~55 ms) in a polars
   process. They now use Boolean Series twins of the mask helpers (`_filled`
   through `zip_with`, `_present_series`, `_valid_series`, `_isin_series`,
   `_text_test_series`, `_first_true`, `_true_positions`) on eager kernels
   only; the ~20-row results lookup uses Python lists. Each method is as fast
   or faster than the numpy mask with numpy loaded (the notebook's eight picks
   1.67 -> 1.47 ms). `PolarsTelemetry` methods keep numpy. Tests:
   `tests/unit/test_gortyn_g4_series_masks.py` (every method against the numpy
   implementation it replaced on 120 random frames), and
   `tests/unit/test_polars_without_pandas.py` (numpy mode) now asserts the
   pick family, `iterlaps(require=)`, the driver lookups and the fastest lap's
   `get_telemetry()` leave numpy unloaded. AGENTS.md updated.

   Files: `src/tif1/models_polars.py`, `tests/unit/test_gortyn_g4_series_masks.py`,
   `tests/unit/test_polars_without_pandas.py`, `AGENTS.md`.

2. `perf(gortyn-G4): probes and A/B harness copy with the lap_analysis workload`

   Files: `tools/perf/gortyn_g4/` (`who_imports_notebook.py`, `method_bench.py`,
   `method_bench_ab.py`, `real_parity.py`, `real_parity_ab.py`, `warm_bench.py`,
   `warm_ab.py`, `analysis_split.py`, `analysis_split_ab.py`, `late_numpy_probe.py`).

3. `docs(gortyn-G4): report`

   Files: `.agents/perf-gortyn/G4.md`, `G4_ab_polars.json`, `G4_ab_polars_10.json`,
   `G4_ab_polars_8.json`, `G4_methods.json`, `G4_COMMITS.md` (the `.log` twins are
   gitignored and stay in the worktree).

(The second G4 agent's commit attempt was denied as well; everything is staged.)
