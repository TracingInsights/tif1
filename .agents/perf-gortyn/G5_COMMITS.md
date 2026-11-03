# G5 intended commits (git commit was denied in this session; everything is staged)

1. `test(gortyn-G5): bring in G2's private-seam tests and upstream canary`
   - tests/unit/test_private_seams.py, tests/unit/test_upstream_canary_report.py,
     tools/upstream_prereleases.py, tools/upstream_canary_report.py,
     .github/workflows/upstream-canary.yml (copies of G2's files)

2. `perf(gortyn-G5): keep the Q18 lap-frame route and total_seconds parity on pandas 3.1`
   - src/tif1/core_utils/frame_codec.py: `_array_layout()`; `_str_slice`/`_timedelta_array`
     and the `_P_STRC`/`_P_TDN` plan codes mirror pandas 3.1's `_cache` / missing `_freq`
   - src/tif1/models.py: `_differential_distance_fast` and `_fastest_lap_frame` use
     `total_seconds()`; `Laps.reset_index` keeps a `level_0` pandas did not create
     (pandas 3.1 `to_sql(index=True)`)
   - tests: layout test, LapTimeSeconds bit-equality test, level_0 test, pandas 3.1 premises

3. `docs(gortyn-G5): report, A/B harness copy and measurements`
   - .agents/perf-gortyn/G5.md, G5_*.json, G5_perf_gate_new_pandas.txt, G5_scripts/,
     tools/perf/gortyn_g5_warm_ab.py
