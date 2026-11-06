# C5 progress

- 18:55 Read brief/hypothesis/N10. Finding: CPython 3.14.5+ reverted the incremental GC
  (3.14.8 headers: `generations[NUM_GENERATIONS]`, `dummy1 /* was work_to_do */`), yet
  `gc_promote_ok()` gates N10's promotion off for every 3.14. Measuring that first.
- 19:20 Gate widened (`generational_gc`: 3.14.5+, 3.15 beta+). 3.14 pandas, 12 pairs
  (corinth_c5_cpu_ab.py): all_tel wall 597 -> 557 (12/12), notebook 675 -> 636 (10/12),
  get_session GC 19 -> 0 ms (12/12); laps / load_only walls flat (8/12, 6/12). Staged
  (commit denied). Next: the rest of the 3.13-vs-3.14 gap (import tif1 14 -> 30 ms etc.).
- 19:45 Same tree, 3.13 vs 3.14 after the gate: in-process within ~10 ms (laps script 454 vs
  455, all_tel 624 vs 642). The rest is upstream (numpy import +16 ms, stdlib
  logging->traceback->_colorize dataclasses +6.5, site +5-8 at startup; pyarrow -10).
  Parity 0/133, 0/131 on 3.13 and 3.14 (3.14 control == 3.13 control too). Unit suite green
  on both (4129 passed). uv denied: no 3.15 / 3.14.0-3.14.4 runs.
- 19:55 Experiment 2: per-lap `SessionMemo._materialize_telemetry` under
  `suspend_gc(promote=True)` (the iterlaps loop's 30 young collections free 0 objects):
  loop GC 8-25 -> ~1 ms on both interpreters. Load ~41: waiting to A/B.
- 21:05 Final A/Bs (load 28-41). 3.14 pandas: all_tel 592 -> 561 (7/8), notebook 743 -> 694
  (7/8); polars laps 245 -> 218 (7/8). 3.13 unchanged within noise; iterlaps loop 141 -> 124
  (9/12). Parity 0/133, 0/131 on both; unit 4131 passed on both; ruff/ty clean; perf gate
  6 passed. Report C5.md written: SHIP both. Everything staged.
