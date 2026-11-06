# C5 intended commits (`git commit` is denied in this sandbox; everything is staged)

1. `perf(corinth-C5): GC promotion on CPython 3.14.5+ and 3.15 (generational collector again)`
   - `src/tif1/core_utils/gc_control.py`: `generational_gc(version)`; `gc_promote_ok()` uses it
     instead of `sys.version_info < (3, 14)`.
   - `tests/unit/test_naxos_n10_gc_promote.py`: the gate table (3.14.0/3.14.4/3.14.5/3.14.8,
     3.15 alpha/beta/final, 3.16, free-threaded).
   - `AGENTS.md`: the `gc_control.py` entry.

2. `perf(corinth-C5): promote the survivors of the lazy per-lap frame build`
   - `src/tif1/cache.py`: `SessionMemo._materialize_telemetry` builds and adopts the lap's frame
     under `suspend_gc(promote=True)`.
   - `tests/unit/test_naxos_n10_gc_promote.py`: `test_per_lap_materialization_*`.

3. `chore(corinth-C5): probes and report`
   - `tools/perf/corinth_c5_*.py`, `.agents/perf-corinth/C5*.{md,json,txt}`.
