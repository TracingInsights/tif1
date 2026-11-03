# E7 intended commit (git commit was denied in the sandbox; everything is staged)

```
perf(elis-E7): small warm pandas wins

- Laps.iloc[i]: box a multi-column timedelta block with one
  ints_to_pytimedelta call instead of one TimedeltaArray __getitem__ per
  column (iloc_200 27 -> 18 ms in process).
- pick_driver: slice a category Driver column's contiguous run of rows
  instead of a boolean take (0.65 -> 0.16 ms; ~5 -> ~1 ms as the first
  take of a process).
- split_qualifying_sessions: QualifyingSession marks once per distinct
  ASCII label in Python instead of three pyarrow kernels and a regex pass
  (fresh-process split 12 -> 6 ms); any other label set keeps the pandas
  path.
- _timedelta_ns / _clock_ns_values: the column's own int64 view for a
  nanosecond column (the same read-only view, ~70-95 us less a call).
- Session._lap_start_lookup: the Driver codes mask instead of an object
  array of the category column (no pyarrow take).
- ergast: QueryPlan, ExecutedQuery and RoundInfo as plain __slots__
  classes with the dataclass constructor, repr and equality (~1.7 ms of
  dataclass processing off the first session.results of a process).
- tests/unit/test_elis_e7.py; tools/perf/elis_e7_*.py probes.
```
