# C9 intended commit (git commit was denied by the sandbox; everything is staged)

```
perf(corinth-C9): schedules without per-event Series; lib="polars" schedules without pandas

get_event_schedule builds its rows as plain dicts (the values each Event
Series held) instead of one pandas Series per event; get_events_remaining
reads whole columns instead of iterrows; get_event/get_event_by_round
resolve on the schedule payload and build only the matched Event.
get_event_schedule/get_events_remaining(lib="polars") return a polars
frame built from Python lists (no pandas, numpy, pyarrow or query plans):
warm 2025 schedule 605 -> 270 ms, polars season loop 726 -> 368 ms
(10/10 pairs). Schedule digest 568 surfaces identical to the control;
polars frames equal the pandas ones value for value (504 frames).
Docs: api-reference/events.mdx "Polars schedules", README row.
Harness: tools/perf/corinth_c9_schedule_ab.py, corinth_c9_schedule_parity.py.
Report: .agents/perf-corinth/C9.md.
```
