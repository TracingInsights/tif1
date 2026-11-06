# N6 intended commit (git commit was denied in the agent session; everything is staged)

```
perf(naxos-N6): cheaper per-lap layers of iterlaps and Lap.telemetry

iterlaps builds each Lap row block directly over one shared BlockPlacement
and skips the Series constructor's object inference when the frame has a
non-nullable numpy int or bool column (every row then stays object dtype;
the rule is probed against maybe_convert_objects once per process, and a
failing probe restores the inference). The frame's object interleave runs
with cyclic GC suspended. Lap.telemetry reads Driver and LapNumber by
cached positions on the shared column Index instead of two Series.get
calls. Values, dtypes, index, names and classes are unchanged; new tests
in test_naxos_n6_lap_rows.py and the pandas-lap-rows seam entry cover the
path. Report, A/B results and probes in .agents/perf-naxos/ and
tools/perf/naxos_n6_*.py.
```
