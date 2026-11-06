# N2: intended commit (git commit was denied in this session; everything is staged)

Commit with `git commit -F` on branch `perf/naxos-N2`, using the message below
(the first line is the subject):

```
perf(naxos-N2): a warm polars load() keeps the driver rows lazy

A warm polars load(telemetry=True) read every driver's IPC frame-tier row
(~115 ms of a race's ~125 ms load) to memoize every lap frame. When the
rows' [lap, rows] spans (read without the blobs) show the tier holds every
pending lap, load() now registers the laps as load(telemetry="lazy") does:
the first two drivers touched read their rows alone, the third wave reads
every remaining row in one pooled batch, and a lap of a
PolarsLaps.iterlaps() walk brings the walk's drivers in one wave, so a loop
over the laps pays the eager read once. Laps the tier lacks keep the eager
batch; cold loads are unchanged. fetch_all_laps_telemetry() keeps laps-row
order when memoized frames and tier frames mix.
```

Not staged on purpose (regenerable scratch, 2-5 MB each): the parity digests
`.agents/perf-naxos/N2_parity_{pandas,polars}.json` and
`.agents/perf-naxos/N2_load_parity_{pandas,polars}_{control,candidate}.json`.
