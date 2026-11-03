# E1 intended commit (git commit was denied in the sandbox; everything is staged)

```
perf(elis-E1): load(telemetry="lazy") fetches a driver's laps in one wave on first access

load(telemetry="lazy") registers the pending laps by driver instead of
fetching all 1128 telemetry files; the first access of any lap of a driver
(lap.telemetry, get_car_data, Laps.telemetry, get_fastest_lap_tel) fetches
that driver's laps in one parallel wave, building the frames the eager batch
builds (same fetch options, frame-tier writes). On a cached session the
second wave reads every remaining driver in one pooled batch (Delphi D5).
fetch_all_laps_telemetry() after a lazy load fetches the untouched drivers in
one batch and returns its map in laps-row order.

Config key load_telemetry_lazy (TIF1_LOAD_TELEMETRY_LAZY, default False)
makes telemetry=True mean "lazy".

Cold 2025 Bahrain four-driver notebook: 9811 -> 2985 ms pandas, 9944 -> 3014
ms polars (5/5 pairs each); parity 0/133 and 0/131 digests, 0/1143 frames
eager vs lazy on both backends.

Adds the E1 cold bench copy (notebook_4drivers, notebook_lazy, iterlaps_all),
the eager-vs-lazy frame parity tool, a per-arm-env warm A/B and two probes.
```
