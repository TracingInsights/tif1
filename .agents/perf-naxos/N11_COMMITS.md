# N11 intended commits (`git commit` is denied in this sandbox; everything is staged)

## 1. perf(naxos-N11): a cold lazy driver's lone first touch fetches one lap, not its wave

load(telemetry="lazy") fetched a driver's whole lap set on the first access
of any of its laps, so a lone-lap read (pick_fastest().get_telemetry(),
lap.telemetry, get_fastest_lap_tel()) paid ~57 files, 57 frame builds and
their cache writes at exit. On a cold session the first touch of a pending
driver asking for one lap now fetches that lap alone (sync transport, no
niquests import); the driver's next touch brings the rest in one wave.
Sequential reads (the predicted next driver), laps the stream already
holds and cached sessions keep the wave.

Also tools/perf/naxos_n11_{cold_bench,timeline,build_probe}.py and the
report .agents/perf-naxos/N11.md.
