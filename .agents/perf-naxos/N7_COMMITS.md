# N7 commits (branch perf/naxos-N7)

`git commit` is denied in this sandbox: everything is staged (`git add -A`).
Intended commits, in order (commit 1 = everything outside
`.agents/perf-naxos/` and `tools/perf/naxos_n7_*.py`; commit 2 = those):

1. `perf(naxos-N7): tel.v2 columnar telemetry files with predicted channels`

   The columnar telemetry files (`{driver}/tel.v2.{first}.bin`, replacing the
   never-published `tel.v1`) store the channels fastf1 and the data pipeline
   derive from others as the ULP distance to a prediction computed with IEEE
   `+ - * /` only: `numpy.interp` over the rows' float64 nanosecond dates
   (one start date per lap, found mod 512 ns) for the car channels at the
   position rows, a quadratic interpolating spline for `x`/`y`/`z` at the car
   rows and `distance` at the position rows, `distance`'s integration,
   `rel_distance = distance / D`, and the pipeline's `acc_x/y/z` (fdlibm
   `atan2` spelled out). Lossless (every lap equal to the JSON decode bit for
   bit, native and numpy decoders). 2025 Bahrain race 52.50 -> 10.79 MB
   (gzip JSON 71.32 MB), qualifying 17.79 -> 4.51 MB. Native kernels
   `tel_integ`/`tel_accx`/`tel_lateral`/`tel_spline`/`tel_interp`/`tel_apply`
   in tif1-native (`native/src/telcols.rs`, GIL released, no new crate),
   numpy twins otherwise; the reader asks for the files only when the
   kernels are installed. Encoder `--verify` decodes with both. Format doc,
   AGENTS.md, native README, tests.

2. `perf(naxos-N7): cold A/B and decode harnesses, report`

   `tools/perf/naxos_n7_cold_local.py` (G3's local throttled CDN, each tree
   encodes its own files, `telemetry_columnar` on in both arms,
   `--candidate-python`), `tools/perf/naxos_n7_decode_bench.py`, and the
   report `.agents/perf-naxos/N7.md` with the result JSON files.
