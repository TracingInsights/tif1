Naxos performance round (N1-N11) plus lazy telemetry by default. Full results: `.agents/perf-naxos/RESULTS.md`; one report per hypothesis in `.agents/perf-naxos/N<k>.md`.

## Behaviour change

- **`load()` is lazy by default** (`load_telemetry_lazy=True`). A driver's telemetry is fetched on the first access of one of its laps; on a cold session a single-lap read fetches that lap alone. fastf1 fetches everything in `load()`: `load(telemetry="eager")`, `load_telemetry_lazy=False` or `TIF1_LOAD_TELEMETRY_LAZY=false` restore that. Documented in `COMPATIBILITY.md` and the migration guide (errors, background retry, offline work, cache contents). Cold four-driver notebook ~10 s -> ~3 s.

## Shipped

| item | effect |
|---|---|
| N10 GC promotion after frame bursts and imports | warm pandas all_tel -12%, get_session GC 26-33 -> 0 ms |
| N3 polars telemetry arithmetic without numpy/query plans | warm polars notebook 716 -> 596 ms, analysis 162 -> 18 |
| N2 warm polars eager load keeps rows lazy | eager `load()` 125-157 -> 9-14 ms |
| N11 lone first touch on cold lazy sessions (+ loop rule) | one driver's fastest-lap telemetry cold 2.9 -> 1.05 s; four drivers 4.2 -> 1.1 s |
| N8 niquests warm-up after the first wave | cold pandas driver_tel 1926 -> 1582 ms, p90 3563 -> 2397 |
| N9 payload-tier writes during the wave | cold post-download tail -1.0..-2.2 s |
| N6 cheaper per-lap layers | pandas lap loop -7..-10 ms |
| N7 tel.v2 columnar files | race 71 MB (gzipped JSON) -> 10.8 MB; cold all_tel at 8 MB/s 9.5 -> 4.3 s. Reader off until the data repos publish the files and tif1-native 0.3.0 ships the kernels |

Not shipped: N1 (OpenBLAS pool deferral, dropped by decision), N4 (lazy submodules, ~5-10 ms), N5 (exit already at its floor).

## Combined, warm (fresh process, wall, median)

- pandas (10 pairs): laps 453 -> 426, all_tel 658 -> 557, notebook 762 -> 711, get_session -25 ms everywhere; 8-10/10 on every workload but driver_tel (even).
- polars (8 pairs): notebook 557 -> 403, load_only 365 -> 212, every workload faster (6-8/8).

## Checks

- Unit suite 4121 passed; ruff, format, ty clean.
- Parity 0/133 (pandas), 0/131 (polars) against the control digests.
- Perf gate 0/120 regressed; baseline rewritten on this tree (digests unchanged).
- N7's Rust kernels are tested by the CI `native` job (built from this commit).

## Follow-ups

- Release tif1-native 0.3.0 (N7 kernels), then publish `tel.v2` files from the data pipeline (`tools/data/encode_driver_telemetry.py --verify`) and turn `telemetry_columnar` on.
- Bump the `tif1-native` floor/lock to 0.2.1 (installed in dev venvs, lock still 0.2.0).
- Add a lazy notebook workload to the perf gate with its own baseline on a quiet machine.
