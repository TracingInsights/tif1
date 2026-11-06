# C8: intended commits (git commit was denied in this sandbox; everything is staged)

Split the staged changes into these three commits, in this order.

## 1. perf(corinth-C8): decode a tel.v2 file in one native call without the GIL

Files: `native/src/telfile.rs` (new), `native/src/lib.rs`,
`native/src/telcols.rs`, `native/README.md`,
`src/tif1/core_utils/columnar_telemetry.py`,
`src/tif1/core_utils/json_utils.py`, `tests/unit/test_columnar_telemetry.py`,
`AGENTS.md`, `design/TELEMETRY_COLUMNAR_FORMAT.md`.

```
perf(corinth-C8): decode a tel.v2 file in one native call without the GIL

tif1_native.tel_decode_file (native/src/telfile.rs) decodes the whole file
body: the orjson header, the byte-shuffled buffers, the value encodings, the
anchors and lap dates, every prediction and the residuals, mirroring the
Python decode_driver_file step for step; only the result objects need the
GIL. decode_driver_file calls it after the zstd step and falls back to the
column-by-column decoder on ValueError (any layout it does not take).

The kernels compute the same bits faster: the quadratic spline is split into
a date-only Basis (shared per lap between x/y/z when their point rows match)
and a per-channel evaluation, residuals are applied without branches, and
unwrap skips fmod when |dd + pi| < 2 pi (fmod returns its argument there).

Race (119 files): one thread 472 -> 298 ms, 8 threads 276 -> 74 ms; decoded
laps bit-identical on all 1405 laps of the 2025 Bahrain race and qualifying.
```

## 2. tools(corinth-C8): decode bench, cold harness with per-arm interpreters, three-decoder verify

Files: `tools/perf/corinth_c8_decode_bench.py`,
`tools/perf/corinth_c8_cold_local.py`, `tools/data/encode_driver_telemetry.py`.

```
tools(corinth-C8): decode bench, cold harness with per-arm interpreters, three-decoder verify

corinth_c8_decode_bench.py times a session's tel.v2 files one-thread and in
an N-thread pool and compares the whole-file, column and numpy decoders bit
for bit. corinth_c8_cold_local.py is N7's local CDN harness with
--control-python and --same-files. encode_driver_telemetry.py --verify now
decodes with all three decoders.
```

## 3. docs(corinth-C8): report and measurements

Files: `.agents/perf-corinth/C8.md`, `C8_PROGRESS.md`, `C8_COMMITS.md`,
`C8_cold_local_{8,100}mbps_{pandas,polars}.json`,
`C8_cold_verify_{pandas,polars}.json`, `C8_ab_pandas.json`,
`C8_parity_{pandas,polars}.json`.

```
docs(corinth-C8): report and measurements
```
