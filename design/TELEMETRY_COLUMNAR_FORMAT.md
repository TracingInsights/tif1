# Columnar telemetry files (`tel.v2`)

Status: format v2 (Naxos N7), reader in tif1; the data repositories do not
publish the files yet. Version 1 (Gortyn G3) was never published and is
replaced: no reader asks for `tel.v1` files. Codec:
`src/tif1/core_utils/columnar_telemetry.py`; native prediction kernels:
`native/src/telcols.rs` (`tif1_native.tel_integ`, `tel_accx`,
`tel_lateral`, `tel_spline`, `tel_interp`, `tel_apply`); encoder for the
publishing pipeline:
`tools/data/encode_driver_telemetry.py`.

## Why

A session's car telemetry is published as one `{driver}/{lap}_tel.json` per
lap: ~150 KB of JSON, ~61 KB gzipped, 1128 files for the 2025 Bahrain race
(71.3 MB gzipped). A cold `load()` with telemetry is bound by those bytes
(~9 s on a ~8 MB/s link) and a driver's wave is 57 requests.

| 2025 Bahrain | lap JSON, gzip | `tel.v1` (G3) | `tel.v2` |
|---|---|---|---|
| Race (1128 laps) | 71.32 MB | 52.50 MB | 10.79 MB (119 files) |
| Qualifying (277 laps) | 24.27 MB | 17.79 MB | 4.51 MB (34 files) |

`tel.v1` stored typed columns. Its floor was the float64 channels whose
mantissas look random (`distance`, `rel_distance`, `DistanceToDriverAhead`,
`acc_x/y/z`, and the interpolated half of `rpm`, `speed`, `throttle`, `x`,
`y`, `z`): ~65% of a file at 6-7 bytes per sample. Most of them are not
random: fastf1 and the data pipeline compute them from other channels.
`tel.v2` stores those channels as predictions plus a correction (below). The
values are unchanged: lossless, bit for bit.

The key observation: fastf1's `merge_channels` interpolates over the
`Date` index, which pandas hands to numpy/scipy as int64 nanoseconds
converted to float64. The live-timing dates are millisecond-precise, so a
row's date is `float(T0 + time_ms * 10**6)` with `T0` the lap's start date,
and only `T0 mod 512` matters (float64 nanosecond dates of 2006-2043 are
multiples of 256, and `10**6 ≡ 64 (mod 512)`). With the right `T0` the
interpolated samples are reproduced: `numpy.interp` exactly, scipy's
quadratic spline to within a few ULPs.

## Paths

`{year}/{gp}/{session}/{driver}/tel.v2.{first}.bin`, next to the lap JSON,
where `first` = 1, 11, 21, ... and the file holds that driver's laps
`first`..`first + 9` that have telemetry. A reader derives every path from the
lap numbers, so there is no manifest. A lap with no JSON file is not in the
file either.

Why 10 laps: one file per driver downloads over one connection, ~4.7 MB/s
against a 6 MB/s per-connection cap in the local model, and was slower than
the 57-request JSON wave spread over 22 connections (G3: 1726 vs 1518 ms,
`driver_tel`). Ten laps keep 6 requests in flight per race driver (~190 KB
each in v2).

Size budget: a race adds ~11 MB and qualifying ~4.5 MB per event to the
`{year}` repository (about +16% of the gzipped JSON). jsDelivr serves files up
to 20 MB from GitHub; one file is ≤ ~0.12 MB.

## Layout (little-endian)

```
magic   8 bytes   b"TIF1TEL\x02"     (last byte = format version)
raw     u64       size of the decompressed body
frame   zstd      one frame (level 19) holding the body:
body:   u32 header length | header (JSON, orjson) | zero padding to 8 | buffers
```

Header:

```json
{"version": 2,
 "laps": [{"lap": 1, "keys": ["time", "rpm", ..., "dataKey"],
           "arrays": {"time": ["f8", 793], "gear": ["i8", 793], "DriverAhead": ["dict", 793]},
           "json": {"dataKey": "2025-Bahrain Grand Prix-Race-VER-1"},
           "cats": {"DriverAhead": ["None", "1"]}}, ...],
 "anchors": ["|u1", 0, 992],
 "columns": [{"key": "time", "kind": "f8", "enc": "dec", "k": 3, "delta": true,
              "buf": ["<i2", 1000, 7930]},
             {"key": "speed", "kind": "f8", "enc": "lerp", "ref": "rpm",
              "anchor": {...}, "rest": {...}, "res": ["|u1", 52000, 3960]}, ...]}
```

- `keys`: the lap's `tel` keys in payload order. The frame builders keep that
  order.
- `arrays`: the channels the native decoder returns as arrays, with their
  dtype (`i8` int64, `f8` float64, `dict` DictionaryChannel) and length.
- `json`: every other value as JSON (the `dataKey` scalar, a channel the
  decoder keeps as a list).
- `cats`: the `DriverAhead` categories of that lap, in first-appearance
  order, the upstream `"None"` token included.
- Rows: when every lap has a float64 `time`, the laps' samples concatenated
  in lap order are the file's rows. A float64 channel with every lap's length
  equal to its `time` length is row-aligned; only those can be predicted, and
  a prediction never reaches across a lap boundary.
- `anchors`: the car rows, a bit mask (`packbits`) over the rows: the rows
  where `rpm`, `speed` and `throttle` are all whole (written when the three
  are row-aligned).
- `dates`: one int64 per lap, the lap's start date `T0` in nanoseconds
  (written with `anchors` when `time` is row-aligned). The encoder searches
  `T0 = 2**60 + 2**59 + k`, `0 <= k < 512`, for the `k` under which `interp`
  reproduces the most `rpm` samples. A row's date is
  `float64(T0 + rint(time * 1000) * 10**6)` (int64 arithmetic, wrapping; NaN
  when `|rint(time * 1000)| >= 2**40` or `time` is NaN).
- `columns`: one per (channel, dtype), in decoding order: the laps' arrays
  concatenated in lap order. A buffer is `[numpy dtype str, byte offset in
  the buffers area, element count]`. Multi-byte buffers are byte-shuffled
  (all first bytes, then all second bytes, ...). A predicted column names the
  channels it reads (`ref`, `speed`, `time`, `of`, `x`, `other`, `distance`);
  they come before it.

### Value encodings (as v1)

| `enc` | used for | stored | decoded |
|---|---|---|---|
| `int` | int64 channels (`gear`, `brake`, `drs`), DriverAhead codes | smallest signed int dtype | `astype(int64)`; DriverAhead `astype(int32)`, -1 = null |
| `dec` | float64 channels where every sample is `n / 10**k`, `k` ≤ 6, all finite, no `-0.0` (`time`, k=3) | `n` narrowed, as deltas when they are smaller or the column never decreases | `cumsum` (deltas) then `n / 10.0**k` |
| `split` | float64 channels with whole samples (`rpm`, `x`, `y`, `z`, ...) | bit mask of whole samples (`packbits`), the whole samples as narrowed ints, the rest raw float64 | `out[mask] = ints`, `out[~mask] = rest` |
| `f64` | anything else | raw float64 | as stored |

A "whole" sample in `split` is finite, `v == trunc(v)`, `|v| < 2**31` and not
`-0.0`. NaN (the upstream `"None"`/null) always stays in the float64 part.

### Predicted encodings (v2)

A predicted sample is stored as `res`: the zigzag-coded distance in units in
the last place from a prediction `p` to the true value `v`:

```
key(f)   = bits(f) XOR ((bits(f) >> 63) AND 0x7FFF_FFFF_FFFF_FFFF)   # int64, order of the floats
d        = key(v) - key(p)                                           # int64, wrapping
res      = (d << 1) XOR (d >> 63)                                    # uint64, narrowed to u1/u2/u4/u8
v        = key⁻¹(key(p) + unzigzag(res))                             # key is its own inverse
```

Every bit pattern round-trips (NaN payloads, `-0.0`, infinities). A NaN
prediction is replaced by `0.0` before use (the NaN that arithmetic produces
differs between CPUs). Predictions use only IEEE double `+ - * /` and
`fmod` (exact), evaluated in the order written here, never a fused
multiply-add, so every decoder computes the same `p`: the numpy twins in the
codec and the Rust kernels agree bit for bit on all 1.3 M rows of the 2025
Bahrain race and qualifying (`tests/unit/test_columnar_telemetry.py` pins
it).

| `enc` | channel | rows predicted | prediction | where it comes from |
|---|---|---|---|---|
| `interp` | `rpm`, `speed`, `throttle`, `DistanceToDriverAhead` (`time`) | non-anchor rows with an anchor before (`j0`) and after (`j1`) in the lap | `numpy.interp` spelled out over the row dates: `slope = (y1 - y0) / (x1 - x0)`, `p = slope * (x - x0) + y0`; when NaN, `slope * (x - x1) + y1`, then `y0` if `y0 == y1`; `y0` when `x == x0` | fastf1's merge interpolates the car channels linearly over the dates (`interpolate(method="index")`) at the position rows |
| `lerp` | `speed`, `throttle`, `DistanceToDriverAhead` (`ref` = `rpm`), when `interp` is larger | as `interp`, and `rpm[j0] != rpm[j1]` | `f = (rpm[i] - rpm[j0]) / (rpm[j1] - rpm[j0])`, `p = y[j0] + (y[j1] - y[j0]) * f` | the same interpolation without the dates: all car channels of a row share the fraction, which `rpm` shows |
| `spline` | `x`, `y`, `z` (`time`) | rows where the channel is not whole (the car rows) | per lap, the quadratic interpolating spline through the whole samples (the position rows) over the row dates, evaluated at the row's date (see below) | fastf1 interpolates `X`/`Y`/`Z` with `interpolate(method="quadratic")` (scipy `make_interp_spline(k=2)`) |
| `integ` | `distance` | anchor rows with an anchor before (`j`) in the lap, in row order | `p = d[j] + speed[i] / 3.6 * (time[i] - time[j])` (reads the decoded `d[j]`) | fastf1's `integrate_distance` (`cumsum(speed / 3.6 * dt)`) over the car samples |
| `integs` | `distance` | as `integ`, and the non-anchor rows | `integ` at the anchors; at the other rows, the lap's quadratic spline through the anchors (as `spline`) | fastf1 interpolates `Distance` with the quadratic spline too |
| `ratio` | `rel_distance` (`of` = `distance`) | all rows | `p = distance[i] / D_lap` (`div`: one float64 per lap, chosen by the encoder) | fastf1's `add_relative_distance` |
| `accx` | `acc_x` | all rows | per lap: `a = g(speed / 3.6) / g(time)`, interior `a[i] > 25` replaced by `a[i-1]`, `w = a * (1/3)`, `p[i] = (w[i-1] + w[i]) + w[i+1]`, ends `w[0] + w[1]` and `w[n-2] + w[n-1]` | the pipeline's longitudinal acceleration |
| `lat` | `acc_y` (`other` = `y`), `acc_z` (`other` = `z`) | all rows | per lap: `θ = atan2(g(other), g(x) + eps)`, `θ[0] = θ[1]`, `unwrap(θ)`, `dθ = g(θ)` with interior `\|dθ[i]\| > 0.5` replaced by `dθ[i-1]`, `a = ((speed / 3.6)²) * (dθ / (g(distance) + 1e-4))`, `\|a\| > 150` → 0, `p[i] = Σ a[j] * (1/9)` over `j = i-4 .. i+4` in the lap, left to right from 0.0 | the pipeline's lateral and vertical acceleration |

`g` is `numpy.gradient` with unit spacing (`(f[i+1] - f[i-1]) / 2`, one-sided
at the ends). A lap of one sample predicts 0. `atan2` is fdlibm's
(`s_atan.c` reduction and polynomial, `e_atan2.c` quadrant rules) written
with `+ - * /` only, so it does not depend on the platform's libm or on
numpy's SIMD loops. `unwrap` is `numpy.unwrap` (period 2π) with the
remainder spelled out from `fmod`. The replacement steps take the previous
*input* sample (`a[i-1]` before any replacement): that is what the published
values match.

The spline (`spline`, `integs`): per lap, through the points `(xd, yd)`
(`n` of them, strictly increasing dates; fewer than three, a NaN date or a
non-increasing one predict 0): knots `xd[0]` three times, the midpoints
`(xd[i + 1] + xd[i]) / 2` for `i = 1 .. n - 3`, `xd[n - 1]` three times
(scipy's choice for `k = 2`); each point's knot interval `l` (`t[l] <= x <
t[l + 1]`, clamped to `[2, n - 1]`) and its three B-spline values from
scipy's `_deBoor_D` recursion; the collocation matrix is tridiagonal (an
entry outside predicts 0 for the lap); it is solved without pivoting
(`f = sub[i] / diag[i-1]`, `diag[i] -= f * sup[i-1]`, `r[i] -= f * r[i-1]`,
then `c[i] = (r[i] - sup[i] * c[i+1]) / diag[i]`), and a target `x` evaluates
to `((0 + c[l-2] h0) + c[l-1] h1) + c[l] h2`. LAPACK's banded solver rounds
differently, so the result lands a few ULPs off scipy's in places; the true
spline also had the samples just outside the lap (fastf1 pads the slice), so
the first and last few rows of a lap carry larger residuals.

`interp` and `lerp` store three parts: the anchor rows' values with a value
encoding (`anchor`), the rows they do not predict with a value encoding
(`rest`), and `res`. `spline` stores the whole-sample mask and integers (as
`split`) and `res` for the other rows. `integ` stores `rest` (non-anchor rows
and each lap's first anchor) and `res`; `integs` stores `rest` (each lap's
first anchor), `res` and `spl` (the non-anchor rows). `ratio`, `accx` and
`lat` store `res` for every row. The encoder estimates each candidate's size
(zstd -3 of its buffers) and keeps the smallest, the value encoding included,
so a channel the predictions do not fit costs nothing extra.

What stays a value encoding: the anchor rows of `rpm`, `speed`, `throttle`,
`DistanceToDriverAhead` (fastf1's driver-ahead distances depend on the other
cars), the whole samples of `x`/`y`/`z`, `time`, the integer channels. Those
are ~60% of a race file now.

Residual sizes on the 2025 Bahrain race: `interp` exact on the interior rows
(the misses are next to car rows whose `speed` is NaN, which are not
anchors); `distance` 99.4% exact at the anchors and ~100% at the position
rows; `x`/`y`/`z` exact on about two laps in three; `rel_distance` 99.9%
within ±4 ULP; `acc_x` ~4 bits; `acc_y` ~3 bits; `acc_z` ~2 bits. The
residuals are not zero because the pipeline computed with operations this
codec does not reproduce (OpenBLAS's fused dot products in
`numpy.convolve`, libm's `atan2`, LAPACK's pivoted band solve).

## Exactness

The decoder returns, per lap, exactly what
`json_utils.decode_telemetry_payload` returns for the lap's JSON with
`tif1-native`: the same keys in the same order, int64/float64 arrays with
identical bits (NaN payloads included), DictionaryChannel codes (int32) and
categories, the same Python values for the rest. The frame builders, the
payload tier writer, the frame tiers and the lazy waves therefore see no
difference.

- `dec`: `n` and `10**k` are exact doubles (`|n| < 2**53`, `k` ≤ 6), so
  `n / 10.0**k` is one correctly rounded IEEE division of the rational
  `n / 10**k`, the same real number the JSON decimal denotes, which the
  parser also rounds correctly.
- Predicted encodings: exact by construction (the residual restores the
  bits whatever the prediction).
- The encoder decodes every column it wrote and compares the bits with its
  input; a column that does not round-trip is stored as `f64`.
- `tools/data/encode_driver_telemetry.py --verify` decodes every file twice
  (native kernels and numpy twins) and builds every lap's pandas and polars
  frame from both and from the JSON (`check_exact=True`): 0 differences on
  all 1405 laps of the 2025 Bahrain race and qualifying.

One assumption outside the file: a process whose floating-point unit flushes
subnormals to zero (some extensions built with `-ffast-math` turn on FTZ/DAZ
for the whole process) computes other predictions when a prediction passes
through a subnormal. Telemetry values are not subnormal in practice; the
decoded values would differ, not fail.

## Reader (tif1)

- Only waves: a batch with two or more laps of one driver
  (`fetch_multiple_async`, `driver_files=None`) or a lazy-stream request
  (`driver_files=True`). A lone lap (`get_fastest_lap_tel`, one lap per
  driver) keeps its 61 KB JSON. A later lap of a file already downloaded is
  served from it.
- One download per file per process (`concurrent.futures.Future` in the fetch
  pool), shared by the wave's lap requests on any event loop. Each lap is
  handed out once; it then lives in the payload/frame tiers as before.
- Decoding: `tif1_native.tel_decode_file` decodes the whole body in one call
  with the GIL released (Corinth C8: ~2.6 ms per ten-lap race file on one
  thread, and the fetch threads decode files in parallel); a build without it
  decodes column by column around the native kernels (~4.1 ms per file
  against 2.3 ms for v1, GIL-bound between kernel calls). Both give the same
  bits. The reader asks for the files only when the
  installed `tif1-native` has the kernels
  (`json_utils.native_columnar_decoder_available()`); the numpy twins decode
  the same bits in ~25 ms per file (the spline's tridiagonal solve runs in
  Python floats), so without the kernels the waves keep the lap JSON.
- Fallback to the lap JSON: when the file is missing, unreadable (bad magic,
  corrupt frame, wrong size: `ColumnarFormatError`), fails on every mirror,
  or lacks the lap.
- 404: only the primary CDN is asked. The mirrors copy the same repository,
  and the JSON fallback is correct either way. The verdict goes to the
  `missing_payloads` tier (key `{year}/{gp}/{session}/{driver}/tel.v2.{first}.bin`,
  and `{year}/{gp}/{session}/tel.v2.session` for the session, TTL
  `missing_payloads_ttl_days`). For the rest of the process no other file of
  that session is asked for.
- Off when `telemetry_columnar` is False (the default until the files are
  published; `TIF1_TELEMETRY_COLUMNAR=1` turns it on), when telemetry is
  validated (the validator reads JSON lists), offline, in Pyodide (no
  fetch pool), and without the native kernels.

## Versioning

The magic's last byte and `header.version` are the format version. A
reader that does not know a version rejects the file
(`ColumnarFormatError`) and falls back to the JSON. A new version gets a new
file name (`tel.v3.{first}.bin`), so old clients keep reading their version or
the JSON. Adding a column encoding or changing a prediction is a version bump.
