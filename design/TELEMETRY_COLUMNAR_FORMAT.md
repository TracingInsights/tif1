# Columnar telemetry files (`tel.v1`)

Status: format v1, reader in tif1 (Gortyn G3); the data repositories do not
publish the files yet. Codec: `src/tif1/core_utils/columnar_telemetry.py`;
encoder for the publishing pipeline: `tools/data/encode_driver_telemetry.py`.

## Why

A session's car telemetry is published as one `{driver}/{lap}_tel.json` per
lap: ~150 KB of JSON, ~61 KB gzipped, 1128 files for the 2025 Bahrain race
(71.3 MB gzipped). A cold `load()` with telemetry is bound by those bytes
(~9 s on a ~8 MB/s link) and a driver's wave is 57 requests. The `tel.v1`
files carry the same values in typed columns: 52.5 MB for the race (−26%),
17.8 MB for qualifying (−27%), 10 laps per file, so a race driver's wave is
6 requests.

Delphi estimated −50% from float32 channels. That is lossy: `rpm`, `speed`,
`throttle`, `x`/`y`/`z` hold about one whole sample per interpolated
float64 sample (fastf1's car/position merge), and `distance`,
`rel_distance`, `DistanceToDriverAhead`, `acc_x/y/z` are float64 throughout.
Their interpolated mantissas carry ~52 random bits each. XOR with the
previous sample or with a linear prediction gains ~1%, and `distance` cannot
be recomputed from `speed` and `time` bit for bit. Those six channels make
~65% of a file. Lossless typed columns are the floor without changing the
upstream values.

## Paths

`{year}/{gp}/{session}/{driver}/tel.v1.{first}.bin`, next to the lap JSON,
where `first` = 1, 11, 21, ... and the file holds that driver's laps
`first`..`first + 9` that have telemetry. A reader derives every path from the
lap numbers, so there is no manifest. A lap with no JSON file is not in the
file either.

Why 10 laps: one file per driver (2.6 MB) downloads over one connection,
~4.7 MB/s against a 6 MB/s per-connection cap in the local model, and was
slower than the 57-request JSON wave spread over 22 connections (1726 vs 1518
ms, `driver_tel`). Ten laps (~450 KB) keep 6 requests in flight per race
driver. One file per lap (b) is 6% larger than one per driver (c). Ten-lap
files are 2% above (c) (52.5 vs 51.4 MB).

Size budget: a race adds ~52 MB and qualifying ~18 MB per season event to the
`{year}` repository (about +73% of the gzipped JSON, +30% of the raw JSON).
jsDelivr serves files up to 20 MB from GitHub; one file is ≤ ~0.6 MB.

## Layout (little-endian)

```
magic   8 bytes   b"TIF1TEL\x01"     (last byte = format version)
raw     u64       size of the decompressed body
frame   zstd      one frame (level 19) holding the body:
body:   u32 header length | header (JSON, orjson) | zero padding to 8 | buffers
```

Header:

```json
{"version": 1,
 "laps": [{"lap": 1, "keys": ["time", "rpm", ..., "dataKey"],
           "arrays": {"time": ["f8", 793], "gear": ["i8", 793], "DriverAhead": ["dict", 793]},
           "json": {"dataKey": "2025-Bahrain Grand Prix-Race-VER-1"},
           "cats": {"DriverAhead": ["None", "1"]}}, ...],
 "columns": [{"key": "time", "kind": "f8", "enc": "dec", "k": 3, "delta": true,
              "buf": ["<i2", 0, 7930]}, ...]}
```

- `keys`: the lap's `tel` keys in payload order. The frame builders keep that
  order.
- `arrays`: the channels the native decoder returns as arrays, with their
  dtype (`i8` int64, `f8` float64, `dict` DictionaryChannel) and length.
- `json`: every other value as JSON (the `dataKey` scalar, a channel the
  decoder keeps as a list).
- `cats`: the `DriverAhead` categories of that lap, in first-appearance
  order, the upstream `"None"` token included.
- `columns`: one per (channel, dtype): the laps' arrays concatenated in lap
  order. A buffer is `[numpy dtype str, byte offset in the buffers area,
  element count]`. Multi-byte buffers are byte-shuffled (all first bytes,
  then all second bytes, ...).

Column encodings:

| `enc` | used for | stored | decoded |
|---|---|---|---|
| `int` | int64 channels (`gear`, `brake`, `drs`), DriverAhead codes | smallest signed int dtype | `astype(int64)`; DriverAhead `astype(int32)`, -1 = null |
| `dec` | float64 channels where every sample is `n / 10**k`, `k` ≤ 6, all finite, no `-0.0` (`time`, k=3) | `n` narrowed, as deltas when they are smaller or the column never decreases | `cumsum` (deltas) then `n / 10.0**k` |
| `split` | float64 channels with whole samples (`rpm`, `speed`, `throttle`, `x`, `y`, `z`, ...) | bit mask of whole samples (`packbits`), the whole samples as narrowed ints, the rest raw float64 | `out[mask] = ints`, `out[~mask] = rest` |
| `f64` | anything else | raw float64 | as stored |

A "whole" sample in `split` is finite, `v == trunc(v)`, `|v| < 2**31` and not
`-0.0`. NaN (the upstream `"None"`/null) always stays in the float64 part.

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
  parser also rounds correctly. The two are equal by construction. The
  encoder still compares the bits.
- The encoder decodes every column it wrote and compares the bits with its
  input; a column that does not round-trip is stored as `f64`.
- `tools/data/encode_driver_telemetry.py --verify` also builds every lap's
  pandas and polars frame from both sources and compares them
  (`check_exact=True`): 0 differences on all 1405 laps of the 2025 Bahrain
  race and qualifying.

There are no intentional differences. The payloads are encoded from the
native decoder's output, so the pipeline needs `tif1-native`. A client
without it reads the JSON as orjson lists, and those build the same frames
(`tests/unit/test_native_decoder.py`).

## Reader (tif1)

- Only waves: a batch with two or more laps of one driver
  (`fetch_multiple_async`, `driver_files=None`) or a lazy-stream request
  (`driver_files=True`). A lone lap (`get_fastest_lap_tel`, one lap per
  driver) keeps its 61 KB JSON. A later lap of a file already downloaded is
  served from it.
- One download per file per process (`concurrent.futures.Future` in the fetch
  pool), shared by the wave's lap requests on any event loop. Each lap is
  handed out once; it then lives in the payload/frame tiers as before.
- Fallback to the lap JSON: when the file is missing, unreadable (bad magic,
  corrupt frame, wrong size: `ColumnarFormatError`), fails on every mirror,
  or lacks the lap.
- 404: only the primary CDN is asked. The mirrors copy the same repository,
  and the JSON fallback is correct either way. The verdict goes to the
  `missing_payloads` tier (key `{year}/{gp}/{session}/{driver}/tel.v1.{first}.bin`,
  TTL `missing_payloads_ttl_days`). For the rest of the process no other file
  of that session is asked for. A session without files costs one request
  per process and per TTL, in parallel with nothing else (one round trip
  before the wave's JSON).
- Off when `telemetry_columnar` is False (`TIF1_TELEMETRY_COLUMNAR=0`), when
  telemetry is validated (the validator reads JSON lists), offline, and in
  Pyodide (no fetch pool).

## Versioning

The magic's last byte and `header.version` are the format version. A
reader that does not know a version rejects the file
(`ColumnarFormatError`) and falls back to the JSON. A new version gets a new
file name (`tel.v2.{first}.bin`), so old clients keep reading v1 or the JSON.
Adding a column encoding is a version bump.
