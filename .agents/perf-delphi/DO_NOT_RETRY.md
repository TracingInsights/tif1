# Ideas already measured in earlier rounds (do not retry)

Network / CDN / transport: full hedging of every request; sharding a batch
across CDN hosts or mirrors; concurrency above 22 (64, 44/88 connections all
slower or timing out), slow-start ramps; StaticDelivr-first; brotli or zstd
Accept-Encoding (gzip is served; zstd alone returns uncompressed bodies);
jsDelivr `.min.json`; several HTTP sessions or connections; uvloop; worker
pools instead of semaphore+gather; aiohttp/curl_cffi (measured, not
integrated); overlapping the laps prelude with the telemetry wave; merging
weather/rcm into the laptimes wave; a speculative Ergast thread on cold loads.

GIL / threading: parse offload during the fetch (+8%); streamed/overlapped
frame assembly; thread-parallel frame assembly (2.5x slower); overlapping the
cache flush with assembly (+36%); overlapping python-lz4 with unpickle
(python-lz4 holds the GIL on decompress; pyarrow's lz4 codec releases it and is
what the frame tier uses); speculative tier prefetch while pandas imports
(slower 3/3); gc.disable during the cold fetch (+15.6%); gc.freeze across
sessions at runtime (no gen-2 collections to save).

Codecs / storage: msgpack, msgspec, pyarrow.json, pysimdjson; a zstd trained
dictionary; zstd -3; stronger lz4 levels; zstd instead of lz4 for frames; raw
uncompressed frame blobs; one blob per session; columnar arrays rebuilt
through the pandas constructor; Arrow IPC for pandas frames (pandas rows use
the typed-columnar TF1 encoding); DriverAhead object instead of str; compact
integer encoding of float channels; one shared frame tier for both backends;
float narrowing / byte shuffle; cache tiers for weather/rcm/results.

SQLite: executemany, synchronous=OFF; one combined write transaction;
page-cache PRAGMA; page_size 65536; blobopen incremental I/O; lazy DDL. mmap
is on (Olympia) and stays on.

Frame construction / dtypes: polars-first assembly for pandas; merged-dict
assembly + per-lap slicing (breaks per-lap dtype parity); numpy-first Int64
construction; float->timedelta via multiply/view; TimedeltaIndex(seconds*1e9);
single-call seconds->timedelta (breaks the NaN guard); pre-typed laps; raw
ndarray Driver column (infers str); numpy concat in the merge; block-concat
bypass; byte-level "None" rewrite; object-column 2-D gather; numpy offsets
string build; dictionary-cast or int8 indices instead of pyarrow take;
per-driver from_arrow in worker threads; lazy polars slicing; fastest laps
from the frame tier; Driver.laps from the laps tier or by slicing the warm
laps frame (stale tier data).

Rust: a Rust JSON->columnar parser (0%); a Rust lap builder (1 ms/session).
Only the GIL-released telemetry decoder (tif1-native) paid off and is shipped.

Import / startup: lazy pandas for pandas sessions (impossible); generic import
trims of tif1's own modules (2.7 ms); logging (9.5 ms); schedule path (5 ms).

Misc: Config.get memo; validation share; results first touch; orchestration
remainder; per-session overhead; orjson-bytes write; batched prefetch
persistence.

Delphi (2026-11-03): decoding the pandas driver rows in the decompression
workers or on a dedicated decoder thread (D4): the decode is GIL-bound (20
rows: 1 worker 24 ms, 8 workers 33-43 ms) and any Python-heavy thread beside
the frame-tier cursor loop slows the 70 MB read 2-3x (the loop re-acquires
the GIL once per row); lowering the switch interval does not rescue it.
Parallel SQLite cursors on several connections for the driver rows (D9
probe): 29-34 ms vs 27 ms; `sqlite3.Blob` has no `readinto` on CPython 3.13.
(Reading each row with `blobopen` inside the decompression worker is a
different thing and did pay off: D9.) Note for D2's predecessor entries
"generic import trims (2.7 ms)" and "schedule path (5 ms)": D2 re-measured
them with pandas 3 and shipped the lazy block probe, the lazy async/CDN/types
imports and the pathlib schedule read (-13 ms); what remains (~20 ms) is
tif1's own module bodies and is not deferrable.
