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

Elis (2026-11-03): starting the niquests import early at the first-wave
handover or at the first telemetry access (GIL convoy with the laps build /
no lead: E9); reading frame-tier rows ahead beside a loop over every lap
(page-fault contention beats the batch: E5); an unconditional lazy-wave
lookahead (a background wave in a foreground wave's tail: +24% on a
four-driver notebook: E10); decompressing into huge-page buffers for a wall
win (exact mechanism, 1-3% wall: E4, now a release follow-up); a polars
query plan on any hot path (0.5 ms each; eager Series kernels are fine: E6).

Gortyn (2026-11-03): chunked `blobopen` reads into a huge-page buffer (halves
the remaining faults, costs GIL round trips: G1); one columnar file per driver
instead of ten-lap files (one connection at ~4.7 MB/s is slower: G3); the
`telemetry_columnar` reader on by default before the data repositories publish
the files (one 404 per session, +10% cold `driver_tel`: G3). Shipped and not to
be redone: polars selections on eager Series masks (G4), liblz4 + huge-page
buffers in tif1-native 0.2.1 (G1/G6, now on PyPI), the pandas 3.1 probe fixes
(G5), the upstream canary (G2).

Post-Gortyn fix (PR #125): an empty frame-tier batch no longer imports pyarrow
for the lz4 codec (polars `fetch_all` 289 -> 150 ms).

Naxos (2026-11-06): deferring OpenBLAS's thread-pool start during numpy's
import (N1: one-thread start, restore + pinning + unpin thread; -5..-10% warm
pandas walls on a 32-core box) was dropped by the maintainer's decision: tif1
must not change other libraries' thread placement or the process environment.
Module-level LazyLoader deferral of pandas/polars/pyarrow submodules (N4:
~5-10 ms, unmeasurable); trimming interpreter exit (N5: already at its floor).
