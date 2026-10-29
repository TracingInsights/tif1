"""Unified caching: SQLite tier, in-memory LRU tiers, backend lap caches, session memo.

This module owns every cache layer in tif1 so there is a single place that knows
what is cached where:

- :class:`LRUCache` — the one bounded-LRU implementation, shared by the
  in-memory tiers and the process-global backend lap caches.
- :class:`Cache` — persistent SQLite tier plus in-memory LRU tiers, fronted by
  a kind-based interface (:meth:`Cache.get_entry`, :meth:`Cache.set_entry`,
  :meth:`Cache.invalidate`). The long-standing ``get``/``set``/
  ``get_telemetry``/``set_telemetry``/``has_session_data`` methods are thin
  wrappers over it and remain the hot paths.
- :func:`get_backend_lap_cache` / :func:`clear_lap_cache` — the process-global
  pandas/polars lap-caching tier (``tif1.core.clear_lap_cache`` remains an
  alias for backward compatibility).
- :class:`SessionMemo` — the per-``Session`` memo tier: path-keyed JSON
  payloads, (driver, lap)-keyed telemetry payloads and materialized
  DataFrames, fastest-lap-reference state, the persistent-cache probe result,
  and per-driver telemetry failure tracking.
"""

from __future__ import annotations

import atexit
import contextlib
import gc
import importlib
import logging
import os
import pickle
import sqlite3
import sys
import threading
import time
import zlib
from collections import OrderedDict
from collections.abc import Iterable, Iterator
from itertools import repeat
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import quote, unquote

from .core_utils.constants import MAX_CACHE_SIZE
from .core_utils.json_utils import (
    NATIVE_TELEMETRY_DECODER_AVAILABLE,
    decode_telemetry_payload,
    json_dumps_bytes,
    json_loads,
)
from .core_utils.runtime import IS_EMSCRIPTEN, THREADS_AVAILABLE

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


TelemetryCacheKey = tuple[int, str, str, str, int]
CacheKind = Literal["json", "telemetry"]
InvalidateScope = Literal["all", "memory", "json", "telemetry", "ergast"]
SessionMemoKind = Literal["json", "telemetry_payload", "telemetry_df"]
FastestLapRefKind = Literal["laps", "drivers"]

# SQLite-tier compression: telemetry payloads compress ~12x (numeric JSON),
# cutting insert/WAL volume on cold write-cache loads. Values below the
# threshold stay plain TEXT so small rows keep the legacy storage shape.
# zstd-1 compresses ~4.5x faster than zlib-3 at a slightly better ratio and
# decompresses ~2x faster (measured on the 1452-payload Monaco set), so it is
# the codec whenever a zstd binding is available; zlib remains the fallback and
# legacy zlib rows stay readable (zstd frames carry a 4-byte magic prefix).
#
# Python 3.14+ ships zstd in the standard library (PEP 784, ``compression.zstd``)
# and is preferred there; older interpreters use the third-party ``zstandard``
# wheel. Both bind the same libzstd and emit identical single-frame blobs with
# the content size in the header, so a cache written under one binding reads
# under the other (verified: package one-shot decompress of stdlib frames and
# vice versa; compress/decompress throughput within noise of each other).
_SQLITE_COMPRESS_MIN_BYTES = 4096
_SQLITE_COMPRESS_LEVEL = 3
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
_ZSTD_LEVEL = 1

_zstd_compress: Callable[[bytes], bytes] | None
_zstd_decompress: Callable[[bytes], bytes] | None
# Exception types a corrupt zstd frame raises; empty when no binding is present
# so ``except _ZSTD_ERRORS`` stays valid (an empty tuple catches nothing).
_ZSTD_ERRORS: tuple[type[Exception], ...]

if sys.version_info >= (3, 14):  # pragma: no cover - interpreter dependent
    try:
        from compression import zstd as _stdlib_zstd
    except ImportError:  # interpreter built without libzstd
        _stdlib_zstd = None
else:  # pragma: no cover - interpreter dependent
    _stdlib_zstd = None

if _stdlib_zstd is not None:  # pragma: no cover - interpreter dependent
    _STDLIB_ZSTD_COMPRESSOR = _stdlib_zstd.ZstdCompressor(level=_ZSTD_LEVEL)
    _STDLIB_ZSTD_FLUSH_FRAME = _stdlib_zstd.ZstdCompressor.FLUSH_FRAME

    def _zstd_compress(blob: bytes) -> bytes:
        # One long-lived compressor (context reuse, internally locked);
        # FLUSH_FRAME closes the frame so each call yields a standalone blob.
        return _STDLIB_ZSTD_COMPRESSOR.compress(blob, _STDLIB_ZSTD_FLUSH_FRAME)

    _zstd_decompress = _stdlib_zstd.decompress
    _ZSTD_ERRORS = (_stdlib_zstd.ZstdError,)
else:  # pragma: no cover - interpreter dependent
    try:
        # importlib keeps the static checker out of it: the wheel is only
        # declared for python_version < "3.14", so a plain import statement is
        # unresolvable on newer interpreters.
        _zstandard = importlib.import_module("zstandard")

        _zstd_compress = _zstandard.ZstdCompressor(level=_ZSTD_LEVEL).compress
        _zstd_decompress = _zstandard.ZstdDecompressor().decompress
        _ZSTD_ERRORS = (_zstandard.ZstdError,)
    except ImportError:  # defensive fallback only
        _zstd_compress = None
        _zstd_decompress = None
        _ZSTD_ERRORS = ()
        # Reached only on an interpreter built without libzstd (3.14+) or with
        # the zstandard wheel removed (3.11-3.13): payload rows fall back to
        # zlib-3, which is measurably slower. Surface it instead of hiding it.
        logger.info("No zstd binding available; SQLite cache falls back to zlib compression")

# Pergamon Q15: a ``zstandard.ZstdDecompressor`` must not be used by two
# threads at once, so worker threads decompress through a per-thread
# instance; the stdlib module-level ``decompress`` (3.14+) builds its own
# context per call and is safe as is. Both release the GIL while decoding.
_zstd_decompress_mt: Callable[[bytes], bytes] | None
if _stdlib_zstd is not None or _zstd_decompress is None:  # pragma: no cover - interpreter dependent
    _zstd_decompress_mt = _zstd_decompress
else:  # pragma: no cover - interpreter dependent
    _ZSTD_TLS = threading.local()

    def _zstd_decompress_mt(blob: bytes) -> bytes:
        dctx = getattr(_ZSTD_TLS, "dctx", None)
        if dctx is None:
            dctx = _ZSTD_TLS.dctx = _zstandard.ZstdDecompressor()
        return dctx.decompress(blob)


# Materialized-frame codec: lz4 (level 0) halves both compress and decompress
# time vs zstd-1 on the 1452-frame Monaco set (~2x faster, +26% stored bytes)
# — the frame tier is write-once/read-many, so read/write speed beats ratio.
# zstd remains the fallback (and legacy zstd rows stay readable via magic).
_lz4_frame: ModuleType | None
try:  # pragma: no cover - trivial import guard
    # importlib for the same reason as zstandard: no lz4 wheels on 3.15 yet.
    _lz4_frame = importlib.import_module("lz4.frame")
    _LZ4_MAGIC = b"\x04\x22\x4d\x18"
except ImportError:  # pragma: no cover - defensive fallback only
    _lz4_frame = None
    _LZ4_MAGIC = b""

# The frame tier needs at least one of its codecs; zlib is deliberately not a
# frame-tier fallback (the tier is a pure speed optimization over the payload
# tier, and zlib would make it slower than re-parsing the payload).
_FRAME_CODEC_AVAILABLE = _lz4_frame is not None or _zstd_compress is not None


def _compress_frame_blob(blob: bytes | bytearray | memoryview) -> bytes:
    """Compress a pickled frame for the frame tier (lz4 when available)."""
    if _lz4_frame is not None:
        return _lz4_frame.compress(blob, compression_level=0)
    if _zstd_compress is None:  # pragma: no cover - callers gate on the tier
        raise ValueError("No frame-tier codec available")
    # The zstd binding is typed for bytes; the driver encoder hands a view.
    return _zstd_compress(blob if isinstance(blob, bytes) else bytes(blob))


_FRAME_COMPRESS_WORKERS = 8


def _frame_compress_pool(n_blobs: int) -> Any:
    """Thread pool for the driver-blob compress of one write (None below two blobs).

    Only python-lz4 is known to release the GIL here; the zstd fallback
    compresses inline.
    """
    if n_blobs < 2 or _lz4_frame is None or not THREADS_AVAILABLE:
        return None
    from concurrent.futures import ThreadPoolExecutor

    return ThreadPoolExecutor(
        max_workers=min(_FRAME_COMPRESS_WORKERS, n_blobs), thread_name_prefix="tif1-frame-lz4"
    )


def _decompress_frame_blob(blob: bytes) -> bytes | bytearray:
    """Decompress a frame-tier blob; both lz4 and legacy zstd rows read.

    lz4 rows come back as a writable ``bytearray`` so the typed-columnar
    decoder can view them in place instead of copying. Raises ``ValueError``
    when no importable codec can read the row (for example a legacy zstd row
    on an interpreter with lz4 but no zstd binding), so the batch reader
    treats it as a corrupt-row miss instead of aborting.
    """
    if _lz4_frame is not None and blob[:4] == _LZ4_MAGIC:
        return _lz4_frame.decompress(blob, return_bytearray=True)
    if _zstd_decompress is None or _zstd_decompress_mt is None:
        raise ValueError("No codec available for frame-tier row")
    return _zstd_decompress_mt(blob)  # thread-safe: P2 calls this from workers


# Pergamon P2: python-lz4 holds the GIL, so decompressing 1400+ frame blobs
# is serial (~35 ms on the Monaco set) however many cores there are.
# pyarrow's lz4 codec reads the same lz4-frame blobs, releases the GIL and
# returns a mutable buffer (no bytearray copy): 8 worker threads measured
# 33 -> 10 ms, and the chunks are consumed as they complete so frame
# decoding overlaps the remaining decompression. The lz4 frame header
# carries the content size python-lz4 stored (FLG bit 3); blobs without it,
# legacy zstd rows and pyarrow builds without lz4 use the serial path.
_PA_LZ4_CODEC: Any = None
_PA_LZ4_PROBED = False
_PARALLEL_DECOMPRESS_MIN_BLOBS = 32
_PARALLEL_DECOMPRESS_MAX_WORKERS = 8
# Legacy per-frame hits below this count are not worth a background rewrite.
_LEGACY_UPGRADE_MIN_FRAMES = 32
_LEGACY_UPGRADE_DELAY_S = 0.25


#: pyarrow's system allocator for the decompressed rows and the IPC reads.
#: The default pool (mimalloc) warms up a heap per thread on first use (~40 ms
#: in a fresh process) and, on the multi-MB buffers of a frame-tier read,
#: stalls for hundreds of ms: the 20 polars IPC rows of a race took 0.5-4.6 s
#: serially (53 ms-1.2 s on 8 threads) against 66-142 ms (22-35 ms).
_PA_LZ4_POOL: list[Any] = [None]


def _pa_system_pool() -> Any:
    """pyarrow's system memory pool (see :data:`_PA_LZ4_POOL`), resolved once."""
    if _PA_LZ4_POOL[0] is None:
        _PA_LZ4_POOL[0] = importlib.import_module("pyarrow").system_memory_pool()
    return _PA_LZ4_POOL[0]


def _pa_lz4_codec() -> Any:
    global _PA_LZ4_CODEC, _PA_LZ4_PROBED
    if not _PA_LZ4_PROBED:
        try:
            pa = importlib.import_module("pyarrow")
            codec = pa.Codec("lz4")
            _pa_system_pool()
            _PA_LZ4_CODEC = codec
        except Exception:  # pragma: no cover - pyarrow without lz4 support
            _PA_LZ4_CODEC = None
        # Marked only once the codec and pool exist: a concurrent first caller
        # never sees "probed" with the codec still unset.
        _PA_LZ4_PROBED = True
    return _PA_LZ4_CODEC


def _lz4_frame_content_size(blob: bytes) -> int:
    """Content size stored in an lz4 frame header, or -1 when absent."""
    if len(blob) < 15 or blob[:4] != _LZ4_MAGIC or not (blob[4] & 0x08):
        return -1
    return int.from_bytes(blob[6:14], "little")


# Pergamon Q11: one process-wide decompression pool instead of an executor
# per batch (thread spin-up + join measured ~0.6 ms per warm load), and one
# task per row when the rows are driver-sized (22 x ~1 MB rows went into 3
# chunks of 8 before: 11.5 -> 6.2 ms on 8 threads). The driver-tier reader
# submits each row the moment SQLite hands it over, so the blob assembly of
# the next row overlaps the decompression of the previous ones.
_DECOMPRESS_POOL: Any = None
_DECOMPRESS_POOL_LOCK = threading.Lock()
_DECOMPRESS_THREAD_PREFIX = "tif1-frame-decompress"
# Rows at least this big are decompressed one per task; smaller rows (the
# legacy per-lap tier, ~16 KB each) are chunked so task overhead stays small.
_PER_ROW_TASK_MIN_BYTES = 256 * 1024


def _decompress_workers() -> int:
    if not THREADS_AVAILABLE:
        return 1
    return min(_PARALLEL_DECOMPRESS_MAX_WORKERS, os.cpu_count() or 1)


def _decompress_pool() -> Any:
    """The shared decompression executor (created on first use, idle costs nothing)."""
    global _DECOMPRESS_POOL
    pool = _DECOMPRESS_POOL
    if pool is None:
        with _DECOMPRESS_POOL_LOCK:
            pool = _DECOMPRESS_POOL
            if pool is None:
                from concurrent.futures import ThreadPoolExecutor

                pool = ThreadPoolExecutor(
                    max_workers=_decompress_workers(), thread_name_prefix=_DECOMPRESS_THREAD_PREFIX
                )
                _DECOMPRESS_POOL = pool
    return pool


def _reset_decompress_pool() -> None:
    """Forget the pool (a forked child must not submit to the parent's threads)."""
    global _DECOMPRESS_POOL
    _DECOMPRESS_POOL = None


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_decompress_pool)


def _can_decompress_in_parallel(codec: Any) -> bool:
    # A pool task that waited on the pool would deadlock once every worker
    # did the same, so work submitted from a worker thread runs inline.
    return (
        codec is not None
        and _decompress_workers() >= 2
        and not threading.current_thread().name.startswith(_DECOMPRESS_THREAD_PREFIX)
    )


def _decompress_row(row: tuple[Any, ...], codec: Any) -> tuple[Any, ...] | None:
    """``(*key, buffer)`` for one frame-tier row, or None when no codec reads it."""
    blob = row[-1]
    size = _lz4_frame_content_size(blob) if codec is not None else -1
    try:
        if size >= 0:
            # pyarrow's decoder is ~2x python-lz4's even serially
            # (0.9 vs 1.9 ms on a 1.2 MB driver row) and copies nothing.
            return (
                *row[:-1],
                codec.decompress(blob, decompressed_size=size, memory_pool=_PA_LZ4_POOL[0]),
            )
        return (*row[:-1], _decompress_frame_blob(blob))
    except (*_ZSTD_ERRORS, RuntimeError, TypeError, ValueError, _arrow_error()):
        return None


def _decompress_frame_rows(
    rows: list[tuple[Any, ...]],
    workers: int | None = None,
    min_parallel: int = _PARALLEL_DECOMPRESS_MIN_BLOBS,
) -> Iterator[list[tuple[Any, ...]]]:
    """Yield ``rows`` with the trailing blob decompressed, in completion order chunks.

    Each yielded chunk is a list of ``(*key, buffer)`` tuples; ``buffer`` is
    writable (a ``bytearray`` or a mutable pyarrow buffer) for lz4 rows.
    Rows a codec cannot read are dropped (the caller logs the miss).
    """
    n_rows = len(rows)
    codec = _pa_lz4_codec()
    if workers is None:
        workers = _decompress_workers()

    def _decompress_chunk(part: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        out: list[tuple[Any, ...]] = []
        for row in part:
            decoded = _decompress_row(row, codec)
            if decoded is not None:
                out.append(decoded)
        return out

    if workers < 2 or n_rows < min_parallel or not _can_decompress_in_parallel(codec):
        yield _decompress_chunk(rows)
        return

    from concurrent.futures import as_completed

    if sum(len(row[-1]) for row in rows) >= n_rows * _PER_ROW_TASK_MIN_BYTES:
        chunk_size = 1
    else:
        chunk_size = max(8, -(-n_rows // (workers * 2)))
    parts = [rows[i : i + chunk_size] for i in range(0, n_rows, chunk_size)]
    pool = _decompress_pool()
    futures = [pool.submit(_decompress_chunk, part) for part in parts]
    for future in as_completed(futures):
        yield future.result()


def _submit_frame_rows(rows: Iterable[tuple[Any, ...]]) -> list[Any]:
    """Start decompressing ``rows`` as they are produced (typically a cursor).

    Returns the pending futures, or the decompressed rows themselves when
    the work had to run inline; :func:`_completed_frame_rows` consumes
    either. Iterating a cursor here keeps SQLite's blob assembly for the
    next row concurrent with the decompression of the rows already read.
    """
    codec = _pa_lz4_codec()
    if not _can_decompress_in_parallel(codec):
        return [row for row in map(_decompress_row, rows, repeat(codec)) if row is not None]
    pool = _decompress_pool()
    return [pool.submit(_decompress_row, row, codec) for row in rows]


def _completed_frame_rows(pending: list[Any]) -> Iterator[tuple[Any, ...]]:
    """Yield the decompressed rows of :func:`_submit_frame_rows` as they complete."""
    if not pending:
        return
    if not hasattr(pending[0], "result"):
        yield from pending
        return
    from concurrent.futures import as_completed

    for future in as_completed(pending):
        row = future.result()
        if row is not None:
            yield row


# Pergamon Q15: a warm load whose frame tiers are empty (pre-frame-tier
# caches, an interrupted background frame write, invalidated frame tables,
# a frame schema bump) reads the payload tier: 1452 rows of zstd JSON on the
# Monaco set, 130 ms of zstd and 370 ms of native parse serially. Both
# release the GIL, so worker threads decode chunks while the caller
# consumes finished ones. Without ``tif1-native`` the workers only
# decompress and the caller parses with orjson (which holds the GIL).
_PARALLEL_PAYLOAD_MIN_ROWS = 64
_PARALLEL_PAYLOAD_MAX_WORKERS = 6
_PARALLEL_PAYLOAD_CHUNKS_PER_WORKER = 4


def _usable_cpu_count() -> int:
    """CPUs this process may run on (affinity / cgroup aware where possible)."""
    if not THREADS_AVAILABLE:
        return 1
    try:
        return len(os.sched_getaffinity(0))
    except (AttributeError, OSError):  # pragma: no cover - non-Linux
        return os.cpu_count() or 1


def _decode_payload_rows(
    rows: list[tuple[Any, Any, Any]],
    workers: int | None = None,
    min_parallel: int = _PARALLEL_PAYLOAD_MIN_ROWS,
) -> Iterator[list[tuple[Any, Any, Any, Any]]]:
    """Yield payload-tier ``(driver, lap, data)`` rows as ``(driver, lap, raw, parsed)`` chunks.

    ``raw`` is the decompressed JSON blob, ``parsed`` the decoded payload.
    Chunks come in completion order. A row that fails to decompress or
    parse raises ``ValueError`` from the consuming iteration, like the
    serial reader.
    """
    n_rows = len(rows)
    if workers is None:
        workers = min(_PARALLEL_PAYLOAD_MAX_WORKERS, _usable_cpu_count())
    if workers < 2 or n_rows < min_parallel:
        yield [
            (driver, lap, raw, decode_telemetry_payload(raw))
            for driver, lap, data in rows
            for raw in (_decode_sqlite_value(data),)
        ]
        return

    parse_in_worker = NATIVE_TELEMETRY_DECODER_AVAILABLE

    def _decode_chunk(part: list[tuple[Any, Any, Any]]) -> list[tuple[Any, Any, Any, Any]]:
        out: list[tuple[Any, Any, Any, Any]] = []
        for driver, lap, data in part:
            raw = _decode_sqlite_value(data)
            out.append(
                (driver, lap, raw, decode_telemetry_payload(raw) if parse_in_worker else None)
            )
        return out

    from concurrent.futures import ThreadPoolExecutor, as_completed

    chunk_size = max(8, -(-n_rows // (workers * _PARALLEL_PAYLOAD_CHUNKS_PER_WORKER)))
    parts = [rows[i : i + chunk_size] for i in range(0, n_rows, chunk_size)]
    with ThreadPoolExecutor(max_workers=min(workers, len(parts))) as pool:
        futures = [pool.submit(_decode_chunk, part) for part in parts]
        try:
            for future in as_completed(futures):
                chunk = future.result()
                if not parse_in_worker:
                    chunk = [
                        (d, lap, raw, decode_telemetry_payload(raw)) for d, lap, raw, _ in chunk
                    ]
                yield chunk
        finally:
            for future in futures:
                future.cancel()


# Parsed-object front tiers: bounded small because parsed payloads are far
# larger than blobs. Repeat hits skip orjson entirely (see RESULTS.md H4).
_PARSED_CACHE_MAX_ITEMS = 128
_PARSED_TELEMETRY_CACHE_MAX_ITEMS = 256

# Materialized-frame tables per backend: pandas frames live in
# ``telemetry_frames`` (pre-existing), polars frames in ``telemetry_frames_pl``
# so both backends can keep a warm frame tier without PK collisions and
# without migrating existing caches.
_FRAME_TABLES: dict[str, str] = {"pandas": "telemetry_frames", "polars": "telemetry_frames_pl"}
# Thebes TH8: polars frames are materialized per driver as one Arrow IPC
# blob (lz4 inside the IPC stream, decoded multi-threaded by polars) with
# the lap boundaries alongside; a lap is a zero-copy slice of the driver
# frame. The per-frame ``telemetry_frames_pl`` table stays readable (rows
# from earlier releases, and drivers whose laps do not share one schema).
_POLARS_IPC_TABLE = "telemetry_frames_pl_ipc"
# Pergamon P3: pandas frames are materialized per driver as one typed-columnar
# blob (``frame_codec.encode_driver_frames``): a session reads ~22 rows
# instead of ~1450, and every lap frame is a view into its driver's arrays.
# The per-frame ``telemetry_frames`` table stays readable (rows written by
# earlier releases, frames whose blocks the codec does not know); a session
# served from it is re-materialized into driver rows in the background.
_PANDAS_DRIVER_TABLE = "telemetry_frames_drv"
_ALL_FRAME_TABLES: tuple[str, ...] = (
    *_FRAME_TABLES.values(),
    _POLARS_IPC_TABLE,
    _PANDAS_DRIVER_TABLE,
)

_ARROW_ERROR: type[Exception] | None = None


def _arrow_error() -> type[Exception]:
    """``pyarrow.ArrowException``, resolved on first use (only when an error is matched).

    A module-level ``from pyarrow import ...`` cost ~160 ms (pyarrow + numpy) on
    every ``import tif1.cache``, including processes that never read a frame
    (``tif1 cache-info``, schedule lookups).
    """
    global _ARROW_ERROR
    if _ARROW_ERROR is None:
        try:  # pragma: no cover - pyarrow is a declared dependency
            from pyarrow import ArrowException

            _ARROW_ERROR = ArrowException
        except ImportError:  # pragma: no cover
            _ARROW_ERROR = ValueError
    return _ARROW_ERROR


def _write_polars_ipc(frame: Any) -> bytes:
    """Arrow IPC file bytes (lz4 frames) for a polars DataFrame.

    pyarrow's writer/reader pair costs ~1 ms per driver blob where polars'
    ``read_ipc`` on an in-memory buffer measured 4-60 ms per call (Thebes
    TH8 probe: 22 blobs 18 ms via pyarrow vs 105-1284 ms via polars).
    """
    import polars as pl
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc

    # Pergamon Q6: strings are stored as Arrow ``string_view`` (polars' own
    # String layout) so the read converts them zero-copy instead of
    # rebuilding views from ``large_string`` offsets (~5 ms per session).
    # Rows written with ``large_string`` read back to the same frame.
    table = frame.to_arrow(compat_level=pl.CompatLevel.newest())
    sink = pa.BufferOutputStream()
    options = pa_ipc.IpcWriteOptions(compression="lz4")
    with pa_ipc.new_file(sink, table.schema, options=options) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _read_polars_ipc(blob: bytes) -> Any:
    """polars DataFrame from Arrow IPC file bytes written by :func:`_write_polars_ipc`."""
    import polars as pl
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc

    table = pa_ipc.open_file(pa.BufferReader(blob), memory_pool=_pa_system_pool()).read_all()
    return pl.from_arrow(table, rechunk=True)


# Schema version of the materialized-frame tier, stored in SQLite's
# ``user_version``. Frame rows carry no per-row version, so when the stored
# version is older the frame tables are dropped once and rebuild from the
# payload tier (which is untouched). History:
#   1 — telemetry "None" sentinels re-typed (DistanceToDriverAhead float, not
#       str); frames written before that hold the stringified column.
#   2 — fastf1 parity (0.11.0): telemetry in fastf1's column order with
#       Source/Status, polars Duration/Datetime/Boolean channels; the laps
#       frame tables are dropped too.
#   3, 4 — 0.11.0 development steps (a 0.10 cache goes straight to 4):
#       polars laps flags (IsPersonalBest, FreshTyre, ...) Boolean with a
#       missing flag read as False, and all-null polars laps columns typed
#       (String/Int64/Float64) instead of Null, as on pandas.
#   5 — 0.11.0 development: pandas rows written while ``SessionTime`` and
#       ``Time`` shared one block stored only the first of the two columns.
_FRAME_TIER_SCHEMA_VERSION = 5

# SQLite maps at most this much of the file (its compile-time ceiling,
# SQLITE_MAX_MMAP_SIZE, caps a larger request anyway).
_SQLITE_MMAP_BYTES = 2**31 - 2**16

# Laps frame tier (Kroton KR-1/KR-10): one processed laps DataFrame per
# (year, gp, session) per backend, mirroring the telemetry frame tier's
# blob format.
_LAPS_FRAME_TABLES: dict[str, str] = {
    "pandas": "laps_frames",
    "polars": "laps_frames_pl",
}


@contextlib.contextmanager
def _suspend_gc():
    """Pause cyclic GC for a short-lived burst of container allocations.

    Batch frame reads/writes allocate ~100 MB of short-lived objects in a
    tight loop; the resulting gen-0 collections cost ~40% of the unpickle
    loop (Rhodes R3: 470 -> 264 ms on the Monaco 1452-frame set) while no
    reference cycles are created. Restores the prior GC state, so nesting
    and callers that disabled GC themselves are safe.
    """
    if gc.isenabled():
        gc.disable()
        try:
            yield
        finally:
            gc.enable()
    else:
        yield


def _encode_sqlite_value(blob: bytes) -> bytes | str:
    """Serialize a JSON blob for the SQLite tier (compress large payloads)."""
    if len(blob) >= _SQLITE_COMPRESS_MIN_BYTES:
        if _zstd_compress is not None:
            return _zstd_compress(blob)
        return zlib.compress(blob, _SQLITE_COMPRESS_LEVEL)
    return blob.decode("utf-8")


def _decode_sqlite_value(stored: Any) -> Any:
    """Invert :func:`_encode_sqlite_value`.

    ``bytes`` marks a compressed row (zstd frames start with a 4-byte magic;
    anything else is a legacy zlib row); ``str`` is plain legacy JSON. A
    corrupt compressed row raises ``ValueError`` so callers' existing error
    handling turns it into a cache miss.
    """
    if isinstance(stored, bytes):
        if (
            _zstd_decompress is not None
            and _zstd_decompress_mt is not None
            and len(stored) >= 4
            and stored[:4] == _ZSTD_MAGIC
        ):
            try:
                return _zstd_decompress_mt(stored)
            except _ZSTD_ERRORS as e:
                raise ValueError(f"Corrupt compressed cache entry: {e}") from e
        try:
            return zlib.decompress(stored)
        except zlib.error as e:
            raise ValueError(f"Corrupt compressed cache entry: {e}") from e
    return stored


# Sentinel returned by SessionMemo.get_fastest_lap_ref_if_current on a source-id miss.
FASTEST_LAP_REF_MISS: Any = object()

# JSON payload paths the session memo retains (plus any nested "DRIVER/..." path).
_MEMO_KNOWN_JSON_PATHS = frozenset(
    {
        "drivers.json",
        "rcm.json",
        "weather.json",
        "position.json",
        "car_data.json",
        "session_info.json",
        "session_laptimes.json",
    }
)


class LRUCache:
    """Thread-safe bounded LRU cache.

    The single LRU implementation backing both the :class:`Cache` in-memory
    tiers and the process-global backend lap caches.

    Reads support a lock-free fast path (``ordered=False``): ``OrderedDict.get``
    is atomic in CPython thanks to the GIL, so hot-path reads skip the lock
    entirely and LRU ordering is maintained through writes only.
    """

    def __init__(self, maxsize: int = MAX_CACHE_SIZE, *, lock: threading.Lock | None = None):
        """Initialize the cache.

        Args:
            maxsize: Maximum number of entries before oldest-first eviction.
            lock: Optional externally-owned lock (lets a container serialize its
                own operations against the same lock it exposes publicly).
        """
        self.cache: OrderedDict[Any, Any] = OrderedDict()
        self.maxsize = maxsize
        self.lock = lock if lock is not None else threading.Lock()

    def get(self, key: Any, *, ordered: bool = True) -> Any | None:
        """Get a value by key.

        Args:
            key: Cache key.
            ordered: When True (default), refresh the LRU position under the
                lock. When False, perform a lock-free read without touching
                LRU ordering (fast path for hot readers).
        """
        if not ordered:
            return self.cache.get(key)
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    def set(self, key: Any, value: Any) -> None:
        """Store a value, evicting the oldest entry when over capacity."""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.maxsize:
                self.cache.popitem(last=False)

    def clear(self) -> None:
        """Clear all cached items."""
        with self.lock:
            self.cache.clear()

    def pop(self, key: Any, default: Any = None) -> Any:
        """Remove key and return its value, or default when absent."""
        with self.lock:
            return self.cache.pop(key, default)

    def __contains__(self, key: Any) -> bool:
        return key in self.cache

    def __len__(self) -> int:
        return len(self.cache)

    def __iter__(self):
        """Lock-free key iteration.

        Concurrent mutation may raise ``RuntimeError``; callers that iterate
        without the lock must handle that (this matches the previous bare
        ``OrderedDict`` iteration semantics).
        """
        return iter(self.cache)


# Process-global backend lap caches (keyed by "{year}_{gp}_{session}_laps").
_global_lap_cache = LRUCache(maxsize=MAX_CACHE_SIZE)
_global_lap_cache_polars = LRUCache(maxsize=MAX_CACHE_SIZE)


def get_backend_lap_cache(lib: Literal["pandas", "polars"]) -> LRUCache:
    """Get the global lap cache instance for a specific DataFrame library."""
    return _global_lap_cache_polars if lib == "polars" else _global_lap_cache


def clear_lap_cache() -> None:
    """Clear both global backend lap caches (pandas and polars)."""
    _global_lap_cache.clear()
    _global_lap_cache_polars.clear()


class SessionMemo:
    """Per-session in-memory memo tier (one instance per ``tif1.core.Session``).

    Consolidates the session-scoped caches previously held as ~11 loose
    ``Session`` attributes behind one kind-based interface:

    - ``"json"``: path-keyed JSON payloads fetched during the session (only
      known payload paths or nested ``"DRIVER/..."`` paths are retained).
    - ``"telemetry_payload"``: ``(driver, lap)``-keyed raw telemetry payloads
      (non-empty dicts only).
    - ``"telemetry_df"``: ``(driver, lap)``-keyed materialized telemetry
      DataFrames.

    It also owns the fastest-lap-reference memo (tagged by laps/drivers source
    ids), the persistent-cache probe result (``has_session_data``), and
    per-driver telemetry failure tracking (counts, unavailable drivers, and
    suppressed-warning drivers).

    Not internally synchronized: the owning session's access patterns are
    effectively single-threaded per event loop.
    """

    def __init__(self) -> None:
        """Initialize empty memo state."""
        self._json_payloads: dict[str, dict[str, Any]] = {}
        self._telemetry_payloads: dict[tuple[str, int], dict[str, Any]] = {}
        self._telemetry_dfs: dict[tuple[str, int], Any] = {}
        # Pergamon Q1: ``(driver, lap)`` -> DriverFrameSource for frames a warm
        # load holds decoded but not yet built; a get() materializes the frame
        # (through ``frame_adopter`` when the owner set one) into
        # ``_telemetry_dfs`` and drops the pending entry.
        self._telemetry_sources: dict[tuple[str, int], Any] = {}
        # Drivers that had one lap built on its own from the payload tier; their
        # next single lap decodes the whole driver row instead.
        self.single_lap_drivers: set[str] = set()
        self.frame_adopter: Callable[[Any, str], Any] | None = None
        self.fastest_lap_ref: tuple[str, int] | None = None
        self.fastest_lap_ref_laps_source_id: int | None = None
        self.fastest_lap_ref_driver_source_id: int | None = None
        self.has_session_data: bool | None = None
        self._telemetry_failure_counts: dict[str, int] = {}
        self._telemetry_unavailable_drivers: set[str] = set()
        self._telemetry_failure_suppressed_drivers: set[str] = set()

    def _store(self, kind: SessionMemoKind) -> dict:
        if kind == "json":
            return self._json_payloads
        if kind == "telemetry_payload":
            return self._telemetry_payloads
        if kind == "telemetry_df":
            return self._telemetry_dfs
        raise ValueError(f"Unknown session memo kind: {kind!r}")

    def get(self, kind: SessionMemoKind, key: Any) -> Any | None:
        """Get a memoized value by kind and key (None when absent).

        A ``"telemetry_df"`` frame still held as a pending source is built
        on this first access and memoized like an eagerly built one.
        """
        if kind == "telemetry_df":
            frame = self._telemetry_dfs.get(key)
            if frame is None and self._telemetry_sources:
                return self._materialize_telemetry(key)
            return frame
        return self._store(kind).get(key)

    def contains(self, kind: SessionMemoKind, key: Any) -> bool:
        """Return True when kind/key is memoized (pending frames count)."""
        if kind == "telemetry_df" and key in self._telemetry_sources:
            return True
        return key in self._store(kind)

    def items(self, kind: SessionMemoKind):
        """Return the (key, value) items view for a kind."""
        if kind == "telemetry_df":
            self.materialize_telemetry()
        return self._store(kind).items()

    def kind_size(self, kind: SessionMemoKind) -> int:
        """Return the number of memoized entries for a kind (pending frames count)."""
        if kind == "telemetry_df":
            return len(self._telemetry_dfs) + len(self._telemetry_sources)
        return len(self._store(kind))

    def add_telemetry_source(self, driver: str, source: Any) -> list[tuple[str, int]]:
        """Hold ``source`` (a decoded driver blob) for every lap it stores.

        Laps already memoized as frames keep their frame. Returns the refs
        now served by the source.
        """
        frames = self._telemetry_dfs
        pending = self._telemetry_sources
        added: list[tuple[str, int]] = []
        for lap in source.laps:
            ref = (driver, lap)
            if ref not in frames:
                pending[ref] = source
                added.append(ref)
        return added

    def _materialize_telemetry(self, key: Any) -> Any | None:
        source = self._telemetry_sources.pop(key, None)
        if source is None:
            return None
        frame = source.frame(key[1])
        adopt = self.frame_adopter
        if adopt is not None:
            adopted = adopt(frame, key[0])
            if adopted is not None:
                frame = adopted
        self._telemetry_dfs[key] = frame
        return frame

    def materialize_telemetry(self) -> None:
        """Build every pending frame (``fetch_all_laps_telemetry`` and bulk readers)."""
        pending = self._telemetry_sources
        if not pending:
            return
        frames = self._telemetry_dfs
        adopt = self.frame_adopter
        for (driver, lap), source in pending.items():
            frame = source.frame(lap)
            if adopt is not None:
                adopted = adopt(frame, driver)
                if adopted is not None:
                    frame = adopted
            frames[(driver, lap)] = frame
        pending.clear()

    def set(self, kind: SessionMemoKind, key: Any, value: Any) -> bool:
        """Store a value under kind/key.

        Kind policies may reject values: ``"json"`` retains dicts at known
        payload paths or nested paths only, and ``"telemetry_payload"``
        retains non-empty dicts only. ``"telemetry_df"`` accepts any value.

        Returns:
            True when the value was stored, False when a kind policy rejected it.
        """
        if kind == "json":
            if not isinstance(value, dict):
                return False
            if key in _MEMO_KNOWN_JSON_PATHS or "/" in key:
                self._json_payloads[key] = value
                return True
            return False
        if kind == "telemetry_payload":
            if isinstance(value, dict) and value:
                self._telemetry_payloads[key] = value
                return True
            return False
        if kind == "telemetry_df":
            self._telemetry_dfs[key] = value
            if self._telemetry_sources:
                self._telemetry_sources.pop(key, None)
            return True
        raise ValueError(f"Unknown session memo kind: {kind!r}")

    def get_fastest_lap_ref_if_current(
        self, source_kind: FastestLapRefKind, source_id: int
    ) -> Any | tuple[str, int] | None:
        """Return the memoized fastest-lap ref when its source tag matches.

        Args:
            source_kind: "laps" to match against the loaded-laps source id,
                "drivers" to match against the drivers-payload source id.
            source_id: ``id()`` of the current source object.

        Returns:
            The memoized ``(driver, lap)`` ref (which is never None for a
            matching tag), or :data:`FASTEST_LAP_REF_MISS` on a source-id miss.
        """
        if source_kind == "laps":
            if self.fastest_lap_ref_laps_source_id == source_id:
                return self.fastest_lap_ref
        elif self.fastest_lap_ref_driver_source_id == source_id:
            return self.fastest_lap_ref
        return FASTEST_LAP_REF_MISS

    def set_fastest_lap_ref(
        self,
        ref: tuple[str, int] | None,
        *,
        source_kind: FastestLapRefKind,
        source_id: int | None,
    ) -> None:
        """Memoize the fastest-lap ref, tagged by the source it was derived from.

        Stores the source tag only when the ref is not None, and clears the
        other source kind's tag so a stale source can never match.
        """
        self.fastest_lap_ref = ref
        if source_kind == "laps":
            self.fastest_lap_ref_laps_source_id = source_id if ref is not None else None
            self.fastest_lap_ref_driver_source_id = None
        else:
            self.fastest_lap_ref_driver_source_id = source_id if ref is not None else None
            self.fastest_lap_ref_laps_source_id = None

    def record_telemetry_failure(self, driver: str) -> int:
        """Increment and return the per-driver telemetry failure count.

        Drivers reaching 3 failures are marked telemetry-unavailable so
        subsequent fetches for them can be short-circuited.
        """
        count = self._telemetry_failure_counts.get(driver, 0) + 1
        self._telemetry_failure_counts[driver] = count
        if count >= 3:
            self._telemetry_unavailable_drivers.add(driver)
        return count

    def telemetry_failure_count(self, driver: str) -> int:
        """Return the number of recorded telemetry failures for a driver."""
        return self._telemetry_failure_counts.get(driver, 0)

    def is_telemetry_unavailable(self, driver: str) -> bool:
        """Return True when telemetry fetches should be short-circuited for a driver."""
        return driver in self._telemetry_unavailable_drivers

    def is_failure_suppressed(self, driver: str) -> bool:
        """Return True when further failure warnings for a driver were already suppressed."""
        return driver in self._telemetry_failure_suppressed_drivers

    def suppress_failure_warnings(self, driver: str) -> None:
        """Mark a driver's further telemetry failure warnings as suppressed."""
        self._telemetry_failure_suppressed_drivers.add(driver)

    def clear(self) -> None:
        """Drop all memoized state (payloads, fastest-lap ref, probe result, failures)."""
        self._json_payloads.clear()
        self._telemetry_payloads.clear()
        self._telemetry_dfs.clear()
        self._telemetry_sources.clear()
        self.fastest_lap_ref = None
        self.fastest_lap_ref_laps_source_id = None
        self.fastest_lap_ref_driver_source_id = None
        self.has_session_data = None
        self._telemetry_failure_counts.clear()
        self._telemetry_unavailable_drivers.clear()
        self._telemetry_failure_suppressed_drivers.clear()


class Cache:
    """Cache with SQLite backend, in-memory LRU tiers, and async support.

    Both in-memory tiers are :class:`LRUCache` instances. Reads are lock-free
    on the hot path (``LRUCache.get(..., ordered=False)``); the SQLite tier is
    consulted only on memory misses.

    The unified interface is :meth:`get_entry` / :meth:`set_entry` /
    :meth:`invalidate`; ``get``/``set``/``get_telemetry``/``set_telemetry``
    are thin wrappers over it.
    """

    def __init__(self, cache_dir: Path | None = None):
        """Initialize cache with optional custom directory.

        Args:
            cache_dir: Cache directory path
        """
        if cache_dir is None:
            from .config import _default_cache_dir, get_config

            config = get_config()
            configured_path = config.get("cache_dir")
            # Never stringify a missing value: Path(str(None)) is a literal
            # "./None" cache directory (stub configs in tests returned None).
            cache_dir = (
                Path(str(configured_path)).expanduser() if configured_path else _default_cache_dir()
            )

        self.cache_dir = cache_dir
        # mode=0o700 matters on Windows too: chmod only toggles the read-only
        # attribute there, while a new directory created with this mode gets
        # a current-user-only ACL (CPython 3.12.4+); the frame tiers are
        # unpickled from this directory. An unusable path (invalid characters,
        # reserved names, no permission) degrades to an uncached session.
        cache_dir_usable = True
        try:
            self.cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as exc:
            cache_dir_usable = False
            logger.warning(
                f"Cache directory {self.cache_dir} is unusable ({exc}); running uncached"
            )
        if cache_dir_usable:
            try:
                # Harden permissions to 0o700 (user read/write/execute only)
                self.cache_dir.chmod(0o700)
            except OSError:
                logger.debug(f"Failed to set restricted permissions on {self.cache_dir}")

        self.conn = None
        self.read_only = False
        self._pending_writes = 0
        self._memory_cache_lock = threading.Lock()  # Lock for memory cache operations
        self._sqlite_lock = threading.Lock()  # Lock for SQLite operations

        # Load config values for cache constants
        from .config import get_config

        config = get_config()
        self._commit_interval = config.get("cache_commit_interval", 100)
        self._sqlite_timeout = config.get("sqlite_timeout", 30.0)
        self._memory_cache_max_items = config.get("memory_cache_max_items", 1024)
        self._memory_telemetry_cache_max_items = config.get(
            "memory_telemetry_cache_max_items", 2048
        )
        self._parsed_cache_max_items = _PARSED_CACHE_MAX_ITEMS
        self._parsed_telemetry_cache_max_items = _PARSED_TELEMETRY_CACHE_MAX_ITEMS
        self._missing_payloads_ttl_days = config.get("missing_payloads_ttl_days", 7.0)

        # Negative-result tier: payloads that returned 4xx on every CDN source
        # (all-missing verdicts). Lazily loaded from SQLite; key -> recorded_at.
        self._missing_payloads: dict[str, float] | None = None

        # Shared write lock for both memory tiers (serializes updates exactly
        # like the previous single OrderedDict lock). Kept separate from the
        # public _memory_cache_lock so compound operations can hold the latter
        # while mutating tiers without deadlocking on the non-reentrant locks.
        self._lru_lock = threading.Lock()
        self._memory_cache: LRUCache = LRUCache(
            maxsize=self._memory_cache_max_items, lock=self._lru_lock
        )
        self._memory_telemetry_cache: LRUCache = LRUCache(
            maxsize=self._memory_telemetry_cache_max_items, lock=self._lru_lock
        )
        self._parsed_cache: LRUCache = LRUCache(
            maxsize=self._parsed_cache_max_items, lock=self._lru_lock
        )
        self._parsed_telemetry_cache: LRUCache = LRUCache(
            maxsize=self._parsed_telemetry_cache_max_items, lock=self._lru_lock
        )

        self._init_sqlite()

    def _init_sqlite(self):
        """Initialize SQLite backend with WAL mode for better concurrency."""
        self.db_path = self.cache_dir / "cache.sqlite"
        conn = None
        try:
            conn = sqlite3.connect(
                str(self.db_path), check_same_thread=False, timeout=self._sqlite_timeout
            )

            # Enable WAL mode for better concurrency. Emscripten's in-memory
            # file system has no shared-memory mmap for the WAL index, so the
            # browser build keeps the rollback journal in memory instead.
            conn.execute(
                "PRAGMA journal_mode=MEMORY" if IS_EMSCRIPTEN else "PRAGMA journal_mode=WAL"
            )
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            if not IS_EMSCRIPTEN and sys.maxsize > 2**32:
                # Frame rows are multi-MB blobs over 4 KB overflow pages: reading
                # them through the memory map instead of one read() per page
                # takes ~25% off a warm session read (33 -> 25 ms for 83 MB).
                conn.execute(f"PRAGMA mmap_size={_SQLITE_MMAP_BYTES}")

            cursor = conn.execute("PRAGMA user_version")
            version_row = cursor.fetchone() if cursor is not None else None
            stored_version = int(version_row[0]) if version_row else 0
            if stored_version < _FRAME_TIER_SCHEMA_VERSION:
                # Derived tier only: dropping it costs one rebuild, never a fetch.
                conn.execute("DROP TABLE IF EXISTS telemetry_frames")
                conn.execute("DROP TABLE IF EXISTS telemetry_frames_pl")
                conn.execute(f"DROP TABLE IF EXISTS {_POLARS_IPC_TABLE}")
                conn.execute(f"DROP TABLE IF EXISTS {_PANDAS_DRIVER_TABLE}")
                for laps_table in _LAPS_FRAME_TABLES.values():
                    conn.execute(f"DROP TABLE IF EXISTS {laps_table}")
                conn.execute(f"PRAGMA user_version = {_FRAME_TIER_SCHEMA_VERSION}")

            conn.execute("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, data TEXT)")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS missing_payloads ("
                "key TEXT PRIMARY KEY, recorded_at REAL)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry_frames (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    driver TEXT,
                    lap INTEGER,
                    frame BLOB,
                    PRIMARY KEY (year, gp, session, driver, lap)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry_frames_pl (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    driver TEXT,
                    lap INTEGER,
                    frame BLOB,
                    PRIMARY KEY (year, gp, session, driver, lap)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_POLARS_IPC_TABLE} (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    driver TEXT,
                    laps TEXT,
                    frame BLOB,
                    PRIMARY KEY (year, gp, session, driver)
                )
                """
            )
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_PANDAS_DRIVER_TABLE} (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    driver TEXT,
                    laps TEXT,
                    frame BLOB,
                    PRIMARY KEY (year, gp, session, driver)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS laps_frames (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    frame BLOB,
                    PRIMARY KEY (year, gp, session)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS laps_frames_pl (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    frame BLOB,
                    PRIMARY KEY (year, gp, session)
                )
                """
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_cache (
                    year INTEGER,
                    gp TEXT,
                    session TEXT,
                    driver TEXT,
                    lap INTEGER,
                    data TEXT,
                    PRIMARY KEY (year, gp, session, driver, lap)
                )
            """)
            conn.commit()

            # Only assign to self.conn after full success
            self.conn = conn
            self._pending_writes = 0
            logger.debug(f"SQLite cache initialized at {self.db_path} with WAL mode")
        except (OSError, sqlite3.Error, TypeError, ValueError) as e:
            # Cleanup on failure
            if conn is not None:
                try:
                    conn.close()
                except (OSError, sqlite3.Error):
                    pass
            self.conn = None
            logger.warning(f"SQLite cache unavailable: {e}")

    def _commit_if_needed(self, force: bool = False) -> None:
        """Commit batched writes periodically to reduce fsync overhead."""
        if self.conn is None:
            return
        if force or self._pending_writes >= self._commit_interval:
            self.conn.commit()
            self._pending_writes = 0

    def flush(self) -> None:
        """Commit every pending write now (batched writes commit every ``cache_commit_interval``).

        ``close()`` commits too; ``flush()`` keeps the connection open, for
        callers that copy the cache file elsewhere (``tif1.browser.persist_cache``).
        """
        _join_frame_writers()
        with self._sqlite_lock:
            self._commit_if_needed(force=True)

    def _get_from_memory(self, key: str) -> Any | None:
        """Get cache entry from in-memory LRU only (no SQLite access).

        Uses truly lock-free reads for maximum concurrency. OrderedDict.get() is
        atomic in CPython due to the GIL. No LRU updates on reads to avoid lock
        contention. LRU ordering is maintained only through writes.

        Args:
            key: Cache key to lookup

        Returns:
            Cached data or None if not found
        """
        if self.conn is None:
            return None
        try:
            # Parsed-object front tier: a repeat hit skips orjson entirely.
            parsed = self._parsed_cache.get(key, ordered=False)
            if parsed is not None:
                return parsed

            # Completely lock-free read - no LRU update
            json_data = self._memory_cache.get(key, ordered=False)

            if json_data is not None:
                parsed = json_loads(json_data)
                self._parsed_cache.set(key, parsed)
                return parsed

            return None
        except (RuntimeError, TypeError, ValueError) as e:
            logger.debug("Memory cache read error for %s: %s", key, e)
            return None

    def get_entry(self, kind: CacheKind, key: Any) -> Any | None:
        """Unified kind-based cache read (thread-safe).

        Args:
            kind: "json" for path-keyed session JSON payloads, "telemetry" for
                per-lap telemetry payloads.
            key: Cache key — a ``str`` for "json" or a
                ``(year, gp, session, driver, lap)`` tuple for "telemetry".

        Returns:
            Cached data or None if not found
        """
        if kind == "json":
            return self._get_json(key)
        if kind == "telemetry":
            year, gp, session, driver, lap = key
            return self._get_telemetry(year, gp, session, driver, lap)
        raise ValueError(f"Unknown cache kind: {kind!r}")

    def set_entry(self, kind: CacheKind, key: Any, value: Any) -> None:
        """Unified kind-based cache write (thread-safe).

        Args:
            kind: "json" or "telemetry" (see :meth:`get_entry`).
            key: Cache key (see :meth:`get_entry`).
            value: Value to cache (must be JSON-serializable).
        """
        if kind == "json":
            self._set_json(key, value)
            return
        if kind == "telemetry":
            year, gp, session, driver, lap = key
            self._set_telemetry(year, gp, session, driver, lap, value)
            return
        raise ValueError(f"Unknown cache kind: {kind!r}")

    # -- negative-result tier -----------------------------------------------

    def _load_missing_payloads(self) -> dict[str, float]:
        """Load the all-4xx verdicts once per process (TTL-filtered)."""
        if self._missing_payloads is not None:
            return self._missing_payloads
        missing: dict[str, float] = {}
        if self.conn is not None:
            try:
                cutoff = time.time() - self._missing_payloads_ttl_days * 86400.0
                with self._sqlite_lock:
                    rows = self.conn.execute(
                        "SELECT key, recorded_at FROM missing_payloads WHERE recorded_at > ?",
                        (cutoff,),
                    ).fetchall()
                missing = dict(rows)
            except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
                logger.debug("Missing-payloads load failed: %s", e)
        self._missing_payloads = missing
        return missing

    def is_known_missing(self, key: str) -> bool:
        """Return True when ``key`` was refused with 4xx by every CDN recently.

        The verdict is TTL-scoped (``missing_payloads_ttl_days``, default 7
        days): after that the payload is re-probed on the next request.
        """
        if self.conn is None:
            return False
        recorded = self._load_missing_payloads().get(key)
        if recorded is None:
            return False
        if time.time() - recorded > self._missing_payloads_ttl_days * 86400.0:
            # Expired verdict: drop it so the next request re-probes.
            self.discard_missing(key)
            return False
        return True

    def record_missing(self, key: str) -> None:
        """Persist an everywhere-missing (all-CDN 4xx) verdict for ``key``."""
        if self.conn is None or self.read_only:
            return
        now = time.time()
        try:
            missing = self._load_missing_payloads()
            missing[key] = now
            with self._sqlite_lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO missing_payloads VALUES (?, ?)", (key, now)
                )
                self._pending_writes += 1
                self._commit_if_needed()
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Missing-payload record skipped: %s", e)

    def discard_missing(self, key: str) -> None:
        """Drop a missing-verdict (the payload exists again or was invalidated)."""
        missing = self._missing_payloads
        if missing is None:
            # Not loaded yet: load once so the membership guard below can
            # skip the DELETE for the overwhelmingly common no-verdict case
            # (every successful fetch calls this; the SQL write must not run).
            missing = self._load_missing_payloads()
        if key not in missing:
            return
        missing.pop(key, None)
        if self.conn is None or self.read_only:
            return
        try:
            with self._sqlite_lock:
                self.conn.execute("DELETE FROM missing_payloads WHERE key = ?", (key,))
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Missing-payload discard skipped: %s", e)

    def invalidate(self, scope: InvalidateScope = "all") -> None:
        """Invalidate cached data by scope.

        Args:
            scope: "all" clears every tier (memory + SQLite, JSON and
                telemetry, equivalent to :meth:`clear`); "memory" clears only
                the in-memory LRU tiers (SQLite persists); "json" clears the
                JSON payload tier (memory + SQLite); "telemetry" clears the
                telemetry tier (memory + SQLite).
        """
        # Ithome U5: a pending background frame write must land (or be
        # dropped) before rows are wiped, otherwise it would re-insert
        # cleared rows afterwards.
        _join_frame_writers()
        if scope == "all":
            self.clear()
            return
        if scope == "memory":
            with self._memory_cache_lock:
                self._memory_cache.clear()
                self._memory_telemetry_cache.clear()
                self._parsed_cache.clear()
                self._parsed_telemetry_cache.clear()
                # The missing-verdict map mirrors SQLite; drop the mirror so
                # the next probe reloads it (the rows themselves persist).
                self._missing_payloads = None
            logger.info("Memory cache tiers invalidated")
            return
        if scope == "ergast":
            self._invalidate_ergast()
            return
        if scope not in ("json", "telemetry"):
            raise ValueError(f"Unknown invalidate scope: {scope!r}")
        if self.conn is None or self.read_only:
            logger.warning("Cannot invalidate cache (no connection or read-only mode)")
            return

        table = "cache" if scope == "json" else "telemetry_cache"
        memory_tier = self._memory_cache if scope == "json" else self._memory_telemetry_cache
        parsed_tier = self._parsed_cache if scope == "json" else self._parsed_telemetry_cache
        with self._sqlite_lock:
            self.conn.execute(f"DELETE FROM {table}")
            # Missing-verdict keys are JSON-tier-shaped even for telemetry
            # paths, so dropping either tier must also drop its verdicts.
            self.conn.execute("DELETE FROM missing_payloads")
            # Laps frames are derived from the laps payloads of either tier.
            for laps_table in _LAPS_FRAME_TABLES.values():
                self.conn.execute(f"DELETE FROM {laps_table}")
            if scope == "telemetry":
                # Frames are derived from telemetry payloads (both backends).
                for frame_table in _ALL_FRAME_TABLES:
                    self.conn.execute(f"DELETE FROM {frame_table}")
            self.conn.commit()
            self._pending_writes = 0
        with self._memory_cache_lock:
            memory_tier.clear()
            parsed_tier.clear()
            if self._missing_payloads is not None:
                self._missing_payloads.clear()
        logger.info("Cache tier invalidated: %s", scope)

    def _invalidate_ergast(self) -> None:
        """Drop the Ergast tier: Stats repository files and Jolpica answers (memory + SQLite)."""
        from .ergast.payloads import JOLPICA_CACHE_PREFIX, STATS_CACHE_PREFIX, clear_memo

        clear_memo()
        if self.conn is not None and not self.read_only:
            with self._sqlite_lock:
                self.conn.execute(
                    "DELETE FROM cache WHERE key LIKE ? OR key LIKE ?",
                    (f"{STATS_CACHE_PREFIX}%", f"{JOLPICA_CACHE_PREFIX}%"),
                )
                self.conn.commit()
                self._pending_writes = 0
        with self._memory_cache_lock:
            for tier in (self._memory_cache, self._parsed_cache):
                ergast_keys = [
                    key
                    for key in list(tier)
                    if isinstance(key, str)
                    and key.startswith((STATS_CACHE_PREFIX, JOLPICA_CACHE_PREFIX))
                ]
                for key in ergast_keys:
                    tier.pop(key, None)
        logger.info("Cache tier invalidated: ergast")

    def _get_json(self, key: str) -> Any | None:
        """Get cached JSON payload (thread-safe); core of ``get``."""
        if self.conn is None:
            return None
        try:
            # Try lock-free memory read first
            result = self._get_from_memory(key)
            if result is not None:
                logger.debug("Cache hit (memory): %s", key)
                return result

            # Memory cache miss - check SQLite with lock
            json_data = None
            with self._sqlite_lock:
                result = self.conn.execute(
                    "SELECT data FROM cache WHERE key = ?", (key,)
                ).fetchone()
                if result:
                    json_data = _decode_sqlite_value(result[0])

            # Update memory cache outside SQLite lock
            if json_data is not None:
                self._memory_cache.set(key, json_data)
                parsed = json_loads(json_data)
                self._parsed_cache.set(key, parsed)
                logger.debug("Cache hit (SQLite): %s", key)
                return parsed

            logger.debug("Cache miss: %s", key)
            return None
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Cache read error for %s: %s", key, e)
            return None

    def get(self, key: str) -> Any | None:
        """Get cached data (thread-safe).

        Uses lock-free memory cache reads for performance. Only acquires lock
        for SQLite access and LRU updates.
        Thin wrapper over ``get_entry("json", key)``.

        Args:
            key: Cache key to lookup

        Returns:
            Cached data or None if not found
        """
        return self.get_entry("json", key)

    async def get_async(self, key: str) -> Any | None:
        """Get cached data asynchronously."""
        import asyncio  # lazy: only async/network paths need it

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.get, key)

    def _store_json_blob(self, key: str, blob: bytes) -> None:
        """Write a serialized JSON blob to both tiers (thread-safe); core of ``set``.

        The memory tier keeps the plain blob; the SQLite tier stores it
        compressed when large (see :func:`_encode_sqlite_value`).
        """
        if self.conn is None or self.read_only:
            return
        try:
            # Update memory cache first (fast operation, <1ms)
            self._memory_cache.set(key, blob)
            # The payload exists: drop any stale everywhere-missing verdict.
            # The verdict map loads once per process (one SELECT) and the
            # SQLite row is deleted only when a verdict was present, so the
            # hot path pays a dict pop and nothing more.
            stale_verdict = self._load_missing_payloads().pop(key, None) is not None

            # Then update SQLite (slower operation)
            with self._sqlite_lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO cache VALUES (?, ?)", (key, _encode_sqlite_value(blob))
                )
                if stale_verdict:
                    self.conn.execute("DELETE FROM missing_payloads WHERE key = ?", (key,))
                self._pending_writes += 1
                self._commit_if_needed()

            logger.debug("Cached: %s", key)
        except (RuntimeError, TypeError, ValueError, sqlite3.Error):
            logger.debug("Cache write skipped: %s", key)

    def _set_json(self, key: str, data: Any) -> None:
        """Set cached JSON payload (thread-safe); core of ``set``.

        The parsed-object tier is read-through: writes drop any parsed entry
        instead of populating it, so stale parsed objects can never survive.
        """
        self._store_json_blob(key, json_dumps_bytes(data))
        self._parsed_cache.pop(key, None)

    def set(self, key: str, data: Any) -> None:
        """Set cached data (thread-safe).

        Optimized to update memory cache first (fast operation, <1ms) before SQLite (slower).
        This ensures minimal lock duration for memory operations.
        Thin wrapper over ``set_entry("json", key, data)``.
        """
        self.set_entry("json", key, data)

    def set_raw(self, key: str, blob: bytes | bytearray | memoryview) -> None:
        """Set cached data from an already-serialized JSON blob (thread-safe).

        Skips re-serialization when the caller still holds the original payload
        bytes (e.g. the async fetch pipeline with validation disabled). The
        SQLite tier compresses large blobs exactly like :meth:`set`. The
        parsed-object entry (if any) is dropped; reads repopulate it.
        """
        self._store_json_blob(key, bytes(blob))
        self._parsed_cache.pop(key, None)

    async def set_async(self, key: str, data: Any) -> None:
        """Set cached data asynchronously."""
        import asyncio  # lazy: only async/network paths need it

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.set, key, data)

    def _get_telemetry(self, year: int, gp: str, session: str, driver: str, lap: int) -> Any | None:
        """Get cached telemetry data (thread-safe); core of ``get_telemetry``."""
        if self.conn is None:
            return None
        try:
            cache_key = (year, gp, session, driver, lap)

            # Parsed-object front tier: a repeat hit skips orjson entirely.
            parsed = self._parsed_telemetry_cache.get(cache_key, ordered=False)
            if parsed is not None:
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                return parsed

            # Completely lock-free memory cache read - no LRU update
            json_data = self._memory_telemetry_cache.get(cache_key, ordered=False)

            if json_data is not None:
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                parsed = decode_telemetry_payload(json_data)
                self._parsed_telemetry_cache.set(cache_key, parsed)
                return parsed

            # Memory cache miss - check SQLite with lock
            with self._sqlite_lock:
                result = self.conn.execute(
                    "SELECT data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? AND driver = ? AND lap = ?",
                    cache_key,
                ).fetchone()
                if result:
                    json_data = _decode_sqlite_value(result[0])

            # Update memory cache outside SQLite lock
            if json_data is not None:
                self._memory_telemetry_cache.set(cache_key, json_data)
                parsed = decode_telemetry_payload(json_data)
                self._parsed_telemetry_cache.set(cache_key, parsed)
                logger.debug("Telemetry cache hit: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
                return parsed

            return None
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Telemetry cache read error: %s", e)
            return None

    def get_telemetry(self, year: int, gp: str, session: str, driver: str, lap: int) -> Any | None:
        """Get cached telemetry data (thread-safe).

        Uses lock-free memory cache reads for performance. Only acquires lock
        for SQLite access and LRU updates.
        Thin wrapper over ``get_entry("telemetry", (year, gp, session, driver, lap))``.

        Args:
            year: Season year
            gp: Grand Prix identifier
            session: Session type
            driver: Driver code
            lap: Lap number

        Returns:
            Cached telemetry data or None if not found
        """
        return self.get_entry("telemetry", (year, gp, session, driver, lap))

    async def get_telemetry_async(
        self, year: int, gp: str, session: str, driver: str, lap: int
    ) -> Any | None:
        """Get cached telemetry data asynchronously."""
        import asyncio  # lazy: only async/network paths need it

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.get_telemetry, year, gp, session, driver, lap)

    def get_telemetry_batch(
        self, year: int, gp: str, session: str, driver_laps: list[tuple[str, int]]
    ) -> dict[tuple[str, int], Any]:
        """Get multiple cached telemetry entries in a single batch (thread-safe)."""
        with _suspend_gc():
            return {
                (driver, lap): parsed
                for driver, lap, parsed in self.iter_telemetry_batch(year, gp, session, driver_laps)
            }

    def iter_telemetry_batch(
        self, year: int, gp: str, session: str, driver_laps: list[tuple[str, int]]
    ) -> Iterator[tuple[str, int, Any]]:
        """Yield cached telemetry payloads as ``(driver, lap, parsed)`` (thread-safe).

        Memory-tier hits come first, then the SQLite rows of the remaining
        refs in decode-completion order: the rows are decompressed and parsed
        by worker threads (Pergamon Q15), so a caller that builds frames while
        iterating overlaps its (GIL-bound) work with the decoding. Refs
        without a cached payload are simply not yielded; a read error ends
        the iteration early (logged), like a partial batch.
        """
        if self.conn is None or not driver_laps:
            return

        misses = []

        # 1. Try memory cache first (lock-free), parsed tier before blob tier
        for driver, lap in driver_laps:
            key = (year, gp, session, driver, lap)
            parsed = self._parsed_telemetry_cache.get(key, ordered=False)
            if parsed is not None:
                yield driver, lap, parsed
                continue
            json_data = self._memory_telemetry_cache.get(key, ordered=False)
            if json_data:
                parsed = decode_telemetry_payload(json_data)
                self._parsed_telemetry_cache.set(key, parsed)
                yield driver, lap, parsed
            else:
                misses.append((driver, lap))

        if not misses:
            return

        # 2. Check SQLite for misses using IN clause for batch lookup
        try:
            with self._sqlite_lock:
                placeholders = ", ".join(["(?, ?)" for _ in misses])
                params = [year, gp, session]
                for driver_code, lap_num in misses:
                    params.extend([driver_code, lap_num])
                query = f"SELECT driver, lap, data FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? AND (driver, lap) IN ({placeholders})"
                rows = self.conn.execute(query, params).fetchall()

            for chunk in _decode_payload_rows(rows) if rows else ():
                for driver_code, lap_num, decoded, parsed in chunk:
                    key = (year, gp, session, driver_code, lap_num)
                    self._memory_telemetry_cache.set(key, decoded)
                    self._parsed_telemetry_cache.set(key, parsed)
                    yield driver_code, lap_num, parsed
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Telemetry batch cache read error: %s", e)

    async def get_telemetry_batch_async(
        self, year: int, gp: str, session: str, driver_laps: list[tuple[str, int]]
    ) -> dict[tuple[str, int], Any]:
        """Get multiple cached telemetry entries asynchronously in a single batch."""
        import asyncio  # lazy: only async/network paths need it

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self.get_telemetry_batch, year, gp, session, driver_laps
        )

    # -- materialized telemetry-frame tier ------------------------------------
    #
    # Warm-load fast path: assembled per-(driver, lap) pandas DataFrames,
    # pickled and zstd-compressed. The payload tier stays the source of
    # truth; frames are derived data written on warm loads only, so cold
    # starts pay nothing for this tier. Unpickling trusts the same 0o700
    # user-local cache directory every other tier already trusts (a
    # corrupted or foreign blob degrades to a tier miss, never a crash).

    def get_telemetry_frames_batch(
        self,
        year: int,
        gp: str,
        session: str,
        driver_laps: list[tuple[str, int]],
        lib: Literal["pandas", "polars"] = "pandas",
        whole_driver: bool = False,
        frame_class: Any = None,
    ) -> dict[tuple[str, int], Any]:
        """Batch-read materialized telemetry frames.

        Args:
            lib: Backend whose frame table to read (``pandas`` or ``polars``);
                rows from the other backend are never returned because a
                foreign frame object would surface in caller maps.
            whole_driver: pandas only — also return every other lap stored
                in a driver row the batch touches (the row is decoded anyway;
                callers memoize the extras so later laps of that driver are
                free).
            frame_class: pandas only — DataFrame subclass to build (e.g.
                ``Telemetry``) instead of wrapping afterwards.

        Returns:
            Mapping of ``(driver, lap)`` to the unpickled DataFrame; refs with
            no (or corrupt) frame rows are absent and fall through to the
            payload tier.
        """
        if self.conn is None or not driver_laps or not _FRAME_CODEC_AVAILABLE:
            return {}
        table = _FRAME_TABLES.get(lib, "telemetry_frames")
        results: dict[tuple[str, int], Any] = {}
        if lib == "polars":
            results = self._get_polars_ipc_frames(year, gp, session, driver_laps)
            driver_laps = [ref for ref in driver_laps if ref not in results]
            if not driver_laps:
                return results
        else:
            results = self._get_driver_frames(
                year, gp, session, driver_laps, whole_driver=whole_driver, frame_class=frame_class
            )
            driver_laps = [ref for ref in driver_laps if ref not in results]
            if not driver_laps:
                return results
        legacy_refs = set(driver_laps)
        try:
            with self._sqlite_lock:
                # Whole-session batches (fetch_all_laps_telemetry) read far
                # more cheaply as one range scan with a Python-side membership
                # filter: the (driver, lap) IN (...) row-value list costs
                # SQLite a transient b-tree per execute (measured 46 vs 19 ms
                # interleaved on the Monaco 1452-frame batch). Partial
                # batches keep the tight IN query so they never drag the
                # whole session partition's blobs through the scan.
                if len(driver_laps) >= 128:
                    count = self.conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE year = ? AND gp = ? AND session = ?",
                        (year, gp, session),
                    ).fetchone()
                    session_frames = count[0] if count else 0
                else:
                    session_frames = 0
                if session_frames and len(driver_laps) * 2 >= session_frames:
                    ref_set = set(driver_laps)
                    rows = [
                        r
                        for r in self.conn.execute(
                            f"SELECT driver, lap, frame FROM {table} "
                            "WHERE year = ? AND gp = ? AND session = ?",
                            (year, gp, session),
                        ).fetchall()
                        if (r[0], r[1]) in ref_set
                    ]
                else:
                    placeholders = ", ".join(["(?, ?)"] * len(driver_laps))
                    params: list[Any] = [year, gp, session]
                    for driver_code, lap_num in driver_laps:
                        params.extend([driver_code, lap_num])
                    query = (
                        f"SELECT driver, lap, frame FROM {table} WHERE year = ? AND gp = ? "
                        f"AND session = ? AND (driver, lap) IN ({placeholders})"
                    )
                    rows = self.conn.execute(query, params).fetchall()
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Telemetry frames batch read error: %s", e)
            return results
        # ~100 MB of short-lived allocations; GC suspension halves this loop
        # (Rhodes R3) and pickle data holds no reference cycles.
        _foreign_marker = "_df" if lib == "polars" else "_mgr"
        frame_errors: tuple[type[BaseException], ...] = (
            AttributeError,
            EOFError,
            ImportError,
            IndexError,
            TypeError,
            ValueError,
            pickle.UnpicklingError,
            *_ZSTD_ERRORS,
        )
        if lib == "polars":
            # polars raises its own ComputeError (a PolarsError, not a
            # ValueError) for a frame state it cannot deserialize, e.g. one
            # written by a polars release with an incompatible format.
            from polars.exceptions import PolarsError

            frame_errors = (*frame_errors, PolarsError)
        if lib == "pandas":
            from .core_utils.frame_codec import FRAME_CODEC_MAGIC, decode_frame
        else:
            FRAME_CODEC_MAGIC, decode_frame = b"", None
        with _suspend_gc():
            for chunk in _decompress_frame_rows(rows):
                for driver_code, lap_num, raw in chunk:
                    try:
                        # Thebes TH1: typed-columnar rows rebuild through
                        # direct block assembly; rows written by earlier
                        # releases are still pickles.
                        if decode_frame is not None and bytes(raw[:4]) == FRAME_CODEC_MAGIC:
                            frame = decode_frame(raw, frame_class)
                        else:
                            frame = pickle.loads(raw)
                        # A foreign-backend blob (hand-migrated cache) degrades
                        # to a tier miss instead of surfacing in caller maps.
                        if not hasattr(frame, _foreign_marker):
                            raise TypeError("foreign frame object")
                        results[(driver_code, lap_num)] = frame
                    except frame_errors:
                        logger.debug(
                            "Corrupt telemetry frame row skipped: %s/%s", driver_code, lap_num
                        )
        if lib == "pandas" and not self.read_only:
            legacy_hits = [
                (driver_code, lap_num, frame)
                for (driver_code, lap_num), frame in results.items()
                if (driver_code, lap_num) in legacy_refs
            ]
            if len(legacy_hits) >= _LEGACY_UPGRADE_MIN_FRAMES:
                # Re-materialize per driver in the background so the next
                # load reads driver rows; the legacy rows are deleted once
                # the driver rows are in (see _set_driver_frames).
                def _upgrade_legacy_rows() -> None:
                    # Let the caller finish its critical path first: the
                    # encode is GIL-bound and would otherwise interleave with
                    # the remaining frame work of the load that found the
                    # legacy rows (measured +60 ms on the 1452-frame batch).
                    if THREADS_AVAILABLE:  # inline (no threads): nothing to yield to
                        time.sleep(_LEGACY_UPGRADE_DELAY_S)
                    self.set_telemetry_frames_batch(year, gp, session, legacy_hits, lib="pandas")

                register_frame_writer(_upgrade_legacy_rows)
        return results

    def set_telemetry_frames_batch(
        self,
        year: int,
        gp: str,
        session: str,
        frames: list[tuple[str, int, Any]],
        lib: Literal["pandas", "polars"] = "pandas",
    ) -> int:
        """Materialize assembled telemetry frames in one bulk write.

        Args:
            frames: ``(driver, lap, DataFrame)`` tuples to persist.
            lib: Backend whose frame table to write (``pandas`` or ``polars``).

        Returns:
            Number of frames written (0 when the tier is unavailable).
        """
        if self.conn is None or self.read_only or not _FRAME_CODEC_AVAILABLE or not frames:
            return 0
        table = _FRAME_TABLES.get(lib, "telemetry_frames")
        written = 0
        if lib == "polars":
            frames, written = self._set_polars_ipc_frames(year, gp, session, frames)
            if not frames:
                return written
        else:
            frames, written = self._set_driver_frames(year, gp, session, frames)
            if not frames:
                return written
        if lib == "pandas":
            from .core_utils.frame_codec import encode_frame
        else:
            encode_frame = None

        def _serialize(frame: Any) -> bytes:
            # Thebes TH1: typed-columnar encoding when every block is a known
            # kind (the telemetry contract's dtypes); pickle otherwise.
            if encode_frame is not None:
                encoded = encode_frame(frame)
                if encoded is not None:
                    return encoded
            return pickle.dumps(frame, protocol=5)

        try:
            with self._sqlite_lock:
                # executemany batches the 1400+ per-session frame rows into
                # one C-level loop (measured ~0.2 s vs ~0.36 s per-row execute
                # on the Monaco 1452-frame set). Serialization inside
                # _suspend_gc: the pickle+zstd burst allocates heavily and GC
                # pauses cost ~25% of this pass (Rhodes R3).
                with _suspend_gc():
                    self.conn.executemany(
                        f"INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            (
                                year,
                                gp,
                                session,
                                driver,
                                lap,
                                _compress_frame_blob(_serialize(frame)),
                            )
                            for driver, lap, frame in frames
                        ),
                    )
                # Per-frame rows add to the per-driver (or IPC) rows written above.
                written += len(frames)
                self._pending_writes += len(frames)
                self._commit_if_needed()
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Telemetry frames write skipped: %s", e)
            return 0
        return written

    # -- pandas per-driver typed-columnar tier (Pergamon P3) -----------------

    def _get_driver_frames(
        self,
        year: int,
        gp: str,
        session: str,
        driver_laps: list[tuple[str, int]],
        whole_driver: bool = False,
        frame_class: Any = None,
    ) -> dict[tuple[str, int], Any]:
        """Serve pandas frames from the per-driver rows (missing refs are absent)."""
        wanted: dict[str, set[int]] = {}
        for driver, lap in driver_laps:
            wanted.setdefault(driver, set()).add(int(lap))
        results: dict[tuple[str, int], Any] = {}
        conn = self.conn
        if conn is None or not wanted:
            return results
        try:
            placeholders = ", ".join("?" for _ in wanted)
            with self._sqlite_lock:
                # The lap list is read first (no blob): a driver row that
                # holds none of the wanted laps is not transferred or
                # decompressed (~1 MB and ~1.5 ms per row).
                useful = list(wanted)
                if not whole_driver:
                    stored = conn.execute(
                        f"SELECT driver, laps FROM {_PANDAS_DRIVER_TABLE} WHERE year = ? "
                        f"AND gp = ? AND session = ? AND driver IN ({placeholders})",
                        (year, gp, session, *wanted),
                    ).fetchall()
                    useful = [
                        driver
                        for driver, laps_json in stored
                        if wanted[driver].intersection(json_loads(laps_json))
                    ]
                    if not useful:
                        return results
                placeholders = ", ".join("?" for _ in useful)
                # Pergamon Q11: each row starts decompressing as SQLite
                # hands it over (the 22-row Monaco read spends ~12 ms
                # assembling blobs from overflow pages; the lz4 work of the
                # rows already fetched runs under it).
                pending = _submit_frame_rows(
                    conn.execute(
                        f"SELECT driver, frame FROM {_PANDAS_DRIVER_TABLE} WHERE year = ? "
                        f"AND gp = ? AND session = ? AND driver IN ({placeholders})",
                        (year, gp, session, *useful),
                    )
                )
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Driver frame tier read error: %s", e)
            return results
        if not pending:
            return results
        from .core_utils.frame_codec import decode_driver_frames

        with _suspend_gc():
            for driver, raw in _completed_frame_rows(pending):
                try:
                    frames = decode_driver_frames(
                        raw, None if whole_driver else wanted[driver], frame_class
                    )
                except (AttributeError, TypeError, ValueError) as e:
                    logger.debug("Corrupt driver frame row skipped: %s: %s", driver, e)
                    continue
                for lap, frame in frames.items():
                    results[(driver, lap)] = frame
        return results

    def get_telemetry_frame_sources(
        self,
        year: int,
        gp: str,
        session: str,
        drivers: list[str],
        frame_class: Any = None,
    ) -> dict[str, Any]:
        """Decoded driver rows that build their lap frames on demand (pandas only).

        Pergamon Q1: the rows are read and decompressed through the same
        streamed path as :meth:`_get_driver_frames` (Q11), but the per-lap
        frame construction is deferred to the returned
        :class:`~tif1.core_utils.frame_codec.DriverFrameSource` objects
        (keyed by driver; drivers with no or a corrupt row are absent).
        """
        conn = self.conn
        if conn is None or not drivers or not _FRAME_CODEC_AVAILABLE:
            return {}
        wanted = list(dict.fromkeys(drivers))
        try:
            placeholders = ", ".join("?" for _ in wanted)
            with self._sqlite_lock:
                pending = _submit_frame_rows(
                    conn.execute(
                        f"SELECT driver, frame FROM {_PANDAS_DRIVER_TABLE} WHERE year = ? "
                        f"AND gp = ? AND session = ? AND driver IN ({placeholders})",
                        (year, gp, session, *wanted),
                    )
                )
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Driver frame tier read error: %s", e)
            return {}
        if not pending:
            return {}
        from .core_utils.frame_codec import DriverFrameSource

        sources: dict[str, Any] = {}
        with _suspend_gc():
            for driver, raw in _completed_frame_rows(pending):
                try:
                    sources[driver] = DriverFrameSource(raw, frame_class)
                except (AttributeError, TypeError, ValueError) as e:
                    logger.debug("Corrupt driver frame row skipped: %s: %s", driver, e)
        return sources

    def get_driver_merged_frame(
        self,
        year: int,
        gp: str,
        session: str,
        driver: str,
        laps: list[int],
        frame_class: Any = None,
        *,
        drop_repeats: bool = False,
    ) -> Any | None:
        """One frame of ``laps`` back to back from the driver's row, or None.

        ``drop_repeats``: leave out the sample each lap repeats at its boundary
        (see :func:`~tif1.core_utils.frame_codec.decode_driver_merged`).

        Pergamon P10: serves ``Laps.telemetry`` for a driver without building
        and concatenating per-lap frames. None when the tier has no row for
        the driver, the row cannot be read, or a lap is not stored in it
        (the caller falls back to per-lap frames + concat).
        """
        conn = self.conn
        if conn is None or not laps or not _FRAME_CODEC_AVAILABLE:
            return None
        try:
            with self._sqlite_lock:
                row = conn.execute(
                    f"SELECT frame FROM {_PANDAS_DRIVER_TABLE} WHERE year = ? AND gp = ? "
                    "AND session = ? AND driver = ?",
                    (year, gp, session, driver),
                ).fetchone()
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Driver frame tier read error: %s", e)
            return None
        if row is None:
            return None
        from .core_utils.frame_codec import decode_driver_merged

        try:
            for chunk in _decompress_frame_rows([(driver, row[0])], min_parallel=2):
                for _driver, raw in chunk:
                    return decode_driver_merged(
                        raw, [int(lap) for lap in laps], frame_class, drop_repeats=drop_repeats
                    )
        except (AttributeError, TypeError, ValueError) as e:
            logger.debug("Corrupt driver frame row skipped: %s: %s", driver, e)
        return None

    def _set_driver_frames(
        self, year: int, gp: str, session: str, frames: list[tuple[str, int, Any]]
    ) -> tuple[list[tuple[str, int, Any]], int]:
        """Write per-driver rows; returns ``(leftover frames, frames written)``.

        A driver's laps are merged with the laps already stored for it, so a
        partial batch never drops frames. Frames the codec cannot encode are
        handed back for the per-frame table. Per-frame rows of the laps now
        covered by a driver row are deleted (they were either the legacy
        copy of the same frame or superseded by this batch).
        """
        from .core_utils.frame_codec import (
            decode_driver_frames,
            encode_driver_frames,
        )

        by_driver: dict[str, dict[int, Any]] = {}
        for driver, lap, frame in frames:
            by_driver.setdefault(driver, {})[int(lap)] = frame
        leftovers: list[tuple[str, int, Any]] = []
        conn = self.conn
        if conn is None:
            return frames, 0
        existing: dict[str, bytes] = {}
        try:
            placeholders = ", ".join("?" for _ in by_driver)
            with self._sqlite_lock:
                for driver, blob in conn.execute(
                    f"SELECT driver, frame FROM {_PANDAS_DRIVER_TABLE} WHERE year = ? "
                    f"AND gp = ? AND session = ? AND driver IN ({placeholders})",
                    (year, gp, session, *by_driver),
                ).fetchall():
                    existing[driver] = bytes(blob)
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Driver frame tier merge read skipped: %s", e)
            existing = {}
        rows: list[list[Any]] = []
        covered: list[tuple[str, int]] = []
        written = 0
        # Pergamon Q13: python-lz4 releases the GIL while compressing, so each
        # driver blob is compressed on a small pool as soon as it is encoded and
        # overlaps the (GIL-bound) encode of the next driver; the serial
        # compress was ~36 ms of the 1452-frame write.
        pool = _frame_compress_pool(len(by_driver))
        try:
            with _suspend_gc():
                for driver, lap_frames in by_driver.items():
                    merged = dict(lap_frames)
                    if driver in existing:
                        try:
                            stored = decode_driver_frames(_decompress_frame_blob(existing[driver]))
                        except (*_ZSTD_ERRORS, AttributeError, RuntimeError, TypeError, ValueError):
                            stored = {}  # unreadable row: rewrite it from this batch
                        for lap, frame in stored.items():
                            merged.setdefault(int(lap), frame)
                    blob, unsupported = encode_driver_frames(list(merged.items()))
                    unsupported_laps = {lap for lap, _ in unsupported}
                    if unsupported:
                        leftovers.extend(
                            (driver, lap, frame)
                            for lap, frame in lap_frames.items()
                            if lap in unsupported_laps
                        )
                    if blob is None:
                        continue
                    stored_laps = [lap for lap in merged if lap not in unsupported_laps]
                    compressed: Any = None
                    if pool is not None:
                        try:
                            compressed = pool.submit(_compress_frame_blob, blob)
                        except RuntimeError:
                            # Interpreter shutdown: concurrent.futures refuses new
                            # work before the exit hook joins this writer, so a
                            # script that ends right after a load would lose its
                            # frames. Compressing inline still completes the write.
                            compressed = None
                    if compressed is None:
                        compressed = _compress_frame_blob(blob)
                    rows.append(
                        [
                            year,
                            gp,
                            session,
                            driver,
                            json_dumps_bytes(sorted(stored_laps)).decode("utf-8"),
                            compressed,
                        ]
                    )
                    covered.extend((driver, lap) for lap in stored_laps)
                    written += sum(1 for lap in lap_frames if lap in stored_laps)
            if pool is not None:
                for row in rows:
                    if not isinstance(row[5], bytes | bytearray):  # a pending Future
                        row[5] = row[5].result()
        finally:
            if pool is not None:
                pool.shutdown(wait=True, cancel_futures=True)
        if rows:
            try:
                with self._sqlite_lock:
                    conn.executemany(
                        f"INSERT OR REPLACE INTO {_PANDAS_DRIVER_TABLE} VALUES (?, ?, ?, ?, ?, ?)",
                        rows,
                    )
                    if covered:
                        conn.executemany(
                            "DELETE FROM telemetry_frames WHERE year = ? AND gp = ? "
                            "AND session = ? AND driver = ? AND lap = ?",
                            [(year, gp, session, driver, lap) for driver, lap in covered],
                        )
                    self._pending_writes += len(rows)
                    self._commit_if_needed()
            except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
                logger.debug("Driver frame tier write skipped: %s", e)
                return frames, 0
        return leftovers, written

    # -- polars per-driver Arrow IPC frame tier (Thebes TH8) ----------------

    def _get_polars_ipc_frames(
        self, year: int, gp: str, session: str, driver_laps: list[tuple[str, int]]
    ) -> dict[tuple[str, int], Any]:
        """Serve polars frames from the per-driver IPC rows (missing refs are absent)."""
        wanted: dict[str, set[int]] = {}
        for driver, lap in driver_laps:
            wanted.setdefault(driver, set()).add(lap)
        results: dict[tuple[str, int], Any] = {}
        conn = self.conn
        if conn is None:
            return results
        import polars as pl
        import pyarrow as pa
        import pyarrow.ipc as pa_ipc
        from polars.exceptions import PolarsError

        # Pergamon P4: the IPC blobs are read in worker threads (pyarrow
        # releases the GIL: 15.5 -> 9.6 ms for 22 driver blobs) and converted
        # to polars ONCE from the concatenated table (10 ms) instead of once
        # per driver (25 ms); a driver's laps are then slices at a global
        # offset. A blob that fails to read or whose schema differs from the
        # others is converted on its own (its laps are still served).
        # Pergamon Q6: each row is handed to the pool as soon as the cursor
        # yields it (the IPC reads overlap the rest of the SQL fetch), and a
        # worker reads its blob single-threaded: the pool already spreads the
        # drivers over the cores, pyarrow's own per-column pool on top only
        # oversubscribes (and is spun up cold on the first warm read).
        parallel = THREADS_AVAILABLE and len(wanted) >= 4 and (os.cpu_count() or 1) > 1
        read_options = pa_ipc.IpcReadOptions(use_threads=not parallel)
        arrow_pool = _pa_system_pool()

        def _read(row: tuple[Any, ...]) -> tuple[Any, ...]:
            driver, laps_json, blob = row
            try:
                reader = pa_ipc.open_file(
                    pa.BufferReader(bytes(blob)), options=read_options, memory_pool=arrow_pool
                )
                return driver, json_loads(laps_json), reader.read_all(), None
            except (TypeError, ValueError, OSError, _arrow_error()) as e:
                return driver, None, None, e

        query = (
            f"SELECT driver, laps, frame FROM {_POLARS_IPC_TABLE} WHERE year = ? "
            f"AND gp = ? AND session = ? AND driver IN ({', '.join('?' for _ in wanted)})"
        )
        params = (year, gp, session, *wanted)
        read_rows: list[tuple[Any, ...]]
        try:
            if parallel:
                from concurrent.futures import ThreadPoolExecutor

                with ThreadPoolExecutor(
                    max_workers=min(_PARALLEL_DECOMPRESS_MAX_WORKERS, len(wanted))
                ) as pool:
                    with self._sqlite_lock:
                        futures = [pool.submit(_read, row) for row in conn.execute(query, params)]
                    read_rows = [future.result() for future in futures]
            else:
                with self._sqlite_lock:
                    rows = conn.execute(query, params).fetchall()
                read_rows = [_read(row) for row in rows]
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.warning("Polars frame tier read error: %s", e)
            return results
        if not read_rows:
            return results

        groups: dict[Any, list[tuple[Any, ...]]] = {}
        for driver, lap_spans, table, error in read_rows:
            if error is not None:
                logger.debug("Corrupt polars frame row skipped: %s: %s", driver, error)
                continue
            groups.setdefault(table.schema, []).append((driver, lap_spans, table))
        for schema_rows in groups.values():
            try:
                tables = [table for _, _, table in schema_rows]
                # Q6: no rechunk. Every driver blob is one record batch, so a
                # lap slice never crosses a chunk boundary and each lap frame
                # is still single-chunk; rechunking only copied ~100 MB.
                combined = pl.from_arrow(
                    pa.concat_tables(tables) if len(tables) > 1 else tables[0], rechunk=False
                )
                offset = 0
                for driver, lap_spans, table in schema_rows:
                    base = offset
                    for raw_lap, raw_rows in lap_spans:
                        lap, n_rows = int(raw_lap), int(raw_rows)
                        if lap in wanted[driver]:
                            results[(driver, lap)] = combined.slice(offset, n_rows)
                        offset += n_rows
                    if offset - base != table.num_rows:
                        raise ValueError("lap spans do not cover the driver frame")
            except (PolarsError, TypeError, ValueError, OSError, _arrow_error()) as e:
                for driver, _spans, _table in schema_rows:
                    logger.debug("Corrupt polars frame row skipped: %s: %s", driver, e)
                    for lap in wanted[driver]:
                        results.pop((driver, lap), None)
        return results

    def _set_polars_ipc_frames(
        self, year: int, gp: str, session: str, frames: list[tuple[str, int, Any]]
    ) -> tuple[list[tuple[str, int, Any]], int]:
        """Write per-driver IPC rows; returns ``(leftover frames, frames written)``.

        A driver's laps are merged with the laps already stored for it, so a
        partial batch never drops frames. Drivers whose laps do not share one
        schema are handed back to the per-frame pickle path.
        """
        import polars as pl
        from polars.exceptions import PolarsError

        by_driver: dict[str, dict[int, Any]] = {}
        for driver, lap, frame in frames:
            by_driver.setdefault(driver, {})[int(lap)] = frame
        leftovers: list[tuple[str, int, Any]] = []
        existing: dict[str, tuple[list, bytes]] = {}
        conn = self.conn
        if conn is None:
            return frames, 0
        try:
            placeholders = ", ".join("?" for _ in by_driver)
            with self._sqlite_lock:
                for driver, laps_json, blob in conn.execute(
                    f"SELECT driver, laps, frame FROM {_POLARS_IPC_TABLE} WHERE year = ? "
                    f"AND gp = ? AND session = ? AND driver IN ({placeholders})",
                    (year, gp, session, *by_driver),
                ).fetchall():
                    existing[driver] = (json_loads(laps_json), bytes(blob))
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Polars frame tier merge read skipped: %s", e)
            existing = {}
        rows: list[tuple[Any, ...]] = []
        written = 0
        for driver, lap_frames in by_driver.items():
            merged = dict(lap_frames)
            if driver in existing:
                try:
                    spans, blob = existing[driver]
                    stored = _read_polars_ipc(blob)
                    offset = 0
                    for lap, n_rows in spans:
                        if int(lap) not in merged:
                            merged[int(lap)] = stored.slice(offset, int(n_rows))
                        offset += int(n_rows)
                except (PolarsError, TypeError, ValueError, OSError, _arrow_error()):
                    pass  # unreadable row: rewrite it from this batch
            ordered = [merged[lap] for lap in sorted(merged)]
            schema = ordered[0].schema
            if any(f.schema != schema for f in ordered[1:]):
                leftovers.extend((driver, lap, frame) for lap, frame in lap_frames.items())
                continue
            try:
                driver_frame = pl.concat(ordered, how="vertical", rechunk=True)
                blob = _write_polars_ipc(driver_frame)
            except (PolarsError, TypeError, ValueError, _arrow_error()) as e:
                logger.debug("Polars IPC encode failed for %s: %s", driver, e)
                leftovers.extend((driver, lap, frame) for lap, frame in lap_frames.items())
                continue
            spans_json = json_dumps_bytes([[lap, len(merged[lap])] for lap in sorted(merged)])
            rows.append((year, gp, session, driver, spans_json.decode("utf-8"), blob))
            written += len(lap_frames)
        if rows:
            try:
                with self._sqlite_lock:
                    conn.executemany(
                        f"INSERT OR REPLACE INTO {_POLARS_IPC_TABLE} VALUES (?, ?, ?, ?, ?, ?)",
                        rows,
                    )
                    self._pending_writes += len(rows)
                    self._commit_if_needed()
            except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
                logger.debug("Polars frame tier write skipped: %s", e)
                return frames, 0
        return leftovers, written

    def get_laps_frame(
        self,
        year: int,
        gp: str,
        session: str,
        lib: Literal["pandas", "polars"] = "pandas",
    ) -> Any:
        """Read the materialized processed-laps DataFrame (KR-1 tier).

        Returns:
            The unpickled pandas DataFrame, or None on a miss / corrupt row /
            foreign-backend row (the caller falls back to the payload path).
        """
        if self.conn is None or not _FRAME_CODEC_AVAILABLE:
            return None
        table = _LAPS_FRAME_TABLES.get(lib, "laps_frames")
        try:
            with self._sqlite_lock:
                row = self.conn.execute(
                    f"SELECT frame FROM {table} WHERE year = ? AND gp = ? AND session = ?",
                    (year, gp, session),
                ).fetchone()
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Laps frame read error: %s", e)
            return None
        if row is None:
            return None
        try:
            # Same blob format and trust model as the telemetry frame tier:
            # unpickling trusts the 0o700 user-local cache dir; a corrupt or
            # foreign blob degrades to a tier miss, never a crash.
            with _suspend_gc():
                frame = pickle.loads(_decompress_frame_blob(row[0]))
            if not hasattr(frame, "_df" if lib == "polars" else "_mgr"):
                raise TypeError("foreign frame object")
            return frame
        except (
            AttributeError,
            EOFError,
            ImportError,
            IndexError,
            TypeError,
            ValueError,
            pickle.UnpicklingError,
            *_ZSTD_ERRORS,
        ):
            logger.debug("Corrupt laps frame row skipped: %s/%s/%s", year, gp, session)
            return None

    def set_laps_frame(
        self,
        year: int,
        gp: str,
        session: str,
        frame: Any,
        lib: Literal["pandas", "polars"] = "pandas",
    ) -> int:
        """Materialize the processed laps DataFrame in the KR-1 tier.

        Returns:
            Number of rows written (0 when the tier is unavailable).
        """
        if self.conn is None or self.read_only or not _FRAME_CODEC_AVAILABLE:
            return 0
        table = _LAPS_FRAME_TABLES.get(lib, "laps_frames")
        try:
            with self._sqlite_lock:
                with _suspend_gc():
                    blob = _compress_frame_blob(pickle.dumps(frame, protocol=5))
                    self.conn.execute(
                        f"INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?)",
                        (year, gp, session, blob),
                    )
                self._pending_writes += 1
                self._commit_if_needed()
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Laps frame write skipped: %s", e)
            return 0
        return 1

    def _set_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        """Set cached telemetry data (thread-safe); core of ``set_telemetry``."""
        if self.conn is None or self.read_only:
            return
        try:
            blob = json_dumps_bytes(data)
            cache_key = (year, gp, session, driver, lap)

            # Update memory cache first (fast operation, <1ms); plain blob.
            # Parsed tier is read-through: drop any parsed entry on write.
            self._memory_telemetry_cache.set(cache_key, blob)
            self._parsed_telemetry_cache.pop(cache_key, None)

            # Then update SQLite (slower operation); compressed when large.
            with self._sqlite_lock:
                self.conn.execute(
                    "INSERT OR REPLACE INTO telemetry_cache VALUES (?, ?, ?, ?, ?, ?)",
                    (*cache_key, _encode_sqlite_value(blob)),
                )
                self._pending_writes += 1
                self._commit_if_needed()

            logger.debug("Telemetry cached: %s/%s/%s/%s/%s", year, gp, session, driver, lap)
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug("Telemetry cache write skipped: %s", e)

    def set_telemetry(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        """Set cached telemetry data (thread-safe).

        Optimized to update memory cache first (fast, <1ms) before SQLite (slower).
        This ensures minimal lock duration for memory operations.
        Thin wrapper over ``set_entry("telemetry", (year, gp, session, driver, lap), data)``.
        """
        self.set_entry("telemetry", (year, gp, session, driver, lap), data)

    async def set_telemetry_async(
        self, year: int, gp: str, session: str, driver: str, lap: int, data: Any
    ) -> None:
        """Set cached telemetry data asynchronously."""
        import asyncio  # lazy: only async/network paths need it

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self.set_telemetry, year, gp, session, driver, lap, data
        )

    def has_session_data(self, year: int, gp: str, session: str) -> bool:
        """Return True when JSON or telemetry cache contains entries for a session.

        Keys are stored with the URL-encoded event and session names, so both
        spellings are accepted (``"Monaco Grand Prix"`` and ``"Monaco%20Grand%20Prix"``).
        """
        if self.conn is None:
            return False

        gp = quote(unquote(gp), safe="")
        session = quote(unquote(session), safe="")
        prefix = f"{year}/{gp}/{session}/"
        try:
            # Check memory cache first (lock-free read)
            if any(str(key).startswith(prefix) for key in self._memory_cache):
                return True

            # Check SQLite with appropriate lock
            with self._sqlite_lock:
                cache_hit = self.conn.execute(
                    "SELECT 1 FROM cache WHERE key LIKE ? LIMIT 1",
                    (f"{prefix}%",),
                ).fetchone()
                if cache_hit is not None:
                    return True

                telemetry_hit = self.conn.execute(
                    "SELECT 1 FROM telemetry_cache WHERE year = ? AND gp = ? AND session = ? LIMIT 1",
                    (year, gp, session),
                ).fetchone()
                return telemetry_hit is not None
        except (RuntimeError, TypeError, ValueError, sqlite3.Error) as e:
            logger.debug(
                "Session cache availability probe failed for %s/%s/%s: %s",
                year,
                gp,
                session,
                e,
            )
            # Default to True on probe failures to preserve cache-read behavior.
            return True

    def clear(self) -> None:
        """Clear all cached data."""
        # Ithome U5: join pending background frame writers first so a
        # post-return write cannot re-insert rows after the wipe.
        _join_frame_writers()
        if self.conn is None or self.read_only:
            logger.warning("Cannot clear cache (no connection or read-only mode)")
            return

        # Acquire both locks to ensure consistency
        with self._sqlite_lock:
            self.conn.execute("DELETE FROM cache")
            self.conn.execute("DELETE FROM telemetry_cache")
            for frame_table in _ALL_FRAME_TABLES:
                self.conn.execute(f"DELETE FROM {frame_table}")
            for laps_table in _LAPS_FRAME_TABLES.values():
                self.conn.execute(f"DELETE FROM {laps_table}")
            self.conn.execute("DELETE FROM missing_payloads")
            self.conn.commit()
            self._pending_writes = 0

        with self._memory_cache_lock:
            self._memory_cache.clear()
            self._memory_telemetry_cache.clear()
            self._parsed_cache.clear()
            self._parsed_telemetry_cache.clear()
            if self._missing_payloads is not None:
                self._missing_payloads.clear()

        logger.info("Cache cleared")

    def close(self) -> None:
        """Close database connection."""
        # Ithome U5: background frame writers must complete before the
        # connection closes (atexit calls this), or their writes would be
        # dropped mid-flight.
        _join_frame_writers()
        self._release_resources()

        # A closed global instance would otherwise keep being handed out by
        # get_cache(), silently dropping every later read and write.
        global _cache
        with _cache_lock:
            if _cache is self:
                _cache = None

    def _release_resources(self) -> None:
        """Close the connection and drop the memory tiers (this instance only).

        Takes only this instance's own locks, so it is safe from ``__del__``:
        a finalizer runs on whichever thread triggered GC, possibly one that
        already holds the module-level ``_cache_lock`` (inside ``get_cache()``)
        or is a frame-writer thread, and must never block on either.
        """
        # Acquire both locks to ensure clean shutdown.
        # Snapshot `self.conn` inside the sqlite lock so concurrent close() calls
        # cannot race into `None.close()`.
        with self._sqlite_lock:
            conn = self.conn
            if conn is not None:
                try:
                    self._commit_if_needed(force=True)
                    conn.close()
                except (OSError, RuntimeError, sqlite3.Error) as e:
                    logger.warning("Error closing cache connection: %s", e)
                finally:
                    self.conn = None

        with self._memory_cache_lock:
            self._memory_cache.clear()
            self._memory_telemetry_cache.clear()
            self._parsed_cache.clear()
            self._parsed_telemetry_cache.clear()

        if conn is not None:
            logger.debug("Cache connection closed")

    def __del__(self):
        """Destructor to ensure connection is closed.

        Unregistering from ``_cache`` and joining frame writers are not needed
        here (an instance still held by ``_cache`` or by a running writer is
        reachable, so it is never finalized) and would risk a GC-time deadlock.
        """
        self._release_resources()


_cache = None
_cache_lock = threading.Lock()

# Ithome U5: post-return background frame-tier writes. fetch_all returns as
# soon as frames are assembled; the bulk tier write runs in a daemon thread
# and is joined by close()/invalidate() so the connection never closes (nor a
# clear wipes rows) under a live writer. A writer registered after close
# finds conn=None and drops out via the existing set_telemetry_frames_batch
# guard.
_frame_writers: list[threading.Thread] = []
_frame_writers_lock = threading.Lock()


def register_frame_writer(write_fn) -> None:
    """Start a daemon frame-tier writer thread and register it for joining.

    Without threads (Pyodide) the write runs inline before returning; the
    callers hold no lock at that point, and the frame tier is complete the
    moment the load returns.
    """
    if not THREADS_AVAILABLE:
        write_fn()
        return
    thread = threading.Thread(target=write_fn, name="tif1-frame-write", daemon=True)
    with _frame_writers_lock:
        _frame_writers.append(thread)
    thread.start()


def _join_frame_writers() -> None:
    """Wait for registered frame writers (close/invalidate paths)."""
    with _frame_writers_lock:
        writers = list(_frame_writers)
        _frame_writers.clear()
    for writer in writers:
        if writer.is_alive():
            writer.join()


def get_cache() -> Cache:
    """Get global cache instance (lazy initialization, thread-safe)."""
    global _cache
    if _cache is not None:
        return _cache

    with _cache_lock:
        if _cache is None:
            _cache = Cache()
        return _cache


def _cleanup_cache() -> None:
    """Cleanup global cache on exit."""
    global _cache
    if _cache is not None:
        _cache.close()
        _cache = None


atexit.register(_cleanup_cache)
