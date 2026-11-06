"""Session bundles (Corinth C10): a session's small JSON files in one zstd container.

A cold session reads a handful of small files one request each: ``drivers.json``
and the session laptimes (the first wave), ``session_info.json``, the weather
and race-control tables, and the Ergast classification with its season index
from the Stats repository. A bundle carries their bytes, unchanged, in one
file next to them (``{year}/{gp}/{session}/session.v1.bin``), so one request
answers them all. ``design/SESSION_BUNDLE_FORMAT.md`` is the specification::

    magic      8 bytes  b"TIF1SES\\x01"
    raw size   8 bytes  little-endian u64, the size of the decompressed body
    frame      one zstd frame (content checksum on) of the body:
        head size  4 bytes  little-endian u32
        head       a JSON object: {"format": 1, "built_at": <epoch seconds>,
                   "entries": [[name, size], ...], "absent": [name, ...]}
        bodies     the entries' bytes, concatenated in ``entries`` order

Entry names are the session-relative paths of the per-year repository
(``drivers.json``) and ``stats/{year}/{slug}/{file}`` for the Stats
repository (the key of its cache tier; ``stats/{year}/events.json`` is the
season index). ``absent`` names the member files the repository did not hold
when the bundle was built. This module only encodes and decodes; the reader
is :mod:`tif1.session_bundle`. It imports neither numpy nor pandas.
"""

from __future__ import annotations

import struct
import sys
import threading
from collections.abc import Iterable, Mapping
from typing import Any

import orjson

MAGIC = b"TIF1SES\x01"
FORMAT_VERSION = 1
#: File name of a session's bundle, next to its JSON files.
BUNDLE_FILE = f"session.v{FORMAT_VERSION}.bin"
#: Per-year repository files a bundle carries (session-relative paths).
SESSION_MEMBERS = (
    "drivers.json",
    "session_laptimes.json",
    "session_info.json",
    "weather.json",
    "rcm.json",
)
#: Name prefix of the Stats repository entries (their cache-tier keys).
STATS_PREFIX = "stats/"
#: Upper bound of a decompressed body (a race's members are ~0.4 MB).
MAX_RAW_SIZE = 64 << 20

_RAW_SIZE = struct.Struct("<Q")
_HEAD_SIZE = struct.Struct("<I")
_PREFIX_SIZE = len(MAGIC) + _RAW_SIZE.size


class SessionBundleError(ValueError):
    """A bundle that is not a valid ``session.v1.bin`` (wrong magic, corrupt frame, bad head)."""


class SessionBundle:
    """A decoded bundle: its members' bytes, the names it lists as absent, its build time."""

    __slots__ = ("absent", "built_at", "entries")

    def __init__(self, entries: dict[str, bytes], absent: frozenset[str], built_at: float) -> None:
        self.entries = entries
        self.absent = absent
        self.built_at = built_at

    def __repr__(self) -> str:
        return (
            f"SessionBundle(entries={sorted(self.entries)!r}, absent={sorted(self.absent)!r}, "
            f"built_at={self.built_at!r})"
        )


def encode_bundle(
    entries: Mapping[str, bytes],
    *,
    absent: Iterable[str] = (),
    built_at: float,
    level: int = 19,
) -> bytes:
    """A ``session.v1.bin`` holding ``entries`` (name -> bytes, kept in order).

    Args:
        entries: Member name -> the file's bytes as the repository serves them
            (after content decoding).
        absent: Member names the repository does not hold.
        built_at: Epoch seconds at which the inputs were read (the reader ages
            the Stats entries from it).
        level: zstd level.

    Raises:
        SessionBundleError: A name appears twice or both as an entry and absent.
    """
    names = list(entries)
    missing = sorted(set(absent))
    if len(set(names)) != len(names) or set(names) & set(missing):
        raise SessionBundleError("duplicate member names")
    bodies = [bytes(entries[name]) for name in names]
    head = orjson.dumps(
        {
            "format": FORMAT_VERSION,
            "built_at": float(built_at),
            "entries": [[name, len(body)] for name, body in zip(names, bodies, strict=True)],
            "absent": missing,
        }
    )
    raw = _HEAD_SIZE.pack(len(head)) + head + b"".join(bodies)
    if len(raw) > MAX_RAW_SIZE:
        raise SessionBundleError(f"bundle body of {len(raw)} bytes exceeds {MAX_RAW_SIZE}")
    return MAGIC + _RAW_SIZE.pack(len(raw)) + _zstd_compress(raw, level)


def decode_bundle(blob: bytes | bytearray | memoryview) -> SessionBundle:
    """Decode a ``session.v1.bin``.

    Raises:
        SessionBundleError: Wrong magic or version, a corrupt or truncated
            frame (zstd's content checksum), or a head whose sizes do not add up.
    """
    data = bytes(blob)
    if len(data) < _PREFIX_SIZE or data[: len(MAGIC)] != MAGIC:
        raise SessionBundleError("not a session bundle (magic)")
    (raw_size,) = _RAW_SIZE.unpack_from(data, len(MAGIC))
    if raw_size > MAX_RAW_SIZE:
        raise SessionBundleError(f"bundle body of {raw_size} bytes exceeds {MAX_RAW_SIZE}")
    try:
        raw = _zstd_decompress(data[_PREFIX_SIZE:], raw_size)
    except Exception as e:  # each binding raises its own error type
        raise SessionBundleError(f"corrupt bundle frame: {e}") from e
    if len(raw) != raw_size or raw_size < _HEAD_SIZE.size:
        raise SessionBundleError("bundle body size mismatch")
    (head_size,) = _HEAD_SIZE.unpack_from(raw, 0)
    start = _HEAD_SIZE.size + head_size
    try:
        head = orjson.loads(raw[_HEAD_SIZE.size : start])
        if head["format"] != FORMAT_VERSION:
            raise SessionBundleError(f"unsupported bundle format {head['format']!r}")
        built_at = float(head["built_at"])
        listed = [(str(name), int(size)) for name, size in head["entries"]]
        absent = frozenset(str(name) for name in head.get("absent", ()))
    except SessionBundleError:
        raise
    except (orjson.JSONDecodeError, KeyError, TypeError, ValueError) as e:
        raise SessionBundleError(f"bad bundle head: {e}") from e
    if start + sum(size for _, size in listed) != raw_size or any(s < 0 for _, s in listed):
        raise SessionBundleError("bundle entry sizes do not match its body")
    entries: dict[str, bytes] = {}
    offset = start
    for name, size in listed:
        entries[name] = raw[offset : offset + size]
        offset += size
    return SessionBundle(entries, absent, built_at)


# -- zstd ------------------------------------------------------------------------

if sys.version_info >= (3, 14):  # pragma: no cover - interpreter dependent
    try:
        from compression import zstd as _stdlib_zstd
    except ImportError:  # interpreter built without libzstd
        _stdlib_zstd = None
else:  # pragma: no cover - interpreter dependent
    _stdlib_zstd = None

_TLS = threading.local()


def _zstd_compress(raw: bytes, level: int) -> bytes:
    """``raw`` as one zstd frame with its content size and checksum."""
    if _stdlib_zstd is not None:  # pragma: no cover - interpreter dependent
        options: dict[Any, int] = {
            _stdlib_zstd.CompressionParameter.compression_level: level,
            _stdlib_zstd.CompressionParameter.checksum_flag: 1,
        }
        return _stdlib_zstd.compress(raw, options=options)
    import zstandard

    return zstandard.ZstdCompressor(level=level, write_checksum=True).compress(raw)


def _zstd_decompress(frame: bytes, raw_size: int) -> bytes:
    """Decompress one zstd frame (a per-thread ``zstandard`` context: not thread-safe)."""
    if _stdlib_zstd is not None:  # pragma: no cover - interpreter dependent
        return _stdlib_zstd.decompress(frame)
    dctx = getattr(_TLS, "dctx", None)
    if dctx is None:
        import zstandard

        dctx = _TLS.dctx = zstandard.ZstdDecompressor()
    return dctx.decompress(frame, max_output_size=raw_size)


__all__ = [
    "BUNDLE_FILE",
    "FORMAT_VERSION",
    "MAGIC",
    "SESSION_MEMBERS",
    "STATS_PREFIX",
    "SessionBundle",
    "SessionBundleError",
    "decode_bundle",
    "encode_bundle",
]
