"""Codec-selection contracts for the SQLite cache tiers.

The cache takes zstd from the standard library (``compression.zstd``) on
Python 3.14+ and from the ``zstandard`` wheel on older interpreters; lz4 is
optional (no 3.15 wheels yet). These tests pin the behaviour that must hold
regardless of which binding the running interpreter ended up with, so a cache
directory stays readable when the interpreter (and therefore the binding)
changes underneath it.
"""

from __future__ import annotations

import sys

import pytest

from tif1 import cache

pytestmark = pytest.mark.unit

LARGE = b'{"speed": [' + b"3," * 9000 + b"3]}"


def test_zstd_binding_matches_interpreter() -> None:
    if sys.version_info >= (3, 14):
        assert cache._stdlib_zstd is not None
    else:
        assert cache._stdlib_zstd is None
    # Every supported interpreter has a zstd binding (stdlib or wheel).
    assert cache._zstd_compress is not None
    assert cache._zstd_decompress is not None
    assert cache._ZSTD_ERRORS


def test_sqlite_rows_are_zstd_frames() -> None:
    assert len(LARGE) >= cache._SQLITE_COMPRESS_MIN_BYTES
    stored = cache._encode_sqlite_value(LARGE)
    assert isinstance(stored, bytes)
    assert stored[:4] == cache._ZSTD_MAGIC
    assert cache._decode_sqlite_value(stored) == LARGE


def test_corrupt_zstd_row_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Corrupt compressed cache entry"):
        cache._decode_sqlite_value(cache._ZSTD_MAGIC + b"\xff" * 32)


def test_zstd_errors_tuple_matches_binding() -> None:
    # The frame-tier batch reader swallows ``*_ZSTD_ERRORS`` as a corrupt row;
    # the tuple must name the exception the active binding actually raises.
    assert cache._zstd_decompress is not None
    with pytest.raises(cache._ZSTD_ERRORS):
        cache._zstd_decompress(cache._ZSTD_MAGIC + b"\xff" * 32)


def test_frame_blob_round_trip_uses_preferred_codec() -> None:
    stored = cache._compress_frame_blob(LARGE)
    expected_magic = cache._LZ4_MAGIC if cache._lz4_frame is not None else cache._ZSTD_MAGIC
    assert stored[:4] == expected_magic
    assert cache._decompress_frame_blob(stored) == LARGE


def test_frame_tier_reads_zstd_rows() -> None:
    # Rows written before the lz4 switch, or on interpreters without lz4
    # wheels, are zstd frames; they must stay readable on every interpreter.
    assert cache._zstd_compress is not None
    legacy = cache._zstd_compress(LARGE)
    assert legacy[:4] == cache._ZSTD_MAGIC
    assert cache._decompress_frame_blob(legacy) == LARGE


def test_zstd_frame_header_carries_content_size() -> None:
    # The ``zstandard`` one-shot decompressor refuses frames without a content
    # size, so frames written by either binding must embed it for a cache to
    # move between interpreters in both directions.
    assert cache._zstd_compress is not None
    frame = cache._zstd_compress(LARGE)
    if cache._stdlib_zstd is not None:
        size = cache._stdlib_zstd.get_frame_info(frame).decompressed_size
    else:
        import zstandard

        size = zstandard.get_frame_parameters(frame).content_size
    assert size == len(LARGE)
