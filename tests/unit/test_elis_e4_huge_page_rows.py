"""Elis E4: frame-tier rows decompressed into huge-page buffers.

``tif1_native.lz4_frame_decompress_into`` (tif1-native >= 0.2.1) decodes a
python-lz4 frame into a caller-owned buffer; ``cache._huge_page_decompress``
hands it a ``MADV_HUGEPAGE`` anonymous mapping for rows of at least one huge
page. Every test is offline; the native ones skip without the extension.
"""

from __future__ import annotations

import mmap
import sys

import numpy as np
import pytest

from tif1 import cache
from tif1.core_utils import frame_narrow, json_utils

lz4_frame = pytest.importorskip("lz4.frame")

native_into = json_utils.NATIVE_LZ4_DECOMPRESS_INTO
needs_native = pytest.mark.skipif(
    native_into is None, reason="tif1-native >= 0.2.1 (lz4_frame_decompress_into) not installed"
)
needs_huge_pages = pytest.mark.skipif(
    native_into is None
    or not sys.platform.startswith("linux")
    or not cache._huge_page_rows_enabled(),
    reason="huge-page rows need tif1-native >= 0.2.1 and Linux THP",
)


def _telemetry_like(n_bytes: int, seed: int = 0) -> bytes:
    """Compressible float64 columns with runs and noise (like a driver row body)."""
    rng = np.random.default_rng(seed)
    n = n_bytes // 8
    steps = np.repeat(rng.normal(size=n // 16 + 1), 16)[:n]
    noise = rng.normal(size=n) * (rng.random(n) < 0.3)
    return (np.cumsum(steps) + noise).astype("float64").tobytes()[:n_bytes]


def _frame(content: bytes, **kwargs) -> bytes:
    return lz4_frame.compress(content, compression_level=0, **kwargs)


@needs_native
@pytest.mark.parametrize(
    "kwargs",
    [
        {},  # python-lz4's defaults: linked 64 KB blocks, content size stored
        {"block_linked": False},
        {"block_size": lz4_frame.BLOCKSIZE_MAX256KB},
        {"block_size": lz4_frame.BLOCKSIZE_MAX1MB},
        {"block_size": lz4_frame.BLOCKSIZE_MAX4MB},
        {"store_size": False},
    ],
)
def test_decompress_into_matches_python_lz4(kwargs) -> None:
    content = _telemetry_like(3 * 1024 * 1024 + 123)
    blob = _frame(content, **kwargs)
    dst = bytearray(len(content))
    assert native_into(blob, dst) == len(content)
    assert bytes(dst) == content == lz4_frame.decompress(blob)


@needs_native
def test_decompress_into_matches_pyarrow_codec_bytes() -> None:
    pa = pytest.importorskip("pyarrow")
    content = _telemetry_like(2 * 1024 * 1024 + 7, seed=3)
    blob = _frame(content)
    expected = pa.Codec("lz4").decompress(blob, decompressed_size=len(content)).to_pybytes()
    dst = bytearray(len(content))
    native_into(memoryview(blob), memoryview(dst))
    assert bytes(dst) == expected


@needs_native
def test_incompressible_blocks_are_stored_raw_and_decode() -> None:
    content = np.random.default_rng(1).bytes(300_000)
    blob = _frame(content)
    dst = bytearray(len(content))
    assert native_into(blob, dst) == len(content)
    assert bytes(dst) == content


@needs_native
def test_larger_destination_keeps_its_tail_untouched() -> None:
    content = _telemetry_like(200_000)
    dst = bytearray(b"\xaa" * (len(content) + 5000))
    assert native_into(_frame(content), dst) == len(content)
    assert bytes(dst[: len(content)]) == content
    assert dst[len(content) :] == b"\xaa" * 5000


@needs_native
@pytest.mark.parametrize("store_size", [True, False])
def test_too_small_destination_raises(store_size: bool) -> None:
    content = _telemetry_like(200_000)
    with pytest.raises(ValueError, match="too small"):
        native_into(_frame(content, store_size=store_size), bytearray(len(content) - 1))


@needs_native
def test_read_only_destination_raises() -> None:
    content = _telemetry_like(10_000)
    with pytest.raises(ValueError, match="read-only"):
        native_into(_frame(content), bytes(len(content)))


@needs_native
@pytest.mark.parametrize(
    "kwargs", [{"content_checksum": True}, {"block_checksum": True}], ids=["content", "block"]
)
def test_checksummed_frames_are_unsupported(kwargs) -> None:
    content = _telemetry_like(10_000)
    with pytest.raises(ValueError, match="unsupported"):
        native_into(_frame(content, **kwargs), bytearray(len(content)))


@needs_native
def test_corrupt_frames_raise() -> None:
    content = _telemetry_like(300_000)
    blob = bytearray(_frame(content))
    with pytest.raises(ValueError, match="corrupt lz4 frame"):
        native_into(bytes(blob[: len(blob) // 2]), bytearray(len(content)))  # truncated
    with pytest.raises(ValueError, match="not an lz4 frame"):
        native_into(b"not an lz4 frame", bytearray(100))
    rng = np.random.default_rng(7)
    for _ in range(50):  # scrambled bodies: an error, never a crash
        damaged = bytearray(blob)
        for at in rng.integers(15, len(damaged), size=20):
            damaged[at] ^= 0xFF
        try:
            native_into(bytes(damaged), bytearray(len(content)))
        except ValueError:
            pass


@needs_huge_pages
def test_huge_page_buffer_is_aligned_writable_and_sized() -> None:
    size = 5 * 1024 * 1024 + 11
    view = cache._huge_page_buffer(size)
    assert len(view) == size
    assert not view.readonly
    address = np.frombuffer(view, dtype=np.uint8).__array_interface__["data"][0]
    assert address % cache._HUGE_PAGE_BYTES == 0
    view[-1] = 7
    assert view[-1] == 7


def _plain_row(size: int) -> tuple[bytes, bytes]:
    content = _telemetry_like(size)
    return content, _frame(content)


def _narrowed_row(content_len: int, expanded_len: int) -> tuple[bytes, bytes]:
    content = _telemetry_like(content_len, seed=5)
    plan = (expanded_len, 0, 0, ())
    return content, frame_narrow.row_prefix(plan) + _frame(content)


@needs_huge_pages
def test_big_plain_row_lands_in_a_huge_page_buffer() -> None:
    content, blob = _plain_row(3 * 1024 * 1024)
    driver, buffer = cache._decompress_row(("VER", blob), cache._pa_lz4_codec())
    assert driver == "VER"
    assert isinstance(buffer, memoryview)
    assert isinstance(buffer.obj, mmap.mmap)
    assert bytes(buffer) == content


@needs_huge_pages
def test_big_narrowed_row_lands_in_a_huge_page_buffer() -> None:
    content, blob = _narrowed_row(3 * 1024 * 1024, 4 * 1024 * 1024)
    for codec in (cache._pa_lz4_codec(), None):
        buffer = cache._expand_narrow_row(blob, codec)
        assert isinstance(buffer, memoryview)
        assert isinstance(buffer.obj, mmap.mmap)
        assert len(buffer) == 4 * 1024 * 1024
        assert bytes(buffer[: len(content)]) == content


@needs_huge_pages
def test_small_rows_keep_the_pyarrow_path() -> None:
    content, blob = _plain_row(cache._HUGE_PAGE_BYTES - 8)
    _driver, buffer = cache._decompress_row(("VER", blob), cache._pa_lz4_codec())
    assert not isinstance(buffer, memoryview)
    assert bytes(memoryview(buffer)) == content


def test_disabled_huge_pages_use_the_codec(monkeypatch) -> None:
    monkeypatch.setattr(cache, "_HUGE_PAGE_ROWS", [False])
    content, blob = _plain_row(3 * 1024 * 1024)
    _driver, buffer = cache._decompress_row(("VER", blob), cache._pa_lz4_codec())
    assert not isinstance(buffer, memoryview)
    assert bytes(memoryview(buffer)) == content
    content, blob = _narrowed_row(3 * 1024 * 1024, 3 * 1024 * 1024)
    assert bytes(memoryview(cache._expand_narrow_row(blob, None))) == content


def test_native_rejection_falls_back_to_the_codec(monkeypatch) -> None:
    def reject(src, dst):
        raise ValueError("unsupported lz4 frame: checksums")

    monkeypatch.setattr(cache, "_HUGE_PAGE_ROWS", [True])
    monkeypatch.setattr(cache, "NATIVE_LZ4_DECOMPRESS_INTO", reject)
    content, blob = _plain_row(3 * 1024 * 1024)
    _driver, buffer = cache._decompress_row(("VER", blob), cache._pa_lz4_codec())
    assert bytes(memoryview(buffer)) == content
    content, blob = _narrowed_row(3 * 1024 * 1024, 3 * 1024 * 1024 + 64)
    buffer = cache._expand_narrow_row(blob, cache._pa_lz4_codec())
    assert bytes(memoryview(buffer)[: len(content)]) == content


def test_huge_page_probe_without_the_extension(monkeypatch) -> None:
    monkeypatch.setattr(cache, "_HUGE_PAGE_ROWS", [None])
    monkeypatch.setattr(cache, "NATIVE_LZ4_DECOMPRESS_INTO", None)
    assert cache._huge_page_rows_enabled() is False
    assert cache._huge_page_decompress(b"", 10) is None


@pytest.mark.skipif(not hasattr(mmap, "MADV_HUGEPAGE"), reason="no MADV_HUGEPAGE here")
@pytest.mark.parametrize(
    ("setting", "expected"),
    [("always [madvise] never\n", True), ("always madvise [never]\n", False), (None, False)],
)
def test_huge_page_probe_reads_the_thp_setting(monkeypatch, tmp_path, setting, expected) -> None:
    path = tmp_path / "enabled"
    if setting is not None:
        path.write_text(setting, encoding="utf-8")
    monkeypatch.setattr(cache, "_HUGE_PAGE_ROWS", [None])
    monkeypatch.setattr(cache, "NATIVE_LZ4_DECOMPRESS_INTO", lambda _src, _dst: 0)
    monkeypatch.setattr(cache, "_THP_ENABLED_PATH", path.as_posix())
    assert cache._huge_page_rows_enabled() is expected


def test_huge_page_probe_off_on_emscripten(monkeypatch) -> None:
    monkeypatch.setattr(cache, "_HUGE_PAGE_ROWS", [None])
    monkeypatch.setattr(cache, "NATIVE_LZ4_DECOMPRESS_INTO", lambda _src, _dst: 0)
    monkeypatch.setattr(cache, "IS_EMSCRIPTEN", True)
    assert cache._huge_page_rows_enabled() is False
