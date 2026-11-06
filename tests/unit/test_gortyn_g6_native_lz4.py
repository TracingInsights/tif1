"""Gortyn G6: ``tif1_native.lz4_frame_decompress_into`` on liblz4's block decoder.

liblz4's safe decoder may write past a block's output inside the room it is
given (wild copies the next block overwrites). With the content size in the
header the last block's room is exactly its output; a frame without it decodes
its last block through a scratch buffer, so the destination past the content
stays untouched. Every test is offline and skips without the extension.
"""

from __future__ import annotations

import numpy as np
import pytest

from tif1.core_utils import json_utils

lz4_frame = pytest.importorskip("lz4.frame")

native_into = json_utils.NATIVE_LZ4_DECOMPRESS_INTO
pytestmark = pytest.mark.skipif(
    native_into is None, reason="tif1-native >= 0.2.1 (lz4_frame_decompress_into) not installed"
)


def _content(n_bytes: int, seed: int) -> bytes:
    """Short repeated runs: many matches, so the decoder's wild copies run to the end."""
    rng = np.random.default_rng(seed)
    words = rng.integers(0, 4, size=n_bytes // 8 + 1).astype("float64")
    return np.repeat(words, 3)[: n_bytes // 8 + 1].tobytes()[:n_bytes]


@pytest.mark.parametrize("block_size", [lz4_frame.BLOCKSIZE_MAX64KB, lz4_frame.BLOCKSIZE_MAX4MB])
@pytest.mark.parametrize("n_bytes", [70_001, 200_000, 1_000_003])
def test_unsized_frame_leaves_the_slack_untouched(block_size: int, n_bytes: int) -> None:
    content = _content(n_bytes, seed=n_bytes)
    blob = lz4_frame.compress(content, compression_level=0, store_size=False, block_size=block_size)
    dst = bytearray(b"\xaa" * (len(content) + 4 * 1024 * 1024))
    assert native_into(blob, dst) == len(content)
    assert bytes(dst[: len(content)]) == content
    assert dst[len(content) :].count(0xAA) == len(dst) - len(content)


@pytest.mark.parametrize("store_size", [True, False])
def test_sized_and_unsized_frames_decode_identically(store_size: bool) -> None:
    content = _content(3 * 1024 * 1024 + 5, seed=11)
    blob = lz4_frame.compress(content, compression_level=0, store_size=store_size)
    dst = bytearray(len(content) + 100)
    assert native_into(memoryview(blob), dst) == len(content)
    assert bytes(dst[: len(content)]) == content == lz4_frame.decompress(blob)


def test_independent_blocks_decode_without_a_dictionary() -> None:
    content = _content(500_000, seed=5)
    blob = lz4_frame.compress(content, compression_level=0, block_linked=False)
    dst = bytearray(len(content))
    assert native_into(blob, dst) == len(content)
    assert bytes(dst) == content


def test_a_match_past_the_window_is_corrupt_not_a_read_out_of_bounds() -> None:
    # One linked block whose first sequence copies from 21 bytes before the
    # frame's output: there is no dictionary, so the decoder must refuse it
    # (a full block of room, so the failure cannot be read as a short buffer).
    header = bytes.fromhex("04224d184040c0")
    block = bytes([0x08, 21, 0, 0x50]) + b"YYYYY"
    blob = header + len(block).to_bytes(4, "little") + block + b"\x00" * 4
    with pytest.raises(ValueError, match="corrupt lz4 frame"):
        native_into(blob, bytearray(64 * 1024 + 64))
