"""Contracts for narrowed pandas driver rows (Olympia H1, ``core_utils/frame_narrow.py``).

A narrowed row stores the clocks, the ``Int64`` channels and the dictionary
codes as small integers and widens them back once per row, right after the
decompression; every reader still sees a ``TF1\\x02`` driver blob.
"""

from __future__ import annotations

import pickle

import numpy as np
import pandas as pd
import pytest

from tif1 import cache as cache_module
from tif1.cache import (
    Cache,
    _compress_driver_row,
    _compress_frame_blob,
    _decompress_frame_blob,
    _decompress_row,
    _expand_narrow_row,
    _pa_lz4_codec,
)
from tif1.core_utils import frame_codec, frame_narrow
from tif1.core_utils.helpers import _create_telemetry_df
from tif1.models import Telemetry

pytestmark = pytest.mark.unit

GP = "Bahrain%20Grand%20Prix"
T0 = 1_744_553_294_267_000_000  # session start as epoch nanoseconds (Date - SessionTime)


def _payload(n: int, lap: int, ahead: list | None = None) -> dict:
    return {
        "Time": [round(0.24 * i + 0.001 * (i % 7), 3) for i in range(n)],
        "RPM": [10000.37 + 13.1 * i + lap for i in range(n)],
        "Speed": [200.25 + 0.5 * i for i in range(n)],
        "nGear": [3 + (i + lap) % 5 for i in range(n)],
        "Throttle": [100.0 - (i % 3) * 0.75 for i in range(n)],
        "Brake": [i % 2 for i in range(n)],
        "DRS": [(i // 3) % 2 * 12 for i in range(n)],
        "DriverAhead": ahead if ahead is not None else ["44"] * n,
    }


def _lap_frame(lap: int, n: int = 9, ahead: list | None = None) -> pd.DataFrame:
    start = 3_334_907_000_000 + lap * 95_123_000_000  # LapStartTime, whole milliseconds
    frame = _create_telemetry_df(_payload(n, lap, ahead), "VER", lap, "pandas", (start, T0 + start))
    assert frame is not None
    return frame


def _frames() -> dict[int, pd.DataFrame]:
    # Two layouts: DriverAhead with strings (str dtype) and all-None (object).
    return {
        1: _lap_frame(1, 9, ["44", None, "16", "16", "44", None, "1", "1", "1"]),
        2: _lap_frame(2, 7, [None] * 7),
        3: _lap_frame(3, 11),
        5: _lap_frame(5, 4, [None] * 4),
    }


def _same(left: pd.DataFrame, right: pd.DataFrame) -> None:
    pd.testing.assert_frame_equal(left, right, check_exact=True)
    assert list(left.dtypes) == list(right.dtypes)
    assert sorted(type(b).__name__ for b in left._mgr.blocks) == sorted(
        type(b).__name__ for b in right._mgr.blocks
    )


def _stored(frames: dict[int, pd.DataFrame]) -> bytes:
    prefix, content, leftovers = frame_codec.encode_driver_row(list(frames.items()))
    assert leftovers == []
    assert prefix is not None
    assert content is not None
    return _compress_driver_row(prefix, content)


def test_narrowed_row_widens_back_to_every_lap_on_both_decompress_paths():
    frames = _frames()
    stored = _stored(frames)
    assert stored[:4] == frame_narrow.NARROW_MAGIC
    codec = _pa_lz4_codec()
    buffers = [_decompress_frame_blob(stored)]
    if codec is not None:
        buffers.append(_decompress_row(("VER", stored), codec)[1])
    for buffer in buffers:
        assert frame_codec.is_driver_blob(buffer)
        decoded = frame_codec.decode_driver_frames(buffer, None, Telemetry)
        assert set(decoded) == set(frames)
        for lap, frame in frames.items():
            assert type(decoded[lap]) is Telemetry
            _same(pd.DataFrame(decoded[lap]), frame)


def test_narrowing_picks_session_time_relative_clocks_and_small_ints():
    prefix, content, _ = frame_codec.encode_driver_row(list(_frames().items()))
    assert prefix is not None
    assert content is not None
    plan, _inner = frame_narrow.split_row(prefix + b"\x00" * 16)
    expanded_len, tail_at, tail_len, ops = plan
    codes = [op[0] for op in ops]
    # Date is SessionTime plus one constant in every group: no stored bytes.
    assert codes.count(frame_narrow.OP_REF) == 2
    assert frame_narrow.OP_CODES in codes
    narrow_dtypes = {op[3] for op in ops if op[0] in (frame_narrow.OP_INT, frame_narrow.OP_CODES)}
    assert narrow_dtypes <= {"|i1", "<i2", "<i4"}
    # The anchor of each group is widened before the columns that read it.
    for op in ops:
        if op[0] == frame_narrow.OP_REF or (op[0] == frame_narrow.OP_INT and op[7] is not None):
            ref_dst = op[-1]
            producer = next(i for i, other in enumerate(ops) if other[1] == ref_dst)
            assert producer < ops.index(op)
    assert len(content) == tail_at + tail_len < expanded_len
    plain, _ = frame_codec.encode_driver_frames(list(_frames().items()))
    assert plain is not None
    assert abs(expanded_len - len(plain)) < 64  # header offsets and alignment padding only


def test_expanded_blob_keeps_every_eight_byte_column_aligned():
    buffer = _decompress_frame_blob(_stored(_frames()))
    source = frame_codec.DriverFrameSource(buffer)
    header, body_start = frame_codec._driver_header(memoryview(buffer))
    assert body_start % 8 == 0
    for _columns, _laps, _lengths, specs in header:
        for spec in specs:
            if spec[0] == "np" and np.dtype(spec[1]).itemsize == 8:
                assert spec[4] % 8 == 0
            if spec[0] == "Int64":
                assert spec[2] % 8 == 0
    frame = source.frame(3)
    for block in frame._mgr.blocks:
        values = getattr(block.values, "_ndarray", block.values)
        if isinstance(values, np.ndarray) and values.dtype.itemsize == 8:
            assert values.ctypes.data % 8 == 0


def test_anchor_widens_first_when_it_is_not_the_first_row_of_its_block():
    frames = {}
    for lap in (1, 2):
        frame = _lap_frame(lap)
        columns = list(frame.columns)
        columns.remove("Time")
        frames[lap] = frame[["Time", *columns]].copy()  # Time before SessionTime in the block
    stored = _stored(frames)
    decoded = frame_codec.decode_driver_frames(_decompress_frame_blob(stored))
    for lap, frame in frames.items():
        _same(decoded[lap], frame)


def test_clocks_with_nat_are_stored_as_is_and_round_trip():
    frame = _lap_frame(1)
    other = _lap_frame(2)
    other.loc[2, "Date"] = pd.NaT
    other.loc[4, "SessionTime"] = pd.NaT
    frames = {1: frame, 2: other}
    prefix, content, _ = frame_codec.encode_driver_row(list(frames.items()))
    assert content is not None
    stored = _compress_driver_row(prefix, content)
    decoded = frame_codec.decode_driver_frames(_decompress_frame_blob(stored))
    for lap, expected in frames.items():
        _same(decoded[lap], expected)


def test_rows_with_nothing_to_narrow_are_the_plain_driver_blob():
    frame = pd.DataFrame({"Speed": np.array([1.5, 2.5, 3.25]), "Brake": [True, False, True]})
    prefix, content, leftovers = frame_codec.encode_driver_row([(1, frame)])
    plain, _ = frame_codec.encode_driver_frames([(1, frame)])
    assert prefix is None
    assert leftovers == []
    assert content is not None
    assert plain is not None
    assert bytes(content) == bytes(plain)
    assert _compress_driver_row(None, content)[:4] != frame_narrow.NARROW_MAGIC
    unsupported = frame.assign(Extra=pd.array([1.5, None, 2.0], dtype="Float64"))
    prefix, content, leftovers = frame_codec.encode_driver_row([(1, unsupported)])
    assert prefix is None
    assert content is None
    assert [(lap, frame is unsupported) for lap, frame in leftovers] == [(1, True)]


def test_rows_holding_an_empty_lap_round_trip():
    empty = _lap_frame(2).iloc[:0].reset_index(drop=True)
    frames = {1: _lap_frame(1), 2: empty}
    prefix, content, leftovers = frame_codec.encode_driver_row(list(frames.items()))
    assert content is not None
    assert leftovers == []
    decoded = frame_codec.decode_driver_frames(
        _decompress_frame_blob(_compress_driver_row(prefix, content))
    )
    _same(decoded[1], frames[1])
    _same(decoded[2], empty)
    # A layout group with no rows at all has empty regions sharing offsets:
    # that row is stored plain.
    prefix, content, _ = frame_codec.encode_driver_row([(2, empty)])
    assert prefix is None
    assert content is not None
    decoded = frame_codec.decode_driver_frames(
        _decompress_frame_blob(_compress_driver_row(None, content))
    )
    _same(decoded[2], empty)


def test_int_recipe_is_exact_or_nothing():
    ms = np.array([3_334_907, 3_334_909, 3_335_089, 9_123_412], dtype="int64") * 1_000_000
    kind, enc, unit, add, uses_ref = frame_narrow.int_recipe(ms.view("m8[ns]"), clock=True)
    assert (kind, unit, add, uses_ref, enc.dtype) == ("int", 1_000_000, 0, False, np.int32)
    recipe = frame_narrow.int_recipe(ms + T0, clock=True, ref=ms)
    assert recipe is not None
    assert recipe[0] == "ref"
    assert recipe[3] == T0
    odd = ms + 7  # not whole milliseconds: falls back to unit 1, and the range is too wide
    assert frame_narrow.int_recipe(odd, clock=True) is None
    small = np.array([-1, 0, 7, 126], dtype="int64")
    assert frame_narrow.int_recipe(small, clock=False)[1].dtype == np.int8
    offset = np.array([10**15, 10**15 + 5], dtype="int64")  # needs an offset to fit
    kind, enc, unit, add, _ = frame_narrow.int_recipe(offset, clock=False)
    assert enc.dtype == np.int8
    assert add == 10**15
    assert frame_narrow.int_recipe(np.zeros(0, dtype="int64"), clock=False) is None
    assert frame_narrow.codes_recipe(np.array([-1, 0, 120], dtype="int32")).dtype == np.int8
    assert frame_narrow.codes_recipe(np.array([0, 40_000], dtype="int32")) is None
    assert frame_narrow.codes_recipe(np.array([0, 2**31 - 1], dtype="int32")) is None


def test_narrowed_rows_round_trip_through_the_zstd_fallback(monkeypatch):
    # Python 3.15 has no lz4 wheels: the frame tier compresses with zstd there.
    if cache_module._zstd_compress is None:  # pragma: no cover - interpreter dependent
        pytest.skip("no zstd binding")
    monkeypatch.setattr(cache_module, "_lz4_frame", None)
    frames = _frames()
    stored = _stored(frames)
    assert stored[:4] == frame_narrow.NARROW_MAGIC
    _plan, inner = frame_narrow.split_row(stored)
    assert bytes(inner[:4]) == cache_module._ZSTD_MAGIC
    for buffer in (
        _decompress_frame_blob(stored),
        _decompress_row(("VER", stored), _pa_lz4_codec())[1],
    ):
        decoded = frame_codec.decode_driver_frames(buffer)
        for lap, frame in frames.items():
            _same(decoded[lap], frame)


def test_pyarrow_refusing_the_slack_buffer_falls_back_to_a_copy():
    frames = _frames()
    stored = _stored(frames)

    class _StrictCodec:
        def decompress(self, *_args, **_kwargs):
            raise ValueError("decompressed size mismatch")

    buffer = _expand_narrow_row(stored, _StrictCodec())
    decoded = frame_codec.decode_driver_frames(buffer)
    for lap, frame in frames.items():
        _same(decoded[lap], frame)


@pytest.mark.parametrize(
    "corrupt",
    [
        lambda row: row[:6],  # truncated plan length
        lambda row: row[:8] + b"\x80\x05junk" + row[14:],  # unreadable plan
        lambda row: row[: len(row) // 2],  # truncated content
    ],
)
def test_corrupt_narrowed_rows_are_misses(corrupt):
    bad = corrupt(_stored(_frames()))
    assert _decompress_row(("VER", bad), _pa_lz4_codec()) is None
    with pytest.raises(ValueError, match="narrowed driver row"):
        _decompress_frame_blob(bad)


def test_plan_that_does_not_match_the_buffer_raises():
    stored = _stored(_frames())
    plan, inner = frame_narrow.split_row(stored)
    raw = bytearray(_decompress_frame_blob(bytes(inner)))
    with pytest.raises(ValueError, match="does not match"):
        frame_narrow.expand(plan, raw)  # content length, not the expanded length
    expanded_len, tail_at, tail_len, _ops = plan
    bad_op = (frame_narrow.OP_INT, expanded_len - 8, 9, "<i4", 0, 1, 0, None)
    with pytest.raises(ValueError, match="outside the row"):
        frame_narrow.expand((expanded_len, tail_at, tail_len, [bad_op]), bytearray(expanded_len))
    with pytest.raises(ValueError, match="unknown op"):
        frame_narrow.expand((expanded_len, tail_at, tail_len, [(9,)]), bytearray(expanded_len))
    with pytest.raises(ValueError, match="tail outside"):
        frame_narrow.split_row(frame_narrow.row_prefix((10, 8, 4, [])))
    with pytest.raises(ValueError, match="not a narrowed"):
        frame_narrow.split_row(b"TF1\x02" + bytes(8))


def test_frame_tier_stores_narrowed_rows_and_reads_them_on_every_path(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = _frames()
        batch = [("VER", lap, frame) for lap, frame in frames.items()]
        assert cache.set_telemetry_frames_batch(2025, GP, "Race", batch) == len(frames)
        row = cache.conn.execute("SELECT frame FROM telemetry_frames_drv").fetchone()[0]
        assert bytes(row[:4]) == frame_narrow.NARROW_MAGIC
        refs = [("VER", lap) for lap in frames]
        hits = cache.get_telemetry_frames_batch(2025, GP, "Race", refs)
        for lap, frame in frames.items():
            _same(hits[("VER", lap)], frame)
        sources = cache.get_telemetry_frame_sources(2025, GP, "Race", ["VER"], Telemetry)
        for lap, frame in frames.items():
            _same(pd.DataFrame(sources["VER"].frame(lap)), frame)
        merged = cache.get_driver_merged_frame(2025, GP, "Race", "VER", [3, 1, 2])
        expected = pd.concat([frames[3], frames[1], frames[2]], ignore_index=True)
        pd.testing.assert_frame_equal(merged, expected, check_exact=True)
        # A partial batch merges with the narrowed row already stored.
        extra = _lap_frame(9, 6)
        assert cache.set_telemetry_frames_batch(2025, GP, "Race", [("VER", 9, extra)]) == 1
        laps = cache.conn.execute("SELECT laps FROM telemetry_frames_drv").fetchone()[0]
        assert laps == "[1,2,3,5,9]"
        hits = cache.get_telemetry_frames_batch(2025, GP, "Race", [("VER", 9), ("VER", 1)])
        _same(hits[("VER", 9)], extra)
        _same(hits[("VER", 1)], frames[1])
    finally:
        cache.close()


def test_plain_driver_rows_from_earlier_releases_still_read(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        frames = _frames()
        plain, _ = frame_codec.encode_driver_frames(list(frames.items()))
        assert plain is not None
        cache.conn.execute(
            "INSERT INTO telemetry_frames_drv VALUES (?, ?, ?, ?, ?, ?)",
            (2025, GP, "Race", "VER", "[1,2,3,5]", _compress_frame_blob(plain)),
        )
        hits = cache.get_telemetry_frames_batch(2025, GP, "Race", [("VER", lap) for lap in frames])
        for lap, frame in frames.items():
            _same(hits[("VER", lap)], frame)
    finally:
        cache.close()


def test_corrupt_narrowed_row_in_the_tier_degrades_to_a_miss(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        stored = _stored(_frames())
        cache.conn.execute(
            "INSERT INTO telemetry_frames_drv VALUES (?, ?, ?, ?, ?, ?)",
            (2025, GP, "Race", "VER", "[1,2,3,5]", stored[: len(stored) - 40]),
        )
        assert cache.get_telemetry_frames_batch(2025, GP, "Race", [("VER", 1)]) == {}
        assert cache.get_telemetry_frame_sources(2025, GP, "Race", ["VER"]) == {}
        assert cache.get_driver_merged_frame(2025, GP, "Race", "VER", [1]) is None
    finally:
        cache.close()


def test_plan_survives_pickle_round_trip_with_plain_python_values():
    prefix, _content, _ = frame_codec.encode_driver_row(list(_frames().items()))
    assert prefix is not None
    plan = pickle.loads(prefix[8:])
    for op in plan[3]:
        assert all(type(v) in (int, str, type(None)) for v in op)
    assert cache_module._NARROW_ROW_MAGIC == frame_narrow.NARROW_MAGIC
