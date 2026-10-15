"""Contract tests for the Thebes performance series.

TH1 - typed-columnar frame-tier codec (``core_utils.frame_codec``) and its
      wiring into the SQLite frame tier (legacy pickled rows stay readable,
      corrupt rows degrade to misses, unsupported frames fall back to pickle).
TH2 - direct block assembly in ``_typed_telemetry_frame`` (same frame as the
      public constructor path, same consolidated block layout).
"""

from __future__ import annotations

import pickle

import numpy as np
import pandas as pd
import pytest

from tif1.cache import Cache, _compress_frame_blob
from tif1.core_utils import frame_codec, helpers
from tif1.core_utils.helpers import _create_telemetry_df, _frame_from_arrays

_PAYLOAD = {
    "time": [0.0, 0.016, 0.256, 0.5],
    "rpm": [10500.0, 10620.0, 10700.0, 10800.0],
    "speed": [250.0, 251.0, 252.0, 253.5],
    "gear": [7, 7, 8, 8],
    "throttle": [100.0, 100.0, 99.0, 98.0],
    "brake": [0, 0, 1, 1],
    "drs": [12, 12, 8, 8],
    "distance": [0.0, 1.1, 2.2, 3.3],
    "rel_distance": [0.0, 0.001, 0.002, 0.003],
    "DriverAhead": ["1", "1", "None", "44"],
    "DistanceToDriverAhead": ["None", 12.5, 12.0, "None"],
    "acc_x": [0.1, 0.2, 0.3, 0.4],
    "x": [1.0, 2.0, 3.0, 4.0],
    "y": [1.0, 2.0, 3.0, 4.0],
    "z": [0.0, 0.0, 0.0, 0.0],
    "dataKey": "2026-Monaco Grand Prix-Race-VER-1",
}


def _frame(payload=_PAYLOAD, driver="VER", lap=1) -> pd.DataFrame:
    frame = _create_telemetry_df(payload, driver, lap, "pandas")
    assert frame is not None
    return frame


def _assert_same_frame(left: pd.DataFrame, right: pd.DataFrame) -> None:
    assert list(left.columns) == list(right.columns)
    assert left.dtypes.equals(right.dtypes)
    assert left.index.equals(right.index)
    assert left.equals(right)
    assert len(left._mgr.blocks) == len(right._mgr.blocks)


# -- TH2: direct block assembly ----------------------------------------------


def test_direct_block_assembly_matches_public_constructor_path():
    frame = _frame()
    columns = list(frame.columns)
    arrays = [
        frame[c].array
        if isinstance(frame[c].dtype, pd.api.extensions.ExtensionDtype)
        else frame[c].to_numpy()
        for c in columns
    ]
    public = _frame_from_arrays(arrays, columns, len(frame))
    direct = helpers._direct_frame_from_columns(arrays, columns, len(frame))
    _assert_same_frame(direct, public)
    _assert_same_frame(frame, public)


def test_direct_block_assembly_consolidates_float_columns_into_one_block():
    frame = _frame()
    float_blocks = [b for b in frame._mgr.blocks if b.dtype == np.dtype("float64")]
    assert len(float_blocks) == 1
    assert float_blocks[0].shape[0] >= 8


def test_direct_block_assembly_shares_the_column_index_between_frames():
    first, second = _frame(lap=1), _frame(lap=2)
    assert first.columns is second.columns


def test_direct_block_frames_are_writable_and_independent():
    first, second = _frame(lap=1), _frame(lap=2)
    first.loc[0, "Speed"] = -1.0
    first.loc[0, "nGear"] = 1
    assert second.loc[0, "Speed"] == 250.0
    assert second.loc[0, "nGear"] == 7


def test_frame_from_columns_falls_back_when_direct_path_unavailable(monkeypatch):
    monkeypatch.setattr(helpers, "_DIRECT_BLOCKS_OK", False)
    frame = _frame()
    _assert_same_frame(frame, _frame())


def test_frame_from_columns_falls_back_on_direct_build_error(monkeypatch):
    def _boom(*_args, **_kwargs):
        raise ValueError("drift")

    monkeypatch.setattr(helpers, "_direct_frame_from_columns", _boom)
    frame = _frame()
    assert list(frame.columns)[-2:] == ["Driver", "LapNumber"]


def test_probe_rejects_drifted_internals(monkeypatch):
    monkeypatch.setattr(helpers, "_DIRECT_BLOCKS_AVAILABLE", False)
    assert helpers._probe_direct_blocks() is False


# -- TH1: frame codec ---------------------------------------------------------


def test_codec_round_trip_is_identical():
    frame = _frame()
    blob = frame_codec.encode_frame(frame)
    assert blob is not None
    assert frame_codec.is_encoded_frame(blob)
    decoded = frame_codec.decode_frame(blob)
    _assert_same_frame(decoded, frame)


def test_codec_round_trip_all_sentinel_string_column():
    payload = dict(_PAYLOAD, DriverAhead=["None", "None", "None", "None"])
    frame = _frame(payload)
    assert frame["DriverAhead"].dtype == object
    decoded = frame_codec.decode_frame(frame_codec.encode_frame(frame))
    _assert_same_frame(decoded, frame)
    assert decoded["DriverAhead"].tolist() == [None] * 4


def test_codec_round_trip_clean_string_column_keeps_str_dtype():
    payload = dict(_PAYLOAD, DriverAhead=["1", "1", "16", "44"])
    frame = _frame(payload)
    assert str(frame["DriverAhead"].dtype) == "str"
    decoded = frame_codec.decode_frame(frame_codec.encode_frame(frame))
    _assert_same_frame(decoded, frame)


def test_codec_round_trip_nullable_int_with_missing_values():
    frame = _frame()
    frame["nGear"] = pd.array([7, None, 8, 8], dtype="Int64")
    decoded = frame_codec.decode_frame(frame_codec.encode_frame(frame))
    _assert_same_frame(decoded, frame)
    assert decoded["nGear"].isna().tolist() == [False, True, False, False]


def test_codec_decoded_frames_are_writable_and_pickle_like_the_original():
    frame = _frame()
    decoded = frame_codec.decode_frame(frame_codec.encode_frame(frame))
    decoded.loc[0, "Speed"] = 1.0
    decoded.loc[0, "DRS"] = 0
    assert decoded.loc[0, "Speed"] == 1.0
    assert pickle.loads(pickle.dumps(decoded)).equals(decoded)


def test_codec_declines_unsupported_frames():
    frame = _frame()
    frame["Extra"] = pd.array([1.5, 2.5, 3.5, 4.5], dtype="Float64")
    assert frame_codec.encode_frame(frame) is None
    assert frame_codec.encode_frame(frame.iloc[1:]) is None  # non-zero-based index


def test_codec_rejects_malformed_blobs():
    with pytest.raises(ValueError, match="typed-columnar"):
        frame_codec.decode_frame(b"not a frame")
    blob = frame_codec.encode_frame(_frame())
    with pytest.raises(ValueError, match="corrupt frame"):
        frame_codec.decode_frame(blob[:40])


def test_frame_tier_writes_codec_rows_and_reads_legacy_pickles(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        frame = _frame()
        assert cache.set_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1, frame)]) == 1
        row = cache.conn.execute("SELECT frame FROM telemetry_frames").fetchone()[0]
        from tif1.cache import _decompress_frame_blob

        assert frame_codec.is_encoded_frame(_decompress_frame_blob(row))
        # A row written by an earlier release (plain pickle) reads too.
        cache.conn.execute(
            "INSERT OR REPLACE INTO telemetry_frames VALUES (?, ?, ?, ?, ?, ?)",
            (2026, "GP", "Race", "VER", 2, _compress_frame_blob(pickle.dumps(frame, protocol=5))),
        )
        cache.conn.execute(
            "INSERT OR REPLACE INTO telemetry_frames VALUES (?, ?, ?, ?, ?, ?)",
            (2026, "GP", "Race", "VER", 3, _compress_frame_blob(b"TF1\x01garbage")),
        )
        cache.conn.commit()
        hits = cache.get_telemetry_frames_batch(
            2026, "GP", "Race", [("VER", 1), ("VER", 2), ("VER", 3)]
        )
        assert set(hits) == {("VER", 1), ("VER", 2)}
        _assert_same_frame(hits[("VER", 1)], frame)
        _assert_same_frame(hits[("VER", 2)], frame)
    finally:
        cache.close()


def test_frame_tier_falls_back_to_pickle_for_unsupported_frames(tmp_path):
    cache = Cache(cache_dir=tmp_path)
    try:
        frame = _frame()
        frame["Extra"] = pd.array([1.5, 2.5, 3.5, 4.5], dtype="Float64")
        assert cache.set_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1, frame)]) == 1
        hits = cache.get_telemetry_frames_batch(2026, "GP", "Race", [("VER", 1)])
        assert hits[("VER", 1)].equals(frame)
    finally:
        cache.close()
