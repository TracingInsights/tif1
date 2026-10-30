"""Contract tests for the native telemetry decoder and its pure-Python twin (Olympia H6).

``tif1-native`` (``native/``) decodes ``_tel.json`` payloads to numpy arrays
and, from 0.2, ``DriverAhead`` to a :class:`DictionaryChannel` (int32 codes
into a short category list). Every frame builder must give the frame the
orjson lists give. The pure-Python half of this module runs everywhere (the
channels are built by hand); the ``native`` half runs only where the
extension is importable (the CI ``native`` job builds it).
"""

from __future__ import annotations

import operator
import pickle
import subprocess
import sys

import numpy as np
import orjson
import pandas as pd
import pytest

import tif1.cache as cache_mod
from tif1.core import _telemetry_payload_has_lists
from tif1.core_utils import json_utils
from tif1.core_utils.helpers import (
    _create_telemetry_df,
    _decoded_channels,
    _merge_telemetry_payloads,
    _telemetry_frame_from_merged,
    _without_null_categories,
)
from tif1.core_utils.json_utils import DictionaryChannel

CLOCK = (5_000_000_000, 1_741_000_000_000_000_000)


def _encode(values: list[str | None]) -> DictionaryChannel:
    """What the native decoder returns for a string channel (first-appearance categories)."""
    positions: dict[str, int] = {}
    codes = [-1 if v is None else positions.setdefault(v, len(positions)) for v in values]
    return DictionaryChannel(np.array(codes, dtype=np.int32), list(positions))


def _payload(n: int, ahead: list[str | None], *, native: bool, sentinel: bool = False) -> dict:
    """A ``_tel.json`` payload in orjson shape (lists) or native shape (arrays + codes)."""
    rng = np.random.default_rng(n)
    time = np.round(np.arange(n) * 0.27 + 0.013, 3)
    speed = np.round(rng.uniform(80, 330, n), 1)
    distance = np.round(np.cumsum(rng.uniform(10, 20, n)), 6)
    gap = np.round(rng.uniform(0, 900, n), 4)
    gear = rng.integers(1, 9, n)
    brake = rng.integers(0, 2, n)
    if native:
        gap_values: object = gap.copy()
        if sentinel:
            gap_values[0] = np.nan  # type: ignore[index]
        return {
            "time": time,
            "speed": speed,
            "gear": gear.astype(np.int64),
            "brake": brake.astype(np.int64),
            "distance": distance,
            "DriverAhead": _encode(ahead),
            "DistanceToDriverAhead": gap_values,
            "dataKey": "k",
        }
    gap_list: list[object] = gap.tolist()
    if sentinel:
        gap_list[0] = "None"
    return {
        "time": time.tolist(),
        "speed": speed.tolist(),
        "gear": gear.tolist(),
        "brake": brake.tolist(),
        "distance": distance.tolist(),
        "DriverAhead": list(ahead),
        "DistanceToDriverAhead": gap_list,
        "dataKey": "k",
    }


AHEAD_CASES = {
    "plain": ["44", "44", "16", "16", "1", "1"],
    "none_token": ["None", "None", "44", "44", "16", "16"],
    "json_null": [None, "44", "44", None, "16", "16"],
    "all_none_token": ["None"] * 6,
    "all_json_null": [None] * 6,
    "mixed_missing": ["None", None, "81", "81", "None", "4"],
}


class TestDictionaryChannel:
    def test_sequence_contract(self):
        channel = _encode(["None", "44", None, "44"])
        assert len(channel) == 4
        assert channel.tolist() == ["None", "44", None, "44"]
        assert list(channel) == ["None", "44", None, "44"]
        assert channel[1] == "44"
        assert channel[1:3] == ["44", None]
        assert channel == ["None", "44", None, "44"]
        assert operator.eq(["None", "44", None, "44"], channel)  # reflected list == channel
        assert channel == _encode(["None", "44", None, "44"])
        assert channel != ["44"]
        assert (channel == 3) is False
        assert "categories=['None', '44']" in repr(channel)
        with pytest.raises(TypeError):
            hash(channel)

    def test_pickle_round_trip(self):
        channel = _encode(["1", None, "1"])
        restored = pickle.loads(pickle.dumps(channel))
        assert restored == channel
        assert restored.codes.dtype == np.int32

    def test_serializes_as_its_list(self):
        values = ["None", "44", None, "16"]
        encoded = {"DriverAhead": _encode(values), "speed": np.array([1.5, 2.0])}
        listed = {"DriverAhead": values, "speed": [1.5, 2.0]}
        assert json_utils.json_dumps_bytes(encoded) == json_utils.json_dumps_bytes(listed)
        assert json_utils.json_dumps(encoded) == json_utils.json_dumps(listed)
        assert orjson.loads(json_utils.json_dumps_bytes(encoded))["DriverAhead"] == values

    def test_unknown_types_still_fail_to_serialize(self):
        with pytest.raises(TypeError):
            json_utils._json_default(object())

    def test_null_categories_are_remapped(self):
        channel = _encode(["None", "44", None, "16", "null", "44"])
        codes, categories = _without_null_categories(channel)
        assert categories == ["44", "16"]
        assert codes.tolist() == [-1, 0, -1, 1, -1, 0]
        clean = _encode(["44", "16"])
        assert _without_null_categories(clean) == (clean.codes, clean.categories)

    def test_channel_types_cover_dictionary_channels(self):
        assert _telemetry_payload_has_lists({"DriverAhead": _encode(["1"])})
        assert not _telemetry_payload_has_lists({"dataKey": "x"})


class TestFrameBuilders:
    @pytest.mark.parametrize("case", sorted(AHEAD_CASES))
    @pytest.mark.parametrize("sentinel", [False, True])
    def test_pandas_lap_frame_matches_list_payload(self, case, sentinel):
        ahead = AHEAD_CASES[case]
        n = len(ahead)
        ref = _create_telemetry_df(
            _payload(n, ahead, native=False, sentinel=sentinel), "VER", 3, "pandas", clock=CLOCK
        )
        got = _create_telemetry_df(
            _payload(n, ahead, native=True, sentinel=sentinel), "VER", 3, "pandas", clock=CLOCK
        )
        pd.testing.assert_frame_equal(ref, got, check_exact=True)
        assert type(ref["DriverAhead"].array) is type(got["DriverAhead"].array)

    @pytest.mark.parametrize("case", sorted(AHEAD_CASES))
    def test_polars_lap_frame_matches_list_payload(self, case):
        pl = pytest.importorskip("polars")
        from polars.testing import assert_frame_equal

        ahead = AHEAD_CASES[case]
        n = len(ahead)
        ref = _create_telemetry_df(
            _payload(n, ahead, native=False), "VER", 3, "polars", clock=CLOCK
        )
        got = _create_telemetry_df(_payload(n, ahead, native=True), "VER", 3, "polars", clock=CLOCK)
        assert isinstance(got, pl.DataFrame)
        assert_frame_equal(ref, got, check_exact=True)

    def test_polars_generic_fallback_decodes_the_channel(self):
        """A payload the typed polars builder refuses (float gears) keeps DriverAhead."""
        pytest.importorskip("polars")
        from polars.testing import assert_frame_equal

        listed = {
            "time": [0.0, 0.5, 1.0],
            "gear": [1.5, 2.0, 3.0],
            "DriverAhead": ["None", "4", "4"],
        }
        native = dict(listed, DriverAhead=_encode(["None", "4", "4"]))
        ref = _create_telemetry_df(listed, "VER", 1, "polars")
        got = _create_telemetry_df(native, "VER", 1, "polars")
        assert ref is not None
        assert got is not None
        assert_frame_equal(ref, got, check_exact=True)
        assert got["DriverAhead"].to_list() == [None, "4", "4"]

    def test_decoded_channels(self):
        plain = {"time": [1.0], "DriverAhead": ["None"]}
        assert _decoded_channels(plain) is plain
        encoded = {"time": [1.0], "DriverAhead": _encode(["None", "4"])}
        assert _decoded_channels(encoded) == {"time": [1.0], "DriverAhead": ["None", "4"]}
        cleaned = _decoded_channels(encoded, clean_driver_ahead=True)
        assert cleaned["DriverAhead"] == [None, "4"]

    def test_merged_frame_matches_list_payloads(self):
        entries_list, entries_native = [], []
        for lap, case in enumerate(sorted(AHEAD_CASES), start=1):
            ahead = AHEAD_CASES[case]
            entries_list.append(("VER", lap, _payload(6, ahead, native=False, sentinel=lap == 2)))
            entries_native.append(("VER", lap, _payload(6, ahead, native=True, sentinel=lap == 2)))
        merged = _merge_telemetry_payloads(entries_native)
        assert isinstance(merged["Time"], np.ndarray)
        assert isinstance(merged["nGear"], np.ndarray)
        assert type(merged["DriverAhead"]).__name__ == "ArrowStringArray"
        ref = _telemetry_frame_from_merged(_merge_telemetry_payloads(entries_list))
        got = _telemetry_frame_from_merged(merged)
        pd.testing.assert_frame_equal(ref, got, check_exact=True)

    def test_merged_frame_with_mixed_shapes(self):
        """An orjson-shaped lap among native ones (a JSON-tier payload) joins the typed columns."""
        cases = sorted(AHEAD_CASES)
        entries_list = [
            ("HAM", i, _payload(6, AHEAD_CASES[c], native=False)) for i, c in enumerate(cases)
        ]
        entries_mixed = [
            ("HAM", i, _payload(6, AHEAD_CASES[c], native=i % 2 == 0)) for i, c in enumerate(cases)
        ]
        merged = _merge_telemetry_payloads(entries_mixed)
        assert isinstance(merged["Speed"], np.ndarray)
        ref = _telemetry_frame_from_merged(_merge_telemetry_payloads(entries_list))
        pd.testing.assert_frame_equal(ref, _telemetry_frame_from_merged(merged), check_exact=True)

    @pytest.mark.parametrize(
        "entries_case",
        ["padded", "sentinel_list", "null_only", "bool_list", "non_str_ahead"],
    )
    def test_merged_frame_falls_back_to_lists(self, entries_case):
        native = _payload(4, ["1", "1", "2", "2"], native=True)
        listed = _payload(4, ["1", "1", "2", "2"], native=False)
        other_native = _payload(4, ["3", "3", "3", "3"], native=True)
        other_listed = _payload(4, ["3", "3", "3", "3"], native=False)
        if entries_case == "padded":
            other_native = dict(other_native, speed=other_native["speed"][:3])
            other_listed = dict(other_listed, speed=other_listed["speed"][:3])
        elif entries_case == "sentinel_list":
            other_native = dict(other_listed)
            other_native["DistanceToDriverAhead"] = ["None", 1.0, 2.0, 3.0]
            other_listed = dict(other_native)
        elif entries_case == "null_only":
            native = dict(native, DriverAhead=_encode([None] * 4))
            listed = dict(listed, DriverAhead=[None] * 4)
            other_native = dict(other_native, DriverAhead=_encode([None] * 4))
            other_listed = dict(other_listed, DriverAhead=[None] * 4)
        elif entries_case == "bool_list":
            other_native = dict(other_listed, brake=[True, False, True, False])
            other_listed = dict(other_native)
        else:
            other_native = dict(other_listed, DriverAhead=[1, 1, 1, 1])
            other_listed = dict(other_native)
        ref = _telemetry_frame_from_merged(
            _merge_telemetry_payloads([("A", 1, listed), ("B", 2, other_listed)])
        )
        got = _telemetry_frame_from_merged(
            _merge_telemetry_payloads([("A", 1, native), ("B", 2, other_native)])
        )
        pd.testing.assert_frame_equal(ref, got, check_exact=True)

    def test_orjson_only_merge_keeps_lists(self):
        listed = _payload(4, ["1", "1", "2", "2"], native=False)
        merged = _merge_telemetry_payloads([("A", 1, listed)])
        assert all(isinstance(values, list) for values in merged.values())


class TestDecoderWiring:
    def test_dict_args_probe(self):
        def old_decoder(payload, numeric=None):  # tif1-native 0.1: two arguments
            return orjson.loads(payload)

        assert json_utils._native_dict_args(old_decoder) == ()

        def new_decoder(payload, numeric=None, dict_channels=None, factory=None):
            data = orjson.loads(payload)
            return {k: factory(*_tuple(v)) if k in dict_channels else v for k, v in data.items()}

        def _tuple(values):
            channel = _encode(values)
            return channel.codes, channel.categories

        assert json_utils._native_dict_args(new_decoder) == (
            json_utils._NATIVE_DICT_KEYS,
            DictionaryChannel,
        )

    def test_dictionary_keys_cover_raw_and_canonical_names(self):
        assert set(json_utils._NATIVE_DICT_KEYS) == {"DriverAhead", "driver_ahead"}

    def test_disable_switch(self):
        code = (
            "from tif1.core_utils import json_utils as j; "
            "print(j.NATIVE_TELEMETRY_DECODER_AVAILABLE, j.native_dictionary_channels())"
        )
        out = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            check=True,
            env={**_subprocess_env(), "TIF1_DISABLE_NATIVE": "1"},
        )
        assert out.stdout.split() == ["False", "False"]

    def test_native_rows_parallelize_earlier(self, monkeypatch):
        """With the native parse in the workers, 16+ payload rows already use the pool."""
        import threading

        seen: set[str] = set()

        def fake_decode(raw):
            seen.add(threading.current_thread().name)
            return {"raw": raw}

        monkeypatch.setattr(cache_mod, "NATIVE_TELEMETRY_DECODER_AVAILABLE", True)
        monkeypatch.setattr(cache_mod, "decode_telemetry_payload", fake_decode)
        monkeypatch.setattr(cache_mod, "_decode_sqlite_value", lambda data: data)
        rows = [("VER", lap, b"{}") for lap in range(cache_mod._PARALLEL_NATIVE_PAYLOAD_MIN_ROWS)]
        decoded = [
            row for chunk in cache_mod._decode_payload_rows(rows, workers=4) for row in chunk
        ]
        assert len(decoded) == len(rows)
        assert threading.current_thread().name not in seen

        seen.clear()
        monkeypatch.setattr(cache_mod, "NATIVE_TELEMETRY_DECODER_AVAILABLE", False)
        decoded = [
            row for chunk in cache_mod._decode_payload_rows(rows, workers=4) for row in chunk
        ]
        assert len(decoded) == len(rows)
        assert seen == {threading.current_thread().name}  # below the orjson threshold


def _subprocess_env() -> dict[str, str]:
    import os

    return {k: v for k, v in os.environ.items() if k != "TIF1_DISABLE_NATIVE"}


native = pytest.mark.skipif(
    not json_utils.native_dictionary_channels(), reason="tif1-native >= 0.2 is not installed"
)

NATIVE_PAYLOADS = {
    "typed": b'{"time":[0,0.25,0.5],"rpm":[10000,10100,10200],"speed":[1.5,2,"None"]}',
    "wrapped": b'{"tel":{"time":[0.0,1e-3,2.5E2],"gear":[1,2,3],"dataKey":"x"}}',
    "ahead_leading_sentinels": b'{"DriverAhead":["None",null,"44","44","1"]}',
    "ahead_all_sentinels": b'{"DriverAhead":["None","None",null]}',
    "ahead_escapes": b'{"DriverAhead":["4\\u00e9","4\\u00e9","\\"q\\"","x\\/y"]}',
    "ahead_unicode": '{"DriverAhead":["ü","ü","日"]}'.encode(),
    "ahead_empty": b'{"DriverAhead":[],"time":[1]}',
    "numeric_all_sentinels": b'{"DistanceToDriverAhead":["None","None"],"time":[1,2]}',
    "whitespace": b'{ "time" : [ 1 , 2.5 ] ,\n "DriverAhead" : [ "1" , null ] }',
}


@native
class TestNativeDecoder:
    @pytest.mark.parametrize("name", sorted(NATIVE_PAYLOADS))
    def test_decodes_like_orjson(self, name):
        raw = NATIVE_PAYLOADS[name]
        decoded = json_utils.decode_telemetry_payload(raw)
        expected = orjson.loads(raw)
        _assert_same_payload(decoded, expected)

    def test_many_categories_use_the_hash_index(self):
        values = [str(i % 50) for i in range(400)] + [None, "None"]
        raw = orjson.dumps({"DriverAhead": values})
        channel = json_utils.decode_telemetry_payload(raw)["DriverAhead"]
        assert isinstance(channel, DictionaryChannel)
        assert len(channel.categories) == 51
        assert channel.tolist() == values

    def test_unsupported_shapes_fall_back_to_orjson(self):
        raw = b'{"DriverAhead":[1,"x"],"brake":[true,false]}'
        assert json_utils.decode_telemetry_payload(raw) == orjson.loads(raw)

    def test_old_call_shape_still_returns_lists(self):
        raw = NATIVE_PAYLOADS["ahead_leading_sentinels"]
        legacy = json_utils._native_decode_telemetry(raw, json_utils._NATIVE_NUMERIC_KEYS)
        assert legacy["DriverAhead"] == ["None", None, "44", "44", "1"]
        codes, categories = json_utils._native_decode_telemetry(raw, [], ["DriverAhead"])[
            "DriverAhead"
        ]
        assert codes.dtype == np.int32
        assert codes.tolist() == [0, -1, 1, 1, 2]
        assert categories == ["None", "44", "1"]

    @pytest.mark.parametrize("lib", ["pandas", "polars"])
    @pytest.mark.parametrize("case", sorted(AHEAD_CASES))
    def test_frames_match_orjson_frames(self, lib, case):
        if lib == "polars":
            pytest.importorskip("polars")
        raw = orjson.dumps(_payload(len(AHEAD_CASES[case]), AHEAD_CASES[case], native=False))
        ref = _create_telemetry_df(orjson.loads(raw), "LEC", 7, lib, clock=CLOCK)
        got = _create_telemetry_df(
            json_utils.decode_telemetry_payload(raw), "LEC", 7, lib, clock=CLOCK
        )
        if lib == "polars":
            from polars.testing import assert_frame_equal

            assert_frame_equal(ref, got, check_exact=True)
        else:
            pd.testing.assert_frame_equal(ref, got, check_exact=True)


def _assert_same_payload(decoded, expected) -> None:
    """``decoded`` (native shapes) holds the values orjson produced (NaN for sentinels)."""
    assert isinstance(decoded, dict)
    assert list(decoded) == list(expected)
    for key, value in expected.items():
        got = decoded[key]
        if isinstance(value, dict):
            _assert_same_payload(got, value)
        elif isinstance(got, DictionaryChannel):
            assert got.tolist() == value, key
        elif isinstance(got, np.ndarray):
            want = [np.nan if v in ("None", None) else v for v in value]
            assert np.array_equal(got, np.array(want, dtype=got.dtype), equal_nan=True), key
            assert got.dtype == (np.int64 if all(type(v) is int for v in value) else np.float64)
        else:
            assert got == value, key
