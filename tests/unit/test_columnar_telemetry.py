"""Per-driver columnar telemetry files (Gortyn G3): the codec and the reader."""

from __future__ import annotations

import asyncio
from typing import Any

import numpy as np
import orjson
import pandas as pd
import polars as pl
import pytest

from tif1 import async_fetch
from tif1.config import get_config
from tif1.core_utils import columnar_telemetry as ct
from tif1.core_utils.helpers import _create_telemetry_df
from tif1.core_utils.json_utils import DictionaryChannel, decode_telemetry_payload
from tif1.payload_loader import InMemoryTransport, get_url_loader

# -- codec -----------------------------------------------------------------------


def _lap(seed: int, n: int = 40) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    time_ms = np.cumsum(rng.integers(1, 300, n))
    rpm = rng.integers(4000, 12000, n).astype(float)
    rpm[1::2] += rng.random(n // 2)  # interpolated samples between whole ones
    speed = rpm / 41.0
    speed[3] = np.nan  # an upstream "None"
    distance = np.cumsum(rng.random(n) * 10)
    distance[0] = -0.0
    codes = rng.integers(-1, 3, n).astype(np.int32)
    return {
        "time": time_ms / 1000.0,
        "rpm": rpm,
        "speed": speed,
        "gear": rng.integers(1, 9, n),
        "brake": rng.integers(0, 2, n),
        "distance": distance,
        "rel_distance": distance / 5412.3,
        "DriverAhead": DictionaryChannel(codes, ["1", "None", "44"]),
        "DistanceToDriverAhead": np.full(n, np.nan),
        "big": rng.integers(-(2**40), 2**40, n),
        "acc_x": rng.normal(size=n) * 1e-300,
        "status": ["a", None] * (n // 2),
        "dataKey": f"2025-Test-Race-VER-{seed}",
    }


def _assert_same_tel(expected: dict[str, Any], actual: dict[str, Any]) -> None:
    assert list(actual) == list(expected)
    for key, value in expected.items():
        got = actual[key]
        if isinstance(value, np.ndarray):
            assert isinstance(got, np.ndarray), key
            assert got.dtype == value.dtype, key
            assert got.view(np.uint8).tobytes() == value.view(np.uint8).tobytes(), key
        elif isinstance(value, DictionaryChannel):
            assert isinstance(got, DictionaryChannel), key
            assert got.categories == value.categories
            assert got.codes.dtype == np.int32
            assert np.array_equal(got.codes, value.codes)
        else:
            assert got == value, key
            assert type(got) is type(value), key


def test_round_trip_is_exact():
    laps = {lap: _lap(lap) for lap in (1, 2, 3, 7)}
    # Key order differs per lap and a lap may lack a channel.
    laps[2] = dict(reversed(list(laps[2].items())))
    del laps[3]["rpm"]
    decoded = ct.decode_driver_file(ct.encode_driver_file(laps))
    assert sorted(decoded) == [1, 2, 3, 7]
    for lap, tel in laps.items():
        _assert_same_tel(tel, decoded[lap])


def test_a_channel_with_both_dtypes_and_an_empty_lap_round_trip():
    laps: dict[int, dict[str, Any]] = {
        1: {"speed": np.array([1.0, 2.5])},
        2: {"speed": np.array([3, 4], dtype=np.int64)},
        3: {"speed": np.array([], dtype=np.float64), "dataKey": None},
    }
    decoded = ct.decode_driver_file(ct.encode_driver_file(laps))
    for lap, tel in laps.items():
        _assert_same_tel(tel, decoded[lap])


def test_encodings_chosen_per_column():
    blob = ct.encode_driver_file({1: _lap(1, 400)})
    body = ct._decompress(blob)
    (size,) = ct._HEADER_SIZE.unpack_from(body, 0)
    header = orjson.loads(bytes(body[4 : 4 + size]))
    encodings = {(c["key"], c["kind"]): c["enc"] for c in header["columns"]}
    assert encodings[("time", "f8")] == "dec"
    assert encodings[("rpm", "f8")] == "split"
    assert encodings[("acc_x", "f8")] == "f64"
    assert encodings[("gear", "i8")] == "int"
    assert encodings[("DriverAhead", "dict")] == "int"


def test_frames_from_the_file_equal_the_frames_from_the_json():
    payload = {
        "tel": {
            "time": [0.0, 0.002, 0.023, 0.182],
            "rpm": [8992.0, 9476.0, 9650.866666666667, 9720.0],
            "speed": [117.0, "None", 128.01666666666668, 130.0],
            "gear": [3, 3, 3, 4],
            "brake": [0, 1, 0, 0],
            "drs": [0, 0, 0, 0],
            "distance": [791.1530555555555, 799.3530555555556, 805.5132539044268, 808.0],
            "DriverAhead": ["31", None, "None", "31"],
            "DistanceToDriverAhead": [None, None, None, None],
            "x": [786.34086655483, 862.0, 914.0, 934.0936413695456],
            "dataKey": "2025-Bahrain Grand Prix-Race-VER-1",
        }
    }
    tel = decode_telemetry_payload(orjson.dumps(payload))["tel"]
    back = ct.decode_driver_file(ct.encode_driver_file({1: tel}))[1]
    for lib in ("pandas", "polars"):
        expected = _create_telemetry_df(tel, "VER", 1, lib)
        actual = _create_telemetry_df(back, "VER", 1, lib)
        if lib == "pandas":
            pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        else:
            assert isinstance(actual, pl.DataFrame)
            assert isinstance(expected, pl.DataFrame)
            assert actual.schema == expected.schema
            assert actual.equals(expected)


def test_driver_files_hold_ten_laps_each():
    assert [ct.chunk_first_lap(lap) for lap in (1, 10, 11, 20, 21, 57)] == [1, 1, 11, 11, 21, 51]
    assert ct.driver_file_path("VER", 11) == "VER/tel.v1.11.bin"
    files = ct.encode_driver_files({lap: {"t": np.arange(3.0) + lap} for lap in range(1, 24)})
    assert list(files) == ["tel.v1.1.bin", "tel.v1.11.bin", "tel.v1.21.bin"]
    assert sorted(ct.decode_driver_file(files["tel.v1.21.bin"])) == [21, 22, 23]


def test_files_decode_concurrently_in_threads():
    """Fetch threads decode files at once: no shared zstd decompressor (Pergamon Q15)."""
    from concurrent.futures import ThreadPoolExecutor

    laps = {lap: _lap(lap, 3000) for lap in range(1, 9)}
    blobs = [ct.encode_driver_file({lap: tel}, level=3) for lap, tel in laps.items()] * 8
    with ThreadPoolExecutor(8) as pool:
        decoded = list(pool.map(ct.decode_driver_file, blobs))
    for result in decoded:
        [(lap, tel)] = result.items()
        _assert_same_tel(laps[lap], tel)


@pytest.mark.parametrize(
    "mangle",
    [
        lambda blob: b"NOTATEL!" + blob[8:],
        lambda blob: blob[:12],
        lambda blob: blob[:-5],
        lambda blob: blob[:8] + (1).to_bytes(8, "little") + blob[16:],
    ],
    ids=["magic", "truncated-head", "truncated-frame", "size"],
)
def test_bad_files_raise_a_format_error(mangle):
    blob = ct.encode_driver_file({1: _lap(1)})
    with pytest.raises(ct.ColumnarFormatError):
        ct.decode_driver_file(mangle(blob))


# -- reader ----------------------------------------------------------------------

YEAR, GP, SESSION = 2025, "Test%20Grand%20Prix", "Race"


class _Response:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def _payload(driver: str, lap: int) -> dict[str, Any]:
    return {
        "tel": {
            "time": [0.0, 0.25, 0.5],
            "speed": [100.0 + lap, 101.5, 102.25],
            "gear": [3, 4, lap % 8],
            "DriverAhead": ["1", "None", driver],
            "dataKey": f"{driver}-{lap}",
        }
    }


class _Stub:
    """Lap JSON over a fake niquests path, columnar files over an InMemoryTransport."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, files: dict[str, Any]) -> None:
        self.json_urls: list[str] = []
        self.transport = InMemoryTransport(files)
        monkeypatch.setattr(get_url_loader(), "transport", self.transport)
        monkeypatch.setattr(async_fetch, "_get_async_session", lambda: object())

        def get_resolved(_client: Any, url: str, _timeout: Any) -> _Response:
            self.json_urls.append(url)
            driver, lap = url.rsplit("/", 2)[-2:]
            body = orjson.dumps(_payload(driver, int(lap.removesuffix("_tel.json"))))
            return _Response(200, body)

        monkeypatch.setattr(async_fetch, "_get_resolved", get_resolved)
        monkeypatch.setitem(get_config()._config, "offline_mode", False)
        monkeypatch.setitem(get_config()._config, "telemetry_columnar", True)
        monkeypatch.setitem(get_config()._config, "cdn_hedge", False)

    @property
    def file_urls(self) -> list[str]:
        return [url for url in self.transport.calls if url.endswith(".bin")]

    def fetch(self, refs: list[tuple[str, int]], **kwargs: Any) -> list[Any]:
        requests = [(YEAR, GP, SESSION, f"{d}/{lap}_tel.json") for d, lap in refs]
        kwargs.setdefault("use_cache", False)
        kwargs.setdefault("write_cache", False)
        kwargs.setdefault("validate_payload", False)
        return asyncio.run(async_fetch.fetch_multiple_async(requests, **kwargs))


def _files_for(driver: str, laps: range) -> dict[str, bytes]:
    tels = {
        lap: decode_telemetry_payload(orjson.dumps(_payload(driver, lap)))["tel"] for lap in laps
    }
    return {
        f"{GP}/{SESSION}/{driver}/{name}": blob
        for name, blob in ct.encode_driver_files(tels, level=3).items()
    }


@pytest.fixture(autouse=True)
def _forget_files():
    async_fetch.reset_driver_files()
    yield
    async_fetch.reset_driver_files()


def _same_payload(expected: dict[str, Any], actual: dict[str, Any]) -> None:
    _assert_same_tel(decode_telemetry_payload(orjson.dumps(expected))["tel"], actual["tel"])


def test_a_wave_takes_its_laps_from_the_driver_files(monkeypatch):
    stub = _Stub(monkeypatch, files=_files_for("VER", range(1, 13)))
    refs = [("VER", lap) for lap in range(1, 13)]
    results = stub.fetch(refs)
    for (driver, lap), result in zip(refs, results, strict=True):
        _same_payload(_payload(driver, lap), result)
    assert stub.json_urls == []
    assert sorted(url.rsplit("/", 1)[1] for url in stub.file_urls) == [
        "tel.v1.1.bin",
        "tel.v1.11.bin",
    ]


def test_a_large_batch_starts_every_file_up_front(monkeypatch):
    files = {**_files_for("VER", range(1, 21)), **_files_for("HAM", range(1, 21))}
    stub = _Stub(monkeypatch, files=files)
    refs = [(d, lap) for d in ("VER", "HAM") for lap in range(1, 21)]
    results = stub.fetch(refs, max_concurrent_requests=3)
    for (driver, lap), result in zip(refs, results, strict=True):
        _same_payload(_payload(driver, lap), result)
    assert stub.json_urls == []
    assert len(stub.file_urls) == 4


def test_a_lone_lap_keeps_its_json(monkeypatch):
    stub = _Stub(monkeypatch, files=_files_for("VER", range(1, 11)))
    [result] = stub.fetch([("VER", 4)])
    _same_payload(_payload("VER", 4), result)
    assert stub.file_urls == []
    assert len(stub.json_urls) == 1


def test_the_stream_flag_takes_a_lone_lap_from_its_file(monkeypatch):
    stub = _Stub(monkeypatch, files=_files_for("VER", range(1, 11)))
    [result] = stub.fetch([("VER", 4)], driver_files=True)
    _same_payload(_payload("VER", 4), result)
    assert stub.json_urls == []
    # A later lap of the same file joins the download already made.
    [result] = stub.fetch([("VER", 5)])
    _same_payload(_payload("VER", 5), result)
    assert len(stub.file_urls) == 1
    assert stub.json_urls == []


def test_a_missing_file_falls_back_to_the_json_once_per_session(monkeypatch):
    stub = _Stub(monkeypatch, files={})
    results = stub.fetch([("VER", 1), ("VER", 2)])
    for lap, result in zip((1, 2), results, strict=True):
        _same_payload(_payload("VER", lap), result)
    assert len(stub.file_urls) == 1  # the primary CDN's 404 decides
    assert len(stub.json_urls) == 2
    stub.fetch([("HAM", 1), ("HAM", 2)])
    assert len(stub.file_urls) == 1  # no other probe for this session


def test_a_missing_file_is_remembered_in_the_missing_tier(monkeypatch):
    stub = _Stub(monkeypatch, files={})
    missing: set[str] = set()

    class _Cache:
        def _get_from_memory(self, _key):
            return None

        def get(self, _key):
            return None

        def is_known_missing(self, key):
            return key in missing

        def record_missing(self, key):
            missing.add(key)

        def discard_missing(self, key):
            missing.discard(key)

    monkeypatch.setattr(async_fetch, "get_cache", _Cache)
    stub.fetch([("VER", 1), ("VER", 2)], use_cache=True)
    assert missing == {
        f"{YEAR}/{GP}/{SESSION}/VER/tel.v1.1.bin",
        f"{YEAR}/{GP}/{SESSION}/{async_fetch.COLUMNAR_SESSION_MARKER}",
    }
    async_fetch.reset_driver_files()  # a new process: only the persisted verdicts
    stub.fetch([("HAM", 1), ("HAM", 2)], use_cache=True)
    assert len(stub.file_urls) == 1  # the session verdict covers every driver


@pytest.mark.parametrize("entry", [b"not a file", RuntimeError, TimeoutError])
def test_an_unusable_file_falls_back_to_the_json(monkeypatch, entry):
    stub = _Stub(monkeypatch, files={f"{GP}/{SESSION}/VER/tel.v1.1.bin": entry})
    results = stub.fetch([("VER", 1), ("VER", 2)])
    for lap, result in zip((1, 2), results, strict=True):
        _same_payload(_payload("VER", lap), result)
    assert len(stub.json_urls) == 2


def test_a_lap_absent_from_its_file_uses_the_json(monkeypatch):
    stub = _Stub(monkeypatch, files=_files_for("VER", range(1, 3)))
    results = stub.fetch([("VER", 1), ("VER", 2), ("VER", 3)])
    _same_payload(_payload("VER", 3), results[2])
    assert [url.rsplit("/", 1)[1] for url in stub.json_urls] == ["3_tel.json"]


@pytest.mark.parametrize("setting", ["off", "validated"])
def test_columnar_off_or_validated_telemetry_reads_only_json(monkeypatch, setting):
    stub = _Stub(monkeypatch, files=_files_for("VER", range(1, 3)))
    if setting == "off":
        monkeypatch.setitem(get_config()._config, "telemetry_columnar", False)
        stub.fetch([("VER", 1), ("VER", 2)])
    else:
        monkeypatch.setitem(get_config()._config, "validate_telemetry", True)
        stub.fetch([("VER", 1), ("VER", 2)], validate_payload=True)
    assert stub.file_urls == []
    assert len(stub.json_urls) == 2


def test_written_payloads_are_the_decoded_laps(monkeypatch):
    stub = _Stub(monkeypatch, files=_files_for("VER", range(1, 3)))
    writes: list[tuple[Any, ...]] = []
    monkeypatch.setattr(async_fetch, "schedule_telemetry_writes", lambda _rows: None)

    class _Cache:
        def set_telemetry(self, *_args):
            raise AssertionError("collected, not written inline")

        def discard_missing(self, _key):
            return None

    monkeypatch.setattr(async_fetch, "get_cache", _Cache)
    stub.fetch([("VER", 1), ("VER", 2)], write_cache=True, telemetry_writes=writes)
    assert [row[:5] for row in writes] == [
        (YEAR, GP, SESSION, "VER", 1),
        (YEAR, GP, SESSION, "VER", 2),
    ]
    _same_payload(_payload("VER", 2), {"tel": writes[1][5]})
