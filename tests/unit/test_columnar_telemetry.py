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
from tif1.core_utils import json_utils
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


def _header(blob: bytes) -> dict[str, Any]:
    body = ct._decompress(blob)
    (size,) = ct._HEADER_SIZE.unpack_from(body, 0)
    return orjson.loads(bytes(body[4 : 4 + size]))


def _encodings(blob: bytes) -> dict[tuple[str, str], str]:
    return {(c["key"], c["kind"]): c["enc"] for c in _header(blob)["columns"]}


def test_encodings_chosen_per_column():
    encodings = _encodings(ct.encode_driver_file({1: _lap(1, 400)}))
    assert encodings[("time", "f8")] == "dec"
    assert encodings[("rpm", "f8")] == "split"
    # no throttle: no car rows, so speed and distance keep their value encodings
    assert encodings[("speed", "f8")] == "split"
    assert encodings[("distance", "f8")] == "f64"
    assert encodings[("rel_distance", "f8")] == "ratio"
    # random noise: the prediction does not pay, the value encoding is smaller
    assert encodings[("acc_x", "f8")] == "f64"
    assert encodings[("gear", "i8")] == "int"
    assert encodings[("DriverAhead", "dict")] == "int"


_LAP_START_NS = 1_744_556_400_123_000_000  # 2025-04-13T15:00:00.123Z


def _merged_lap(seed: int, n_car: int = 160) -> dict[str, Any]:
    """A lap shaped like fastf1's car/position merge and the pipeline's derived channels.

    Car rows hold whole ``rpm``/``speed``/``throttle``; fastf1 interpolates
    them at the position rows with ``numpy.interp`` over float64 nanosecond
    dates. Position rows hold whole ``x``/``y``/``z``, a quadratic spline
    gives them at the car rows (here the codec's own spline: scipy is not a
    dependency). ``distance`` integrates ``speed`` over the car rows (spline
    at the position rows), ``rel_distance`` divides it by one length per lap,
    and the accelerations follow the pipeline's formulas (with numpy's
    ``arctan2`` and ``convolve``, which the codec does not use).
    """
    rng = np.random.default_rng(seed)
    car_ms = np.cumsum(rng.integers(200, 300, n_car))
    pos_ms = np.setdiff1d(np.sort(rng.choice(car_ms[-1], n_car, replace=False)), car_ms)
    t_ms = np.union1d(car_ms, pos_ms)
    car = np.isin(t_ms, car_ms)
    pos = ~car
    t = t_ms / 1000.0
    dates = (_LAP_START_NS + t_ms * 1_000_000).astype(np.float64)

    def car_channel(values: np.ndarray) -> np.ndarray:
        out = np.interp(dates, dates[car], values)
        out[car] = values
        return out

    def pos_channel(values: np.ndarray) -> np.ndarray:
        out = np.empty(t.size)
        out[pos] = values
        out[car] = ct._spline_lap(dates[pos], values, dates[car])
        return out

    speed_car = np.clip(np.cumsum(rng.integers(-8, 9, n_car)) + 200, 0, 340).astype(float)
    speed_car[5] = np.nan  # an upstream "None"
    rpm = car_channel(rng.integers(8000, 12000, n_car).astype(float))
    speed = car_channel(speed_car)
    throttle = car_channel(rng.integers(0, 101, n_car).astype(float))
    distance_car = np.cumsum(np.nan_to_num(speed_car) / 3.6 * np.diff(car_ms, prepend=0) / 1000)
    distance = np.empty(t.size)
    distance[car] = distance_car
    distance[pos] = ct._spline_lap(dates[car], distance_car, dates[pos])
    pos_n = int(pos.sum())
    x = pos_channel(np.cumsum(rng.integers(-40, 41, pos_n)).astype(float))
    y = pos_channel(np.cumsum(rng.integers(-40, 41, pos_n)).astype(float))
    z = pos_channel(rng.integers(-170, -150, pos_n).astype(float))

    # The accelerations: the pipeline's formulas, a few ULPs away from the
    # codec's predictions (the pipeline used libm's atan2 and fused dot products).
    n = t.size
    layout = ct._Rows([n])

    def ulps_away(values: np.ndarray) -> np.ndarray:
        up = np.nextafter(np.nextafter(values, np.inf), np.inf)
        return np.where(rng.random(n) < 0.5, up, values)

    acc_x = ulps_away(ct._accx_predict(speed, t, layout))
    acc = {
        name: ulps_away(ct._lateral_predict(speed, x, other, distance, layout))
        for name, other in (("acc_y", y), ("acc_z", z))
    }
    return {
        "time": t,
        "rpm": rpm,
        "speed": speed,
        "gear": rng.integers(1, 9, n),
        "throttle": throttle,
        "brake": rng.integers(0, 2, n),
        "drs": np.zeros(n, dtype=np.int64),
        "distance": distance,
        "rel_distance": distance / 5412.3,
        "DriverAhead": DictionaryChannel(rng.integers(-1, 3, n).astype(np.int32), ["1", "None"]),
        "DistanceToDriverAhead": car_channel(rng.random(n_car) * 300),
        "acc_x": acc_x,
        "acc_y": acc["acc_y"],
        "acc_z": acc["acc_z"],
        "x": x,
        "y": y,
        "z": z,
        "dataKey": f"2025-Test-Race-VER-{seed}",
    }


def test_derived_channels_are_stored_as_predictions():
    laps = {lap: _merged_lap(lap) for lap in range(1, 5)}
    blob = ct.encode_driver_file(laps)
    encodings = _encodings(blob)
    for key in ("rpm", "speed", "throttle", "DistanceToDriverAhead"):
        assert encodings[(key, "f8")] == "interp", key
    for key in ("x", "y", "z"):
        assert encodings[(key, "f8")] == "spline", key
    assert encodings[("distance", "f8")] == "integs"
    assert encodings[("rel_distance", "f8")] == "ratio"
    assert encodings[("acc_x", "f8")] == "accx"
    assert encodings[("acc_y", "f8")] == "lat"
    assert encodings[("acc_z", "f8")] == "lat"
    decoded = ct.decode_driver_file(blob)
    for lap, tel in laps.items():
        _assert_same_tel(tel, decoded[lap])


def test_lap_start_dates_are_recovered_modulo_512_ns():
    """The encoder finds the lap start under which numpy.interp reproduces rpm."""
    lap = _merged_lap(5)
    blob = ct.encode_driver_file({1: lap})
    body = ct._decompress(blob)
    (size,) = ct._HEADER_SIZE.unpack_from(body, 0)
    header = orjson.loads(bytes(body[4 : 4 + size]))
    start = 4 + size + (-(4 + size) % 8)
    (found,) = ct._buffer(header["dates"], body[start:]).tolist()
    assert found % 512 == _LAP_START_NS % 512
    # with the true start the rows between car rows are predicted exactly
    rows = ct._Rows([lap["time"].size])
    anchors = ct._Anchors(
        ct._whole(lap["rpm"]) & ct._whole(lap["speed"]) & ct._whole(lap["throttle"]), rows
    )
    dates = ct._row_dates(lap["time"], np.array([found]), rows)
    at, j0, j1 = ct._interp_rows(anchors)
    predicted = ct._interp_predict(dates, lap["rpm"], at, j0, j1)
    # (the rows next to the car row whose speed is NaN, not an anchor, miss)
    assert np.count_nonzero(ct._residuals(lap["rpm"][at], predicted)) <= 0.05 * at.size


def test_without_dates_the_fraction_of_rpm_predicts_the_car_channels(monkeypatch):
    laps = {lap: _merged_lap(lap + 30) for lap in range(1, 3)}
    monkeypatch.setitem(ct._PREDICTED, "speed", [("lerp", {"ref": "rpm"})])
    blob = ct.encode_driver_file(laps, level=3)
    assert _encodings(blob)[("speed", "f8")] == "lerp"
    decoded = ct.decode_driver_file(blob)
    for lap, tel in laps.items():
        _assert_same_tel(tel, decoded[lap])


def test_spline_edge_cases_predict_zero():
    xs = np.array([1.0, 2.0, 3.0, 4.0])
    targets = np.array([1.5, np.nan, 9.0])
    assert ct._spline_lap(xs[:2], xs[:2], targets).tolist() == [0.0, 0.0, 0.0]
    assert ct._spline_lap(xs[::-1].copy(), xs, targets).tolist() == [0.0, 0.0, 0.0]
    out = ct._spline_lap(xs, xs * 2.0, targets)
    assert out[0] == pytest.approx(3.0)
    assert np.isnan(out[1])
    assert out[2] == pytest.approx(18.0)  # extrapolated from the last interval


def test_predictions_shrink_the_file(monkeypatch):
    laps = {lap: _merged_lap(lap) for lap in range(1, 5)}
    predicted = len(ct.encode_driver_file(laps, level=3))
    monkeypatch.setattr(ct, "_PREDICTED", {})
    plain = ct.encode_driver_file(laps, level=3)
    assert set(_encodings(plain).values()) <= {"int", "dec", "split", "f64"}
    assert predicted < 0.8 * len(plain)


def test_numpy_decoder_equals_the_native_kernels(monkeypatch):
    """Without the tif1-native kernels the numpy twins decode the same bits."""
    laps = {lap: _merged_lap(lap + 10) for lap in range(1, 4)}
    blob = ct.encode_driver_file(laps, level=3)
    default = ct.decode_driver_file(blob)
    for name in [name for name in dir(json_utils) if name.startswith("NATIVE_TEL_")]:
        monkeypatch.setattr(json_utils, name, None)
    fallback = ct.decode_driver_file(blob)
    for lap, tel in laps.items():
        _assert_same_tel(tel, default[lap])
        _assert_same_tel(tel, fallback[lap])


def test_native_kernels_compute_the_numpy_predictions(monkeypatch):
    if json_utils.NATIVE_TEL_SPLINE is None:
        pytest.skip("the installed tif1-native has no tel.v2 kernels")
    laps = [_merged_lap(seed, 300) for seed in range(20, 26)]
    cat = {key: np.concatenate([lap[key] for lap in laps]) for key in laps[0] if key != "dataKey"}
    rows = ct._Rows([lap["time"].size for lap in laps] + [0, 1])
    for key in ("speed", "time", "x", "y", "z", "distance"):
        cat[key] = np.append(cat[key], [7.5])  # a lap of one sample, after an empty one
    sizes = rows.sizes
    pairs = [
        (
            ct._accx_predict(cat["speed"], cat["time"], rows),
            json_utils.NATIVE_TEL_ACCX(cat["speed"], cat["time"], sizes),
        )
    ]
    for other in ("y", "z"):
        args = (cat["speed"], cat["x"], cat[other], cat["distance"])
        pairs.append(
            (ct._lateral_predict(*args, rows), json_utils.NATIVE_TEL_LATERAL(*args, sizes))
        )
    dates = ct._row_dates(cat["time"], np.full(sizes.size, _LAP_START_NS), rows)
    for key in ("x", "distance"):
        points = ct._whole(cat[key]) if key == "x" else ~ct._whole(cat["x"])
        targets = np.flatnonzero(~points)
        native_values = ct._spline_predict(dates, cat[key], points, targets, rows)
        with monkeypatch.context() as patch:
            patch.setattr(json_utils, "NATIVE_TEL_SPLINE", None)
            numpy_values = ct._spline_predict(dates, cat[key], points, targets, rows)
        pairs.append((numpy_values, native_values))
    for numpy_values, native_values in pairs:
        assert native_values.view(np.int64).tolist() == numpy_values.view(np.int64).tolist()
    with pytest.raises(ValueError, match="lap sizes"):
        json_utils.NATIVE_TEL_ACCX(cat["speed"], cat["time"], sizes[:-1])
    with pytest.raises(ValueError, match="not ascending"):
        json_utils.NATIVE_TEL_SPLINE(dates, cat["x"], points, targets[::-1].copy(), sizes)


def test_integ_runs_and_single_rows_decode_alike(monkeypatch):
    """The numpy ``integ`` decoder: zero-residual runs, residual rows and NaN sums."""
    monkeypatch.setattr(json_utils, "NATIVE_TEL_INTEG", None)
    lap = _merged_lap(3)
    lap["distance"][40:44] = np.nan  # NaN sums inside a run
    lap["distance"][60] = 0.0
    lap["distance"][61] += 1e-9  # a residual mid-run
    laps = {1: lap, 2: _merged_lap(4)}
    decoded = ct.decode_driver_file(ct.encode_driver_file(laps, level=3))
    for number, tel in laps.items():
        _assert_same_tel(tel, decoded[number])


@pytest.mark.parametrize(
    "values",
    [
        [0.0, -0.0, 1.0, -1.0, np.inf, -np.inf, 5e-324, -5e-324, 1.7976931348623157e308],
        [float.fromhex("0x1.8p+1"), np.nan, -np.nan],
    ],
)
def test_residuals_restore_every_bit_pattern(values):
    actual = np.array(values, dtype=np.float64)
    actual = np.append(actual, np.frombuffer(np.uint64(0x7FF4_0000_0000_0001).tobytes(), "<f8"))
    for predicted in (np.zeros_like(actual), actual[::-1].copy(), np.full_like(actual, -3.25)):
        zigzag = ct._residuals(actual, predicted)
        back = ct._apply_residuals(predicted, zigzag)
        assert back.view(np.uint64).tolist() == actual.view(np.uint64).tolist()


def test_portable_atan2_tracks_numpy():
    rng = np.random.default_rng(7)
    y = rng.normal(size=5000) * 10.0 ** rng.uniform(-6, 6, 5000)
    x = rng.normal(size=5000) * 10.0 ** rng.uniform(-6, 6, 5000)
    ours = ct._atan2(y, x)
    assert np.max(np.abs(ours - np.arctan2(y, x))) < 1e-15
    quadrants = ct._atan2(np.array([1.0, 1.0, -1.0, -1.0]), np.array([1.0, -1.0, -1.0, 1.0]))
    assert np.allclose(quadrants, [np.pi / 4, 3 * np.pi / 4, -3 * np.pi / 4, -np.pi / 4])


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
    assert ct.driver_file_path("VER", 11) == "VER/tel.v2.11.bin"
    files = ct.encode_driver_files({lap: {"t": np.arange(3.0) + lap} for lap in range(1, 24)})
    assert list(files) == ["tel.v2.1.bin", "tel.v2.11.bin", "tel.v2.21.bin"]
    assert sorted(ct.decode_driver_file(files["tel.v2.21.bin"])) == [21, 22, 23]


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
        lambda blob: blob[:7] + b"\x01" + blob[8:],
    ],
    ids=["magic", "truncated-head", "truncated-frame", "size", "version-1"],
)
def test_bad_files_raise_a_format_error(mangle):
    blob = ct.encode_driver_file({1: _lap(1)})
    with pytest.raises(ct.ColumnarFormatError):
        ct.decode_driver_file(mangle(blob))


# -- reader ----------------------------------------------------------------------

YEAR, GP, SESSION = 2025, "Test%20Grand%20Prix", "Race"
_REAL_KERNEL_CHECK = json_utils.native_columnar_decoder_available


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
        # the reader needs tif1-native's tel.v2 kernels; the numpy twins decode
        # the same laps here
        monkeypatch.setattr(json_utils, "native_columnar_decoder_available", lambda: True)
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
        "tel.v2.1.bin",
        "tel.v2.11.bin",
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
        f"{YEAR}/{GP}/{SESSION}/VER/tel.v2.1.bin",
        f"{YEAR}/{GP}/{SESSION}/{async_fetch.COLUMNAR_SESSION_MARKER}",
    }
    async_fetch.reset_driver_files()  # a new process: only the persisted verdicts
    stub.fetch([("HAM", 1), ("HAM", 2)], use_cache=True)
    assert len(stub.file_urls) == 1  # the session verdict covers every driver


@pytest.mark.parametrize("entry", [b"not a file", RuntimeError, TimeoutError])
def test_an_unusable_file_falls_back_to_the_json(monkeypatch, entry):
    stub = _Stub(monkeypatch, files={f"{GP}/{SESSION}/VER/tel.v2.1.bin": entry})
    results = stub.fetch([("VER", 1), ("VER", 2)])
    for lap, result in zip((1, 2), results, strict=True):
        _same_payload(_payload("VER", lap), result)
    assert len(stub.json_urls) == 2


def test_a_lap_absent_from_its_file_uses_the_json(monkeypatch):
    stub = _Stub(monkeypatch, files=_files_for("VER", range(1, 3)))
    results = stub.fetch([("VER", 1), ("VER", 2), ("VER", 3)])
    _same_payload(_payload("VER", 3), results[2])
    assert [url.rsplit("/", 1)[1] for url in stub.json_urls] == ["3_tel.json"]


@pytest.mark.parametrize("setting", ["off", "validated", "no-native-kernels"])
def test_columnar_off_or_validated_telemetry_reads_only_json(monkeypatch, setting):
    stub = _Stub(monkeypatch, files=_files_for("VER", range(1, 3)))
    if setting == "off":
        monkeypatch.setitem(get_config()._config, "telemetry_columnar", False)
        stub.fetch([("VER", 1), ("VER", 2)])
    elif setting == "no-native-kernels":
        monkeypatch.setattr(json_utils, "native_columnar_decoder_available", _REAL_KERNEL_CHECK)
        monkeypatch.setattr(json_utils, "NATIVE_TEL_SPLINE", None)
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
