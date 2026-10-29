"""Telemetry frame building and the frame tier: the fast paths give the generic results.

Covers the session clock built with the frame (``clock=``), the polars build
from ready numpy arrays (``_polars_typed_telemetry_frame``) against the
generic expression path, and the frame-tier write when the compression pool
refuses work (interpreter shutdown).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pytest

from tif1.cache import Cache
from tif1.core_utils import helpers
from tif1.core_utils.helpers import _create_telemetry_df

pl = pytest.importorskip("polars")

N = 12
START_NS = 395 * 10**9
DATE_NS = pd.Timestamp("2025-04-13 15:01:35").value


def _payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "time": [round(0.27 * i, 3) for i in range(N)],
        "rpm": [10000.0 + 10 * i for i in range(N)],
        "speed": [200.0 + i for i in range(N)],
        "gear": [7] * N,
        "throttle": [99.5] * N,
        "brake": [False] * (N - 2) + [True, True],
        "drs": [0] * N,
        "distance": [14.0 * i for i in range(N)],
        "rel_distance": [i / (N - 1) for i in range(N)],
        "DriverAhead": ["16"] * N,
        "DistanceToDriverAhead": [12.5] * N,
        "acc_x": [0.1] * N,
        "acc_y": [0.2] * N,
        "acc_z": [0.3] * N,
        "x": [float(i) for i in range(N)],
        "y": [1.0] * N,
        "z": [2.0] * N,
        "dataKey": "2025-Bahrain-VER-5",
    }
    payload.update(overrides)
    return payload


PAYLOADS = {
    "clean": _payload(),
    "int_rpm": _payload(rpm=[10000 + i for i in range(N)]),
    "none_sentinels": _payload(speed=["None", *[200.0] * (N - 1)], x=[*[1.0] * (N - 1), "None"]),
    "leader": _payload(DriverAhead=["None"] * N, DistanceToDriverAhead=[None] * N),
    "driver_ahead_partial": _payload(DriverAhead=["None", *["16"] * (N - 1)]),
    "brake_ints": _payload(brake=[0] * (N - 1) + [1]),
    "nan_array": _payload(speed=np.array([np.nan, *[200.0] * (N - 1)])),
    "float_gear": _payload(gear=[7.0] * N),
    "no_x": {k: v for k, v in _payload().items() if k != "x"},
}


def _generic(payload: dict[str, Any], clock: tuple[int, int] | None, monkeypatch) -> Any:
    """The polars frame built by the generic expression path only."""
    with monkeypatch.context() as patch:
        patch.setattr(helpers, "_polars_typed_telemetry_frame", lambda *_a, **_k: None)
        return _create_telemetry_df(payload, "VER", 5, "polars", clock=clock)


@pytest.mark.parametrize("name", list(PAYLOADS))
@pytest.mark.parametrize("clock", [None, (START_NS, DATE_NS), (START_NS, np.iinfo("int64").min)])
def test_polars_typed_build_equals_the_generic_path(name, clock, monkeypatch):
    fast = _create_telemetry_df(PAYLOADS[name], "VER", 5, "polars", clock=clock)
    generic = _generic(PAYLOADS[name], clock, monkeypatch)
    assert fast.schema == generic.schema
    assert fast.equals(generic)


def test_polars_typed_build_takes_the_clean_payloads():
    col_data = {
        helpers.TELEMETRY_RENAME_MAP.get(k, k): v
        for k, v in PAYLOADS["leader"].items()
        if isinstance(v, list)
    }
    helpers._ensure_polars_available()
    frame = helpers._polars_typed_telemetry_frame(col_data, "VER", 5, (START_NS, DATE_NS))
    assert frame is not None
    assert frame.schema["DistanceToDriverAhead"] == pl.Float64
    assert frame.columns[:2] == ["Date", "SessionTime"]


@pytest.mark.parametrize("lib", ["pandas", "polars"])
@pytest.mark.parametrize("name", ["clean", "none_sentinels", "leader"])
def test_clock_built_with_the_frame_is_lap_start_plus_time(lib, name):
    frame = _create_telemetry_df(PAYLOADS[name], "VER", 5, lib, clock=(START_NS, DATE_NS))
    data = frame if lib == "pandas" else frame.to_pandas()
    assert list(data.columns[:2]) == ["Date", "SessionTime"]
    assert (data["SessionTime"] - data["Time"]).eq(pd.Timedelta(START_NS, unit="ns")).all()
    assert (data["Date"] - data["Time"]).eq(pd.Timestamp(DATE_NS)).all()
    plain = _create_telemetry_df(PAYLOADS[name], "VER", 5, lib)
    assert "SessionTime" not in plain.columns


def test_frame_write_completes_when_the_pool_refuses_work(tmp_path, monkeypatch):
    """At interpreter shutdown concurrent.futures refuses work before the writer is joined."""

    class _ShutDownPool:
        def submit(self, *_args, **_kwargs):
            raise RuntimeError("cannot schedule new futures after interpreter shutdown")

        def shutdown(self, *_args, **_kwargs):
            return None

    import tif1.cache as cache_module

    monkeypatch.setattr(cache_module, "_frame_compress_pool", lambda _n: _ShutDownPool())
    cache = Cache(tmp_path)
    frames = [
        ("VER", lap, _create_telemetry_df(PAYLOADS["clean"], "VER", lap, "pandas"))
        for lap in (1, 2)
    ] + [("HAM", 1, _create_telemetry_df(PAYLOADS["clean"], "HAM", 1, "pandas"))]
    written = cache.set_telemetry_frames_batch(2025, "GP", "Race", frames)
    assert written == 3
    stored = cache.get_telemetry_frames_batch(
        2025, "GP", "Race", [("VER", 1), ("VER", 2), ("HAM", 1)], lib="pandas"
    )
    assert set(stored) == {("VER", 1), ("VER", 2), ("HAM", 1)}
    pd.testing.assert_frame_equal(
        pd.DataFrame(stored[("VER", 2)]), pd.DataFrame(frames[1][2]), check_dtype=True
    )


@pytest.mark.usefixtures("ergast_offline")
@pytest.mark.parametrize("lib", ["pandas", "polars"])
def test_ergast_only_telemetry_is_empty_without_a_request(lib, monkeypatch):
    """Seasons before 2018 have no telemetry files: no batch, no request, fastf1's columns."""
    import tif1
    from tif1 import core
    from tif1.payload_loader import InMemoryTransport

    batch: list[str] = []

    async def fetch_multiple(requests, **_kwargs):
        batch.extend(request[3] for request in requests)
        return [None] * len(requests)

    monkeypatch.setattr(core, "fetch_multiple_async", fetch_multiple)
    session = tif1.get_session(1950, "British Grand Prix", "Race", enable_cache=False, lib=lib)
    number = str(session.drivers[0])
    transport = InMemoryTransport()
    session._payload_loader.transport = transport
    timing_requests = len(batch) + len(transport.calls)

    tel = session.car_data[number]
    fastest = session.get_fastest_lap_tel()
    driver_fastest = session.get_driver(number).get_fastest_lap_tel()
    assert session.fetch_all_laps_telemetry() == {}
    for frame in (tel, fastest, driver_fastest):
        assert len(frame) == 0
        assert list(frame.columns[:2]) == ["Date", "SessionTime"]
        assert frame.session is session
    assert len(batch) + len(transport.calls) == timing_requests
    assert not [path for path in [*batch, *transport.calls] if "_tel.json" in path]


# The session clock puts ``SessionTime`` next to ``Time`` in one 2-D timedelta
# block; the frame codecs once stored only the first row of such a block.
CLOCK = (START_NS, DATE_NS)


def _clocked_frames(driver: str = "VER") -> list[tuple[int, pd.DataFrame]]:
    return [
        (lap, _create_telemetry_df(PAYLOADS[name], driver, lap, "pandas", clock=CLOCK))
        for lap, name in ((1, "clean"), (2, "leader"), (3, "none_sentinels"))
    ]


def _consolidated_frame() -> pd.DataFrame:
    """Two timedelta and two datetime columns, each pair sharing one block."""
    times = np.arange(N, dtype="int64") * 250_000_000
    frame = pd.DataFrame(
        {
            "SessionTime": (times + START_NS).view("timedelta64[ns]"),
            "Speed": np.linspace(200.0, 210.0, N),
            "Time": times.view("timedelta64[ns]"),
            "Date": (times + DATE_NS).view("datetime64[ns]"),
            "Start": (times * 2 + DATE_NS).view("datetime64[ns]"),
        }
    )
    frame = frame._consolidate()
    assert sorted(len(b.mgr_locs) for b in frame._mgr.blocks if b.dtype.kind in "mM") == [2, 2]
    return frame


def _assert_readable_and_equal(got: pd.DataFrame, expected: pd.DataFrame) -> None:
    for column in expected.columns:  # a short block raised IndexError on access
        got[column].to_numpy()
    pd.testing.assert_frame_equal(pd.DataFrame(got), pd.DataFrame(expected), check_exact=True)


def test_clocked_frame_keeps_time_and_session_time_in_one_block():
    """The layout the codec tests below exercise (else they test nothing)."""
    frame = _clocked_frames()[0][1]
    blocks = [b for b in frame._mgr.blocks if b.dtype.kind == "m"]
    assert [list(frame.columns[b.mgr_locs.as_array]) for b in blocks] == [["SessionTime", "Time"]]


@pytest.mark.parametrize("make", [_consolidated_frame, lambda: _clocked_frames()[0][1]])
def test_frame_codec_keeps_every_column_of_a_datetime_block(make):
    from tif1.core_utils.frame_codec import decode_frame, encode_frame

    frame = make()
    blob = encode_frame(frame)
    assert blob is not None
    _assert_readable_and_equal(decode_frame(blob), frame)


def test_driver_codec_keeps_every_column_of_a_datetime_block():
    from tif1.core_utils.frame_codec import (
        DriverFrameSource,
        decode_driver_frames,
        decode_driver_merged,
        encode_driver_frames,
    )

    frames = _clocked_frames()
    blob, leftovers = encode_driver_frames(frames)
    assert blob is not None
    assert leftovers == []
    decoded = decode_driver_frames(bytes(blob))
    source = DriverFrameSource(bytes(blob))
    for lap, frame in frames:
        _assert_readable_and_equal(decoded[lap], frame)
        _assert_readable_and_equal(source.frame(lap), frame)
    merged = decode_driver_merged(bytes(blob), [lap for lap, _ in frames])
    assert merged is not None
    _assert_readable_and_equal(merged, pd.concat([frame for _, frame in frames], ignore_index=True))


def _short_datetime_block(header_specs: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """The specs the old encoders wrote: a 2-column datetime block holding one row."""
    out = []
    for spec in header_specs:
        short = spec[0] == "np" and np.dtype(spec[1]).kind == "m" and len(spec[3]) == 2
        out.append((spec[0], spec[1], 1, *spec[3:]) if short else spec)
    assert out != header_specs
    return out


def test_frame_codec_rejects_a_block_shorter_than_its_placement():
    import pickle

    from tif1.core_utils import frame_codec

    blob = frame_codec.encode_frame(_clocked_frames()[0][1])
    assert blob is not None
    view = memoryview(blob)
    header_len = frame_codec._HEADER_LEN.unpack_from(view, 4)[0]
    columns, n, specs = pickle.loads(view[8 : 8 + header_len])
    header = pickle.dumps((columns, n, _short_datetime_block(specs)), protocol=5)
    corrupt = b"".join(
        (blob[:4], frame_codec._HEADER_LEN.pack(len(header)), header, blob[8 + header_len :])
    )
    with pytest.raises(ValueError, match="placement"):
        frame_codec.decode_frame(corrupt)


def test_driver_codec_rejects_a_block_shorter_than_its_placement():
    import pickle

    from tif1.core_utils import frame_codec

    blob, _ = frame_codec.encode_driver_frames(_clocked_frames())
    assert blob is not None
    raw = bytes(blob)
    header, body_start = frame_codec._driver_header(memoryview(raw))
    header = [
        (columns, laps, lengths, _short_datetime_block(specs))
        for columns, laps, lengths, specs in header
    ]
    header_bytes = pickle.dumps(header, protocol=5)
    corrupt = b"".join(
        (raw[:4], frame_codec._HEADER_LEN.pack(len(header_bytes)), header_bytes, raw[body_start:])
    )
    with pytest.raises(ValueError, match="placement"):
        frame_codec.decode_driver_frames(corrupt)


def test_frame_tier_round_trips_clocked_frames(tmp_path):
    """Every pandas read path of the frame tier: batch, per-driver sources, merged laps."""
    cache = Cache(tmp_path)
    frames = _clocked_frames("VER") + [(4, f) for _, f in _clocked_frames("HAM")[:1]]
    rows = [("VER", lap, frame) for lap, frame in frames[:3]] + [("HAM", 4, frames[3][1])]
    assert cache.set_telemetry_frames_batch(2025, "GP", "Race", rows) == 4
    refs = [(driver, lap) for driver, lap, _ in rows]
    batch = cache.get_telemetry_frames_batch(2025, "GP", "Race", refs, lib="pandas")
    sources = cache.get_telemetry_frame_sources(2025, "GP", "Race", ["VER", "HAM"])
    for driver, lap, frame in rows:
        _assert_readable_and_equal(batch[(driver, lap)], frame)
        _assert_readable_and_equal(sources[driver].frame(lap), frame)
    merged = cache.get_driver_merged_frame(2025, "GP", "Race", "VER", [1, 2, 3])
    assert merged is not None
    _assert_readable_and_equal(
        merged, pd.concat([frame for _, _, frame in rows[:3]], ignore_index=True)
    )
