"""``load(telemetry="lazy")``: a driver's laps arrive in one wave on first access.

The lazy load must build the frames the eager load builds, fetch each
driver once, and leave ``fetch_all_laps_telemetry()`` one batch for the
drivers not touched yet. It is the default meaning of ``telemetry=True``
(``load_telemetry_lazy`` True); ``telemetry="eager"`` and
``load_telemetry_lazy=False`` give fastf1's eager load. Every payload is
served from memory.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from tif1 import async_fetch, core
from tif1 import config as config_module
from tif1.config import get_config

from .test_polars_session_wiring import TEL_SAMPLES, _Cdn

ALL_LAPS = [f"{code}/{lap}_tel.json" for code in ("VER", "HAM") for lap in (1, 2, 3)]
VER_LAPS = ["VER/1_tel.json", "VER/2_tel.json", "VER/3_tel.json"]
HAM_LAPS = ["HAM/1_tel.json", "HAM/2_tel.json", "HAM/3_tel.json"]


@pytest.fixture
def cdn(monkeypatch) -> _Cdn:
    served = _Cdn(monkeypatch)
    # The eager batch imports the fetcher from its module at call time.
    monkeypatch.setattr(async_fetch, "fetch_multiple_async", core.fetch_multiple_async)
    # One fetch_multiple_async batch per wave (no stream): these tests count
    # batches. The stream (E10) is covered by test_lazy_telemetry_stream.py.
    monkeypatch.setitem(get_config()._config, "lazy_telemetry_lookahead", 0)
    return served


def _telemetry_batches(cdn: _Cdn) -> list[list[str]]:
    return [batch for batch in cdn.batches if any("_tel" in path for path in batch)]


def _lap(session: core.Session, driver: str, number: int) -> Any:
    laps = session.laps.pick_drivers(driver)
    return next(lap for _, lap in laps.iterlaps() if int(lap["LapNumber"]) == number)


def _assert_same_frame(left: Any, right: Any, lib: str) -> None:
    assert type(left) is type(right)
    if lib == "polars":
        assert left.schema == right.schema
        assert left.equals(right)
    else:
        pd.testing.assert_frame_equal(pd.DataFrame(left), pd.DataFrame(right), check_exact=True)


@pytest.mark.parametrize("lib", ["pandas", "polars"])
class TestLazyLoad:
    def test_load_fetches_no_telemetry(self, cdn, lib):
        session = cdn.session(lib)
        session.load(telemetry="lazy", weather=False, messages=False)
        assert _telemetry_batches(cdn) == []
        assert set(session._lazy_telemetry_refs) == {"VER", "HAM"}

    def test_first_lap_access_fetches_that_lap_then_the_driver_in_one_wave(self, cdn, lib):
        # Naxos N11: a lone first touch fetches one file; the next touch of
        # the driver brings the rest of its laps in one wave.
        session = cdn.session(lib)
        session.load(telemetry="lazy", weather=False, messages=False)
        tel = _lap(session, "VER", 2).telemetry
        assert len(tel) == TEL_SAMPLES
        assert _telemetry_batches(cdn) == []
        assert [url for url in cdn.transport.calls if "_tel" in url][-1].endswith("VER/2_tel.json")
        assert set(session._lazy_telemetry_refs) == {"VER", "HAM"}
        _lap(session, "VER", 3).get_car_data()
        assert _telemetry_batches(cdn) == [["VER/1_tel.json", "VER/3_tel.json"]]
        _lap(session, "VER", 1).get_car_data()
        assert _telemetry_batches(cdn) == [["VER/1_tel.json", "VER/3_tel.json"]]
        assert sum("_tel" in url for url in cdn.transport.calls) == 1
        assert set(session._lazy_telemetry_refs) == {"HAM"}

    def test_merged_telemetry_is_one_wave(self, cdn, lib):
        session = cdn.session(lib)
        session.load(telemetry="lazy", weather=False, messages=False)
        tel = session.laps.pick_drivers("HAM").telemetry
        assert len(tel) == 3 * TEL_SAMPLES
        assert _telemetry_batches(cdn) == [HAM_LAPS]

    def test_a_batch_of_two_drivers_is_one_wave(self, cdn, lib):
        session = cdn.session(lib)
        session.load(telemetry="lazy", weather=False, messages=False)
        frames = session._telemetry_frames_for_refs([("VER", 1), ("HAM", 3)])
        assert set(frames) == {("VER", 1), ("HAM", 3)}
        assert _telemetry_batches(cdn) == [ALL_LAPS]
        assert session._lazy_telemetry_refs == {}

    def test_fetch_all_after_a_touch_fetches_the_rest_in_one_batch(self, cdn, lib):
        session = cdn.session(lib)
        session.load(telemetry="lazy", weather=False, messages=False)
        _ = _lap(session, "VER", 1).telemetry  # a lone first touch: VER/1 alone
        lazy_all = session.fetch_all_laps_telemetry()
        assert _telemetry_batches(cdn) == [["VER/2_tel.json", "VER/3_tel.json", *HAM_LAPS]]
        assert session._lazy_telemetry_refs == {}

        eager = cdn.session(lib)
        eager.load(telemetry="eager", weather=False, messages=False)
        assert _telemetry_batches(cdn)[-1] == ALL_LAPS
        # The same flow: a touched lap's memo entry is the session-wrapped frame.
        _ = _lap(eager, "VER", 1).telemetry
        eager_all = eager.fetch_all_laps_telemetry()
        assert list(lazy_all) == list(eager_all)  # laps-row order, as a full batch
        for ref, frame in eager_all.items():
            _assert_same_frame(frame, lazy_all[ref], lib)

    def test_lap_frames_equal_the_eager_load(self, cdn, lib):
        lazy = cdn.session(lib)
        lazy.load(telemetry="lazy", weather=False, messages=False)
        eager = cdn.session(lib)
        eager.load(telemetry="eager", weather=False, messages=False)
        _assert_same_frame(_lap(eager, "HAM", 2).telemetry, _lap(lazy, "HAM", 2).telemetry, lib)
        _assert_same_frame(
            eager.laps.pick_drivers("VER").telemetry, lazy.laps.pick_drivers("VER").telemetry, lib
        )

    def test_default_load_is_lazy(self, cdn, lib):
        assert get_config().get("load_telemetry_lazy") is True
        session = cdn.session(lib)
        session.load(weather=False, messages=False)
        assert _telemetry_batches(cdn) == []
        assert set(session._lazy_telemetry_refs) == {"VER", "HAM"}
        _ = session.laps.pick_drivers("HAM").telemetry
        assert _telemetry_batches(cdn) == [HAM_LAPS]

    def test_config_key_makes_true_lazy(self, cdn, lib, monkeypatch):
        monkeypatch.setitem(get_config()._config, "load_telemetry_lazy", True)
        session = cdn.session(lib)
        session.load(weather=False, messages=False)
        assert _telemetry_batches(cdn) == []
        _ = session.laps.pick_drivers("HAM").telemetry
        assert _telemetry_batches(cdn) == [HAM_LAPS]

    def test_config_key_false_makes_true_eager(self, cdn, lib, monkeypatch):
        monkeypatch.setitem(get_config()._config, "load_telemetry_lazy", False)
        session = cdn.session(lib)
        session.load(weather=False, messages=False)
        assert _telemetry_batches(cdn) == [ALL_LAPS]
        assert session._lazy_telemetry_refs == {}
        _ = _lap(session, "VER", 1).telemetry
        assert _telemetry_batches(cdn) == [ALL_LAPS]

    def test_eager_load_is_unchanged(self, cdn, lib):
        session = cdn.session(lib)
        session.load(telemetry="eager", weather=False, messages=False)
        assert _telemetry_batches(cdn) == [ALL_LAPS]
        assert session._lazy_telemetry_refs == {}
        _ = _lap(session, "VER", 1).telemetry
        assert _telemetry_batches(cdn) == [ALL_LAPS]

    def test_eager_string_overrides_the_config_key(self, cdn, lib, monkeypatch):
        monkeypatch.setitem(get_config()._config, "load_telemetry_lazy", True)
        session = cdn.session(lib)
        session.load(telemetry="EAGER", weather=False, messages=False)
        assert _telemetry_batches(cdn) == [ALL_LAPS]

    def test_lazy_string_overrides_the_config_key(self, cdn, lib, monkeypatch):
        monkeypatch.setitem(get_config()._config, "load_telemetry_lazy", False)
        session = cdn.session(lib)
        session.load(telemetry="lazy", weather=False, messages=False)
        assert _telemetry_batches(cdn) == []


@pytest.mark.parametrize(("from_cache", "third_wave"), [(False, ["C"]), (True, ["C", "D"])])
def test_third_wave_of_a_cached_session_takes_every_pending_driver(
    cdn, monkeypatch, from_cache, third_wave
):
    """Cached sessions batch the rest on the third wave (Delphi D5, Naxos N2); the CDN never does."""
    session = cdn.session("polars")
    session._lazy_telemetry_refs = {code: [(code, 1), (code, 2)] for code in ("A", "B", "C", "D")}
    # Each driver's lone first touch is behind it: these touches are waves.
    session._lazy_lone_drivers = {"A", "B", "C", "D"}
    waves: list[list[str]] = []
    monkeypatch.setattr(
        session,
        "_fetch_lazy_telemetry_wave",
        lambda refs: waves.append(list(dict.fromkeys(driver for driver, _ in refs))),
    )
    monkeypatch.setattr(session, "_lazy_telemetry_from_cache", lambda: from_cache)
    session._ensure_lazy_telemetry([("A", 2)])
    session._ensure_lazy_telemetry([("A", 1)])  # already fetched: no wave
    session._ensure_lazy_telemetry([("B", 1)])
    session._ensure_lazy_telemetry([("C", 1)])
    assert waves == [["A"], ["B"], third_wave]
    assert set(session._lazy_telemetry_refs) == ({"D"} - set(third_wave))


@pytest.mark.parametrize(
    ("touched", "expected"),
    [
        (["D", "A"], [["D"], ["A"]]),
        (["A", "B"], [["A"], ["B"]]),  # laps-row order too: two drivers read two rows
        (["D", "A", "F", "B"], [["D"], ["A"], ["F", "B", "C", "E", "G"]]),
    ],
)
def test_cached_waves_read_two_drivers_alone(cdn, monkeypatch, touched, expected):
    """Naxos N2: two drivers compared after a warm load read two rows, a third the rest."""
    session = cdn.session("polars")
    codes = ("A", "B", "C", "D", "E", "F", "G")
    session._set_lazy_telemetry_refs([(code, lap) for code in codes for lap in (1, 2)])
    waves: list[list[str]] = []
    monkeypatch.setattr(
        session,
        "_fetch_lazy_telemetry_wave",
        lambda refs: waves.append(list(dict.fromkeys(driver for driver, _ in refs))),
    )
    monkeypatch.setattr(session, "_lazy_telemetry_from_cache", lambda: True)
    for code in touched:
        session._ensure_lazy_telemetry([(code, 2)])
    assert waves == expected
    assert set(session._lazy_telemetry_refs) == set(codes) - {d for wave in waves for d in wave}


def test_unknown_telemetry_string_raises(cdn):
    session = cdn.session("pandas")
    with pytest.raises(ValueError, match="'lazy' or 'eager'"):
        session.load(telemetry="later", weather=False, messages=False)


def test_lazy_load_of_a_session_without_laps_registers_nothing(cdn, monkeypatch):
    session = cdn.session("pandas")
    monkeypatch.setattr(core.Session, "laps", property(lambda _self: pd.DataFrame()))
    session.load(weather=False, messages=False)
    assert not getattr(session, "_lazy_telemetry_refs", None)
    assert _telemetry_batches(cdn) == []


async def test_load_async_default_defers_the_telemetry(cdn):
    session = cdn.session("pandas")
    await session.load_async(weather=False, messages=False)
    assert _telemetry_batches(cdn) == []
    assert set(session._lazy_telemetry_refs) == {"VER", "HAM"}


async def test_load_async_lazy_defers_the_telemetry(cdn):
    session = cdn.session("pandas")
    await session.load_async(telemetry="lazy", weather=False, messages=False)
    assert _telemetry_batches(cdn) == []
    assert set(session._lazy_telemetry_refs) == {"VER", "HAM"}


@pytest.mark.parametrize(
    ("value", "expected"), [(None, True), ("true", True), ("0", False), ("false", False)]
)
def test_env_var_sets_the_config_key(monkeypatch, tmp_path, value, expected):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(config_module.Path, "home", staticmethod(lambda: tmp_path))
    if value is None:
        monkeypatch.delenv("TIF1_LOAD_TELEMETRY_LAZY", raising=False)
    else:
        monkeypatch.setenv("TIF1_LOAD_TELEMETRY_LAZY", value)
    monkeypatch.setattr(config_module.Config, "_instance", None)
    assert config_module.Config().get("load_telemetry_lazy") is expected


def test_memoized_frames_and_tier_hits_keep_laps_row_order(cdn, monkeypatch):
    # A warm lazy session: the touched driver (HAM) is memoized, the tier
    # serves the rest; fetch_all's map must still follow the laps rows.
    session = cdn.session("pandas")
    lap_refs = [("VER", 1), ("VER", 2), ("HAM", 1), ("HAM", 2)]
    for ref in lap_refs[2:]:
        session._memo.set("telemetry_df", ref, pd.DataFrame({"Speed": [1.0]}))

    class _Tier:
        def get_telemetry_frames_batch(self, *args, **_kwargs):
            year_gp_session_refs = args
            return {ref: pd.DataFrame({"Speed": [2.0]}) for ref in year_gp_session_refs[3]}

    monkeypatch.setattr(core, "get_cache", lambda: _Tier())
    monkeypatch.setattr(session, "_session_cache_available", lambda: True)
    monkeypatch.setattr(session, "_adopt_telemetry_frame", lambda frame, _driver: frame)
    session.enable_cache = True
    frames, pending = session._memoized_telemetry(lap_refs, False)
    assert pending == []
    assert list(frames) == lap_refs
