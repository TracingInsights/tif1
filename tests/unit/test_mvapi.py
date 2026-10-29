"""Unit tests for ``tif1.mvapi`` (the MultiViewer client) through an in-memory transport."""

from __future__ import annotations

import copy
import math
import time
from typing import Any

import pandas as pd
import pytest

import tif1.mvapi
from tif1.cache import get_cache
from tif1.config import get_config
from tif1.exceptions import DataNotFoundError, InvalidDataError, NetworkError
from tif1.mvapi import api as api_mod
from tif1.mvapi import get_circuit, get_circuit_info, get_circuits_index
from tif1.mvapi.api import (
    CIRCUITS_INDEX_CACHE_KEY,
    CIRCUITS_INDEX_URL,
    circuit_cache_key,
    circuit_url,
)

MV_PAYLOAD: dict[str, Any] = {
    "circuitKey": 63,
    "circuitName": "Sakhir",
    "corners": [
        {"angle": -246.6, "length": 7045.7, "number": 1, "trackPosition": {"x": 42.4, "y": 8329.2}},
        {
            "angle": -101.2,
            "length": 7988.4,
            "number": 2,
            "letter": "A",
            "trackPosition": {"x": 820.8, "y": 7879.0},
        },
    ],
    "countryIocCode": "BRN",
    "countryKey": 36,
    "countryName": "Bahrain",
    "location": "Sakhir",
    "marshalLights": [
        {
            "angle": -182.6,
            "length": 52869.5,
            "number": 1,
            "trackPosition": {"x": -403.0, "y": 819.3},
        }
    ],
    "marshalSectors": [
        {
            "angle": -183.0,
            "length": 53069.8,
            "number": 1,
            "trackPosition": {"x": -393.2, "y": 1019.2},
        }
    ],
    "meetingKey": "1124",
    "meetingName": "Bahrain Grand Prix",
    "miniSectorsIndexes": [20, 38],
    "pitLoss": {"normal": "23.84", "sc": "15.46", "vsc": "16.43"},
    "raceDate": "2022-03-20",
    "rotation": 92,
    "round": 1,
    "x": [-375, -372],
    "y": [1344, 1443],
    "year": 2022,
}

MV_INDEX: dict[str, Any] = {
    "63": {
        "name": "Sakhir",
        "country": "Bahrain",
        "years": [2019, 2020, 2021, 2022],
        "circuitKey": 63,
        "countryKey": 36,
        "iocCountryCode": "BRN",
        "pitLoss": {"normal": "23.84", "sc": "15.46", "vsc": "16.43"},
    }
}

URL_63_2024 = circuit_url(year=2024, circuit_key=63)
KEY_63_2024 = circuit_cache_key(year=2024, circuit_key=63)


@pytest.fixture
def mv_transport(monkeypatch):
    """Route every MultiViewer request through an in-memory transport."""
    from tif1.payload_loader import InMemoryTransport, PayloadLoader

    transport = InMemoryTransport()
    monkeypatch.setattr(api_mod, "get_url_loader", lambda: PayloadLoader(transport=transport))
    return transport


def _set_offline(monkeypatch, value: bool) -> None:
    monkeypatch.setitem(get_config()._config, "offline_mode", value)


class TestUrls:
    def test_circuit_url_and_cache_key(self):
        assert URL_63_2024 == "https://api.multiviewer.app/api/v1/circuits/63/2024"
        assert KEY_63_2024 == "mvapi/circuits/63/2024.json"
        assert CIRCUITS_INDEX_URL == "https://api.multiviewer.app/api/v1/circuits"
        assert CIRCUITS_INDEX_CACHE_KEY == "mvapi/circuits/index.json"
        assert "raw.githubusercontent.com" not in api_mod.BASE_URL

    def test_user_agent_identifies_tif1(self):
        """The shared transport sends ``tif1/{version}``; ``get_url`` takes no headers."""
        from tif1.http_session import user_agent

        assert user_agent().startswith("tif1/")

    def test_package_exports(self):
        from tif1.models import CircuitInfo

        assert tif1.mvapi.CircuitInfo is CircuitInfo
        assert set(tif1.mvapi.__all__) == {
            "CircuitInfo",
            "get_circuit",
            "get_circuit_info",
            "get_circuits_index",
        }


class TestGetCircuit:
    def test_fetches_once_and_caches_permanently(self, mv_transport):
        mv_transport.add(URL_63_2024, MV_PAYLOAD)

        data = get_circuit(year=2024, circuit_key=63)

        assert data == MV_PAYLOAD
        assert mv_transport.calls == [URL_63_2024]
        row = get_cache().get_entry("json", KEY_63_2024)
        assert row["payload"] == MV_PAYLOAD
        assert abs(row["fetched_at"] - time.time()) < 60

        # Second call: served from the cache, no request.
        mv_transport.default = NetworkError(url="unused", status_code=None)
        assert get_circuit(year=2024, circuit_key=63) == MV_PAYLOAD
        assert mv_transport.calls == [URL_63_2024]

    def test_old_cache_row_never_expires(self, mv_transport):
        get_cache().set_entry("json", KEY_63_2024, {"fetched_at": 0.0, "payload": MV_PAYLOAD})
        mv_transport.default = NetworkError(url="unused", status_code=None)

        assert get_circuit(year=2024, circuit_key=63) == MV_PAYLOAD
        assert mv_transport.calls == []

    def test_malformed_cache_row_is_refetched(self, mv_transport):
        get_cache().set_entry("json", KEY_63_2024, {"payload": "nope"})
        mv_transport.add(URL_63_2024, MV_PAYLOAD)

        assert get_circuit(year=2024, circuit_key=63) == MV_PAYLOAD
        assert mv_transport.calls == [URL_63_2024]

    @pytest.mark.parametrize(
        "entry",
        [
            DataNotFoundError(url=URL_63_2024),
            NetworkError(url=URL_63_2024, status_code=503),
            InvalidDataError(reason="bad json"),
            ValueError("boom"),
            TypeError("boom"),
            ["not", "a", "dict"],
        ],
    )
    def test_failures_return_none_and_cache_nothing(self, mv_transport, entry):
        mv_transport.add(URL_63_2024, entry)

        assert get_circuit(year=2024, circuit_key=63) is None
        assert mv_transport.calls == [URL_63_2024]
        assert get_cache().get_entry("json", KEY_63_2024) is None

    def test_unknown_circuit_is_none(self, mv_transport):
        # No mapping: the transport raises DataNotFoundError (a 404).
        assert get_circuit(year=2024, circuit_key=999) is None
        assert mv_transport.calls == [circuit_url(year=2024, circuit_key=999)]

    def test_offline_mode_uses_only_the_cache(self, mv_transport, monkeypatch):
        _set_offline(monkeypatch, True)
        mv_transport.add(URL_63_2024, MV_PAYLOAD)

        assert get_circuit(year=2024, circuit_key=63) is None
        assert mv_transport.calls == []

        get_cache().set_entry("json", KEY_63_2024, {"fetched_at": 1.0, "payload": MV_PAYLOAD})
        assert get_circuit(year=2024, circuit_key=63) == MV_PAYLOAD
        assert mv_transport.calls == []

    def test_logs_failures_at_info(self, mv_transport, caplog):
        mv_transport.add(URL_63_2024, NetworkError(url=URL_63_2024, status_code=None))
        with caplog.at_level("INFO", logger="tif1.mvapi.api"):
            assert get_circuit(year=2024, circuit_key=63) is None
        assert any(
            record.levelname == "INFO" and "MultiViewer" in record.message
            for record in caplog.records
        )


class TestGetCircuitsIndex:
    def test_fetches_and_caches_with_ttl(self, mv_transport, monkeypatch):
        monkeypatch.setitem(get_config()._config, "schedule_ttl_hours", 1.0)
        mv_transport.add(CIRCUITS_INDEX_URL, MV_INDEX)

        assert get_circuits_index() == MV_INDEX
        assert mv_transport.calls == [CIRCUITS_INDEX_URL]
        row = get_cache().get_entry("json", CIRCUITS_INDEX_CACHE_KEY)
        assert row["payload"] == MV_INDEX

        # Fresh row: no request.
        assert get_circuits_index() == MV_INDEX
        assert mv_transport.calls == [CIRCUITS_INDEX_URL]

        # Expired row: refetched.
        get_cache().set_entry(
            "json",
            CIRCUITS_INDEX_CACHE_KEY,
            {"fetched_at": time.time() - 2 * 3600, "payload": {"stale": {}}},
        )
        assert get_circuits_index() == MV_INDEX
        assert mv_transport.calls == [CIRCUITS_INDEX_URL, CIRCUITS_INDEX_URL]

    def test_failed_refresh_serves_the_stale_row(self, mv_transport, monkeypatch):
        monkeypatch.setitem(get_config()._config, "schedule_ttl_hours", 1.0)
        stale = {"1": {"name": "Old", "country": "X", "years": [2019], "circuitKey": 1}}
        get_cache().set_entry(
            "json",
            CIRCUITS_INDEX_CACHE_KEY,
            {"fetched_at": time.time() - 2 * 3600, "payload": stale},
        )
        mv_transport.default = NetworkError(url="unused", status_code=None)

        assert get_circuits_index() == stale
        assert mv_transport.calls == [CIRCUITS_INDEX_URL]

    def test_nothing_cached_and_fetch_fails(self, mv_transport):
        mv_transport.default = DataNotFoundError(url=CIRCUITS_INDEX_URL)
        assert get_circuits_index() is None

    def test_offline_mode(self, mv_transport, monkeypatch):
        _set_offline(monkeypatch, True)
        mv_transport.add(CIRCUITS_INDEX_URL, MV_INDEX)

        assert get_circuits_index() is None
        assert mv_transport.calls == []

        get_cache().set_entry(
            "json", CIRCUITS_INDEX_CACHE_KEY, {"fetched_at": 0.0, "payload": MV_INDEX}
        )
        assert get_circuits_index() == MV_INDEX  # stale is fine offline
        assert mv_transport.calls == []


class TestGetCircuitInfo:
    def test_parses_like_fastf1_with_extras(self, mv_transport):
        mv_transport.add(URL_63_2024, copy.deepcopy(MV_PAYLOAD))

        info = get_circuit_info(year=2024, circuit_key=63)

        assert info is not None
        assert list(info.corners.columns) == ["X", "Y", "Number", "Letter", "Angle", "Distance"]
        assert info.corners["X"].dtype == "float64"
        assert info.corners["Number"].dtype == "int64"
        assert info.corners["Letter"].dtype == object
        assert info.corners["Number"].tolist() == [1, 2]
        assert info.corners["Letter"].tolist() == ["", "A"]
        assert info.corners["X"].tolist() == [42.4, 820.8]
        assert info.corners["Angle"].tolist() == [-246.6, -101.2]
        assert all(math.isnan(value) for value in info.corners["Distance"])
        assert len(info.marshal_lights) == 1
        assert len(info.marshal_sectors) == 1
        assert info.rotation == 92.0
        assert info.source == "multiviewer"
        assert info.circuit_key == 63
        assert info.circuit_name == "Sakhir"
        assert info.location == "Sakhir"
        assert info.country == "Bahrain"
        assert info.country_code == "BRN"
        assert info.country_key == 36
        assert info.pit_loss == {"normal": 23.84, "sc": 15.46, "vsc": 16.43}
        assert isinstance(info.track, pd.DataFrame)
        assert info.track["X"].tolist() == [-375.0, -372.0]
        assert info.mini_sector_index == [20, 38]

    def test_failure_is_none(self, mv_transport, caplog):
        mv_transport.default = NetworkError(url="unused", status_code=None)
        with caplog.at_level("WARNING", logger="tif1.mvapi.data"):
            assert get_circuit_info(year=2024, circuit_key=63) is None
        assert any("Failed to load circuit info" in record.message for record in caplog.records)

    def test_polars_frames(self, mv_transport):
        pl = pytest.importorskip("polars")
        mv_transport.add(URL_63_2024, copy.deepcopy(MV_PAYLOAD))

        info = get_circuit_info(year=2024, circuit_key=63, lib="polars")

        assert info is not None
        assert isinstance(info.corners, pl.DataFrame)
        assert info.corners.schema["Number"] == pl.Int64
        assert info.corners.schema["Letter"] == pl.String
        assert info.corners["Distance"].null_count() == 2
