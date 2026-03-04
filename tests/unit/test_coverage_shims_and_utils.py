"""Coverage-focused tests for shim modules and utility edge branches."""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

import tif1


def test_fastf1_compat_set_log_level_and_cache_shims(monkeypatch, tmp_path):
    from tif1 import fastf1_compat

    fake_cache_obj = MagicMock()
    fake_cache_obj.close = MagicMock()
    fake_cache_obj.clear = MagicMock()
    fake_cache_obj.cache_dir = tmp_path

    import tif1.cache as cache_module

    monkeypatch.setattr(cache_module, "_cache", fake_cache_obj, raising=False)
    monkeypatch.setattr(cache_module, "get_cache", lambda: fake_cache_obj, raising=True)

    result = fastf1_compat.Cache.enable_cache(cache_dir=tmp_path)
    assert result is fake_cache_obj
    fake_cache_obj.close.assert_called_once()

    fastf1_compat.Cache.clear_cache()
    fake_cache_obj.clear.assert_called_once()

    fastf1_compat.set_log_level(logging.INFO)
    assert logging.getLogger("tif1").level == logging.INFO


def test_tif1_root_cache_export_and_fastf1_cache_modes(monkeypatch, tmp_path):
    from tif1 import fastf1_compat
    from tif1.config import get_config

    fake_cache_obj = MagicMock()
    fake_cache_obj.close = MagicMock()
    fake_cache_obj.clear = MagicMock()
    fake_cache_obj.cache_dir = tmp_path

    import tif1.cache as cache_module

    monkeypatch.setattr(cache_module, "_cache", fake_cache_obj, raising=False)
    monkeypatch.setattr(cache_module, "get_cache", lambda: fake_cache_obj, raising=True)

    assert tif1.Cache is fastf1_compat.Cache
    result = tif1.Cache.enable_cache(cache_dir=tmp_path, force_renew=True)
    assert result is fake_cache_obj
    fake_cache_obj.close.assert_called_once()
    fake_cache_obj.clear.assert_called_once()

    cache_path, cache_size = tif1.Cache.get_cache_info()
    assert cache_path == str(tmp_path)
    assert isinstance(cache_size, int)

    cfg = get_config()
    cfg.set("enable_cache", True)
    tif1.Cache.set_disabled()
    assert cfg.get("enable_cache") is False
    tif1.Cache.set_enabled()
    assert cfg.get("enable_cache") is True

    with tif1.Cache.disabled():
        assert cfg.get("enable_cache") is False
    assert cfg.get("enable_cache") is True

    tif1.Cache.offline_mode(True)
    assert cfg.get("offline_mode") is True
    tif1.Cache.offline_mode(False)
    assert cfg.get("offline_mode") is False

    tif1.Cache.ci_mode(True)
    assert cfg.get("ci_mode") is True
    tif1.Cache.ci_mode(False)
    assert cfg.get("ci_mode") is False


def test_lazy_shim_modules_export_core_symbols():
    from tif1 import core, io_pipeline, lap_ops, models, session

    assert session.Session is core.Session
    assert session.get_session is core.get_session
    assert models.Lap is core.Lap
    assert models.Laps is core.Laps
    assert io_pipeline._create_lap_df is core._create_lap_df
    assert lap_ops._coerce_lap_number is core._coerce_lap_number


def test_tif1_lazy_module_attr_and_setup_logging():
    plotting_module = tif1.plotting
    assert plotting_module is not None

    with pytest.raises(AttributeError, match="has no attribute"):
        _ = tif1.__getattr__("__not_real_export__")

    tif1.setup_logging(logging.DEBUG)
    assert logging.getLogger("tif1").level == logging.DEBUG


def test_backend_conversion_import_error_paths(monkeypatch):
    from tif1.core_utils import backend_conversion as conv

    dataframe = pd.DataFrame({"a": [1]})

    monkeypatch.setattr(conv, "POLARS_AVAILABLE", False, raising=True)
    with pytest.raises(ImportError):
        conv.pandas_to_polars(dataframe)


def test_backend_conversion_error_paths(monkeypatch):
    from tif1.core_utils import backend_conversion as conv

    dataframe = pd.DataFrame({"a": [1]})

    class _BadPl:
        @staticmethod
        def from_pandas(*_args, **_kwargs):
            raise RuntimeError("boom")

        class DataFrame:
            pass

    monkeypatch.setattr(conv, "POLARS_AVAILABLE", True, raising=True)
    monkeypatch.setattr(conv, "pl", _BadPl, raising=True)

    with pytest.raises(ValueError, match="Failed to convert pandas DataFrame to polars"):
        conv.pandas_to_polars(dataframe)

    bad_df = SimpleNamespace(
        to_pandas=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))  # noqa: ARG005
    )
    with pytest.raises(ValueError, match="Failed to convert polars DataFrame to pandas"):
        conv.polars_to_pandas(bad_df)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Invalid target_backend"):
        conv.convert_backend(dataframe, "duckdb")

    with pytest.raises(ValueError, match="Cannot convert"):
        conv.convert_backend({"not": "df"}, "pandas")  # type: ignore[arg-type]


def test_json_utils_fallback_paths(monkeypatch):
    from tif1.core_utils import json_utils

    monkeypatch.setattr(
        json_utils,
        "_ORJSON",
        SimpleNamespace(
            loads=lambda obj: (_ for _ in ()).throw(RuntimeError("fail")),  # noqa: ARG005
            dumps=lambda obj: (_ for _ in ()).throw(RuntimeError("fail")),  # noqa: ARG005
        ),
        raising=True,
    )

    assert json_utils.json_loads('{"x":1}') == {"x": 1}
    assert json_utils.json_dumps({"x": 1}) == '{"x": 1}'

    response = MagicMock()
    response.content = b"not-json"
    response.json.return_value = {"fallback": True}
    assert json_utils.parse_response_json(response) == {"fallback": True}


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        (None, "must be an object"),
        ({"schema_version": 1, "years": []}, "missing 'years'"),
        ({"schema_version": 1, "years": {2025: {}}}, "Invalid year key"),
        ({"schema_version": 1, "years": {"2025": "bad"}}, "must be object"),
        ({"schema_version": 1, "years": {"2025": {"events": "bad", "sessions": {}}}}, "events"),
        (
            {"schema_version": 1, "years": {"2025": {"events": ["A"], "sessions": []}}},
            "sessions map",
        ),
    ],
)
def test_schedule_schema_invalid_shapes(payload, match):
    from tif1.exceptions import InvalidDataError
    from tif1.schedule_schema import validate_schedule_payload

    with pytest.raises(InvalidDataError, match=match):
        validate_schedule_payload(payload)


def test_helpers_edge_paths():
    from tif1.core_utils.helpers import (
        _create_telemetry_df,
        _filter_valid_laptimes,
        _is_empty_df,
        _normalize_row_iteration,
        _rename_columns,
    )

    class _LenOnly:
        def __len__(self):
            return 0

    class _EmptyAttr:
        empty = True

    assert _is_empty_df(_LenOnly(), "pandas") is True
    assert _is_empty_df(_EmptyAttr(), "polars") is True

    laps = pd.DataFrame({"Driver": ["VER"]})
    same = _filter_valid_laptimes(laps, "polars")
    assert same is laps

    dropped = _rename_columns(pd.DataFrame({"a": [1], "b": [2]}), {"a": None, "b": "B"}, "pandas")
    assert list(dropped.columns) == ["B"]

    non_list = _create_telemetry_df({"data_key": "k1"}, "VER", 1, "pandas")
    assert non_list is None

    rows = list(_normalize_row_iteration(pd.DataFrame({"x": [1]}), "pandas"))
    assert rows[0]["x"] == 1
