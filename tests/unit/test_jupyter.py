"""Tests for jupyter module."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tif1.jupyter import (
    JupyterDisplayMixin,
    _is_notebook,
    display_dataframe_summary,
    display_driver_info,
    display_lap_info,
    display_session_info,
    enable_jupyter_display,
)


class TestIsNotebook:
    def test_returns_false_in_test_environment(self):
        assert _is_notebook() is False

    def test_returns_false_when_get_ipython_missing(self):
        mock_module = MagicMock(spec=[])
        with patch("importlib.import_module", return_value=mock_module):
            assert _is_notebook() is False

    def test_returns_false_when_shell_is_none(self):
        mock_module = MagicMock()
        mock_module.get_ipython.return_value = None
        with patch("importlib.import_module", return_value=mock_module):
            assert _is_notebook() is False

    def test_returns_true_when_ipkernel_in_config(self):
        mock_shell = MagicMock()
        mock_shell.config = {"IPKernelApp": True}
        mock_module = MagicMock()
        mock_module.get_ipython.return_value = mock_shell
        with patch("importlib.import_module", return_value=mock_module):
            assert _is_notebook() is True

    def test_returns_false_when_ipkernel_not_in_config(self):
        mock_shell = MagicMock()
        mock_shell.config = {}
        mock_module = MagicMock()
        mock_module.get_ipython.return_value = mock_shell
        with patch("importlib.import_module", return_value=mock_module):
            assert _is_notebook() is False

    def test_returns_false_on_import_error(self):
        with patch("importlib.import_module", side_effect=ImportError("no IPython")):
            assert _is_notebook() is False

    def test_returns_false_on_generic_exception(self):
        with patch("importlib.import_module", side_effect=RuntimeError("boom")):
            assert _is_notebook() is False


class TestJupyterDisplayMixin:
    def test_repr_html_returns_repr_when_not_notebook(self):
        class MyObj(JupyterDisplayMixin):
            def __repr__(self):
                return "MyObj()"

        obj = MyObj()
        assert obj._repr_html_() == "MyObj()"

    def test_repr_html_calls_generate_html_in_notebook(self):
        class MyObj(JupyterDisplayMixin):
            def _generate_html(self):
                return "<b>hello</b>"

        obj = MyObj()
        with patch("tif1.jupyter._is_notebook", return_value=True):
            assert obj._repr_html_() == "<b>hello</b>"

    def test_repr_html_falls_back_on_generate_html_error(self):
        class MyObj(JupyterDisplayMixin):
            def __repr__(self):
                return "MyObj(fallback)"

            def _generate_html(self):
                raise ValueError("render failed")

        obj = MyObj()
        with patch("tif1.jupyter._is_notebook", return_value=True):
            assert obj._repr_html_() == "MyObj(fallback)"

    def test_generate_html_raises_not_implemented(self):
        obj = JupyterDisplayMixin()
        with pytest.raises(NotImplementedError):
            obj._generate_html()


class TestDisplaySessionInfo:
    def test_renders_session_html(self):
        session = SimpleNamespace(
            year=2025,
            gp="Saudi%20Arabia",
            session="Free%20Practice%201",
            lib="pandas",
            drivers=["VER", "HAM", "LEC"],
            _drivers=True,
        )
        html = display_session_info(session)
        assert "2025" in html
        assert "Saudi Arabia" in html
        assert "Free Practice 1" in html
        assert "pandas" in html
        assert "3" in html

    def test_renders_not_loaded_when_drivers_not_loaded(self):
        session = SimpleNamespace(
            year=2024,
            gp="Monza",
            session="Race",
            lib="pandas",
            drivers=[],
            _drivers=None,
        )
        html = display_session_info(session)
        assert "Not loaded" in html


class TestDisplayDriverInfo:
    def test_renders_driver_html(self):
        session = SimpleNamespace(year=2025, gp="Monaco", session="Qualifying")
        driver = SimpleNamespace(
            driver="VER",
            session=session,
            _laps=[1, 2, 3],
        )
        html = display_driver_info(driver)
        assert "VER" in html
        assert "2025" in html
        assert "Monaco" in html
        assert "Qualifying" in html
        assert "Yes" in html

    def test_renders_no_laps(self):
        session = SimpleNamespace(year=2024, gp="Spa", session="Race")
        driver = SimpleNamespace(driver="HAM", session=session, _laps=None)
        html = display_driver_info(driver)
        assert "No" in html


class TestDisplayLapInfo:
    def test_renders_lap_html(self):
        session = SimpleNamespace(year=2025, gp="Silverstone", session="Race")
        lap = SimpleNamespace(
            lap_number=42,
            driver="NOR",
            session=session,
            _telemetry={"speed": [300]},
        )
        html = display_lap_info(lap)
        assert "42" in html
        assert "NOR" in html
        assert "2025" in html
        assert "Silverstone" in html
        assert "Yes" in html

    def test_renders_no_telemetry(self):
        session = SimpleNamespace(year=2024, gp="Monza", session="Sprint")
        lap = SimpleNamespace(lap_number=1, driver="LEC", session=session, _telemetry=None)
        html = display_lap_info(lap)
        assert "No" in html


class TestDisplayDataframeSummary:
    def test_pandas_dataframe(self):
        dataframe = pd.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        html = display_dataframe_summary(dataframe)
        assert "3" in html
        assert "2 columns" in html
        assert "MB" in html

    def test_polars_like_dataframe(self):
        mock_df = MagicMock()
        mock_df.shape = (100, 5)
        mock_df.estimated_size.return_value = 2048.0
        # Ensure it's not recognized as pandas
        with patch.object(pd.DataFrame, "__instancecheck__", return_value=False):
            html = display_dataframe_summary(mock_df)
        assert "100" in html
        assert "5 columns" in html
        assert "MB" in html

    def test_polars_like_without_estimated_size(self):
        mock_df = MagicMock(spec=["shape"])
        mock_df.shape = (50, 3)
        with patch.object(pd.DataFrame, "__instancecheck__", return_value=False):
            html = display_dataframe_summary(mock_df)
        assert "50" in html
        assert "0.00 MB" in html


class TestEnableJupyterDisplay:
    def test_skips_when_not_notebook(self):
        with patch("tif1.jupyter._is_notebook", return_value=False):
            enable_jupyter_display()

    def test_sets_repr_html_in_notebook(self):
        mock_session = MagicMock()
        mock_driver = MagicMock()
        mock_lap = MagicMock()

        with (
            patch("tif1.jupyter._is_notebook", return_value=True),
            patch.dict(
                "sys.modules",
                {"tif1.core": MagicMock(Session=mock_session, Driver=mock_driver, Lap=mock_lap)},
            ),
        ):
            enable_jupyter_display()

        assert hasattr(mock_session, "_repr_html_")
        assert hasattr(mock_driver, "_repr_html_")
        assert hasattr(mock_lap, "_repr_html_")

    def test_handles_import_error_gracefully(self):
        with (
            patch("tif1.jupyter._is_notebook", return_value=True),
            patch.dict("sys.modules", {"tif1.core": None}),
        ):
            enable_jupyter_display()
