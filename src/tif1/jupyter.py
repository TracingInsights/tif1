"""Jupyter notebook integration with rich display."""

import html
import importlib
import logging
import urllib.parse
from typing import TYPE_CHECKING, Any, Union, cast

try:
    import pandas as pd
except ImportError:  # pragma: no cover - pandas is required in runtime deps
    pd = None  # type: ignore[assignment]

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)


def _is_notebook() -> bool:
    """Check if running in Jupyter notebook."""
    try:
        ipython = importlib.import_module("IPython")
        get_ipython = getattr(ipython, "get_ipython", None)
        if get_ipython is None:
            return False
        shell = get_ipython()
        if shell is None:
            return False
        if "IPKernelApp" in getattr(shell, "config", {}):
            return True
    except Exception:
        pass
    return False


class JupyterDisplayMixin:
    """Mixin for rich Jupyter display."""

    def _repr_html_(self) -> str:
        """Rich HTML representation for Jupyter."""
        if not _is_notebook():
            return repr(self)

        try:
            return self._generate_html()
        except Exception as e:
            logger.warning(f"Failed to generate HTML display: {e}")
            return repr(self)

    def _generate_html(self) -> str:
        """Generate HTML representation."""
        raise NotImplementedError


def display_session_info(session) -> str:
    """Generate HTML display for Session."""
    gp_name = html.escape(urllib.parse.unquote(str(session.gp)))
    session_name = html.escape(urllib.parse.unquote(str(session.session)))
    lib = html.escape(str(session.lib))
    year = html.escape(str(session.year))
    num_drivers = (
        len(session.drivers) if hasattr(session, "_drivers") and session._drivers else "Not loaded"
    )
    num_drivers_str = html.escape(str(num_drivers))

    html_content = f"""
    <div style="border: 1px solid #ddd; padding: 10px; border-radius: 5px; background: #f9f9f9;">
        <h3 style="margin-top: 0;">🏎️ F1 Session</h3>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 5px;"><strong>Year:</strong></td>
                <td style="padding: 5px;">{year}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Grand Prix:</strong></td>
                <td style="padding: 5px;">{gp_name}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Session:</strong></td>
                <td style="padding: 5px;">{session_name}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Library:</strong></td>
                <td style="padding: 5px;">{lib}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Drivers:</strong></td>
                <td style="padding: 5px;">{num_drivers_str}</td>
            </tr>
        </table>
    </div>
    """
    return html_content


def display_driver_info(driver) -> str:
    """Generate HTML display for Driver."""
    driver_code = html.escape(str(driver.driver))
    year = html.escape(str(driver.session.year))
    gp_name = html.escape(urllib.parse.unquote(str(driver.session.gp)))
    session_name = html.escape(urllib.parse.unquote(str(driver.session.session)))
    laps_loaded = "Yes" if driver._laps is not None else "No"

    html_content = f"""
    <div style="border: 1px solid #ddd; padding: 10px; border-radius: 5px; background: #f0f8ff;">
        <h3 style="margin-top: 0;">👤 Driver: {driver_code}</h3>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 5px;"><strong>Session:</strong></td>
                <td style="padding: 5px;">{year} {gp_name} - {session_name}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Laps loaded:</strong></td>
                <td style="padding: 5px;">{laps_loaded}</td>
            </tr>
        </table>
    </div>
    """
    return html_content


def display_lap_info(lap) -> str:
    """Generate HTML display for Lap."""
    lap_num = html.escape(str(lap.lap_number))
    driver_code = html.escape(str(lap.driver))
    year = html.escape(str(lap.session.year))
    gp_name = html.escape(urllib.parse.unquote(str(lap.session.gp)))
    session_name = html.escape(urllib.parse.unquote(str(lap.session.session)))
    telemetry_loaded = "Yes" if lap._telemetry is not None else "No"

    html_content = f"""
    <div style="border: 1px solid #ddd; padding: 10px; border-radius: 5px; background: #fff8dc;">
        <h3 style="margin-top: 0;">🏁 Lap {lap_num}</h3>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 5px;"><strong>Driver:</strong></td>
                <td style="padding: 5px;">{driver_code}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Session:</strong></td>
                <td style="padding: 5px;">{year} {gp_name} - {session_name}</td>
            </tr>
            <tr>
                <td style="padding: 5px;"><strong>Telemetry loaded:</strong></td>
                <td style="padding: 5px;">{telemetry_loaded}</td>
            </tr>
        </table>
    </div>
    """
    return html_content


def display_dataframe_summary(df: Union["pd.DataFrame", "pl.DataFrame"]) -> str:
    """Generate summary display for DataFrame."""
    is_pandas = pd is not None and isinstance(df, pd.DataFrame)

    if is_pandas:
        df_pd = cast(pd.DataFrame, df)
        rows, cols = df_pd.shape
        memory = df_pd.memory_usage(deep=True).sum() / 1024 / 1024
    else:
        df_pl = cast(Any, df)
        rows, cols = df_pl.shape
        memory = df_pl.estimated_size() / 1024 / 1024 if hasattr(df_pl, "estimated_size") else 0

    html = f"""
    <div style="border-left: 3px solid #4CAF50; padding-left: 10px; margin: 10px 0;">
        <strong>DataFrame Summary:</strong> {rows:,} rows × {cols} columns | Memory: {memory:.2f} MB
    </div>
    """
    return html


def enable_jupyter_display():
    """Enable rich Jupyter display for tif1 objects."""
    if not _is_notebook():
        logger.info("Not in Jupyter environment, skipping display setup")
        return

    try:
        from tif1.core import Driver, Lap, Session

        # Add _repr_html_ methods
        Session._repr_html_ = lambda self: display_session_info(self)  # type: ignore[attr-defined]
        Driver._repr_html_ = lambda self: display_driver_info(self)  # type: ignore[attr-defined]
        Lap._repr_html_ = lambda self: display_lap_info(self)  # type: ignore[attr-defined]

        logger.info("Jupyter display enabled")
    except Exception as e:
        logger.warning(f"Failed to enable Jupyter display: {e}")


# Auto-enable if in notebook
if _is_notebook():
    try:
        enable_jupyter_display()
    except Exception:
        pass
