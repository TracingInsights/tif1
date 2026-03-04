"""Zero-copy backend conversion utilities for pandas ↔ polars."""

import logging
from typing import TYPE_CHECKING

import pandas as pd

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    pl = None  # type: ignore
    POLARS_AVAILABLE = False

if TYPE_CHECKING:
    from .helpers import DataFrame

logger = logging.getLogger(__name__)


def pandas_to_polars(df: pd.DataFrame, *, rechunk: bool = False) -> "pl.DataFrame":
    """Convert pandas DataFrame to polars using zero-copy Arrow conversion.

    Args:
        df: Pandas DataFrame to convert
        rechunk: Whether to rechunk the result. False for zero-copy (default).

    Returns:
        Polars DataFrame

    Raises:
        ImportError: If polars is not available
        ValueError: If conversion fails
    """
    if not POLARS_AVAILABLE:
        raise ImportError("polars is not installed")

    try:
        # Use from_pandas with rechunk=False for zero-copy conversion via Arrow
        return pl.from_pandas(df, rechunk=rechunk)
    except Exception as e:
        logger.warning(f"Zero-copy pandas→polars conversion failed: {e}")
        raise ValueError(f"Failed to convert pandas DataFrame to polars: {e}") from e


def polars_to_pandas(df: "pl.DataFrame", *, use_pyarrow: bool = True) -> pd.DataFrame:
    """Convert polars DataFrame to pandas using zero-copy Arrow conversion.

    Args:
        df: Polars DataFrame to convert
        use_pyarrow: Whether to use PyArrow extension arrays for zero-copy (default True)

    Returns:
        Pandas DataFrame

    Raises:
        ImportError: If polars is not available
        ValueError: If conversion fails
    """
    if not POLARS_AVAILABLE:
        raise ImportError("polars is not installed")

    try:
        # Use to_pandas with use_pyarrow_extension_array=True for zero-copy via Arrow
        return df.to_pandas(use_pyarrow_extension_array=use_pyarrow)
    except Exception as e:
        logger.warning(f"Zero-copy polars→pandas conversion failed: {e}")
        raise ValueError(f"Failed to convert polars DataFrame to pandas: {e}") from e


def convert_backend(df: "DataFrame", target_backend: str) -> "DataFrame":
    """Convert DataFrame to target backend using zero-copy when possible.

    Args:
        df: DataFrame to convert (pandas or polars)
        target_backend: Target backend ("pandas" or "polars")

    Returns:
        DataFrame in target backend format

    Raises:
        ValueError: If target_backend is invalid or conversion fails
        ImportError: If polars is not available and target is "polars"
    """
    if target_backend not in {"pandas", "polars"}:
        raise ValueError(f"Invalid target_backend: {target_backend}. Must be 'pandas' or 'polars'")

    # Check if already in target backend
    if isinstance(df, pd.DataFrame) and target_backend == "pandas":
        return df

    if POLARS_AVAILABLE and isinstance(df, pl.DataFrame) and target_backend == "polars":
        return df

    # Convert pandas → polars
    if isinstance(df, pd.DataFrame) and target_backend == "polars":
        return pandas_to_polars(df, rechunk=False)

    # Convert polars → pandas
    if POLARS_AVAILABLE and isinstance(df, pl.DataFrame) and target_backend == "pandas":
        return polars_to_pandas(df, use_pyarrow=True)

    raise ValueError(f"Cannot convert {type(df).__name__} to {target_backend}")
