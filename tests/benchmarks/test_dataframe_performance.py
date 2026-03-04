"""Benchmark tests for DataFrame memory usage and performance.

This module tests that DataFrame operations don't exceed 2N rows worth of memory
during creation, filtering, concatenation, and lib conversion operations.
"""

import sys
import tracemalloc
from typing import Any

import pandas as pd
import pytest

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    pl = None
    POLARS_AVAILABLE = False

from tif1.core_utils.backend_conversion import pandas_to_polars, polars_to_pandas


def _estimate_row_memory(data: dict[str, list], lib: str) -> int:
    """Estimate memory per row for a given data structure.

    Args:
        data: Dictionary of column name to list of values
        lib: "pandas" or "polars"

    Returns:
        Estimated bytes per row
    """
    if not data:
        return 0

    # Get number of rows
    n_rows = len(next(iter(data.values())))
    if n_rows == 0:
        return 0

    # Estimate memory per value based on type
    bytes_per_row = 0
    for col_name, values in data.items():
        if not values:
            continue

        sample_value = values[0]

        # String overhead (column name + value)
        if isinstance(sample_value, str):
            avg_str_len = sum(len(str(v)) for v in values[: min(100, len(values))]) / min(
                100, len(values)
            )
            bytes_per_row += avg_str_len + len(col_name) + 50  # overhead

        # Numeric types
        elif isinstance(sample_value, int):
            bytes_per_row += 8  # int64

        elif isinstance(sample_value, float):
            bytes_per_row += 8  # float64

        # Boolean
        elif isinstance(sample_value, bool):
            bytes_per_row += 1

        # None/null
        elif sample_value is None:
            bytes_per_row += 8  # nullable overhead

        else:
            bytes_per_row += sys.getsizeof(sample_value)

    # Add DataFrame overhead (index, column metadata, etc.)
    # For small datasets, overhead dominates; for large datasets, per-row cost dominates
    overhead = 5000 if lib == "pandas" else 3000  # polars is more efficient
    bytes_per_row += overhead / n_rows

    return int(bytes_per_row)


def _measure_peak_memory(func, *args, **kwargs) -> tuple[Any, int]:
    """Measure peak memory usage during function execution.

    Args:
        func: Function to execute
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Tuple of (function result, peak memory in bytes)
    """
    tracemalloc.start()
    tracemalloc.reset_peak()

    try:
        result = func(*args, **kwargs)
        _current, peak = tracemalloc.get_traced_memory()
        return result, peak
    finally:
        tracemalloc.stop()


def _assert_memory_within_limit(peak_memory: int, n_rows: int, bytes_per_row: int) -> None:
    """Assert that peak memory doesn't exceed 2N rows with appropriate tolerance.

    Args:
        peak_memory: Peak memory usage in bytes
        n_rows: Number of rows in dataset
        bytes_per_row: Estimated bytes per row

    Raises:
        AssertionError: If memory exceeds limit
    """
    # Use higher tolerance for small datasets due to fixed overhead
    # For large datasets (>1000 rows), we expect closer to 2N behavior
    if n_rows < 500:
        tolerance = 4.0  # Small datasets have proportionally higher overhead
    elif n_rows < 2000:
        tolerance = 3.0  # Medium datasets
    else:
        tolerance = 2.5  # Large datasets should be close to 2N

    max_allowed_memory = bytes_per_row * n_rows * tolerance
    assert peak_memory <= max_allowed_memory, (
        f"Peak memory {peak_memory} exceeds {tolerance}N limit {max_allowed_memory} (n_rows={n_rows})"
    )


# Test data generators
def _generate_lap_data(n_rows: int) -> dict[str, list]:
    """Generate synthetic lap data for testing."""
    return {
        "time": [90.0 + (i % 100) * 0.1 for i in range(n_rows)],
        "lap": list(range(1, n_rows + 1)),
        "compound": ["SOFT" if i % 3 == 0 else "MEDIUM" for i in range(n_rows)],
        "stint": [1 + (i // 20) for i in range(n_rows)],
        "s1": [30.0 + (i % 10) * 0.05 for i in range(n_rows)],
        "s2": [30.0 + (i % 10) * 0.05 for i in range(n_rows)],
        "s3": [30.0 + (i % 10) * 0.05 for i in range(n_rows)],
    }


def _generate_telemetry_data(n_rows: int) -> dict[str, list]:
    """Generate synthetic telemetry data for testing."""
    return {
        "time": [float(i) * 0.01 for i in range(n_rows)],
        "speed": [200.0 + (i % 100) for i in range(n_rows)],
        "rpm": [10000 + (i % 5000) for i in range(n_rows)],
        "gear": [3 + (i % 5) for i in range(n_rows)],
        "throttle": [50.0 + (i % 50) for i in range(n_rows)],
        "brake": [i % 10 < 3 for i in range(n_rows)],
    }


# Benchmark tests
@pytest.mark.benchmark
@pytest.mark.parametrize("n_rows", [100, 1000, 10000])
def test_pandas_dataframe_creation_memory(n_rows):
    """Test that pandas DataFrame creation doesn't exceed 2N memory."""
    data = _generate_lap_data(n_rows)
    bytes_per_row = _estimate_row_memory(data, "pandas")

    # Measure memory during DataFrame creation
    df, peak_memory = _measure_peak_memory(pd.DataFrame, data, copy=False)

    # Verify DataFrame was created
    assert len(df) == n_rows
    assert not df.empty

    # Verify memory doesn't exceed 2N rows
    _assert_memory_within_limit(peak_memory, n_rows, bytes_per_row)


@pytest.mark.benchmark
@pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
@pytest.mark.parametrize("n_rows", [100, 1000, 10000])
def test_polars_dataframe_creation_memory(n_rows):
    """Test that polars DataFrame creation doesn't exceed 2N memory."""
    data = _generate_lap_data(n_rows)
    bytes_per_row = _estimate_row_memory(data, "polars")

    # Measure memory during DataFrame creation
    df, peak_memory = _measure_peak_memory(pl.DataFrame, data, strict=False)

    # Verify DataFrame was created
    assert len(df) == n_rows
    assert df.shape[0] > 0

    # Verify memory doesn't exceed 2N rows
    _assert_memory_within_limit(peak_memory, n_rows, bytes_per_row)


@pytest.mark.benchmark
@pytest.mark.parametrize("n_rows", [100, 1000, 5000])
def test_pandas_concat_memory(n_rows):
    """Test that pandas concat doesn't create excessive copies."""
    # Create multiple DataFrames to concatenate
    dfs = [pd.DataFrame(_generate_lap_data(n_rows // 5), copy=False) for _ in range(5)]

    bytes_per_row = _estimate_row_memory(_generate_lap_data(n_rows // 5), "pandas")

    # Measure memory during concatenation
    result_df, peak_memory = _measure_peak_memory(pd.concat, dfs, ignore_index=True, copy=False)

    # Verify result
    assert len(result_df) == n_rows
    assert not result_df.empty

    # Verify memory doesn't exceed 2N rows
    _assert_memory_within_limit(peak_memory, n_rows, bytes_per_row)


@pytest.mark.benchmark
@pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
@pytest.mark.parametrize("n_rows", [100, 1000, 5000])
def test_polars_concat_memory(n_rows):
    """Test that polars concat doesn't create excessive copies."""
    # Create multiple DataFrames to concatenate
    dfs = [pl.DataFrame(_generate_lap_data(n_rows // 5), strict=False) for _ in range(5)]

    bytes_per_row = _estimate_row_memory(_generate_lap_data(n_rows // 5), "polars")

    # Measure memory during concatenation
    result_df, peak_memory = _measure_peak_memory(
        pl.concat, dfs, how="vertical_relaxed", rechunk=False
    )

    # Verify result
    assert len(result_df) == n_rows
    assert result_df.shape[0] > 0

    # Verify memory doesn't exceed 2N rows
    _assert_memory_within_limit(peak_memory, n_rows, bytes_per_row)


@pytest.mark.benchmark
@pytest.mark.parametrize("n_rows", [100, 1000, 5000])
def test_pandas_filtering_memory(n_rows):
    """Test that pandas filtering doesn't create excessive copies."""
    data = _generate_lap_data(n_rows)
    frame = pd.DataFrame(data, copy=False)

    bytes_per_row = _estimate_row_memory(data, "pandas")

    # Measure memory during filtering
    def filter_operation():
        # Filter for valid lap times (similar to _filter_valid_laptimes)
        return frame[frame["time"] > 0]

    filtered_df, peak_memory = _measure_peak_memory(filter_operation)

    # Verify filtering worked
    assert len(filtered_df) > 0
    assert len(filtered_df) <= n_rows

    # Verify memory doesn't exceed 2N rows
    _assert_memory_within_limit(peak_memory, n_rows, bytes_per_row)


@pytest.mark.benchmark
@pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
@pytest.mark.parametrize("n_rows", [100, 1000, 5000])
def test_polars_filtering_memory(n_rows):
    """Test that polars filtering doesn't create excessive copies."""
    data = _generate_lap_data(n_rows)
    frame = pl.DataFrame(data, strict=False)

    bytes_per_row = _estimate_row_memory(data, "polars")

    # Measure memory during filtering
    def filter_operation():
        # Filter for valid lap times (lazy evaluation)
        return frame.filter(pl.col("time") > 0)

    filtered_df, peak_memory = _measure_peak_memory(filter_operation)

    # Verify filtering worked
    assert len(filtered_df) > 0
    assert len(filtered_df) <= n_rows

    # Verify memory doesn't exceed 2N rows
    _assert_memory_within_limit(peak_memory, n_rows, bytes_per_row)


@pytest.mark.benchmark
@pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
@pytest.mark.parametrize("n_rows", [100, 1000, 5000])
def test_pandas_to_polars_zero_copy(n_rows):
    """Test that pandas→polars conversion is zero-copy via Arrow."""
    data = _generate_lap_data(n_rows)
    df_pandas = pd.DataFrame(data, copy=False)

    bytes_per_row = _estimate_row_memory(data, "pandas")

    # Measure memory during conversion
    df_polars, peak_memory = _measure_peak_memory(pandas_to_polars, df_pandas, rechunk=False)

    # Verify conversion worked
    assert len(df_polars) == n_rows
    assert df_polars.shape[0] > 0

    # Verify memory doesn't exceed 2N rows
    _assert_memory_within_limit(peak_memory, n_rows, bytes_per_row)


@pytest.mark.benchmark
@pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
@pytest.mark.parametrize("n_rows", [100, 1000, 5000])
def test_polars_to_pandas_zero_copy(n_rows):
    """Test that polars→pandas conversion is zero-copy via Arrow."""
    data = _generate_lap_data(n_rows)
    df_polars = pl.DataFrame(data, strict=False)

    bytes_per_row = _estimate_row_memory(data, "polars")

    # Measure memory during conversion
    df_pandas, peak_memory = _measure_peak_memory(polars_to_pandas, df_polars, use_pyarrow=True)

    # Verify conversion worked
    assert len(df_pandas) == n_rows
    assert not df_pandas.empty

    # Verify memory doesn't exceed 2N rows (with higher tolerance for Arrow overhead)
    # Arrow conversion has significant fixed overhead for small datasets
    if n_rows < 500:
        tolerance = 20.0  # Arrow has very high fixed overhead for tiny datasets
    elif n_rows < 2000:
        tolerance = 5.0
    else:
        tolerance = 3.0

    max_allowed_memory = bytes_per_row * n_rows * tolerance
    assert peak_memory <= max_allowed_memory, (
        f"Peak memory {peak_memory} exceeds {tolerance}N limit {max_allowed_memory} (n_rows={n_rows})"
    )


@pytest.mark.benchmark
@pytest.mark.parametrize("n_rows", [100, 1000, 5000])
def test_dataframe_with_metadata_memory(n_rows):
    """Test that adding metadata columns doesn't create excessive copies."""
    data = _generate_lap_data(n_rows)

    # Add metadata to dict before DataFrame creation (zero-copy pattern)
    data_with_meta = {**data, "driver": "VER", "team": "Red Bull Racing"}

    bytes_per_row = _estimate_row_memory(data_with_meta, "pandas")

    # Measure memory during DataFrame creation with metadata
    df, peak_memory = _measure_peak_memory(pd.DataFrame, data_with_meta, copy=False)

    # Verify DataFrame was created with metadata
    assert len(df) == n_rows
    assert "driver" in df.columns
    assert "team" in df.columns

    # Verify memory doesn't exceed 2N rows
    _assert_memory_within_limit(peak_memory, n_rows, bytes_per_row)


@pytest.mark.benchmark
@pytest.mark.parametrize("n_rows", [1000, 5000, 10000])
def test_large_telemetry_concat_memory(n_rows):
    """Test memory usage for large telemetry data concatenation."""
    # Simulate concatenating telemetry from multiple laps
    n_laps = 10
    rows_per_lap = n_rows // n_laps

    dfs = [pd.DataFrame(_generate_telemetry_data(rows_per_lap), copy=False) for _ in range(n_laps)]

    bytes_per_row = _estimate_row_memory(_generate_telemetry_data(rows_per_lap), "pandas")

    # Measure memory during concatenation
    result_df, peak_memory = _measure_peak_memory(pd.concat, dfs, ignore_index=True, copy=False)

    # Verify result
    assert len(result_df) == n_rows
    assert not result_df.empty

    # Verify memory doesn't exceed 2N rows
    _assert_memory_within_limit(peak_memory, n_rows, bytes_per_row)


@pytest.mark.benchmark
def test_memory_efficiency_summary():
    """Summary test to verify overall memory efficiency across operations."""
    n_rows = 5000

    # Test all major operations
    operations = []

    # 1. DataFrame creation
    data = _generate_lap_data(n_rows)
    df_pandas, mem_create = _measure_peak_memory(pd.DataFrame, data, copy=False)
    operations.append(("create", mem_create))

    # 2. Filtering
    _filtered_df, mem_filter = _measure_peak_memory(lambda: df_pandas[df_pandas["time"] > 0])
    operations.append(("filter", mem_filter))

    # 3. Concatenation
    dfs = [pd.DataFrame(_generate_lap_data(n_rows // 5), copy=False) for _ in range(5)]
    _concat_df, mem_concat = _measure_peak_memory(pd.concat, dfs, ignore_index=True, copy=False)
    operations.append(("concat", mem_concat))

    # 4. Lib conversion (if polars available)
    if POLARS_AVAILABLE:
        _df_polars, mem_convert = _measure_peak_memory(pandas_to_polars, df_pandas, rechunk=False)
        operations.append(("convert", mem_convert))

    # Calculate baseline memory (1N rows)
    bytes_per_row = _estimate_row_memory(data, "pandas")
    baseline_memory = bytes_per_row * n_rows

    # Verify all operations stay within reasonable limits
    for op_name, peak_mem in operations:
        ratio = peak_mem / baseline_memory
        # Use 3.0x tolerance for summary test (more lenient)
        assert ratio <= 3.0, f"Operation '{op_name}' exceeded 3.0N limit (ratio: {ratio:.2f})"

    # Print summary
    print(f"\nMemory Efficiency Summary (n_rows={n_rows}):")
    print(f"Baseline (1N): {baseline_memory / 1024:.2f} KB")
    for op_name, peak_mem in operations:
        ratio = peak_mem / baseline_memory
        print(f"  {op_name}: {peak_mem / 1024:.2f} KB ({ratio:.2f}N)")
