"""Unit tests for zero-copy backend conversion utilities."""

import pandas as pd
import pytest

try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

from tif1.core_utils.backend_conversion import (
    convert_backend,
    pandas_to_polars,
    polars_to_pandas,
)


class TestPandasToPolars:
    """Test pandas → polars conversion."""

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_basic_conversion(self):
        """Test basic pandas to polars conversion."""
        df_pd = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df_pl = pandas_to_polars(df_pd)

        assert isinstance(df_pl, pl.DataFrame)
        assert df_pl.shape == (3, 2)
        assert df_pl.columns == ["a", "b"]
        assert df_pl["a"].to_list() == [1, 2, 3]
        assert df_pl["b"].to_list() == ["x", "y", "z"]

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_zero_copy_default(self):
        """Test that rechunk=False is the default for zero-copy."""
        df_pd = pd.DataFrame({"a": [1, 2, 3]})
        df_pl = pandas_to_polars(df_pd)

        # Verify conversion succeeded
        assert isinstance(df_pl, pl.DataFrame)
        assert df_pl["a"].to_list() == [1, 2, 3]

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_with_rechunk(self):
        """Test conversion with rechunk=True."""
        df_pd = pd.DataFrame({"a": [1, 2, 3]})
        df_pl = pandas_to_polars(df_pd, rechunk=True)

        assert isinstance(df_pl, pl.DataFrame)
        assert df_pl["a"].to_list() == [1, 2, 3]

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_empty_dataframe(self):
        """Test conversion of empty DataFrame."""
        df_pd = pd.DataFrame()
        df_pl = pandas_to_polars(df_pd)

        assert isinstance(df_pl, pl.DataFrame)
        assert df_pl.is_empty()

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_mixed_types(self):
        """Test conversion with mixed data types."""
        df_pd = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            }
        )
        df_pl = pandas_to_polars(df_pd)

        assert isinstance(df_pl, pl.DataFrame)
        assert df_pl.shape == (3, 4)
        assert df_pl["int_col"].to_list() == [1, 2, 3]
        assert df_pl["str_col"].to_list() == ["a", "b", "c"]
        assert df_pl["bool_col"].to_list() == [True, False, True]

    def test_polars_not_available(self):
        """Test error when polars is not available."""
        from unittest.mock import patch

        import tif1.core_utils.backend_conversion as bc_module

        # Mock POLARS_AVAILABLE flag to simulate polars not being installed
        with patch.object(bc_module, "POLARS_AVAILABLE", False):
            df_pd = pd.DataFrame({"a": [1, 2, 3]})
            with pytest.raises(ImportError, match="polars is not installed"):
                bc_module.pandas_to_polars(df_pd)


class TestPolarsToPandas:
    """Test polars → pandas conversion."""

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_basic_conversion(self):
        """Test basic polars to pandas conversion."""
        df_pl = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df_pd = polars_to_pandas(df_pl)

        assert isinstance(df_pd, pd.DataFrame)
        assert df_pd.shape == (3, 2)
        assert list(df_pd.columns) == ["a", "b"]
        assert df_pd["a"].tolist() == [1, 2, 3]
        assert df_pd["b"].tolist() == ["x", "y", "z"]

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_zero_copy_default(self):
        """Test that use_pyarrow=True is the default for zero-copy."""
        df_pl = pl.DataFrame({"a": [1, 2, 3]})
        df_pd = polars_to_pandas(df_pl)

        # Verify conversion succeeded
        assert isinstance(df_pd, pd.DataFrame)
        assert df_pd["a"].tolist() == [1, 2, 3]

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_without_pyarrow(self):
        """Test conversion with use_pyarrow=False."""
        df_pl = pl.DataFrame({"a": [1, 2, 3]})
        df_pd = polars_to_pandas(df_pl, use_pyarrow=False)

        assert isinstance(df_pd, pd.DataFrame)
        assert df_pd["a"].tolist() == [1, 2, 3]

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_empty_dataframe(self):
        """Test conversion of empty DataFrame."""
        df_pl = pl.DataFrame()
        df_pd = polars_to_pandas(df_pl)

        assert isinstance(df_pd, pd.DataFrame)
        assert df_pd.empty

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_mixed_types(self):
        """Test conversion with mixed data types."""
        df_pl = pl.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            }
        )
        df_pd = polars_to_pandas(df_pl)

        assert isinstance(df_pd, pd.DataFrame)
        assert df_pd.shape == (3, 4)
        assert df_pd["int_col"].tolist() == [1, 2, 3]
        assert df_pd["str_col"].tolist() == ["a", "b", "c"]
        assert df_pd["bool_col"].tolist() == [True, False, True]

    def test_polars_not_available(self):
        """Test error when polars is not available."""
        from unittest.mock import patch

        import tif1.core_utils.backend_conversion as bc_module

        # Mock POLARS_AVAILABLE flag to simulate polars not being installed
        with patch.object(bc_module, "POLARS_AVAILABLE", False):
            with pytest.raises(ImportError, match="polars is not installed"):
                # Pass a mock object since we can't create a real polars DataFrame
                bc_module.polars_to_pandas(None)  # type: ignore


class TestConvertBackend:
    """Test generic backend conversion."""

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_pandas_to_polars(self):
        """Test pandas → polars via convert_backend."""
        df_pd = pd.DataFrame({"a": [1, 2, 3]})
        df_pl = convert_backend(df_pd, "polars")

        assert isinstance(df_pl, pl.DataFrame)
        assert df_pl["a"].to_list() == [1, 2, 3]

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_polars_to_pandas(self):
        """Test polars → pandas via convert_backend."""
        df_pl = pl.DataFrame({"a": [1, 2, 3]})
        df_pd = convert_backend(df_pl, "pandas")

        assert isinstance(df_pd, pd.DataFrame)
        assert df_pd["a"].tolist() == [1, 2, 3]

    def test_pandas_to_pandas_noop(self):
        """Test that pandas → pandas is a no-op."""
        df_pd = pd.DataFrame({"a": [1, 2, 3]})
        df_result = convert_backend(df_pd, "pandas")

        assert df_result is df_pd  # Same object

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_polars_to_polars_noop(self):
        """Test that polars → polars is a no-op."""
        df_pl = pl.DataFrame({"a": [1, 2, 3]})
        df_result = convert_backend(df_pl, "polars")

        assert df_result is df_pl  # Same object

    def test_invalid_backend(self):
        """Test error with invalid backend."""
        df_pd = pd.DataFrame({"a": [1, 2, 3]})
        with pytest.raises(ValueError, match="Invalid target_backend"):
            convert_backend(df_pd, "invalid")

    def test_polars_not_available(self):
        """Test error when converting to polars without polars installed."""
        from unittest.mock import patch

        import tif1.core_utils.backend_conversion as bc_module

        # Mock POLARS_AVAILABLE flag to simulate polars not being installed
        with patch.object(bc_module, "POLARS_AVAILABLE", False):
            df_pd = pd.DataFrame({"a": [1, 2, 3]})
            with pytest.raises(ImportError, match="polars is not installed"):
                bc_module.convert_backend(df_pd, "polars")


class TestRoundTripConversion:
    """Test round-trip conversions preserve data."""

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_pandas_polars_pandas(self):
        """Test pandas → polars → pandas round-trip."""
        df_original = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
            }
        )

        # Convert to polars and back
        df_pl = pandas_to_polars(df_original)
        df_result = polars_to_pandas(df_pl)

        # Verify data is preserved
        assert isinstance(df_result, pd.DataFrame)
        assert df_result.shape == df_original.shape
        assert list(df_result.columns) == list(df_original.columns)
        assert df_result["int_col"].tolist() == df_original["int_col"].tolist()
        assert df_result["str_col"].tolist() == df_original["str_col"].tolist()

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_polars_pandas_polars(self):
        """Test polars → pandas → polars round-trip."""
        df_original = pl.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
            }
        )

        # Convert to pandas and back
        df_pd = polars_to_pandas(df_original)
        df_result = pandas_to_polars(df_pd)

        # Verify data is preserved
        assert isinstance(df_result, pl.DataFrame)
        assert df_result.shape == df_original.shape
        assert df_result.columns == df_original.columns
        assert df_result["int_col"].to_list() == df_original["int_col"].to_list()
        assert df_result["str_col"].to_list() == df_original["str_col"].to_list()

    @pytest.mark.skipif(not POLARS_AVAILABLE, reason="polars not installed")
    def test_convert_backend_round_trip(self):
        """Test round-trip using convert_backend."""
        df_original = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        # pandas → polars → pandas
        df_pl = convert_backend(df_original, "polars")
        df_result = convert_backend(df_pl, "pandas")

        assert isinstance(df_result, pd.DataFrame)
        assert df_result["a"].tolist() == df_original["a"].tolist()
        assert df_result["b"].tolist() == df_original["b"].tolist()
