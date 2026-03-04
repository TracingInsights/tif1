"""Unit tests for HTTP session dynamic pool sizing."""

from unittest.mock import MagicMock, patch


def test_pool_sizing_defaults_to_max_workers():
    """Test that pool_connections defaults to max_workers when not explicitly set."""
    from tif1 import http_session

    mock_config = MagicMock()
    mock_config.get.side_effect = lambda key, default=None: {
        "max_workers": 30,
        "pool_connections": None,
        "pool_maxsize": None,
        "http_resolvers": ["standard"],
    }.get(key, default)

    mock_session = MagicMock()
    mock_niquests = MagicMock()
    mock_niquests.Session.return_value = mock_session
    mock_niquests.adapters.HTTPAdapter = MagicMock()

    with (
        patch("tif1.http_session._import_niquests", return_value=mock_niquests),
        patch("tif1.config.get_config", return_value=mock_config),
    ):
        http_session._shared_session = None
        http_session._create_session()

        # Verify pool_connections = max(256, max_workers) = 256
        # Verify pool_maxsize = max(512, 4 * pool_connections) = 1024
        adapter_call = mock_niquests.adapters.HTTPAdapter.call_args
        assert adapter_call[1]["pool_connections"] == 256
        assert adapter_call[1]["pool_maxsize"] == 1024


def test_pool_sizing_minimum_10_connections():
    """Test that pool_connections has a minimum of 256 even with low max_workers."""
    from tif1 import http_session

    mock_config = MagicMock()
    mock_config.get.side_effect = lambda key, default=None: {
        "max_workers": 5,
        "pool_connections": None,
        "pool_maxsize": None,
        "http_resolvers": ["standard"],
    }.get(key, default)

    mock_session = MagicMock()
    mock_niquests = MagicMock()
    mock_niquests.Session.return_value = mock_session
    mock_niquests.adapters.HTTPAdapter = MagicMock()

    with (
        patch("tif1.http_session._import_niquests", return_value=mock_niquests),
        patch("tif1.config.get_config", return_value=mock_config),
    ):
        http_session._shared_session = None
        http_session._create_session()

        # Verify pool_connections = 256 (minimum)
        # Verify pool_maxsize = max(512, 4 * 256) = 1024
        adapter_call = mock_niquests.adapters.HTTPAdapter.call_args
        assert adapter_call[1]["pool_connections"] == 256
        assert adapter_call[1]["pool_maxsize"] == 1024


def test_pool_sizing_respects_explicit_pool_connections():
    """Test that explicit pool_connections config is respected."""
    from tif1 import http_session

    mock_config = MagicMock()
    mock_config.get.side_effect = lambda key, default=None: {
        "max_workers": 20,
        "pool_connections": 50,
        "pool_maxsize": None,
        "http_resolvers": ["standard"],
    }.get(key, default)

    mock_session = MagicMock()
    mock_niquests = MagicMock()
    mock_niquests.Session.return_value = mock_session
    mock_niquests.adapters.HTTPAdapter = MagicMock()

    with (
        patch("tif1.http_session._import_niquests", return_value=mock_niquests),
        patch("tif1.config.get_config", return_value=mock_config),
    ):
        http_session._shared_session = None
        http_session._create_session()

        # Verify pool_connections = max(10, 50) = 50 (explicit)
        # Verify pool_maxsize = max(512, 50, 4 * 50) = 512
        adapter_call = mock_niquests.adapters.HTTPAdapter.call_args
        assert adapter_call[1]["pool_connections"] == 50
        assert adapter_call[1]["pool_maxsize"] == 512


def test_pool_sizing_respects_explicit_pool_maxsize():
    """Test that explicit pool_maxsize config is respected."""
    from tif1 import http_session

    mock_config = MagicMock()
    mock_config.get.side_effect = lambda key, default=None: {
        "max_workers": 20,
        "pool_connections": None,
        "pool_maxsize": 150,
        "http_resolvers": ["standard"],
    }.get(key, default)

    mock_session = MagicMock()
    mock_niquests = MagicMock()
    mock_niquests.Session.return_value = mock_session
    mock_niquests.adapters.HTTPAdapter = MagicMock()

    with (
        patch("tif1.http_session._import_niquests", return_value=mock_niquests),
        patch("tif1.config.get_config", return_value=mock_config),
    ):
        http_session._shared_session = None
        http_session._create_session()

        # Verify pool_connections = max(256, max_workers) = 256
        # Verify pool_maxsize = max(512, 256, 150) = 512 (minimum enforced)
        adapter_call = mock_niquests.adapters.HTTPAdapter.call_args
        assert adapter_call[1]["pool_connections"] == 256
        assert adapter_call[1]["pool_maxsize"] == 512


def test_pool_sizing_maxsize_at_least_connections():
    """Test that pool_maxsize is at least pool_connections."""
    from tif1 import http_session

    mock_config = MagicMock()
    mock_config.get.side_effect = lambda key, default=None: {
        "max_workers": 20,
        "pool_connections": 50,
        "pool_maxsize": 30,  # Less than pool_connections
        "http_resolvers": ["standard"],
    }.get(key, default)

    mock_session = MagicMock()
    mock_niquests = MagicMock()
    mock_niquests.Session.return_value = mock_session
    mock_niquests.adapters.HTTPAdapter = MagicMock()

    with (
        patch("tif1.http_session._import_niquests", return_value=mock_niquests),
        patch("tif1.config.get_config", return_value=mock_config),
    ):
        http_session._shared_session = None
        http_session._create_session()

        # Verify pool_maxsize is at least pool_connections and minimum 512
        adapter_call = mock_niquests.adapters.HTTPAdapter.call_args
        assert adapter_call[1]["pool_connections"] == 50
        assert adapter_call[1]["pool_maxsize"] == 512  # max(512, 50, 30)


def test_pool_sizing_high_concurrency():
    """Test pool sizing with high concurrency settings."""
    from tif1 import http_session

    mock_config = MagicMock()
    mock_config.get.side_effect = lambda key, default=None: {
        "max_workers": 100,
        "pool_connections": None,
        "pool_maxsize": None,
        "http_resolvers": ["standard"],
    }.get(key, default)

    mock_session = MagicMock()
    mock_niquests = MagicMock()
    mock_niquests.Session.return_value = mock_session
    mock_niquests.adapters.HTTPAdapter = MagicMock()

    with (
        patch("tif1.http_session._import_niquests", return_value=mock_niquests),
        patch("tif1.config.get_config", return_value=mock_config),
    ):
        http_session._shared_session = None
        http_session._create_session()

        # Verify pool scales with high concurrency: max(256, 100) = 256
        adapter_call = mock_niquests.adapters.HTTPAdapter.call_args
        assert adapter_call[1]["pool_connections"] == 256
        assert adapter_call[1]["pool_maxsize"] == 1024  # max(512, 4 * 256)
