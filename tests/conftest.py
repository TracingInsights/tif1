"""Shared pytest configuration and fixtures."""

import pytest


@pytest.fixture(autouse=True)
def reset_global_state():
    """Reset global state before each test to prevent cross-test contamination."""
    from tif1 import async_fetch as async_fetch_module
    from tif1.cdn import get_cdn_manager
    from tif1.retry import reset_circuit_breaker

    # Reset before test
    async_fetch_module.cleanup_resources()
    async_fetch_module._async_session = None
    get_cdn_manager().reset()
    reset_circuit_breaker()

    yield

    # Reset after test
    async_fetch_module.cleanup_resources()
    async_fetch_module._async_session = None
    get_cdn_manager().reset()
    reset_circuit_breaker()
