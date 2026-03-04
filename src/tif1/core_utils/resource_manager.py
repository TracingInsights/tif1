"""Resource management utilities for guaranteed cleanup.

This module provides a base class for managing resources with automatic cleanup
in error paths. Resources are tracked and cleaned up in reverse order of creation.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ResourceManager:
    """Base class for resource management with guaranteed cleanup.

    This class provides a pattern for tracking resources and ensuring they are
    properly cleaned up even when initialization fails partway through. Resources
    are cleaned up in reverse order of creation (LIFO).

    Usage:
        class MyManager(ResourceManager):
            def __init__(self):
                super().__init__()
                try:
                    conn = create_connection()
                    self._register_resource("connection", conn)

                    pool = create_pool()
                    self._register_resource("pool", pool)

                    self._initialized = True
                except Exception:
                    self._cleanup_resources()
                    raise

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                self._cleanup_resources()
                return False

    Attributes:
        _resources: List of (name, resource) tuples tracked for cleanup
        _initialized: Flag indicating successful initialization
    """

    def __init__(self) -> None:
        """Initialize the resource manager."""
        self._resources: list[tuple[str, Any]] = []
        self._initialized: bool = False

    def _register_resource(self, name: str, resource: Any) -> None:
        """Register a resource for cleanup tracking.

        Resources are cleaned up in reverse order of registration (LIFO).
        This ensures dependencies are respected during cleanup.

        Args:
            name: Human-readable name for the resource (used in logging)
            resource: The resource object to track. Should have a close() or
                     shutdown() method for cleanup.

        Example:
            self._register_resource("database", db_connection)
            self._register_resource("thread_pool", executor)
        """
        self._resources.append((name, resource))
        logger.debug(f"Registered resource: {name}")

    def _cleanup_resources(self) -> None:
        """Cleanup all registered resources in reverse order.

        This method attempts to clean up all resources even if individual
        cleanup operations fail. Cleanup errors are logged but do not prevent
        other resources from being cleaned up.

        Resources are cleaned up by calling:
        1. close() method if available
        2. shutdown(wait=True) method if available
        3. Otherwise, the resource is skipped

        After cleanup, the resource list is cleared.
        """
        if not self._resources:
            return

        errors: list[tuple[str, Exception]] = []

        # Cleanup in reverse order (LIFO)
        for name, resource in reversed(self._resources):
            try:
                if hasattr(resource, "close"):
                    logger.debug(f"Closing resource: {name}")
                    resource.close()
                elif hasattr(resource, "shutdown"):
                    logger.debug(f"Shutting down resource: {name}")
                    resource.shutdown(wait=True)
                else:
                    logger.debug(f"Resource {name} has no cleanup method, skipping")
            except Exception as e:
                errors.append((name, e))
                logger.warning(f"Error cleaning up resource {name}: {e}")

        # Clear the resource list
        self._resources.clear()

        # Log summary if there were errors
        if errors:
            error_summary = ", ".join(f"{name}: {e!s}" for name, e in errors)
            logger.warning(f"Cleanup completed with errors: {error_summary}")
        else:
            logger.debug("All resources cleaned up successfully")

    def __enter__(self) -> ResourceManager:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> bool:
        """Exit context manager and cleanup resources.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred

        Returns:
            False to propagate any exception that occurred
        """
        self._cleanup_resources()
        return False
