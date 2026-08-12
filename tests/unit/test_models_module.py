"""Tests for the candidate-3 model seam: ``tif1.models`` as the canonical module.

The DataFrame model family lives in ``tif1.models`` and depends on the session
only through the narrow :class:`tif1.models.TelemetryProvider` protocol.
``tif1.core`` re-exports the classes for backward compatibility.
"""

from __future__ import annotations

import sys
import typing

import pytest


def test_models_module_defines_model_family() -> None:
    """All moved model classes are defined in tif1.models, not re-exported from core."""
    from tif1 import models

    for name in (
        "CircuitInfo",
        "Driver",
        "DriverResult",
        "Lap",
        "Laps",
        "LazyTelemetryDict",
        "SessionResults",
        "Telemetry",
        "TelemetryProvider",
        "_IterLapResult",
        "_LapInternal",
    ):
        assert getattr(models, name).__module__ == "tif1.models", name


def test_core_reexports_model_identity() -> None:
    """tif1.core aliases are the identical tif1.models classes (not copies)."""
    from tif1 import core, models

    for name in (
        "CircuitInfo",
        "Driver",
        "DriverResult",
        "Lap",
        "Laps",
        "LazyTelemetryDict",
        "SessionResults",
        "Telemetry",
        "_IterLapResult",
        "_LapInternal",
    ):
        assert getattr(core, name) is getattr(models, name), name


def test_models_module_does_not_import_core() -> None:
    """models.py must never import tif1.core at runtime (import-cycle guard)."""
    for mod_name in list(sys.modules):
        if mod_name == "tif1" or mod_name.startswith("tif1."):
            del sys.modules[mod_name]

    import tif1.models

    assert "tif1.core" not in sys.modules, "tif1.models must not import tif1.core at runtime"

    # Restore a fully-imported package for subsequent tests in this process.
    import tif1
    import tif1.core  # noqa: F401


def test_circuit_info_lazy_export_targets_models() -> None:
    """The package lazy-export map must point CircuitInfo at tif1.models."""
    import tif1

    assert tif1._LAZY_EXPORTS["CircuitInfo"] == ("tif1.models", "CircuitInfo")
    assert tif1.CircuitInfo.__module__ == "tif1.models"


def test_session_satisfies_telemetry_provider_protocol() -> None:
    """Session is the production adapter for the TelemetryProvider seam."""
    from tif1 import models
    from tif1.core import Session

    session = Session(2021, "Bahrain Grand Prix", "FP1", enable_cache=False)

    # Every protocol member must be backed by a real attribute on Session.
    # isinstance() against a runtime_checkable protocol performs hasattr() on
    # the *instance*, which fires lazy-loading property getters (e.g.
    # ``_drivers_data``) that fetch payloads over the network — so conformance
    # is asserted structurally on the class instead: methods and properties
    # resolve on the class without running getters, and plain data members
    # (``year``, ``_laps``, ...) live in the instance ``__dict__``.
    protocol_attrs = getattr(models.TelemetryProvider, "__protocol_attrs__", None)
    if protocol_attrs is None:
        # Python 3.11 computes the member set on the fly (private typing API).
        get_protocol_attrs = getattr(typing, "_get_protocol_attrs", None)
        protocol_attrs = (
            get_protocol_attrs(models.TelemetryProvider) if get_protocol_attrs else set()
        )
    assert protocol_attrs, "expected a runtime_checkable protocol exposing its members"
    for member in protocol_attrs:
        present = hasattr(Session, member) or member in session.__dict__
        assert present, f"Session is missing protocol member {member!r}"


def test_telemetry_provider_rejects_non_conforming_objects() -> None:
    """Objects lacking the protocol's methods fail the runtime check."""
    from tif1 import models

    assert not isinstance(object(), models.TelemetryProvider)


def test_models_public_all() -> None:
    """models.__all__ covers the public model family and the protocol."""
    from tif1 import models

    assert set(models.__all__) == {
        "CircuitInfo",
        "Driver",
        "DriverResult",
        "Lap",
        "Laps",
        "LazyTelemetryDict",
        "SessionResults",
        "Telemetry",
        "TelemetryProvider",
    }


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
