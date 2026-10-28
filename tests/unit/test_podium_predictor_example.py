"""Discover optional example tests without leaking their standalone module names."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_DIRECTORY = REPOSITORY_ROOT / "examples" / "podium_predictor"
OPTIONAL_MODEL_MODULES = (
    "numpy",
    "pandas",
    "requests",
    "sklearn",
    "scipy",
    "xgboost",
    "matplotlib",
)
ISOLATED_UNITTEST_RUNNER = '''
"""Run standalone example tests in an isolated interpreter with real sockets blocked."""
import builtins
import socket
import sys
import unittest

def blocked_network(*_args: object, **_kwargs: object) -> None:
    """Reject accidental real network access in this unit-only subprocess."""
    raise OSError("network access is blocked in podium predictor unit tests")

socket.socket.connect = blocked_network
socket.socket.connect_ex = blocked_network
socket.socket.sendto = blocked_network
socket.getaddrinfo = blocked_network
if sys.argv[2] == "test_tif1_adapter.py":
    original_import = builtins.__import__

    def base_only_import(name: str, *args: object, **kwargs: object) -> object:
        """Keep the adapter smoke independent of optional model packages."""
        if name.partition(".")[0] in {"sklearn", "xgboost"}:
            raise AssertionError("adapter smoke imported an optional model package: " + name)
        return original_import(name, *args, **kwargs)

    builtins.__import__ = base_only_import
suite = unittest.defaultTestLoader.discover(sys.argv[1], pattern=sys.argv[2])
if suite.countTestCases() == 0:
    raise SystemExit("No podium predictor tests were discovered")
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(0 if result.wasSuccessful() else 1)
'''


def optional_model_skip_reason() -> str | None:
    """Check optional requirements without importing numerical or model packages."""
    if sys.version_info < (3, 12):
        return "the optional podium model example requires Python 3.12 or newer"
    missing = [name for name in OPTIONAL_MODEL_MODULES if importlib.util.find_spec(name) is None]
    if missing:
        return "optional podium model dependencies are not installed: " + ", ".join(missing)
    return None


def run_example_tests(pattern: str) -> None:
    """Execute an example test pattern without polluting core test imports or caches."""
    environment = os.environ.copy()
    environment["TIF1_CDN_PRECONNECT"] = "false"
    environment["PYTHONUTF8"] = "1"
    with tempfile.TemporaryDirectory(dir=REPOSITORY_ROOT) as directory:
        isolated = Path(directory)
        environment["TIF1_CACHE_DIR"] = str(isolated / "tif1-cache")
        environment["MPLCONFIGDIR"] = str(isolated / "matplotlib-cache")
        completed = subprocess.run(
            [sys.executable, "-c", ISOLATED_UNITTEST_RUNNER, str(EXAMPLE_DIRECTORY), pattern],
            cwd=REPOSITORY_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=False,
            timeout=180,
        )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_tif1_adapter_without_optional_model_imports() -> None:
    """Exercise the public-API adapter on every supported core Python version."""
    run_example_tests("test_tif1_adapter.py")


def test_optional_podium_model_suite() -> None:
    """Run the complete offline model, cache, report and adapter suite when installed."""
    reason = optional_model_skip_reason()
    if reason is not None:
        pytest.skip(reason)
    run_example_tests("test_*.py")


def test_optional_dependency_guard_does_not_import_models(monkeypatch: pytest.MonkeyPatch) -> None:
    """Report an absent optional dependency by metadata lookup without importing it."""
    monkeypatch.setattr(sys, "version_info", (3, 12, 0))
    looked_up = []

    def find_optional(name: str) -> object | None:
        """Record discovery queries and emulate a base install without XGBoost."""
        looked_up.append(name)
        return None if name == "xgboost" else object()

    monkeypatch.setattr(importlib.util, "find_spec", find_optional)
    reason = optional_model_skip_reason()
    assert reason is not None
    assert "xgboost" in reason
    assert looked_up == list(OPTIONAL_MODEL_MODULES)


def test_python_311_skips_optional_models_before_dependency_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Avoid resolving optional model modules on the supported Python 3.11 base job."""
    monkeypatch.setattr(sys, "version_info", (3, 11, 0))

    def unexpected_lookup(_name: str) -> None:
        """Fail if a Python 3.11 core run inspects the optional model environment."""
        pytest.fail("optional models should not be inspected on Python 3.11")

    monkeypatch.setattr(importlib.util, "find_spec", unexpected_lookup)
    reason = optional_model_skip_reason()
    assert reason is not None
    assert "Python 3.12" in reason
