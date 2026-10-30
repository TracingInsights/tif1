"""The Pyodide dependency contract: pyproject markers against tools/pyodide_target.json.

micropip installs tif1 in the browser from the ``sys_platform == 'emscripten'``
requirement lines. Native packages come from the Pyodide distribution, so each
twin's specifier has to accept the version Pyodide ships; packages without a
WebAssembly build must be excluded there. A dependabot floor bump that breaks
the browser install fails here, without a network.
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

ROOT = Path(__file__).resolve().parents[2]
TARGET = json.loads((ROOT / "tools" / "pyodide_target.json").read_text(encoding="utf-8"))

# No WebAssembly wheel exists: these must never apply under Pyodide.
NATIVE_ONLY = {"niquests", "polars", "rapidfuzz", "nest-asyncio2", "tif1-native"}
# Pure-Python wheels micropip fetches from PyPI; no Pyodide twin is needed.
PURE_PYTHON = {"typer", "rich", "typing-extensions", "seaborn", "timple"}

PYODIDE_ENV = {
    "sys_platform": "emscripten",
    "platform_system": "Emscripten",
    "platform_machine": "wasm32",
    "platform_release": "",
    "platform_version": "",
    "os_name": "posix",
    "python_version": ".".join(TARGET["python"].split(".")[:2]),
    "python_full_version": TARGET["python"],
    "implementation_name": "cpython",
    "implementation_version": TARGET["python"],
    "platform_python_implementation": "CPython",
    "extra": "",
}
NATIVE_ENV = {
    **PYODIDE_ENV,
    "sys_platform": "linux",
    "platform_system": "Linux",
    "platform_machine": "x86_64",
    "python_version": "3.12",
    "python_full_version": "3.12.0",
    "implementation_version": "3.12.0",
}


def _requirements() -> list[Requirement]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return [Requirement(line) for line in data["project"]["dependencies"]]


def _applies(requirement: Requirement, env: dict[str, str]) -> bool:
    return requirement.marker is None or requirement.marker.evaluate(env)


def _pyodide_requirements() -> list[Requirement]:
    return [req for req in _requirements() if _applies(req, PYODIDE_ENV)]


def test_native_only_packages_are_excluded_from_pyodide() -> None:
    names = {canonicalize_name(req.name) for req in _pyodide_requirements()}
    assert not names & NATIVE_ONLY


def test_native_only_packages_still_install_natively() -> None:
    names = {canonicalize_name(req.name) for req in _requirements() if _applies(req, NATIVE_ENV)}
    assert names >= NATIVE_ONLY


@pytest.mark.parametrize("requirement", _pyodide_requirements(), ids=str)
def test_pyodide_requirement_accepts_the_shipped_build(requirement: Requirement) -> None:
    name = canonicalize_name(requirement.name)
    shipped = TARGET["packages"].get(name)
    if shipped is None:
        assert name in PURE_PYTHON, (
            f"{requirement}: not in tools/pyodide_target.json and not pure Python; "
            "add an emscripten twin with a major-only floor (see pyproject.toml)"
        )
        return
    assert requirement.specifier.contains(Version(shipped), prereleases=True), (
        f"{requirement} rejects the Pyodide {TARGET['pyodide']} build ({shipped}); "
        "the browser install would fail"
    )


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, True),
        ({"platform_machine": "aarch64"}, True),
        (
            {"sys_platform": "darwin", "platform_system": "Darwin", "platform_machine": "arm64"},
            True,
        ),
        (
            {"sys_platform": "win32", "platform_system": "Windows", "platform_machine": "AMD64"},
            True,
        ),
        (
            {"sys_platform": "win32", "platform_system": "Windows", "platform_machine": "ARM64"},
            True,
        ),
        ({"platform_machine": "ppc64le"}, False),
        ({"sys_platform": "freebsd14", "platform_system": "FreeBSD"}, False),
        ({"platform_python_implementation": "PyPy", "implementation_name": "pypy"}, False),
    ],
)
def test_native_decoder_follows_the_wheel_matrix(overrides: dict[str, str], expected: bool) -> None:
    """tif1-native applies exactly where .github/workflows/native.yml publishes wheels."""
    (requirement,) = [r for r in _requirements() if canonicalize_name(r.name) == "tif1-native"]
    assert _applies(requirement, {**NATIVE_ENV, **overrides}) is expected


def test_pyodide_transport_dependency_is_declared() -> None:
    names = {canonicalize_name(req.name) for req in _pyodide_requirements()}
    assert "urllib3" in names
