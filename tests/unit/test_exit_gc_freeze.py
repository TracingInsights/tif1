"""The exit hook that freezes the heap before CPython's final collections (Delphi D1)."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

CHILD = textwrap.dedent(
    """
    import atexit, gc, os, sys

    def report():
        # Registered before tif1 is imported, so this runs after tif1's hook.
        sys.stdout.write("freeze_count=%d\\n" % gc.get_freeze_count())
        sys.stdout.flush()

    atexit.register(report)
    import tif1

    if os.environ.get("CHILD_USE_CACHE") == "1":
        from tif1.cache import get_cache
        import tif1.cache as cache_module
        get_cache().set("probe/key", {"x": 1})
        real_freeze = gc.freeze

        def spy_freeze():
            # tif1's cache cleanup hook (registered later) must have run first.
            sys.stdout.write("cache_closed_before_freeze=%s\\n" % (cache_module._cache is None))
            real_freeze()

        gc.freeze = spy_freeze
    if os.environ.get("CHILD_SET_CONFIG") == "1":
        tif1.get_config().set("exit_gc_freeze", False)
    """
)


def _run(tmp_path: Path, **env: str) -> str:
    child_env = {
        **os.environ,
        "TIF1_CACHE_DIR": str(tmp_path / "cache"),
        "TIF1_OFFLINE_MODE": "true",
        "HOME": str(tmp_path),
        **env,
    }
    proc = subprocess.run(
        [sys.executable, "-c", CHILD],
        check=True,
        capture_output=True,
        text=True,
        env=child_env,
        timeout=120,
    )
    return proc.stdout


@pytest.mark.unit
def test_heap_is_frozen_at_exit_by_default(tmp_path: Path) -> None:
    out = _run(tmp_path)
    count = int(out.split("freeze_count=")[1].split()[0])
    assert count > 0


@pytest.mark.unit
def test_environment_variable_turns_the_hook_off(tmp_path: Path) -> None:
    out = _run(tmp_path, TIF1_EXIT_GC_FREEZE="0")
    assert "freeze_count=0" in out


@pytest.mark.unit
def test_setting_turns_the_hook_off(tmp_path: Path) -> None:
    out = _run(tmp_path, CHILD_SET_CONFIG="1")
    assert "freeze_count=0" in out


@pytest.mark.unit
def test_cache_is_closed_before_the_freeze(tmp_path: Path) -> None:
    out = _run(tmp_path, CHILD_USE_CACHE="1")
    assert "cache_closed_before_freeze=True" in out
    count = int(out.split("freeze_count=")[1].split()[0])
    assert count > 0
