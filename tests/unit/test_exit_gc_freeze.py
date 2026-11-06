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

    calls = []
    exiting = []
    real_freeze = gc.freeze

    def spy_freeze():
        # CPython 3.12 freezes a few hundred objects of its own at exit, so the
        # freeze count alone cannot tell whether tif1's hook ran: its call is
        # counted here instead. Only calls made once exit began count: tif1's
        # imports promote their objects with gc.freeze(); gc.unfreeze() at
        # runtime (Naxos N10).
        if not exiting:
            real_freeze()
            return
        calls.append(1)
        if os.environ.get("CHILD_USE_CACHE") == "1":
            import tif1.cache as cache_module
            # tif1's cache cleanup hook (registered later) must have run first.
            sys.stdout.write("cache_closed_before_freeze=%s\\n" % (cache_module._cache is None))
        real_freeze()

    gc.freeze = spy_freeze

    def report():
        # Registered before tif1 is imported, so this runs after tif1's hook.
        sys.stdout.write("freeze_calls=%d freeze_count=%d\\n" % (len(calls), gc.get_freeze_count()))
        sys.stdout.flush()

    atexit.register(report)
    import tif1

    if os.environ.get("CHILD_USE_CACHE") == "1":
        from tif1.cache import get_cache
        get_cache().set("probe/key", {"x": 1})
    if os.environ.get("CHILD_SET_CONFIG") == "1":
        tif1.get_config().set("exit_gc_freeze", False)
    # Registered last, so it runs first at exit: before tif1's hooks.
    atexit.register(lambda: exiting.append(1))
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
    assert "freeze_calls=1 " in out
    count = int(out.split("freeze_count=")[1].split()[0])
    assert count > 0


@pytest.mark.unit
def test_environment_variable_turns_the_hook_off(tmp_path: Path) -> None:
    out = _run(tmp_path, TIF1_EXIT_GC_FREEZE="0")
    assert "freeze_calls=0 " in out


@pytest.mark.unit
def test_setting_turns_the_hook_off(tmp_path: Path) -> None:
    out = _run(tmp_path, CHILD_SET_CONFIG="1")
    assert "freeze_calls=0 " in out


@pytest.mark.unit
def test_cache_is_closed_before_the_freeze(tmp_path: Path) -> None:
    out = _run(tmp_path, CHILD_USE_CACHE="1")
    assert "cache_closed_before_freeze=True" in out
    assert "freeze_calls=1 " in out
