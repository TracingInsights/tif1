"""Delphi D2: a warm pandas ``get_session`` imports only what it uses.

The session startup used to import the async fetch layer and the HTTP
session module, the CDN manager, the pandas block-assembly probe (which
builds probe frames through pyarrow's pandas shim), ``tif1.types``, the
Ergast value parsers and ``importlib.resources`` although a cached session
touches none of them before its first telemetry frame (or never). The first
test runs ``get_session`` in a fresh interpreter, offline, against an empty
cache, and lists what the import of ``tif1`` plus the call added to
``sys.modules``; the others pin the lazy seams the trim introduced.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from importlib.resources import files
from pathlib import Path

import pytest

import tif1
from tif1 import core, events
from tif1.core_utils import frame_codec, pandas_blocks

_CHILD = r"""
import json
import sys

before = set(sys.modules)
import tif1

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
added = sorted(set(sys.modules) - before)
report = {
    "added": added,
    "pandas": "pandas" in sys.modules,
    "models": "tif1.models" in sys.modules,
    "event": session.event_name,
    "session": session.session_name,
    "lib": session.lib,
}
print(json.dumps(report))
"""

#: Modules a warm pandas ``get_session`` must not import (nothing cached
#: needs them before the first telemetry frame, if at all).
_NOT_ON_THE_WARM_STARTUP_PATH = (
    "asyncio",
    "importlib.resources",
    "niquests",
    "polars",
    "pyarrow.pandas_compat",
    "pydantic",
    "rapidfuzz",
    "rich",
    "tif1.async_fetch",
    "tif1.cdn",
    "tif1.core_utils.frame_codec",
    "tif1.core_utils.pandas_blocks",
    "tif1.ergast.structure",
    "tif1.fuzzy",
    "tif1.http_session",
    "tif1.plotting",
    "tif1.types",
    "tif1.validation",
    "typer",
)


def test_warm_pandas_get_session_imports_only_what_it_uses(tmp_path):
    package_root = str(Path(tif1.__file__).resolve().parent.parent)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([package_root, *filter(None, [env.get("PYTHONPATH")])])
    env["HOME"] = str(tmp_path)
    env["USERPROFILE"] = str(tmp_path)
    env["TIF1_CACHE_DIR"] = str(tmp_path / "cache")
    env["TIF1_OFFLINE_MODE"] = "true"
    env["TIF1_PRELOAD_IMPORTS"] = "0"
    completed = subprocess.run(
        [sys.executable, "-c", _CHILD],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        cwd=tmp_path,
        timeout=300,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout.strip().splitlines()[-1])
    assert (report["event"], report["session"], report["lib"]) == (
        "Bahrain Grand Prix",
        "Race",
        "pandas",
    )
    # A pandas session still imports pandas and the model family up front.
    assert report["pandas"] is True
    assert report["models"] is True
    added = set(report["added"])
    unwanted = sorted(name for name in _NOT_ON_THE_WARM_STARTUP_PATH if name in added)
    assert unwanted == [], unwanted


# -- the block-assembly probe runs on first use ------------------------------


def test_direct_blocks_probe_runs_once_on_first_use(monkeypatch):
    calls: list[int] = []
    real = pandas_blocks._probe_direct_blocks

    def _counting() -> bool:
        calls.append(1)
        return real()

    monkeypatch.setattr(pandas_blocks, "_probe_direct_blocks", _counting)
    monkeypatch.setattr(pandas_blocks, "_DIRECT_BLOCKS_OK", None)
    assert pandas_blocks._direct_blocks_ok() is True
    assert pandas_blocks._direct_blocks_ok() is True
    assert pandas_blocks._DIRECT_BLOCKS_OK is True
    assert calls == [1]


def test_direct_blocks_probe_verdict_gates_the_block_route(monkeypatch):
    monkeypatch.setattr(pandas_blocks, "_probe_direct_blocks", lambda: False)
    monkeypatch.setattr(pandas_blocks, "_DIRECT_BLOCKS_OK", None)
    monkeypatch.setattr(frame_codec, "_FAST_LAP_FRAMES", None)

    def _boom(*_args, **_kwargs):
        raise AssertionError("the direct route must not run when the probe failed")

    monkeypatch.setattr(pandas_blocks, "_direct_frame_from_columns", _boom)
    import numpy as np

    frame = pandas_blocks._frame_from_columns(
        [np.array([1.0, 2.0]), np.array([3.0, 4.0])], ["Speed", "RPM"], 2
    )
    assert list(frame.columns) == ["Speed", "RPM"]
    assert pandas_blocks._DIRECT_BLOCKS_OK is False
    assert frame_codec._fast_lap_frames() is False
    assert frame_codec.encode_frame(frame) is None


def test_pinned_probe_verdict_is_honoured_without_probing(monkeypatch):
    monkeypatch.setattr(pandas_blocks, "_DIRECT_BLOCKS_OK", False)

    def _never() -> bool:
        raise AssertionError("a pinned verdict must not re-probe")

    monkeypatch.setattr(pandas_blocks, "_probe_direct_blocks", _never)
    assert pandas_blocks._direct_blocks_ok() is False


def test_load_pandas_blocks_probes_eagerly(monkeypatch):
    from tif1.core_utils import helpers

    monkeypatch.setattr(pandas_blocks, "_DIRECT_BLOCKS_OK", None)
    helpers._load_pandas_blocks()
    assert pandas_blocks._DIRECT_BLOCKS_OK is True
    assert helpers._direct_blocks_ok is pandas_blocks._direct_blocks_ok


# -- core's delegates keep their names (tests patch them) --------------------


def test_core_fetch_multiple_async_delegates_with_keywords(monkeypatch):
    import asyncio

    from tif1 import async_fetch

    seen: dict[str, object] = {}

    async def _fake(requests, **kwargs):
        seen["requests"] = requests
        seen.update(kwargs)
        return [None] * len(requests)

    monkeypatch.setattr(async_fetch, "fetch_multiple_async", _fake)
    requests = [(2025, "Bahrain Grand Prix", "Race", "drivers.json")]
    result = asyncio.run(core.fetch_multiple_async(requests, use_cache=False))
    assert result == [None]
    assert seen == {"requests": requests, "use_cache": False}


def test_core_get_cdn_manager_is_the_shared_singleton():
    from tif1 import cdn

    assert core.get_cdn_manager() is cdn.get_cdn_manager()


def test_payload_loader_resolves_the_cdn_manager_lazily():
    from tif1 import cdn
    from tif1.payload_loader import PayloadLoader

    loader = PayloadLoader(2025, "Bahrain Grand Prix", "Race", enable_cache=False)
    assert loader.cdn_manager is cdn.get_cdn_manager()


def test_ergast_payload_loader_resolves_the_cdn_manager_lazily():
    from tif1 import cdn
    from tif1.ergast.payloads import ErgastPayloadLoader

    assert ErgastPayloadLoader().cdn_manager is cdn.get_cdn_manager()


# -- regex patterns of the startup path compile on first use ------------------


def test_lazy_pattern_compiles_once_and_rebinds_the_global():
    import re

    from tif1.core_utils.lazy_import import LazyPattern, lazy_pattern

    namespace: dict[str, object] = {}
    namespace["PAT"] = lazy_pattern(r"^ab+$", namespace, "PAT", re.IGNORECASE)
    proxy = namespace["PAT"]
    assert isinstance(proxy, LazyPattern)
    assert proxy.fullmatch("ABB") is not None
    compiled = namespace["PAT"]
    assert isinstance(compiled, re.Pattern)
    assert compiled.flags & re.IGNORECASE
    # The proxy keeps working for anything that captured it before the rebind.
    assert proxy.match("x") is None
    assert namespace["PAT"] is compiled


def test_startup_patterns_behave_like_compiled_patterns():
    from tif1.core_utils import session_state

    assert core._ISO_SECONDS.fullmatch("2025-04-13T15:00:00") is not None
    assert core._ISO_SECONDS.fullmatch("2025-04-13") is None
    assert session_state._ISO_TIMESTAMP.fullmatch("2025-04-13T15:00:00.123") is not None
    assert events._ALIAS_NOISE_RE.sub(" ", "bahrain grand prix").split() == ["bahrain"]
    from tif1 import models

    assert models._CLASSIFIED_STATUS_RE.match("+1 Lap") is not None
    assert models._CLASSIFIED_STATUS_RE.match("Accident") is None
    assert models._LEGACY_OFFSET_RE.match("1S") is not None


# -- vendored schedules are read without importlib.resources -----------------


@pytest.mark.parametrize("relative", ["f1schedule/schedule_2025.json", "ergast/schedule_1950.json"])
def test_vendored_schedule_bytes_match_the_package_resource(relative):
    expected = files("tif1").joinpath(f"data/schedules/{relative}").read_bytes()
    assert events._vendored_schedule_bytes(relative) == expected


def test_vendored_schedule_names_list_the_season_files():
    names = events._vendored_schedule_names("f1schedule")
    assert "schedule_2025.json" in names
    assert all(events._F1SCHEDULE_YEAR_PATTERN.match(n) for n in names if n.endswith(".json"))


def test_missing_vendored_schedule_raises_oserror():
    with pytest.raises(FileNotFoundError):
        events._vendored_schedule_bytes("f1schedule/schedule_1899.json")
    assert events._load_vendored_f1schedule_year(1899) is None


def test_vendored_schedules_fall_back_to_importlib_resources(monkeypatch):
    monkeypatch.setattr(events, "_package_dir_on_disk", False)
    expected = files("tif1").joinpath("data/schedules/f1schedule/schedule_2025.json").read_bytes()
    assert events._vendored_schedule_bytes("f1schedule/schedule_2025.json") == expected
    assert "schedule_2025.json" in events._vendored_schedule_names("f1schedule")
