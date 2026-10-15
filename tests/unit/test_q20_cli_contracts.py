"""Contract tests for Pergamon Q20 (CLI fixed costs on a warm cache).

- ``tif1.cache`` (``tif1 cache-info``/``cache-clear``) imports neither pandas
  nor numpy nor pyarrow: ``core_utils`` re-exports lazily and the pyarrow
  exception class is resolved on first use.
- The schedule (``tif1.events``) answers event and session names without
  pandas; ``Event``/``EventSchedule`` live in ``tif1.event_frames`` and keep
  ``tif1.events`` as their public module.
- ``tif1 fastest`` adopts the laps frame tier when it is warm.
"""

from __future__ import annotations

import os
import pickle
import subprocess
import sys
import textwrap
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
from typer.testing import CliRunner

import tif1.events as events_mod
from tif1 import cache as cache_mod
from tif1.cli import _adopt_warm_laps, app


def _fresh_python(code: str, tmp_path) -> str:
    env = dict(os.environ, TIF1_CACHE_DIR=str(tmp_path), PYTHONDONTWRITEBYTECODE="1")
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    )
    return proc.stdout.strip()


def test_cache_module_imports_no_frame_libraries(tmp_path):
    out = _fresh_python(
        """
        import sys
        import tif1.cache
        cache = tif1.cache.get_cache()
        print(sorted(m for m in ("pandas", "numpy", "pyarrow") if m in sys.modules))
        """,
        tmp_path,
    )
    assert out == "[]"


def test_schedule_names_need_no_pandas(tmp_path):
    # 2024 is served from the vendored schedule files (no network).
    out = _fresh_python(
        """
        import sys
        from tif1.events import _build_events_for_year, get_sessions
        names = _build_events_for_year(2024)
        sessions = get_sessions(2024, "Spa")  # alias -> Belgian Grand Prix
        print(len(names) > 20, sessions[-1], "pandas" in sys.modules)
        """,
        tmp_path,
    )
    assert out == "True Race False"


def test_core_utils_reexports_resolve_lazily():
    from tif1 import core_utils
    from tif1.core_utils import constants, helpers

    for name in core_utils.__all__:
        source = constants if name.isupper() else helpers
        assert getattr(core_utils, name) is getattr(source, name)
    with pytest.raises(AttributeError):
        _ = core_utils.not_an_export


def test_arrow_error_is_pyarrow_exception():
    import pyarrow as pa

    assert cache_mod._arrow_error() is pa.ArrowException


def test_event_classes_keep_their_public_module_and_pickle():
    from tif1 import event_frames

    assert events_mod.Event is event_frames.Event
    assert events_mod.EventSchedule is event_frames.EventSchedule
    assert events_mod.Event.__module__ == "tif1.events"
    assert events_mod.EventSchedule.__module__ == "tif1.events"
    with pytest.raises(AttributeError):
        _ = events_mod.NotAnEvent

    schedule = events_mod.get_event_schedule(2024)
    assert isinstance(schedule, events_mod.EventSchedule)
    restored = pickle.loads(pickle.dumps(schedule))
    assert type(restored) is events_mod.EventSchedule
    assert restored.equals(schedule)
    assert restored.year == schedule.year
    assert list(schedule["EventName"]) == events_mod._build_events_for_year(2024)


@pytest.mark.parametrize("query", ["Belgian Grand Prix", "spa", "Silverstone", "Monacco"])
def test_name_resolution_matches_the_event_object(query):
    event = events_mod.get_event_by_name(2024, query)
    assert isinstance(event, events_mod.Event)
    assert str(event.EventName) == events_mod._resolve_event_name(2024, query)
    assert events_mod.get_sessions(2024, query) == events_mod.get_sessions(
        2024, str(event.EventName)
    )


def test_name_resolution_errors_are_unchanged():
    with pytest.raises(events_mod.EventNotFoundError):
        events_mod.get_sessions(2024, "Moon GP")
    with pytest.raises(events_mod.EventNotFoundError):
        events_mod.get_event_by_name(2024, "Moon GP")


def test_events_command_prints_the_schedule_names():
    result = CliRunner().invoke(app, ["events", "2024"])
    assert result.exit_code == 0
    for name in events_mod._build_events_for_year(2024):
        assert name in result.stdout


class _FakeSession:
    def __init__(self, tier: Any) -> None:
        self._laps = None
        self._tier = tier
        self.tier_reads = 0

    def _laps_from_frame_tier(self) -> Any:
        self.tier_reads += 1
        return self._tier


def test_fastest_adopts_warm_laps_frame():
    frame = pd.DataFrame({"Driver": ["VER"], "LapTime": [pd.Timedelta(seconds=72.5)]})
    sess = _FakeSession(frame)
    _adopt_warm_laps(sess)
    assert sess._laps is frame

    cold = _FakeSession(None)
    _adopt_warm_laps(cold)
    assert cold._laps is None
    assert cold.tier_reads == 1

    loaded = SimpleNamespace(_laps=frame, _laps_from_frame_tier=None)
    _adopt_warm_laps(loaded)  # already loaded: the tier is not consulted
    assert loaded._laps is frame
