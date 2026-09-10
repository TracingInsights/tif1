"""Trace which thread issues telemetry GETs and when prefetch waves start/finish.

Patches tif1.http_session to record (thread, url) with timestamps, plus hooks
on the bulk-prefetch entry to see whether the background wave runs at all.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path

cache_dir = tempfile.mkdtemp(prefix="tif1-diag2-cache-")
os.environ["TIF1_CACHE_DIR"] = cache_dir

if len(sys.argv) > 1:
    sys.path.insert(0, str(Path(sys.argv[1]).resolve()))

import tif1  # noqa: E402
from tif1 import http_session  # noqa: E402

_t0 = time.perf_counter()
_gets: list[tuple[str, str, float]] = []
_events: list[tuple[str, str, float]] = []


def _note(event: str) -> None:
    _events.append((event, threading.current_thread().name, time.perf_counter() - _t0))


class _CountingSession:
    def __init__(self, inner) -> None:
        self._inner = inner

    def get(self, url, *args, **kwargs):
        _gets.append((threading.current_thread().name, url, time.perf_counter() - _t0))
        return self._inner.get(url, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


_orig_get_session = http_session.get_session


def _patched_get_session():
    return _CountingSession(_orig_get_session())


http_session.get_session = _patched_get_session
import tif1.async_fetch as af  # noqa: E402

af.get_http_session = _patched_get_session

# Hook the bulk prefetch entry/exit
from tif1 import core  # noqa: E402

_orig_prefetch = core.Session._prefetch_all_loaded_laps_telemetry


def _traced_prefetch(self, *, ultra_cold: bool) -> None:
    _note(f"bulk-prefetch-start ultra_cold={ultra_cold}")
    try:
        _orig_prefetch(self, ultra_cold=ultra_cold)
    finally:
        _note("bulk-prefetch-end")


core.Session._prefetch_all_loaded_laps_telemetry = _traced_prefetch

_note("import-done")
session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
_note("get-session-done")
_ = session.laps
_note("laps-done")
tel = session.fetch_all_laps_telemetry()
_note("telemetry-done")

thread_counts = Counter(name for name, _, _ in _gets)
main_thread = threading.main_thread().name
main_gets = [t for name, _, t in _gets if name == main_thread]
bg_gets = [t for name, _, t in _gets if name != main_thread]
print(
    json.dumps(
        {
            "events": [(e, th, round(t, 2)) for e, th, t in _events],
            "gets_by_thread": dict(thread_counts),
            "main_wave_span_s": [round(min(main_gets), 2), round(max(main_gets), 2)]
            if main_gets
            else None,
            "bg_wave_span_s": [round(min(bg_gets), 2), round(max(bg_gets), 2)] if bg_gets else None,
            "telemetry_frames": len(tel),
        },
        indent=2,
    )
)
