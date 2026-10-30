"""Count HTTP GETs issued during a full cold load() to detect duplicate fetch waves.

Patches tif1.http_session to count every request URL. Runs the same phases as
the Monaco cold benchmark in-process (throwaway cache dir).
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path

cache_dir = tempfile.mkdtemp(prefix="tif1-diag-cache-")
os.environ["TIF1_CACHE_DIR"] = cache_dir

if len(sys.argv) > 1:
    sys.path.insert(0, str(Path(sys.argv[1]).resolve()))

import tif1  # noqa: E402
from tif1 import http_session  # noqa: E402

_counts: Counter[str] = Counter()
_order: list[str] = []


class _CountingSession:
    """Duck-typed wrapper around the shared niquests session."""

    def __init__(self, inner) -> None:
        self._inner = inner

    def get(self, url, *args, **kwargs):
        _counts[url] += 1
        _order.append(url)
        return self._inner.get(url, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


_orig_get_session = http_session.get_session


def _patched_get_session():
    return _CountingSession(_orig_get_session())


http_session.get_session = _patched_get_session
import tif1.async_fetch as af  # noqa: E402

af.get_http_session = _patched_get_session

session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
_ = session.laps
tel = session.fetch_all_laps_telemetry()

total = sum(_counts.values())
dupes = {u: c for u, c in _counts.items() if c > 1}
tel_urls = [u for u in _counts if "_tel.json" in u]
tel_dupes = {u: c for u, c in dupes.items() if "_tel.json" in u}
print(
    json.dumps(
        {
            "total_gets": total,
            "unique_urls": len(_counts),
            "duplicate_urls": len(dupes),
            "tel_urls": len(tel_urls),
            "tel_dupes": len(tel_dupes),
            "tel_dup_extra_gets": sum(c - 1 for c in tel_dupes.values()),
            "telemetry_frames": len(tel),
        },
        indent=2,
    )
)
