"""K9 deterministic harness: missing-file retry storm, current vs candidate.

Simulates the exact live condition measured on the warm path: jsDelivr returns
403 (rate-limit window) for a file that 404s on every mirror. The fake session
answers instantly (zero latency), so all measured wall time is retry/backoff
sleep — the mechanism that produced the 5.7-12.5 s warm-load tail.

Compares the shipped retry behavior (3 attempts x backoff sleeps, NetworkError)
against the K9 candidate (all-4xx exhaustion -> DataNotFoundError, no retry).
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time

os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-k9-")
# Deterministic sleeps: no jitter.
os.environ["TIF1_RETRY_JITTER"] = "false"

CANDIDATE = len(sys.argv) > 1 and sys.argv[1] == "--candidate"
if CANDIDATE:
    sys.path.insert(0, "src")

import tif1.async_fetch as af  # noqa: E402
from tif1.exceptions import DataNotFoundError, NetworkError  # noqa: E402

probes: list[str] = []


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self):
        import niquests  # noqa: F401

        if self.status_code >= 400:
            # niquests HTTPError carries .response.status_code like production
            from niquests.exceptions import HTTPError

            raise HTTPError(f"{self.status_code} for url", response=self, request=None)


class _FakeSession:
    """jsDelivr 403s; HuggingFace and StaticDelivr 404 (missing everywhere)."""

    def get(self, url, timeout=None):  # noqa: ARG002
        probes.append(url)
        if "jsdelivr" in url:
            return _FakeResponse(403)
        return _FakeResponse(404)

    def close(self):
        pass


af._get_async_session = lambda: _FakeSession()


async def run_one():
    return await af.fetch_json_async(2026, "Monaco%20Grand%20Prix", "Race", "STR/58_tel.json")


t0 = time.perf_counter()
try:
    asyncio.run(run_one())
    outcome = "success (unexpected)"
except DataNotFoundError:
    outcome = f"DataNotFoundError (fatal, no retry) after {time.perf_counter() - t0:.2f}s"
except NetworkError:
    outcome = f"NetworkError (retryable) after {time.perf_counter() - t0:.2f}s"

print(f"variant={'candidate' if CANDIDATE else 'current'}")
print(f"outcome: {outcome}")
print(f"probes: {len(probes)} CDN requests")
for p in probes:
    print(f"  {p}")
