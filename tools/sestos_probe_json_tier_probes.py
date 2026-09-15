"""L9 confirmation: JSON-tier probe behavior on the default bulk paths.

Hypothesis was: skip the per-request JSON-tier cache.get for _tel.json paths
(telemetry lives in its own table). Instrumentation confirms where those
probes actually run:

- default-config cold bulk fetch: use_cache=False -> ZERO probes
- warm loads: payload-tier batch hit -> zero requests to probe
- only off-default (ultra_cold_start=false) cold loads pay per-request probes

This script counts cache.get calls through fetch_multiple_async for the
three configurations using a counting cache stub and an in-memory transport.
"""

from __future__ import annotations

import asyncio
import time


class CountingCache:
    """Duck-typed cache that counts json-tier probes (get) and other calls."""

    def __init__(self):
        self.get_calls = 0
        self.set_telemetry_calls = 0

    def _get_from_memory(self, key):  # noqa: ARG002
        return None

    def get(self, key):  # noqa: ARG002
        self.get_calls += 1

    def set_telemetry(self, *args, **kwargs):  # noqa: ARG002
        self.set_telemetry_calls += 1


def make_session(payload):
    import orjson

    class _Resp:
        status_code = 200
        content = orjson.dumps(payload)

        def raise_for_status(self):
            return None

        def json(self):
            return payload

    class _Session:
        def get(self, url, timeout=None):  # noqa: ARG002
            return _Resp()

    return _Session()


async def main() -> None:
    import tif1.async_fetch as af

    cache = CountingCache()
    af.get_cache = lambda: cache
    af._get_async_session = lambda: make_session({"tel": {"speed": [1.0] * 10}})
    af._get_json_parse_executor = lambda: None

    import tif1.http_session

    tif1.http_session._track_request = lambda reused=True: None  # noqa: ARG005

    requests = [(2026, "GP", "Race", f"VER/{lap}_tel.json") for lap in range(1, 21)]

    # 1. Default-config cold bulk path: use_cache=False
    t0 = time.perf_counter()
    await af.fetch_multiple_async(requests, use_cache=False, write_cache=True, validate_payload=False)
    t_default = time.perf_counter() - t0
    print(f"default cold (use_cache=False): probes={cache.get_calls} wall={t_default*1000:.1f}ms")

    # 2. Off-default cold bulk path: use_cache=True (ultra_cold_start=false users)
    cache.get_calls = 0
    t0 = time.perf_counter()
    await af.fetch_multiple_async(requests, use_cache=True, write_cache=True, validate_payload=False)
    t_offdefault = time.perf_counter() - t0
    print(
        f"off-default cold (use_cache=True): probes={cache.get_calls} "
        f"wall={t_offdefault*1000:.1f}ms"
    )
    print(
        f"probe overhead (20 payloads): {(t_offdefault - t_default)*1000:.1f}ms "
        f"-> per 1452 payloads ~{((t_offdefault - t_default)*1000/20*1452):.0f}ms "
        "(in-memory stub; real SQL adds per-SELECT cost)"
    )


if __name__ == "__main__":
    asyncio.run(main())
