"""Async data loading example.

``session.laps`` runs the same async fetch pipeline internally, so it is not
slower than ``laps_async()``. Use ``await session.laps_async()`` when your code
already runs inside an event loop, for example in an async web handler or a
Jupyter cell with top-level await.
"""

import asyncio
import time

import tif1


async def load_data_async():
    """Load laps with the async API."""
    # enable_cache=False forces a fetch from the CDN so both timings are cold
    session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", enable_cache=False)

    start = time.time()
    laps = await session.laps_async()
    elapsed = time.time() - start

    print(f"✓ Async: Loaded {len(laps)} laps in {elapsed:.2f}s")
    return laps, elapsed


def load_data_sync():
    """Load laps with the sync property."""
    session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", enable_cache=False)

    start = time.time()
    laps = session.laps
    elapsed = time.time() - start

    print(f"✓ Sync:  Loaded {len(laps)} laps in {elapsed:.2f}s")
    return laps, elapsed


if __name__ == "__main__":
    print("=" * 60)
    print("ASYNC vs SYNC LOADING COMPARISON")
    print("=" * 60)
    print("\nBoth sessions use enable_cache=False, so both fetch from the CDN.")
    print("The first fetch also opens the HTTPS connection.\n")

    # Test async loading
    print("Testing async loading...")
    laps_async, time_async = asyncio.run(load_data_async())

    # Test sync loading
    print("\nTesting sync loading...")
    laps_sync, time_sync = load_data_sync()

    # Show comparison
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Async time: {time_async:.2f}s")
    print(f"Sync time:  {time_sync:.2f}s")
    print(f"Same laps:  {laps_async.equals(laps_sync)}")
    print("\n💡 Tip: Both paths use the same async fetcher; pick the one that fits your code.")
