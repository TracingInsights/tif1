"""Async data loading example."""

import asyncio
import time

import tif1


async def load_data_async():
    """Load data asynchronously (4-5x faster)."""
    session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")

    start = time.time()
    laps = await session.laps_async()
    elapsed = time.time() - start

    print(f"✓ Async: Loaded {len(laps)} laps in {elapsed:.2f}s")
    return laps, elapsed


def load_data_sync():
    """Load data synchronously."""
    session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")

    start = time.time()
    laps = session.laps
    elapsed = time.time() - start

    print(f"✓ Sync:  Loaded {len(laps)} laps in {elapsed:.2f}s")
    return laps, elapsed


if __name__ == "__main__":
    print("=" * 60)
    print("ASYNC vs SYNC LOADING COMPARISON")
    print("=" * 60)

    # Clear cache for fair comparison
    cache = tif1.get_cache()
    cache.clear()
    print("\nCache cleared for fair comparison\n")

    # Test async loading
    print("Testing async loading...")
    laps_async, time_async = asyncio.run(load_data_async())

    # Clear cache again
    cache.clear()

    # Test sync loading
    print("\nTesting sync loading...")
    laps_sync, time_sync = load_data_sync()

    # Show comparison
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    speedup = time_sync / time_async if time_async > 0 else 0
    print(f"Async time: {time_async:.2f}s")
    print(f"Sync time:  {time_sync:.2f}s")
    print(f"Speedup:    {speedup:.1f}x faster with async")
    print("\n💡 Tip: Async loading is especially beneficial with cold cache!")
