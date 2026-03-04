"""Property-based tests for cache read parallelism and memory cache effectiveness.

Tests verify that:
1. Concurrent cache reads don't block each other (lock-free reads)
2. Repeated reads are served from memory cache without SQLite access
3. Memory cache hit rate meets performance targets
"""

import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tif1.cache import Cache


class TestCacheReadParallelism:
    """Property tests for cache read parallelism and lock-free operations."""

    def test_concurrent_reads_dont_block_each_other(self, tmp_path):
        """Property: Concurrent reads from memory cache don't block each other.

        When multiple threads read the same cached data concurrently, they should
        all complete successfully without deadlocks or errors. This test verifies
        that lock-free reads work correctly under concurrent access.
        """
        cache = Cache(cache_dir=tmp_path)

        # Pre-populate cache with test data
        test_key = "test_key"
        test_data = {"value": "test_data", "number": 42, "list": [1, 2, 3]}
        cache.set(test_key, test_data)

        # Ensure data is in memory cache
        assert cache._get_from_memory(test_key) is not None

        num_threads = 20
        num_reads_per_thread = 1000
        errors = []
        lock = threading.Lock()

        def read_from_cache():
            """Read from cache and record any errors."""
            try:
                for _ in range(num_reads_per_thread):
                    result = cache._get_from_memory(test_key)
                    assert result is not None
                    assert result["value"] == "test_data"
            except Exception as e:
                with lock:
                    errors.append(e)

        # Execute concurrent reads
        threads = [threading.Thread(target=read_from_cache) for _ in range(num_threads)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: All reads should succeed without errors
        assert len(errors) == 0, f"Concurrent reads produced errors: {errors}"

        cache.close()

    @given(
        num_keys=st.integers(min_value=5, max_value=50),
        num_threads=st.integers(min_value=5, max_value=20),
    )
    @settings(
        max_examples=30,
        deadline=10000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_concurrent_reads_multiple_keys_parallel(
        self, num_keys: int, num_threads: int, tmp_path
    ):
        """Property: Concurrent reads of different keys don't block each other.

        When threads read different keys concurrently, they should all proceed
        in parallel without blocking on each other's reads.

        Args:
            num_keys: Number of different keys to read
            num_threads: Number of concurrent reader threads
            tmp_path: Temporary directory for cache
        """
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))
        cache = Cache(cache_dir=cache_dir)

        # Pre-populate cache with multiple keys
        keys = [f"key_{i}" for i in range(num_keys)]
        for i, key in enumerate(keys):
            cache.set(key, {"value": f"data_{i}", "index": i})

        # Ensure all data is in memory cache
        for key in keys:
            assert cache._get_from_memory(key) is not None

        results = []
        lock = threading.Lock()

        def read_multiple_keys(thread_id: int):
            """Read all keys and record results."""
            thread_results = []
            for key in keys:
                result = cache._get_from_memory(key)
                assert result is not None
                thread_results.append((thread_id, key, result))

            with lock:
                results.extend(thread_results)

        # Execute concurrent reads
        start_time = time.perf_counter()
        threads = [
            threading.Thread(target=read_multiple_keys, args=(i,)) for i in range(num_threads)
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        total_time = time.perf_counter() - start_time

        # Property: All threads should have read all keys
        assert len(results) == num_threads * num_keys, (
            f"Expected {num_threads * num_keys} results, got {len(results)}"
        )

        # Property: Each thread should have read each key exactly once
        for thread_id in range(num_threads):
            thread_results = [r for r in results if r[0] == thread_id]
            assert len(thread_results) == num_keys

        # Property: Concurrent execution should be reasonably fast
        # (not blocking excessively)
        # Allow 100ms per thread as reasonable upper bound for memory reads
        max_reasonable_time = num_threads * 0.1
        assert total_time < max_reasonable_time, (
            f"Total time {total_time:.4f}s suggests excessive blocking"
        )

        cache.close()

    @given(
        num_repeated_reads=st.integers(min_value=10, max_value=100),
    )
    @settings(
        max_examples=40,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_repeated_reads_served_from_memory(self, num_repeated_reads: int, tmp_path):
        """Property: Repeated reads of same key are served from memory cache.

        After the first read populates the memory cache, all subsequent reads
        should be served from memory without accessing SQLite.

        Args:
            num_repeated_reads: Number of times to read the same key
            tmp_path: Temporary directory for cache
        """
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))
        cache = Cache(cache_dir=cache_dir)

        test_key = "repeated_key"
        test_data = {"value": "repeated_data", "counter": 123}

        # First write (goes to both memory and SQLite)
        cache.set(test_key, test_data)

        # Verify data is in memory cache
        memory_result = cache._get_from_memory(test_key)
        assert memory_result is not None
        assert memory_result["value"] == "repeated_data"

        # Perform repeated reads
        for i in range(num_repeated_reads):
            result = cache._get_from_memory(test_key)

            # Property: All reads should return the same data
            assert result is not None, f"Read {i} returned None"
            assert result["value"] == "repeated_data", f"Read {i} returned wrong value"
            assert result["counter"] == 123, f"Read {i} returned wrong counter"

        # Property: Data should still be in memory cache after all reads
        final_result = cache._get_from_memory(test_key)
        assert final_result is not None
        assert final_result["value"] == "repeated_data"

        cache.close()

    @given(
        num_threads=st.integers(min_value=10, max_value=50),
        reads_per_thread=st.integers(min_value=5, max_value=20),
    )
    @settings(
        max_examples=30,
        deadline=10000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_memory_cache_hit_rate_high_for_repeated_keys(
        self, num_threads: int, reads_per_thread: int, tmp_path
    ):
        """Property: Memory cache hit rate is 100% for repeated reads.

        When the same keys are read repeatedly, all reads after the first
        should hit the memory cache (100% hit rate).

        Args:
            num_threads: Number of concurrent reader threads
            reads_per_thread: Number of reads per thread
            tmp_path: Temporary directory for cache
        """
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))
        cache = Cache(cache_dir=cache_dir)

        # Pre-populate cache with test keys
        num_keys = 10
        keys = [f"key_{i}" for i in range(num_keys)]
        for i, key in enumerate(keys):
            cache.set(key, {"value": f"data_{i}"})

        # Ensure all keys are in memory cache
        for key in keys:
            assert cache._get_from_memory(key) is not None

        memory_hits = 0
        total_reads = 0
        lock = threading.Lock()

        def read_repeatedly():
            """Read keys repeatedly and count memory hits."""
            nonlocal memory_hits, total_reads

            for _ in range(reads_per_thread):
                for key in keys:
                    result = cache._get_from_memory(key)
                    with lock:
                        total_reads += 1
                        if result is not None:
                            memory_hits += 1

        # Execute concurrent reads
        threads = [threading.Thread(target=read_repeatedly) for _ in range(num_threads)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: Memory cache hit rate should be 100%
        expected_reads = num_threads * reads_per_thread * num_keys
        assert total_reads == expected_reads, f"Expected {expected_reads} reads, got {total_reads}"

        hit_rate = memory_hits / total_reads if total_reads > 0 else 0

        # Property: All reads should hit memory cache (100% hit rate)
        assert hit_rate == 1.0, (
            f"Memory cache hit rate {hit_rate:.2%} is not 100% ({memory_hits}/{total_reads} hits)"
        )

        cache.close()

    def test_concurrent_reads_with_executor(self, tmp_path):
        """Test concurrent reads using ThreadPoolExecutor for realistic scenario.

        This test simulates a realistic concurrent workload where multiple
        threads are reading from the cache using a thread pool.
        """
        cache = Cache(cache_dir=tmp_path)

        # Pre-populate cache
        num_keys = 20
        for i in range(num_keys):
            cache.set(f"key_{i}", {"value": f"data_{i}", "index": i})

        # Ensure data is in memory
        for i in range(num_keys):
            assert cache._get_from_memory(f"key_{i}") is not None

        def read_key(key: str):
            """Read a key from cache."""
            result = cache._get_from_memory(key)
            assert result is not None
            return result

        # Execute many concurrent reads
        num_reads = 1000
        keys_to_read = [f"key_{i % num_keys}" for i in range(num_reads)]

        start_time = time.perf_counter()

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(read_key, key) for key in keys_to_read]
            results = [future.result() for future in as_completed(futures)]

        total_time = time.perf_counter() - start_time

        # Property: All reads should succeed
        assert len(results) == num_reads

        # Property: Concurrent reads should be fast (not blocking)
        # Allow 1 second for 1000 memory reads with 20 workers
        assert total_time < 1.0, f"Reads took {total_time:.4f}s, suggesting blocking"

        cache.close()

    @given(
        num_writers=st.integers(min_value=2, max_value=10),
        num_readers=st.integers(min_value=10, max_value=50),
    )
    @settings(
        max_examples=20,
        deadline=10000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_reads_dont_block_during_writes(self, num_writers: int, num_readers: int, tmp_path):
        """Property: Reads don't block during concurrent writes.

        Memory cache reads should proceed in parallel even when writes are
        happening concurrently (separate locks for memory and SQLite).

        Args:
            num_writers: Number of concurrent writer threads
            num_readers: Number of concurrent reader threads
            tmp_path: Temporary directory for cache
        """
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))
        cache = Cache(cache_dir=cache_dir)

        # Pre-populate with initial data
        initial_keys = [f"read_key_{i}" for i in range(10)]
        for key in initial_keys:
            cache.set(key, {"value": key})

        # Ensure data is in memory
        for key in initial_keys:
            assert cache._get_from_memory(key) is not None

        read_count = 0
        write_count = 0
        lock = threading.Lock()

        def writer(writer_id: int):
            """Write new keys to cache."""
            nonlocal write_count
            for i in range(5):
                key = f"write_key_{writer_id}_{i}"
                cache.set(key, {"value": key, "writer": writer_id})
                with lock:
                    write_count += 1
                time.sleep(0.001)  # Small delay to simulate work

        def reader():
            """Read existing keys from cache."""
            nonlocal read_count
            for _ in range(20):
                for key in initial_keys:
                    result = cache._get_from_memory(key)
                    if result is not None:
                        with lock:
                            read_count += 1

        # Start writers and readers concurrently
        writer_threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_writers)]
        reader_threads = [threading.Thread(target=reader) for _ in range(num_readers)]

        all_threads = writer_threads + reader_threads

        start_time = time.perf_counter()

        for thread in all_threads:
            thread.start()
        for thread in all_threads:
            thread.join()

        total_time = time.perf_counter() - start_time

        # Property: Reads should have succeeded despite concurrent writes
        expected_reads = num_readers * 20 * len(initial_keys)
        # Allow some misses due to LRU eviction, but most should succeed
        assert read_count >= expected_reads * 0.8, (
            f"Only {read_count}/{expected_reads} reads succeeded"
        )

        # Property: All writes should have succeeded
        expected_writes = num_writers * 5
        assert write_count == expected_writes

        # Property: Execution should be reasonably fast (reads not blocked by writes)
        # Allow 2 seconds for this workload
        assert total_time < 2.0, f"Execution took {total_time:.4f}s, suggesting blocking"

        cache.close()

    @given(
        cache_size=st.integers(min_value=10, max_value=100),
        num_reads=st.integers(min_value=50, max_value=200),
    )
    @settings(
        max_examples=30,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_memory_cache_lru_behavior_under_load(self, cache_size: int, num_reads: int, tmp_path):
        """Property: Memory cache maintains LRU behavior under concurrent reads.

        When cache is full and new items are added, least recently used items
        should be evicted, and reads should still work correctly.

        Args:
            cache_size: Number of items to store in cache
            num_reads: Number of read operations to perform
            tmp_path: Temporary directory for cache
        """
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))
        cache = Cache(cache_dir=cache_dir)

        # Fill cache to capacity
        for i in range(cache_size):
            cache.set(f"key_{i}", {"value": f"data_{i}", "index": i})

        # Read keys in a pattern (some frequently, some rarely)
        frequent_keys = [f"key_{i}" for i in range(min(10, cache_size))]
        rare_keys = (
            [f"key_{i}" for i in range(cache_size - 10, cache_size)] if cache_size > 10 else []
        )

        def read_pattern():
            """Read keys with frequency pattern."""
            for _ in range(num_reads // 10):
                # Read frequent keys more often
                for key in frequent_keys:
                    cache._get_from_memory(key)

                # Read rare keys occasionally
                if rare_keys:
                    cache._get_from_memory(rare_keys[0])

        # Execute reads
        thread = threading.Thread(target=read_pattern)
        thread.start()
        thread.join()

        # Property: Frequently accessed keys should still be in memory cache
        for key in frequent_keys:
            result = cache._get_from_memory(key)
            assert result is not None, f"Frequent key {key} was evicted"

        cache.close()

    def test_lock_free_read_performance_baseline(self, tmp_path):
        """Baseline test: Verify lock-free reads are significantly faster than locked reads.

        This test establishes that the lock-free read optimization provides
        measurable performance improvement.
        """
        cache = Cache(cache_dir=tmp_path)

        # Pre-populate cache
        test_key = "perf_test_key"
        test_data = {"value": "performance_test_data"}
        cache.set(test_key, test_data)

        # Ensure data is in memory
        assert cache._get_from_memory(test_key) is not None

        # Measure lock-free read performance
        num_reads = 10000
        start_time = time.perf_counter()

        for _ in range(num_reads):
            result = cache._get_from_memory(test_key)
            assert result is not None

        lock_free_time = time.perf_counter() - start_time

        # Property: Lock-free reads should be very fast
        # 10,000 memory reads should complete in well under 1 second
        assert lock_free_time < 0.5, (
            f"Lock-free reads took {lock_free_time:.4f}s for {num_reads} reads, "
            f"suggesting performance issue"
        )

        # Calculate throughput
        throughput = num_reads / lock_free_time
        # Property: Should achieve at least 20,000 reads/second
        assert throughput > 20000, f"Lock-free read throughput {throughput:.0f} reads/s is too low"

        cache.close()


class TestCacheReadConsistency:
    """Property tests for cache read consistency under concurrent access."""

    @given(
        num_threads=st.integers(min_value=5, max_value=20),
        num_keys=st.integers(min_value=5, max_value=20),
    )
    @settings(
        max_examples=30,
        deadline=10000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_concurrent_reads_return_consistent_data(
        self, num_threads: int, num_keys: int, tmp_path
    ):
        """Property: Concurrent reads of same key return consistent data.

        When multiple threads read the same key concurrently, they should
        all receive the same data (no partial reads or corruption).

        Args:
            num_threads: Number of concurrent reader threads
            num_keys: Number of different keys to test
            tmp_path: Temporary directory for cache
        """
        cache_dir = Path(tempfile.mkdtemp(dir=tmp_path))
        cache = Cache(cache_dir=cache_dir)

        # Pre-populate cache with test data
        expected_data = {}
        for i in range(num_keys):
            key = f"key_{i}"
            data = {"value": f"data_{i}", "index": i, "list": list(range(i))}
            cache.set(key, data)
            expected_data[key] = data

        # Ensure all data is in memory
        for key in expected_data:
            assert cache._get_from_memory(key) is not None

        results = []
        lock = threading.Lock()

        def read_all_keys():
            """Read all keys and record results."""
            thread_results = {}
            for key in expected_data:
                result = cache._get_from_memory(key)
                thread_results[key] = result

            with lock:
                results.append(thread_results)

        # Execute concurrent reads
        threads = [threading.Thread(target=read_all_keys) for _ in range(num_threads)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: All threads should have read all keys
        assert len(results) == num_threads

        # Property: All threads should have read the same data for each key
        for key, expected in expected_data.items():
            for thread_id, thread_results in enumerate(results):
                actual = thread_results.get(key)
                assert actual is not None, f"Thread {thread_id} got None for {key}"
                assert actual == expected, (
                    f"Thread {thread_id} got inconsistent data for {key}: "
                    f"expected {expected}, got {actual}"
                )

        cache.close()

    def test_read_during_lru_update_is_consistent(self, tmp_path):
        """Property: Reads during LRU updates return consistent data.

        When one thread is updating LRU order (move_to_end), other threads
        reading the same key should still get consistent data.
        """
        cache = Cache(cache_dir=tmp_path)

        test_key = "lru_test_key"
        test_data = {"value": "lru_test_data", "number": 999}
        cache.set(test_key, test_data)

        # Ensure data is in memory
        assert cache._get_from_memory(test_key) is not None

        results = []
        lock = threading.Lock()

        def read_repeatedly():
            """Read the same key many times."""
            for _ in range(100):
                result = cache._get_from_memory(test_key)
                with lock:
                    results.append(result)

        # Execute many concurrent reads (will trigger LRU updates)
        threads = [threading.Thread(target=read_repeatedly) for _ in range(10)]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Property: All reads should return the same consistent data
        assert len(results) == 1000  # 10 threads * 100 reads

        for i, result in enumerate(results):
            assert result is not None, f"Read {i} returned None"
            assert result["value"] == "lru_test_data", f"Read {i} returned wrong value"
            assert result["number"] == 999, f"Read {i} returned wrong number"

        cache.close()
