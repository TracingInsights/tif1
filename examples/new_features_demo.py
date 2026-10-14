"""Example demonstrating all new tif1 improvements."""

import logging

import tif1
from tif1.cdn import CDNSource


def main():
    """Demonstrate all new features."""

    print("=" * 60)
    print("tif1 - New Features Demo")
    print("=" * 60)

    # 1. Configuration Management
    print("\n1. Configuration Management")
    print("-" * 60)
    config = tif1.get_config()
    print(f"Cache dir: {config.get('cache_dir')}")
    print(f"Max retries: {config.get('max_retries')}")
    print(f"Lib: {config.get('lib')}")
    print(f"Validate data: {config.get('validate_data')}")

    # Set custom configuration (in memory only; config.save() writes ~/.tif1rc)
    # validate_data checks weather and race-control payloads when they are
    # fetched; validate_lap_times does the same for lap payloads.
    config.set("max_retries", 5)
    config.set("validate_data", True)
    config.set("validate_lap_times", True)
    print(f"Updated max_retries to: {config.get('max_retries')}")

    # 2. Logging Setup
    print("\n2. Logging Setup")
    print("-" * 60)
    tif1.setup_logging(logging.INFO)
    print("Logging enabled at INFO level")

    # 3. Circuit Breaker
    print("\n3. Circuit Breaker Status")
    print("-" * 60)
    cb = tif1.get_circuit_breaker()
    print(f"State: {cb.state}")
    print(f"Failures: {cb.failures}")
    print(f"Threshold: {cb.threshold}")

    # 4. CDN Manager
    print("\n4. CDN Manager")
    print("-" * 60)
    cdn = tif1.get_cdn_manager()
    sources = cdn.get_sources()
    print(f"Available CDN sources: {len(sources)}")
    for source in sources:
        print(f"  - {source.name} (priority {source.priority})")

    # Add custom CDN (example)
    print("\nAdding custom CDN source...")
    custom_cdn = CDNSource(
        name="Example CDN",
        base_url="https://example.com/f1-data",
        priority=3,
        enabled=False,  # Disabled for demo
    )
    cdn.add_source(custom_cdn)
    print(f"Total CDN sources: {len(cdn.sources)}")

    # 5. Type Definitions
    print("\n5. Type Definitions")
    print("-" * 60)
    from tif1.types import BackendType, CompoundType, SessionType

    print(f"Session types: {SessionType.__args__}")
    print(f"Lib types: {BackendType.__args__}")
    print(f"Compound types: {CompoundType.__args__[:3]}...")

    # 6. Load Session with All Features
    print("\n6. Loading Session with Enhanced Features")
    print("-" * 60)
    try:
        session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1", lib="pandas")
        print(f"Session loaded: {session.year} {session.event_name}")
        print(f"Lib: {session.lib}")
        print(f"Cache enabled: {session.enable_cache}")

        # Get drivers
        drivers = session.drivers_df
        print(f"\nDrivers in session: {len(drivers)}")
        for _, driver in drivers.head(3).iterrows():
            print(f"  - {driver['Driver']}: {driver['Team']}")
        if len(drivers) > 3:
            print(f"  ... and {len(drivers) - 3} more")

        # Get laps (payloads fetched from the CDN are validated; a cache hit
        # serves the already-processed frame)
        print("\nLoading laps...")
        laps = session.laps
        print(f"Total laps loaded: {len(laps)}")
        print(f"Columns: {list(laps.columns)[:5]}...")

        # Get fastest laps
        print("\nFastest laps per driver:")
        fastest = session.get_fastest_laps(by_driver=True)
        print(fastest[["Driver", "LapTime", "Compound"]].head(3))

        # Analyze specific driver
        print("\n7. Driver Analysis")
        print("-" * 60)
        ver = session.get_driver("VER")
        print(f"Driver: {ver.driver}")
        ver_fastest = ver.get_fastest_lap()
        if not ver_fastest.empty:
            lap_time = ver_fastest["LapTimeSeconds"].iloc[0]
            compound = ver_fastest["Compound"].iloc[0]
            print(f"Fastest lap: {lap_time:.3f}s on {compound}")

        # 8. Telemetry with Validation
        print("\n8. Telemetry Data")
        print("-" * 60)
        if not ver_fastest.empty:
            lap_num = int(ver_fastest["LapNumber"].iloc[0])
            lap = ver.get_lap(lap_num)
            telemetry = lap.telemetry
            if not telemetry.empty:
                print(f"Telemetry points: {len(telemetry)}")
                print(f"Columns: {list(telemetry.columns)[:5]}...")
                print(f"Max speed: {telemetry['Speed'].max():.1f} km/h")
                print(f"Max RPM: {telemetry['RPM'].max():.0f}")

    except tif1.DataNotFoundError:
        print("Data not available for this session")
    except tif1.NetworkError as e:
        print(f"Network error: {e}")
        print("Check circuit breaker status")
    except tif1.InvalidDataError as e:
        print(f"Data validation failed: {e}")

    # 9. Cache Management
    print("\n9. Cache Management")
    print("-" * 60)
    cache = tif1.get_cache()
    print(f"Cache directory: {cache.cache_dir}")
    print(f"Cache database: {cache.db_path}")
    print("Cache operations available: get, set, invalidate, clear")

    # 10. Summary
    print("\n" + "=" * 60)
    print("Demo Complete!")
    print("=" * 60)
    print("\nNew features demonstrated:")
    print("✓ Configuration management (.tif1rc)")
    print("✓ Retry with exponential backoff & jitter")
    print("✓ Circuit breaker pattern")
    print("✓ CDN fallback system")
    print("✓ Enhanced data validation")
    print("✓ HTTP connection pooling (niquests)")
    print("✓ Type definitions for IDE support")
    print("✓ Comprehensive error handling")
    print("\nFor Jupyter notebooks:")
    print("✓ Rich HTML display (tif1.jupyter.enable_jupyter_display())")
    print("\nFor testing:")
    print("✓ Fuzzing tests (tests/unit/test_fuzzing.py)")
    print("\nDocumentation:")
    print("✓ API reference: https://tif1.tracinginsights.com/api-reference/overview")


if __name__ == "__main__":
    main()
