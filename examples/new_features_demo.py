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

    # Set custom configuration
    config.set("max_retries", 5)
    config.set("validate_data", True)
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
        print(f"Session loaded: {session.year} {session.gp.replace('%20', ' ')}")
        print(f"Lib: {session.lib}")
        print(f"Cache enabled: {session.enable_cache}")

        # Get drivers
        drivers = session.drivers_df
        print(f"\nDrivers in session: {len(drivers)}")
        for _, driver in drivers.head(3).iterrows():
            print(f"  - {driver['Driver']}: {driver['Team']}")
        if len(drivers) > 3:
            print(f"  ... and {len(drivers) - 3} more")

        # Get laps with validation
        print("\nLoading laps with data validation...")
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
            lap_time = ver_fastest["LapTime"].values[0]
            compound = (
                ver_fastest["Compound"].values[0] if "Compound" in ver_fastest.columns else "N/A"
            )
            print(f"Fastest lap: {lap_time:.3f}s on {compound}")

        # 8. Telemetry with Validation
        print("\n8. Telemetry Data")
        print("-" * 60)
        if not ver_fastest.empty:
            lap_num = (
                ver_fastest["LapNumber"].values[0]
                if "LapNumber" in ver_fastest.columns
                else ver_fastest["lap"].values[0]
            )
            lap = ver.get_lap(int(lap_num))
            telemetry = lap.telemetry
            if not telemetry.empty:
                print(f"Telemetry points: {len(telemetry)}")
                print(f"Columns: {list(telemetry.columns)[:5]}...")
                if "Speed" in telemetry.columns:
                    print(f"Max speed: {telemetry['Speed'].max():.1f} km/h")
                if "RPM" in telemetry.columns:
                    print(f"Max RPM: {telemetry['RPM'].max()}")

    except tif1.DataNotFoundError:
        print("Data not available for this session")
    except tif1.NetworkError as e:
        print(f"Network error: {e}")
        print("Check circuit breaker status")
    except tif1.InvalidDataError as e:
        print(f"Data validation failed: {e}")
    except Exception as e:
        print(f"Error: {e}")

    # 9. Cache Management
    print("\n9. Cache Management")
    print("-" * 60)
    cache = tif1.get_cache()
    print(f"Cache directory: {cache.cache_dir}")
    print("Cache operations available: get, set, clear")

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
    print("✓ Improved HTTP/2 connection pooling")
    print("✓ Type definitions for IDE support")
    print("✓ Comprehensive error handling")
    print("\nFor Jupyter notebooks:")
    print("✓ Rich HTML display (auto-enabled)")
    print("\nFor testing:")
    print("✓ Fuzzing tests (tests/test_fuzzing.py)")
    print("\nDocumentation:")
    print("✓ Complete API reference (docs/api-reference/complete.mdx)")
    print("✓ Improvements summary (IMPROVEMENTS.md)")


if __name__ == "__main__":
    main()
