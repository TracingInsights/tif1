# tif1 Examples

This directory contains practical examples demonstrating various features of tif1.

## Quick Start

### Simple Example
Start here if you're new to tif1:
```bash
python examples/example.py
```

### Basic Usage
Comprehensive introduction to core features:
```bash
python examples/basic_usage.py
```

## Examples by Feature

### 📊 Data Analysis
- **`fastest_laps.py`** - Analyze fastest laps, compare drivers and teammates
- **`data_exploration.py`** - Discover available data and understand data structures

### ⚡ Performance
- **`async_loading.py`** - Compare async vs sync loading (4-5x faster)
- **`polars_backend.py`** - Use Polars for 2x faster data processing
- **`cache_management.py`** - Manage cache for faster repeated access

### 🗄️ Advanced
- **`error_handling.py`** - Proper error handling patterns

## Running Examples

All examples can be run directly:
```bash
# Run any example
python examples/<example_name>.py

# Or with uv
uv run python examples/<example_name>.py
```

## Example Output

Each example includes:
- ✓ Clear section headers
- 📊 Formatted data output
- 💡 Tips and best practices
- ⚡ Performance metrics (where applicable)

## Requirements

Most examples work with the base installation:
```bash
pip install tif1
```



## Learning Path

1. **Start**: `example.py` - Simple introduction
2. **Learn**: `basic_usage.py` - Core features
3. **Explore**: `data_exploration.py` - Understand data structure
4. **Analyze**: `fastest_laps.py` - Data analysis patterns
5. **Optimize**: `async_loading.py`, `cache_management.py` - Performance
6. **Advanced**: `polars_backend.py` - Advanced features
7. **Production**: `error_handling.py` - Robust error handling

## Common Patterns

### Loading a Session
```python
import tif1
session = tif1.get_session(2025, "Abu Dhabi Grand Prix", "Practice 1")
```

### Getting Lap Data
```python
laps = session.laps  # All laps
driver = session.get_driver("VER")  # Specific driver
driver_laps = driver.laps
```

### Getting Telemetry
```python
lap = driver.get_lap(19)
telemetry = lap.telemetry
```

### Error Handling
```python
try:
    session = tif1.get_session(2025, "Event Name", "Session")
except tif1.DataNotFoundError:
    print("Data not available")
```

## Tips

- 🚀 Use `lib="polars"` for large datasets
- 💾 Enable caching for repeated access
- ⚡ Use `laps_async()` for faster initial loading

- ✅ Always handle errors properly

## Need Help?

- Check the [README](../README.md) for API documentation
- See [API.md](../API.md) for detailed API reference
- Open an issue on GitHub for bugs or questions
