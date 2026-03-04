# Contributing to tif1

Thank you for your interest in contributing to tif1! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.10 or higher
- [uv](https://github.com/astral-sh/uv) package manager

### Installation

1. Clone the repository:
```bash
git clone https://github.com/TracingInsights/tif1.git
cd tif1
```

2. Install dependencies:
```bash
uv sync --all-extras
```

3. Install git hooks with prek:
```bash
uv run prek install
uv run prek run --all-files
```

## Development Workflow

### Running Tests

Run all tests:
```bash
uv run pytest tests/ -v
```

Run specific test file:
```bash
uv run pytest tests/test_core.py -v
```

Run with coverage:
```bash
uv run pytest -o addopts='' tests/ -v -n auto --dist=loadfile --cov=src/tif1 --cov-report=html
```

Run only unit tests (skip integration):
```bash
uv run pytest tests/ -v -m "not integration"
```

Run integration tests (serial):
```bash
uv run pytest -o addopts='' tests/ -v -n 0 -m integration
```

Run benchmarks (serial for stable timing):
```bash
uv run pytest -o addopts='' tests/test_benchmarks.py -v -m benchmark --benchmark-only --no-cov -n 0
```

### Code Quality

Run linting:
```bash
uv run ruff check src/ tests/
```

Run formatting:
```bash
uv run ruff format src/ tests/
```

Run type checking:
```bash
uv run ty check src/tif1
```

Run all checks (prek):
```bash
uv run prek run --all-files
```

### Running Examples

```bash
uv run python examples/basic_usage.py
uv run python examples/async_loading.py
```

## Code Style

- Follow PEP 8 guidelines
- Use type hints for all functions
- Write docstrings for all public APIs (Google style)
- Keep line length to 100 characters
- Use meaningful variable names

## Testing Guidelines

- Write tests for all new features
- Maintain 80%+ code coverage
- Use pytest fixtures for common setup
- Mock external dependencies
- Add integration tests for real data access

## Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes
4. Run tests and linting: `uv run pytest && uv run ruff check`
5. Commit with clear messages: `git commit -m "Add feature X"`
6. Push to your fork: `git push origin feature/your-feature`
7. Open a Pull Request

### PR Requirements

- All tests must pass
- Code coverage should not decrease
- Code must pass linting and type checking
- Include tests for new features
- Update documentation if needed
- Add entry to CHANGELOG.md

## Commit Messages

Use clear, descriptive commit messages:

- `feat: Add new feature`
- `fix: Fix bug in X`
- `docs: Update documentation`
- `test: Add tests for Y`
- `refactor: Refactor Z`
- `perf: Improve performance of W`

## Documentation

- Update docstrings for code changes
- Add examples for new features
- Update README.md if needed
- Add API documentation in docs/

## Questions?

Open an issue or discussion on GitHub.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
