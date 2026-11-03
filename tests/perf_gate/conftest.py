"""Options of the performance gate (``tests/perf_gate``)."""

from __future__ import annotations


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--write-baseline",
        action="store_true",
        default=False,
        help="perf gate: rewrite tests/fixtures/perf/baseline.json and the parity digests",
    )
