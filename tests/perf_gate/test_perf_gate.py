"""CI performance regression gate (marker ``perf_gate``; CI job ``perf-gate``).

Runs the fresh-process warm workloads of ``tools/perf/perf_gate.py`` against a
cache seeded from the vendored bundle ``tests/fixtures/perf/payloads.bin.xz``
and fails when an in-process phase regressed against the committed
``tests/fixtures/perf/baseline.json`` (after scaling by the machine factor),
or when a user-visible surface no longer matches the committed parity digest.

Run it alone and without xdist (other tests running beside it skew the times):

    uv run pytest tests/perf_gate -m perf_gate -p no:xdist -o addopts='' -v

``--write-baseline`` rewrites the baseline and the digests from this machine
(review the diff of the digests: any change there is a user-visible change).
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.perf_gate

_GATE_PATH = Path(__file__).resolve().parents[2] / "tools" / "perf" / "perf_gate.py"
#: The deliberate slowdown of the self-test: 50 ms per ``Session.laps`` access.
_INJECTED_SLEEP_S = 0.05


def _load_gate():
    spec = importlib.util.spec_from_file_location("tif1_perf_gate", _GATE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses resolve their module by name
    spec.loader.exec_module(module)
    return module


gate = _load_gate()


@pytest.fixture(scope="module")
def write_baseline(request) -> bool:
    return bool(request.config.getoption("--write-baseline", default=False))


@pytest.fixture(scope="module")
def gate_run(write_baseline):
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.skip("run the perf gate with -p no:xdist (parallel tests skew its timings)")
    if write_baseline:
        run = gate.run_gate(reps=gate.BASELINE_REPS)
        gate.write_outputs(run, gate.BASELINE_REPS)
    else:
        run = gate.run_gate(baseline=gate.load_baseline())
    print(f"\nperf gate run: {run.seconds:.0f} s")
    if run.confirmed:
        print("re-measured: " + ", ".join(f"{lib}/{w}" for lib, w in run.confirmed))
    return run


@pytest.fixture(scope="module")
def comparison(gate_run):
    return gate.compare(gate.load_baseline(), gate_run.phases)


def test_phases_within_baseline(comparison, write_baseline) -> None:
    print("\n" + gate.format_table(comparison))
    if write_baseline:
        pytest.skip(f"baseline rewritten: {gate.BASELINE}")
    assert comparison.ok, "phases regressed:\n" + gate.format_table(
        comparison, only_regressions=True
    )


@pytest.mark.parametrize("kind", gate.DIGEST_KINDS)
@pytest.mark.parametrize("lib", gate.LIBS)
def test_parity_digest(gate_run, lib: str, kind: str) -> None:
    """``cold``: frames built from the payload tier; ``warm``: read back from the frame tiers."""
    expected = gate.expected_digest(lib, kind)
    diffs = gate.compare_digests(expected, gate_run.digests[lib][kind])
    files = gate.parity_path(lib).name + (
        f" + {gate.cold_delta_path(lib).name}" if kind == "cold" else ""
    )
    assert not diffs, f"{len(diffs)} surfaces differ from {files}:\n" + "\n".join(diffs[:60])


def test_gate_flags_injected_slowdown(comparison, write_baseline) -> None:
    """A 50 ms sleep in ``Session.laps`` must fail the comparison on the ``laps`` phase."""
    if write_baseline:
        pytest.skip("baseline rewritten")
    run = gate.run_gate(
        libs=("pandas",),
        workloads=("laps", "results"),
        reps=1,
        digests=False,
        inject_sleep=_INJECTED_SLEEP_S,
    )
    # The clean run's machine factor: the injected phases must not set it.
    injected = gate.compare(gate.load_baseline(), run.phases, factor=comparison.factor)
    flagged = {(r.workload, r.phase) for r in injected.regressions}
    assert ("laps", "laps") in flagged, gate.format_table(injected)
    assert ("results", "laps") in flagged, gate.format_table(injected)
