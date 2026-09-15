"""N2 A/B: brotli Accept-Encoding negotiation vs status quo (gzip).

Alternates `uv pip (un)install brotli` between cold benchmark runs so both
variants see the same edge conditions. Writes a JSON report.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

BENCH = "tools/monaco_telemetry_cold_benchmark.py"
OUT = Path(".agents/perf-lindos/n2-brotli-ab.json")


def run_variant(variant: str) -> dict:
    if variant == "B":
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "brotli"],
            check=True,
            capture_output=True,
        )
    else:
        subprocess.run(
            [sys.executable, "-m", "pip", "uninstall", "-y", "-q", "brotli"],
            check=True,
            capture_output=True,
        )
    proc = subprocess.run(
        [sys.executable, BENCH, "--year", "2026", "--gp", "Monaco Grand Prix", "--session", "Race"],
        capture_output=True,
        text=True,
        check=True,
    )
    run = json.loads(proc.stdout.strip().splitlines()[-1])
    run["variant"] = variant
    return run


def main() -> None:
    runs = []
    order = ["A", "B"] * 4
    for variant in order:
        t0 = time.perf_counter()
        run = run_variant(variant)
        run["wall_s"] = round(time.perf_counter() - t0, 1)
        runs.append(run)
        print(
            f"{variant} total={run['total_s']}s telemetry={run['telemetry_s']}s "
            f"(wall {run['wall_s']}s)",
            flush=True,
        )
    import statistics

    def med(v, key):
        vals = [r[key] for r in runs if r["variant"] == v]
        return statistics.median(vals)

    report = {
        "label": "n2-brotli-accept-encoding",
        "runs": runs,
        "median": {
            v: {"total_s": med(v, "total_s"), "telemetry_s": med(v, "telemetry_s")} for v in "AB"
        },
    }
    OUT.write_text(json.dumps(report, indent=2) + "\n")
    print("median:", json.dumps(report["median"]))


if __name__ == "__main__":
    main()
