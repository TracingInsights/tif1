"""Corinth C5: interpreter startup and exit cost, Python 3.13 vs 3.14 (interleaved).

For each interpreter: ``-c pass`` (bare startup + exit), ``-S -c pass`` (no
site), and the warm pandas ``laps`` script with its in-process time printed,
so ``process wall - in-process`` = startup + exit. ``--exit-probe`` also times
the exit itself (from the last statement to process end, via a timestamp the
child prints just before returning).

    TIF1_CACHE_DIR=/tmp/corinth/warm-C5 TIF1_OFFLINE_MODE=true PYTHONPATH=src \
        python tools/perf/corinth_c5_startup_exit.py --reps 10 [--lib pandas]
"""

from __future__ import annotations

import argparse
import subprocess
import time
from statistics import median

PYTHONS = {
    "3.13": "/home/harish/Documents/tif1/.venv/bin/python",
    "3.14": "/tmp/corinth/venv314/bin/python",
}

SCRIPT = """
import time
t0 = time.perf_counter()
import tif1
s = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib={lib!r})
s.laps
{extra}
print("INPROC", (time.perf_counter() - t0) * 1000)
print("END", time.time())
"""


def _run(cmd: list[str]) -> tuple[float, float, float]:
    t0 = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    t1 = time.time()
    inproc = end = 0.0
    for line in proc.stdout.splitlines():
        if line.startswith("INPROC"):
            inproc = float(line.split()[1])
        elif line.startswith("END"):
            end = float(line.split()[1])
    return (t1 - t0) * 1000, inproc, (t1 - end) * 1000 if end else 0.0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reps", type=int, default=10)
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--all-tel", action="store_true")
    args = parser.parse_args()
    extra = "s.fetch_all_laps_telemetry()" if args.all_tel else ""
    script = SCRIPT.format(lib=args.lib, extra=extra)
    res: dict[str, dict[str, list[float]]] = {
        k: {"pass": [], "nosite": [], "wall": [], "inproc": [], "exit": []} for k in PYTHONS
    }
    for i in range(args.reps):
        order = list(PYTHONS.items()) if i % 2 == 0 else list(PYTHONS.items())[::-1]
        for label, py in order:
            r = res[label]
            r["pass"].append(_run([py, "-c", "pass"])[0])
            r["nosite"].append(_run([py, "-S", "-c", "pass"])[0])
            wall, inproc, exit_ms = _run([py, "-c", script])
            r["wall"].append(wall)
            r["inproc"].append(inproc)
            r["exit"].append(exit_ms)
    for label, r in res.items():
        startup = median(r["wall"]) - median(r["inproc"]) - median(r["exit"])
        print(
            f"{label}: -c pass {median(r['pass']):6.1f}  -S -c pass {median(r['nosite']):6.1f}  "
            f"script wall {median(r['wall']):6.1f}  in-process {median(r['inproc']):6.1f}  "
            f"exit {median(r['exit']):6.1f}  startup ~{startup:6.1f} ms"
        )


if __name__ == "__main__":
    main()
