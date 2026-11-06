"""Naxos N11: time the per-wave frame build of one driver from its payloads (offline).

Reads one driver's lap payloads from the warm cache's payload tier (the shape a
cold wave hands to ``Session._frames_from_payloads``) and times the build,
best of ``--reps``, with a per-step breakdown under ``--profile``.

Usage:
    TIF1_CACHE_DIR=/tmp/naxos/warm-N11 TIF1_OFFLINE_MODE=true \
        python tools/perf/naxos_n11_build_probe.py --lib pandas --driver VER
"""

from __future__ import annotations

import argparse
import time


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lib", default="pandas")
    parser.add_argument("--driver", default="VER")
    parser.add_argument("--reps", type=int, default=7)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()

    import tif1

    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race", lib=args.lib)
    laps = session.laps
    refs = [ref for ref in session._telemetry_lap_refs(laps) if ref[0] == args.driver]
    _, missing, found = session._fetch_telemetry_batch_from_refs(refs, skip_cache=False)
    print(f"{len(found)} payloads, {len(missing)} missing; channel types:", end=" ")
    print({k: type(v).__name__ for k, v in found[0][2].items()})

    def build() -> float:
        session._memo.clear() if hasattr(session._memo, "clear") else None
        t0 = time.perf_counter()
        session._frames_from_payloads(found, {})
        return (time.perf_counter() - t0) * 1000

    times = sorted(build() for _ in range(args.reps))
    print(
        f"[{args.lib}] build {len(found)} frames: best {times[0]:.1f} median "
        f"{times[len(times) // 2]:.1f} ms"
    )
    if args.profile:
        import cProfile
        import pstats

        prof = cProfile.Profile()
        prof.enable()
        build()
        prof.disable()
        pstats.Stats(prof).sort_stats("cumulative").print_stats(30)


if __name__ == "__main__":
    main()
