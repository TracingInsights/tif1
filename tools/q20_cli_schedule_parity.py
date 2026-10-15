"""Q20: byte parity of ``tif1 events``/``tif1 sessions`` control vs candidate.

``events`` for every season 2017-2027 and ``sessions`` for every event name,
its lower-case form, every alias of :data:`EVENT_NAME_ALIASES` and a few typos
and non-events, per season 2018-2026 (fresh process per command).

Usage:
    uv run python tools/q20_cli_schedule_parity.py --control-src /tmp/tif1-pergamon-control/src \
        --candidate-src src --cache /tmp/tif1-q-Q20/cache --workers 12
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from q20_cli_parity import _run

from tif1.core_utils.constants import EVENT_NAME_ALIASES
from tif1.events import _build_events_for_year


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--control-src", required=True)
    parser.add_argument("--candidate-src", required=True)
    parser.add_argument("--cache", required=True)
    parser.add_argument("--workers", type=int, default=12)
    opts = parser.parse_args()
    ctl = str(Path(opts.control_src).resolve())
    cand = str(Path(opts.candidate_src).resolve())

    commands: list[list[str]] = [["events", str(y)] for y in range(2017, 2028)]
    for year in range(2018, 2027):
        names = _build_events_for_year(year)
        queries = [*names, *(n.lower() for n in names), *EVENT_NAME_ALIASES]
        queries += ["Monacco", "Britain", "Sao Paolo", "Moon GP"]
        commands += [["sessions", str(year), q] for q in dict.fromkeys(queries)]

    def check(args: list[str]) -> str | None:
        a, b = _run(ctl, opts.cache, args), _run(cand, opts.cache, args)
        return None if a == b else f"DIFF {args}: {a!r:.300} vs {b!r:.300}"

    with ThreadPoolExecutor(max_workers=opts.workers) as pool:
        diffs = [d for d in pool.map(check, commands) if d]
    for d in diffs[:20]:
        print(d)
    print(f"{len(commands)} commands, {len(diffs)} differences")


if __name__ == "__main__":
    main()
