"""Elis E3 probe: what does niquests' OCSP check cost a fresh process, and does a seeded verdict save it?

Each child is a fresh interpreter that builds tif1's shared HTTP session and
times its first GET against the primary CDN (jsDelivr): the connection, the TLS
handshake, the revocation check and the response. Modes:

- ``off``: ``http_revocation_check`` False (no check at all).
- ``on``: the default check, nothing seeded (today's cold process).
- ``seeded``: the default check with niquests' in-process revocation cache
  restored (``__setstate__``) from the state a previous ``on`` child pickled.

Run from the repository root::

    python tools/perf/elis_e3_ocsp_probe.py --reps 8 --state .e3scratch/ocsp_state.pickle
"""

from __future__ import annotations

import argparse
import os
import pickle
import statistics
import subprocess
import sys
import time
from pathlib import Path

URL_PATH = ("2025", "Bahrain%20Grand%20Prix", "Race", "drivers.json")


def _child(mode: str, state_path: Path) -> None:
    t0 = time.perf_counter()
    if mode == "off":
        os.environ["TIF1_HTTP_REVOCATION_CHECK"] = "false"
    from tif1 import http_session
    from tif1.cdn import get_cdn_manager

    session = http_session.get_session()
    t_session = time.perf_counter()
    if mode == "seeded":
        from niquests.extensions.revocation._ocsp import InMemoryRevocationStatus

        cache = InMemoryRevocationStatus.__new__(InMemoryRevocationStatus)
        cache.__setstate__(pickle.loads(state_path.read_bytes()))
        session._ocsp_cache = cache
        for adapter in session.adapters.values():
            if hasattr(adapter, "_ocsp_cache"):
                adapter._ocsp_cache = cache
    t_seed = time.perf_counter()
    year, gp, sess, path = URL_PATH
    url = get_cdn_manager().get_sources()[0].format_url(int(year), gp, sess, path)
    response = session.get(url, timeout=10)
    response.raise_for_status()
    t_get = time.perf_counter()
    verified = getattr(response, "ocsp_verified", None)
    if mode == "on" and session._ocsp_cache is not None:
        state_path.write_bytes(pickle.dumps(session._ocsp_cache.__getstate__()))
    print(
        f"{(t_session - t0) * 1e3:.1f} {(t_seed - t_session) * 1e3:.1f} "
        f"{(t_get - t_seed) * 1e3:.1f} {verified} "
        f"{len(session._ocsp_cache) if session._ocsp_cache is not None else 0}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child")
    parser.add_argument("--reps", type=int, default=8)
    parser.add_argument("--state", default=".e3scratch/ocsp_state.pickle")
    args = parser.parse_args()
    state = Path(args.state)
    if args.child:
        _child(args.child, state)
        return
    results: dict[str, list[float]] = {"off": [], "on": [], "seeded": []}
    for rep in range(args.reps):
        for mode in ("on", "seeded", "off"):
            out = subprocess.run(
                [sys.executable, __file__, "--child", mode, "--state", str(state)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=True,
                env={**os.environ, "TIF1_CDN_PRECONNECT": "false"},
            ).stdout.split()
            results[mode].append(float(out[2]))
            print(
                f"rep {rep} {mode:7s} session {out[0]} seed {out[1]} get {out[2]} "
                f"verified={out[3]} entries={out[4]}"
            )
    for mode, values in results.items():
        print(f"{mode:7s} median first GET {statistics.median(values):.1f} ms  {values}")


if __name__ == "__main__":
    main()
