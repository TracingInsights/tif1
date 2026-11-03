"""Elis E10 probe: does the lazy stream queue the next driver on sequential access?

Cold (needs ``TIF1_CACHE_DIR`` pointing at an empty directory): a lazy load,
then every lap of the first three drivers in laps-row order; prints which
drivers the stream holds payloads for (the third driver's successor should be
among them) and the drivers still pending.
"""

from __future__ import annotations

import tif1


def main() -> None:
    session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
    session.load(telemetry="lazy")
    laps = session.laps
    drivers = list(dict.fromkeys(str(d) for d in laps["Driver"]))
    print("laps-row driver order", drivers)
    first = set(drivers[:3])
    for _, lap in laps.iterlaps():
        if lap["Driver"] in first:
            _ = lap.telemetry
    stream = session._lazy_stream
    results = stream.results() if stream is not None else {}
    print("stream payloads", len(results), "drivers", sorted({key[0] for key in results}))
    print("pending", len(session._lazy_telemetry_refs), "next", session._lazy_lookahead_next)


if __name__ == "__main__":
    main()
