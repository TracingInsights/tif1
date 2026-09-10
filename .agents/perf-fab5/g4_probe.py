"""G4 behavior probe: laps-only flow must not fetch weather/rcm; load() must."""

from __future__ import annotations

import os
import tempfile

os.environ["TIF1_CACHE_DIR"] = tempfile.mkdtemp(prefix="tif1-g4-")

import tif1


def probe(label: str, flow) -> None:
    session = tif1.get_session(2026, "Monaco Grand Prix", "Race")
    flow(session)
    fetched = {
        path: session._get_local_payload(path) is not None
        for path in ("weather.json", "rcm.json", "drivers.json")
    }
    print(f"{label}: fetched={fetched}")


def laps_only(session) -> None:
    _ = session.laps


def load_all(session) -> None:
    session.load(laps=True, telemetry=False, weather=True, messages=True)


def weather_only(session) -> None:
    _ = session.weather


def main() -> None:
    probe("laps-only   ", laps_only)
    probe("load(all)   ", load_all)
    probe("weather-only", weather_only)


if __name__ == "__main__":
    main()
