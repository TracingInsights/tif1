"""Q18 lap-frame decode: hand-built arrays vs pandas' own slicing, on the installed pandas.

Builds VER's per-lap car-data frames of the 2025 Bahrain race from the warm
cache, encodes them as one driver blob and times ``decode_driver_frames``
(every lap) with the probes' verdict and with ``_FAST_ARRAYS`` forced off.
"""

import sys
import time

import pandas as pd

import tif1
from tif1.core_utils import frame_codec as fc

session = tif1.get_session(2025, "Bahrain Grand Prix", "Race")
session.load(laps=True, telemetry=False, weather=False, messages=False)
laps = session.laps.pick_drivers("VER")
frames = [(int(lap["LapNumber"]), pd.DataFrame(lap.telemetry)) for _, lap in laps.iterlaps()]
blob, leftovers = fc.encode_driver_frames(frames)
assert blob is not None and not leftovers
fc._FAST_ARRAYS = None
fast_flags = fc._fast_arrays()


def best(flags, reps=200):
    fc._FAST_ARRAYS = flags
    out = []
    for _ in range(reps):
        buf = bytearray(blob)
        t0 = time.perf_counter()
        fc.decode_driver_frames(buf)
        out.append(time.perf_counter() - t0)
    out.sort()
    return out[0] * 1e3, out[len(out) // 2] * 1e3


fast = best(fast_flags)
slow = best((False, False))
fc._FAST_ARRAYS = None
print(
    f"pandas {pd.__version__} laps={len(frames)} probes={fast_flags} layout={fc._array_layout()} "
    f"decode best/median ms: probes {fast[0]:.2f}/{fast[1]:.2f}  "
    f"fallback {slow[0]:.2f}/{slow[1]:.2f}  ratio {slow[0] / fast[0]:.2f}x",
    file=sys.stdout,
)
