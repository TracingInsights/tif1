import sys
import time

t0 = time.perf_counter()
import tif1  # noqa: E402 (timed: the import is part of the measurement)

out = {}


def step(n, f):
    t = time.perf_counter()
    r = f()
    out[n] = round((time.perf_counter() - t) * 1000)
    return r


s = step(
    "get_session", lambda: tif1.get_session(2026, "Mexico City Grand Prix", "R", lib=sys.argv[1])
)
step("laps", lambda: s.laps)
step("results", lambda: s.results)
out["total"] = round((time.perf_counter() - t0) * 1000)
print(sys.argv[1], sys.argv[2], out)
