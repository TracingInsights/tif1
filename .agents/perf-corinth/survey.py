"""Corinth survey: one flow per fresh process; prints phase times (ms)."""

import sys
import time

t0 = time.perf_counter()
flow, lib = sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "pandas"
out = {}


def step(name, fn):
    t = time.perf_counter()
    r = fn()
    out[name] = round((time.perf_counter() - t) * 1000)
    return r


import tif1  # noqa: E402 (timed: the import is part of the measurement)

out["import"] = round((time.perf_counter() - t0) * 1000)
if flow == "season":
    sched = step("schedule", lambda: tif1.get_event_schedule(2025))
    rounds = (
        [int(r) for r in sched["RoundNumber"] if int(r) > 0]
        if lib == "pandas"
        else [int(r) for r in sched["RoundNumber"].to_list() if int(r) > 0]
    )

    def loop():
        n = 0
        for r in rounds:
            try:
                s = tif1.get_session(2025, r, "R", lib=lib)
                n += len(s.laps)
                _ = s.results
            except Exception as e:
                out.setdefault("errors", []).append(f"{r}:{type(e).__name__}")
        return n

    step("loop_laps_results", loop)
    out["rounds"] = len(rounds)
elif flow == "schedule":
    step("get_event_schedule", lambda: tif1.get_event_schedule(2024))
    step("get_event", lambda: tif1.get_event(2024, "Monza"))
    step("get_session_resolve", lambda: tif1.get_session(2024, "Monza", "Q"))
elif flow == "ergast":
    from tif1.ergast import Ergast

    e = step("Ergast()", Ergast)
    step("race_results_2024", lambda: e.get_race_results(season=2024))
    step("driver_standings_2024", lambda: e.get_driver_standings(season=2024))
    step("qualifying_2023_r5", lambda: e.get_qualifying_results(season=2023, round=5))
elif flow == "old":
    s = step("get_session", lambda: tif1.get_session(1998, "Monaco", "R", lib=lib))
    step("laps", lambda: s.laps)
    step("results", lambda: s.results)
elif flow == "plot":
    s = tif1.get_session(2025, "Bahrain Grand Prix", "Qualifying")
    from tif1 import plotting

    step("import_plotting", lambda: None)

    def fig():
        import matplotlib as mpl

        mpl.use("Agg")
        import matplotlib.pyplot as plt

        lap = s.laps.pick_drivers("VER").pick_fastest()
        tel = lap.get_car_data().add_distance()
        f, ax = plt.subplots()
        ax.plot(
            tel["Distance"],
            tel["Speed"],
            color=plotting.get_team_color("Red Bull Racing", session=s),
        )
        f.savefig("/tmp/corinth/plot.png")

    step("speed_trace_png", fig)
out["total"] = round((time.perf_counter() - t0) * 1000)
print(flow, lib, out)
