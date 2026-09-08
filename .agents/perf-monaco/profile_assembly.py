"""Profile the current per-payload telemetry assembly (variant A) offline."""

import cProfile
import pickle
import pstats

from tif1.core_utils.helpers import _create_telemetry_df

with open("/tmp/monaco_tel_payloads.pkl", "rb") as f:
    entries: list[tuple[str, int, dict]] = pickle.load(f)


def variant_a() -> int:
    n = 0
    for driver, lap, tel in entries:
        df = _create_telemetry_df(tel, driver, lap, "pandas")
        if df is not None and not df.empty:
            n += 1
    return n


variant_a()  # warmup
cProfile.run("variant_a()", "/tmp/prof_assembly.out")
p = pstats.Stats("/tmp/prof_assembly.out")
p.sort_stats("cumulative").print_stats(22)
