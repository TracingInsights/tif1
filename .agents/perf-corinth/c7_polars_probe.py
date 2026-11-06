"""C7 scratch: what a polars session hands the CLI (types, timings)."""

import sys
import time

t0 = time.perf_counter()
import tif1  # noqa: E402

lib = sys.argv[1]
sess = tif1.get_session(2025, "Bahrain", sys.argv[2] if len(sys.argv) > 2 else "R", lib=lib)
t1 = time.perf_counter()
res = sess.results
t2 = time.perf_counter()
print(
    type(res).__name__, f"get_session {1000 * (t1 - t0):.0f} ms results {1000 * (t2 - t1):.0f} ms"
)
if lib == "polars":
    print(res.schema)
    print(res.row(0, named=True))
    print(res.row(19, named=True))
else:
    print(res.dtypes)
    print(res.iloc[0].to_dict())
d = sess.drivers_df
print(type(d).__name__, d.columns if lib == "polars" else list(d.columns))
fl = sess.get_fastest_laps(by_driver=True)
print(type(fl).__name__, fl.schema if lib == "polars" else fl.dtypes)
t3 = time.perf_counter()
print(f"total {1000 * (t3 - t0):.0f} ms", "pandas" in sys.modules, "pyarrow" in sys.modules)
