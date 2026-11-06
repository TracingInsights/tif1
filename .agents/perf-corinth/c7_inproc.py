"""C7 scratch: in-process split of a CLI command (imports vs typer dispatch vs body)."""

import cProfile
import pstats
import sys
import time

t0 = time.perf_counter()
import tif1.cli  # noqa: E402

t1 = time.perf_counter()
sys.argv = ["tif1", *sys.argv[1:]]
prof = cProfile.Profile() if "PROFILE" in __import__("os").environ else None
try:
    if prof:
        prof.enable()
    tif1.cli.app()
except SystemExit:
    pass
finally:
    if prof:
        prof.disable()
t2 = time.perf_counter()
print(f"import tif1.cli {1000 * (t1 - t0):.1f} ms, app() {1000 * (t2 - t1):.1f} ms", file=sys.stderr)
if prof:
    pstats.Stats(prof, stream=sys.stderr).sort_stats("cumulative").print_stats(40)
