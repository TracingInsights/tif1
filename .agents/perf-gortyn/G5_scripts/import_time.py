"""Fresh-process import time of pandas / polars, locked site-packages vs .newpkgs, interleaved."""

import os
import statistics
import subprocess
import sys

NEW = "/tmp/tif1-gortyn/G5/.newpkgs"
CODE = "import time; t=time.perf_counter(); import {mod}; print((time.perf_counter()-t)*1e3)"
for mod in ("pandas", "polars", "numpy", "pyarrow"):
    res = {"locked": [], "new": []}
    for i in range(12):
        for arm in ("locked", "new") if i % 2 == 0 else ("new", "locked"):
            env = dict(os.environ)
            env.pop("PYTHONPATH", None)
            if arm == "new":
                env["PYTHONPATH"] = NEW
            out = subprocess.run(
                [sys.executable, "-c", CODE.format(mod=mod)],
                env=env, capture_output=True, text=True, check=True,
            )
            res[arm].append(float(out.stdout.strip()))
    print(
        f"{mod:8s} locked median {statistics.median(res['locked']):6.1f} min {min(res['locked']):6.1f}"
        f" | new median {statistics.median(res['new']):6.1f} min {min(res['new']):6.1f}"
    )
