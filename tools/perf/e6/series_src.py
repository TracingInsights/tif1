"""E6 probe: print the code after the docstring of polars Series methods (plan or native)."""

from __future__ import annotations

import inspect
import sys

import polars as pl

for name in sys.argv[1].split(","):
    src = inspect.getsource(getattr(pl.Series, name))
    body = src[src.rfind('"""') + 3 :].strip()
    print(f"===== {name}\n{body[:500]}")
