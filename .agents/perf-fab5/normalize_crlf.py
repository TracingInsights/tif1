"""Normalize line endings of the given files to CRLF (repo style)."""

import sys
from pathlib import Path

for arg in sys.argv[1:]:
    p = Path(arg)
    raw = p.read_bytes()
    normalized = raw.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")
    if normalized != raw:
        p.write_bytes(normalized)
        print(f"normalized {arg}")
    else:
        print(f"unchanged {arg}")
