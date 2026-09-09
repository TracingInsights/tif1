"""Normalize a text file's line endings to CRLF (per-file convention fix).

Usage: python3 .agents/perf-kleitor/normalize_crlf.py FILE [FILE ...]
"""

import sys
from pathlib import Path

for arg in sys.argv[1:]:
    path = Path(arg)
    data = path.read_bytes()
    lines = data.replace(b"\r\n", b"\n").split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    path.write_bytes(b"\r\n".join(lines) + b"\r\n")
    print(f"normalized {arg} -> CRLF ({len(lines)} lines)")
