"""Normalize working-tree files to each file's HEAD line-ending style."""

import subprocess
import sys
from pathlib import Path


def head_dominant_ending(path: Path) -> bytes:
    raw = subprocess.run(
        ["git", "show", f"HEAD:{path.as_posix()}"], capture_output=True, check=True
    ).stdout
    crlf = raw.count(b"\r\n")
    lf = raw.count(b"\n") - crlf
    return b"\r\n" if crlf >= lf else b"\n"


for arg in sys.argv[1:]:
    p = Path(arg)
    ending = head_dominant_ending(p)
    raw = p.read_bytes()
    normalized = raw.replace(b"\r\n", b"\n")
    if ending == b"\r\n":
        normalized = normalized.replace(b"\n", b"\r\n")
    if normalized != raw:
        p.write_bytes(normalized)
        style = "CRLF" if ending == b"\r\n" else "LF"
        print(f"normalized {arg} -> {style}")
    else:
        style = "CRLF" if ending == b"\r\n" else "LF"
        print(f"unchanged {arg} ({style})")
