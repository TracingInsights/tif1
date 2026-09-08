"""Apply text replacements to CRLF (or LF) source files from a JSON patch file.

Usage: python .agents/perf-monaco/patch_crlf.py patch.json

patch.json: [{"path": "...", "old": "...", "new": "..."}, ...]
Strings use ordinary \n; they are converted to the target file's line-ending
style before matching. Each "old" must occur exactly once.
"""

import json
import sys
from pathlib import Path


def main() -> None:
    patch = json.loads(Path(sys.argv[1]).read_text())
    for entry in patch:
        path = Path(entry["path"])
        raw = path.read_bytes()
        crlf = b"\r\n" in raw
        text = raw.decode("utf-8")
        if crlf:
            old = entry["old"].replace("\n", "\r\n")
            new = entry["new"].replace("\n", "\r\n")
        else:
            old, new = entry["old"], entry["new"]
        count = text.count(old)
        if count != 1:
            raise SystemExit(f"{path}: old string occurs {count} times (need exactly 1)")
        path.write_bytes(text.replace(old, new).encode("utf-8"))
        print(f"patched {path}")


if __name__ == "__main__":
    main()
