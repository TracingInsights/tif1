#!/usr/bin/env python3
"""Run Python code blocks from the documentation headless and report results.

Executes every fenced Python block per page (except blocks whose title names
Pyodide, which run in the browser only, and ``.py``-titled module listings)
sequentially in one subprocess per
page (blocks in a page share state, matching the reading order). Blocks that
raise are reported with the exception; a block whose own text announces an
error ("Raises", "Error", "try:") is classified as an error demonstration.
Bodiless signature listings on reference pages (``def f(x: int) -> str``) are
reported as signatures, not failures.

Usage:
    uv run python tools/run_doc_examples.py [paths ...] [--json] [--timeout 300]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

RUNNER = r"""
import inspect
import json
import os
import shutil
import matplotlib
matplotlib.use("Agg")
import sys
from pathlib import Path

# The pages run against the developer's real data cache (fast, offline), so
# the cache-maintenance examples they demonstrate (Cache.clear(),
# invalidate("all"), tif1.Cache.clear_cache(), enable_cache(force_renew=True),
# shutil.rmtree on the cache dir, unlinking cache.sqlite) would wipe it. Those
# calls are still argument-checked against the real signature, then skipped.
import tif1.cache as _cache_mod
from tif1 import fastf1_compat as _fastf1_compat

_PROTECTED = Path(os.environ["TIF1_CACHE_DIR"]).resolve()


def _guard(owner, name, static=False):
    sig = inspect.signature(getattr(owner, name))

    def skipped(*args, **kwargs):
        sig.bind(*args, **kwargs)
        print(f"[run_doc_examples] skipped {owner.__name__}.{name}() on the shared cache", file=sys.stderr)

    setattr(owner, name, staticmethod(skipped) if static else skipped)


_guard(_cache_mod.Cache, "clear")
_guard(_cache_mod.Cache, "invalidate")
_guard(_fastf1_compat.Cache, "clear_cache", static=True)
def _touches_protected(path):
    try:
        target = Path(path).resolve()
    except (TypeError, ValueError):
        return False
    return target == _PROTECTED or _PROTECTED in target.parents or target in _PROTECTED.parents


def _refuse_on_protected(owner, name):
    real = getattr(owner, name)

    def safe(path, *args, **kwargs):
        if _touches_protected(path):
            print(f"[run_doc_examples] refused {name}({path}) on the shared cache", file=sys.stderr)
            return None
        return real(path, *args, **kwargs)

    setattr(owner, name, safe)


# Every deletion path, not only rmtree: a page once unlinked cache.sqlite.
for _owner, _name in (
    (shutil, "rmtree"),
    (os, "remove"),
    (os, "unlink"),
    (os, "rmdir"),
    (Path, "unlink"),
    (Path, "rmdir"),
):
    _refuse_on_protected(_owner, _name)

import re as _re


def _is_signature(block):
    lines = block.splitlines()
    if not lines:
        return False
    out = []
    head = _re.compile(r"^\s*(async\s+def|def|class)\s+\w+")
    for line in lines:
        out.append(line + ": ..." if head.match(line) and not line.rstrip().endswith(":") and line == line.lstrip() else line)
    joined = "\n".join(out)
    try:
        compile(joined, "<sig>", "exec")
    except SyntaxError:
        # Multi-line signatures: the closing ``) -> T`` line carries the colon.
        try:
            compile(_re.sub(r"(\)\s*(?:->\s*[^\n:]+)?)\s*$", r"\1: ...", block, flags=_re.M), "<sig>", "exec")
        except SyntaxError:
            return False
    return True


blocks = json.loads(sys.argv[1])
# Blocks share one namespace in reading order. Tabbed pandas/polars examples
# (fence titles "Pandas ..." / "Polars ...") are alternatives, not a sequence:
# each backend continues its own namespace, seeded from the page state the
# first time it appears.
ns_main = {}
# A Jupyter kernel exposes display() as a builtin; the notebook pages use it.
try:
    from IPython.display import display as _display

    ns_main["display"] = _display
except ImportError:
    pass
tab_namespaces = {}
results = []
for i, (title, block) in enumerate(blocks):
    backend = next((name for name in ("Pandas", "Polars") if title.startswith(name)), None)
    ns = ns_main if backend is None else tab_namespaces.setdefault(backend, dict(ns_main))
    try:
        code = compile(block, f"<block {i}>", "exec")
    except SyntaxError as exc:
        # Reference pages show bodiless signatures (``def f(x: int) -> str``); a
        # block that compiles once every signature line gets a ``...`` body is a
        # signature listing, not a broken example.
        results.append({"index": i, "status": "signature" if _is_signature(block) else "error", "error": f"{type(exc).__name__}: {exc}"})
        continue
    try:
        exec(code, ns)
        results.append({"index": i, "status": "ok"})
    except Exception as exc:  # noqa: BLE001
        results.append({"index": i, "status": "error", "error": f"{type(exc).__name__}: {exc}"})
print(json.dumps(results))
"""


def extract_blocks(page: Path) -> list[tuple[str, str]]:
    """``(fence title, code)`` for every runnable Python block of ``page``."""
    text = page.read_text(encoding="utf-8").replace("\r\n", "\n")
    blocks: list[tuple[str, str]] = []
    current: list[str] = []
    in_fence = False
    lang = ""
    title = ""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(("```", "~~~")):
            if in_fence:
                blocks.append((title, "\n".join(current)))
                current = []
                in_fence = False
            else:
                in_fence = True
                # The language is the first word; Mintlify puts a tab or file
                # title after it (```python Pandas inside a CodeGroup).
                info = stripped.lstrip("`~").split()
                lang = info[0].lower() if info else ""
                # A title naming Pyodide marks browser-only code (top-level
                # await, micropip); a title that is a ``.py`` path marks a
                # module listing. Neither is a native example.
                title = " ".join(info[1:])
                if "pyodide" in title.lower() or title.endswith(".py"):
                    lang = ""
            continue
        if in_fence and lang in {"python", "py"}:
            current.append(line)
    return [(t, textwrap.dedent(b).strip()) for t, b in blocks if b.strip()]


def is_error_demo(block: str) -> bool:
    lowered = block.lower()
    return "raises" in lowered or "try:" in lowered or "except" in lowered or "traceback" in lowered


def is_network_demo(block: str) -> bool:
    return bool(re.search(r"get_session|\.load\(|laps_async|get_fastest|fetch|Session\(", block))


def run_page(page: Path, timeout: int) -> dict[str, object]:
    blocks = extract_blocks(page)
    if not blocks:
        return {"page": page.as_posix(), "blocks": 0, "results": []}
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", delete=False, encoding="utf-8"
    ) as handle:
        handle.write(RUNNER)
        runner_path = handle.name
    # Doc blocks may call config.save(), which writes $HOME/.tif1rc, and write
    # files (to_csv, sqlite3.connect, config.save("./x.json")) relative to the
    # working directory. Run each page with a throwaway HOME that is also its
    # working directory (removed afterwards), keep the shared data cache, and
    # pin the default backend.
    env = dict(os.environ)
    home = tempfile.mkdtemp(prefix="tif1_docs_")
    env["HOME"] = home
    shared_cache = Path.home() / ".cache" / "tif1"
    env["TIF1_CACHE_DIR"] = str(shared_cache)
    env["TIF1_LIB"] = "pandas"
    try:
        proc = subprocess.run(  # noqa: PLW1510 -- exit code handled below
            [sys.executable, runner_path, json.dumps(blocks)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=home,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return {"page": page.as_posix(), "blocks": len(blocks), "status": "timeout"}
    finally:
        Path(runner_path).unlink(missing_ok=True)
        shutil.rmtree(home, ignore_errors=True)
    if proc.returncode != 0:
        return {
            "page": page.as_posix(),
            "blocks": len(blocks),
            "status": "crashed",
            "error": proc.stderr[-2000:],
        }
    try:
        results = json.loads(proc.stdout.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError):
        return {
            "page": page.as_posix(),
            "blocks": len(blocks),
            "status": "crashed",
            "error": proc.stdout[-2000:] + proc.stderr[-2000:],
        }
    for i, (_title, block) in enumerate(blocks):
        for row in results:
            if row["index"] == i and row["status"] == "error":
                row["error_demo"] = is_error_demo(block)
                row["network"] = is_network_demo(block)
    return {"page": page.as_posix(), "blocks": len(blocks), "results": results}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="*", type=Path, help="mdx files or directories")
    parser.add_argument("--json", action="store_true", help="print JSON report")
    parser.add_argument("--timeout", type=int, default=420, help="seconds per page")
    args = parser.parse_args(argv)

    targets = args.paths or [
        REPO_ROOT / "docs/tutorials",
        REPO_ROOT / "docs/guides",
        REPO_ROOT / "docs/quickstart.mdx",
        REPO_ROOT / "docs/getting-started.mdx",
        REPO_ROOT / "docs/examples.mdx",
        REPO_ROOT / "docs/installation.mdx",
        REPO_ROOT / "docs/introduction.mdx",
    ]
    pages: list[Path] = []
    for target in targets:
        if target.is_dir():
            pages.extend(sorted(target.rglob("*.mdx")))
        elif target.is_file():
            pages.append(target)

    report = []
    for page in pages:
        row = run_page(page, args.timeout)
        report.append(row)
        failures = [
            r
            for r in row.get("results", [])
            if r.get("status") == "error" and not r.get("error_demo")
        ]
        status = row.get("status", "ok")
        print(
            f"{page.as_posix()}: blocks={row['blocks']} status={status} failures={len(failures)}",
            flush=True,
        )
        for f in failures:
            print(f"    block {f['index']}: {f['error']}", flush=True)

    if args.json:
        print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
