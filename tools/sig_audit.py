#!/usr/bin/env python3
"""Signature audit: compare documented API signatures against source introspection.

Extracts `def name(params)` definitions and `tif1.module.func(...)` call patterns
from docs/api-reference pages and compares them with the public API of src/tif1.
Emits a text report (or JSON with --json) listing:

- documented signatures whose parameter list differs from source
- source public symbols that no API page documents (coverage gaps)

Heuristic by design: parameter-name comparison only, defaults and types are
reported from source for manual review.

Usage:
    uv run python tools/sig_audit.py
    uv run python tools/sig_audit.py --json
    uv run python tools/sig_audit.py --page docs/api-reference/core.mdx
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import pkgutil
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"
API_DIR = DOCS_DIR / "api-reference"

# Page files that do not map 1:1 onto a module name.
PAGE_MODULE_ALIASES = {
    "http": "http_session",
    "session": "core",
    "core": "core",
    "utilities": "utils",
    "fastf1-compat": "fastf1_compat",
}

DEF_RE = re.compile(r"^\s*(?:async\s+)?def\s+(\w+)\s*\(([^)]*)\)", re.MULTILINE)
PARAM_SPLIT_RE = re.compile(r",(?![^(\[]*[)\]])")


def _clean_param(raw: str) -> str | None:
    """Reduce a documented parameter fragment to a bare name."""
    token = raw.strip()
    if not token or token.startswith(("*", "/", "**")):
        return None
    name = token.split("=")[0].split(":")[0].strip()
    if name in {"self", "cls"}:
        return None
    return name or None


def _source_params(sig: inspect.Signature) -> list[str]:
    names = []
    for param in sig.parameters.values():
        if param.name in {"self", "cls"}:
            continue
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        names.append(param.name)
    return names


def collect_source_api() -> dict[str, dict[str, object]]:
    """Import tif1 and map public callable names to source signatures."""
    sys.path.insert(0, str(REPO_ROOT / "src"))
    for stale in [m for m in sys.modules if m == "tif1" or m.startswith("tif1.")]:
        del sys.modules[stale]
    import tif1

    api: dict[str, dict[str, object]] = {}
    for _, mod_name, _ in pkgutil.iter_modules(tif1.__path__, "tif1."):
        module = importlib.import_module(mod_name)
        short = mod_name.removeprefix("tif1.")
        for attr_name, member in vars(module).items():
            if attr_name.startswith("_"):
                continue
            member_module = getattr(member, "__module__", "")
            if not member_module.startswith("tif1"):
                continue
            key = f"{short}.{attr_name}"
            if callable(member) and not isinstance(member, type):
                try:
                    api[key] = {
                        "params": _source_params(inspect.signature(member)),
                        "module": member_module,
                    }
                except (TypeError, ValueError):
                    continue
            elif isinstance(member, type) and member_module == mod_name:
                api[key] = {"params": [], "module": member_module, "class": True}
                for m_name, m_obj in inspect.getmembers(member, inspect.isfunction):
                    if m_name.startswith("_"):
                        continue
                    if not getattr(m_obj, "__module__", "").startswith("tif1"):
                        continue  # inherited (e.g. pandas DataFrame) members
                    m_key = f"{key}.{m_name}"
                    try:
                        api[m_key] = {
                            "params": _source_params(inspect.signature(m_obj)),
                            "module": member_module,
                        }
                    except (TypeError, ValueError):
                        continue
    return api


def extract_docs_defs(page: Path) -> list[tuple[str, list[str], int]]:
    """Return (name, params, line) for every `def` in fenced python blocks."""
    text = page.read_text(encoding="utf-8").replace("\r\n", "\n")
    out: list[tuple[str, list[str], int]] = []
    in_fence = False
    fence_lang = ""
    for number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith(("```", "~~~")):
            if in_fence:
                in_fence = False
            else:
                in_fence = True
                fence_lang = stripped.lstrip("`~").strip()
            continue
        if not in_fence or fence_lang not in {"python", "py", ""}:
            continue
        match = re.match(r"^(?:async\s+)?def\s+(\w+)\s*\(([^)]*)\)", line)
        if not match:
            continue  # indented defs are example-local wrappers, not documented API
        name, raw_params = match.group(1), match.group(2)
        params = [p for p in (_clean_param(x) for x in PARAM_SPLIT_RE.split(raw_params)) if p]
        out.append((name, params, number))
    return out


def page_body(page: Path) -> str:
    return page.read_text(encoding="utf-8").replace("\r\n", "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", action="store_true", help="print JSON report")
    parser.add_argument("--page", type=Path, help="audit a single docs page")
    args = parser.parse_args(argv)

    source_api = collect_source_api()
    pages = [args.page] if args.page else sorted(API_DIR.glob("*.mdx"))
    all_body = "\n".join(page_body(p) for p in pages)

    mismatches: list[dict[str, object]] = []
    for page in pages:
        page_hint = PAGE_MODULE_ALIASES.get(page.stem, page.stem.replace("-", "_"))
        for name, doc_params, line in extract_docs_defs(page):
            # Skip helpers the doc defines inline for its own example.
            candidates = [
                (key, info) for key, info in source_api.items() if key.split(".")[-1] == name
            ]
            if not candidates:
                continue
            hinted = [c for c in candidates if c[0].split(".")[0] == page_hint]
            pool = hinted or candidates
            if len(pool) > 1:
                continue  # ambiguous name across modules; manual review
            key, info = pool[0]
            src_params = info["params"]
            if doc_params != src_params:
                mismatches.append(
                    {
                        "page": page.name,
                        "line": line,
                        "symbol": key,
                        "doc_params": doc_params,
                        "source_params": src_params,
                    }
                )

    undocumented = []
    for key in sorted(source_api):
        leaf = key.split(".")[-1]
        module_prefix = key.split(".")[0]
        searchable = {f"tif1.{module_prefix}.{leaf}", f"tif1.{leaf}", leaf}
        if not any(s in all_body for s in searchable):
            undocumented.append(key)

    if args.json:
        print(
            json.dumps(
                {
                    "source_symbols": len(source_api),
                    "pages": len(pages),
                    "mismatches": mismatches,
                    "undocumented": undocumented,
                },
                indent=2,
            )
        )
        return 0

    print(f"source public symbols: {len(source_api)}")
    print(f"pages audited: {len(pages)}")
    print(f"\nsignature mismatches: {len(mismatches)}")
    for m in mismatches:
        print(f"  {m['page']}:{m['line']} {m['symbol']}")
        print(f"    doc:    ({', '.join(m['doc_params'])})")  # type: ignore[arg-type]
        print(f"    source: ({', '.join(m['source_params'])})")  # type: ignore[arg-type]
    print(f"\nundocumented source symbols: {len(undocumented)}")
    for key in undocumented:
        print(f"  {key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
