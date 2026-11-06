"""Static drift check of tif1's private seams between two upstream wheels, in memory.

Gortyn G2: the /tmp quota of the shared box left no room for a throwaway venv,
so this downloads the locked and the newest wheel of each seam package into
memory (no disk writes) and compares the source of every private symbol tif1
uses (``tests/unit/test_private_seams.py``): ``same``, ``changed`` (with a
unified diff) or ``MISSING``. Compiled seams are checked by their symbol names
in the extension binary. Run from the repo root::

    python .agents/perf-gortyn/G2_wheel_drift.py [words]

With ``words``, only the diffs of the symbols whose label contains one are shown.
"""

from __future__ import annotations

import ast
import difflib
import io
import re
import sys
import urllib.request
import zipfile

import orjson

TAG = re.compile(r"cp313-cp313-manylinux.*x86_64|cp3\d+-abi3-manylinux.*x86_64|py3-none-any")

# (package, old, new, [(path in wheel, symbol, kind)])
# kind: "def" = a function/class (dotted for methods) whose source is compared;
# "text" = a regex that must match the file; "bin" = bytes that must be in the file.
CHECKS: list[tuple[str, str, str, list[tuple[str, str, str]]]] = [
    (
        "pandas",
        "3.0.6",
        "3.1.0",
        [
            ("pandas/core/internals/__init__.py", r"\bBlockManager\b", "text"),
            ("pandas/core/internals/__init__.py", r"\bSingleBlockManager\b", "text"),
            ("pandas/core/internals/blocks.py", "new_block", "def"),
            ("pandas/core/internals/blocks.py", "ExtensionBlock", "def"),
            ("pandas/core/internals/blocks.py", "Block.iget", "def"),
            ("pandas/core/internals/managers.py", "interleaved_dtype", "def"),
            ("pandas/core/internals/managers.py", "BlockManager.fast_xs", "def"),
            ("pandas/core/internals/managers.py", "BlockManager.__init__", "def"),
            ("pandas/core/internals/managers.py", "SingleBlockManager.from_array", "def"),
            ("pandas/core/internals/managers.py", "SingleBlockManager.__init__", "def"),
            ("pandas/core/construction.py", "sanitize_array", "def"),
            ("pandas/core/construction.py", "ensure_wrapped_if_datetimelike", "def"),
            ("pandas/core/frame.py", "DataFrame._from_arrays", "def"),
            ("pandas/core/frame.py", "DataFrame._ixs", "def"),
            ("pandas/core/frame.py", "DataFrame._constructor_from_mgr", "def"),
            ("pandas/core/frame.py", "DataFrame._constructor_sliced_from_mgr", "def"),
            ("pandas/core/generic.py", "NDFrame._from_mgr", "def"),
            ("pandas/core/indexing.py", "_iLocIndexer._getitem_axis", "def"),
            ("pandas/core/arrays/arrow/array.py", "ArrowExtensionArray._from_pyarrow_array", "def"),
            ("pandas/core/arrays/arrow/array.py", "ArrowExtensionArray.__getitem__", "def"),
            ("pandas/core/arrays/arrow/array.py", "ArrowExtensionArray._concat_same_type", "def"),
            ("pandas/core/arrays/string_arrow.py", "ArrowStringArray.__init__", "def"),
            ("pandas/core/arrays/masked.py", "BaseMaskedArray._simple_new", "def"),
            ("pandas/core/arrays/timedeltas.py", "TimedeltaArray._simple_new", "def"),
            ("pandas/core/arrays/datetimes.py", "DatetimeArray._simple_new", "def"),
            ("pandas/_libs/lib.pyi", "maybe_convert_objects", "def"),
            ("pandas/_libs/tslibs/timedeltas.pyi", "ints_to_pytimedelta", "def"),
            ("pandas/_libs/internals.pyi", "BlockPlacement", "def"),
            ("pandas/_libs/arrays.pyi", "NDArrayBacked._simple_new", "def"),
        ],
    ),
    (
        "polars",
        "2.0.0",
        "2.1.0",
        [
            ("polars/dataframe/frame.py", "DataFrame._from_pydf", "def"),
            ("polars/dataframe/frame.py", r"self\._df\b", "text"),
            ("polars/_plr.py", r"PyDataFrame|polars_runtime", "text"),
        ],
    ),
    ("polars-runtime-32", "2.0.0", "2.1.0", [("*.so", "read_ipc_stream", "bin")]),
    (
        "niquests",
        "3.21.1",
        "3.21.2",
        [
            ("niquests/extensions/revocation/_ocsp/__init__.py", "InMemoryRevocationStatus", "def"),
            ("niquests/extensions/revocation/_ocsp/__init__.py", r"qh3\._hazmat", "text"),
            ("niquests/sessions.py", r"self\._ocsp_cache\b", "text"),
            ("niquests/adapters.py", r"self\._ocsp_cache\b", "text"),
        ],
    ),
    (
        "qh3",
        "1.9.4",
        "2.1.0",
        [
            ("qh3/_hazmat*.so", "OCSPResponse", "bin"),
            ("qh3/_hazmat.pyi", "OCSPResponse", "def"),
            ("qh3/_hazmat.pyi", "Certificate", "def"),
        ],
    ),
]


def _wheel(package: str, version: str) -> zipfile.ZipFile:
    url = f"https://pypi.org/pypi/{package}/{version}/json"
    with urllib.request.urlopen(url, timeout=60) as response:
        files = orjson.loads(response.read())["urls"]
    wheels = [f for f in files if f["filename"].endswith(".whl") and TAG.search(f["filename"])]
    if not wheels:
        raise SystemExit(f"no matching wheel for {package} {version}")
    with urllib.request.urlopen(wheels[0]["url"], timeout=300) as response:
        return zipfile.ZipFile(io.BytesIO(response.read()))


def _member(wheel: zipfile.ZipFile, pattern: str) -> str | None:
    if "*" not in pattern:
        if pattern in wheel.namelist():
            return pattern
        # a module may be a package directory instead (``_ocsp.py`` vs ``_ocsp/__init__.py``)
        alt = pattern.replace("/__init__.py", ".py")
        return alt if alt in wheel.namelist() else None
    regex = re.compile(re.escape(pattern).replace(r"\*", "[^/]*") + "$")
    return next((n for n in wheel.namelist() if regex.search(n)), None)


def _source(text: str, dotted: str) -> str | None:
    tree = ast.parse(text)
    nodes: list[ast.AST] = list(tree.body)
    found: ast.AST | None = None
    parts = dotted.split(".")
    for i, part in enumerate(parts):
        found = None
        queue = list(nodes)
        while queue:
            node = queue.pop(0)
            if isinstance(node, ast.If | ast.Try):  # TYPE_CHECKING / version branches
                queue.extend(node.body)
                continue
            if getattr(node, "name", None) == part and isinstance(
                node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef
            ):
                found = node  # the last definition wins (overloads)
        if found is None:
            return None
        if i < len(parts) - 1:
            nodes = list(getattr(found, "body", []))
    return ast.get_source_segment(text, found) if found is not None else None


def _check(wheel: zipfile.ZipFile, path: str, symbol: str, kind: str) -> str | None:
    member = _member(wheel, path)
    if member is None:
        return None
    data = wheel.read(member)
    if kind == "bin":
        return "present" if symbol.encode() in data else None
    text = data.decode("utf-8")
    if kind == "text":
        return "present" if re.search(symbol, text) else None
    return _source(text, symbol)


def main() -> int:
    """Print one line per symbol and the diffs of the changed ones."""
    drifted = 0
    for package, old, new, checks in CHECKS:
        print(f"\n## {package} {old} -> {new}")
        before, after = _wheel(package, old), _wheel(package, new)
        for path, symbol, kind in checks:
            a, b = _check(before, path, symbol, kind), _check(after, path, symbol, kind)
            label = f"{path} :: {symbol}"
            if b is None:
                drifted += 1
                print(f"MISSING  {label}" + ("" if a is not None else " (absent in old too)"))
            elif a == b:
                print(f"same     {label}")
            else:
                drifted += 1
                print(f"changed  {label}")
                if sys.argv[1:] and not any(word in label for word in sys.argv[1:]):
                    continue
                diff = difflib.unified_diff(
                    (a or "").splitlines(), b.splitlines(), old, new, lineterm="", n=1
                )
                for line in list(diff)[2:60]:
                    print(f"    {line}")
        if package == "niquests":
            for name in ("METADATA",):
                meta = next(n for n in after.namelist() if n.endswith(".dist-info/" + name))
                reqs = [
                    line
                    for line in after.read(meta).decode().splitlines()
                    if line.startswith("Requires-Dist") and "extra ==" not in line
                ]
                print("    " + "; ".join(reqs))
    print(f"\n{drifted} drifted symbol(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
