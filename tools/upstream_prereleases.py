"""List the newest release and pre-releases of tif1's upstreams with private seams.

Used by the upstream canary (``.github/workflows/upstream-canary.yml``) to put
the versions next to a failure, and by hand before a lock bump::

    python tools/upstream_prereleases.py            # PyPI index, newest first
    python tools/upstream_prereleases.py --installed  # what this interpreter has

The packages are the ones ``tests/unit/test_private_seams.py`` names.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import sys
import urllib.request

import orjson
from packaging.version import InvalidVersion, Version

#: Distributions whose private API tif1 relies on (see tests/unit/test_private_seams.py).
PACKAGES = (
    "pandas",
    "polars",
    "polars-runtime-32",
    "numpy",
    "pyarrow",
    "niquests",
    "urllib3-future",
    "qh3",
)


def _index_versions(package: str) -> tuple[str, list[str]]:
    """(latest stable, pre-releases newer than it) from PyPI's JSON API."""
    url = f"https://pypi.org/pypi/{package}/json"
    with urllib.request.urlopen(url, timeout=30) as response:
        data = orjson.loads(response.read())
    stable = Version(data["info"]["version"])
    pre: list[Version] = []
    for raw, files in data["releases"].items():
        if not files:
            continue
        try:
            version = Version(raw)
        except InvalidVersion:
            continue
        if version.is_prerelease and version > stable:
            pre.append(version)
    return str(stable), [str(v) for v in sorted(pre, reverse=True)]


def main(argv: list[str] | None = None) -> int:
    """Print one line per package."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--installed", action="store_true", help="installed versions only")
    args = parser.parse_args(argv)
    for package in PACKAGES:
        if args.installed:
            try:
                print(f"{package:<20} {importlib.metadata.version(package)}")
            except importlib.metadata.PackageNotFoundError:
                print(f"{package:<20} (not installed)")
            continue
        stable, pre = _index_versions(package)
        print(f"{package:<20} stable {stable:<14} pre-releases: {', '.join(pre[:4]) or '-'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
