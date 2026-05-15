#!/usr/bin/env python3
"""
Version update script for pyproject.toml
Usage: python update-version.py <new_version>
"""

import hashlib
import re
import sys
from pathlib import Path


def get_file_hash(filepath):
    """Get SHA256 hash of file contents."""
    with open(filepath, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def update_version(new_version):
    """Update version in pyproject.toml with validation."""
    pyproject_path = Path("pyproject.toml")

    if not pyproject_path.exists():
        print("❌ pyproject.toml not found")
        return False

    # Validate version format before proceeding
    if not re.match(r"^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$", new_version):
        print(f"❌ Invalid version format: {new_version}")
        return False

    # Get original file hash
    original_hash = get_file_hash(pyproject_path)

    try:
        # Read current content
        content = pyproject_path.read_text(encoding="utf-8")

        # Validate current version field exists
        if not re.search(r'(?m)^version = ".*"', content):
            print("❌ No version field found in pyproject.toml")
            return False

        # Update version
        new_content = re.sub(r'(?m)^version = ".*"', f'version = "{new_version}"', content)

        # Validate change was made
        if content == new_content:
            print("❌ Version was not updated (already at target version?)")
            return False

        # Write updated content
        pyproject_path.write_text(new_content, encoding="utf-8")

        # Verify file was actually changed
        new_hash = get_file_hash(pyproject_path)
        if original_hash == new_hash:
            print("❌ File hash unchanged after version update")
            return False

        print(f"✅ Successfully updated version to {new_version}")
        return True

    except OSError as e:
        print(f"❌ Failed to update version: {e}")
        return False


def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python update-version.py <new_version>")
        sys.exit(1)

    new_version = sys.argv[1]

    if not update_version(new_version):
        sys.exit(1)


if __name__ == "__main__":
    main()
